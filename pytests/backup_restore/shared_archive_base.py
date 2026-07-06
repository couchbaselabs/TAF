"""
Shared Backup Archive – Base test class.

All helper methods and setUp / tearDown live here.  Individual scenario
files (shared_archive_happy_path.py, shared_archive_rebalance.py, …)
inherit from SharedArchiveBaseTest and only contain test_* methods.

Supported cloud providers
--------------------------
Pass ``cloud_provider=<value>`` as a test parameter.  Accepted values:

  aws   (default) – Capella on AWS, archive stored in S3
  gcp             – Capella on GCP, archive stored in Google Cloud Storage
  azure           – Capella on Azure, archive stored in Azure Blob Storage

Credential resolution per provider
------------------------------------
AWS:
  1. AWS_ACCESS_KEY_ID already in os.environ          → used as-is
  2. TAF_CAPELLA_ROLE_ARN set                          → STS AssumeRole
  3. AWS_PROFILE / TAF_AWS_PROFILE set                 → named profile
  4. None of the above                                 → boto3 default chain

  Recommended local workflow:
    cd /path/to/couchbase-cloud
    go run scripts/cbc-aws-assumerole/main.go -profile cbc-main -account dbaas-test-0005
    export AWS_PROFILE=dbaas-test-0005-temp

GCP:
  1. GOOGLE_APPLICATION_CREDENTIALS already set       → used as-is
  2. TAF_GCP_CREDENTIALS_FILE set                     → pointed at SA JSON file
  3. None of the above                                → Application Default Credentials (gcloud auth)

  Recommended local workflow:
    gcloud auth application-default login
    # or export GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa-key.json

Azure:
  cbbackupmgr uses storage-account-key auth (no --capella).
  Archive URL: az://cbc-storage-{cluster_id}/backups
    - Container: cbc-storage-{cluster_id}  (full UUID with hyphens)
    - Account:   {cluster_id_no_hyphens[:24]}  (passed via --obj-access-key-id)
    - Key:       storage account access key  (passed via --obj-secret-access-key)

  Credential resolution order:
    1. -p azure_storage_key=<key>  or  AZURE_STORAGE_KEY env var  → used as-is
    2. Azure SDK dynamic fetch using:
         AZURE_AAD_TENANT_ID  + AZURE_AAD_CLIENT_ID  + AZURE_AAD_CLIENT_SECRET
         + AZURE_SUBSCRIPTION_ID
       Fetches the key from resource group rg-{cluster_id} at runtime.
       Install: pip install azure-mgmt-storage azure-identity
"""

import copy
import os
import platform
import re
import shutil
import shutil as _shutil  # alias used in module-level _default_cbbackupmgr_path()
import subprocess
import threading
import time
import urllib.request

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

try:
    from azure.identity import AzureCliCredential
    from azure.mgmt.storage import StorageManagementClient
    HAS_AZURE_SDK = True
except ImportError:
    HAS_AZURE_SDK = False

from capella_utils.dedicated import CapellaUtils
from constants.platform_constants.os_constants import Linux, Mac, Windows
from dedicatedbasetestcase import ProvisionedBaseTestCase


def _default_cbbackupmgr_path():
    """
    Resolve cbbackupmgr without requiring an explicit test parameter.

    Resolution order:
      1. ``cbbackupmgr`` found on PATH (e.g. dev-tools tarball bin dir
         added via ``export PATH=/path/to/dev-tools/bin:$PATH``)
      2. OS standard Couchbase Server install path:
           Linux   : /opt/couchbase/bin/cbbackupmgr
           macOS   : /Applications/Couchbase Server.app/.../bin/cbbackupmgr
           Windows : C:/Program Files/Couchbase/Server/bin/cbbackupmgr.exe

    To use a dev-tools tarball without passing -p cbbackupmgr_path each time:
      export PATH=/Users/you/Downloads/couchbase-server-dev-tools-x.y.z/bin:$PATH
    """
    # 1. Check PATH first
    on_path = _shutil.which("cbbackupmgr")
    if on_path:
        return on_path

    # 2. Fall back to OS standard install
    system = platform.system()
    if system == "Darwin":
        return Mac.COUCHBASE_BIN_PATH + "cbbackupmgr"
    if system == "Windows":
        return Windows.COUCHBASE_BIN_PATH + "cbbackupmgr.exe"
    return Linux.COUCHBASE_BIN_PATH + "cbbackupmgr"

try:
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

# ---------------------------------------------------------------------------
# Archive URL scheme per cloud provider
# ---------------------------------------------------------------------------

_ARCHIVE_SCHEMES = {
    "aws":   "s3",
    "gcp":   "gs",
    "azure": "az",
}

# ---------------------------------------------------------------------------
# Base test class
# ---------------------------------------------------------------------------

class SharedArchiveBaseTest(ProvisionedBaseTestCase):
    """
    Base class for Shared Backup Archive tests.

    Handles:
      - setUp: provider detection, credential injection, cluster resolution,
               staging-dir management, region detection
      - tearDown: optional cluster preservation
      - All cbbackupmgr helper methods shared across scenario subclasses
      - All Capella cluster helper methods (allow IP, sample bucket, scale, …)

    Subclasses only need to define test_* methods.
    """

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def setUp(self):
        super(SharedArchiveBaseTest, self).setUp()

        # ---- Cloud provider ----
        # Read from 'provider' (the existing TAF parameter that ProvisionedBaseTestCase
        # already uses to decide which cloud to deploy clusters on: aws / gcp / azure).
        # This way a single -p provider=gcp both provisions GCP clusters AND uses
        # gs:// archive URLs + GCP credentials — no second parameter needed.
        # Fall back to the legacy 'cloud_provider' param for backward compat,
        # then to 'aws' if neither is set.
        self.cloud_provider = (
            self.input.param("provider", None)
            or self.input.param("cloud_provider", "aws")
        ).lower()
        if self.cloud_provider not in _ARCHIVE_SCHEMES:
            self.fail(
                "Unsupported provider '%s'. Valid values: %s"
                % (self.cloud_provider, ", ".join(sorted(_ARCHIVE_SCHEMES)))
            )
        self.log.info("Cloud provider: %s", self.cloud_provider)

        # ---- cbbackupmgr binary ----
        # Default: OS-appropriate path for an installed Couchbase Server,
        # matching the convention in cb_tools_base.py:
        #   Linux : /opt/couchbase/bin/cbbackupmgr
        #   macOS : /Applications/Couchbase Server.app/.../bin/cbbackupmgr
        #   Windows: C:/Program Files/Couchbase/Server/bin/cbbackupmgr.exe
        # Override with -p cbbackupmgr_path=/path/to/cbbackupmgr when using
        # a dev-tools tarball or non-standard install.
        self.cbbackupmgr_path = self.input.param(
            "cbbackupmgr_path",
            _default_cbbackupmgr_path(),
        )

        # ---- Object-store parameters ----
        self.obj_region = self.input.param("obj_region", "")
        self.repo_name = self.input.param("repo_name", "shared-repo")
        self.staging_dir_backup = self.input.param(
            "staging_dir_backup",
            os.path.expanduser("~/couchbase/tmp/staging_backup"),
        )
        self.staging_dir_restore = self.input.param(
            "staging_dir_restore",
            os.path.expanduser("~/couchbase/tmp/staging_restore"),
        )

        # ---- Backup / restore parameters ----
        # Default to 1 thread — cbbackupmgr 8.0.0-3777 has a race condition
        # where parallel threads fail to create the per-vBucket 'data/'
        # subdirectory in the staging dir before opening the SQLite index,
        # producing "unable to open database file: no such file or directory".
        # Increase via -p backup_threads=N once that bug is fixed upstream.
        self.backup_threads = self.input.param("backup_threads", 1)
        self.sample_bucket = self.input.param("sample_bucket", "travel-sample")
        self.no_ssl_verify = self.input.param("no_ssl_verify", True)

        # ---- Inject cloud credentials before any subprocess is launched ----
        self._inject_credentials()

        # ---- Resolve source and destination clusters ----
        tenant = self.tenants[0]
        if len(tenant.clusters) < 2:
            self.fail(
                "Test requires num_clusters=2; found only %d cluster(s)."
                % len(tenant.clusters)
            )
        self.source_cluster = tenant.clusters[0]
        self.dest_cluster = tenant.clusters[1]
        self.tenant = tenant

        # ---- Build archive URL from provider + cluster id ----
        scheme = _ARCHIVE_SCHEMES[self.cloud_provider]
        self.bucket_name = "cbc-storage-%s" % self.source_cluster.id

        if self.cloud_provider == "azure":
            # Azure archive URL: az://<container>/<path>
            #   Container = cbc-storage-{cluster_id}  (full UUID with hyphens)
            #   Path      = backups
            # Account name (hyphens removed, max 24 chars) passed via
            # --obj-access-key-id per StorageAccountName() in couchbase-cloud:
            #   internal/clusters/infra/azure/storage.go
            self.azure_storage_account = (
                self.input.param("azure_storage_account", None)
                or os.environ.get("AZURE_STORAGE_ACCOUNT", None)
                or self.source_cluster.id.replace("-", "")[:24]
            )
            self.azure_storage_key = (
                self.input.param("azure_storage_key", None)
                or os.environ.get("AZURE_STORAGE_KEY", None)
                or self._fetch_azure_storage_key()
            )
            if not self.azure_storage_key:
                self.fail(
                    "Azure storage account key is required. Options:\n"
                    "  1. Set AZURE_STORAGE_KEY env var\n"
                    "  2. Pass -p azure_storage_key=<key>\n"
                    "  3. Run 'az login' and set AZURE_SUBSCRIPTION_ID env var"
                )
            self.archive = "az://%s/backups" % self.bucket_name
        else:
            # AWS:  s3://cbc-storage-{cluster_id}/backups
            # GCP:  gs://cbc-storage-{cluster_id}/backups
            self.archive = "%s://%s/backups" % (scheme, self.bucket_name)

        # ---- Wipe and recreate staging directories ----
        # Stale staging metadata from a previous run's cluster causes
        # "remote archive does not exist" errors.
        for staging_dir in (self.staging_dir_backup, self.staging_dir_restore):
            if os.path.exists(staging_dir):
                self.log.info("Removing stale staging dir: %s", staging_dir)
                shutil.rmtree(staging_dir)
            os.makedirs(staging_dir)

        # ---- Auto-detect region if not explicitly set ----
        if not self.obj_region:
            self.obj_region = self._detect_region() or "us-east-1"
        else:
            detected = self._detect_region()
            if detected and detected != self.obj_region:
                self.log.info(
                    "Detected region '%s' overrides configured region '%s'",
                    detected, self.obj_region,
                )
                self.obj_region = detected

        self.log.info(
            "Source cluster      : %s  SRV=%s",
            self.source_cluster.id, self.source_cluster.srv,
        )
        self.log.info(
            "Destination cluster : %s  SRV=%s",
            self.dest_cluster.id, self.dest_cluster.srv,
        )
        self.log.info(
            "Archive             : %s  region=%s",
            self.archive, self.obj_region,
        )

    def tearDown(self):
        """
        By default (skip_teardown=False) clusters are destroyed after each test
        to avoid leaving orphaned Capella resources.

        Pass skip_teardown=True to preserve clusters between runs (useful when
        iterating quickly on a single test to avoid the ~5 min cluster-creation
        overhead).
        """
        skip = self.input.param("skip_teardown", False)
        if skip:
            self.log.info(
                "tearDown: skip_teardown=True — leaving clusters intact "
                "(source=%s, dest=%s)",
                getattr(self, "source_cluster", type("_", (), {"id": "?"})()).id,
                getattr(self, "dest_cluster", type("_", (), {"id": "?"})()).id,
            )
        else:
            super(SharedArchiveBaseTest, self).tearDown()

    # ------------------------------------------------------------------
    # Credential injection (provider-specific)
    # ------------------------------------------------------------------

    def _inject_credentials(self):
        """Dispatch to the provider-specific credential injector."""
        dispatch = {
            "aws":   self._inject_aws_credentials,
            "gcp":   self._inject_gcp_credentials,
            "azure": self._inject_azure_credentials,
        }
        dispatch[self.cloud_provider]()

    def _inject_aws_credentials(self):
        """
        Inject AWS credentials into os.environ so cbbackupmgr subprocesses
        can reach Capella-managed S3 buckets.

        Resolution order (first match wins):
          1. AWS_ACCESS_KEY_ID already set → used as-is
          2. TAF_CAPELLA_ROLE_ARN set      → STS AssumeRole
          3. AWS_PROFILE / TAF_AWS_PROFILE → named profile
          4. None of the above             → boto3 default chain
        """
        if os.environ.get("AWS_ACCESS_KEY_ID"):
            self.log.info(
                "AWS_ACCESS_KEY_ID already set (key: %s…) — skipping injection",
                os.environ["AWS_ACCESS_KEY_ID"][:8],
            )
            return

        if not HAS_BOTO3:
            raise RuntimeError(
                "boto3 is required for AWS credential injection; "
                "install it with: pip install boto3"
            )

        role_arn = os.environ.get("TAF_CAPELLA_ROLE_ARN", "")
        profile = os.environ.get("TAF_AWS_PROFILE") or os.environ.get("AWS_PROFILE", "")

        if role_arn:
            self.log.info("Assuming AWS role via STS: %s", role_arn)
            try:
                sts = boto3.client("sts")
                resp = sts.assume_role(
                    RoleArn=role_arn,
                    RoleSessionName="TafCbbackupmgr",
                    DurationSeconds=3600,
                )
            except (ClientError, NoCredentialsError) as exc:
                raise RuntimeError("STS AssumeRole failed: %s" % exc) from exc
            creds = resp["Credentials"]
            self.log.info("Role assumed; expires at %s", creds["Expiration"])
            os.environ["AWS_ACCESS_KEY_ID"] = creds["AccessKeyId"]
            os.environ["AWS_SECRET_ACCESS_KEY"] = creds["SecretAccessKey"]
            os.environ["AWS_SESSION_TOKEN"] = creds["SessionToken"]
            os.environ.pop("AWS_PROFILE", None)
            os.environ.pop("AWS_DEFAULT_PROFILE", None)

        elif profile:
            self.log.info("Loading AWS credentials from profile '%s'", profile)
            try:
                session = boto3.Session(profile_name=profile)
                raw = session.get_credentials()
                if raw is None:
                    raise RuntimeError(
                        "Profile '%s' returned no credentials" % profile
                    )
                resolved = raw.get_frozen_credentials()
            except Exception as exc:
                raise RuntimeError(
                    "Failed to load credentials from profile '%s': %s"
                    % (profile, exc)
                ) from exc
            os.environ["AWS_ACCESS_KEY_ID"] = resolved.access_key
            os.environ["AWS_SECRET_ACCESS_KEY"] = resolved.secret_key
            if resolved.token:
                os.environ["AWS_SESSION_TOKEN"] = resolved.token
            else:
                os.environ.pop("AWS_SESSION_TOKEN", None)
            os.environ.pop("AWS_PROFILE", None)
            os.environ.pop("AWS_DEFAULT_PROFILE", None)
            self.log.info(
                "Injected AWS credentials from profile '%s' (key: %s…)",
                profile, resolved.access_key[:8],
            )
        else:
            self.log.info(
                "No AWS credential override set — using boto3 default chain"
            )

    def _inject_gcp_credentials(self):
        """
        Inject GCP credentials into os.environ so cbbackupmgr subprocesses
        can reach Capella-managed GCS buckets.

        Resolution order (first match wins):
          1. GOOGLE_APPLICATION_CREDENTIALS already set → used as-is
          2. TAF_GCP_CREDENTIALS_FILE set               → pointed at SA JSON
          3. None of the above                          → gcloud ADC
        """
        if os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
            self.log.info(
                "GOOGLE_APPLICATION_CREDENTIALS already set: %s",
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"],
            )
            return

        creds_file = os.environ.get("TAF_GCP_CREDENTIALS_FILE", "")
        if creds_file:
            if not os.path.isfile(creds_file):
                raise RuntimeError(
                    "TAF_GCP_CREDENTIALS_FILE '%s' does not exist" % creds_file
                )
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = creds_file
            self.log.info(
                "Injected GCP credentials from TAF_GCP_CREDENTIALS_FILE: %s",
                creds_file,
            )
        else:
            self.log.info(
                "No GCP credential file configured — relying on "
                "Application Default Credentials (gcloud auth application-default login)"
            )

    def _inject_azure_credentials(self):
        """
        Azure uses storage-account-key auth (no --capella).
        Credentials are resolved in setUp() and stored on self.azure_storage_key.
        Nothing to inject into os.environ here.
        """
        self.log.info(
            "Azure: storage key will be resolved after cluster creation"
        )

    def _fetch_azure_storage_key(self):
        """
        Dynamically fetch the Azure storage account key using AzureCliCredential.

        Requires:
          - az login already performed (personal account or service principal)
          - AZURE_SUBSCRIPTION_ID set to the subscription containing Capella clusters
            (couchbasetest1-rcm: afdee39c-e9a0-4c3e-9165-62cdbc3c7add)

        Resource group follows the pattern: rg-{cluster_id}
        """
        if not HAS_AZURE_SDK:
            self.log.warning(
                "azure-mgmt-storage / azure-identity not installed — "
                "cannot fetch storage key dynamically. "
                "Install with: pip install azure-mgmt-storage azure-identity"
            )
            return None

        subscription_id = os.environ.get("AZURE_SUBSCRIPTION_ID", "")
        if not subscription_id:
            self.log.warning(
                "AZURE_SUBSCRIPTION_ID not set — cannot fetch storage key dynamically"
            )
            return None
        resource_group = "rg-%s" % self.source_cluster.id

        self.log.info(
            "Fetching Azure storage account key for account=%s "
            "resource_group=%s subscription=%s",
            self.azure_storage_account, resource_group, subscription_id,
        )

        def _list_keys(tenant_id=None):
            credential = AzureCliCredential(
                **{"tenant_id": tenant_id} if tenant_id else {}
            )
            client = StorageManagementClient(credential, subscription_id)
            result = client.storage_accounts.list_keys(
                resource_group, self.azure_storage_account
            )
            # SDK may return a model object or dict; normalise via as_dict()
            return result.as_dict()["keys"][0]["value"]

        try:
            # First attempt: use CLI default tenant (or AZURE_TENANT_ID if set)
            tenant_id = os.environ.get("AZURE_TENANT_ID", None)
            key = _list_keys(tenant_id)
            self.log.info("Successfully fetched Azure storage account key")
            return key
        except Exception as exc:
            # If wrong tenant, parse required tenant from error and retry once
            error_msg = str(exc)
            match = re.search(r"sts\.windows\.net/([0-9a-f-]{36})/'[^']*associated", error_msg)
            if match:
                required_tenant = match.group(1)
                self.log.info("Retrying with required tenant: %s", required_tenant)
                try:
                    key = _list_keys(required_tenant)
                    self.log.info("Successfully fetched Azure storage account key")
                    return key
                except Exception as exc2:
                    self.log.warning(
                        "Failed to fetch Azure storage account key (retry): %s", exc2
                    )
                    return None
            self.log.warning(
                "Failed to fetch Azure storage account key: %s. "
                "Run 'az login' and ensure AZURE_SUBSCRIPTION_ID is set.",
                exc,
            )
            return None

    # Region detection (provider-specific)
    # ------------------------------------------------------------------

    def _detect_region(self):
        """Dispatch to the provider-specific region detector."""
        if self.cloud_provider == "aws":
            return self._detect_s3_region(self.bucket_name)
        if self.cloud_provider == "gcp":
            return self._detect_gcs_region(self.bucket_name)
        # Azure does not use --obj-region in the same way.
        return None

    def _detect_s3_region(self, bucket_name):
        """
        Detect the AWS region of *bucket_name* without IAM permissions.

        S3 returns the 'x-amz-bucket-region' header on a 301 redirect even
        for unauthenticated requests.
        """
        url = "https://%s.s3.amazonaws.com/" % bucket_name

        class _NoRedirect(urllib.request.HTTPErrorProcessor):
            def http_response(self, request, response):
                return response
            https_response = http_response

        try:
            opener = urllib.request.build_opener(_NoRedirect)
            resp = opener.open(url, timeout=10)
            region = resp.headers.get("x-amz-bucket-region")
            if region:
                self.log.info(
                    "Detected S3 bucket '%s' region: %s", bucket_name, region
                )
                return region
            self.log.warning(
                "x-amz-bucket-region header absent for bucket '%s' (HTTP %s)",
                bucket_name, resp.status,
            )
        except Exception as exc:
            self.log.warning(
                "Could not detect S3 region for bucket '%s': %s",
                bucket_name, exc,
            )
        return None

    def _detect_gcs_region(self, bucket_name):
        """
        Detect the GCS region of *bucket_name* via the public JSON API.

        Uses an unauthenticated request to the GCS bucket metadata endpoint.
        Returns None if the bucket is private (expected for Capella buckets) —
        caller falls back to the configured obj_region.
        """
        url = "https://storage.googleapis.com/storage/v1/b/%s?fields=location" % bucket_name
        try:
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req, timeout=10) as resp:
                import json
                data = json.loads(resp.read().decode())
                location = data.get("location", "")
                if location:
                    region = location.lower()
                    self.log.info(
                        "Detected GCS bucket '%s' location: %s",
                        bucket_name, region,
                    )
                    return region
        except Exception as exc:
            self.log.warning(
                "Could not detect GCS region for bucket '%s': %s — "
                "bucket is likely private (Capella-managed); using configured region",
                bucket_name, exc,
            )
        return None

    # ------------------------------------------------------------------
    # cbbackupmgr helpers
    # ------------------------------------------------------------------

    def _cluster_url(self, cluster):
        """Return couchbases:// SRV connection string for a Capella cluster."""
        return "couchbases://%s" % cluster.srv

    def _common_obj_flags(self, staging_dir):
        """
        Object-store flags shared by every cbbackupmgr subcommand.

        --obj-region is included for AWS and GCP.  Azure does not use it.
        Azure uses --obj-access-key-id (account name) and --obj-secret-access-key (account key).
        --no-ssl-verify is intentionally excluded — the 'config' subcommand
        rejects it (exit 64).  Use _cluster_ssl_flags() for backup/restore.
        """
        flags = ["--obj-staging-dir", staging_dir]
        if self.cloud_provider in ("aws", "gcp") and self.obj_region:
            flags += ["--obj-region", self.obj_region]
        if self.cloud_provider == "azure":
            flags += [
                "--obj-access-key-id", self.azure_storage_account,
                "--obj-secret-access-key", self.azure_storage_key,
            ]
        return flags

    def _cluster_ssl_flags(self):
        """Return SSL flags for cluster-connecting subcommands (backup / restore)."""
        return ["--no-ssl-verify"] if self.no_ssl_verify else []

    def _run_cbbackupmgr(self, args, label="cbbackupmgr"):
        """
        Execute cbbackupmgr as a subprocess and return (stdout, stderr, rc).
        """
        cmd = [self.cbbackupmgr_path] + args
        self.log.info("[%s] Running: %s", label, " ".join(cmd))
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        if result.stdout:
            self.log.info("[%s] STDOUT:\n%s", label, result.stdout.strip())
        if result.stderr:
            self.log.info("[%s] STDERR:\n%s", label, result.stderr.strip())
        if result.returncode != 0:
            self.log.error(
                "[%s] Command FAILED (rc=%d): %s",
                label, result.returncode, " ".join(cmd),
            )
        return result.stdout, result.stderr, result.returncode

    def _run_cbbackupmgr_process(self, args, label="cbbackupmgr"):
        """
        Launch cbbackupmgr as a non-blocking subprocess and return the Popen object.
        """
        cmd = [self.cbbackupmgr_path] + args
        self.log.info("[%s] Starting (non-blocking): %s", label, " ".join(cmd))
        return subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

    def _assert_success(self, stdout, stderr, rc, success_phrase, label):
        """Assert rc == 0 and *success_phrase* appears in combined output."""
        combined = stdout + stderr
        self.assertEqual(
            rc, 0,
            "[%s] Exited with rc=%d.\nSTDOUT: %s\nSTDERR: %s"
            % (label, rc, stdout, stderr),
        )
        self.assertIn(
            success_phrase,
            combined,
            "[%s] Expected '%s' in output.\nSTDOUT: %s\nSTDERR: %s"
            % (label, success_phrase, stdout, stderr),
        )

    def _configure_repo(self):
        """Configure the shared backup repository."""
        self.log.info("=== Configuring backup repository ===")
        args = [
            "config",
            "--archive", self.archive,
            "--repo", self.repo_name,
        ]
        if self.cloud_provider != "azure":
            args.append("--capella")
        args += self._common_obj_flags(self.staging_dir_backup)
        stdout, stderr, rc = self._run_cbbackupmgr(args, label="config")
        self.assertEqual(
            rc, 0,
            "Repository config failed (rc=%d).\nSTDOUT: %s\nSTDERR: %s"
            % (rc, stdout, stderr),
        )
        self.log.info("Repository configured: %s", (stdout + stderr).strip())

    def _build_backup_args(self, cluster, staging_dir, full_backup=True):
        """Build the cbbackupmgr backup argument list."""
        args = [
            "backup",
            "--archive", self.archive,
            "--repo", self.repo_name,
            "-c", self._cluster_url(cluster),
            "-u", self.rest_username,
            "-p", self.rest_password,
            "--threads", str(self.backup_threads),
        ] + self._common_obj_flags(staging_dir) + self._cluster_ssl_flags()
        if full_backup:
            args.append("--full-backup")
        return args

    def _take_backup(self, cluster, staging_dir, full_backup=True, label="backup"):
        """Execute cbbackupmgr backup. Returns (stdout, stderr, returncode)."""
        self.log.info("[%s] Taking backup from %s", label, cluster.srv)
        args = self._build_backup_args(cluster, staging_dir, full_backup=full_backup)
        return self._run_cbbackupmgr(args, label=label)

    def _build_restore_args(self, cluster, start_ts, end_ts, staging_dir,
                            resume=False):
        """Build the cbbackupmgr restore argument list."""
        args = [
            "restore",
            "--archive", self.archive,
            "--repo", self.repo_name,
            "-c", self._cluster_url(cluster),
            "-u", self.rest_username,
            "-p", self.rest_password,
            "--start", start_ts,
            "--end", end_ts,
            "--obj-read-only",
            "--auto-resolve-conflicts",
            # --force-updates bypasses the "doc already exists" check.
            # Without it, cbbackupmgr fetches every existing doc's metadata
            # before deciding whether to skip it — this is O(n) KV GETs over
            # the cluster that slow restores to a crawl when the destination
            # already has data (e.g. from a previous restore in the same test).
            "--force-updates",
        ] + self._common_obj_flags(staging_dir) + self._cluster_ssl_flags()
        if resume:
            args.append("--resume")
        return args

    def _restore(self, cluster, start_ts, end_ts, staging_dir, label="restore",
                 resume=False):
        """Execute cbbackupmgr restore. Returns (stdout, stderr, returncode)."""
        self.log.info(
            "[%s] Restoring to %s  start=%s  end=%s  resume=%s",
            label, cluster.srv, start_ts, end_ts, resume,
        )
        args = self._build_restore_args(
            cluster, start_ts, end_ts, staging_dir, resume=resume
        )
        return self._run_cbbackupmgr(args, label=label)

    def _start_restore_process(self, cluster, start_ts, end_ts, staging_dir,
                               label="restore"):
        """Launch a non-blocking restore process. Returns Popen object."""
        self.log.info(
            "[%s] Starting non-blocking restore to %s  start=%s  end=%s",
            label, cluster.srv, start_ts, end_ts,
        )
        args = self._build_restore_args(
            cluster, start_ts, end_ts, staging_dir, resume=False
        )
        return self._run_cbbackupmgr_process(args, label=label)

    def _list_backups(self, staging_dir):
        """Run cbbackupmgr info and return the raw output string."""
        self.log.info("=== Listing available backups ===")
        args = [
            "info",
            "--archive", self.archive,
            "--repo", self.repo_name,
        ] + self._common_obj_flags(staging_dir)
        stdout, stderr, rc = self._run_cbbackupmgr(args, label="info")
        self.assertEqual(
            rc, 0,
            "Listing backups failed (rc=%d).\nSTDOUT: %s\nSTDERR: %s"
            % (rc, stdout, stderr),
        )
        return stdout + stderr

    def _parse_latest_backup_timestamp(self, info_output):
        """Extract the most-recent backup timestamp from cbbackupmgr info output."""
        timestamps = re.findall(
            r"(\d{4}-\d{2}-\d{2}T\d{2}_\d{2}_\d{2}[^\s]+)", info_output
        )
        self.assertTrue(
            timestamps,
            "No backup timestamp found in info output:\n%s" % info_output,
        )
        latest = timestamps[-1]
        self.log.info("Latest backup timestamp: %s", latest)
        return latest

    def _run_concurrent(self, *funcs):
        """Run each callable in its own thread; return list of result dicts."""
        results = [{} for _ in funcs]
        threads = []
        for idx, func in enumerate(funcs):
            t = threading.Thread(
                target=func, args=(results[idx],), name="op-%d" % idx
            )
            threads.append(t)
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        return results

    # ------------------------------------------------------------------
    # Capella cluster helpers
    # ------------------------------------------------------------------

    def _add_allowed_ip(self, cluster):
        """Add the test-runner machine's public IP to the cluster's allowlist."""
        self.log.info("Adding allowed IP for cluster %s", cluster.id)
        try:
            CapellaUtils.allow_my_ip(self.pod, self.tenant, cluster.id, allowall=True)
            self.log.info("Allowed IP added for cluster %s", cluster.id)
        except Exception as exc:
            self.log.warning(
                "allow_my_ip for cluster %s raised (may already exist): %s",
                cluster.id, exc,
            )

    def _load_sample_bucket(self, cluster):
        """Load self.sample_bucket into *cluster* and wait for it to be ready."""
        self.log.info(
            "Loading sample bucket '%s' into cluster %s",
            self.sample_bucket, cluster.id,
        )
        CapellaUtils.load_sample_bucket(
            self.pod, self.tenant, cluster.id, self.sample_bucket
        )
        self.sleep(30, "Waiting for sample bucket load to complete")

    def _create_destination_bucket(self):
        """Create self.sample_bucket on the destination cluster."""
        self.log.info(
            "Creating bucket '%s' on destination cluster %s",
            self.sample_bucket, self.dest_cluster.id,
        )
        bucket_params = {
            "name": self.sample_bucket,
            "memoryAllocationInMb": 200,
            "bucketConflictResolution": "seqno",
            "durabilityLevel": "none",
            "replicas": 1,
            "flush": False,
            "timeToLiveInSeconds": 0,
            "type": "couchbase",
            "storageBackend": "couchstore",
        }
        try:
            CapellaUtils.create_bucket(
                self.pod, self.tenant, self.dest_cluster, bucket_params
            )
            self.sleep(10, "Waiting for bucket creation on destination")
        except Exception as exc:
            self.log.warning(
                "Bucket creation on destination raised (may already exist): %s", exc
            )

    # ------------------------------------------------------------------
    # Rebalance / scale helpers
    # ------------------------------------------------------------------

    def _get_current_specs(self, cluster=None):
        """Fetch live cluster specs from Capella."""
        cluster = cluster or self.source_cluster
        info = CapellaUtils.get_cluster_info(self.pod, self.tenant, cluster.id)
        raw_specs = (
            info.get("data", {}).get("services")
            or info.get("data", {}).get("config", {}).get("specs")
            or info.get("data", {}).get("specs")
            or info.get("specs")
        )
        if not raw_specs:
            self.fail(
                "Could not extract specs from cluster info response: %s" % info
            )
        self.log.info(
            "Fetched %d spec group(s) for cluster %s", len(raw_specs), cluster.id
        )
        return raw_specs

    def _trigger_scale_out(self, specs, cluster=None, delta=1):
        """Submit a non-blocking scale-out (+delta nodes). Returns (modified_specs, original_count)."""
        cluster = cluster or self.source_cluster
        modified = copy.deepcopy(specs)
        original_count = modified[0]["count"]
        modified[0]["count"] = original_count + delta
        self.log.info(
            "Scaling cluster %s: %d → %d nodes",
            cluster.id, original_count, modified[0]["count"],
        )
        CapellaUtils.scale(self.pod, self.tenant, cluster, {"specs": modified})
        self.log.info("Scale-out request accepted — rebalance is now in progress.")
        return modified, original_count

    def _wait_for_cluster_healthy(self, cluster=None, timeout=1800):
        """Block until *cluster* state is 'healthy'."""
        cluster = cluster or self.source_cluster
        self.log.info(
            "Waiting for cluster %s to reach healthy state (timeout=%ds)…",
            cluster.id, timeout,
        )
        CapellaUtils.wait_until_done(
            self.pod, self.tenant, cluster.id,
            msg="cluster operation",
            timeout=timeout,
        )
        self.log.info("Cluster %s is healthy.", cluster.id)

    # ------------------------------------------------------------------
    # Couchbase REST API helpers (direct HTTPS to port 18091)
    # ------------------------------------------------------------------

    def _cluster_mgmt_url(self, cluster):
        """Return the HTTPS management base URL for a Capella cluster."""
        return "https://%s:18091" % cluster.srv

    def _rest_get(self, cluster, path):
        """GET request to the cluster management REST API."""
        url = self._cluster_mgmt_url(cluster) + path
        resp = requests.get(
            url,
            auth=(self.rest_username, self.rest_password),
            verify=False,
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()

    def _rest_post(self, cluster, path, data):
        """POST request to the cluster management REST API."""
        url = self._cluster_mgmt_url(cluster) + path
        resp = requests.post(
            url,
            auth=(self.rest_username, self.rest_password),
            data=data,
            verify=False,
            timeout=60,
        )
        return resp

    def _get_otp_nodes(self, cluster):
        """Return all otpNode name strings from /pools/nodes for *cluster*."""
        data = self._rest_get(cluster, "/pools/nodes")
        otp_nodes = [n["otpNode"] for n in data.get("nodes", [])]
        self.log.info("Cluster %s otp nodes: %s", cluster.id, otp_nodes)
        return otp_nodes

    def _pick_non_orchestrator_node(self, cluster):
        """
        Return (all_otp_nodes, node_to_failover) where node_to_failover is
        not the current cluster orchestrator.

        Queries /pools/default for the masterNode field to identify the
        real orchestrator, then picks the last non-orchestrator node.
        """
        pools = self._rest_get(cluster, "/pools/default")
        master = pools.get("masterNode", "")
        all_nodes = self._get_otp_nodes(cluster)
        self.assertTrue(
            len(all_nodes) >= 2,
            "Need at least 2 nodes to failover one; found: %s" % all_nodes,
        )
        non_orchestrators = [n for n in all_nodes if n != master]
        self.assertTrue(
            non_orchestrators,
            "No non-orchestrator nodes found (master=%s, nodes=%s)"
            % (master, all_nodes),
        )
        chosen = non_orchestrators[-1]
        self.log.info(
            "Orchestrator: %s — selected for failover: %s", master, chosen
        )
        return all_nodes, chosen

    def _hard_failover_node(self, cluster, otp_node):
        """Hard-failover *otp_node* on *cluster* via POST /controller/failOver."""
        self.log.info(
            "Hard-failing over node '%s' on cluster %s", otp_node, cluster.id
        )
        resp = self._rest_post(
            cluster,
            "/controller/failOver",
            {"otpNode": otp_node},
        )
        if resp.status_code == 200:
            self.log.info("Failover accepted for node '%s' (rc=200)", otp_node)
        else:
            self.log.warning(
                "Failover returned rc=%d for node '%s': %s",
                resp.status_code, otp_node, resp.text,
            )
        return resp.status_code

    def _rebalance_cluster(self, cluster, known_nodes, eject_nodes=None):
        """Trigger a rebalance to eject failed-over nodes."""
        eject_nodes = eject_nodes or []
        data = {
            "knownNodes": ",".join(known_nodes),
            "ejectedNodes": ",".join(eject_nodes),
        }
        self.log.info(
            "Triggering rebalance on cluster %s — ejecting: %s",
            cluster.id, eject_nodes,
        )
        resp = self._rest_post(cluster, "/controller/rebalance", data)
        if resp.status_code == 200:
            self.log.info("Rebalance started on cluster %s", cluster.id)
        else:
            self.log.warning(
                "Rebalance request returned rc=%d: %s",
                resp.status_code, resp.text,
            )
        return resp.status_code

    def _wait_for_rebalance_started(self, cluster=None, timeout=60):
        """
        Poll /pools/default/tasks until a rebalance task reports status=running,
        then return.  Falls through after *timeout* seconds with a warning so
        concurrent ops still proceed even if the poll window was too short.
        """
        cluster = cluster or self.source_cluster
        deadline = time.time() + timeout
        self.log.info(
            "Waiting for rebalance to start on cluster %s (timeout=%ds)…",
            cluster.id, timeout,
        )
        while time.time() < deadline:
            try:
                tasks = self._rest_get(cluster, "/pools/default/tasks")
                for task in tasks:
                    if (task.get("type") == "rebalance"
                            and task.get("status") == "running"):
                        self.log.info(
                            "Rebalance is running on cluster %s (progress=%.1f%%)",
                            cluster.id, task.get("progress", 0),
                        )
                        return
            except Exception as exc:
                self.log.warning("Error polling rebalance status: %s", exc)
            time.sleep(3)
        self.log.warning(
            "Rebalance did not start within %ds — proceeding with concurrent ops",
            timeout,
        )

    def _scale_back_in(self, modified_specs, original_count, cluster=None):
        """Restore cluster node count to original_count and wait for healthy."""
        cluster = cluster or self.source_cluster
        restore_specs = copy.deepcopy(modified_specs)
        restore_specs[0]["count"] = original_count
        self.log.info(
            "Scaling cluster %s back to %d node(s).", cluster.id, original_count
        )
        CapellaUtils.scale(self.pod, self.tenant, cluster, {"specs": restore_specs})
        self._wait_for_cluster_healthy(cluster=cluster)
        self.log.info("Scale-in complete — cluster restored to original size.")
