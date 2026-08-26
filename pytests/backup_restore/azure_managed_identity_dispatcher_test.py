"""
Azure Managed Identity backup/restore regression test (MB-72909 / CBSE-23562)
-- dispatcher-provisioned variant.

Bug (MB-72909): tools-common's Azure SDK wrapper always set
ManagedIdentityCredentialOptions.ID = ClientID(os.Getenv("AZURE_CLIENT_ID")).
When AZURE_CLIENT_ID was unset, this produced a non-nil ClientID(""). azidentity
treats any non-nil ID -- including an empty one -- as "use a user-assigned
identity", so it sent client_id="" to IMDS, which cannot resolve any identity.
Customers who only configured a system-assigned managed identity (no client ID
at all) could not back up to Azure Blob Storage. Hit in production by Tractor
Supply Company (CBSE-23562) on 8.0.0-3777. Fixed in tools-common commit
eb1f5e9c -- but only in cbbackupmgr builds where the bump landed in the
"backup" repo itself; some builds only bumped the separate "cbbs" (Capella
Backup Service) repo and still have the bug. Don't assume a Jira fixVersions
entry means the CLI binary itself is fixed; check which repo the bump commit
landed in.

Architecture: a deliberately different approach from azure_managed_identity_test.py
(the original, fully self-contained, VM-owning design -- see that file's
docstring). Kept as a separate file/commit since both are useful: this
version assumes the node in node.ini was already provisioned and installed
by the real os_certify/dispatcher pipeline by the time OnPremBaseTest.setUp()
runs, so it never creates, installs, or destroys any VM itself -- only
tearDown() removes the role assignment and storage account this test itself
created.

Since the dispatcher never passes --computer-name when provisioning, Azure
defaults the VM's OS hostname to its ARM resource name -- _discover_vm_name()
recovers it that way over SSH instead of needing a new env var or dispatcher
change. No identity is requested by the dispatcher either, so this test
attaches a system-assigned identity itself via
ComputeManagementClient.virtual_machines.begin_update(), creates a fresh
storage account in the same resource group as the VM, grants it "Storage
Blob Data Contributor", waits for the grant to propagate, and then runs the
backup/restore body.

CLI flag gotcha (filed as MB-73516): for true system-assigned-identity mode,
do NOT pass --obj-access-key-id at all, even without a secret -- it gets
treated as a user-assigned-identity client-id override, and a storage account
name there makes IMDS reject it with an unrelated-looking "identity not
found" error. Pass --obj-endpoint https://<account>.blob.core.windows.net
instead (handled by AzureProvider(use_managed_identity=True) already).

AZURE_ENDPOINT gotcha: this Jenkins job has a standing family of AZURE_*
credentials globally injected for the existing backup_restore Azure test
suite, including AZURE_ENDPOINT pointed at its own shared account.
AzureProvider prefers AZURE_ENDPOINT over a value computed from
AZURE_STORAGE_ACCOUNT, so both must be overridden in _build_backup_mgr() --
overriding AZURE_STORAGE_ACCOUNT alone silently leaves cbbackupmgr pointed at
the unrelated shared account, producing a permission error that looks
identical to an RBAC-propagation race but never resolves no matter how long
you wait.

Requirements to run this test:
  - Run via the os_certify/dispatcher pipeline so node.ini already has a
    reachable, installed Azure node.
  - AZURE_TENANT_ID / AZURE_CLIENT_ID / AZURE_CLIENT_SECRET / AZURE_SUBSCRIPTION_ID
    env vars for a service principal -- same names azure_provider.py already
    reads for its KMS/Key Vault path. Falls back to AzureCliCredential
    (az login) if unset, for local/manual runs -- with rights, scoped to
    PERSISTENT_RESOURCE_GROUP, to: update VM identity (Virtual Machine
    Contributor), create/delete a storage account (Storage Account
    Contributor, or Contributor), and create role assignments there (a role
    that can write role assignments, e.g. User Access Administrator).
"""

import os
import time
import uuid

from azure.core.exceptions import (
    ClientAuthenticationError, HttpResponseError, ResourceNotFoundError,
)
from azure.identity import AzureCliCredential, ClientSecretCredential
from azure.mgmt.authorization import AuthorizationManagementClient
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.compute.models import (
    ResourceIdentityType, VirtualMachineIdentity, VirtualMachineUpdate,
)
from azure.mgmt.storage import StorageManagementClient
from azure.mgmt.storage.models import Kind, Sku, SkuName, StorageAccountCreateParameters

from bucket_utils.bucket_ready_functions import BucketUtils
from cb_server_rest_util.rest_client import RestConnection
# sys.path carries both "." and "couchbase_utils", so these modules are
# importable with or without the package prefix -- but each spelling builds a
# separate module object for the same file. backup_utils.py imports
# AzureProvider prefixed, so match it here or the provider handed to
# BackupMgrUtil is a different class than the one it was written against.
from couchbase_utils.backup_utils.backup_utils import BackupMgrUtil
from couchbase_utils.cloud_provider_utils.azure_provider import AzureProvider
from onPrem_basetestcase import OnPremBaseTest
from platform_constants.os_constants import Linux
from shell_util.remote_connection import RemoteMachineShellConnection


PERSISTENT_RESOURCE_GROUP = "qe-os-certify"  # resource group the dispatcher provisions VMs into
CONTAINER_NAME = "backups"

# Built-in Azure role "Storage Blob Data Contributor" -- same GUID for every
# subscription/tenant.
STORAGE_BLOB_DATA_CONTRIBUTOR_ROLE_ID = "ba92f5b4-2d11-453d-a403-e96b0029c9fe"

_IMDS_TOKEN_URL = (
    "http://169.254.169.254/metadata/identity/oauth2/token"
    "?api-version=2018-02-01&resource=https://storage.azure.com/"
)

# cbbackupmgr's error text when a storage write hits an RBAC grant that
# hasn't fully propagated yet -- transient, not a real permission problem.
_RBAC_PROPAGATION_SIGNATURE = "does not have the permission to access this resource"


class AzureManagedIdentityDispatcherTest(OnPremBaseTest):
    """
    MB-72909 / CBSE-23562, dispatcher-provisioned variant -- see module
    docstring for the assumptions this depends on.
    """

    def setUp(self):
        super(AzureManagedIdentityDispatcherTest, self).setUp()

        self.resource_group = self.input.param("resource_group", PERSISTENT_RESOURCE_GROUP)
        self.container = self.input.param("container", CONTAINER_NAME)
        self.rest_username = self.cluster.master.rest_username
        self.rest_password = self.cluster.master.rest_password
        self.bucket_name = self.input.param("bucket_name", "default")
        self.num_items = self.input.param("num_items", 2000)

        self.vm_name = None
        self.vm_location = None
        self.vm_principal_id = None
        self.storage_account = None
        self._storage_account_created = False
        self.role_assignment_name = str(uuid.uuid4())
        self._role_assignment_created = False
        self._cleaned_up = False

        self.shell = None
        self.backup_mgr = None
        self.archive = None
        self.repo = None

        try:
            # OnPremBaseTest.setUp() provisions the cluster but never
            # auto-creates a bucket -- that's left to subclasses. create_bucket()
            # only exists on ClusterSetup (a different OnPremBaseTest subclass),
            # not on OnPremBaseTest itself, so call BucketUtils directly instead
            # of inheriting ClusterSetup just for this one helper.
            self.bucket_util.create_default_bucket(
                cluster=self.cluster, bucket_name=self.bucket_name,
                flush_enabled=self.flush_enabled)
            self._require_azure_credentials()
            self.shell = RemoteMachineShellConnection(self.cluster.master)
            self._discover_vm_name()
            self._attach_managed_identity()
            self._create_storage_account()
            self._grant_storage_role()
            self._wait_for_role_propagation()
            self._require_no_client_id_on_vm()
            self._build_backup_mgr()
            self._load_docs()
        except Exception:
            self._cleanup()
            raise

    def tearDown(self):
        self._cleanup()
        super(AzureManagedIdentityDispatcherTest, self).tearDown()

    def _system_install_cli(self, cli_tool):
        """
        Point a cb_tools wrapper at the system Couchbase install and let it
        sudo. CbCmdBase falls back to Linux.NONROOT_CB_BIN_PATH (~/cb/opt/...)
        whenever ssh_username != "root" (cb_tools_base.py) -- a branch meant
        for non-root *installs*. Dispatcher-provisioned nodes are an ordinary
        root-owned package install reached over a sudo-capable non-root user,
        which that branch can't express, so correct the binary path and turn
        sudo on for the connection it holds.
        """
        cli_tool.cbstatCmd = "%s%s" % (Linux.COUCHBASE_BIN_PATH, cli_tool.binaryName)
        cli_tool.shellConn.use_sudo = True
        return cli_tool

    # ------------------------------------------------------------------
    # Preconditions -- fail with a clear message, never skipTest
    # ------------------------------------------------------------------

    def _require_azure_credentials(self):
        missing = [
            name for name in (
                "AZURE_TENANT_ID", "AZURE_CLIENT_ID", "AZURE_CLIENT_SECRET",
                "AZURE_SUBSCRIPTION_ID",
            )
            if not os.environ.get(name)
        ]
        # Falls back to the operator's own `az login` session for local/manual
        # runs where no service principal is available. AZURE_SUBSCRIPTION_ID
        # is still required either way since AzureCliCredential doesn't
        # expose it.
        if missing and missing != ["AZURE_TENANT_ID", "AZURE_CLIENT_ID", "AZURE_CLIENT_SECRET"]:
            self.fail(
                "Missing Azure credentials: %s. Either set the full "
                "service-principal env var set (AZURE_TENANT_ID/CLIENT_ID/"
                "CLIENT_SECRET/SUBSCRIPTION_ID), or run `az login` and set "
                "just AZURE_SUBSCRIPTION_ID to use the az-login fallback."
                % ", ".join(missing)
            )
        if missing:
            self.log.warning(
                "No service-principal env vars found -- falling back to "
                "AzureCliCredential (az login). This is a local-only "
                "workaround; the real dispatcher pipeline must use a real "
                "service principal, not an interactive login."
            )
            credential = AzureCliCredential()
        else:
            credential = ClientSecretCredential(
                tenant_id=os.environ["AZURE_TENANT_ID"],
                client_id=os.environ["AZURE_CLIENT_ID"],
                client_secret=os.environ["AZURE_CLIENT_SECRET"],
            )
        self.azure_subscription_id = os.environ["AZURE_SUBSCRIPTION_ID"]
        self.compute_client = ComputeManagementClient(credential, self.azure_subscription_id)
        self.auth_client = AuthorizationManagementClient(credential, self.azure_subscription_id)
        self.storage_client = StorageManagementClient(credential, self.azure_subscription_id)
        try:
            list(self.compute_client.virtual_machines.list(self.resource_group))
        except (ClientAuthenticationError, HttpResponseError) as exc:
            self.fail(
                "Azure service-principal credentials were rejected while "
                "listing VMs in %s: %s" % (self.resource_group, exc)
            )

    def _discover_vm_name(self):
        # get_hostname() returns a list of output lines (this codebase's
        # usual execute_command_raw convention), not a plain string.
        hostname_lines = self.shell.get_hostname()
        hostname = "".join(hostname_lines or []).strip()
        if not hostname:
            self.fail(
                "Could not read this node's hostname over SSH -- needed to "
                "recover its Azure VM resource name (see module docstring)."
            )
        self.vm_name = hostname.split(".")[0]
        try:
            vm = self.compute_client.virtual_machines.get(self.resource_group, self.vm_name)
            self.vm_location = vm.location
        except ResourceNotFoundError:
            self.fail(
                "Hostname '%s' does not match any VM in resource group %s. "
                "The hostname-recovers-the-ARM-name assumption in this "
                "test's module docstring may not hold for however this "
                "node was actually provisioned."
                % (self.vm_name, self.resource_group)
            )

    def _attach_managed_identity(self):
        self.log.info("Attaching system-assigned identity to %s", self.vm_name)
        try:
            vm = self.compute_client.virtual_machines.begin_update(
                self.resource_group, self.vm_name,
                VirtualMachineUpdate(
                    identity=VirtualMachineIdentity(type=ResourceIdentityType.SYSTEM_ASSIGNED),
                ),
            ).result()
        except HttpResponseError as exc:
            self.fail(
                "Failed to attach a system-assigned identity to %s: %s"
                % (self.vm_name, exc)
            )
        self.vm_principal_id = vm.identity.principal_id if vm.identity else None
        if not self.vm_principal_id:
            self.fail("Updated %s but it has no principalId" % self.vm_name)
        self.log.info("%s principalId=%s", self.vm_name, self.vm_principal_id)

    def _create_storage_account(self):
        """Created fresh in the VM's own resource group every run, rather
        than a fixed account, since a fixed one may not be reachable from
        whatever Azure credentials are active at runtime."""
        self.storage_account = "taf%s" % uuid.uuid4().hex[:20]
        self.log.info(
            "Creating storage account %s in %s (%s)",
            self.storage_account, self.resource_group, self.vm_location)
        try:
            self.storage_client.storage_accounts.begin_create(
                self.resource_group, self.storage_account,
                StorageAccountCreateParameters(
                    sku=Sku(name=SkuName.STANDARD_LRS),
                    kind=Kind.STORAGE_V2,
                    location=self.vm_location,
                ),
            ).result()
            self.storage_client.blob_containers.create(
                self.resource_group, self.storage_account, self.container, {})
        except HttpResponseError as exc:
            self.fail(
                "Failed to create storage account %s (container %s) in %s: "
                "%s" % (self.storage_account, self.container,
                        self.resource_group, exc))
        self._storage_account_created = True

    def _grant_storage_role(self):
        self.log.info(
            "Granting Storage Blob Data Contributor on %s to principalId %s",
            self.storage_account, self.vm_principal_id,
        )
        scope = (
            "/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Storage"
            "/storageAccounts/%s"
            % (self.azure_subscription_id, self.resource_group, self.storage_account)
        )
        role_definition_id = (
            "/subscriptions/%s/providers/Microsoft.Authorization/roleDefinitions/%s"
            % (self.azure_subscription_id, STORAGE_BLOB_DATA_CONTRIBUTOR_ROLE_ID)
        )
        self._role_scope = scope
        try:
            self.auth_client.role_assignments.create(
                scope, self.role_assignment_name,
                {"role_definition_id": role_definition_id, "principal_id": self.vm_principal_id},
            )
        except HttpResponseError as exc:
            self.fail(
                "Failed to grant Storage Blob Data Contributor to %s on %s: "
                "%s" % (self.vm_principal_id, scope, exc)
            )
        self._role_assignment_created = True
        self._role_grant_time = time.time()

    def _wait_for_role_propagation(self, timeout=180):
        """
        Poll from the node itself: fetch an IMDS token and attempt an actual
        write -- PUT then DELETE a small marker blob under the target
        container -- rather than a plain List Containers call. RBAC
        propagation can lag for the write path even after a read-type check
        already succeeds, so a list-only probe isn't representative of what
        cbbackupmgr actually needs.
        """
        marker_blob = "rbac-probe-%s" % uuid.uuid4().hex[:8]
        check_cmd = (
            "TOKEN=$(curl -s -H 'Metadata: true' '%s' | "
            "python3 -c 'import sys,json; print(json.load(sys.stdin)[\"access_token\"])'); "
            "CODE=$(curl -s -o /dev/null -w '%%{http_code}' -X PUT "
            "-H \"Authorization: Bearer $TOKEN\" -H 'x-ms-version: 2020-04-08' "
            "-H 'x-ms-blob-type: BlockBlob' -H 'Content-Length: 0' "
            "'https://%s.blob.core.windows.net/%s/%s'); "
            "curl -s -o /dev/null -X DELETE -H \"Authorization: Bearer $TOKEN\" "
            "-H 'x-ms-version: 2020-04-08' "
            "'https://%s.blob.core.windows.net/%s/%s'; "
            "echo $CODE"
            % (_IMDS_TOKEN_URL, self.storage_account, self.container, marker_blob,
               self.storage_account, self.container, marker_blob)
        )
        deadline = time.time() + timeout
        http_code = None
        while time.time() < deadline:
            output, _ = self.shell.execute_command(check_cmd)
            http_code = "".join(output or []).strip()
            if http_code == "201":
                self.log.info(
                    "RBAC propagated (write-verified) after %.1fs",
                    time.time() - self._role_grant_time
                )
                return
            time.sleep(10)
        self.fail(
            "Storage role for %s never propagated for writes within %ds "
            "(last check: HTTP %s). This is a timing/RBAC issue, not the "
            "MB-72909 bug itself." % (self.vm_principal_id, timeout, http_code)
        )

    def _require_no_client_id_on_vm(self):
        """
        MB-72909 only reproduces when AZURE_CLIENT_ID is unset -- the bug was
        that an unset var still yielded a non-nil, empty ClientID. Assert
        rather than assume, including under sudo (how cbbackupmgr actually
        runs), so a misconfigured node fails loudly instead of this test
        passing for the wrong reason (exercising the user-assigned path).
        """
        for prefix in ("", "sudo "):
            output, _ = self.shell.execute_command(
                "%sprintenv AZURE_CLIENT_ID" % prefix)
            value = "".join(output or []).strip()
            if value:
                self.fail(
                    "AZURE_CLIENT_ID is set to '%s' in this node's '%s' "
                    "environment. MB-72909 lives in the code path taken "
                    "when it is UNSET, so leaving it set would make this "
                    "test exercise user-assigned identity and pass "
                    "vacuously." % (value, (prefix + "shell").strip())
                )

    # ------------------------------------------------------------------
    # Data + backup setup
    # ------------------------------------------------------------------

    def _load_docs(self):
        """
        Load docs from the node itself, against 127.0.0.1, so no KV port
        has to be exposed beyond whatever the dispatcher's NSG already
        allows for normal TAF test traffic.
        """
        cmd = (
            "%scbc-pillowfight -U couchbase://127.0.0.1/%s -u %s -P %s "
            "-I %d -t 4 -m 256 -M 256 --populate-only --random-body "
            "--key-prefix=mi -Dtimeout=10"
        ) % (Linux.COUCHBASE_BIN_PATH, self.bucket_name,
             self.rest_username, self.rest_password, self.num_items)
        output, error = self.shell.execute_command(cmd, timeout=600)
        self.log.debug("pillowfight output=%s error=%s", output, error)
        self._wait_for_item_count(self.num_items, "initial load")

    def _wait_for_item_count(self, expected, label, timeout=600):
        """
        Poll directly rather than via BackupMgrUtil.monitor_restore() --
        that helper's log lines are hardcoded to say "restore", which would
        be misleading when this is called right after the initial doc load.
        """
        deadline = time.time() + timeout
        curr_items = 0
        while time.time() < deadline:
            curr_items = BucketUtils.get_buckets_item_count(
                self.cluster, self.bucket_name)
            if curr_items >= expected:
                return
            time.sleep(5)
        self.fail(
            "Bucket %s only reached %d/%d items after %s (%ds)"
            % (self.bucket_name, curr_items, expected, label, timeout))

    def _wait_for_empty_bucket(self, timeout=120):
        deadline = time.time() + timeout
        curr_items = None
        while time.time() < deadline:
            curr_items = BucketUtils.get_buckets_item_count(
                self.cluster, self.bucket_name)
            if curr_items == 0:
                return
            time.sleep(5)
        self.fail("Bucket %s still held %s items %ds after flush"
                  % (self.bucket_name, curr_items, timeout))

    def _build_backup_mgr(self):
        # Both must be set: AzureProvider prefers AZURE_ENDPOINT over a value
        # computed from AZURE_STORAGE_ACCOUNT (see module docstring's
        # AZURE_ENDPOINT gotcha).
        os.environ["AZURE_STORAGE_ACCOUNT"] = self.storage_account
        os.environ["AZURE_ENDPOINT"] = (
            "https://%s.blob.core.windows.net" % self.storage_account)
        provider = AzureProvider(log=self.log, use_managed_identity=True)

        repo = "mb72909-%s" % uuid.uuid4().hex[:8]
        self.repo = repo
        self.archive = "az://%s/%s" % (self.container, repo)
        self.backup_mgr = self._system_install_cli(
            BackupMgrUtil(self.cluster.master, cloud_provider=provider,
                          obj_staging_dir="/tmp/staging-%s" % repo))

    def _create_repo_with_retry(self, timeout=90):
        """
        Even after _wait_for_role_propagation() succeeds, RBAC propagation
        can still lag non-uniformly across the storage front-end fleet for a
        few more seconds and race cbbackupmgr's own lockfile-creation write.
        Retry only this specific permission-denied signature.
        """
        deadline = time.time() + timeout
        while True:
            stdout, stderr = self.backup_mgr.create_repo(self.archive, self.repo)
            combined = "".join(stdout or []) + "".join(stderr or [])
            if _RBAC_PROPAGATION_SIGNATURE not in combined:
                return stdout, stderr
            if time.time() >= deadline:
                return stdout, stderr
            self.log.info(
                "Storage role not fully propagated yet for writes -- "
                "retrying create_repo")
            time.sleep(10)

    def _backup_with_retry(self, cluster_host, timeout=90):
        """
        Immediately after a fresh doc load, cbbackupmgr's first GET to the
        cluster's own /api/v1/backup status endpoint can transiently 500
        with service.backup_not_possible / "Migration active" while
        ns_server's backup service finishes registering the bucket --
        unrelated to MB-72909. Also retries the RBAC-propagation signature
        as defense-in-depth, since backup() writes to the same account via a
        separate code path from create_repo().
        """
        deadline = time.time() + timeout
        while True:
            stdout, stderr = self.backup_mgr.backup(
                self.archive, self.repo, cluster_host=cluster_host,
                full_backup=True)
            combined = "".join(stdout or []) + "".join(stderr or [])
            if "service.backup_not_possible" not in combined and \
                    "Migration active" not in combined and \
                    _RBAC_PROPAGATION_SIGNATURE not in combined:
                return stdout, stderr
            if time.time() >= deadline:
                return stdout, stderr
            self.log.info(
                "Backup service not ready yet (Migration active) or RBAC "
                "still propagating -- retrying")
            time.sleep(10)

    # ------------------------------------------------------------------
    # Cleanup -- the role assignment and storage account this test created;
    # the VM stays the dispatcher's problem entirely.
    # ------------------------------------------------------------------

    def _cleanup(self):
        if self._cleaned_up:
            return
        self._cleaned_up = True

        shells = [self.shell]
        if self.backup_mgr is not None:
            shells.append(self.backup_mgr.shellConn)
        for shell in shells:
            if shell is not None:
                try:
                    shell.disconnect()
                except Exception as exc:
                    self.log.warning("Shell disconnect failed: %s", exc)

        if self._role_assignment_created:
            try:
                self.auth_client.role_assignments.delete(
                    self._role_scope, self.role_assignment_name)
            except (HttpResponseError, ResourceNotFoundError) as exc:
                self.log.warning("Role assignment delete failed: %s", exc)

        if self._storage_account_created:
            try:
                self.storage_client.storage_accounts.delete(
                    self.resource_group, self.storage_account)
            except HttpResponseError as exc:
                self.log.warning(
                    "Storage account delete failed for %s: %s",
                    self.storage_account, exc)

    # ------------------------------------------------------------------
    # The test
    # ------------------------------------------------------------------

    def test_backup_restore_via_system_assigned_managed_identity(self):
        cluster_host = "http://127.0.0.1:8091"

        stdout, stderr = self._create_repo_with_retry()
        combined = ("".join(stdout or []) + "".join(stderr or [])).lower()
        if "userassignedclientid" in combined or "identity not found" in combined:
            self.fail(
                "Repository config failed with the MB-72909 signature -- "
                "system-assigned managed identity was not honoured.\n%s%s"
                % (stdout, stderr))

        stdout, stderr = self._backup_with_retry(cluster_host)
        self.assertIn(
            "Backup completed successfully", "".join(stdout or []),
            "Backup failed:\n%s%s" % (stdout, stderr))
        self.log.info(
            "Full backup succeeded using only the system-assigned managed "
            "identity (no storage key, no AZURE_CLIENT_ID) -- MB-72909 "
            "backup path verified.")

        rest = RestConnection(self.cluster.master)
        status, content = rest.bucket.flush_bucket(self.bucket_name)
        self.assertTrue(status, "Bucket flush failed: %s" % content)
        self._wait_for_empty_bucket()

        stdout, stderr = self.backup_mgr.restore(
            self.archive, self.repo, cluster_host=cluster_host)
        self.assertIn(
            "Restore completed successfully", "".join(stdout or []),
            "Restore failed:\n%s%s" % (stdout, stderr))
        self._wait_for_item_count(self.num_items, "restore")
        self.log.info(
            "Restore succeeded and all %d items are back -- MB-72909 "
            "restore path verified via system-assigned managed identity.",
            self.num_items)
