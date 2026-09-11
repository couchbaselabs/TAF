import base64
import json
import os
import re
import ssl
import tempfile
import time
import traceback
import uuid
from urllib.parse import quote_plus, urlencode

import requests
from cb_constants import CbServer
from cb_server_rest_util.security.security_api import SecurityRestAPI
from couchbase_utils.rbac_utils.Rbac_ready_functions import RbacUtils
from couchbase_utils.security_utils.crl_utils import CRLUtils
from couchbase_utils.security_utils.x509main import x509main
from cryptography import x509
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID
from membase.api.rest_client import RestConnection
from shell_util.remote_connection import RemoteMachineShellConnection

from cbas.cbas_base_server import CBASBaseTest


class CBASCRLBase(CBASBaseTest):
    """
    Fixture for CRL (Certificate Revocation List) tests against the Analytics
    service, per Analytics_CRL_Lifecycle_TestPlan.

    Deliberately a sibling of pytests/backup_restore/crl_base.py rather than a
    subclass of it: that one is rooted in CollectionBase and requires a Backup
    Service node, while these tests need CBASBaseTest's Analytics setup. The
    CA-trust / certificate / clientCertAuth helpers below are the same shape in
    both, and are the natural thing to lift into a shared mixin if a third
    service-specific CRL suite appears -- see the note on each helper that has
    a counterpart.
    """

    ANALYTICS_PORT = CbServer.cbas_port          # 8095
    ANALYTICS_SSL_PORT = CbServer.ssl_cbas_port  # 18095
    MGMT_SSL_PORT = 18091

    def setUp(self):
        super(CBASCRLBase, self).setUp()

        self.crl_utils = CRLUtils(log=self.log)
        self.rest = RestConnection(self.cluster.master)

        # Cleaned up in tearDown
        self._created_files = []
        self._rbac_users = []
        self._trusted_ca_ids = []
        self._temp_pem_files = []
        # Datasets created on a link, dropped before the links they depend on.
        self._created_datasets = []
        # Buckets this suite created, as (server, name), dropped in tearDown.
        self._buckets_created = []
        # Topology changes a section-7 test made. Recorded rather than
        # reversed: CBASBaseTest re-initialises the cluster in the next
        # test's setUp, so undoing them here would duplicate that work and
        # risk fighting it. Logged in tearDown so a later failure that turns
        # out to be topology-related is traceable to the test that moved it.
        self._rebalanced_in = []
        self._rebalanced_out = []
        self._failed_over = []
        # Remote clusters a section-3 test has configured as a link target.
        # Each entry owns its own cleanup lists -- see
        # _setup_remote_link_target -- so nothing here interferes with the
        # single-cluster state above.
        self._remote_targets = []

        self._require_crl_supported()
        self._require_analytics_node()

        # Unique CN per test, NOT a constant one. CRLs are matched to a
        # certificate by issuer NAME, so a CRL or trusted CA surviving a
        # previous test's failed teardown collides with a fresh CA that has the
        # same CN but a different key -- and the validator then rejects the
        # stale CRL against the new key with `invalid_signature`, leaving
        # revocation status 'undetermined' and failing closed under Require.
        # Observed exactly that after a teardown could not reach the node:
        # three trusted CAs all named CN=AnalyticsCRLTestCA1, and a leftover
        # CRL from one of them poisoning the next test.
        self.ca_cert, self.ca_key = self.crl_utils.generate_ca(
            f"AnalyticsCRLTestCA_{uuid.uuid4().hex[:8]}"
        )
        self._trust_ca_on_cluster(self.ca_cert)

    def tearDown(self):
        # Log any failure's traceback before the cleanup below, so a failing
        # run is triageable while it is still executing rather than only once
        # testrunner prints its summary.
        self._log_test_failure()

        # Plain HTTP, and first: a test that left clientCertAuth in
        # 'mandatory' walls out self.rest, and every other cleanup step below
        # goes over HTTPS with no client certificate attached.
        try:
            self._disable_client_cert_auth()
        except Exception as exc:
            self.log.warning(f"clientCertAuth disable error: {exc}")
        for label, fn in (
            ("audit restore", self._restore_audit),
            # Datasets first: a dataset on a remote link depends on that
            # link, so dropping the link out from under it fails.
            ("remote dataset cleanup", self._cleanup_datasets),
            # Before the remote-target teardown below: a link is C1 metadata
            # pointing at C2, and dropping it after C2's certificates have
            # already been rolled back would try to reach a target that no
            # longer trusts the link's certificate.
            ("external link cleanup", self._cleanup_links),
            ("remote link target cleanup", self._cleanup_remote_targets),
            ("bucket cleanup", self._cleanup_buckets),
            ("topology change report", self._report_topology_changes),
            ("crlsValidate counter removal", self._crls_validate_counter_stop),
            ("CRL file cleanup", self._cleanup_created_files),
            ("CRL settings reset", self._reset_crl_settings),
            ("RBAC user cleanup", self._cleanup_rbac_users),
            ("temp PEM cleanup", self._cleanup_temp_pem_files),
            ("trusted CA cleanup", self._cleanup_trusted_cas),
        ):
            try:
                fn()
            except Exception as exc:
                self.log.warning(f"{label} error: {exc}")
        super(CBASCRLBase, self).tearDown()

    # ── Preconditions ────────────────────────────────────────────────────────


    # ── External object store (section 4) ────────────────────────────────────

    # Matches the packets cbauth sends to ns_server to ask for a revocation
    # verdict. Counting them is how a test shows a code path consulted the
    # CRL machinery -- or, for an external link, that it did not.
    _CRLS_VALIDATE_MATCH = ("OUTPUT -o lo -p tcp --dport 8091 "
                            "-m string --string crlsValidate --algo bm")
    CRLS_VALIDATE_RULE = f"{_CRLS_VALIDATE_MATCH} -j ACCEPT"

    def _require_object_store(self):
        """
        Connection details for an S3-compatible object store.

        Prefers AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY, which is what the
        rest of TAF uses (AWSProvider reads exactly these, and the WORM and
        Iceberg suites already run on them) and what CI exports, so these
        tests pick up an existing AWS-capable job with no extra wiring. Falls
        back to LOCALSTACK_* for a local S3-compatible store -- typically
        MinIO -- which is how the suite was developed and how it runs where
        no AWS identity is available.

        No credential is defaulted. This used to fall back to literal
        'minioadmin' values, which put a credential in the source tree for a
        rule that says not to, and turned a missing-configuration mistake
        into a confusing authentication failure against whatever endpoint
        was set. An incomplete environment now says so directly.

        Fails rather than skips when unset: a silently skipped test reports
        as passing coverage that never ran, and section 4 is exactly the area
        where that would matter.

        Returns:
            dict with endpoint, access_key, secret_key, region, bucket. The
            endpoint is None for real AWS, where the service's own default
            applies -- see _create_s3_link, which omits serviceEndpoint then.
        """
        aws_key = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
        if aws_key and aws_secret:
            return {
                # Unset for real S3: Analytics resolves the regional endpoint
                # itself. Only set this to reach an S3-compatible service
                # while still using the AWS_* credentials.
                "endpoint": os.getenv("AWS_S3_ENDPOINT") or None,
                "access_key": aws_key,
                "secret_key": aws_secret,
                "region": os.getenv("AWS_REGION", "us-east-1"),
                "bucket": os.getenv("AWS_S3_BUCKET", "test-backup-taf"),
            }

        endpoint = os.getenv("LOCALSTACK_ENDPOINT")
        access_key = os.getenv("LOCALSTACK_ACCESS_KEY_ID")
        secret_key = os.getenv("LOCALSTACK_SECRET_ACCESS_KEY")
        if not (endpoint and access_key and secret_key):
            missing = [
                name for name, value in (
                    ("LOCALSTACK_ENDPOINT", endpoint),
                    ("LOCALSTACK_ACCESS_KEY_ID", access_key),
                    ("LOCALSTACK_SECRET_ACCESS_KEY", secret_key),
                ) if not value
            ]
            self.fail(
                "This test needs an S3-compatible object store the Analytics "
                "node can reach. Either export AWS_ACCESS_KEY_ID and "
                "AWS_SECRET_ACCESS_KEY (plus optionally AWS_REGION / "
                "AWS_S3_BUCKET), which is what the rest of TAF uses, or point "
                "the suite at a local store with LOCALSTACK_ENDPOINT, "
                "LOCALSTACK_ACCESS_KEY_ID and LOCALSTACK_SECRET_ACCESS_KEY "
                "(plus optionally LOCALSTACK_REGION / LOCALSTACK_BUCKET). "
                f"No AWS credentials were set and these are missing: "
                f"{', '.join(missing)}."
            )
        return {
            "endpoint": endpoint,
            "access_key": access_key,
            "secret_key": secret_key,
            "region": os.getenv("LOCALSTACK_REGION", "us-east-1"),
            "bucket": os.getenv("LOCALSTACK_BUCKET", "cbas-crl-test"),
        }

    def _create_s3_link(self, name, store, dataverse="Default"):
        """
        Create an S3-type external link pointing at `store`, and register it
        for cleanup. Returns the link_properties dict that was used.

        Access-key authentication on purpose: that is the case section 4 asks
        about, and it is the one where nothing certificate-shaped is involved.

        serviceEndpoint is omitted entirely when the store has none, rather
        than sent as null: that is the real-AWS case, where Analytics derives
        the regional endpoint itself. Sending the key with an empty value
        would make it resolve nothing.
        """
        link_properties = {
            "name": name,
            "dataverse": dataverse,
            "scope": dataverse,
            "type": "s3",
            "accessKeyId": store["access_key"],
            "secretAccessKey": store["secret_key"],
            "region": store["region"],
        }
        if store.get("endpoint"):
            link_properties["serviceEndpoint"] = store["endpoint"]
        created = self.cbas_util.create_link(
            self.cluster, link_properties, create_dataverse=False
        )
        if not created:
            self.fail(f"Could not create S3 external link {dataverse}.{name}")
        self._created_links.append((dataverse, name))
        target = store.get("endpoint") or f"AWS S3 ({store['region']})"
        self.log.info(
            f"Created S3 external link {dataverse}.{name} -> {target}"
        )
        return link_properties

    def _cleanup_links(self):
        """Drop every external link a test created."""
        for dataverse, name in getattr(self, "_created_links", []):
            try:
                self.cbas_util.drop_link(
                    self.cluster, f"{dataverse}.{name}", if_exists=True
                )
            except Exception as exc:
                self.log.warning(f"Link {dataverse}.{name} drop error: {exc}")
        self._created_links = []

    def _cleanup_datasets(self):
        """
        Drop every dataset a test created on a remote link.

        Runs before _cleanup_links: a dataset on a remote link depends on
        that link, so dropping the link first leaves the dataset orphaned and
        the drop fails.
        """
        for full_name in getattr(self, "_created_datasets", []):
            try:
                self.cbas_util.drop_dataset(
                    self.cluster, full_name, if_exists=True)
            except Exception as exc:
                self.log.warning(f"Dataset {full_name} drop error: {exc}")
        self._created_datasets = []

    def _link_exists(self, name, dataverse="Default"):
        """True if the named link is present in Analytics metadata."""
        return self.cbas_util.validate_link_in_metadata(
            self.cluster, name, dataverse, "s3"
        )

    # ── crlsValidate accounting ──────────────────────────────────────────────

    def _crls_validate_counter_start(self):
        """Install the counting rule on the Analytics node; return the count."""
        shell = RemoteMachineShellConnection(self.cbas_node)
        try:
            # Drop any rule left by an aborted run so the count comes from one
            # rule rather than several stacked ones.
            shell.execute_command(
                f"iptables -D {self.CRLS_VALIDATE_RULE} 2>/dev/null")
            shell.execute_command(f"iptables -I {self.CRLS_VALIDATE_RULE}")
            self._crls_counter_installed = True
        finally:
            shell.disconnect()
        return self._crls_validate_count()

    def _crls_validate_count(self):
        """Packets matched so far, or None if the rule is absent."""
        shell = RemoteMachineShellConnection(self.cbas_node)
        try:
            # execute_command returns (stdout, stderr) in that order.
            output, _ = shell.execute_command(
                "iptables -L OUTPUT -v -n | grep crlsValidate | awk '{print $1}'"
            )
        finally:
            shell.disconnect()
        for line in (output or []):
            token = line.strip()
            if token.isdigit():
                return int(token)
        return None

    def _crls_validate_counter_stop(self):
        """Remove the counting rule. Safe when it was never installed."""
        if not getattr(self, "_crls_counter_installed", False):
            return
        shell = RemoteMachineShellConnection(self.cbas_node)
        try:
            shell.execute_command(
                f"iptables -D {self.CRLS_VALIDATE_RULE} 2>/dev/null")
        finally:
            shell.disconnect()
        self._crls_counter_installed = False

    def _require_crl_supported(self):
        if not self.cluster_util.is_enterprise_edition(self.cluster):
            self.fail("CRL support requires an Enterprise Edition cluster.")

    def _require_analytics_node(self):
        if not self.cluster.cbas_nodes:
            self.fail(
                "Analytics CRL tests need a cbas node -- pass "
                "services_init with 'cbas' on at least one node."
            )
        self.cbas_node = self.cluster.cbas_nodes[0]
        # External links created during a test -- dropped in tearDown
        self._created_links = []
        # Whether the crlsValidate counting iptables rule is installed
        self._crls_counter_installed = False

    # ── Failure reporting ────────────────────────────────────────────────────

    def _log_test_failure(self):
        """
        Emit the current test's traceback, if it failed, at the start of
        tearDown. Reads unittest's own _outcome.errors -- the structure
        cb_basetest.is_test_failed() checks -- so it reports what testrunner
        will report, just sooner. Best-effort: _outcome is a unittest internal
        whose shape changed after Python 3.10, so a miss logs nothing rather
        than masking the real failure.
        """
        try:
            outcome = getattr(self, "_outcome", None)
            errors = getattr(outcome, "errors", None) if outcome else None
            if not errors:
                return
            for _test, exc_info in errors:
                if not exc_info:
                    continue
                self.log.error(
                    f"TEST FAILED: {self._testMethodName}\n"
                    + "".join(traceback.format_exception(*exc_info)).strip()
                )
        except Exception as exc:
            self.log.warning(f"Could not log test failure traceback: {exc}")

    # ── CA trust (counterpart: backup_restore/crl_base.py) ───────────────────

    @staticmethod
    def _ca_dir(shell):
        os_type = shell.extract_remote_info().distribution_type
        if os_type == "windows":
            install_path = x509main.WININSTALLPATH
        elif os_type == "Mac":
            install_path = x509main.MACINSTALLPATH
        else:
            install_path = x509main.LININSTALLPATH
        return f"{install_path}{x509main.CHAINFILEPATH}/CA"

    @staticmethod
    def _ca_remote_filename(ca_cert):
        cn_attrs = ca_cert.subject.get_attributes_for_oid(NameOID.COMMON_NAME)
        cn = cn_attrs[0].value if cn_attrs else "ca"
        safe_cn = re.sub(r"[^A-Za-z0-9_.-]", "_", cn)
        return f"{safe_cn}_{ca_cert.serial_number}.pem"

    def _trust_ca_on_cluster(self, ca_cert, server=None, rest=None,
                             record_into=None):
        """
        Copy `ca_cert` into a node's CA inbox and load it into the trust store.

        Args:
            server: node whose inbox receives the PEM. Defaults to the local
                cluster's master.
            rest: RestConnection used for loadTrustedCAs and for reading back
                the assigned id. Must belong to the SAME cluster as `server`
                -- a section-3 test trusts the test CA on the remote cluster
                too, and loading it through the local cluster's REST endpoint
                would silently do nothing there.
            record_into: list to append the assigned trusted-CA id to, so the
                right teardown path removes it. Defaults to the local
                cluster's list.
        """
        server = server or self.cluster.master
        rest = rest or self.rest
        if record_into is None:
            record_into = self._trusted_ca_ids
        pem_bytes = self.crl_utils.cert_to_pem(ca_cert)
        remote_filename = self._ca_remote_filename(ca_cert)

        shell = RemoteMachineShellConnection(server)
        try:
            ca_dir = self._ca_dir(shell)
            shell.execute_command(f"mkdir -p {ca_dir}")
            with tempfile.NamedTemporaryFile(
                delete=False, suffix=".pem", mode="wb"
            ) as tmp_file:
                tmp_file.write(pem_bytes)
                local_path = tmp_file.name
            try:
                shell.copy_file_local_to_remote(
                    local_path, f"{ca_dir}/{remote_filename}"
                )
            finally:
                os.remove(local_path)
        finally:
            shell.disconnect()

        status, content = rest.load_trusted_CAs()
        if not status:
            self.fail(f"Failed to load trusted CAs on {server.ip}: {content}")

        cn_attrs = ca_cert.subject.get_attributes_for_oid(NameOID.COMMON_NAME)
        cn = cn_attrs[0].value if cn_attrs else None
        try:
            trusted = json.loads(content)
            matching = [
                entry.get("id") for entry in trusted
                if cn and cn in entry.get("subject", "")
            ]
            if matching:
                record_into.append(max(matching))
        except (ValueError, TypeError) as exc:
            self.log.warning(
                f"Could not identify trusted CA id for {cn!r}; it will not be "
                f"auto-untrusted in tearDown: {exc}"
            )

    # ── mTLS / CRL configuration ─────────────────────────────────────────────

    def _enable_client_cert_auth(self, state="enable", prefixes=None,
                                 master=None):
        if prefixes is None:
            prefixes = [{"path": "subject.cn", "prefix": "", "delimiter": ""}]
        status, content, _ = SecurityRestAPI(
            master or self.cluster.master
        ).set_client_cert_auth_config(state=state, prefixes=prefixes)
        self.assertTrue(status, f"Failed to set clientCertAuth: {content}")

    def _disable_client_cert_auth(self, master=None):
        # Plain HTTP on purpose -- works even while clientCertAuth is
        # 'mandatory', which walls out every HTTPS call including the one that
        # would relax it.
        server = master or self.cluster.master
        requests.post(
            f"http://{server.ip}:8091/settings/clientCertAuth",
            auth=(server.rest_username, server.rest_password),
            headers={"Content-Type": "application/json"},
            json={"state": "disable", "prefixes": []},
            timeout=30,
        )

    def _reset_crl_settings(self, rest=None):
        self.crl_utils.set_settings(
            rest or self.rest,
            policyPerScope={"clientAuth": "Disabled", "nodeToNode": "Disabled"},
            checkIntermediateCerts=False,
            urls=[],
        )

    # ── Certificates and CRL files ───────────────────────────────────────────

    def _write_temp_pem(self, pem_bytes, suffix=".pem"):
        with tempfile.NamedTemporaryFile(
            delete=False, suffix=suffix, mode="wb"
        ) as tmp_file:
            tmp_file.write(pem_bytes)
            path = tmp_file.name
        self._temp_pem_files.append(path)
        return path

    def _cleanup_temp_pem_files(self):
        for path in self._temp_pem_files:
            try:
                os.remove(path)
            except OSError:
                pass
        self._temp_pem_files = []

    def _client_cert_for(self, username, role="analytics_admin"):
        """
        An RBAC user plus a leaf certificate whose CN maps to it.

        Returns:
            tuple: (cert_path, key_path, serial)
        """
        user, _ = self._create_rbac_test_user(username, role)
        cert, key, serial = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, user
        )
        cert_path = self._write_temp_pem(self.crl_utils.cert_to_pem(cert))
        key_path = self._write_temp_pem(self.crl_utils.key_to_pem(key))
        return cert_path, key_path, serial

    def _track_uploaded_file(self, filename):
        self._created_files.append(filename)

    def _cleanup_created_files(self):
        for filename in self._created_files:
            status, _ = self.crl_utils.delete_file(self.rest, filename)
            if not status:
                self.log.warning(f"Failed to delete CRL file {filename}")
        self._created_files = []

    # ── RBAC ─────────────────────────────────────────────────────────────────

    def _create_rbac_test_user(self, username, role, password="Couchbase@1234",
                               master=None, record_into=None):
        """
        Create an RBAC user and grant it a role.

        `master` / `record_into` let a section-3 test create the user on a
        LINK TARGET instead: the certificate a link presents is mapped to a
        user by the remote cluster, so the user has to exist there, and its
        cleanup belongs to that target rather than to the local cluster's
        list.
        """
        RbacUtils((master or self.cluster.master))._create_user_and_grant_role(
            username, role, password=password
        )
        if record_into is None:
            record_into = self._rbac_users
        record_into.append(username)
        return username, password

    def _cleanup_rbac_users(self):
        for username in self._rbac_users:
            try:
                self.rest.delete_builtin_user(username)
            except Exception as exc:
                self.log.warning(f"Failed to delete RBAC user {username}: {exc}")
        self._rbac_users = []

    # Only a CA whose subject carries this marker is ever deleted, on top of
    # matching a recorded id. Deleting the wrong entry here is not a tidy-up
    # failure but a cluster-breaking one: an earlier version of the backup CRL
    # suite filtered on `id == 0` and removed the cluster's OWN generated CA,
    # which stranded it with no trust anchor and surfaced later as
    # "x509: certificate signed by unknown authority" on unrelated rebalances.
    TEST_CA_SUBJECT_MARKER = "AnalyticsCRLTestCA"

    def _untrust_ca_ids(self, server, ca_ids):
        """
        Delete the given trusted-CA ids from one cluster, by REST.

        Uses DELETE /pools/default/trustedCAs/<id>, which answers 204.

        This replaced a chronicle_kv edit driven through rest.diag_eval. That
        never worked from a test runner at all: /diag/eval only accepts
        requests originating on the node, so every call returned "API is
        accessible from localhost only" and the warning was swallowed by
        tearDown's try/except. The result was silent -- test CAs accumulated
        on every cluster (nine on one node across a single session) while the
        teardown reported nothing. Correctness was not affected, because each
        test generates a CA with a unique CN precisely so a leftover cannot
        collide, but the trust store grew without bound.

        Deletes only ids the listing confirms are uploaded CAs carrying this
        suite's subject marker, so a mis-recorded id cannot take out a real
        CA. An id the listing does not confirm is left alone rather than
        deleted: when the listing itself fails there is nothing to check
        against, and deleting unchecked is exactly the cluster-breaking
        outcome the marker exists to prevent. Leaving a test CA behind is a
        tidy-up miss and costs nothing -- each test's CA has a unique CN, so
        a leftover cannot collide with a later run.
        """
        auth = (server.rest_username, server.rest_password)
        base = f"http://{server.ip}:8091"
        try:
            listing = requests.get(f"{base}/pools/default/trustedCAs",
                                   auth=auth, timeout=30)
            if listing.status_code == 200:
                entries = {e.get("id"): e for e in listing.json()}
            else:
                self.log.warning(
                    f"Listing trusted CAs on {server.ip} returned "
                    f"{listing.status_code}; leaving every recorded CA in "
                    f"place rather than deleting ids this cannot verify.")
                entries = {}
        except (requests.exceptions.RequestException, ValueError) as exc:
            self.log.warning(
                f"Could not list trusted CAs on {server.ip}: {exc}. Leaving "
                f"every recorded CA in place rather than deleting ids this "
                f"cannot verify.")
            entries = {}

        for ca_id in ca_ids:
            entry = entries.get(ca_id)
            if entry is None:
                # Either the listing failed, or the id is already gone. Both
                # mean the same thing here: nothing confirms this id is ours,
                # so do not issue the DELETE.
                self.log.warning(
                    f"Not deleting trusted CA id={ca_id} on {server.ip}: the "
                    f"listing does not confirm it belongs to this suite.")
                continue
            subject = entry.get("subject", "")
            if (entry.get("type") != "uploaded"
                    or self.TEST_CA_SUBJECT_MARKER not in subject):
                self.log.warning(
                    f"Refusing to delete trusted CA id={ca_id} on "
                    f"{server.ip}: type={entry.get('type')!r} "
                    f"subject={subject!r} is not one of this suite's."
                )
                continue
            try:
                resp = requests.delete(
                    f"{base}/pools/default/trustedCAs/{ca_id}",
                    auth=auth, timeout=60)
                if resp.status_code not in (200, 202, 204, 404):
                    self.log.warning(
                        f"Trusted CA id={ca_id} delete on {server.ip} "
                        f"returned {resp.status_code}: {resp.text[:200]}")
            except requests.exceptions.RequestException as exc:
                self.log.warning(
                    f"Trusted CA id={ca_id} delete on {server.ip}: {exc}")

    def _cleanup_trusted_cas(self):
        """Drop this run's test CAs from the local cluster's trust store."""
        if not self._trusted_ca_ids:
            return
        self._untrust_ca_ids(self.cluster.master, self._trusted_ca_ids)
        self._trusted_ca_ids = []

    # ── Analytics requests ───────────────────────────────────────────────────

    def _analytics_query(self, statement, cert=None, auth=None, timeout=60,
                         node=None):
        """
        POST a SQL++ statement to the Analytics service over TLS, optionally
        presenting a client certificate.

        Bypasses CbasUtil deliberately: this suite needs `cert=` plumbed
        straight through to `requests`, which the util layer does not expose,
        and `verify=False` because the node's own certificate is self-signed
        (enabling verification would abort locally before the server ever
        evaluates the CRL).
        """
        node = node or self.cbas_node
        url = (f"https://{node.ip}:{self.ANALYTICS_SSL_PORT}"
               f"/analytics/service")
        return requests.post(
            url, data={"statement": statement}, cert=cert, auth=auth,
            verify=False, timeout=timeout, headers={"Connection": "close"},
        )

    def _mgmt_request(self, path="/pools/default", cert=None, auth=None,
                      timeout=30, node=None):
        """
        The same request against ns_server's mgmt port, for the cross-service
        consistency scenario: Analytics enforcement must match ns_server for
        the same certificate and policy.
        """
        node = node or self.cluster.master
        url = f"https://{node.ip}:{self.MGMT_SSL_PORT}{path}"
        return requests.get(
            url, cert=cert, auth=auth, verify=False, timeout=timeout,
            headers={"Connection": "close"},
        )

    def _read_analytics_log(self, grep=None, tail_lines=3000, node=None,
                            all_nodes=False):
        """
        Lines from the Analytics logs, by default on self.cbas_node.

        Reads analytics_info/error/debug together: the CRL verdict for a
        rejected handshake has been observed in analytics_info.log, while the
        Java exception chain carrying the same detail lands in the error log,
        and which one a given build uses is not worth assuming.

        Args:
            grep: substring to filter on (e.g. a certificate CN), applied on
                the node so only matching lines cross the wire.
            tail_lines: cap per file -- a validator problem can produce very
                large logs, and an unbounded read over SSH looks like a hang.
            node: read this node instead of self.cbas_node.
            all_nodes: read EVERY Analytics node, plus the cluster's CC node.
                Needed by any assertion that something is ABSENT from the
                logs: self.cbas_node is just cbas_nodes[0], which need not be
                the node that handled the operation, and reading the wrong
                node makes a leak look like clean output. The same mistake
                against the audit log once nearly produced a filed bug.
        """
        if all_nodes:
            targets, seen = [], set()
            candidates = list(self.cluster.cbas_nodes or [])
            cc_node = getattr(self.cluster, "cbas_cc_node", None)
            if cc_node is not None:
                candidates.append(cc_node)
            for candidate in candidates:
                if candidate.ip not in seen:
                    seen.add(candidate.ip)
                    targets.append(candidate)
            lines = []
            for target in targets:
                lines.extend(self._read_analytics_log(
                    grep=grep, tail_lines=tail_lines, node=target))
            return lines
        pattern = grep.replace("'", "") if grep else ""
        cmd = (
            "for f in /opt/couchbase/var/lib/couchbase/logs/analytics_info.log "
            "/opt/couchbase/var/lib/couchbase/logs/analytics_error.log "
            "/opt/couchbase/var/lib/couchbase/logs/analytics_debug.log; do "
            f"[ -f \"$f\" ] && tail -n {int(tail_lines)} \"$f\"; done"
        )
        if pattern:
            cmd += f" | grep -a '{pattern}'"
        cmd += " || true"
        shell = RemoteMachineShellConnection(node or self.cbas_node)
        try:
            output, _ = shell.execute_command(cmd)
        finally:
            shell.disconnect()
        return [line.rstrip("\n") for line in (output or []) if line.strip()]

    # A ConnectionError only proves refusal if it carries one of these. Same
    # set the backup and FTS CRL suites use, so a rejection is identified
    # consistently across suites.
    TLS_REJECTION_MARKERS = (
        "certificate revoked", "tlsv1 alert", "sslv3 alert",
        "alert certificate", "handshake failure", "bad certificate",
        "unknown ca", "certificate unknown", "certificate required",
        "decrypt error",
    )

    @classmethod
    def _is_tls_rejection(cls, exc):
        """
        True when a ConnectionError carries an explicit TLS alert, i.e. the
        peer refused the certificate rather than the connection simply
        failing.

        Shared by assert_cert_refused and the long-running probes, so both
        agree on what counts as a refusal instead of each deciding for
        itself.
        """
        text = str(exc).lower()
        return any(marker in text for marker in cls.TLS_REJECTION_MARKERS)

    def assert_cert_refused(self, request_fn, msg):
        """
        Assert a certificate was refused, in either of the two forms the PRD
        permits.

        The PRD's runtime-enforcement section says of the revocation check:
        "For Phase I. It is acceptable to return 401 instead of sending a tls
        alert to allow NS-server to use the callback." A test that insists on a
        TLS alert would therefore fail against a conformant implementation that
        answers 401 instead. Analytics sends alerts today, so these tests pass
        either way -- accepting both keeps them from breaking on a legitimate
        change.

        Deliberately narrow about what counts as refusal:
          TLS-layer alert (SSLError)           -> refused
          ConnectionError carrying a TLS alert -> refused
          ConnectionError with no TLS alert    -> FAILURE, not refusal
          HTTP 401                             -> refused (the PRD's allowance)
          anything else, including 200/403/5xx -> failure

        403 is excluded because it means the certificate was accepted and then
        denied by RBAC, which is a different outcome from revocation.

        The ConnectionError split is the important one. A wrong port, a node
        that is down and a dropped packet all raise ConnectionError just as a
        refused handshake does, so accepting the bare exception would let
        every test here pass against a product that never enforced anything.
        Only an error carrying an explicit TLS alert counts.

        Args:
            request_fn: zero-arg callable performing the request.
            msg: what the caller is proving, used in the failure message.

        Returns:
            The response when refusal came as a 401, else None.
        """
        try:
            resp = request_fn()
        except requests.exceptions.SSLError:
            # A TLS-layer failure is a refusal by definition.
            return None
        except requests.exceptions.ConnectionError as exc:
            # NOT a refusal on its own. A wrong port, a down node or a
            # dropped packet also raises ConnectionError, and treating those
            # as "revocation enforced" lets the test pass while the product
            # does nothing. Only accept one that carries an explicit TLS
            # alert marker.
            if not self._is_tls_rejection(exc):
                self.fail(
                    f"{msg}. The connection failed, but with no TLS alert to "
                    f"show it was refused for the certificate: {exc}. That "
                    f"looks like an infrastructure problem (wrong port, node "
                    f"down, packet dropped) rather than enforcement, and "
                    f"passing on it would hide a product that never enforced "
                    f"at all."
                )
            return None
        self.assertEqual(
            resp.status_code, 401,
            f"{msg}. The connection was not refused at the TLS layer, so the "
            f"only other acceptable outcome is HTTP 401 (per the PRD's Phase I "
            f"allowance); got {resp.status_code}: {resp.text[:300]}"
        )
        return resp

    def _wait_for_analytics_ok(self, statement="SELECT 1;", cert=None,
                               auth=None, timeout_s=60, interval=3,
                               node=None):
        """
        Poll until Analytics answers 200, absorbing the propagation delay
        between a CA-trust / CRL / clientCertAuth change landing on
        cluster.master and the Analytics node picking it up. Raises the last
        SSLError, or returns the last non-200 response, if the deadline passes.
        """
        deadline = time.time() + timeout_s
        last_exc = None
        last_resp = None
        while time.time() < deadline:
            try:
                resp = self._analytics_query(statement, cert=cert, auth=auth,
                                             node=node)
                if resp.status_code == 200:
                    return resp
                last_resp = resp
            except requests.exceptions.SSLError as exc:
                last_exc = exc
            time.sleep(interval)
        if last_resp is not None:
            return last_resp
        raise last_exc

    # ── Diagnostics, metrics and audit (sections 6 and 9) ────────────────────

    # Statuses the diagnostics endpoint uses for a certificate it is happy
    # with. Anything else means it would refuse, which is how the section 6
    # consistency check maps a verdict onto expected runtime behaviour.
    # Determined empirically against 8.5.0-1009 rather than taken from the
    # plan, whose status list ("not_revoked", "unknown_missing_crl", ...) does
    # not match what the server actually returns.
    DIAGNOSTICS_OK_STATUSES = ("valid", "not_revoked", "ok")

    def _diagnostics_verdict(self, pem_bytes, policy="Require"):
        """
        Ask the admin diagnostic endpoint how it judges one certificate.

        Returns (status, details) from the single results entry, or
        (None, raw) if the call or its shape was not what we expect.
        """
        pem = pem_bytes.decode() if isinstance(pem_bytes, bytes) else pem_bytes
        status, content = self.crl_utils.diagnostics_validate(
            self.rest, policy=policy, certs=[pem]
        )
        if not status or not isinstance(content, dict):
            return None, content
        results = content.get("results") or []
        if len(results) != 1:
            return None, content
        return results[0].get("status"), results[0].get("details")

    def _diagnostics_says_acceptable(self, verdict_status):
        """True if the diagnostics verdict is one a connection should survive."""
        return (verdict_status or "").lower() in self.DIAGNOSTICS_OK_STATUSES

    def _crl_metrics(self, service="cbas", node=None):
        """
        The cbauth CRL metrics for one service, from Prometheus on ns_server.

        Returns {metric_name: float}. Four families exist on 8.5.0-1009, all
        cache-oriented and all carrying a service label:
        cm_cbauth_crl_cache_{current_items,hit_total,miss_total,max_items}.

        Defaults to the Analytics node, not the master: /metrics is served
        per node and reports that node's own cbauth counters, so scraping the
        master shows nothing for a check performed on a different node.
        """
        node = node or self.cbas_node
        resp = requests.get(
            f"http://{node.ip}:8091/metrics",
            auth=(node.rest_username, node.rest_password), timeout=60,
        )
        resp.raise_for_status()
        out = {}
        for line in resp.text.splitlines():
            if not line.startswith("cm_cbauth_crl"):
                continue
            if f'service="{service}"' not in line:
                continue
            name = line.split("{", 1)[0]
            try:
                out[name] = float(line.rsplit(" ", 1)[1])
            except (IndexError, ValueError):
                continue
        return out

    def _cbas_crl_metrics(self, node=None):
        """
        The Analytics service's own CRL metrics, from Prometheus on ns_server.

        Returns {series: float} keyed by the full series including labels,
        e.g. 'cbas_crl_checks_total{result="rejected",scope="clientAuth"}'.
        Label order is whatever the exporter emits, so match with
        _cbas_crl_metric_sum rather than by exact key.

        Added by MB-73654 (cbas-core 3acd9eb, first in 8.5.0-1101), and
        distinct from the cm_cbauth_crl_* family that _crl_metrics reads:
        these are cbas_-prefixed on the Analytics registry. ns_server
        re-exposes them on its own /metrics alongside the other cbas_ series,
        so one scrape of the same endpoint serves both families.

        Scraped from the Analytics node for the same reason as _crl_metrics:
        each process reports its own counters.
        """
        node = node or self.cbas_node
        resp = requests.get(
            f"http://{node.ip}:8091/metrics",
            auth=(node.rest_username, node.rest_password), timeout=60,
        )
        resp.raise_for_status()
        out = {}
        for line in resp.text.splitlines():
            if not line.startswith("cbas_crl"):
                continue
            try:
                series, value = line.rsplit(" ", 1)
                out[series.strip()] = float(value)
            except (IndexError, ValueError):
                continue
        return out

    @staticmethod
    def _cbas_crl_metric_sum(metrics, name, **labels):
        """
        Sum every series of `name` whose label set includes all of `labels`.

        Matching a subset rather than an exact series string keeps callers
        independent of label ordering and of labels added later.

        :param metrics: dict from _cbas_crl_metrics
        :param name: metric name without labels, e.g. cbas_crl_checks_total
        :param labels: label values that must all be present
        """
        total = 0.0
        for series, value in metrics.items():
            if series.split("{", 1)[0] != name:
                continue
            if all(f'{key}="{val}"' in series for key, val in labels.items()):
                total += value
        return total

    def _crl_metric_services(self, node=None):
        """Every service label present on the cbauth CRL metrics."""
        node = node or self.cbas_node
        resp = requests.get(
            f"http://{node.ip}:8091/metrics",
            auth=(node.rest_username, node.rest_password), timeout=60,
        )
        resp.raise_for_status()
        services = set()
        for line in resp.text.splitlines():
            if line.startswith("cm_cbauth_crl") and 'service="' in line:
                services.add(line.split('service="', 1)[1].split('"', 1)[0])
        return services

    def _set_audit(self, enabled):
        """Turn cluster auditing on or off, remembering the original state."""
        if not hasattr(self, "_audit_was_enabled"):
            try:
                self._audit_was_enabled = bool(
                    self.rest.getAuditSettings().get("auditdEnabled"))
            except Exception:
                self._audit_was_enabled = False
        self.rest.setAuditSettings(enabled="true" if enabled else "false")
        self._audit_restore_needed = True

    def _restore_audit(self):
        """Put auditing back the way the test found it."""
        if not getattr(self, "_audit_restore_needed", False):
            return
        try:
            self.rest.setAuditSettings(
                enabled="true" if self._audit_was_enabled else "false")
        except Exception as exc:
            self.log.warning(f"Audit restore error: {exc}")
        self._audit_restore_needed = False

    def _read_audit_log(self, since_marker=None, tail_lines=2000, node=None):
        """
        Lines from audit.log across the cluster.

        Every node keeps its own audit.log and only records what it handled,
        so a REST action against the master is not visible on the Analytics
        node. Unless a specific node is asked for, all of them are read and
        the lines concatenated.
        """
        nodes = [node] if node else list(
            self.cluster.nodes_in_cluster or [self.cbas_node])
        lines = []
        for target in nodes:
            shell = RemoteMachineShellConnection(target)
            try:
                # The file is NOT audit.log. ns_server writes a
                # per-rotation file named <host>-<timestamp>-audit.log and
                # points current-audit.log at it, so a plain audit.log read
                # finds nothing and looks exactly like "auditing recorded
                # nothing".
                output, _ = shell.execute_command(
                    f"tail -n {tail_lines} "
                    f"/opt/couchbase/var/lib/couchbase/logs/current-audit.log "
                    f"2>/dev/null || tail -n {tail_lines} "
                    f"/opt/couchbase/var/lib/couchbase/logs/*-audit.log "
                    f"2>/dev/null"
                )
                lines.extend(output or [])
            except Exception as exc:
                self.log.warning(f"audit.log read failed on {target.ip}: {exc}")
            finally:
                shell.disconnect()
        return lines

    # ── Remote Couchbase link targets (section 3) ────────────────────────────
    #
    # Section 3 covers a SECOND, independent certificate trust relationship:
    # an Analytics Link authenticating OUTBOUND to a remote Couchbase cluster
    # over mTLS, versus the inbound client access every other section tests.
    # The revocation policy that governs it is the REMOTE cluster's, because
    # the remote is the TLS server that validates the presented certificate.
    #
    # The link property shape used here (encryption=full plus certificate,
    # clientCertificate and clientKey, with username/password deliberately
    # absent) mirrors the `encryption=full2` case already proven by
    # cbas_external_links_CB_cluster.py's CBASExternalLinks suite. That suite
    # builds its certificates with x509main's on-disk CA hierarchy; this one
    # keeps generating them in memory through CRLUtils instead, because a CRL
    # test needs the issuing CA's private key and the leaf's serial number in
    # hand to sign and publish a revocation, and reading those back out of
    # x509main's directory layout would buy nothing.

    # Omitting username/password from a full-encryption link is what makes the
    # revocation result unambiguous: there is no alternate credential left for
    # the remote to fall back to, so a link that stops working after its
    # serial is published can only have stopped for the certificate.
    LINK_ENCRYPTION_FULL = "full"

    # The bucket a section-3 dataset ingests from on the link target. Created
    # by cluster_kv_infra=default on that cluster (see the conf), which is
    # bucket_util.create_default_bucket's fixed name.
    REMOTE_BUCKET = "default"

    # How a link failure names each of the three causes section 3 requires to
    # be distinguishable. Matched case-insensitively against the whole error
    # body. Asserted asymmetrically -- see _assert_link_error_kind -- because
    # a revocation failure surfacing as a timeout is the actual risk, while
    # some overlap in generic transport words is not worth failing over.
    LINK_REVOKED_MARKERS = (
        "revoked", "revocation", "crl", "certificate_revoked",
        "bad certificate", "certificate unknown", "unknown ca",
    )
    LINK_UNREACHABLE_MARKERS = (
        "connection refused", "no route to host", "timed out", "timeout",
        "unreachable", "unknownhost", "unknown host", "failed to connect",
        "connect timed out",
    )
    LINK_CREDENTIAL_MARKERS = (
        "unauthorized", "invalid credentials", "authentication failed",
        "401", "forbidden", "403", "invalid username",
    )

    # Substrings that must never appear in a log line, audit record or REST
    # response describing a link failure. PRIVATE KEY armour only -- see
    # CERT_MATERIAL_MARKERS below for why a certificate is not on this list.
    KEY_MATERIAL_MARKERS = (
        "-----begin private key-----",
        "-----begin rsa private key-----",
        "-----begin ec private key-----",
        "-----begin encrypted private key-----",
    )

    # A certificate is deliberately NOT treated as a leak.
    #
    # Section 3's last bullet says "link certificate/key material is not
    # exposed in logs, audit events, or diagnostic output", but a certificate
    # is public by construction -- it is sent in the clear during every TLS
    # handshake, so anyone who can reach the endpoint already has it. The
    # security property that matters is the PRIVATE key, and on 8.5.0-1073
    # Analytics redacts exactly that:
    #     clientKey='<redacted 1704 chars>', password='<redacted>'
    # while logging certificates=[-----BEGIN CERTIFICATE-----...] in full.
    # Zero private-key PEM markers appear anywhere in analytics_*.log.
    #
    # An earlier version of this suite listed certificate armour alongside
    # the key markers and failed the test on that line. That was the test
    # being wrong, not the product: it also meant a certificate appearing
    # first would mask the private-key question, which is the one worth
    # asking. Certificate presence is now recorded as an observation.
    CERT_MATERIAL_MARKERS = (
        "-----begin certificate-----",
    )

    def _remote_cluster(self, index=1):
        """
        One of the extra clusters CBASBaseTest built from `num_of_clusters`.

        Ordered by cluster name, so index 0 is the local Analytics cluster
        (self.cluster) and index 1 upwards are the link targets. Fails with
        the conf that would fix it rather than raising IndexError, because
        every section-3 test is unrunnable without this and a bare KeyError
        several frames down is not a useful diagnosis.
        """
        names = sorted(self.cb_clusters)
        if len(names) <= index:
            self.fail(
                f"This test needs at least {index + 1} clusters, got "
                f"{names}. Section-3 conf lines must carry "
                f"num_of_clusters={index + 1} with pipe-separated nodes_init "
                f"and services_init, e.g. num_of_clusters=2,nodes_init=2|1,"
                f"services_init=kv:n1ql:index-kv:cbas|kv -- CBASBaseTest "
                f"builds its clusters from those params and ignores the "
                f"ini's own [clusterN] sections."
            )
        return self.cb_clusters[names[index]]

    def _served_certificate(self, server, port=None):
        """
        The certificate a node actually serves on a TLS port.

        Reads the wire rather than the REST API's view: after a
        reloadCertificate the two can disagree if the reload quietly did
        nothing, and this suite's teardown depends on knowing which is true.
        """
        pem = ssl.get_server_certificate((server.ip, port or 18091))
        return x509.load_pem_x509_certificate(pem.encode())

    def _install_node_certificate(self, server, ca_cert, ca_key,
                                  record_into=None):
        """
        Reissue `server`'s own node certificate from the given CA and reload
        it. Returns the new certificate's serial.

        Needed on the link TARGET, not for its own sake: a link with
        encryption=full verifies the remote's node certificate against the
        `certificate` bundle it was configured with, so the remote has to be
        serving something that bundle actually chains to. Without this the
        link fails on chain verification and the failure is indistinguishable
        from the revocation the test is trying to observe.

        Two details are load bearing. SERVER_AUTH must be present alongside
        CLIENT_AUTH -- a node certificate is presented on inbound TLS and
        also used outbound between nodes -- and the SANs must cover every
        address used to reach the node, or peers reject it on name mismatch
        long before revocation is ever consulted.

        The CA must already be trusted on that cluster (_trust_ca_on_cluster
        against the same cluster's REST endpoint) or the reload is refused.
        """
        cert, key, serial = self.crl_utils.generate_leaf_cert(
            ca_cert, ca_key, server.ip,
            extended_key_usage=[ExtendedKeyUsageOID.SERVER_AUTH,
                                ExtendedKeyUsageOID.CLIENT_AUTH],
            dns_names=[server.ip, "127.0.0.1", "localhost"],
        )
        chain_pem = self.crl_utils.cert_to_pem(cert)
        key_pem = self.crl_utils.key_to_pem(key)

        inbox = "/opt/couchbase/var/lib/couchbase/inbox"
        shell = RemoteMachineShellConnection(server)
        try:
            shell.execute_command(f"mkdir -p {inbox}")
            # base64 through a single echo rather than a heredoc: PEM is
            # multi-line and full of characters the remote shell mangles.
            for payload, name in ((chain_pem, "chain.pem"),
                                  (key_pem, "pkey.key")):
                encoded = base64.b64encode(payload).decode()
                shell.execute_command(
                    f"echo {encoded} | base64 -d > {inbox}/{name}")
            shell.execute_command(f"chown -R couchbase:couchbase {inbox}")
            shell.execute_command(f"chmod 600 {inbox}/pkey.key")
        finally:
            shell.disconnect()

        rest = RestConnection(server)
        status, content, _ = rest._http_request(
            rest.baseUrl + "node/controller/reloadCertificate", "POST")
        if not status:
            self.fail(
                f"reloadCertificate failed on {server.ip}: {content}. The "
                f"test CA has to be trusted on that node's own cluster before "
                f"a certificate issued by it can be loaded."
            )
        if record_into is not None:
            record_into.append(server.ip)
        self.log.info(
            f"Node {server.ip} now serves a test-CA certificate, "
            f"serial={serial}"
        )
        return serial

    def _restore_self_signed_node_certs(self, target):
        """
        Put a link target's nodes back on built-in self-signed certificates.

        Cluster-wide in one call, and it WAITS for the swap to be observable
        on the wire before returning. Dropping the trust anchor while a node
        still presents a test-CA certificate leaves that cluster unable to
        verify its own peers, which surfaces later as "x509: certificate
        signed by unknown authority" and fails the NEXT test's rebalance
        rather than this one's teardown.
        """
        if not target["cert_nodes"]:
            return
        cluster = target["cluster"]
        self.log.info(
            f"Restoring self-signed node certs on {target['name']} "
            f"(test certs were installed on {target['cert_nodes']})"
        )
        RestConnection(cluster.master).regenerate_cluster_certificate()

        ca_cn = target["ca_cert"].subject.rfc4514_string().split(
            "CN=")[-1].split(",")[0]
        deadline = time.time() + 120
        for server in cluster.servers:
            if server.ip not in target["cert_nodes"]:
                continue
            while time.time() < deadline:
                try:
                    issuer = self._served_certificate(server).issuer
                    if ca_cn not in issuer.rfc4514_string():
                        break
                except Exception as exc:
                    self.log.warning(
                        f"Could not read {server.ip}'s served cert while "
                        f"waiting for the self-signed swap: {exc}")
                time.sleep(5)
            else:
                self.log.error(
                    f"Node {server.ip} still presents a {ca_cn} certificate "
                    f"after 120s. Leaving its CA trusted rather than "
                    f"stranding the cluster without a trust anchor."
                )
                target["ca_ids"] = []
                return

        # Regenerating swaps the ACTIVE certificate but leaves whatever was
        # staged in the inbox on disk, where the next test would start from
        # this test's chain and key.
        inbox = "/opt/couchbase/var/lib/couchbase/inbox"
        for server in cluster.servers:
            if server.ip not in target["cert_nodes"]:
                continue
            shell = RemoteMachineShellConnection(server)
            try:
                shell.execute_command(
                    f"rm -f {inbox}/chain.pem {inbox}/pkey.key")
            finally:
                shell.disconnect()
        target["cert_nodes"] = []

    def _setup_remote_link_target(self, remote_cluster=None, label=None,
                                  policy="Require", ca_cert=None, ca_key=None):
        """
        Make a remote cluster usable as an mTLS Analytics Link target whose
        certificates this suite can revoke.

        Does five things on the REMOTE cluster, in the order they depend on
        each other:
          1. trusts a CA (this test's own by default),
          2. reissues the remote's node certificate from that CA, so a
             full-encryption link can verify what the remote serves,
          3. enables clientCertAuth so a presented certificate maps to a user,
          4. publishes a benign CRL from that CA. Under Require with no
             applicable CRL, cbauth answers `status undetermined` and fails
             closed -- so without this every link would fail for the wrong
             reason and the test would prove nothing,
          5. sets the remote's clientAuth policy.

        clientCertAuth is left at "enable" rather than "mandatory" on purpose.
        A link configured with certificates carries no username or password at
        all, so there is nothing for the remote to fall back to either way,
        and "enable" keeps the remote's own admin REST reachable for the CRL
        publishing this suite does throughout the test.

        Returns a target dict carrying its own teardown state; register it in
        self._remote_targets and _cleanup_remote_targets unwinds it.
        """
        remote_cluster = remote_cluster or self._remote_cluster()
        ca_cert = ca_cert or self.ca_cert
        ca_key = ca_key or self.ca_key
        label = label or remote_cluster.name

        target = {
            "name": label,
            "cluster": remote_cluster,
            "rest": RestConnection(remote_cluster.master),
            "ca_cert": ca_cert,
            "ca_key": ca_key,
            "ca_pem": self.crl_utils.cert_to_pem(ca_cert),
            "crl_filename": f"cbas_crl_link_{label.lower()}.pem",
            "crl_number": 0,
            # teardown state, unwound by _cleanup_remote_targets
            "files": [],
            "users": [],
            "ca_ids": [],
            "cert_nodes": [],
            "cert_auth": False,
            "crl_policy": False,
            "bucket": None,
        }
        self._remote_targets.append(target)

        # Bucket first: it is plain REST against the remote and has
        # nothing to do with certificates, so a failure here is
        # unambiguous rather than tangled up with mTLS setup.
        self._ensure_remote_bucket(target)

        self._trust_ca_on_cluster(
            ca_cert, server=remote_cluster.master, rest=target["rest"],
            record_into=target["ca_ids"],
        )
        self._install_node_certificate(
            remote_cluster.master, ca_cert, ca_key,
            record_into=target["cert_nodes"],
        )
        self._enable_client_cert_auth(
            state="enable", master=remote_cluster.master)
        target["cert_auth"] = True

        # Benign CRL first, policy second: the other order leaves a window in
        # which Require is live with no applicable CRL, and anything
        # connecting in that window fails closed for a reason unrelated to
        # the test.
        self._publish_remote_crl(target, [])
        status, content = self.crl_utils.set_settings(
            target["rest"],
            policyPerScope={"clientAuth": policy, "nodeToNode": "Disabled"},
        )
        self.assertTrue(
            status,
            f"Could not set clientAuth={policy} on link target "
            f"{label}: {content}"
        )
        target["crl_policy"] = True
        self.log.info(
            f"Link target {label} ({remote_cluster.master.ip}) ready: test CA "
            f"trusted, node cert reissued, clientCertAuth enabled, "
            f"clientAuth={policy}"
        )
        return target

    def _ensure_remote_bucket(self, target, ram_quota_mb=256):
        """Section-3 entry point: ensure the bucket on a link target."""
        self._ensure_bucket(target["cluster"].master, self.REMOTE_BUCKET,
                            ram_quota_mb=ram_quota_mb)
        target["bucket"] = self.REMOTE_BUCKET

    def _ensure_bucket(self, server, name, ram_quota_mb=256):
        """
        Make sure the link target has the bucket a section-3 dataset ingests
        from, creating it if absent, and wait until it is actually servable.

        Deliberately direct REST rather than cluster_kv_infra / bucket_util.

        cluster_kv_infra=...|default asks CBASBaseTest to build the bucket on
        the secondary cluster, and on a param-built secondary cluster that
        path fails: bucket_util.get_updated_bucket_server_list exhausts its
        15 x 2s retry resolving the bucket's vBucketServerMap against
        cluster.nodes_in_cluster and setUp then raises "Create bucket default
        failed: Bucket not warmed up" -- while the bucket is in fact healthy
        and serving (verified against a live 8.5.0-1073 node whose bucket had
        a one-entry serverList at the moment the retry gave up). That is a
        framework bug in the multi-cluster path, and the tests here do not
        need any of the bookkeeping it exists to maintain: nothing in section
        3 touches cluster.buckets, loads documents, or asks bucket_util
        anything. All the dataset needs is for the bucket to exist.

        Waits on the two conditions that actually matter for an Analytics
        dataset to ingest -- the bucket is healthy and has a non-empty
        vBucketServerMap.serverList -- rather than on a node-object match.
        """
        auth = (server.rest_username, server.rest_password)
        base = f"http://{server.ip}:8091"

        resp = requests.get(f"{base}/pools/default/buckets/{name}",
                            auth=auth, timeout=30)
        if resp.status_code == 404:
            created = requests.post(
                f"{base}/pools/default/buckets", auth=auth, timeout=60,
                data={
                    "name": name,
                    "bucketType": "membase",
                    "ramQuotaMB": ram_quota_mb,
                    # replicaNumber=0 because the link target is a single
                    # node; a replica it cannot place leaves the bucket
                    # permanently degraded.
                    "replicaNumber": 0,
                    "storageBackend": "couchstore",
                    "flushEnabled": 1,
                },
            )
            self.assertIn(
                created.status_code, (200, 202),
                f"Could not create bucket {name} on {server.ip}: "
                f"{created.status_code} "
                f"{created.text[:300]}"
            )
            self._buckets_created.append((server, name))
            self.log.info(
                f"Created bucket {name} on {server.ip}")
        elif resp.status_code != 200:
            self.fail(
                f"Could not read bucket {name} on {server.ip}: "
                f"{resp.status_code} {resp.text[:300]}"
            )

        deadline = time.time() + 180
        last = None
        while time.time() < deadline:
            info = requests.get(f"{base}/pools/default/buckets/{name}",
                                auth=auth, timeout=30)
            if info.status_code == 200:
                body = info.json()
                nodes = body.get("nodes") or []
                server_list = (body.get("vBucketServerMap") or {}).get(
                    "serverList") or []
                healthy = nodes and all(
                    n.get("status") == "healthy" for n in nodes)
                if healthy and server_list:
                    self.log.info(
                        f"Bucket {name} on {server.ip} is servable "
                        f"(serverList={server_list})"
                    )
                    return
                last = (f"healthy={bool(healthy)} "
                        f"serverList={server_list} nodes={len(nodes)}")
            else:
                last = f"{info.status_code} {info.text[:200]}"
            time.sleep(3)
        self.fail(
            f"Bucket {name} on {server.ip} did not become "
            f"servable within 180s (last: {last}). A dataset cannot ingest "
            f"from it, so the section-3 connect would not be a remote "
            f"operation."
        )

    def _delete_remote_bucket(self, target):
        """Drop the bucket this suite created on a link target."""
        if not target.get("bucket"):
            return
        server = target["cluster"].master
        try:
            requests.delete(
                f"http://{server.ip}:8091/pools/default/buckets/"
                f"{target['bucket']}",
                auth=(server.rest_username, server.rest_password), timeout=60,
            )
        except requests.exceptions.RequestException as exc:
            self.log.warning(
                f"{target['name']} bucket {target['bucket']} delete: {exc}")
        target["bucket"] = None

    def _report_topology_changes(self):
        """Log any topology this test moved, for the next test's triage."""
        for label, nodes in (("rebalanced in", self._rebalanced_in),
                             ("rebalanced out", self._rebalanced_out),
                             ("failed over", self._failed_over)):
            if nodes:
                self.log.info(
                    f"This test {label}: {[n.ip for n in nodes]}. The cluster "
                    f"is rebuilt by the next test's setUp.")
        self._rebalanced_in = []
        self._rebalanced_out = []
        self._failed_over = []

    def _cleanup_buckets(self):
        """Drop every bucket this suite created, on whichever cluster."""
        for server, name in getattr(self, "_buckets_created", []):
            try:
                requests.delete(
                    f"http://{server.ip}:8091/pools/default/buckets/{name}",
                    auth=(server.rest_username, server.rest_password),
                    timeout=60)
            except requests.exceptions.RequestException as exc:
                self.log.warning(f"Bucket {name} on {server.ip} delete: {exc}")
        self._buckets_created = []

    def _query_node(self):
        """
        A node running the query service, or a clear failure saying which
        conf param would fix it.
        """
        nodes = list(getattr(self.cluster, "query_nodes", []) or [])
        if not nodes:
            nodes = [s for s in self.cluster.servers
                     if "n1ql" in (getattr(s, "services", "") or "")]
        if not nodes:
            self.fail(
                "This test loads documents through the query service, but no "
                "node in the cluster runs n1ql. Add it to services_init, e.g. "
                "services_init=kv:n1ql:index-kv:cbas."
            )
        return nodes[0]

    def _n1ql_insert_docs(self, bucket, count, key_prefix, node=None,
                          scope="_default", collection="_default"):
        """
        Insert `count` trivial documents through the Query service.

        N1QL rather than the SDK on purpose: an INSERT with explicit keys
        needs no index and no SDK bootstrap, so a section-5 ingestion test
        does not acquire an SDK dependency just to put rows in a bucket.

        The node is chosen from the cluster's QUERY nodes, not from
        cluster.master. In an Analytics topology the master is frequently the
        kv:cbas node, which runs no query service, and posting there fails
        with a bare "Connection refused" on 8093 that looks like a cluster
        problem rather than a wrong-node mistake.
        """
        node = node or self._query_node()
        rows = ", ".join(
            f'("{key_prefix}{i}", {{"id": {i}, "src": "{key_prefix}"}})'
            for i in range(count)
        )
        stmt = (f"INSERT INTO `{bucket}`.`{scope}`.`{collection}` "
                f"(KEY, VALUE) VALUES {rows}")
        resp = requests.post(
            f"http://{node.ip}:8093/query/service",
            auth=(node.rest_username, node.rest_password),
            data={"statement": stmt}, timeout=180,
        )
        self.assertEqual(
            resp.status_code, 200,
            f"N1QL insert of {count} docs into {bucket} failed: "
            f"{resp.status_code} {resp.text[:400]}"
        )
        self.log.info(f"Inserted {count} docs into {bucket} as {key_prefix}*")

    def _publish_remote_crl(self, target, serials):
        """
        Publish a CRL on the link target revoking exactly `serials`.

        Every call bumps crlNumber, and `serials` is the COMPLETE set that
        must remain revoked -- a CRL with a higher crlNumber from the same
        issuer supersedes the previous one wholesale, so re-listing is not
        redundant. Omitting a serial here un-revokes it, which is how this
        suite restores connectivity for the "removed from the CRL" bullet and
        also how it has previously produced a silent false pass.
        """
        target["crl_number"] += 1
        status, content = self.crl_utils.revoke_and_upload(
            target["rest"], target["ca_cert"], target["ca_key"],
            list(serials), target["crl_filename"],
            crl_number=target["crl_number"],
        )
        self.assertTrue(
            status,
            f"CRL upload to link target {target['name']} failed "
            f"(crlNumber={target['crl_number']}, serials={list(serials)}): "
            f"{content}"
        )
        if target["crl_filename"] not in target["files"]:
            target["files"].append(target["crl_filename"])
        self.log.info(
            f"Published CRL on {target['name']}: crlNumber="
            f"{target['crl_number']}, revoked={list(serials)}"
        )

    def _mint_link_client_cert(self, target, username, role="admin"):
        """
        An RBAC user on the link target plus a client certificate whose CN
        maps to it, for use as a link's clientCertificate/clientKey.

        Returns:
            dict: {"cert_pem", "key_pem", "serial", "username"}

        `admin` by default, and deliberately: revocation is enforced before
        identity mapping and RBAC, so the role cannot change the revoked
        outcome, while an under-privileged user could easily break the
        positive baseline and make the test look like a revocation failure.
        Section 10's own bullet on non-admin users is a separate scenario.
        """
        self._create_rbac_test_user(
            username, role, master=target["cluster"].master,
            record_into=target["users"],
        )
        cert, key, serial = self.crl_utils.generate_leaf_cert(
            target["ca_cert"], target["ca_key"], username
        )
        return {
            "cert_pem": self.crl_utils.cert_to_pem(cert),
            "key_pem": self.crl_utils.key_to_pem(key),
            "serial": serial,
            "username": username,
        }

    def _couchbase_link_props(self, name, target, client=None, hostname=None,
                              dataverse="Default", username=None,
                              password=None, encryption=None):
        """
        Link properties for a remote Couchbase link.

        With `client` (from _mint_link_client_cert) this is the mTLS form
        section 3 is about: encryption=full, the target's CA as `certificate`,
        and the client key pair, with no username or password. With
        `username`/`password` instead it is the credential form, used only as
        a contrast case for the error-distinguishability bullet.
        """
        props = {
            "name": name,
            "dataverse": dataverse,
            "scope": dataverse,
            "type": "couchbase",
            "hostname": hostname or target["cluster"].master.ip,
            "encryption": encryption or self.LINK_ENCRYPTION_FULL,
        }
        if client:
            props["certificate"] = target["ca_pem"].decode()
            props["clientCertificate"] = client["cert_pem"].decode()
            props["clientKey"] = client["key_pem"].decode()
        if username:
            props["username"] = username
            props["password"] = password
            if props["encryption"] == self.LINK_ENCRYPTION_FULL:
                props["certificate"] = target["ca_pem"].decode()
        return props

    def _link_url(self, dataverse, name, node=None):
        node = node or self.cluster.cbas_cc_node
        if CbServer.use_https:
            base = f"https://{node.ip}:{self.ANALYTICS_SSL_PORT}"
        else:
            base = f"http://{node.ip}:{self.ANALYTICS_PORT}"
        return (f"{base}/analytics/link/{quote_plus(dataverse)}"
                f"/{quote_plus(name)}")

    def _link_rest(self, method, props, timeout=180):
        """
        Create (POST) or alter (PUT) a link, returning the raw outcome.

        Bypasses CbasUtil deliberately. create_link returns a bare boolean and
        update_external_link_properties swallows the body, but three of
        section 3's bullets are assertions ABOUT the error text -- that it
        names revocation rather than a timeout, that creation is refused
        rather than silently deferred, that no key material appears in it --
        so the body has to survive the call.

        Returns:
            tuple: (ok: bool, status_code: int|None, body: str)
        """
        body = dict(props)
        dataverse = body.pop("dataverse", "Default")
        name = body.pop("name")
        payload = {k: v for k, v in body.items() if v}
        try:
            resp = requests.request(
                method,
                self._link_url(dataverse, name),
                data=urlencode(payload),
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                auth=(self.cluster.master.rest_username,
                      self.cluster.master.rest_password),
                verify=False, timeout=timeout,
            )
        except requests.exceptions.RequestException as exc:
            return False, None, str(exc)
        return resp.status_code in (200, 201, 202), resp.status_code, resp.text

    def _connect_link(self, name, dataverse="Default", timeout=180):
        """
        CONNECT LINK, returning (ok, error_text).

        Uses the statement path rather than cbas_util.connect_link because the
        latter reduces the outcome to a boolean, and the error text is the
        assertion for half of section 3.
        """
        statement = f"connect link {dataverse}.`{name}`;"
        status, _, errors, _, _ = (
            self.cbas_util.execute_statement_on_cbas_util(
                self.cluster, statement, timeout=timeout,
                analytics_timeout=timeout)
        )
        text = json.dumps(errors) if errors else ""
        return status == "success", text

    def _disconnect_link(self, name, dataverse="Default", timeout=180):
        """DISCONNECT LINK, tolerating an already-disconnected link."""
        statement = f"disconnect link {dataverse}.`{name}`;"
        status, _, errors, _, _ = (
            self.cbas_util.execute_statement_on_cbas_util(
                self.cluster, statement, timeout=timeout,
                analytics_timeout=timeout)
        )
        return status == "success", json.dumps(errors) if errors else ""

    def _assert_link_error_kind(self, text, kind, msg, redact=()):
        """
        Assert a link error names `kind` ("revoked", "unreachable" or
        "credentials") and, for the two non-revocation kinds, that it does NOT
        read as a revocation.

        Asymmetric on purpose. What section 3 is protecting against is a
        revocation failure that reports as a network or credential problem,
        and vice versa -- an operator who cannot tell those apart will chase
        the wrong cause. Requiring the revocation markers to be absent from
        every other failure is the sharp half of that; requiring generic
        transport words to be absent from a revocation failure is not, since
        a refused handshake legitimately mentions the connection.

        Matching is on WORD BOUNDARIES, not substrings, and the caller's own
        identifiers are redacted first. Both guard the same trap: every
        object this suite creates is named `crl_link_...`, so a substring
        search for "crl" matches the link's own name in any error that quotes
        it. That produced a false failure here -- a perfectly clear
        "Connect timed out" for an unroutable host was reported as reading
        like a revocation, purely because the link was called
        crl_link_err_unreachable. The same shape of bug (a marker matching
        the test's own generated string rather than the server's message)
        has now bitten this work twice, once as a false PASS.
        """
        redacted = text or ""
        for token in (redact or ()):
            if token:
                redacted = re.sub(re.escape(str(token)), " ", redacted,
                                  flags=re.IGNORECASE)
        lowered = redacted.lower()

        def names(marker):
            # \b against an underscore does not fire, so a marker cannot
            # match a fragment of a snake_case identifier.
            return re.search(rf"\b{re.escape(marker)}\b", lowered) is not None

        expected = {
            "revoked": self.LINK_REVOKED_MARKERS,
            "unreachable": self.LINK_UNREACHABLE_MARKERS,
            "credentials": self.LINK_CREDENTIAL_MARKERS,
        }[kind]
        hit = [marker for marker in expected if names(marker)]
        self.assertTrue(
            hit,
            f"{msg}. The error names none of the {kind} markers "
            f"{list(expected)}; it read: {text[:600]}"
        )
        if kind != "revoked":
            revoked_hit = [m for m in self.LINK_REVOKED_MARKERS if names(m)]
            self.assertFalse(
                revoked_hit,
                f"{msg}. A {kind} failure must not read as a revocation, but "
                f"the error carries {revoked_hit}: {text[:600]}"
            )
        self.log.info(f"Link error correctly reads as {kind} (matched {hit})")

    def _assert_no_key_material(self, blobs, client, where):
        """
        Assert the link's PRIVATE KEY does not appear in `blobs` (an iterable
        of strings), and record whether its certificate does.

        Checks a slice of the actual base64 body as well as the armour: a
        service that strips the BEGIN/END lines while still logging the
        payload would pass an armour-only check while having leaked the key.

        The private key is the assertion; the certificate is an observation.
        See CERT_MATERIAL_MARKERS for why.
        """
        # A middle slice, not the head: the first base64 line of a PKCS#8 key
        # is largely a fixed algorithm prefix shared by every RSA key, so
        # matching on it would risk a false positive against unrelated PEM.
        def body_slice(pem_bytes):
            lines = [ln for ln in pem_bytes.decode().splitlines()
                     if ln and not ln.startswith("-----")]
            joined = "".join(lines)
            return joined[len(joined) // 3:][:48] if len(joined) > 96 else None

        needles = [m for m in self.KEY_MATERIAL_MARKERS]
        key_chunk = body_slice(client["key_pem"])
        if key_chunk:
            needles.append(key_chunk.lower())

        # Observation only, so a public certificate in the logs is reported
        # rather than failed on.
        cert_chunk = body_slice(client["cert_pem"])
        for blob in blobs:
            lowered = (blob or "").lower()
            cert_hit = [m for m in self.CERT_MATERIAL_MARKERS
                        if m in lowered]
            if cert_chunk and cert_chunk.lower() in lowered:
                cert_hit.append("certificate body")
            if cert_hit:
                self.log.info(
                    f"{where}: contains the link's CERTIFICATE "
                    f"({cert_hit}). Not treated as a leak -- a certificate "
                    f"is public and is sent in the clear on every handshake. "
                    f"The private-key assertion below is the one that counts."
                )
                break

        for blob in blobs:
            lowered = (blob or "").lower()
            for needle in needles:
                self.assertNotIn(
                    needle, lowered,
                    f"{where} exposes the link's PRIVATE KEY "
                    f"(matched {needle[:24]!r}...). Section 3 requires link "
                    f"key material to stay out of logs, audit events and "
                    f"diagnostic output even when a revocation failure is "
                    f"being reported. Note this is the private key, not the "
                    f"certificate -- a certificate in the logs is expected "
                    f"and is not what this asserts."
                )
        self.log.info(f"{where}: no link private-key material present")

    def _cleanup_remote_targets(self):
        """
        Unwind every remote link target, in the reverse of the order
        _setup_remote_link_target built it.

        Node certificates go back BEFORE the CA is untrusted -- see
        _restore_self_signed_node_certs for what happens otherwise -- and the
        CRL policy is relaxed before either, so a target left in Require with
        its CRL already deleted cannot wall out its own cleanup.
        """
        for target in self._remote_targets:
            label = target["name"]
            if target["crl_policy"]:
                try:
                    self._reset_crl_settings(rest=target["rest"])
                except Exception as exc:
                    self.log.warning(f"{label} CRL settings reset: {exc}")
            for filename in target["files"]:
                try:
                    self.crl_utils.delete_file(target["rest"], filename)
                except Exception as exc:
                    self.log.warning(f"{label} CRL file {filename}: {exc}")
            target["files"] = []
            if target["cert_auth"]:
                try:
                    self._disable_client_cert_auth(
                        master=target["cluster"].master)
                except Exception as exc:
                    self.log.warning(f"{label} clientCertAuth disable: {exc}")
                target["cert_auth"] = False
            try:
                self._restore_self_signed_node_certs(target)
            except Exception as exc:
                self.log.warning(f"{label} node cert restore: {exc}")
            for username in target["users"]:
                try:
                    target["rest"].delete_builtin_user(username)
                except Exception as exc:
                    self.log.warning(f"{label} user {username}: {exc}")
            target["users"] = []
            self._delete_remote_bucket(target)
            if target["ca_ids"]:
                try:
                    self._untrust_ca_ids(
                        target["cluster"].master, target["ca_ids"])
                except Exception as exc:
                    self.log.warning(f"{label} trusted CA cleanup: {exc}")
                target["ca_ids"] = []
        self._remote_targets = []

