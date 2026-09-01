import base64
import json
import os
import re
import socket
import ssl
import tempfile
import time
import traceback
import uuid

import requests
from cryptography import x509
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

from cb_constants import CbServer
from cb_server_rest_util.security.security_api import SecurityRestAPI
from couchbase_utils.backup_utils.backup_utils import BackupMgrUtil
from couchbase_utils.cloud_provider_utils.cloud_provider_interface import \
    CloudOperationError
from couchbase_utils.cloud_provider_utils.localstack_provider import \
    LocalstackProvider
from couchbase_utils.rbac_utils.Rbac_ready_functions import RbacUtils
from couchbase_utils.security_utils.crl_utils import CRLUtils
from couchbase_utils.security_utils.x509main import x509main
from membase.api.rest_client import RestConnection
from pytests.bucket_collections.collections_base import CollectionBase
from shell_util.remote_connection import RemoteMachineShellConnection
from TestInput import TestInputSingleton


class CRLBackupRestoreBase(CollectionBase):
    """
    Base class for CRL (Certificate Revocation List) enforcement tests on
    backup/restore's own TLS paths -- the Backup Service REST API (18097)
    and cbbackupmgr's peer-certificate verification against the cluster.
    See the CRL_Backup_Restore_TestPlan doc: this covers the P0 scenarios
    that don't need a multi-node backup-service gRPC topology (leader/
    follower node-cert revocation), a long-running backup revoked mid-flight,
    or object-store credentials -- those are a deferred follow-up.

    Deliberately independent of pytests/security/crl_base.py's CRLBase:
    that one inherits OnPremBaseTest, but backup_restore tests must inherit
    CollectionBase (see pytests/backup_restore AGENTS.md), so the CRL/mTLS
    fixture helpers below are kept self-contained rather than forcing a
    shared ancestor across two otherwise-unrelated base classes.
    """

    BACKUP_SERVICE_PORT = CbServer.ssl_backup_port  # 18097

    # cbbs's internal gRPC listener. The leader invokes gRPC on other backup
    # nodes here, and a locally spawned cbbackupmgr calls the verify-peer-cert
    # method on it.
    GRPC_PORT = 9124

    # Plain-HTTP Backup Service port. Used for the cross-node probes below so
    # they exercise the OUTBOUND (nodeToNode) path without dragging inbound
    # clientAuth/mTLS into the picture.
    BACKUP_SERVICE_HTTP_PORT = 8097

    # Enough HTTP/2 to make a gRPC listener answer: the mandatory connection
    # preface followed by an empty SETTINGS frame. A server that is willing to
    # talk replies with its own SETTINGS frame; one that has rejected the
    # connection sends a TLS alert or hangs up instead.
    H2_PREFACE = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
    H2_EMPTY_SETTINGS = bytes([0, 0, 0, 4, 0, 0, 0, 0, 0])

    def setUp(self):
        self._self_heal_stuck_client_cert_auth()
        super().setUp()

        self.crl_utils = CRLUtils(log=self.log)
        self.rest = RestConnection(self.cluster.master)

        self._require_crl_supported()
        self._require_backup_service()
        self._self_heal_stuck_trusted_cas()

        # Uploaded CRL filenames created during a test — cleaned up in tearDown
        self._created_files = []
        # RBAC users created during a test — cleaned up in tearDown
        self._rbac_users = []
        # Trusted CA ids uploaded during a test — cleaned up in tearDown
        self._trusted_ca_ids = []
        # Temp PEM files written for mTLS handshake helpers — cleaned up in tearDown
        self._temp_pem_files = []
        # Backup archive dirs created on self.backup_node — cleaned up in tearDown
        self._archive_dirs = []
        # PEM files copied onto self.backup_node for cbbackupmgr's
        # --client-cert/--client-key — cleaned up in tearDown
        self._remote_pem_files = []
        # Object-store archives / staging dirs (P0-14) — cleaned up in tearDown
        self._obj_archives = []
        self._obj_staging_dirs = []
        # Whether the crlsValidate counting iptables rule is currently installed
        self._crls_counter_installed = False
        # Nodes whose certificate this test reissued from the test CA --
        # every one has to go back to a built-in self-signed cert in
        # tearDown, or a test-CA-issued (possibly revoked) node cert
        # outlives the test and breaks every later test on these machines.
        self._nodes_with_test_certs = []

        self.backup_node = self.cluster.backup_nodes[0]
        # Every BackupMgrUtil opens an SSH shell via CbCmdBase and holds it
        # for its lifetime. Nothing closes it implicitly, so each one built
        # here or in the factories below is registered and disconnected in
        # tearDown -- otherwise the framework's connect/disconnect accounting
        # reports a leak ("Shell disconnection mismatch") on every test.
        self._backup_mgrs = []
        self.backup_mgr = self._track_backup_mgr(BackupMgrUtil(self.backup_node))

        # Unique CN per test, NOT a constant one. CRLs are matched to a
        # certificate by issuer NAME, so a CRL or trusted CA left behind by a
        # previous test whose teardown could not complete collides with a fresh
        # CA carrying the same CN but a different key. The validator then
        # rejects that stale CRL against the new key with `invalid_signature`,
        # revocation status becomes 'undetermined', and everything fails closed
        # under Require -- a failure that looks like a product bug and is not.
        # Observed exactly that in the Analytics suite, which had this same
        # constant-CN pattern: 11 trusted CAs all named CN=AnalyticsCRLTestCA1,
        # one of whose stale CRLs poisoned the following test.
        self.ca_cert, self.ca_key = self.crl_utils.generate_ca(
            f"BackupCRLTestCA_{uuid.uuid4().hex[:8]}"
        )
        self._trust_ca_on_cluster(self.ca_cert)

    def tearDown(self):
        # Log any failure's traceback FIRST, before the cleanup below and
        # before onPrem_basetestcase's multi-minute per-node coredump scan.
        # testrunner only prints the failure block once the whole test
        # (including that scan) has finished, which on a 3-node cluster is
        # ~10 minutes of not knowing whether a run failed or why. Logging it
        # here makes a failing run triageable while it is still executing.
        self._log_test_failure()

        # _disable_client_cert_auth() goes over plain HTTP (port 8091)
        # specifically so it works even if a test left clientCertAuth in
        # 'mandatory' -- every other cleanup step below goes over HTTPS with
        # no client cert attached, so it MUST run first. Otherwise a test
        # that leaked 'mandatory' state walls out this class's own self.rest
        # (and every other HTTPS call in tearDown), and each one burns
        # several minutes retrying before giving up with
        # ServerUnavailableException.
        try:
            self._disable_client_cert_auth()
        except Exception as exc:
            self.log.warning(f"clientCertAuth disable error: {exc}")
        # Before anything else that talks to the node: an iptables rule left
        # behind would silently skew every later test's crlsValidate counts.
        try:
            self._crls_validate_counter_stop()
        except Exception as exc:
            self.log.warning(f"crlsValidate counter removal error: {exc}")
        try:
            self._cleanup_archive_dirs()
        except Exception as exc:
            self.log.warning(f"Archive dir cleanup error: {exc}")
        try:
            self._cleanup_object_store()
        except Exception as exc:
            self.log.warning(f"Object-store cleanup error: {exc}")
        try:
            self._cleanup_remote_pem_files()
        except Exception as exc:
            self.log.warning(f"Remote PEM file cleanup error: {exc}")
        try:
            self._cleanup_created_files()
        except Exception as exc:
            self.log.warning(f"CRL file cleanup error: {exc}")
        try:
            self._reset_crl_settings()
        except Exception as exc:
            self.log.warning(f"CRL settings reset error: {exc}")
        try:
            self._cleanup_rbac_users()
        except Exception as exc:
            self.log.warning(f"RBAC user cleanup error: {exc}")
        try:
            self._cleanup_temp_pem_files()
        except Exception as exc:
            self.log.warning(f"Temp PEM file cleanup error: {exc}")
        # Restore node certificates BEFORE untrusting this suite's CAs, and
        # not the other way round.
        #
        # Cleaning the trust store first looks tempting but strands the
        # cluster: while a node still serves a test-CA certificate, removing
        # that CA breaks node-to-node TLS, and the regenerate that was
        # supposed to recover is itself a chronicle write that then cannot
        # complete. Observed exactly that -- a run left every node on
        # CN=BackupCRLTestCA_5dd09f73 with the CA already gone from the trust
        # store, needing a manual regenerate to recover.
        #
        # This order is safe because _cleanup_trusted_cas removes only the
        # ids this test tracked, so it never touches the CA that regeneration
        # just installed. (The helper that DID delete the cluster's own CA
        # was _self_heal_stuck_trusted_cas, via "keep only id 0"; that is
        # fixed separately by matching on subject.)
        try:
            self._restore_self_signed_node_certs()
        except Exception as exc:
            self.log.warning(f"Node certificate restore error: {exc}")
        try:
            self._cleanup_trusted_cas()
        except Exception as exc:
            self.log.warning(f"Trusted CA cleanup error: {exc}")
        # Last, and after every cleanup step above that may still issue
        # commands through a BackupMgrUtil (archive/object-store cleanup).
        try:
            self._disconnect_backup_mgrs()
        except Exception as exc:
            self.log.warning(f"BackupMgrUtil cleanup error: {exc}")
        finally:
            super().tearDown()

    def _log_test_failure(self):
        """
        Emit the current test's failure traceback into the test log, if it
        failed. Reads unittest's own _outcome.errors -- the same structure
        cb_basetest.is_test_failed() checks -- so it reports exactly what
        testrunner will later report, just sooner.

        Deliberately best-effort: _outcome is a unittest internal whose
        shape changed after Python 3.10 (`errors` gave way to `result`), so
        every access is guarded and a miss simply logs nothing rather than
        masking the real failure with an error from the logger itself.
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

    # ── Self-healing preconditions ──────────────────────────────────────────
    # Mirrors pytests/security/crl_base.py's CRLBase -- see that file for the
    # rationale (an aborted prior test can leave clientCertAuth='mandatory'
    # or a stale trusted CA behind, locking out this test's own setUp).

    def _self_heal_stuck_client_cert_auth(self):
        server = TestInputSingleton.input.servers[0]
        base_url = f"http://{server.ip}:8091"
        auth = (server.rest_username, server.rest_password)

        try:
            resp = requests.get(
                f"{base_url}/settings/clientCertAuth", auth=auth, timeout=30
            )
            resp.raise_for_status()

            if resp.json().get("state") == "mandatory":
                print(
                    f"[CRLBackupRestoreBase] {server.ip} was stuck with "
                    f"clientCertAuth='mandatory'. Resetting to 'disable' via "
                    f"HTTP before setUp()."
                )
                reset = requests.post(
                    f"{base_url}/settings/clientCertAuth", auth=auth, timeout=30,
                    headers={"Content-Type": "application/json"},
                    json={"state": "disable", "prefixes": []},
                )
                reset.raise_for_status()

        except requests.exceptions.RequestException:
            pass

    # Subject marker every CA this suite generates carries. Used to decide
    # what may be untrusted -- see _self_heal_stuck_trusted_cas.
    TEST_CA_SUBJECT_MARKER = "BackupCRLTestCA_"

    def _self_heal_stuck_trusted_cas(self):
        """
        Untrust CAs left behind by a previous run of THIS suite.

        Selected by subject, never by id. The earlier version kept only the
        CA at id 0, which looks equivalent and is not: after any
        regenerateCertificate the cluster's own generated CA is no longer id
        0, so "keep id 0" silently deleted the CA that signs every node's
        certificate. The cluster was then serving certificates signed by a CA
        it did not trust, which breaks node-to-node TLS -- and because
        chronicle's own inter-node traffic runs over it, the next chronicle
        write hangs. That surfaced as loadTrustedCAs returning HTTP 500 with
        chronicle_rsm:leader_request timing out underneath, and it cost
        several ten-minute runs plus a full node reset before the chain was
        traced back to here.
        """
        # Returns the number of CAs removed. Validated against a live 8.5.0
        # cluster: an earlier form computed the count as
        # "length(Certs) - length(NewCerts) + 1" and died with badarith.
        code = (
            "{ok, {Certs, _Rev}} = chronicle_kv:get(kv, ca_certificates), "
            "IsTestCA = fun(PL) -> case re:run("
            "proplists:get_value(subject, PL, \"\"), "
            f"\"{self.TEST_CA_SUBJECT_MARKER}\") of "
            "{match, _} -> true; _ -> false end end, "
            "NewCerts = lists:filter(fun(PL) -> not IsTestCA(PL) end, Certs), "
            "Removed = length(Certs) - length(NewCerts), "
            "chronicle_kv:set(kv, ca_certificates, NewCerts), "
            "Removed."
        )
        try:
            status, removed_count = self.rest.diag_eval(code)
            if not status:
                raise RuntimeError(f"diag/eval failed: {removed_count}")
            removed = int(removed_count)
            if removed > 0:
                self.log.warning(
                    f"{self.cluster.master.ip} had {removed} stale trusted "
                    f"CA(s) from a previous run of this suite -- untrusted "
                    f"them before this test starts. The cluster's own "
                    f"generated CA is deliberately left alone."
                )
        except Exception as exc:
            self.log.warning(f"Trusted CA self-heal error: {exc}")

        shell = RemoteMachineShellConnection(self.cluster.master)
        try:
            ca_dir = self._ca_dir(shell)
            shell.execute_command(f"rm -f {ca_dir}/*")
        except Exception as exc:
            self.log.warning(f"Trusted CA inbox/CA cleanup error: {exc}")
        finally:
            shell.disconnect()

    # ── EE / backup-service gating ───────────────────────────────────────────

    def _require_crl_supported(self):
        if not self.cluster_util.is_enterprise_edition(self.cluster):
            self.fail("CRL support requires an Enterprise Edition cluster.")

    def _require_backup_service(self):
        if not self.cluster.backup_nodes:
            self.fail(
                "CRL backup/restore tests need a Backup Service node -- "
                "pass backup_nodes or add 'backup' to services_init."
            )

    # ── CA trust setup (mirrors pytests/security/crl_base.py) ───────────────

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

    def _trust_ca_on_cluster(self, ca_cert, server=None):
        server = server or self.cluster.master
        pem_bytes = self.crl_utils.cert_to_pem(ca_cert)
        remote_filename = self._ca_remote_filename(ca_cert)

        shell = RemoteMachineShellConnection(server)
        try:
            ca_dir = self._ca_dir(shell)
            shell.execute_command(f"mkdir -p {ca_dir}")
            # A test whose teardown could not run leaves its CA behind here,
            # and ns_server loads every file in this directory. Clearing
            # first keeps one failed run from compounding into the next.
            shell.execute_command(f"rm -f {ca_dir}/BackupCRLTestCA_*.pem")
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

        # Retry rather than fail on the first refusal. loadTrustedCAs writes
        # to chronicle, and setUp calls this immediately after cluster init,
        # rebalance and a bucket load -- while chronicle can still be
        # settling. On a busy cluster it comes back "Unexpected server error,
        # request logged." (HTTP 500), and ns_server logs a no_quorum
        # activity failure at the same moment; the identical call against the
        # same idle cluster succeeds. Failing on the first attempt turned that
        # into a lost 10-minute run several times over.
        status, content = False, None
        for attempt in range(5):
            status, content = self.rest.load_trusted_CAs()
            if status:
                break
            self.log.warning(
                f"load_trusted_CAs attempt {attempt + 1}/5 on {server.ip} "
                f"failed ({content}); chronicle may still be settling")
            time.sleep(10)
        if not status:
            self.fail(
                f"Failed to load trusted CAs on {server.ip} after 5 "
                f"attempts: {content}")

        cn_attrs = ca_cert.subject.get_attributes_for_oid(NameOID.COMMON_NAME)
        cn = cn_attrs[0].value if cn_attrs else None
        try:
            trusted = json.loads(content)
            matching_ids = [
                entry.get("id") for entry in trusted
                if cn and cn in entry.get("subject", "")
            ]
            if matching_ids:
                self._trusted_ca_ids.append(max(matching_ids))
        except (ValueError, TypeError) as exc:
            self.log.warning(
                f"Could not identify trusted CA id for {cn!r} -- it won't be "
                f"auto-untrusted in tearDown: {exc}"
            )

    @staticmethod
    def _ca_remote_filename(ca_cert):
        cn_attrs = ca_cert.subject.get_attributes_for_oid(NameOID.COMMON_NAME)
        cn = cn_attrs[0].value if cn_attrs else "ca"
        safe_cn = re.sub(r"[^A-Za-z0-9_.-]", "_", cn)
        return f"{safe_cn}_{ca_cert.serial_number}.pem"

    # ── Cleanup helpers ──────────────────────────────────────────────────────

    def _track_uploaded_file(self, filename):
        self._created_files.append(filename)

    def _cleanup_created_files(self):
        for filename in self._created_files:
            status, _ = self.crl_utils.delete_file(self.rest, filename)
            if not status:
                self.log.warning(f"Failed to delete CRL file {filename} in teardown")
        self._created_files = []

    def _reset_crl_settings(self):
        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Disabled", "nodeToNode": "Disabled"},
            directory="/opt/couchbase/var/lib/couchbase/inbox/crls",
            dirPollIntervalMs=60000,
            checkIntermediateCerts=False,
            urls=[],
            urlPollIntervalMs=3600000,
        )

    def _disable_client_cert_auth(self):
        server = self.cluster.master
        requests.post(
            f"http://{server.ip}:8091/settings/clientCertAuth",
            auth=(server.rest_username, server.rest_password),
            headers={"Content-Type": "application/json"},
            json={"state": "disable", "prefixes": []},
            timeout=30,
        )

    def _enable_client_cert_auth(self, state="enable", prefixes=None):
        if prefixes is None:
            prefixes = [{"path": "subject.cn", "prefix": "", "delimiter": ""}]

        status, content, _ = SecurityRestAPI(
            self.cluster.master
        ).set_client_cert_auth_config(state=state, prefixes=prefixes)

        self.assertTrue(status, f"Failed to enable clientCertAuth: {content}")

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
            except OSError as exc:
                self.log.warning(f"Failed to remove temp PEM file {path}: {exc}")

        self._temp_pem_files = []

    def _cleanup_trusted_cas(self):
        if not self._trusted_ca_ids:
            return
        ids_literal = "[" + ",".join(str(i) for i in self._trusted_ca_ids) + "]"
        code = (
            "{ok, {Certs, _Rev}} = chronicle_kv:get(kv, ca_certificates), "
            f"Ids = {ids_literal}, "
            "NewCerts = lists:filter(fun(PL) -> "
            "not lists:member(proplists:get_value(id, PL), Ids) end, Certs), "
            "chronicle_kv:set(kv, ca_certificates, NewCerts)."
        )
        status, content = self.rest.diag_eval(code)
        if not status:
            self.log.warning(f"Trusted CA cleanup diag/eval failed: {content}")
        self._trusted_ca_ids = []

    def _create_rbac_test_user(self, username, role, password="Couchbase@1234"):
        rbac_utils = RbacUtils(self.cluster.master)
        rbac_utils._create_user_and_grant_role(username, role, password=password)
        self._rbac_users.append(username)
        return username, password

    def _cleanup_rbac_users(self):
        for username in self._rbac_users:
            try:
                self.rest.delete_builtin_user(username)
            except Exception as exc:
                self.log.warning(f"Failed to delete RBAC user {username}: {exc}")
        self._rbac_users = []

    # ── Backup-service specific helpers ──────────────────────────────────────

    def _backup_service_request(self, method, path, cert=None, auth=None,
                                timeout=30, **kwargs):
        """
        Direct HTTPS request against the Backup Service REST API (18097) on
        self.backup_node, optionally presenting a client cert for mTLS.

        Bypasses BackupRestApi/CBRestConnection deliberately -- like
        CRLUtils.perform_mtls_handshake, this needs a raw `requests` call so
        `cert=` can be passed straight through, and `verify=False` is
        required for the same reason as every other mTLS check in this
        suite: the node's self-signed server cert lacks CA:TRUE.
        """
        url = f"https://{self.backup_node.ip}:{self.BACKUP_SERVICE_PORT}{path}"
        return requests.request(
            method, url, cert=cert, auth=auth, verify=False, timeout=timeout,
            headers={"Connection": "close"}, **kwargs
        )

    # A ConnectionError only proves refusal if it carries one of these. Same
    # set the FTS CRL suite uses, so a rejection is identified consistently
    # across suites.
    TLS_REJECTION_MARKERS = (
        "certificate revoked", "tlsv1 alert", "sslv3 alert",
        "alert certificate", "handshake failure", "bad certificate",
        "unknown ca", "certificate unknown", "certificate required",
        "decrypt error",
    )

    def assert_cert_refused(self, request_fn, msg):
        """
        Assert a certificate was refused, in either of the two forms the PRD
        permits.

        The PRD's runtime-enforcement section says of the revocation check:
        "For Phase I. It is acceptable to return 401 instead of sending a tls
        alert to allow NS-server to use the callback." A test that insists on
        a TLS alert would therefore fail against a conformant implementation
        that answers 401 instead. cbbs sends alerts today, so these tests pass
        either way -- accepting both keeps them from breaking on a legitimate
        change.

        Deliberately narrow about what counts as refusal:
          TLS-layer alert / connection error -> refused
          HTTP 401                           -> refused (the PRD's allowance)
          anything else, including 200/403/5xx -> failure

        403 is excluded because it means the certificate was accepted and then
        denied by RBAC, which is a different outcome from revocation. 500 is
        excluded because it is MB-73277, not a valid way to refuse.

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
            text = str(exc).lower()
            if not any(marker in text
                       for marker in self.TLS_REJECTION_MARKERS):
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

    def _wait_for_backup_service_ok(self, method, path, cert=None, auth=None,
                                    timeout_s=30, interval=2,
                                    expected_status=200, **kwargs):
        """
        Poll _backup_service_request until it returns `expected_status`,
        absorbing the brief propagation delay between a CRL/clientCertAuth
        change landing on self.cluster.master and cbbs -- a separate process,
        not necessarily co-located with master -- picking it up. Mirrors
        pytests/security/crl_test.py's _wait_until_handshake.

        Every positive check after a settings change should come through here
        rather than asserting on a single immediate request: the change is not
        synchronous, so an immediate assert races the propagation and fails
        intermittently for reasons unrelated to what it is testing.

        expected_status exists because a positive outcome is not always 200 --
        the enforcement-order test needs to wait for a 403 from RBAC.

        Raises the last SSLError, or returns the last unexpected response, if
        the deadline passes without seeing expected_status.
        """
        deadline = time.time() + timeout_s
        last_exc = None
        last_resp = None
        while time.time() < deadline:
            try:
                resp = self._backup_service_request(
                    method, path, cert=cert, auth=auth, **kwargs
                )
                if resp.status_code == expected_status:
                    return resp
                last_resp = resp
            except requests.exceptions.SSLError as exc:
                last_exc = exc
            time.sleep(interval)
        if last_resp is not None:
            return last_resp
        raise last_exc

    def _new_archive_dir(self, label):
        """A fresh, tracked cbbackupmgr archive directory on self.backup_node."""
        path = f"/tmp/crl_bkp_{label}_{uuid.uuid4().hex[:8]}"
        self._archive_dirs.append(path)
        return path

    def _read_backup_service_log(self, since_marker=None, tail_lines=4000):
        """
        Return backup_service.log lines from self.backup_node.

        Args:
            since_marker: if given, only lines at or after the LAST occurrence
                of this substring are returned -- lets a test scope its
                assertions to what happened after a point it created, rather
                than to whatever the node logged earlier today.
            tail_lines: cap, because a single validator outage can produce
                hundreds of thousands of lines (cbbs restarts its MetaKV
                observer with no backoff once cbauth goes stale), and reading
                an unbounded log over SSH is slow enough to look like a hang.

        Returns:
            list[str]
        """
        shell = RemoteMachineShellConnection(self.backup_node)
        try:
            output, _ = shell.execute_command(
                f"tail -n {int(tail_lines)} "
                f"/opt/couchbase/var/lib/couchbase/logs/backup_service.log "
                f"2>/dev/null || true"
            )
        finally:
            shell.disconnect()
        lines = [line.rstrip("\n") for line in (output or [])]
        if since_marker:
            for idx in range(len(lines) - 1, -1, -1):
                if since_marker in lines[idx]:
                    return lines[idx:]
        return lines

    def _track_backup_mgr(self, backup_mgr):
        """Register a BackupMgrUtil so tearDown closes its SSH shell."""
        self._backup_mgrs.append(backup_mgr)
        return backup_mgr

    def _disconnect_backup_mgrs(self):
        """Close the SSH shell held by every BackupMgrUtil this test built."""
        for backup_mgr in getattr(self, "_backup_mgrs", []):
            try:
                backup_mgr.disconnect()
            except Exception as exc:
                self.log.warning(f"BackupMgrUtil disconnect error: {exc}")
        self._backup_mgrs = []


    # ── Internal gRPC listener (9124) probes ────────────────────────────────

    # HTTP/2 frame types we care about, from the 4th byte of the 9-byte
    # frame header (3-byte length, 1-byte type, 1-byte flags, 4-byte stream).
    H2_FRAME_SETTINGS = 0x04
    H2_FRAME_GOAWAY = 0x07

    @classmethod
    def _classify_h2_reply(cls, data):
        """
        What the server's reply to our preface actually means.

        Reading "any bytes came back" as success is too loose: a listener that
        refuses the connection can still answer, and GOAWAY in particular is
        bytes on the wire that mean the opposite of a granted channel. Only a
        SETTINGS frame is the server agreeing to speak HTTP/2 with us.

        Returns "SETTINGS", "GOAWAY", "CLOSED" (nothing came back), "SHORT"
        (fewer bytes than a frame header) or "FRAME_0xNN" for anything else.
        """
        if not data:
            return "CLOSED"
        if len(data) < 9:
            return "SHORT"
        frame_type = data[3]
        if frame_type == cls.H2_FRAME_SETTINGS:
            return "SETTINGS"
        if frame_type == cls.H2_FRAME_GOAWAY:
            return "GOAWAY"
        return f"FRAME_0x{frame_type:02x}"

    def _grpc_channel_probe(self, cert=None, payload=None, timeout=20):
        """
        Open a TLS connection to cbbs's internal gRPC listener and report
        whether the server granted a usable HTTP/2 channel.

        Returns one of:
          "SETTINGS"    -- the server answered with an HTTP/2 SETTINGS frame,
                           i.e. it granted a usable channel
          "GOAWAY"      -- it answered, but to refuse the connection
          "CLOSED"      -- it hung up without answering
          "SHORT" /
          "FRAME_0xNN"  -- it answered with something else
          "<ExcName>"   -- the connection or handshake failed outright

        A raw socket rather than CRLUtils.perform_mtls_handshake: 9124 speaks
        gRPC over HTTP/2, so an HTTP/1 GET would prove nothing about it. And
        the write-then-read matters -- under TLS 1.3 the server sends its
        Finished before it has processed the client's certificate, so a
        rejected certificate surfaces on the first read rather than as a
        handshake failure. Probing the handshake alone would report every
        certificate as accepted.
        """
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        # Same reason as every other mTLS check in this suite: the node's
        # self-signed server cert lacks CA:TRUE, so verifying it would abort
        # locally before the server ever judged our client certificate.
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        if cert:
            context.load_cert_chain(certfile=cert[0], keyfile=cert[1])
        try:
            context.set_alpn_protocols(["h2"])
        except NotImplementedError:
            self.log.warning("ALPN unsupported locally; probing without it")

        if payload is None:
            payload = self.H2_PREFACE + self.H2_EMPTY_SETTINGS
        try:
            with socket.create_connection(
                    (self.backup_node.ip, self.GRPC_PORT), timeout=timeout
            ) as sock:
                with context.wrap_socket(sock) as tls:
                    tls.sendall(payload)
                    return self._classify_h2_reply(tls.recv(64))
        except (ssl.SSLError, socket.timeout, OSError) as exc:
            return type(exc).__name__

    def _backup_service_pid_and_fds(self):
        """
        (pid, open file descriptor count) for cbbs on self.backup_node, or
        (None, None) if it could not be read. Used to show that a probe left
        the service alive and did not leak descriptors.
        """
        shell = RemoteMachineShellConnection(self.backup_node)
        try:
            output, _ = shell.execute_command("pgrep -x backup | head -1")
            pid = next(
                (line.strip() for line in (output or []) if line.strip().isdigit()),
                None,
            )
            if pid is None:
                return None, None
            output, _ = shell.execute_command(
                f"ls /proc/{pid}/fd 2>/dev/null | wc -l")
            fds = next(
                (int(line.strip()) for line in (output or [])
                 if line.strip().isdigit()), None
            )
            return pid, fds
        finally:
            shell.disconnect()


    # ── Node certificates issued by the test CA ─────────────────────────────

    def _served_certificate(self, server, port=None):
        """
        The certificate a node actually serves on a TLS port, as an x509
        object. Reads what is on the wire rather than what the REST API says
        is configured -- after a reloadCertificate the two can disagree if
        the reload silently did nothing.
        """
        pem = ssl.get_server_certificate(
            (server.ip, port or CbServer.ssl_port))
        return x509.load_pem_x509_certificate(pem.encode())

    def _install_node_certificate(self, server, ca_cert, ca_key,
                                  chain_suffix=b""):
        """
        Reissue `server`'s own node certificate from the given CA and reload
        it, so that this node's certificate can later be revoked by a CRL
        this suite controls. Returns the new certificate's serial.

        Two details are load bearing. The certificate needs SERVER_AUTH as
        well as CLIENT_AUTH -- a node certificate is presented on inbound
        TLS, and cbbs also presents it outbound -- and its SANs must cover
        every address the cluster uses to reach the node, otherwise peers
        reject it on name mismatch long before revocation is consulted, and
        the failure looks like enforcement when it is not.

        `chain_suffix` is appended to the leaf in chain.pem, for a cert
        issued by an intermediate: the node has to serve the intermediate
        too, since the cluster only trusts the root.

        The CA must already be trusted cluster-wide (_trust_ca_on_cluster)
        or the reload is refused outright.
        """
        cert, key, serial = self.crl_utils.generate_leaf_cert(
            ca_cert, ca_key, server.ip,
            extended_key_usage=[ExtendedKeyUsageOID.SERVER_AUTH,
                                ExtendedKeyUsageOID.CLIENT_AUTH],
            dns_names=[server.ip, "127.0.0.1", "localhost"],
        )
        chain_pem = self.crl_utils.cert_to_pem(cert) + chain_suffix
        key_pem = self.crl_utils.key_to_pem(key)

        inbox = "/opt/couchbase/var/lib/couchbase/inbox"
        shell = RemoteMachineShellConnection(server)
        try:
            shell.execute_command(f"mkdir -p {inbox}")
            # base64 through a single echo rather than a heredoc: PEM is
            # multi-line and full of characters the remote shell would other-
            # wise mangle.
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
            raise Exception(
                f"reloadCertificate failed on {server.ip}: {content}")
        if server.ip not in self._nodes_with_test_certs:
            self._nodes_with_test_certs.append(server.ip)
        self.log.info(
            f"Node {server.ip} now serving a test-CA certificate, serial="
            f"{serial}")
        return serial

    def _restore_self_signed_node_certs(self):
        """
        Put every node back on a built-in self-signed certificate.

        Cluster-wide rather than per-node: regenerateCertificate reissues for
        the whole cluster in one call, and leaving even one node on a revoked
        test certificate would break the next test's cluster setup.
        """
        if not getattr(self, "_nodes_with_test_certs", []):
            return
        self.log.info(
            f"Restoring self-signed node certs (test certs were installed "
            f"on {self._nodes_with_test_certs})")
        RestConnection(self.cluster.master).regenerate_cluster_certificate()

        # Wait until every node is demonstrably off the test CA before the
        # caller goes on to drop that CA from the trust store. regenerate and
        # reloadCertificate both propagate asynchronously -- cbbs logs a "TLS
        # config refreshed" per node as it catches up -- so removing the trust
        # anchor while a node still presents a test-CA certificate leaves the
        # cluster unable to verify its own peers, which surfaces later as
        # "x509: certificate signed by unknown authority" on leader-to-
        # follower gRPC and fails the NEXT test's rebalance rather than this
        # one's teardown.
        ca_cn = self.ca_cert.subject.rfc4514_string().split(
            "CN=")[-1].split(",")[0]
        deadline = time.time() + 120
        for server in self.cluster.servers[:self.nodes_init]:
            if server.ip not in self._nodes_with_test_certs:
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
                    f"after 120s. Leaving the CA trusted rather than "
                    f"stranding the cluster without its trust anchor.")
                return

        # Regenerating swaps the ACTIVE certificate but leaves what was
        # staged in the inbox on disk. Left there, the next test's node-cert
        # work starts from another test's chain and key, so remove them.
        inbox = "/opt/couchbase/var/lib/couchbase/inbox"
        for server in self.cluster.servers[:self.nodes_init]:
            if server.ip not in self._nodes_with_test_certs:
                continue
            shell = RemoteMachineShellConnection(server)
            try:
                shell.execute_command(
                    f"rm -f {inbox}/chain.pem {inbox}/pkey.key")
            except Exception as exc:
                self.log.warning(
                    f"Inbox cleanup failed on {server.ip}: {exc}")
            finally:
                shell.disconnect()
        self._nodes_with_test_certs = []


    # ── Forcing leader-to-follower gRPC (section C / P0-07) ─────────────────

    def _create_backup_plan(self, node, plan_name):
        """
        Create a minimal backup plan on `node`, replacing any leftover of the
        same name. Needed because a repository cannot be created without a
        plan, and repository creation is what forces the cross-node gRPC
        round trip the section C tests measure.
        """
        base = (f"http://{node.ip}:{self.BACKUP_SERVICE_HTTP_PORT}"
                f"/api/v1/plan/{plan_name}")
        auth = (self.cluster.master.rest_username,
                self.cluster.master.rest_password)
        requests.delete(base, auth=auth, timeout=60)
        body = {
            "description": "CRL section C cross-node probe",
            "tasks": [{
                "name": "crl_probe_task",
                "task_type": "BACKUP",
                "schedule": {"job_type": "BACKUP", "frequency": 1,
                             "period": "HOURS"},
            }],
        }
        resp = requests.post(base, json=body, auth=auth, timeout=60)
        if resp.status_code != 200:
            raise Exception(
                f"Could not create probe plan {plan_name}: "
                f"{resp.status_code} {resp.text}")
        return plan_name

    def _delete_backup_plan(self, node, plan_name):
        """Best-effort removal of a probe plan."""
        try:
            requests.delete(
                f"http://{node.ip}:{self.BACKUP_SERVICE_HTTP_PORT}"
                f"/api/v1/plan/{plan_name}",
                auth=(self.cluster.master.rest_username,
                      self.cluster.master.rest_password),
                timeout=60)
        except Exception as exc:
            self.log.warning(f"Probe plan cleanup failed: {exc}")

    def _cross_node_archive_probe(self, node, plan_name, label):
        """
        Force a leader-to-follower gRPC round trip and report what came back.

        Creating a backup repository makes the service ask every other backup
        node to verify it can reach the archive location, over the internal
        gRPC channel. Pointed at a local (non-shared) path that check ALWAYS
        fails -- which is exactly the point. The error text says whether the
        peer was reached at all:

          * "could not read file ... no such file or directory" -- the gRPC
            channel worked and the peer answered; only the file was missing.
          * "authentication handshake failed" / "x509" / "certificate" -- the
            channel itself was refused.

        That distinction is what makes revocation on the outbound path
        observable from outside, without needing a shared NFS archive or
        object-store credentials just to get a repository created.

        Returns (status_code, body_text, elapsed_seconds).
        """
        url = (f"http://{node.ip}:{self.BACKUP_SERVICE_HTTP_PORT}"
               f"/api/v1/cluster/self/repository/active/{label}")
        start = time.time()
        try:
            resp = requests.post(
                url, json={"plan": plan_name, "archive": f"/tmp/{label}"},
                auth=(self.cluster.master.rest_username,
                      self.cluster.master.rest_password),
                timeout=300)
            return resp.status_code, resp.text, time.time() - start
        except requests.exceptions.RequestException as exc:
            return None, f"{type(exc).__name__}: {exc}", time.time() - start

    @staticmethod
    def _revoked_reason_in(text):
        """
        True if `text` blames REVOCATION specifically, as opposed to merely
        failing TLS.

        The distinction is the whole point of P0-07: under Require with no
        applicable CRL, cbauth answers "status undetermined" and the
        handshake is refused fail-closed. That is a TLS failure but it is not
        a revocation-specific reason, and a test that accepted it would pass
        without ever proving revocation was consulted.
        """
        lowered = (text or "").lower()
        # "is revoked" / "revocation" in a sentence, not the substring
        # "revoked" that a path like /tmp/crl_x_revoked would also satisfy.
        return "is revoked" in lowered or "revocation" in lowered

    @staticmethod
    def _undetermined_reason_in(text):
        """True if the peer's revocation status could not be determined."""
        return "undetermined" in (text or "").lower()

    @staticmethod
    def _tls_failure_in(text):
        """
        True if `text` blames the TLS transport.

        Deliberately does NOT include a bare "revoked" token. An earlier
        version did, and matched on the caller's own archive path
        (/tmp/crl_inter_revoked) rather than on any product error -- turning a
        test that should have failed into a silent pass. Every token here
        names something only a transport failure produces.
        """
        lowered = (text or "").lower()
        return any(token in lowered for token in (
            "x509", "authentication handshake failed", "tls:",
            "certificate signed by unknown authority", "bad certificate",
            "crlsvalidate"))

    @staticmethod
    def _file_level_failure_in(text):
        """True if `text` blames the archive file rather than the transport."""
        lowered = (text or "").lower()
        return any(token in lowered for token in (
            "could not read file", "no such file or directory",
            "cannot access location"))

    def _local_backup_mgr(self):
        """
        A BackupMgrUtil writing to a local (filesystem) archive on
        self.backup_node -- the reference leg for object-store comparisons,
        and separate from self.backup_mgr so a test can hold both at once.
        """
        return self._track_backup_mgr(BackupMgrUtil(self.backup_node))

    def _insecure_backup_mgr(self):
        """
        A BackupMgrUtil with --no-ssl-verify forced ON.

        Passed explicitly rather than inherited: CbBackupMgr derives the flag
        from CbServer.use_https, which is False unless a run enables TLS, so a
        test that merely hoped the flag was present would silently exercise
        the ordinary path instead.
        """
        return self._track_backup_mgr(
            BackupMgrUtil(self.backup_node, no_ssl_verify=True))

    # ── Object-store (P0-14) helpers ────────────────────────────────────────

    def _object_store_backup_mgr(self):
        """
        A BackupMgrUtil whose archive lives in an S3-compatible object store,
        for the scenarios that must prove object-store TLS is independent of
        cluster CRL policy.

        Uses LocalstackProvider, which is S3-compatible and therefore also
        drives a MinIO endpoint -- see LOCALSTACK_* env vars, including
        LOCALSTACK_CACERT for the CA path on self.backup_node. Fails the test
        (never skips) when the endpoint is not configured, so a missing
        fixture is visible rather than silently reducing coverage.

        Returns:
            tuple: (BackupMgrUtil, archive_uri, staging_dir)
        """
        if not os.getenv("LOCALSTACK_ENDPOINT"):
            self.fail(
                "Object-store CRL scenarios need an S3-compatible endpoint. "
                "Set LOCALSTACK_ENDPOINT / LOCALSTACK_ACCESS_KEY_ID / "
                "LOCALSTACK_SECRET_ACCESS_KEY, plus LOCALSTACK_CACERT (the CA "
                "path on the backup node) for an https endpoint."
            )
        bucket = os.getenv("LOCALSTACK_BUCKET", "p014-crl-test")
        try:
            provider = LocalstackProvider(log=self.log)
        except CloudOperationError as exc:
            self.fail(f"Object-store provider is misconfigured: {exc}")

        uniq = uuid.uuid4().hex[:8]
        archive = f"s3://{bucket}/crl_{uniq}"
        # cbbackupmgr rejects a staging dir already bound to a different
        # remote archive ("existing staging directory is for another remote
        # archive"), so each archive gets its own -- and it must not be /tmp,
        # which cbbackupmgr warns against.
        staging_dir = f"/data/crl_obj_staging_{uniq}"
        shell = RemoteMachineShellConnection(self.backup_node)
        try:
            shell.execute_command(f"mkdir -p {staging_dir}")
        finally:
            shell.disconnect()
        self._obj_staging_dirs.append(staging_dir)
        self._obj_archives.append(archive)

        backup_mgr = self._track_backup_mgr(BackupMgrUtil(
            self.backup_node, cloud_provider=provider,
            obj_staging_dir=staging_dir,
        ))
        return backup_mgr, archive, staging_dir

    def _cleanup_object_store(self):
        """Remove object-store archives and their staging dirs."""
        if self._obj_archives:
            try:
                provider = LocalstackProvider(log=self.log)
                for archive in self._obj_archives:
                    try:
                        provider.cleanup_for_bkrs(archive)
                    except Exception as exc:
                        self.log.warning(
                            f"Object-store cleanup failed for {archive}: {exc}")
            except CloudOperationError as exc:
                self.log.warning(f"Skipping object-store cleanup: {exc}")
            self._obj_archives = []
        if self._obj_staging_dirs:
            shell = RemoteMachineShellConnection(self.backup_node)
            try:
                for path in self._obj_staging_dirs:
                    shell.execute_command(f"rm -rf -- {path}")
            finally:
                shell.disconnect()
            self._obj_staging_dirs = []

    # ── crlsValidate call counting ──────────────────────────────────────────
    # cbauth posts revocation checks to the ns_server on the SAME node as the
    # calling service, over plain HTTP on loopback 8091 (verified: cbbs
    # dispatches every ns_server request to localhost:8091, and blocking only
    # that port reproduces a validator outage). A non-blocking iptables rule
    # matching the request path therefore yields an exact call count, which is
    # the only hard way to assert "makes zero crlsValidate calls" -- inferring
    # it from logs cannot distinguish "no call" from "call not logged".

    # Same match, two targets: ACCEPT to count without interfering, DROP to
    # make the validator unreachable while leaving every other cbbs ->
    # ns_server call working. Blocking the whole port instead would also cut
    # metakv/pools/auth, stalling unrelated requests and driving cbbs into a
    # log-flooding MetaKV restart loop.
    _CRLS_VALIDATE_MATCH = (
        "OUTPUT -o lo -p tcp --dport 8091 "
        "-m string --string crlsValidate --algo bm"
    )
    CRLS_VALIDATE_RULE = f"{_CRLS_VALIDATE_MATCH} -j ACCEPT"
    CRLS_VALIDATE_DROP_RULE = f"{_CRLS_VALIDATE_MATCH} -j DROP"

    def _crls_validate_counter_start(self):
        """Install the counting rule and return the current packet count."""
        shell = RemoteMachineShellConnection(self.backup_node)
        try:
            # Drop any rule left by an aborted earlier run so counts start
            # from a single known rule rather than several stacked ones.
            shell.execute_command(
                f"iptables -D {self.CRLS_VALIDATE_RULE} 2>/dev/null")
            shell.execute_command(f"iptables -I {self.CRLS_VALIDATE_RULE}")
            self._crls_counter_installed = True
        finally:
            shell.disconnect()
        return self._crls_validate_count()

    def _crls_validate_count(self):
        """Packets matched by the counting rule so far, or None if absent."""
        shell = RemoteMachineShellConnection(self.backup_node)
        try:
            # execute_command returns (stdout, stderr) in that order -- reading
            # the second element here silently yields stderr, which is empty,
            # and the count comes back None.
            output, _ = shell.execute_command(
                "iptables -L OUTPUT -v -n | grep crlsValidate "
                "| awk '{print $1}'"
            )
        finally:
            shell.disconnect()
        for line in (output or []):
            token = line.strip()
            if token.isdigit():
                return int(token)
        return None

    def _crls_validate_counter_stop(self):
        """Remove the counting rule. Safe to call when it was never added."""
        if not getattr(self, "_crls_counter_installed", False):
            return
        shell = RemoteMachineShellConnection(self.backup_node)
        try:
            shell.execute_command(
                f"iptables -D {self.CRLS_VALIDATE_RULE} 2>/dev/null")
        finally:
            shell.disconnect()
        self._crls_counter_installed = False

    def _cleanup_archive_dirs(self):
        if not self._archive_dirs:
            return
        shell = RemoteMachineShellConnection(self.backup_node)
        try:
            for path in self._archive_dirs:
                shell.execute_command(f"rm -rf -- {path}")
        finally:
            shell.disconnect()
        self._archive_dirs = []

    def _copy_pem_to_backup_node(self, local_path):
        """
        Copy a locally-generated PEM file onto self.backup_node's
        filesystem, for cbbackupmgr's --client-cert/--client-key.

        cbbackupmgr runs on self.backup_node over SSH -- a path from
        _write_temp_pem() only exists on the machine running the test
        process itself, so passing it straight into --client-cert/
        --client-key fails with a remote "no such file or directory" as
        soon as the server actually asks for the certificate.
        """
        remote_path = f"/tmp/{uuid.uuid4().hex[:12]}_{os.path.basename(local_path)}"
        shell = RemoteMachineShellConnection(self.backup_node)
        try:
            shell.copy_file_local_to_remote(local_path, remote_path)
        finally:
            shell.disconnect()
        self._remote_pem_files.append(remote_path)
        return remote_path

    def _cleanup_remote_pem_files(self):
        if not self._remote_pem_files:
            return
        shell = RemoteMachineShellConnection(self.backup_node)
        try:
            for path in self._remote_pem_files:
                shell.execute_command(f"rm -f -- {path}")
        finally:
            shell.disconnect()
        self._remote_pem_files = []
