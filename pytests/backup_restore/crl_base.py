import json
import os
import re
import tempfile
import time
import traceback
import uuid

import requests
from cryptography.x509.oid import NameOID

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

        self.backup_node = self.cluster.backup_nodes[0]
        self.backup_mgr = BackupMgrUtil(self.backup_node)

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
        try:
            self._cleanup_trusted_cas()
        except Exception as exc:
            self.log.warning(f"Trusted CA cleanup error: {exc}")
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

    def _self_heal_stuck_trusted_cas(self):
        code = (
            "{ok, {Certs, _Rev}} = chronicle_kv:get(kv, ca_certificates), "
            "NewCerts = lists:filter(fun(PL) -> "
            "proplists:get_value(id, PL) =:= 0 end, Certs), "
            "OldCount = length(Certs), "
            "chronicle_kv:set(kv, ca_certificates, NewCerts), "
            "OldCount."
        )
        try:
            status, old_count = self.rest.diag_eval(code)
            if not status:
                raise RuntimeError(f"diag/eval failed: {old_count}")
            removed = int(old_count) - 1
            if removed > 0:
                self.log.warning(
                    f"{self.cluster.master.ip} had {removed} stale trusted "
                    f"CA(s) left over from a previous run -- untrusted all "
                    f"of them (kept only the node's own auto-generated CA) "
                    f"before this test starts."
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

        status, content = self.rest.load_trusted_CAs()
        if not status:
            self.fail(f"Failed to load trusted CAs on {server.ip}: {content}")

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

    def _local_backup_mgr(self):
        """
        A BackupMgrUtil writing to a local (filesystem) archive on
        self.backup_node -- the reference leg for object-store comparisons,
        and separate from self.backup_mgr so a test can hold both at once.
        """
        return BackupMgrUtil(self.backup_node)

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

        backup_mgr = BackupMgrUtil(
            self.backup_node, cloud_provider=provider,
            obj_staging_dir=staging_dir,
        )
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
