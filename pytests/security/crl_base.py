import json
import os
import re
import tempfile

import requests
from cryptography.x509.oid import NameOID

from cb_server_rest_util.security.security_api import SecurityRestAPI
from membase.api.rest_client import RestConnection
from shell_util.remote_connection import RemoteMachineShellConnection

from couchbase_utils.rbac_utils.Rbac_ready_functions import RbacUtils
from couchbase_utils.security_utils.crl_utils import CRLUtils
from couchbase_utils.security_utils.x509main import x509main
from pytests.onPrem_basetestcase import ClusterSetup
from TestInput import TestInputSingleton


class CRLBase(ClusterSetup):
    """
    Base class for CRL (Certificate Revocation List) tests against Couchbase
    Server Enterprise.
    """

    def setUp(self):
        self._self_heal_stuck_client_cert_auth()
        super().setUp()

        self.crl_utils = CRLUtils(log=self.log)
        self.rest = RestConnection(self.cluster.master)

        self._require_crl_supported()
        self._self_heal_stuck_trusted_cas()

        # Uploaded CRL filenames created during a test — cleaned up in tearDown
        self._created_files = []
        # RBAC users created during a test — cleaned up in tearDown
        self._rbac_users = []
        # Trusted CA ids uploaded during a test — cleaned up in tearDown
        self._trusted_ca_ids = []
        # Temp PEM files written for mTLS handshake helpers — cleaned up in tearDown
        self._temp_pem_files = []

        self.ca_cert, self.ca_key = self.crl_utils.generate_ca("TestCA1")
        self._trust_ca_on_cluster(self.ca_cert)

    def tearDown(self):
        try:
            self._cleanup_created_files()
        except Exception as exc:
            self.log.warning(f"CRL file cleanup error: {exc}")
        try:
            self._reset_crl_settings()
        except Exception as exc:
            self.log.warning(f"CRL settings reset error: {exc}")
        try:
            self._disable_client_cert_auth()
        except Exception as exc:
            self.log.warning(f"clientCertAuth disable error: {exc}")
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

    # ── Self-healing precondition ───────────────────────────────────────────

    def _self_heal_stuck_client_cert_auth(self):
        """
        Resets 'clientCertAuth' to 'disable' if a previously aborted test left 
        it in 'mandatory' mode. 

        If left in 'mandatory' mode, all subsequent HTTPS REST calls (including 
        the test framework's own setUp) will fail with a TLS "certificate required" 
        error. This method uses the plain HTTP port (8091) to bypass the TLS layer 
        and clear the setting safely.

        Notes:
            - Executes before super().setUp(), meaning self.cluster and self.log 
              are not yet initialized. Uses TestInputSingleton and standard print().
            - Execution is best-effort. Exceptions are caught and ignored so that 
              genuine node-down errors surface naturally during the real setUp().
        """
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
                    f"[CRLBase] {server.ip} was stuck with clientCertAuth='mandatory'. "
                    f"Resetting to 'disable' via HTTP before setUp()."
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
        """Untrusts every CA except the node's auto-generated one, clearing
        both the chronicle `ca_certificates` key and the inbox/CA directory
        on disk, before this test trusts its own fresh CA. Must run after
        super().setUp() since it needs diag/eval. Best-effort: logs, doesn't
        raise."""
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

    # ── EE / compat gating ───────────────────────────────────────────────────

    def _require_crl_supported(self):
        """Fail immediately if the cluster can't run CRL tests — EE-only per
        CRL_API_Contract.md's assert_supported/0 gate. Compat version isn't
        checked here; this suite assumes it always runs against Totoro+
        (8.1+) clusters."""
        if not self.cluster_util.is_enterprise_edition(self.cluster):
            self.fail("CRL support requires an Enterprise Edition cluster.")

    # ── CA trust setup ───────────────────────────────────────────────────────

    @staticmethod
    def _ca_dir(shell):
        """Returns the OS-appropriate inbox/CA path for the connected shell's host."""
        os_type = shell.extract_remote_info().distribution_type
        if os_type == "windows":
            install_path = x509main.WININSTALLPATH
        elif os_type == "Mac":
            install_path = x509main.MACINSTALLPATH
        else:
            install_path = x509main.LININSTALLPATH
        return f"{install_path}{x509main.CHAINFILEPATH}/CA"

    def _trust_ca_on_cluster(self, ca_cert, server=None):
        """
        Write ca_cert's PEM into the node's inbox/CA folder and instruct the
        cluster to load it (POST /node/controller/loadTrustedCAs), mirroring
        x509main._upload_cluster_ca_certificate but for an in-memory-generated
        CA rather than one already on disk from an x509main._generate_cert call.

        Each CA gets its own remote filename, derived from its CN plus serial
        number -- reusing a single fixed filename across calls would let a
        second _trust_ca_on_cluster() call silently overwrite (and thus
        un-trust) a CA a test already trusted, e.g. tests that need more than
        one simultaneously-trusted CA to check CRL-scope isolation.
        """
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

        # Track this CA's id (matched by CN) for teardown cleanup.
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
        """Unique-per-CA remote filename: sanitized CN + serial number, so
        distinct CAs trusted in the same test never collide on disk."""
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
        """
        Reset every /settings/crl field back to its documented default, not
        just policyPerScope. Found the hard way: a test that configures
        `urls`/`urlPollIntervalMs` (e.g. test_crl_url_poll_ingestion) left
        the cluster polling a now-dead URL every few seconds indefinitely
        after its own HTTP server was torn down, since only resetting
        policyPerScope left `urls` still pointed at it -- generating
        continuous "unexpected HTTP status 404" warnings on the node with
        no test still running to explain them.
        """
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
        """
        Disables client certificate authentication (clientCertAuth) on the cluster.

        Notes:
            - This call intentionally uses plain HTTP (port 8091) instead of HTTPS. 
              If the node was left in 'mandatory' mTLS mode, using HTTPS would 
              cause this reset call to be locked out by the very state it is trying 
              to clear.
        """
        server = self.cluster.master
        requests.post(
            f"http://{server.ip}:8091/settings/clientCertAuth",
            auth=(server.rest_username, server.rest_password),
            headers={"Content-Type": "application/json"},
            json={"state": "disable", "prefixes": []},
            timeout=30,
        )

    def _enable_client_cert_auth(self, state="enable", prefixes=None):
        """
        Enables client certificate authentication on the cluster.

        Args:
            state (str): 'enable' (optional mTLS) or 'mandatory' (strict mTLS). Defaults to 'enable'.
            prefixes (list): RBAC identity mapping rules. Defaults to matching any Common Name (CN).

        Notes:
            - Using state="mandatory" forces a client certificate on every TLS connection. 
              This will lock out standard username/password-based admin REST calls 
              for the remainder of the test unless explicitly reverted.
        """
        if prefixes is None:
            prefixes = [{"path": "subject.cn", "prefix": "", "delimiter": ""}]
        
        status, content, _ = SecurityRestAPI(
            self.cluster.master
        ).set_client_cert_auth_config(state=state, prefixes=prefixes)
        
        self.assertTrue(status, f"Failed to enable clientCertAuth: {content}")

    def _write_temp_pem(self, pem_bytes, suffix=".pem"):
        """
        Writes in-memory PEM bytes to a tracked temporary file on disk.

        Args:
            pem_bytes (bytes): The certificate or key data to write.
            suffix (str): The file extension. Defaults to ".pem".

        Returns:
            str: The absolute filesystem path to the created temporary file.
        """
        with tempfile.NamedTemporaryFile(
            delete=False, suffix=suffix, mode="wb"
        ) as tmp_file:
            tmp_file.write(pem_bytes)
            path = tmp_file.name
            
        self._temp_pem_files.append(path)
        return path

    def _cleanup_temp_pem_files(self):
        """
        Deletes all tracked temporary PEM files created during the test run.
        File deletion failures are logged as warnings rather than raising exceptions.
        """
        for path in self._temp_pem_files:
            try:
                os.remove(path)
            except OSError as exc:
                self.log.warning(f"Failed to remove temp PEM file {path}: {exc}")
                
        self._temp_pem_files = []

    def _cleanup_trusted_cas(self):
        """Untrusts every CA this test trusted via _trust_ca_on_cluster,
        via a chronicle_kv edit (no REST endpoint exists for this).
        Best-effort: logs, doesn't raise."""
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

    def _grant_rbac_role(self, username, role, password="Couchbase@1234"):
        """Changes an already-created test user's role in place (e.g. to
        test a live downgrade taking effect). Unlike _create_rbac_test_user,
        doesn't re-track the username -- it's assumed to already be tracked
        from the original _create_rbac_test_user call."""
        RbacUtils(self.cluster.master)._create_user_and_grant_role(
            username, role, password=password
        )

    def _cleanup_rbac_users(self):
        for username in self._rbac_users:
            try:
                self.rest.delete_builtin_user(username)
            except Exception as exc:
                self.log.warning(f"Failed to delete RBAC user {username}: {exc}")
        self._rbac_users = []
