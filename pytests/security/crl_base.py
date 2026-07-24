import os
import tempfile

from cb_server_rest_util.security.security_api import SecurityRestAPI
from membase.api.rest_client import RestConnection
from shell_util.remote_connection import RemoteMachineShellConnection

from couchbase_utils.rbac_utils.Rbac_ready_functions import RbacUtils
from couchbase_utils.security_utils.crl_utils import CRLUtils
from couchbase_utils.security_utils.x509main import x509main
from pytests.onPrem_basetestcase import OnPremBaseTest


class CRLBase(OnPremBaseTest):
    """
    Base class for CRL (Certificate Revocation List) tests against Couchbase
    Server Enterprise.
    """

    def setUp(self):
        super().setUp()

        self.crl_utils = CRLUtils(log=self.log)
        self.rest = RestConnection(self.cluster.master)

        self._require_crl_supported()

        # Uploaded CRL filenames created during a test — cleaned up in tearDown
        self._created_files = []
        # RBAC users created during a test — cleaned up in tearDown
        self._rbac_users = []
        # Trusted CA ids uploaded during a test — cleaned up in tearDown
        self._trusted_ca_ids = []

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
        finally:
            super().tearDown()

    # ── EE / compat gating ───────────────────────────────────────────────────

    def _require_crl_supported(self):
        """Fail immediately if the cluster can't run CRL tests — EE-only per
        CRL_API_Contract.md's assert_supported/0 gate. Compat version isn't
        checked here; this suite assumes it always runs against Totoro+
        (8.1+) clusters."""
        if not self.cluster_util.is_enterprise_edition(self.cluster):
            self.fail("CRL support requires an Enterprise Edition cluster.")

    # ── CA trust setup ───────────────────────────────────────────────────────

    def _trust_ca_on_cluster(self, ca_cert, server=None):
        """
        Write ca_cert's PEM into the node's inbox/CA folder and instruct the
        cluster to load it (POST /node/controller/loadTrustedCAs), mirroring
        x509main._upload_cluster_ca_certificate but for an in-memory-generated
        CA rather than one already on disk from an x509main._generate_cert call.
        """
        server = server or self.cluster.master
        pem_bytes = self.crl_utils.cert_to_pem(ca_cert)

        shell = RemoteMachineShellConnection(server)
        try:
            os_type = shell.extract_remote_info().distribution_type
            if os_type == "windows":
                install_path = x509main.WININSTALLPATH
            elif os_type == "Mac":
                install_path = x509main.MACINSTALLPATH
            else:
                install_path = x509main.LININSTALLPATH
            ca_dir = f"{install_path}{x509main.CHAINFILEPATH}/CA"
            shell.execute_command(f"mkdir -p {ca_dir}")
            with tempfile.NamedTemporaryFile(
                delete=False, suffix=".pem", mode="wb"
            ) as tmp_file:
                tmp_file.write(pem_bytes)
                local_path = tmp_file.name
            try:
                shell.copy_file_local_to_remote(
                    local_path, f"{ca_dir}/crl_test_ca.pem"
                )
            finally:
                os.remove(local_path)
        finally:
            shell.disconnect()

        status, content = self.rest.load_trusted_CAs()
        if not status:
            self.fail(f"Failed to load trusted CAs on {server.ip}: {content}")

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
        )

    def _disable_client_cert_auth(self):
        SecurityRestAPI(self.cluster.master).cleanup_client_cert_auth()

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
