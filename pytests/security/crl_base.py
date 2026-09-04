import json

import requests

from membase.api.rest_client import RestConnection
from shell_util.remote_connection import RemoteMachineShellConnection

from couchbase_utils.cb_tools.cb_cli import CbCli
from couchbase_utils.rbac_utils.Rbac_ready_functions import RbacUtils
from couchbase_utils.security_utils.crl_utils import CRLUtils
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

        # RBAC users created during a test — cleaned up in tearDown. (CRL
        # file/CA/temp-PEM tracking lives on self.crl_utils itself now — see
        # crl_utils.py's "Test fixture helpers" section.)
        self._rbac_users = []

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
        """Untrusts every leftover CA from a previous run, clearing both the
        `ca_certificates` chronicle key (via the real per-id REST endpoint,
        not a raw chronicle_kv overwrite) and the inbox/CA directory on
        disk, before this test trusts its own fresh CA. Must run after
        super().setUp() since it needs the REST API up.

        Deliberately does NOT try to guess which CA id is "the node's own
        auto-generated one" (CA ids are just a monotonically increasing
        counter -- id 0 is only special the very first time a node boots,
        and stops being current the moment node-init's rename-triggered
        cert regen creates a new CA+leaf pair, which happens on every
        freshly-provisioned node). Instead this tries to delete every CA
        present and relies on the server's own protection: DELETE
        /pools/default/trustedCAs/{id} refuses (error, not 204) to remove a
        CA that's actually in use by a node's current certificate. That
        naturally leaves the real current CA alone and only removes
        genuinely orphaned ones, however many there are or whatever id they
        landed on. Best-effort: logs, doesn't raise.
        """
        try:
            status, content = self.rest.get_trusted_CAs()
            if not status:
                raise RuntimeError(f"GET trustedCAs failed: {content}")
            cas = json.loads(content)
            removed = 0
            for entry in cas:
                ca_id = entry.get("id")
                try:
                    del_status, _, _ = self.rest.delete_trusted_CA(ca_id)
                    if del_status:
                        removed += 1
                except Exception as exc:
                    # One CA's delete failing (e.g. a transient network
                    # hiccup) must not stop the rest of this cleanup pass --
                    # otherwise a single bad iteration leaves every
                    # later-listed CA (including genuinely stale ones)
                    # untouched, needlessly carrying the problem into the
                    # next test too.
                    self.log.warning(
                        f"Trusted CA self-heal: delete of id={ca_id} "
                        f"failed, continuing with the rest: {exc}"
                    )
            if removed > 0:
                self.log.warning(
                    f"{self.cluster.master.ip} had {removed} stale trusted "
                    f"CA(s) left over from a previous run -- untrusted "
                    f"them before this test starts (any CA still actually "
                    f"in use was left alone by the server)."
                )
        except Exception as exc:
            self.log.warning(f"Trusted CA self-heal error: {exc}")

        shell = RemoteMachineShellConnection(self.cluster.master)
        try:
            ca_dir = self.crl_utils._ca_dir(shell)
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
        (8.5+) clusters."""
        if not self.cluster_util.is_enterprise_edition(self.cluster):
            self.fail("CRL support requires an Enterprise Edition cluster.")

    # ── CA trust / cleanup fixture helpers ───────────────────────────────────
    #
    # Thin wrappers over couchbase_utils.security_utils.crl_utils.CRLUtils --
    # the actual logic (and its cleanup-tracking state) lives there so it can
    # be shared with pytests/upgrade/crl_upgrade.py's CRLUpgradeTests, which
    # can't inherit CRLBase (CRLBase(ClusterSetup) and UpgradeBase
    # (BaseTestCase) diverge at different intermediate ancestors and can't be
    # combined via multiple inheritance without MRO conflicts + double
    # cluster-init). Kept as same-named methods here so every existing call
    # site in crl_test.py needs no changes.

    @property
    def _created_files(self):
        """Some crl_test.py tests mutate this list directly (e.g.
        `self._created_files.remove(filename)` after deleting a file
        themselves) rather than going through _track_uploaded_file /
        _cleanup_created_files -- exposed as the live list on
        self.crl_utils so those call sites keep working unchanged."""
        return self.crl_utils.created_files

    def _trust_ca_on_cluster(self, ca_cert, server=None):
        self.crl_utils.trust_ca_on_cluster(
            self.rest, server or self.cluster.master, ca_cert
        )

    def _track_uploaded_file(self, filename):
        self.crl_utils.track_uploaded_file(filename)

    def _cleanup_created_files(self):
        self.crl_utils.cleanup_created_files(self.rest)

    def _reset_crl_settings(self):
        self.crl_utils.reset_crl_settings(self.rest)

    def _disable_client_cert_auth(self):
        self.crl_utils.disable_client_cert_auth(self.cluster.master)

    def _enable_client_cert_auth(self, state="enable", prefixes=None):
        self.crl_utils.enable_client_cert_auth(
            self.cluster.master, state=state, prefixes=prefixes
        )

    def _write_temp_pem(self, pem_bytes, suffix=".pem"):
        return self.crl_utils.write_temp_pem(pem_bytes, suffix=suffix)

    def _cleanup_temp_pem_files(self):
        self.crl_utils.cleanup_temp_pem_files()

    def _cleanup_trusted_cas(self):
        self.crl_utils.cleanup_trusted_cas(self.rest)

    def _deploy_node_cert(self, server, cert, key):
        self.crl_utils.deploy_node_cert(RestConnection(server), server, cert, key)

    def _deploy_client_cert(self, server, cert, key):
        self.crl_utils.deploy_client_cert(server, cert, key)

    def _set_client_cert_verification(self, server, enabled):
        self.crl_utils.set_client_cert_verification(server, enabled)

    # ── Node-to-node encryption helpers (opt-in -- only needed by tests that
    # exercise real nodeToNode CRL enforcement; nothing else in this suite,
    # or in CRLUpgradeTests, needs n2n encryption today, so this logic just
    # lives here rather than in CRLUtils). ──────────────────────────────────

    def _enable_n2n_encryption(self, servers, level="all"):
        """
        Enables node-to-node encryption + sets the cluster encryption level
        on each of `servers`, disabling auto-failover first and restoring it
        after -- toggling n2n briefly bounces every node's inter-node
        listener, matching cb_n2n_encryption.py's proven call order.
        """
        prior_autofailover = self.rest.get_autofailover_settings()
        self.rest.update_autofailover_settings(False, prior_autofailover.timeout)
        for server in servers:
            shell = RemoteMachineShellConnection(server)
            try:
                cb_cli = CbCli(shell, no_ssl_verify=True)
                cb_cli.enable_n2n_encryption()
                cb_cli.set_n2n_encryption_level(level=level)
            finally:
                shell.disconnect()
        if prior_autofailover.enabled:
            self.rest.update_autofailover_settings(
                True, prior_autofailover.timeout, maxCount=prior_autofailover.maxCount
            )

    def _disable_n2n_encryption(self, servers):
        """Reverses _enable_n2n_encryption: drop the encryption level to
        'control' first, then disable -- same order as
        cb_n2n_encryption.py's disable_n2n_cluster()."""
        prior_autofailover = self.rest.get_autofailover_settings()
        self.rest.update_autofailover_settings(False, prior_autofailover.timeout)
        for server in servers:
            shell = RemoteMachineShellConnection(server)
            try:
                cb_cli = CbCli(shell, no_ssl_verify=True)
                cb_cli.set_n2n_encryption_level(level="control")
                cb_cli.disable_n2n_encryption()
            finally:
                shell.disconnect()
        if prior_autofailover.enabled:
            self.rest.update_autofailover_settings(
                True, prior_autofailover.timeout, maxCount=prior_autofailover.maxCount
            )

    # ── RBAC helpers ─────────────────────────────────────────────────────────

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
