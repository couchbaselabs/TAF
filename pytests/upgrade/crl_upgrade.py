import json

from cryptography.x509.oid import NameOID

from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from membase.api.rest_client import RestConnection

from couchbase_utils.security_utils.crl_utils import CRLUtils
from upgrade.upgrade_base import UpgradeBase


class CRLUpgradeTests(UpgradeBase):
    """
    CRL coverage needing a genuinely mixed-version cluster, which
    crl_test.py's CRLBase(ClusterSetup) suite can't construct.
    CRLBase(ClusterSetup) and UpgradeBase(BaseTestCase) diverge at
    different ancestors and can't be combined via multiple inheritance,
    so this can't inherit CRLBase directly -- fixture logic instead
    lives once in CRLUtils (shared with CRLBase), reached here via thin
    `self._foo(...)` wrappers. CRLBase's self-heal methods aren't needed
    here since UpgradeBase reinstalls nodes fresh every run.
    """

    MGMT_PORT = 18091

    def setUp(self):
        super(CRLUpgradeTests, self).setUp()
        self.crl_utils = CRLUtils(log=self.log)
        self.rest = RestConnection(self.cluster.master)

        # CA trust is a pre-existing x509 operation, not CRL-gated -- safe
        # to do here even though self.cluster.master is still on the
        # pre-CRL initial (oldest-in-chain) version.
        self.ca_cert, self.ca_key = self.crl_utils.generate_ca("UpgradeCRLTestCA")
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
            self._cleanup_temp_pem_files()
        except Exception as exc:
            self.log.warning(f"Temp PEM file cleanup error: {exc}")
        try:
            self._cleanup_trusted_cas()
        except Exception as exc:
            self.log.warning(f"Trusted CA cleanup error: {exc}")
        finally:
            super(CRLUpgradeTests, self).tearDown()

    # ── CA trust / cleanup fixture helpers ───────────────────────────────────
    # Thin wrappers over CRLUtils -- see class docstring and crl_utils.py's
    # "Test fixture helpers" section. Mirrors CRLBase's own wrappers in
    # pytests/security/crl_base.py.

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

    # ── Tests ────────────────────────────────────────────────────────────────

    def test_upgrade_crl_config_survives_online_upgrade(self):
        """Existing CA trust/x509 config survives an online upgrade from a
        pre-CRL version; CRL defaults to Disabled immediately post-upgrade;
        CRL can then be configured and enforced correctly. The final
        enforce step is the first real exercise of CRL activation
        immediately after a live upgrade (fresh cbauth push-config
        registration, freshly-started CRL poller) -- a failure there is a
        genuine finding, not a pre-known gap."""
        # -- Before: every node on the pre-CRL initial version. --
        status, _ = self.crl_utils.get_settings(self.rest)
        self.assertFalse(
            status, "A pre-CRL cluster should have no CRL REST support at all"
        )
        status, trusted = self.rest.get_trusted_CAs()
        self.assertTrue(status, f"GET trustedCAs failed pre-upgrade: {trusted}")
        ca_cn = self.ca_cert.subject.get_attributes_for_oid(NameOID.COMMON_NAME)[0].value
        self.assertTrue(
            any(ca_cn in entry.get("subject", "") for entry in json.loads(trusted)),
            f"Trusted CA {ca_cn!r} should be visible pre-upgrade: {trusted}",
        )
        self.log.info("Pre-upgrade: no CRL REST support, ordinary CA trust works")

        # self.upgrade_version starts out == self.upgrade_chain[0] (the
        # INITIAL version) from setUp's own chain-walk -- fetch_node_to_
        # upgrade() compares node versions against self.upgrade_version,
        # so it must be reassigned to each real target before calling it,
        # matching security_upgrade.py's established pattern. A single-
        # hop chain (e.g. ["8.0.2", "8.5.0-XXXX"]) makes this loop run once.
        for target_version in self.upgrade_chain[1:]:
            self.initial_version = self.upgrade_version
            self.upgrade_version = target_version
            node_to_upgrade = self.fetch_node_to_upgrade()
            while node_to_upgrade is not None:
                self.upgrade_function[self.upgrade_type](node_to_upgrade)
                node_to_upgrade = self.fetch_node_to_upgrade()
        self.log.info("All nodes upgraded to a CRL-capable version")

        # -- After: CRL support exists, defaults to Disabled, prior CA
        # trust survived the upgrade untouched. --
        status, settings = self.crl_utils.get_settings(self.rest)
        self.assertTrue(status, f"CRL REST support should exist post-upgrade: {settings}")
        self.assertEqual(
            settings.get("policyPerScope"),
            {"clientAuth": "Disabled", "nodeToNode": "Disabled"},
            f"CRL should default to Disabled immediately post-upgrade: {settings}",
        )
        status, trusted = self.rest.get_trusted_CAs()
        self.assertTrue(status, f"GET trustedCAs failed post-upgrade: {trusted}")
        self.assertTrue(
            any(ca_cn in entry.get("subject", "") for entry in json.loads(trusted)),
            f"Trusted CA {ca_cn!r} should have survived the upgrade: {trusted}",
        )
        self.log.info(
            "Post-upgrade: CRL support exists, defaults to Disabled, "
            "pre-upgrade CA trust survived unchanged"
        )

        # -- Configure + enforce for real. --
        self._enable_client_cert_auth(state="enable")
        valid_cert, valid_key, _ = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, "postUpgradeValid"
        )
        revoked_cert, revoked_key, revoked_serial = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, "postUpgradeRevoked"
        )
        valid_cert_path = self._write_temp_pem(self.crl_utils.cert_to_pem(valid_cert))
        valid_key_path = self._write_temp_pem(self.crl_utils.key_to_pem(valid_key))
        revoked_cert_path = self._write_temp_pem(self.crl_utils.cert_to_pem(revoked_cert))
        revoked_key_path = self._write_temp_pem(self.crl_utils.key_to_pem(revoked_key))

        status, settings = self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Require", "nodeToNode": "Disabled"},
        )
        self.assertTrue(status, f"set_settings(Require) failed post-upgrade: {settings}")
        filename = "post_upgrade_crl.pem"
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key, [revoked_serial], filename, crl_number=1,
        )
        self.assertTrue(status, f"Revoking CRL upload failed post-upgrade: {content}")
        self._track_uploaded_file(filename)
        self.crl_utils.reload_crl(self.rest)

        self.assertEqual(
            self.crl_utils.probe_mtls_state(
                self.cluster.master.ip, self.MGMT_PORT,
                revoked_cert_path, revoked_key_path,
            ),
            "rejected",
            "Revoked cert should be rejected under freshly-activated "
            "post-upgrade CRL enforcement",
        )
        self.assertEqual(
            self.crl_utils.probe_mtls_state(
                self.cluster.master.ip, self.MGMT_PORT,
                valid_cert_path, valid_key_path,
            ),
            "connected",
            "Valid cert should connect under freshly-activated "
            "post-upgrade CRL enforcement",
        )
        self.log.info(
            "CRL configured and enforced correctly immediately after a "
            "live online upgrade from a pre-CRL version"
        )

    def test_mixed_version_health_warning_and_require_policy_block(self):
        """Closes two PRD-gap rows in one pass (same mixed-version window
        for both, avoids reprovisioning it twice):

        1. Health warning for partial CRL enforcement mid-upgrade -- known
        gap, pinned here (menelaus_web_alerts_srv.erl only has
        crl_expired/crl_expires_soon, no mixed-version alert).

        2. Does a cluster-wide clientAuth=Require push get blocked
        mid-upgrade -- CONFIRMED LIVE: ns_server gates the *entire* CRL
        REST API cluster-wide until every node is upgraded (an upgraded
        node still 404s "CRL feature not yet enabled in this cluster").
        So yes, blocked, via a whole-feature gate rather than a
        per-policy check -- and no enforcement-divergence check is needed
        either, since configuration itself is blocked until uniform.
        Strict is moot (already confirmed dropped from spec, same as
        Require).
        """
        # See the identical comment in test_upgrade_crl_config_survives_
        # online_upgrade -- self.upgrade_version must be reassigned to
        # each real target before fetch_node_to_upgrade() will match
        # anything.
        for target_version in self.upgrade_chain[1:]:
            self.initial_version = self.upgrade_version
            self.upgrade_version = target_version
            node_to_upgrade = self.fetch_node_to_upgrade()
            while node_to_upgrade is not None:
                self.upgrade_function[self.upgrade_type](node_to_upgrade)
                node_to_upgrade = self.fetch_node_to_upgrade()
                if node_to_upgrade is not None:
                    self._assert_mixed_version_crl_behavior()
        self.log.info(
            "Mixed-version-window CRL behavior checked at every step of "
            "the rolling upgrade"
        )

    def _assert_mixed_version_crl_behavior(self):
        # The CRL-support gate is cluster-wide, not per-node (confirmed
        # live) -- CRLUtils.node_supports_crl() would report False for
        # every node throughout the whole window, including already-
        # upgraded ones, so it can't be used to detect the window here.
        # Use each node's own reported build version instead.
        servers = self.cluster.servers[:self.nodes_init]
        upgraded_flags = []
        for server in servers:
            _, node_info = ClusterRestAPI(server).node_details()
            upgraded_flags.append(self.upgrade_version in node_info.get("version", ""))
        self.assertTrue(
            any(upgraded_flags) and not all(upgraded_flags),
            f"Expected a genuinely mixed-version window by node build "
            f"version, got upgraded_flags={upgraded_flags} -- the "
            f"upgrade loop may be broken",
        )
        upgraded_server = servers[upgraded_flags.index(True)]
        upgraded_rest = RestConnection(upgraded_server)

        # -- Whole-feature gate, confirmed live: an already-upgraded
        # node's own CRL REST API stays disabled while any other node in
        # the same cluster is still pre-CRL. --
        status, content = self.crl_utils.get_settings(upgraded_rest)
        self.log.info(
            f"GET /settings/crl on already-upgraded node "
            f"{upgraded_server.ip} during mixed-version window: "
            f"status={status}, content={content!r}"
        )
        self.assertFalse(
            status,
            f"Expected the CRL feature to stay gated on an upgraded node "
            f"while the rest of the cluster is still pre-CRL, got: {content}",
        )
        self.assertIn(
            "not yet enabled", str(content).lower(),
            f"Expected the mixed-version-specific gate message, got: {content}",
        )

        # Pushing Require fails via the exact same cluster-wide gate --
        # not a separate, policy-value-specific check.
        status, content = self.crl_utils.set_settings(
            upgraded_rest,
            policyPerScope={"clientAuth": "Require", "nodeToNode": "Disabled"},
        )
        self.log.info(
            f"set_settings(Require) during mixed-version window: "
            f"status={status}, content={content!r}"
        )
        self.assertFalse(
            status,
            f"Require push should be blocked by the same cluster-wide "
            f"CRL gate during the mixed-version window, got: {content}",
        )

        # -- Health-warning half: known gap, pinned. get_alert_messages
        # hits /pools/default, a non-CRL endpoint that works fine on the
        # gated node. --
        alert_msgs = self.crl_utils.get_alert_messages(upgraded_rest)
        self.assertFalse(
            any(
                "mixed" in m.lower() or "partial" in m.lower()
                for m in alert_msgs
            ),
            "Known gap: no health warning exists for the mixed-version "
            "CRL gate state either -- only crl_expired/crl_expires_soon "
            "alert types exist in ns_server. If this now fails, the gap "
            "has been fixed -- flip this assertion and assert the new "
            "warning's text.",
        )
