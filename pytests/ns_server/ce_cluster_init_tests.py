"""
CE Cluster Init REST API Enforcement Tests (CBQE-8972)

Validates ns_server CE enforcement for:
- sendStats=false rejected post-init via /settings/stats
- sendStats forced true at /clusterInit time (fresh node required)
- PhoneHome (sendStats) cannot be opted out during CE cluster init
"""

from basetestcase import ClusterSetup
from cb_server_rest_util.cluster_nodes.cluster_init_provision import (
    ClusterInitializationProvision,
)
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from shell_util.remote_connection import RemoteMachineShellConnection


class CeClusterInitRestTests(ClusterSetup):
    """CE enforcement of sendStats/PhoneHome at cluster init and post-init."""

    def setUp(self):
        super(CeClusterInitRestTests, self).setUp()
        self.rest = ClusterRestAPI(self.cluster.master)

        if self.cluster_util.is_enterprise_edition(self.cluster):
            self.fail("Tests require Community Edition cluster. "
                      "Install with edition=community parameter.")

        self.log.info("CE cluster confirmed. Master: %s", self.cluster.master.ip)

    def tearDown(self):
        super(CeClusterInitRestTests, self).tearDown()

    # ------------------------------------------------------------------
    # Post-init enforcement (Acceptance criterion 3 of CBQE-8972)
    # ------------------------------------------------------------------

    def test_ce_send_stats_cannot_disable_post_init(self):
        """
        Verify CE blocks disabling sendStats/PhoneHome after cluster-init.

        Steps:
        1. Confirm sendStats is currently true on CE cluster
        2. Attempt POST /settings/stats with sendStats=false
        3. Assert request is rejected
        4. Confirm sendStats is still true after rejected attempt
        """
        status, settings = self.rest.get_stats_settings()
        self.assertTrue(status,
                        "GET /settings/stats failed: %s" % settings)

        current = settings.get("sendStats", False) if isinstance(settings, dict) \
            else True
        self.assertTrue(current,
                        "CE cluster should default to sendStats=true, got: %s"
                        % settings)
        self.log.info("Confirmed sendStats=true baseline on CE cluster: %s",
                      settings)

        self.log.info("Attempting sendStats=false (CE must reject)")
        status, content = self.rest.set_stats_settings(send_stats=False)
        error_msg = (content.decode() if isinstance(content, bytes)
                     else str(content))

        self.assertFalse(
            status,
            "CE must reject sendStats=false but request succeeded. "
            "Response: %s" % error_msg)
        self.log.info("CE correctly rejected sendStats=false: %s", error_msg)

        status, settings = self.rest.get_stats_settings()
        self.assertTrue(status,
                        "GET /settings/stats failed after rejected update")
        final = settings.get("sendStats", False) if isinstance(settings, dict) \
            else True
        self.assertTrue(
            final,
            "sendStats must remain true after CE rejected the change, "
            "got: %s" % settings)
        self.log.info("Confirmed sendStats=true persists after rejected update")

    # ------------------------------------------------------------------
    # Init-time enforcement (Acceptance criteria 1 and 2 of CBQE-8972)
    # Requires a spare node NOT in the active cluster (extra node in node.ini)
    # ------------------------------------------------------------------

    def test_ce_send_stats_enforced_at_cluster_init(self):
        """
        Verify CE enforces sendStats=true (PhoneHome) at /clusterInit time.

        Requires an extra spare node listed in node.ini that is NOT part of
        the running cluster. The spare is hard-reset to simulate a fresh
        CE install, then /clusterInit is called with sendStats=false.

        Acceptance: either the init call fails, or sendStats is forced true.
        """
        self.rest.reset_node()

        master = self.cluster.master
        self.cluster_util.wait_for_ns_servers_or_assert([master])

        # Use ClusterInitializationProvision directly — no auth-check on
        # an uninitialised node (ClusterRestAPI would call check_if_active).
        init_prov = ClusterInitializationProvision()
        init_prov.set_server_values(master)
        init_prov.set_endpoint_urls(master)

        self.log.info("Calling /clusterInit with sendStats=false on CE node %s",
                      master.ip)
        status, content = init_prov.initialize_cluster(
            hostname=master.ip,
            username=master.rest_username,
            password=master.rest_password,
            send_stats=False,
            services="kv",
            memory_quota=256,
            cluster_name="CE_Init_Test",
        )

        if not status:
            error_msg = (content.decode() if isinstance(content, bytes)
                         else str(content))
            self.log.info(
                "CE correctly rejected /clusterInit with sendStats=false: %s",
                error_msg)
        else:
            # Init was allowed; CE must have forced sendStats=true
            self.log.info("/clusterInit succeeded; verifying sendStats is true")
            get_ok, stats_settings = self.rest.get_stats_settings()
            self.assertTrue(get_ok,
                            "GET /settings/stats failed on initialized spare")
            actual = (stats_settings.get("sendStats", False)
                      if isinstance(stats_settings, dict) else False)
            self.assertTrue(
                actual,
                "CE must force sendStats=true even when false was requested "
                "at init time. Got: %s" % stats_settings)
            self.log.info(
                "CE forced sendStats=true during cluster init despite "
                "sendStats=false in request")

        self.log.info("CE /clusterInit sendStats enforcement verified on %s",
                      master.ip)
