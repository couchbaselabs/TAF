from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from custom_exceptions.exception import RebalanceFailedException
from couchbase_utils.cluster_utils.cluster_ready_functions import ClusterUtils
from shell_util.remote_connection import RemoteMachineShellConnection
from rebalance_utils.rebalance_util import RebalanceUtil
from upgrade.upgrade_base import UpgradeBase


class CEBaseTest(UpgradeBase):
    """
    Base class for Community Edition restriction and upgrade tests.

    Extends UpgradeBase with CE-specific guards, version-aware enforcement
    checks, and shared helpers used across all CE test scenarios.

    setUp contract: pass community_upgrade=True in conf so UpgradeBase
    installs the target build as Community Edition.
    """

    CE_NODE_LIMIT = 5
    CE_REBALANCE_ERROR = "Cannot rebalance with more than 5"
    # CE node-limit and service restrictions are enforced from 8.1.0 onwards.
    ENFORCED_CE_MIN_VERSION = (8, 1, 0)

    def setUp(self):
        super(CEBaseTest, self).setUp()
        self.log_setup_status(self.__class__.__name__, "started", "setup")
        self._require_ce_cluster()
        status = ClusterRestAPI(self.cluster.master).update_auto_failover_settings(
            enabled="false")
        self.assertTrue(status, "Failed to disable auto-failover")
        self.log.info("CE cluster confirmed, auto-failover disabled")
        self.log_setup_status(self.__class__.__name__, "completed", "setup")

    def tearDown(self):
        super(CEBaseTest, self).tearDown()


    def _require_ce_cluster(self):
        """Fail immediately if the cluster is not Community Edition."""
        if self.cluster_util.is_enterprise_edition(self.cluster):
            self.fail("Requires a Community Edition cluster. "
                      "Pass community_upgrade=True.")

    def _node_has_enforced_ce(self, server):
        """
        Return True if the node's version is >= ENFORCED_CE_MIN_VERSION.

        Uses a proper integer-tuple comparison so two-digit minor/patch
        versions (e.g. 8.10.0) are never miscompared by string ordering.
        CE node-limit and service restrictions are only enforced from 8.1+.
        """
        status, info = ClusterRestAPI(server).node_details()
        if not status:
            self.log.warning("node_details failed for %s — assuming no CE "
                             "enforcement", server.ip)
            return False
        raw_version = (info or {}).get("version", "")
        parts = raw_version.split("-")[0].split(".")
        try:
            version_tuple = tuple(int(p) for p in parts[:3])
        except ValueError:
            return False
        return version_tuple >= self.ENFORCED_CE_MIN_VERSION

    def _require_enforced_ce_version(self):
        """
        Skip the test if no cluster node is running a version that enforces
        the CE restrictions (< 8.1.0).  Uses skipTest so the result is
        marked SKIP rather than FAIL on older builds.
        """
        if not self.cluster.nodes_in_cluster:
            self.fail("cluster.nodes_in_cluster is empty — "
                      "setUp may not have completed")
        if not any(self._node_has_enforced_ce(n)
                   for n in self.cluster.nodes_in_cluster):
            self.skipTest(
                "CE restrictions are only enforced on %s+; "
                "cluster is on an older version"
                % ".".join(str(v) for v in self.ENFORCED_CE_MIN_VERSION))


    def _online_swap_all_nodes(self, edition="enterprise"):
        """
        Rolling online-swap upgrade of every cluster node to self.upgrade_version.

        Pass edition='community' for a CE→CE hop; the default 'enterprise' is
        used for a CE→EE hop.

        For the CE hop we manually pre-install CE on the spare before each
        swap, then call online_swap(install_on_spare_node=False) so the base
        implementation's default EE install is skipped.
        """
        max_iterations = len(self.cluster.nodes_in_cluster) + 1
        iterations = 0
        node_to_upgrade = self.fetch_node_to_upgrade()
        while node_to_upgrade is not None and iterations < max_iterations:
            iterations += 1
            if edition == "community":
                self._install_on_spare(edition="community")
                self.online_swap(node_to_upgrade, install_on_spare_node=False)
            else:
                self.online_swap(node_to_upgrade)
            self.cluster_util.print_cluster_stats(self.cluster)
            node_to_upgrade = self.fetch_node_to_upgrade()
        if iterations >= max_iterations and node_to_upgrade is not None:
            self.fail("Online swap loop exceeded max iterations — "
                      "possible fetch_node_to_upgrade bug")

    def _install_on_spare(self, edition="enterprise"):
        """
        Install ``edition`` build of ``self.upgrade_version`` on the spare
        node and wait for the REST endpoint to become reachable.
        """
        self.log.info("Installing %s %s on spare node %s",
                      edition, self.upgrade_version, self.spare_node.ip)
        result = self.upgrade_helper.new_install_version_on_all_nodes(
            nodes=[self.spare_node],
            version=self.upgrade_version,
            edition=edition,
            install_tasks=["populate_build_url", "check_url_status",
                           "download_build", "uninstall", "install"])
        if result is not None:
            self.fail("Failed to install %s %s on spare node %s"
                      % (edition, self.upgrade_version, self.spare_node.ip))
        self.cluster_util.wait_for_ns_servers_or_assert(
            [self.spare_node], wait_time=120)

    def _offline_upgrade_single_node(self, node, version, edition="community"):
        """
        True offline upgrade: downloads the new build via the standard TAF
        machinery, then stops CB, soft-removes the existing package
        (preserving /opt/couchbase/var so the Erlang node name and cluster
        config survive on disk), and installs the new build.

        new_install_version_on_all_nodes is used only for the download step.
        Its uninstall step ('rm -rf /opt/couchbase') is intentionally skipped
        because it wipes the cluster config, causing the node to restart as
        ns_1@<hostname> instead of ns_1@<ip>.  That breaks Erlang distribution
        authentication and produces a spurious prepare_rebalance_failed
        timeout instead of the CE node-limit error this test targets.
        """
        self.log.info("Offline-upgrading %s → %s %s (preserving cluster state)",
                      node.ip, edition, version)

        # Step 1: download-only — TAF resolves the correct URL for any OS/arch
        result = self.upgrade_helper.new_install_version_on_all_nodes(
            nodes=[node],
            version=version,
            edition=edition,
            install_tasks=["populate_build_url", "check_url_status",
                           "download_build"])
        if result is not None:
            self.fail("Failed to download %s %s for offline upgrade on %s"
                      % (edition, version, node.ip))

        shell = RemoteMachineShellConnection(node)
        try:
            # Locate the downloaded package in /tmp
            out, _ = shell.execute_command(
                "ls /tmp/couchbase-server-*{}* 2>/dev/null".format(version))
            pkg_path = out[0].strip() if out else None
            if not pkg_path:
                self.fail("Downloaded package for %s not found in /tmp on %s"
                          % (version, node.ip))
            self.log.info("Downloaded package on %s: %s", node.ip, pkg_path)

            chronicle_dir = ("/opt/couchbase/var/lib/couchbase"
                             "/config/chronicle")
            chronicle_bak = "/tmp/chronicle_bak"

            # Step 2: stop CB
            self.log.info("Stopping couchbase-server on %s", node.ip)
            shell.execute_command("systemctl stop couchbase-server || true")
            self.sleep(5, "Wait for couchbase-server to stop on %s" % node.ip)

            # Step 3: back up chronicle BEFORE dpkg -r.
            #   The deb prerm script intentionally deletes the chronicle
            #   directory on remove.  Without it the node restarts with a
            #   fresh chronicle history ID, split-braining the cluster and
            #   causing every subsequent rebalance to return 503/500.
            self.log.info("Backing up chronicle on %s", node.ip)
            shell.execute_command(
                "rm -rf {bak} ; cp -r {src} {bak}".format(
                    src=chronicle_dir, bak=chronicle_bak))

            # Step 4: soft-remove — preserves /opt/couchbase/var (dist_cfg,
            #   config.dat) but chronicle is deleted by the prerm script.
            self.log.info("Soft-removing existing CB package on %s", node.ip)
            if pkg_path.endswith(".deb"):
                shell.execute_command(
                    "dpkg -r couchbase-server-community couchbase-server"
                    " 2>/dev/null || true")
            else:
                shell.execute_command(
                    "rpm -e couchbase-server-community couchbase-server"
                    " 2>/dev/null || true")

            # Step 5: install new build WITHOUT auto-starting CB.
            #   INSTALL_DONT_START_SERVER=1 skips the systemd-ctl start in
            #   the postinst script so CB never writes a fresh chronicle
            #   instance before we restore the backup.
            self.log.info("Installing %s %s on %s", edition, version, node.ip)
            if pkg_path.endswith(".deb"):
                out, err = shell.execute_command(
                    "INSTALL_DONT_START_SERVER=1 dpkg -i {}".format(pkg_path))
            else:
                out, err = shell.execute_command(
                    "rpm -U {}".format(pkg_path))
            self.log.info("Install — stdout: %s | stderr: %s", out, err)

            # Step 6: restore chronicle then start CB.
            self.log.info("Restoring chronicle on %s", node.ip)
            shell.execute_command(
                "cp -r {bak} {dst} && "
                "chown -R couchbase:couchbase {dst}".format(
                    bak=chronicle_bak, dst=chronicle_dir))
            self.log.info("Starting couchbase-server on %s", node.ip)
            shell.execute_command("systemctl start couchbase-server")
        finally:
            shell.disconnect()

        self.cluster_util.wait_for_ns_servers_or_assert([node], wait_time=120)

    def _wait_for_node_active(self, node, timeout=120):
        """Poll until node appears as an active cluster member."""
        elapsed = 0
        while elapsed < timeout:
            cluster_nodes = ClusterUtils.get_nodes(self.cluster.master)
            if any(n.ip == node.ip for n in cluster_nodes):
                self.log.info("Node %s rejoined cluster as active", node.ip)
                return
            self.sleep(5, "Waiting for %s to rejoin cluster" % node.ip)
            elapsed += 5
        self.fail("Node %s did not rejoin cluster within %ds"
                  % (node.ip, timeout))

    def _attempt_rebalance_expect_ce_failure(self, rest, known_nodes,
                                             check_ce_message=True):
        """
        Trigger a rebalance expected to be rejected by the CE 5-node limit.

        Handles two rejection paths:
          Synchronous — REST call returns HTTP 4xx; error is in the body.
          Async — REST call is accepted (200), ns_server then sets
            errorMessage in /pools/default/tasks.

        For the async path the method polls the tasks endpoint for up to 10s
        so the task is registered before monitor_rebalance begins, avoiding
        the race where status reads "notRunning" before the CE check fires
        and monitor_rebalance exits as success (progress=100, no error seen).

        check_ce_message=True (default): asserts the CE_REBALANCE_ERROR
          substring is present in the error — use for tests where the CE
          orchestrator returns a clean rejection message.
        check_ce_message=False: asserts only that rebalance failed — use for
          mixed-version clusters where the CE check may manifest as a node
          timeout/crash rather than a clean error string.

        Returns the full error string.
        """
        error_msg = None
        # Retry up to 3 times if the cluster is still settling after a node
        # rejoin (REST returns 503 "Service is temporarily unavailable").
        for attempt in range(3):
            status, content = rest.rebalance(known_nodes=known_nodes,
                                             eject_nodes=[])
            if not status:
                raw = content.decode("utf-8") if isinstance(content, bytes) \
                    else str(content)
                if "temporarily unavailable" in raw.lower() and attempt < 2:
                    self.sleep(15, "Cluster not yet ready for rebalance "
                               "(attempt %d/3) — waiting for node to settle"
                               % (attempt + 1))
                    continue
                error_msg = raw
            break

        if not status and error_msg is None:
            raw = content.decode("utf-8") if isinstance(content, bytes) \
                else str(content)
            error_msg = raw

        if error_msg and "temporarily unavailable" in error_msg.lower():
            self.fail("Cluster not ready for rebalance after 3 retries: "
                      "%s" % error_msg)

        if status:
            for _ in range(5):
                _, tasks = rest.cluster_tasks()
                rebalance_task = next(
                    (t for t in (tasks or [])
                     if isinstance(t, dict) and t.get("type") == "rebalance"),
                    None)
                if rebalance_task and (
                        rebalance_task.get("status") == "running"
                        or "errorMessage" in rebalance_task):
                    break
                self.sleep(2, "Waiting for rebalance task to initialize")

            try:
                RebalanceUtil(self.cluster).monitor_rebalance()
                self.fail("Rebalance succeeded — expected CE node-limit to block it")
            except RebalanceFailedException as e:
                error_msg = str(e)

        self.assertIsNotNone(error_msg,
                             "No error returned — CE restriction was not triggered")
        if check_ce_message:
            self.assertIn(self.CE_REBALANCE_ERROR, error_msg,
                          "Expected CE restriction error. Got: %s" % error_msg)
            self.log.info("Rebalance correctly rejected with CE restriction "
                          "message: %s", error_msg)
        else:
            self.log.info("Rebalance correctly blocked (CE restriction active). "
                          "Error: %s", error_msg)
        return error_msg
