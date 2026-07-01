# -*- coding: utf-8 -*-
"""
Lighthouse Failover Tests
Validates that the Lighthouse Collector correctly reflects node failure
and recovery events on the UCP portal.

Inherits from LighthouseBase for portal/multi-cluster setup.
Uses NodeFailureTask for failure induction — the same task class used by
ConcurrentFailoverTests — so failure behaviour is identical to standard
failover regression tests.
"""
from Jython_tasks.task import ConcurrentFailoverTask, NodeFailureTask
from lighthouse.lighthouse_base import LighthouseBase
from lighthouse.collector_helper_methods import (
    LIGHTHOUSE_DEFAULT_PORTAL_PORT,
    get_collector_settings,
    restore_collector_settings,
    get_cb_cluster_uuid,
    get_cb_cluster_nodes_services,
    get_portal_cluster,
    wait_for_cluster_on_portal,
    wait_for_portal_node_count,
    set_lighthouse_ns_config_via_diag_eval,
    set_lighthouse_interval_via_diag_eval,
)
from membase.api.rest_client import RestConnection
from unified_control_plane import LighthouseCollectorClient


class LighthouseFailoverTests(LighthouseBase):
    """
    Portal-facing failover tests.

    Reuses NodeFailureTask (from ConcurrentFailoverTests infrastructure)
    for failure induction/revert, and RestConnection.fail_over() for manual
    failover triggers — the same code paths used in concurrent_failovers.py.

    The lighthouse collector helper functions are standalone, so they are
    called directly without requiring LighthouseBase in the inheritance path
    of the failover classes.
    """

    # failure_method values that require an explicit revert step.
    # restart_couchbase self-heals and has no revert entry in NodeFailureTask.
    _NEEDS_REVERT = frozenset({"stop_couchbase", "stop_memcached"})

    def setUp(self):
        super(LighthouseFailoverTests, self).setUp()
        self.collector_clients = [
            LighthouseCollectorClient(cluster.master)
            for cluster in self.clusters
        ]
        self._original_settings = []
        for i, client in enumerate(self.collector_clients):
            settings = get_collector_settings(client)
            if settings is None:
                self.fail(
                    "Could not fetch collector settings from cluster %d "
                    "(%s) during setUp" % (i, client.ip))
            self._original_settings.append(settings)
            self.log.info("Cluster %d original collector settings: %s"
                          % (i, settings))

        # Snapshot autofailover settings so tearDown can restore them
        self._rest_primary = RestConnection(self.cluster.master)
        afo = self._rest_primary.get_autofailover_settings()
        self._original_afo_enabled = afo.enabled
        self._original_afo_timeout = afo.timeout
        # afo.count is the current event count; afo.maxCount is the maximum
        # number of nodes allowed to autofailover — we want to restore the latter
        self._original_afo_max_count = getattr(afo, 'maxCount', 1)

    def tearDown(self):
        for i, (client, saved) in enumerate(
                zip(self.collector_clients, self._original_settings)):
            try:
                restore_collector_settings(client, saved)
                self.log.info("Cluster %d: collector settings restored" % i)
            except Exception as e:
                self.log.warning(
                    "Cluster %d: failed to restore collector settings: %s"
                    % (i, e))

        # Restore autofailover settings.  After an autofailover the cluster
        # keeps a non-zero count; update_autofailover_settings fails (MB-7282)
        # until that count is reset via POST /settings/autoFailover/resetCount.
        # Try each node in turn — the active orchestrator accepts both calls.
        afo_restored = False
        for node in self.cluster.nodes_in_cluster:
            try:
                node_rest = RestConnection(node)
                # Reset the autofailover event count first — MB-7282 workaround
                node_rest._http_request(
                    node_rest.baseUrl + 'settings/autoFailover/resetCount',
                    'POST', '')
                ok = node_rest.update_autofailover_settings(
                    self._original_afo_enabled,
                    self._original_afo_timeout,
                    maxCount=self._original_afo_max_count)
                if ok:
                    self.log.info(
                        "Primary: autofailover settings restored via %s"
                        % node.ip)
                    afo_restored = True
                    break
            except Exception:
                continue
        if not afo_restored:
            self.log.warning(
                "Primary: could not restore autofailover settings on any node")

        super(LighthouseFailoverTests, self).tearDown()

    # ==================== Private helpers ====================

    def _induce_failure(self, node, failure_method):
        """
        Induce failure on node using NodeFailureTask — same task class as
        ConcurrentFailoverTests.
        """
        task = NodeFailureTask(
            self.task_manager, node, failure_method,
            task_type="induce_failure")
        self.task_manager.add_new_task(task)
        self.task_manager.get_task_result(task)
        self.assertTrue(
            task.result,
            "Failed to induce '%s' on %s" % (failure_method, node.ip))
        self.log.info("Failure '%s' induced on %s" % (failure_method, node.ip))

    def _revert_failure(self, node, failure_method):
        """
        Revert a previously induced failure using NodeFailureTask.
        Only call for failure_method values in _NEEDS_REVERT.
        """
        task = NodeFailureTask(
            self.task_manager, node, failure_method,
            task_type="revert_failure")
        self.task_manager.add_new_task(task)
        self.task_manager.get_task_result(task)
        self.log.info(
            "Failure '%s' reverted on %s" % (failure_method, node.ip))

    def _trigger_report_and_restore_interval(self, server, portal_domain,
                                             label="", sleep_seconds=60):
        """
        Set a short reporting interval via diag/eval to trigger an immediate
        report, sleep long enough for the report to fire and the portal to
        process it, then restore the interval to 2 h.

        sleep_seconds defaults to 60 — long enough that the cluster state is
        fully propagated before the report is sent and the portal is updated.
        """
        diag_status, diag_content = set_lighthouse_ns_config_via_diag_eval(
            server,
            reporting_endpoint=portal_domain,
            reporting_port=LIGHTHOUSE_DEFAULT_PORTAL_PORT,
            reporting_interval_hours=1 / 60.0)
        self.assertTrue(
            diag_status,
            "%s: diag/eval for report trigger failed: %s"
            % (label, diag_content))
        self.sleep(sleep_seconds,
                   "waiting for report to fire (%s)" % label)
        restore_status, restore_content = \
            set_lighthouse_interval_via_diag_eval(server, 2)
        self.assertTrue(
            restore_status,
            "%s: failed to restore reporting interval to 2 h: %s"
            % (label, restore_content))

    # ==================== Tests ====================

    def test_node_failure_reflected_on_portal(self):
        """
        Verify that node failure and recovery are correctly reflected on the
        UCP portal, and that other clusters' telemetry is unaffected.

        Two distinct behaviours are exercised depending on trigger_failover:

        Without failover (trigger_failover=False):
            Autofailover is disabled before the failure is induced.  The node
            becomes unhealthy but stays in the cluster roster, so the portal
            node count must remain at N.  After the failure is reverted the
            portal must still show N.

        With failover (trigger_failover=True):
            A manual failover is triggered after the failure is induced.  The
            node is ejected from the cluster, so the portal must drop to N-1.
            After the node is added back the portal must return to N.

        Params:
            failure_method (str, default "stop_couchbase"):
                Failure to induce before checking the portal.
                  "stop_couchbase"    hard stop of couchbase-server; requires
                                      explicit revert (start_couchbase).
                  "stop_memcached"    kills memcached only; requires explicit
                                      revert (start_memcached).
                  "restart_couchbase" restarts couchbase-server; node
                                      self-heals, no revert step needed.
                For graceful failover (failover_type=graceful) no failure is
                induced — the node is gracefully failed over while healthy.

            master_node (bool, default False):
                True  — target the current orchestrator.  After inducing
                        failure, find_orchestrator is called with a live node
                        to update primary.master before triggering a report.
                False — target the last non-master node.

            trigger_failover (bool, default False):
                False — verify portal count stays at N during and after
                        the failure (no failover triggered).
                True  — verify portal drops to N-1 after failover, then
                        returns to N after the node is added back.

            failover_type (str, default "graceful"):
                "graceful" — graceful failover via REST (node must be
                             healthy; failure_method is skipped).
                "forceful" — hard failover via REST (failure is induced
                             first, then forceful failover is triggered).

        Requires:
            nodes_init >= 3 for the primary cluster so at least one
            non-master node is available when master_node=False.
        """
        failure_method = self.input.param("failure_method", "stop_couchbase")
        remove_master = self.input.param("master_node", False)
        trigger_failover = self.input.param("trigger_failover", False)
        failover_type = self.input.param("failover_type", "graceful")

        # Graceful and forceful failover are pure API calls (same as
        # ConcurrentFailoverTests) — no failure induction needed beforehand.
        # Only the no-failover scenario induces a failure to test portal
        # behaviour while the node is degraded but still in the cluster.
        induce_pre_failure = not trigger_failover

        portal_domain = 'lighthouse.couchbase.internal'
        primary = self.cluster
        rest = RestConnection(primary.master)

        status, content, _ = self.ucp_client.session_login(
            self.ucp_portal.username, self.ucp_portal.password)
        self.assertTrue(status, "Portal login failed: %s" % content)
        self.log.info("Portal login successful")

        try:
            # --- Initial reports for all clusters + record baseline counts ---
            cluster_uuids = []
            for i, cluster in enumerate(self.clusters):
                uuid = get_cb_cluster_uuid(cluster.master)
                self.assertIsNotNone(
                    uuid,
                    "Cluster %d: could not retrieve UUID from /pools" % i)
                cluster_uuids.append(uuid)
                self.log.info("Cluster %d UUID: %s" % (i, uuid))

                diag_status, diag_content = \
                    set_lighthouse_ns_config_via_diag_eval(
                        cluster.master,
                        reporting_endpoint=portal_domain,
                        reporting_port=LIGHTHOUSE_DEFAULT_PORTAL_PORT,
                        reporting_interval_hours=1 / 3600.0)
                self.assertTrue(
                    diag_status,
                    "Cluster %d: initial diag/eval failed: %s"
                    % (i, diag_content))

            self.sleep(10, "waiting for initial reports to fire on all clusters")

            for i, (cluster, uuid) in enumerate(
                    zip(self.clusters, cluster_uuids)):
                restore_status, restore_content = \
                    set_lighthouse_interval_via_diag_eval(cluster.master, 2)
                self.assertTrue(
                    restore_status,
                    "Cluster %d: failed to restore reporting interval: %s"
                    % (i, restore_content))
                appeared = wait_for_cluster_on_portal(
                    self.ucp_client, uuid, timeout=60, poll_interval=5)
                self.assertTrue(
                    appeared,
                    "Cluster %d UUID '%s' did not appear on portal within "
                    "60 s after initial report trigger" % (i, uuid))
                self.log.info("Cluster %d: confirmed on portal" % i)

            initial_portal_counts = []
            for i, uuid in enumerate(cluster_uuids):
                portal_cluster = get_portal_cluster(self.ucp_client, uuid)
                self.assertIsNotNone(
                    portal_cluster,
                    "Cluster %d: could not fetch cluster from portal" % i)
                count = len(
                    portal_cluster.get('telemetry', {}).get('nodes', []))
                initial_portal_counts.append(count)
                self.log.info(
                    "Cluster %d: initial portal node count = %d" % (i, count))

            # --- Pick the node to target ---
            if remove_master:
                node_to_fail = primary.master
                self.log.info(
                    "master_node=True: targeting orchestrator %s"
                    % node_to_fail.ip)
            else:
                non_master_nodes = [
                    n for n in primary.nodes_in_cluster
                    if n.ip != primary.master.ip]
                self.assertTrue(
                    non_master_nodes,
                    "Primary cluster has no non-master node available; "
                    "conf requires nodes_init >= 3 for this test")
                node_to_fail = non_master_nodes[-1]
                self.log.info(
                    "master_node=False: targeting non-master node %s"
                    % node_to_fail.ip)

            # Capture services before failure for the add-back step
            cb_nodes_services = get_cb_cluster_nodes_services(primary.master)
            node_key = "%s:8091" % node_to_fail.ip
            node_services = (cb_nodes_services.get(node_key)
                             or cb_nodes_services.get(node_to_fail.ip)
                             or [])
            self.log.info(
                "Target node %s | services=%s | failure_method=%s | "
                "trigger_failover=%s | failover_type=%s"
                % (node_to_fail.ip, node_services, failure_method,
                   trigger_failover, failover_type))

            # ===== Phase 1: Failure ===========================================

            if not trigger_failover:
                # Prevent autofailover from ejecting the node while it is down
                self.log.info(
                    "trigger_failover=False: disabling autofailover on primary")
                rest.update_autofailover_settings(False, 120)

            if induce_pre_failure:
                self._induce_failure(node_to_fail, failure_method)
                if failure_method == "restart_couchbase":
                    # Node self-heals; wait long enough for it to rejoin before
                    # triggering the report so the cluster state is stable
                    self.sleep(
                        60,
                        "waiting for couchbase-server to restart on %s"
                        % node_to_fail.ip)
                else:
                    # Hard failure — give the cluster time to mark the node
                    # unhealthy before triggering the report
                    self.sleep(
                        10,
                        "waiting for cluster to detect failure on %s"
                        % node_to_fail.ip)

            if remove_master and induce_pre_failure:
                # Master is down (or restarting) — find the current orchestrator
                # from any remaining live node before triggering the report
                remaining = [n for n in primary.nodes_in_cluster
                             if n.ip != node_to_fail.ip]
                self.cluster_util.find_orchestrator(primary, remaining[0])
                rest = RestConnection(primary.master)
                self.log.info(
                    "Orchestrator after failure: %s" % primary.master.ip)

            if trigger_failover:
                rest_nodes = rest.get_nodes()
                target_otp = [n for n in rest_nodes
                              if n.ip == node_to_fail.ip]
                self.assertTrue(
                    target_otp,
                    "Node %s not found in cluster node list for failover"
                    % node_to_fail.ip)
                graceful = (failover_type == "graceful")
                fo_status = rest.fail_over(
                    target_otp[0].id, graceful=graceful)
                self.assertTrue(
                    fo_status,
                    "%s failover of %s failed"
                    % (failover_type, node_to_fail.ip))
                self.log.info(
                    "%s failover of %s initiated"
                    % (failover_type, node_to_fail.ip))
                self.sleep(5, "waiting for failover to start")
                reb_result = rest.monitorRebalance()
                self.assertTrue(
                    reb_result,
                    "Rebalance after %s failover of %s failed"
                    % (failover_type, node_to_fail.ip))
                self.log.info(
                    "%s failover of %s complete"
                    % (failover_type, node_to_fail.ip))

            # Expected portal count after failure/failover
            if trigger_failover:
                expected_phase1 = initial_portal_counts[0] - 1
                phase1_label = "after %s failover" % failover_type
            else:
                expected_phase1 = initial_portal_counts[0]
                phase1_label = "after failure (no failover)"

            self._trigger_report_and_restore_interval(
                primary.master, portal_domain,
                label="primary %s" % phase1_label)

            reflected = wait_for_portal_node_count(
                self.ucp_client, cluster_uuids[0], expected_phase1,
                timeout=120, poll_interval=5)
            self.assertTrue(
                reflected,
                "Primary portal did not show %d node(s) within 120 s %s"
                % (expected_phase1, phase1_label))

            portal_cluster = get_portal_cluster(
                self.ucp_client, cluster_uuids[0])
            portal_nodes = portal_cluster.get('telemetry', {}).get('nodes', [])
            self.assertEqual(
                len(portal_nodes), expected_phase1,
                "Primary: portal shows %d node(s) %s, expected %d"
                % (len(portal_nodes), phase1_label, expected_phase1))
            self.log.info(
                "PASS - primary portal node count %s: %d"
                % (phase1_label, expected_phase1))

            for i, uuid in enumerate(cluster_uuids[1:], start=1):
                portal_cluster = get_portal_cluster(self.ucp_client, uuid)
                portal_nodes = portal_cluster.get(
                    'telemetry', {}).get('nodes', [])
                self.assertEqual(
                    len(portal_nodes), initial_portal_counts[i],
                    "Cluster %d: portal count changed unexpectedly %s: "
                    "expected %d, got %d"
                    % (i, phase1_label, initial_portal_counts[i],
                       len(portal_nodes)))
                self.log.info(
                    "PASS - cluster %d portal count unchanged %s: %d"
                    % (i, phase1_label, initial_portal_counts[i]))

            # ===== Phase 2: Recovery ==========================================

            if induce_pre_failure and failure_method in self._NEEDS_REVERT:
                self._revert_failure(node_to_fail, failure_method)
                self.sleep(
                    30,
                    "waiting for node %s to rejoin cluster after revert"
                    % node_to_fail.ip)

            if trigger_failover:
                self.log.info(
                    "Adding failed-over node %s back to cluster"
                    % node_to_fail.ip)
                self.cluster_util.add_node(
                    primary, node_to_fail,
                    services=node_services if node_services else None)
                self.log.info(
                    "Rebalance-in of %s complete" % node_to_fail.ip)

            self._trigger_report_and_restore_interval(
                primary.master, portal_domain,
                label="primary after recovery")

            reflected = wait_for_portal_node_count(
                self.ucp_client, cluster_uuids[0], initial_portal_counts[0],
                timeout=120, poll_interval=5)
            self.assertTrue(
                reflected,
                "Primary portal did not return to %d node(s) within 120 s "
                "after recovery" % initial_portal_counts[0])

            portal_cluster = get_portal_cluster(
                self.ucp_client, cluster_uuids[0])
            portal_nodes = portal_cluster.get('telemetry', {}).get('nodes', [])
            self.assertEqual(
                len(portal_nodes), initial_portal_counts[0],
                "Primary: portal shows %d node(s) after recovery, expected %d"
                % (len(portal_nodes), initial_portal_counts[0]))
            self.log.info(
                "PASS - primary portal node count after recovery: %d"
                % initial_portal_counts[0])

            for i, uuid in enumerate(cluster_uuids[1:], start=1):
                portal_cluster = get_portal_cluster(self.ucp_client, uuid)
                portal_nodes = portal_cluster.get(
                    'telemetry', {}).get('nodes', [])
                self.assertEqual(
                    len(portal_nodes), initial_portal_counts[i],
                    "Cluster %d: portal count changed after primary recovery: "
                    "expected %d, got %d"
                    % (i, initial_portal_counts[i], len(portal_nodes)))
                self.log.info(
                    "PASS - cluster %d portal count unchanged after recovery: %d"
                    % (i, initial_portal_counts[i]))

        finally:
            if not trigger_failover:
                # Re-enable autofailover that was disabled for this test
                try:
                    rest.update_autofailover_settings(True, 120)
                    self.log.info("Primary: autofailover re-enabled")
                except Exception as e:
                    self.log.warning(
                        "Primary: failed to re-enable autofailover: %s" % e)
            self.ucp_client.session_logout()
            self.log.info("Portal logout complete")

    def test_autofailover_reflected_on_portal(self):
        """
        Verify that autofailover of a node is correctly reflected on the UCP
        portal, and that other clusters' telemetry is unaffected.

        Uses ConcurrentFailoverTask — the same task class used in
        ConcurrentFailoverTests — which both induces the failure and monitors
        until the cluster's autofailover fires and the resulting rebalance
        completes.  This mirrors the AUTO path in __run_test() exactly.

        Flow:
          1. Enable autofailover with a short timeout.
          2. ConcurrentFailoverTask(task_type="induce_failure") stops the node
             and blocks until autofailover triggers and rebalance finishes.
          3. Trigger a collector report; verify portal shows N-1.
          4. ConcurrentFailoverTask(task_type="revert_failure") starts the node
             back up, then add-back via rest.add_back_node() + rebalance.
          5. Trigger a collector report; verify portal shows N.

        Params:
            failure_method (str, default "stop_couchbase"):
                Failure to induce — must be one that keeps the node down long
                enough to trigger autofailover.  "restart_couchbase" is not
                recommended here; the node typically rejoins before the timeout.
                  "stop_couchbase"  hard-stops couchbase-server.
                  "stop_memcached"  kills memcached only.

            master_node (bool, default False):
                True  — target the current orchestrator.
                False — target the last non-master node.

            timeout (int, default 30):
                Autofailover trigger timeout in seconds.

        Requires:
            nodes_init >= 3 for the primary cluster.
        """
        failure_method = self.input.param("failure_method", "stop_couchbase")
        remove_master = self.input.param("master_node", False)
        afo_timeout = self.input.param("timeout", 30)

        portal_domain = 'lighthouse.couchbase.internal'
        primary = self.cluster
        rest = RestConnection(primary.master)

        status, content, _ = self.ucp_client.session_login(
            self.ucp_portal.username, self.ucp_portal.password)
        self.assertTrue(status, "Portal login failed: %s" % content)
        self.log.info("Portal login successful")

        failure_reverted = False
        active_nodes = []
        node_to_fail = None
        try:
            # --- Initial reports for all clusters + record baseline counts ---
            cluster_uuids = []
            for i, cluster in enumerate(self.clusters):
                uuid = get_cb_cluster_uuid(cluster.master)
                self.assertIsNotNone(
                    uuid,
                    "Cluster %d: could not retrieve UUID from /pools" % i)
                cluster_uuids.append(uuid)
                self.log.info("Cluster %d UUID: %s" % (i, uuid))

                diag_status, diag_content = \
                    set_lighthouse_ns_config_via_diag_eval(
                        cluster.master,
                        reporting_endpoint=portal_domain,
                        reporting_port=LIGHTHOUSE_DEFAULT_PORTAL_PORT,
                        reporting_interval_hours=1 / 3600.0)
                self.assertTrue(
                    diag_status,
                    "Cluster %d: initial diag/eval failed: %s"
                    % (i, diag_content))

            self.sleep(10, "waiting for initial reports to fire on all clusters")

            for i, (cluster, uuid) in enumerate(
                    zip(self.clusters, cluster_uuids)):
                restore_status, restore_content = \
                    set_lighthouse_interval_via_diag_eval(cluster.master, 2)
                self.assertTrue(
                    restore_status,
                    "Cluster %d: failed to restore reporting interval: %s"
                    % (i, restore_content))
                appeared = wait_for_cluster_on_portal(
                    self.ucp_client, uuid, timeout=60, poll_interval=5)
                self.assertTrue(
                    appeared,
                    "Cluster %d did not appear on portal within 60 s"
                    % i)
                self.log.info("Cluster %d: confirmed on portal" % i)

            initial_portal_counts = []
            for i, uuid in enumerate(cluster_uuids):
                portal_cluster = get_portal_cluster(self.ucp_client, uuid)
                self.assertIsNotNone(
                    portal_cluster,
                    "Cluster %d: could not fetch cluster from portal" % i)
                count = len(
                    portal_cluster.get('telemetry', {}).get('nodes', []))
                initial_portal_counts.append(count)
                self.log.info(
                    "Cluster %d: initial portal node count = %d" % (i, count))

            # --- Pick the node to target ---
            if remove_master:
                node_to_fail = primary.master
                self.log.info(
                    "master_node=True: targeting orchestrator %s"
                    % node_to_fail.ip)
            else:
                non_master_nodes = [
                    n for n in primary.nodes_in_cluster
                    if n.ip != primary.master.ip]
                self.assertTrue(
                    non_master_nodes,
                    "Primary cluster has no non-master node; "
                    "conf requires nodes_init >= 3 for this test")
                node_to_fail = non_master_nodes[-1]
                self.log.info(
                    "master_node=False: targeting non-master node %s"
                    % node_to_fail.ip)

            cb_nodes_services = get_cb_cluster_nodes_services(primary.master)
            node_key = "%s:8091" % node_to_fail.ip
            node_services = (cb_nodes_services.get(node_key)
                             or cb_nodes_services.get(node_to_fail.ip)
                             or [])
            self.log.info(
                "Target node %s | services=%s | failure_method=%s | "
                "afo_timeout=%ds"
                % (node_to_fail.ip, node_services,
                   failure_method, afo_timeout))

            # ===== Phase 1: Enable AFO, induce failure, wait for autofailover =

            self.log.info(
                "Enabling autofailover: timeout=%ds maxCount=1" % afo_timeout)
            rest.update_autofailover_settings(True, afo_timeout, maxCount=1)

            # Identify active nodes before inducing failure.
            # When failing the master, ConcurrentFailoverTask must use a
            # surviving node as its REST monitor endpoint — if we use
            # primary.master and stop_couchbase is induced, ns_server goes down
            # on the master and the task cannot poll autofailover status.
            active_nodes = [n for n in primary.nodes_in_cluster
                            if n.ip != node_to_fail.ip]
            fo_master = active_nodes[0] if remove_master else primary.master
            self.log.info(
                "ConcurrentFailoverTask monitor node: %s" % fo_master.ip)

            # ConcurrentFailoverTask mirrors the AUTO path in __run_test():
            # it induces the failure via NodeFailureTask internally, then
            # monitors until autofailover fires and the rebalance completes.
            fo_task = ConcurrentFailoverTask(
                task_manager=self.task_manager,
                master=fo_master,
                servers_to_fail={node_to_fail: failure_method},
                expected_fo_nodes=1,
                task_type="induce_failure")
            self.task_manager.add_new_task(fo_task)
            self.task_manager.get_task_result(fo_task)
            self.assertTrue(
                fo_task.result,
                "Autofailover of %s did not complete successfully"
                % node_to_fail.ip)
            self.log.info(
                "Autofailover of %s complete" % node_to_fail.ip)

            if remove_master:
                self.cluster_util.find_orchestrator(primary, active_nodes[0])
                rest = RestConnection(primary.master)
                self.log.info(
                    "Orchestrator after autofailover: %s" % primary.master.ip)

            self._trigger_report_and_restore_interval(
                primary.master, portal_domain,
                label="primary after autofailover")

            expected_after_fo = initial_portal_counts[0] - 1
            reflected = wait_for_portal_node_count(
                self.ucp_client, cluster_uuids[0], expected_after_fo,
                timeout=120, poll_interval=5)
            self.assertTrue(
                reflected,
                "Primary portal did not show %d node(s) within 120 s "
                "after autofailover" % expected_after_fo)

            portal_cluster = get_portal_cluster(
                self.ucp_client, cluster_uuids[0])
            portal_nodes = portal_cluster.get('telemetry', {}).get('nodes', [])
            self.assertEqual(
                len(portal_nodes), expected_after_fo,
                "Primary: portal shows %d node(s) after autofailover, "
                "expected %d" % (len(portal_nodes), expected_after_fo))
            self.log.info(
                "PASS - primary portal shows %d after autofailover"
                % expected_after_fo)

            for i, uuid in enumerate(cluster_uuids[1:], start=1):
                portal_cluster = get_portal_cluster(self.ucp_client, uuid)
                portal_nodes = portal_cluster.get(
                    'telemetry', {}).get('nodes', [])
                self.assertEqual(
                    len(portal_nodes), initial_portal_counts[i],
                    "Cluster %d: portal count changed unexpectedly after "
                    "autofailover: expected %d, got %d"
                    % (i, initial_portal_counts[i], len(portal_nodes)))
                self.log.info(
                    "PASS - cluster %d portal count unchanged after "
                    "autofailover: %d" % (i, initial_portal_counts[i]))

            # ===== Phase 2: Recovery ==========================================

            # Revert failure — bring the process back up before add-back.
            # ConcurrentFailoverTask(task_type="revert_failure") mirrors the
            # revert block in ConcurrentFailoverTests.__run_test() finally.
            self.log.info("Reverting failure on %s" % node_to_fail.ip)
            revert_task = ConcurrentFailoverTask(
                task_manager=self.task_manager,
                master=fo_master,
                servers_to_fail={node_to_fail: failure_method},
                task_type="revert_failure")
            self.task_manager.add_new_task(revert_task)
            self.task_manager.get_task_result(revert_task)
            failure_reverted = True

            self.sleep(
                30,
                "waiting for %s to come back online after revert"
                % node_to_fail.ip)

            # Disable autofailover before add-back so the node cannot trigger
            # another autofailover while it is being re-integrated
            rest.update_autofailover_settings(False, afo_timeout, maxCount=1)

            # Add back the autofailed-over node using delta recovery for KV,
            # then rebalance — mirrors ConcurrentFailoverTests recovery block
            self.log.info(
                "Adding failed-over node %s back to cluster" % node_to_fail.ip)
            rest_nodes = rest.get_nodes(inactive_added=True,
                                        inactive_failed=True)
            for n in rest_nodes:
                if (n.ip == node_to_fail.ip
                        and n.clusterMembership == "inactiveFailed"):
                    rest.add_back_node(n.id)
                    if "kv" in [s.lower() for s in node_services]:
                        rest.set_recovery_type(n.id, "delta")
                        self.log.info(
                            "Set delta recovery for KV node %s"
                            % node_to_fail.ip)
                    break

            rebalance_result = self.cluster_util.rebalance(primary)
            self.assertTrue(
                rebalance_result,
                "Rebalance after adding back %s failed" % node_to_fail.ip)
            self.log.info("Rebalance-in of %s complete" % node_to_fail.ip)

            self._trigger_report_and_restore_interval(
                primary.master, portal_domain,
                label="primary after recovery")

            reflected = wait_for_portal_node_count(
                self.ucp_client, cluster_uuids[0], initial_portal_counts[0],
                timeout=120, poll_interval=5)
            self.assertTrue(
                reflected,
                "Primary portal did not return to %d node(s) within 120 s "
                "after recovery" % initial_portal_counts[0])

            portal_cluster = get_portal_cluster(
                self.ucp_client, cluster_uuids[0])
            portal_nodes = portal_cluster.get('telemetry', {}).get('nodes', [])
            self.assertEqual(
                len(portal_nodes), initial_portal_counts[0],
                "Primary: portal shows %d after recovery, expected %d"
                % (len(portal_nodes), initial_portal_counts[0]))
            self.log.info(
                "PASS - primary portal returns to %d after recovery"
                % initial_portal_counts[0])

            for i, uuid in enumerate(cluster_uuids[1:], start=1):
                portal_cluster = get_portal_cluster(self.ucp_client, uuid)
                portal_nodes = portal_cluster.get(
                    'telemetry', {}).get('nodes', [])
                self.assertEqual(
                    len(portal_nodes), initial_portal_counts[i],
                    "Cluster %d: portal count changed after primary recovery: "
                    "expected %d, got %d"
                    % (i, initial_portal_counts[i], len(portal_nodes)))
                self.log.info(
                    "PASS - cluster %d portal count unchanged after recovery: "
                    "%d" % (i, initial_portal_counts[i]))

        finally:
            # Revert failure if the test failed before Phase 2 reached it.
            # Without this, the node's process stays down and MB-7282 blocks
            # tearDown from restoring autofailover settings on any node.
            if node_to_fail is not None and not failure_reverted:
                try:
                    cleanup_master = (active_nodes[0] if active_nodes
                                      else primary.master)
                    cleanup_task = ConcurrentFailoverTask(
                        task_manager=self.task_manager,
                        master=cleanup_master,
                        servers_to_fail={node_to_fail: failure_method},
                        task_type="revert_failure")
                    self.task_manager.add_new_task(cleanup_task)
                    self.task_manager.get_task_result(cleanup_task)
                    self.sleep(30, "cleanup: waiting for %s to rejoin"
                               % node_to_fail.ip)
                    self.log.info(
                        "cleanup: failure reverted on %s" % node_to_fail.ip)
                except Exception as e:
                    self.log.warning(
                        "cleanup: could not revert failure on %s: %s"
                        % (node_to_fail.ip, e))
            self.ucp_client.session_logout()
            self.log.info("Portal logout complete")
