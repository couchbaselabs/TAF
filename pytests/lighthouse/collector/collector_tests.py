# -*- coding: utf-8 -*-
"""
Collector Tests
Validates the Lighthouse Collector configuration API on the Couchbase
Server node (/internal/settings/lighthouse).

Inherits from LighthouseBase for cluster/test infrastructure.

The collector client (LighthouseCollectorClient) targets the orchestrator
CB Server node on port 8091 -- not the UCP portal.
"""
from lighthouse.lighthouse_base import LighthouseBase
from lighthouse.collector_helper_methods import (
    CB_TO_PORTAL_SERVICE_MAP,
    LIGHTHOUSE_DEFAULT_PORTAL_PORT,
    get_collector_settings,
    restore_collector_settings,
    get_cb_cluster_uuid,
    get_cb_cluster_node_count,
    get_cb_cluster_nodes_services,
    get_portal_cluster,
    get_portal_cluster_nodes,
    wait_for_cluster_on_portal,
    wait_for_portal_node_count,
    set_lighthouse_ns_config_via_diag_eval,
    set_lighthouse_interval_via_diag_eval,
    get_cb_cluster_aggregate_hardware,
    get_cb_cluster_services_union,
    assert_within_tolerance,
    get_collector_metrics,
    get_lighthouse_failure_reason_from_logs,
)
from unified_control_plane import LighthouseCollectorClient

# RFC 5737 TEST-NET-1 — documentation-reserved block, guaranteed unreachable
_UNREACHABLE_HOST = '192.0.2.1'


class CollectorTests(LighthouseBase):

    def setUp(self):
        super(CollectorTests, self).setUp()
        # Build one collector client per cluster (master node of each).
        # self.clusters is populated by LighthouseBase and always contains
        # at least the primary cluster.
        self.collector_clients = [
            LighthouseCollectorClient(cluster.master)
            for cluster in self.clusters
        ]
        # Snapshot settings on every cluster so tearDown can restore them all.
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
        super(CollectorTests, self).tearDown()

    def test_get_collector_settings_returns_defaults(self):
        """
        Verify GET /internal/settings/lighthouse on every cluster returns
        all expected keys with the correct default values from the design doc.

        Steps (repeated per cluster):
        1. Call get_collector_settings()
        2. Assert all 6 mandatory keys are present
        3. Assert each key matches the design-doc v1.4 default value
        """
        expected_defaults = {
            'enabled': True,
            'endpoint': 'lighthouse.couchbase.internal',
            'reportIntervalHours': 2,
            'reportTimeoutSeconds': 1,
            'externalNodesMaxPayloadBytes': 10240,
            'externalNodesMaxCount': 100,
        }
        for i, client in enumerate(self.collector_clients):
            self.log.info("Checking default settings on cluster %d (%s)"
                          % (i, client.ip))
            settings = get_collector_settings(client)
            self.assertIsNotNone(
                settings,
                "Cluster %d: GET /internal/settings/lighthouse returned None"
                % i)
            for key, default_val in expected_defaults.items():
                self.assertIn(
                    key, settings,
                    "Cluster %d: missing key '%s' in collector settings"
                    % (i, key))
                self.assertEqual(
                    settings[key], default_val,
                    "Cluster %d: '%s' expected %r, got %r"
                    % (i, key, default_val, settings[key]))
            self.log.info("Cluster %d: PASS - all defaults correct" % i)

    def test_verify_setting_of_configs(self):
        """
        Verify POST /internal/settings/lighthouse accepts minimum boundary
        values and rejects invalid (zero/negative) values.

        Minimum config: all fields set to their lowest documented valid values.
        No upper bound is documented so large-value testing is out of scope.

        Invalid cases: zero and negative values for reportIntervalHours and
        reportTimeoutSeconds, which ns_server enforces as positive integers.
        externalNodesMaxPayloadBytes and externalNodesMaxCount have no
        documented lower bound so they are not included in invalid cases.

        Steps (repeated per cluster):
        1. POST minimum config, GET and assert all fields persisted.
        2. POST each invalid case and assert it is rejected.
        """
        MINIMUM_CONFIG = {
            'params': {
                'enabled': False,
                'endpoint': 'a.b',
                'report_interval_hours': 1,
                'report_timeout_seconds': 1,
                'external_nodes_max_payload_bytes': 1,
                'external_nodes_max_count': 1,
            },
            'expected': {
                'enabled': False,
                'endpoint': 'a.b',
                'reportIntervalHours': 1,
                'reportTimeoutSeconds': 1,
                'externalNodesMaxPayloadBytes': 1,
                'externalNodesMaxCount': 1,
            },
        }

        INVALID_CASES = [
            {
                'label': 'negative_report_interval',
                'params': {'report_interval_hours': -1},
            },
            {
                'label': 'zero_report_interval',
                'params': {'report_interval_hours': 0},
            },
            {
                'label': 'negative_report_timeout',
                'params': {'report_timeout_seconds': -1},
            },
            {
                'label': 'zero_report_timeout',
                'params': {'report_timeout_seconds': 0},
            },
        ]

        for i, client in enumerate(self.collector_clients):
            self.log.info(
                "Cluster %d: posting minimum config" % i)
            status, content, _ = client.update_lighthouse_settings(
                **MINIMUM_CONFIG['params'])
            self.assertTrue(
                status,
                "Cluster %d: minimum config was rejected: %s" % (i, content))
            settings = get_collector_settings(client)
            self.assertIsNotNone(
                settings,
                "Cluster %d: GET after minimum config returned None" % i)
            for key, expected_val in MINIMUM_CONFIG['expected'].items():
                self.assertEqual(
                    settings.get(key), expected_val,
                    "Cluster %d: minimum config: '%s' expected %r, got %r"
                    % (i, key, expected_val, settings.get(key)))
            self.log.info("Cluster %d: PASS - minimum config" % i)

            for case in INVALID_CASES:
                label = case['label']
                self.log.info(
                    "Cluster %d: posting invalid case '%s'" % (i, label))
                status, content, _ = client.update_lighthouse_settings(
                    **case['params'])
                self.assertFalse(
                    status,
                    "Cluster %d: invalid case '%s' should have been "
                    "rejected but succeeded: %s" % (i, label, content))
                self.log.info(
                    "Cluster %d: PASS - invalid case '%s' correctly rejected"
                    % (i, label))

    def test_verify_telemetry_accuracy(self):
        """
        Verify that the collector reports accurate cluster telemetry to the
        portal for every cluster in the topology.

        Fields verified (per cluster):
          - Node count matches active nodes reported by /pools
          - Services per node match /pools node services

        Steps (repeated per cluster):
        1. Login to portal with admin credentials.
        2. Record ground truth from CB: cluster UUID, node count,
           {hostname: services} map.
        3. Ensure collector endpoint is lighthouse.couchbase.internal and
           trigger an immediate report (any POST to
           /internal/settings/lighthouse fires one).
        4. Poll portal until the cluster UUID appears (up to 60 s).
        5. Fetch nodes[] from the portal cluster record.
        6. Assert node count matches.
        7. Assert services per node match, using hostname as the join key
           with an IP-only fallback for format differences.
        8. Logout from portal.
        """
        portal_domain = 'lighthouse.couchbase.internal'

        status, content, _ = self.ucp_client.session_login(
            self.ucp_portal.username, self.ucp_portal.password)
        self.assertTrue(status, "Portal login failed: %s" % content)
        self.log.info("Portal login successful")

        try:
            for i, (cluster, client) in enumerate(
                    zip(self.clusters, self.collector_clients)):

                # --- ground truth from CB ---
                cluster_uuid = get_cb_cluster_uuid(cluster.master)
                self.assertIsNotNone(
                    cluster_uuid,
                    "Cluster %d: could not retrieve cluster UUID from "
                    "/pools" % i)
                self.log.info("Cluster %d UUID: %s" % (i, cluster_uuid))

                cb_node_count = get_cb_cluster_node_count(cluster.master)
                cb_nodes_services = get_cb_cluster_nodes_services(
                    cluster.master)
                self.log.info(
                    "Cluster %d CB ground truth: node_count=%d services=%s"
                    % (i, cb_node_count, cb_nodes_services))

                # Set endpoint, port, and ~1-second interval all at once via
                diag_status, diag_content = set_lighthouse_ns_config_via_diag_eval(
                    cluster.master,
                    reporting_endpoint=portal_domain,
                    reporting_port=LIGHTHOUSE_DEFAULT_PORTAL_PORT,
                    reporting_interval_hours=1 / 3600.0)
                self.assertTrue(
                    diag_status,
                    "Cluster %d: diag/eval failed: %s" % (i, diag_content))
                self.log.info(
                    "Cluster %d: diag/eval endpoint+port+interval set, "
                    "content=%s" % (i, diag_content))

                # Wait 10 s at 1-second interval to ensure at least one report
                # lands on the portal before restoring to 2 h.
                self.sleep(10, "waiting for initial report to fire")

                # Restore interval to 2 h to stop repeated reports from
                # polluting the portal while we poll.
                restore_status, restore_content = \
                    set_lighthouse_interval_via_diag_eval(cluster.master, 2)
                self.assertTrue(
                    restore_status,
                    "Cluster %d: failed to restore interval to 2 h: %s"
                    % (i, restore_content))
                self.log.info(
                    "Cluster %d: reporting interval restored to 2 h" % i)

                appeared = wait_for_cluster_on_portal(
                    self.ucp_client, cluster_uuid, timeout=60, poll_interval=5)
                self.assertTrue(
                    appeared,
                    "Cluster %d: UUID '%s' did not appear on portal within "
                    "60 s after report trigger" % (i, cluster_uuid))
                self.log.info(
                    "Cluster %d: cluster appeared on portal" % i)

                # --- fetch and log full portal telemetry record ---
                portal_cluster = get_portal_cluster(
                    self.ucp_client, cluster_uuid)
                self.log.info(
                    "Cluster %d: portal telemetry received: %s"
                    % (i, portal_cluster))

                # Nodes live at telemetry.nodes, not at the top level.
                portal_telemetry = portal_cluster.get('telemetry', {}) \
                    if portal_cluster else {}
                portal_nodes = portal_telemetry.get('nodes', [])
                self.assertTrue(
                    portal_nodes,
                    "Cluster %d: portal telemetry has no nodes" % i)

                # --- verify node count ---
                self.assertEqual(
                    len(portal_nodes), cb_node_count,
                    "Cluster %d: portal reports %d node(s), CB has %d"
                    % (i, len(portal_nodes), cb_node_count))
                self.log.info(
                    "Cluster %d: PASS - node count=%d" % (i, cb_node_count))

                # --- verify services per node ---
                # Portal uses IP-only hostnames; CB uses "ip:8091".
                # Portal service names differ from CB: kv->data, n1ql->query.
                for portal_node in portal_nodes:
                    hostname = portal_node.get('hostname', '')
                    portal_services = sorted(portal_node.get('services', []))

                    cb_services_raw = cb_nodes_services.get(hostname)
                    if cb_services_raw is None:
                        for cb_host, svcs in cb_nodes_services.items():
                            if cb_host.split(':')[0] == hostname:
                                cb_services_raw = svcs
                                break

                    self.assertIsNotNone(
                        cb_services_raw,
                        "Cluster %d: portal node '%s' has no matching CB "
                        "node" % (i, hostname))

                    cb_services_mapped = sorted(
                        CB_TO_PORTAL_SERVICE_MAP.get(s, s)
                        for s in cb_services_raw)

                    self.assertEqual(
                        portal_services, cb_services_mapped,
                        "Cluster %d: node '%s' portal services=%r, "
                        "CB services (mapped)=%r"
                        % (i, hostname, portal_services, cb_services_mapped))
                    self.log.info(
                        "Cluster %d: PASS - node '%s' services %r"
                        % (i, hostname, portal_services))

        finally:
            self.ucp_client.session_logout()
            self.log.info("Portal logout complete")

    def test_verify_cluster_aggregate_telemetry(self):
        """
        Verify that the portal reports accurate aggregate cluster telemetry
        for every cluster in the topology.

        Fields verified (per cluster):
          - nodeCount          exact - active node count
          - cpuPhysicalCores   exact - sum via shell /proc/cpuinfo per node
          - cpuLogicalCores    exact - sum via /nodes/self cpuCount per node
          - ramBytesTotal      within 2% - sum of /nodes/self memoryTotal
          - ramBytesUsed       within 2% - sum of (memoryTotal - memoryFree); volatile
          - storageBytesTotal  within 2% - sum of hdd[*].sizeKBytes * 1024
          - storageBytesUsed   within 2% - sum of (sizeKBytes - free) * 1024; volatile
          - services           exact - sorted union of all node services, portal-mapped
          - product.name       exact - "Couchbase Server"
          - product.edition    exact - matches CB build edition
          - product.version    exact - major.minor from CB version string

        Steps (repeated per cluster):
        1. Collect ground truth: node count, hardware aggregate, services union
           from CB REST API and shell.
        2. Set endpoint + short interval via diag/eval to trigger an immediate
           report to the portal.
        3. Poll portal until the cluster UUID appears (up to 60 s).
        4. Fetch cluster telemetry from portal.
        5. Assert each field against ground truth using the tolerances above.
        """
        portal_domain = 'lighthouse.couchbase.internal'

        status, content, _ = self.ucp_client.session_login(
            self.ucp_portal.username, self.ucp_portal.password)
        self.assertTrue(status, "Portal login failed: %s" % content)
        self.log.info("Portal login successful")

        try:
            for i, (cluster, client) in enumerate(
                    zip(self.clusters, self.collector_clients)):

                # --- ground truth from CB ---
                cluster_uuid = get_cb_cluster_uuid(cluster.master)
                self.assertIsNotNone(
                    cluster_uuid,
                    "Cluster %d: could not retrieve UUID from /pools" % i)

                cb_node_count = get_cb_cluster_node_count(cluster.master)
                expected_services = get_cb_cluster_services_union(cluster)

                hardware = get_cb_cluster_aggregate_hardware(cluster)
                self.assertIsNotNone(
                    hardware,
                    "Cluster %d: failed to fetch hardware metrics from one "
                    "or more nodes" % i)
                self.log.info(
                    "Cluster %d ground truth: uuid=%s node_count=%d "
                    "hardware=%s services=%s"
                    % (i, cluster_uuid, cb_node_count, hardware,
                       expected_services))

                # --- trigger an immediate report ---
                diag_status, diag_content = set_lighthouse_ns_config_via_diag_eval(
                    cluster.master,
                    reporting_endpoint=portal_domain,
                    reporting_port=LIGHTHOUSE_DEFAULT_PORTAL_PORT,
                    reporting_interval_hours=1 / 3600.0)
                self.assertTrue(
                    diag_status,
                    "Cluster %d: diag/eval failed: %s" % (i, diag_content))
                self.log.info(
                    "Cluster %d: report triggered, content=%s"
                    % (i, diag_content))

                # Wait 10 s at 1-second interval to ensure at least one report
                # lands on the portal before restoring to 2 h.
                self.sleep(10, "waiting for initial report to fire")

                # Restore interval to 2 h to stop repeated reports from
                # polluting the portal while we poll.
                restore_status, restore_content = \
                    set_lighthouse_interval_via_diag_eval(cluster.master, 2)
                self.assertTrue(
                    restore_status,
                    "Cluster %d: failed to restore interval to 2 h: %s"
                    % (i, restore_content))
                self.log.info(
                    "Cluster %d: reporting interval restored to 2 h" % i)

                appeared = wait_for_cluster_on_portal(
                    self.ucp_client, cluster_uuid, timeout=60, poll_interval=5)
                self.assertTrue(
                    appeared,
                    "Cluster %d: UUID '%s' did not appear on portal within "
                    "60 s" % (i, cluster_uuid))
                self.log.info("Cluster %d: cluster appeared on portal" % i)

                # --- fetch portal aggregate telemetry ---
                portal_cluster = get_portal_cluster(self.ucp_client,
                                                    cluster_uuid)
                self.assertIsNotNone(
                    portal_cluster,
                    "Cluster %d: could not fetch cluster record from portal"
                    % i)
                self.log.info(
                    "Cluster %d portal record: %s" % (i, portal_cluster))

                telemetry = portal_cluster.get('telemetry', {})
                self.assertTrue(
                    telemetry,
                    "Cluster %d: portal cluster record has no telemetry" % i)
                self.log.info(
                    "Cluster %d portal telemetry: %s" % (i, telemetry))

                # --- nodeCount (exact) - derived from len(nodes) ---
                portal_node_count = len(telemetry.get('nodes', []))
                self.assertEqual(
                    portal_node_count, cb_node_count,
                    "Cluster %d: nodeCount: portal=%r, CB=%r"
                    % (i, portal_node_count, cb_node_count))
                self.log.info(
                    "Cluster %d: PASS - nodeCount=%d" % (i, cb_node_count))

                # --- CPU cores (exact) ---
                self.assertEqual(
                    telemetry.get('cpuPhysicalCores'),
                    hardware['cpu_physical_cores'],
                    "Cluster %d: cpuPhysicalCores: portal=%r, CB=%r"
                    % (i, telemetry.get('cpuPhysicalCores'),
                       hardware['cpu_physical_cores']))
                self.assertEqual(
                    telemetry.get('cpuLogicalCores'),
                    hardware['cpu_logical_cores'],
                    "Cluster %d: cpuLogicalCores: portal=%r, CB=%r"
                    % (i, telemetry.get('cpuLogicalCores'),
                       hardware['cpu_logical_cores']))
                self.log.info(
                    "Cluster %d: PASS - cpuPhysical=%d cpuLogical=%d"
                    % (i, hardware['cpu_physical_cores'],
                       hardware['cpu_logical_cores']))

                # --- RAM (within 2%) ---
                assert_within_tolerance(self,
                    telemetry.get('ramBytesTotal', 0),
                    hardware['ram_bytes_total'],
                    tolerance_pct=2,
                    label="Cluster %d ramBytesTotal" % i)
                assert_within_tolerance(self,
                    telemetry.get('ramBytesUsed', 0),
                    hardware['ram_bytes_used'],
                    tolerance_pct=2,
                    label="Cluster %d ramBytesUsed" % i)
                self.log.info(
                    "Cluster %d: PASS - ramBytesTotal~%d ramBytesUsed~%d"
                    % (i, hardware['ram_bytes_total'],
                       hardware['ram_bytes_used']))

                # --- Storage (within 2%) ---
                assert_within_tolerance(self,
                    telemetry.get('storageBytesTotal', 0),
                    hardware['storage_bytes_total'],
                    tolerance_pct=2,
                    label="Cluster %d storageBytesTotal" % i)
                assert_within_tolerance(self,
                    telemetry.get('storageBytesUsed', 0),
                    hardware['storage_bytes_used'],
                    tolerance_pct=2,
                    label="Cluster %d storageBytesUsed" % i)
                self.log.info(
                    "Cluster %d: PASS - storageBytesTotal~%d storageBytesUsed~%d"
                    % (i, hardware['storage_bytes_total'],
                       hardware['storage_bytes_used']))

                # --- Services union (exact, sorted) ---
                portal_services = sorted(telemetry.get('services', []))
                self.assertEqual(
                    portal_services, expected_services,
                    "Cluster %d: services: portal=%r, CB (mapped)=%r"
                    % (i, portal_services, expected_services))
                self.log.info(
                    "Cluster %d: PASS - services=%r" % (i, portal_services))

                # --- Product info ---
                product = telemetry.get('product', {})
                self.assertEqual(
                    product.get('name'), 'Couchbase Server',
                    "Cluster %d: product.name: portal=%r"
                    % (i, product.get('name')))
                self.assertEqual(
                    product.get('edition'), hardware['edition'],
                    "Cluster %d: product.edition: portal=%r, CB=%r"
                    % (i, product.get('edition'), hardware['edition']))
                self.assertEqual(
                    product.get('version'), hardware['version'],
                    "Cluster %d: product.version: portal=%r, CB=%r"
                    % (i, product.get('version'), hardware['version']))
                self.log.info(
                    "Cluster %d: PASS - product=%s %s %s"
                    % (i, product.get('name'), product.get('version'),
                       product.get('edition')))

                self.log.info(
                    "Cluster %d: PASS - all aggregate telemetry fields "
                    "verified" % i)

        finally:
            self.ucp_client.session_logout()
            self.log.info("Portal logout complete")

    def test_node_rebalance_reflected_on_portal(self):
        """
        Verify that rebalancing a node out of the primary cluster is reflected
        on the portal, that adding it back is also reflected, and that the
        other clusters' telemetry is unaffected throughout.

        Requires nodes_init >= 3 for the primary cluster so a non-master
        node is available to rebalance out while keeping the cluster viable.

        Params:
            master_node (bool, default False): when True the current
                orchestrator is the node removed and added back; when False
                a non-master node is used instead.

        Steps:
        1.  Login to portal.
        2.  Trigger an initial report for every cluster (1-second interval via
            diag/eval), sleep 10 s, restore to 2 h, and confirm all cluster
            UUIDs appear on the portal.
        3.  Record the initial portal node count for every cluster.
        4.  Pick the node to remove: the current orchestrator if master_node
            is True, otherwise the last non-master node. Capture its CB
            service list for use during the rebalance-in.
        5.  Rebalance the node out of the primary cluster and wait for
            completion. If the orchestrator was removed, resolve the new
            orchestrator via find_orchestrator before proceeding.
        6.  Trigger a fresh report from the primary cluster (same diag/eval
            pattern as step 2).
        7.  Poll the portal until primary node count reaches N-1 (max 120 s).
        8.  Assert primary portal node count == N-1.
        9.  Assert all other clusters' portal node counts are unchanged.
        10. Rebalance the node back into the primary cluster and wait for
            completion.
        11. Trigger a fresh report from the primary cluster.
        12. Poll the portal until primary node count returns to N (max 120 s).
        13. Assert primary portal node count == N.
        14. Assert all other clusters' portal node counts are still unchanged.
        """
        portal_domain = 'lighthouse.couchbase.internal'
        primary = self.cluster

        status, content, _ = self.ucp_client.session_login(
            self.ucp_portal.username, self.ucp_portal.password)
        self.assertTrue(status, "Portal login failed: %s" % content)
        self.log.info("Portal login successful")

        try:
            # --- Steps 2-3: initial reports + record baseline node counts ---
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
                    "Cluster %d: diag/eval for initial report failed: %s"
                    % (i, diag_content))

            self.sleep(10, "waiting for initial reports to fire on all clusters")

            for i, (cluster, uuid) in enumerate(
                    zip(self.clusters, cluster_uuids)):
                restore_status, restore_content = \
                    set_lighthouse_interval_via_diag_eval(cluster.master, 2)
                self.assertTrue(
                    restore_status,
                    "Cluster %d: failed to restore interval to 2 h: %s"
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
                    "Cluster %d: could not fetch cluster record from portal"
                    % i)
                count = len(
                    portal_cluster.get('telemetry', {}).get('nodes', []))
                initial_portal_counts.append(count)
                self.log.info(
                    "Cluster %d: initial portal node count = %d" % (i, count))

            # --- Step 4: pick the node to remove ---
            remove_master = self.input.param("master_node", False)
            if remove_master:
                node_to_remove = primary.master
                self.log.info(
                    "master_node=True: removing orchestrator node %s"
                    % node_to_remove.ip)
            else:
                non_master_nodes = [
                    n for n in primary.nodes_in_cluster
                    if n.ip != primary.master.ip]
                self.assertTrue(
                    non_master_nodes,
                    "Primary cluster has no non-master node available; "
                    "conf requires nodes_init >= 3 for this test")
                node_to_remove = non_master_nodes[-1]
                self.log.info(
                    "master_node=False: removing non-master node %s"
                    % node_to_remove.ip)

            # Capture services before the node leaves the cluster
            cb_nodes_services = get_cb_cluster_nodes_services(primary.master)
            node_key = "%s:8091" % node_to_remove.ip
            node_services = (cb_nodes_services.get(node_key)
                             or cb_nodes_services.get(node_to_remove.ip)
                             or [])
            self.log.info(
                "Node selected for removal: %s (services: %s)"
                % (node_to_remove.ip, node_services))

            # ===== Phase 1: Rebalance out =====================================

            self.log.info(
                "Phase 1: rebalancing out node %s" % node_to_remove.ip)
            rebalance_task = self.task.async_rebalance(
                primary, to_add=[], to_remove=[node_to_remove])
            self.task_manager.get_task_result(rebalance_task)
            self.assertTrue(
                rebalance_task.result,
                "Rebalance-out of %s failed" % node_to_remove.ip)
            self.log.info("Rebalance-out complete")

            if remove_master:
                remaining = [n for n in primary.nodes_in_cluster
                             if n.ip != node_to_remove.ip]
                self.cluster_util.find_orchestrator(primary, remaining[0])
                self.log.info(
                    "New orchestrator after rebalance-out: %s"
                    % primary.master.ip)

            # Trigger report from primary after topology change
            diag_status, diag_content = set_lighthouse_ns_config_via_diag_eval(
                primary.master,
                reporting_endpoint=portal_domain,
                reporting_port=LIGHTHOUSE_DEFAULT_PORTAL_PORT,
                reporting_interval_hours=1 / 3600.0)
            self.assertTrue(
                diag_status,
                "Primary: diag/eval failed after rebalance-out: %s"
                % diag_content)

            self.sleep(10, "waiting for post-rebalance-out report to fire")

            restore_status, restore_content = \
                set_lighthouse_interval_via_diag_eval(primary.master, 2)
            self.assertTrue(
                restore_status,
                "Primary: failed to restore interval to 2 h after "
                "rebalance-out: %s" % restore_content)

            # Wait for portal to reflect removal
            expected_after_removal = initial_portal_counts[0] - 1
            reflected = wait_for_portal_node_count(
                self.ucp_client, cluster_uuids[0], expected_after_removal,
                timeout=120, poll_interval=5)
            self.assertTrue(
                reflected,
                "Primary cluster portal did not drop to %d node(s) within "
                "120 s after rebalance-out" % expected_after_removal)

            # Verify primary count
            portal_cluster = get_portal_cluster(
                self.ucp_client, cluster_uuids[0])
            portal_nodes = portal_cluster.get('telemetry', {}).get('nodes', [])
            self.assertEqual(
                len(portal_nodes), expected_after_removal,
                "Primary: portal reports %d node(s) after rebalance-out, "
                "expected %d"
                % (len(portal_nodes), expected_after_removal))
            self.log.info(
                "PASS - primary portal node count after rebalance-out: %d"
                % expected_after_removal)

            # Verify other clusters are unchanged
            for i, uuid in enumerate(cluster_uuids[1:], start=1):
                portal_cluster = get_portal_cluster(self.ucp_client, uuid)
                portal_nodes = portal_cluster.get(
                    'telemetry', {}).get('nodes', [])
                self.assertEqual(
                    len(portal_nodes), initial_portal_counts[i],
                    "Cluster %d: portal node count changed unexpectedly during "
                    "primary rebalance-out: expected %d, got %d"
                    % (i, initial_portal_counts[i], len(portal_nodes)))
                self.log.info(
                    "PASS - cluster %d portal node count unchanged after "
                    "primary rebalance-out: %d" % (i, initial_portal_counts[i]))

            # ===== Phase 2: Rebalance in ======================================

            self.log.info(
                "Phase 2: adding node %s back" % node_to_remove.ip)
            self.cluster_util.add_node(
                primary, node_to_remove,
                services=node_services if node_services else None)
            self.log.info("Rebalance-in complete")

            # Trigger report from primary after add-back
            diag_status, diag_content = set_lighthouse_ns_config_via_diag_eval(
                primary.master,
                reporting_endpoint=portal_domain,
                reporting_port=LIGHTHOUSE_DEFAULT_PORTAL_PORT,
                reporting_interval_hours=1 / 3600.0)
            self.assertTrue(
                diag_status,
                "Primary: diag/eval failed after rebalance-in: %s"
                % diag_content)

            self.sleep(10, "waiting for post-rebalance-in report to fire")

            restore_status, restore_content = \
                set_lighthouse_interval_via_diag_eval(primary.master, 2)
            self.assertTrue(
                restore_status,
                "Primary: failed to restore interval to 2 h after "
                "rebalance-in: %s" % restore_content)

            # Wait for portal to reflect the add-back
            reflected = wait_for_portal_node_count(
                self.ucp_client, cluster_uuids[0], initial_portal_counts[0],
                timeout=120, poll_interval=5)
            self.assertTrue(
                reflected,
                "Primary cluster portal did not return to %d node(s) within "
                "120 s after rebalance-in" % initial_portal_counts[0])

            # Verify primary count restored
            portal_cluster = get_portal_cluster(
                self.ucp_client, cluster_uuids[0])
            portal_nodes = portal_cluster.get('telemetry', {}).get('nodes', [])
            self.assertEqual(
                len(portal_nodes), initial_portal_counts[0],
                "Primary: portal reports %d node(s) after rebalance-in, "
                "expected %d"
                % (len(portal_nodes), initial_portal_counts[0]))
            self.log.info(
                "PASS - primary portal node count after rebalance-in: %d"
                % initial_portal_counts[0])

            # Verify other clusters still unchanged
            for i, uuid in enumerate(cluster_uuids[1:], start=1):
                portal_cluster = get_portal_cluster(self.ucp_client, uuid)
                portal_nodes = portal_cluster.get(
                    'telemetry', {}).get('nodes', [])
                self.assertEqual(
                    len(portal_nodes), initial_portal_counts[i],
                    "Cluster %d: portal node count changed unexpectedly after "
                    "primary rebalance-in: expected %d, got %d"
                    % (i, initial_portal_counts[i], len(portal_nodes)))
                self.log.info(
                    "PASS - cluster %d portal node count unchanged after "
                    "primary rebalance-in: %d" % (i, initial_portal_counts[i]))

        finally:
            self.ucp_client.session_logout()
            self.log.info("Portal logout complete")

    # ==================== Metrics tests ====================

    def test_telemetry_metrics_reflect_success_and_failure_counts(self):
        """
        Verify cm_telemetry_sends_count{result="success"} and
        cm_telemetry_sends_count{result="failure"} Prometheus metrics increment
        correctly, and that the reporting interval is respected.

        Part 1 — Success path (30 s interval, 2 min window):
            Set reporting_interval_hours = 30 s via diag/eval so the interval
            timer fires ~4 times in 2 minutes.  Assert the success counter grew
            by >= 3 and the failure counter is unchanged.
            diag/eval is used (not REST POST) so no extra immediate report fires
            — only the periodic timer drives the count.

        Part 2 — Failure path + interval validation (60 s interval, 2 min window):
            Switch to an unreachable endpoint (192.0.2.1, RFC 5737 TEST-NET-1)
            and set interval to 60 s.  With the default reportTimeoutSeconds=1,
            each attempt fails within ~1 s.  After 2 minutes, assert the failure
            counter grew by 1–3 (≈ 2 expected at 60 s cadence).  The upper bound
            of 3 distinguishes a 60 s interval from a 30 s interval, which would
            produce ~4 failures — validating that the changed interval is respected.
            Assert success counter unchanged.

        RULE: domain is restored to lighthouse.couchbase.internal before exit.
        """
        portal_domain = 'lighthouse.couchbase.internal'
        server = self.cluster.master

        try:
            # ===== Part 1: Success (30 s interval, 2 min) ====================

            self.log.info(
                "Part 1: setting 30s reporting interval to %s" % portal_domain)

            baseline = get_collector_metrics(server)
            self.log.info(
                "Part 1 baseline: success=%d failure=%d"
                % (baseline['telemetry_sends_success'],
                   baseline['telemetry_sends_failure']))

            diag_status, diag_content = set_lighthouse_ns_config_via_diag_eval(
                server,
                reporting_endpoint=portal_domain,
                reporting_port=LIGHTHOUSE_DEFAULT_PORTAL_PORT,
                reporting_interval_hours=30 / 3600.0)
            self.assertTrue(
                diag_status,
                "Part 1: diag/eval failed: %s" % diag_content)

            self.sleep(120,
                       "Part 1: waiting 2 min for ~4 reports at 30s interval")

            restore_status, restore_content = \
                set_lighthouse_interval_via_diag_eval(server, 2)
            self.assertTrue(
                restore_status,
                "Part 1: failed to restore interval to 2h: %s" % restore_content)

            after1 = get_collector_metrics(server)
            success_delta_1 = (after1['telemetry_sends_success']
                               - baseline['telemetry_sends_success'])
            failure_delta_1 = (after1['telemetry_sends_failure']
                               - baseline['telemetry_sends_failure'])
            self.log.info(
                "Part 1 result: success_delta=%d failure_delta=%d"
                % (success_delta_1, failure_delta_1))

            # 30s interval over 2 min → ~4 reports; require at least 3
            self.assertGreaterEqual(
                success_delta_1, 3,
                "Part 1: expected >= 3 successes at 30s interval over 2 min, "
                "got %d" % success_delta_1)
            self.assertEqual(
                failure_delta_1, 0,
                "Part 1: expected 0 failures on valid endpoint, got %d"
                % failure_delta_1)
            self.log.info(
                "PASS Part 1: success counter +%d" % success_delta_1)

            # ===== Part 2: Failure + interval validation (60 s, 2 min) =======

            self.log.info(
                "Part 2: setting 60s interval with unreachable endpoint %s"
                % _UNREACHABLE_HOST)

            # Set bad domain + 60s interval via diag/eval.
            # diag/eval does NOT trigger an immediate report, so only the
            # periodic timer drives the count — making the delta a clean
            # measure of how many intervals fit in the window.
            diag_status, diag_content = set_lighthouse_ns_config_via_diag_eval(
                server,
                reporting_endpoint=_UNREACHABLE_HOST,
                reporting_port=LIGHTHOUSE_DEFAULT_PORTAL_PORT,
                reporting_interval_hours=60 / 3600.0)
            self.assertTrue(
                diag_status,
                "Part 2: diag/eval failed: %s" % diag_content)

            # Baseline after config change, before any failures fire
            baseline2 = get_collector_metrics(server)
            self.log.info(
                "Part 2 baseline: success=%d failure=%d"
                % (baseline2['telemetry_sends_success'],
                   baseline2['telemetry_sends_failure']))

            self.sleep(120,
                       "Part 2: waiting 2 min for ~2 failures at 60s interval")

            # Restore domain to lighthouse.couchbase.internal before assertions
            restore_status, restore_content = \
                set_lighthouse_ns_config_via_diag_eval(
                    server,
                    reporting_endpoint=portal_domain,
                    reporting_interval_hours=2)
            self.assertTrue(
                restore_status,
                "Part 2: failed to restore domain/interval: %s"
                % restore_content)

            after2 = get_collector_metrics(server)
            success_delta_2 = (after2['telemetry_sends_success']
                               - baseline2['telemetry_sends_success'])
            failure_delta_2 = (after2['telemetry_sends_failure']
                               - baseline2['telemetry_sends_failure'])
            self.log.info(
                "Part 2 result: success_delta=%d failure_delta=%d"
                % (success_delta_2, failure_delta_2))

            # 60s interval over 2 min → ~2 failures; [1, 3] tolerates jitter.
            # Upper bound of 3 distinguishes from 30s interval (~4 failures)
            # — this is the interval-respected assertion.
            self.assertGreaterEqual(
                failure_delta_2, 1,
                "Part 2: expected >= 1 failure against unreachable endpoint, "
                "got %d" % failure_delta_2)
            self.assertLessEqual(
                failure_delta_2, 3,
                "Part 2: expected <= 3 failures at 60s interval over 2 min "
                "(> 3 would indicate interval < 60s is not respected), "
                "got %d" % failure_delta_2)
            self.assertEqual(
                success_delta_2, 0,
                "Part 2: expected 0 successes on unreachable endpoint, got %d"
                % success_delta_2)
            self.log.info(
                "PASS Part 2: failure counter +%d (interval validated)"
                % failure_delta_2)

            # ---- Log verification: confirm failure reason was logged ----------
            failure_reason = get_lighthouse_failure_reason_from_logs(server)
            self.assertIsNotNone(
                failure_reason,
                "Part 2: expected a failure reason in debug.log, found none")
            self.log.info(
                "Part 2: failure reason from debug.log: %s" % failure_reason)
            self.log.info(
                "PASS Part 2: failure reason confirmed in logs")

        finally:
            # Always restore domain to lighthouse.couchbase.internal and
            # interval to 2h — tearDown handles the REST API settings,
            # this covers the diag/eval ns_config layer.
            try:
                set_lighthouse_ns_config_via_diag_eval(
                    server,
                    reporting_endpoint=portal_domain,
                    reporting_interval_hours=2)
                self.log.info(
                    "Finally: domain restored to %s, interval to 2h"
                    % portal_domain)
            except Exception as e:
                self.log.warning(
                    "Finally: could not restore settings: %s" % e)
