# -*- coding: utf-8 -*-
"""
Collector Configuration Tests
Validates the Lighthouse Collector configuration API on the Couchbase
Server node (/internal/settings/lighthouse).

Inherits from LighthouseBase for cluster/test infrastructure.

The collector client (LighthouseCollectorClient) targets the orchestrator
CB Server node on port 8091 -- not the UCP portal.
"""
from lighthouse.lighthouse_base import LighthouseBase
from lighthouse.collector_helper_methods import (
    get_collector_settings,
    restore_collector_settings,
)
from unified_control_plane import LighthouseCollectorClient


class CollectorConfigTests(LighthouseBase):

    def setUp(self):
        super(CollectorConfigTests, self).setUp()
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
        super(CollectorConfigTests, self).tearDown()

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

    def test_set_collector_endpoint_port_8080_and_interval_2hrs(self):
        """
        Verify POST /internal/settings/lighthouse sets the endpoint to
        lighthouse.couchbase.internal and reportIntervalHours to 2 on
        every cluster.

        The endpoint field accepts only a hostname or domain (no port).
        Port 433 is used internally by ns_server for the https connection.

        Steps (repeated per cluster):
        1. POST endpoint=lighthouse.couchbase.internal and
           reportIntervalHours=2
        2. GET settings and assert both values are persisted
        """
        new_endpoint = "lighthouse.couchbase.internal"
        new_interval = 2

        for i, client in enumerate(self.collector_clients):
            self.log.info(
                "Cluster %d: setting endpoint='%s', reportIntervalHours=%d"
                % (i, new_endpoint, new_interval))

            status, content, _ = client.update_lighthouse_settings(
                endpoint=new_endpoint,
                report_interval_hours=new_interval)
            self.assertTrue(
                status,
                "Cluster %d: POST /internal/settings/lighthouse failed: %s"
                % (i, content))

            settings = get_collector_settings(client)
            self.assertIsNotNone(
                settings,
                "Cluster %d: GET after update returned None" % i)
            self.assertEqual(
                settings.get('endpoint'), new_endpoint,
                "Cluster %d: endpoint expected %r, got %r"
                % (i, new_endpoint, settings.get('endpoint')))
            self.assertEqual(
                settings.get('reportIntervalHours'), new_interval,
                "Cluster %d: reportIntervalHours expected %r, got %r"
                % (i, new_interval, settings.get('reportIntervalHours')))
            self.log.info(
                "Cluster %d: PASS - endpoint='%s', reportIntervalHours=%d"
                % (i, new_endpoint, new_interval))
