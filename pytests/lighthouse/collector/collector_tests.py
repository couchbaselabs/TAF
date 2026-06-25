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
    get_collector_settings,
    restore_collector_settings,
)
from unified_control_plane import LighthouseCollectorClient


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
        Verify POST /internal/settings/lighthouse accepts valid min/max
        boundary values and rejects invalid (negative/zero) values.

        Valid cases (min and max):
          - minimum_config: all fields set to their lowest valid values
          - maximum_config: all fields set to large but valid values

        Invalid cases: negative/zero values for reportIntervalHours and
        reportTimeoutSeconds, which ns_server enforces as positive integers.
        externalNodesMaxPayloadBytes and externalNodesMaxCount are not
        server-validated for sign, so they are not included here.

        Steps (repeated per cluster per case):
        1. POST the config values
        2. For valid cases: GET settings and assert each key persisted
        3. For invalid cases: assert the POST was rejected
        """
        VALID_CASES = [
            {
                'label': 'minimum_config',
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
            },
            {
                'label': 'maximum_config',
                'params': {
                    'enabled': True,
                    'endpoint': 'lighthouse.couchbase.internal',
                    'report_interval_hours': 168,
                    'report_timeout_seconds': 3600,
                    'external_nodes_max_payload_bytes': 1048576,
                    'external_nodes_max_count': 10000,
                },
                'expected': {
                    'enabled': True,
                    'endpoint': 'lighthouse.couchbase.internal',
                    'reportIntervalHours': 168,
                    'reportTimeoutSeconds': 3600,
                    'externalNodesMaxPayloadBytes': 1048576,
                    'externalNodesMaxCount': 10000,
                },
            },
        ]

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
            for case in VALID_CASES:
                label = case['label']
                self.log.info(
                    "Cluster %d: posting valid case '%s'" % (i, label))
                status, content, _ = client.update_lighthouse_settings(
                    **case['params'])
                self.assertTrue(
                    status,
                    "Cluster %d: valid case '%s' was rejected: %s"
                    % (i, label, content))
                settings = get_collector_settings(client)
                self.assertIsNotNone(
                    settings,
                    "Cluster %d: GET after valid case '%s' returned None"
                    % (i, label))
                for key, expected_val in case['expected'].items():
                    self.assertEqual(
                        settings.get(key), expected_val,
                        "Cluster %d: case '%s': '%s' expected %r, got %r"
                        % (i, label, key, expected_val, settings.get(key)))
                self.log.info(
                    "Cluster %d: PASS - valid case '%s'" % (i, label))

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
