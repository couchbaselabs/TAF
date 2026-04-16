import re
import time
import yaml
import os

from StatsLib.StatsOperations import StatsHelper
from shell_util.remote_connection import RemoteMachineShellConnection


class MetricSeriesHelper(object):
    def __init__(self, metric_name, labels=None):
        self.metric_name = metric_name
        self.labels = labels or {}
        label_predicate = "".join(
            [r'(?=[^}}]*\b{0}="{1}")'.format(re.escape(k), re.escape(v))
             for k, v in self.labels.items()]
        )
        self._metric_pattern = re.compile(
            r'^' + re.escape(self.metric_name) +
            r'(?:\{' + label_predicate + r'[^}]*\})?\s+([0-9]+(?:\.[0-9]+)?)(?:\s+[0-9]+)?\s*$'
        )

    def get_value(self, metrics_lines):
        for line in metrics_lines:
            if not line or line.startswith("#"):
                continue
            m = self._metric_pattern.match(line.strip())
            if m:
                return float(m.group(1))
        return 0.0

    def get_matching_lines(self, metrics_lines, limit=10):
        def line_matches(line):
            if self.metric_name not in line:
                return False
            for k, v in self.labels.items():
                if '{0}="{1}"'.format(k, v) not in line:
                    return False
            return True

        return [l for l in metrics_lines if line_matches(l)][:limit]


class StatsBasicOpsUtil(object):
    def __init__(self, log):
        self.log = log

    def load_metrics_config(self, test_input):
        config_path = test_input.param(
            "metrics_config_file",
            "conf/scalable_stats/metrics_info.yml")
        if not os.path.exists(config_path):
            fallback_path = "conf/scalable_stats/metrics_info.yml"
            if os.path.exists(fallback_path):
                self.log.warning("Config file %s not found, falling back to %s",
                                 config_path, fallback_path)
                config_path = fallback_path
        with open(config_path, "r") as fp:
            cfg = yaml.safe_load(fp) or {}
        return cfg

    @staticmethod
    def fetch_metrics(server):
        return StatsHelper(server).get_all_metrics()

    @staticmethod
    def validate_metrics(server, content, metrics_validation_cfg=None, policy_name="default"):
        metrics_validation_cfg = metrics_validation_cfg or {}
        configured_allowed_prefixes = metrics_validation_cfg.get("allowed_prefixes")
        policies = metrics_validation_cfg.get("policies", {})
        policy = policies.get(policy_name, policies.get("default", {}))
        fail_on_unknown_prefix = bool(policy.get("fail_on_unknown_prefix", False))
        skip_prefix_validation = bool(policy.get("skip_prefix_validation", False))
        allowed_prefixes = None if skip_prefix_validation else configured_allowed_prefixes
        StatsHelper(server)._validate_metrics(
            content,
            allowed_prefixes=allowed_prefixes,
            fail_on_unknown_prefix=fail_on_unknown_prefix
        )

    def log_metric_snapshot(self, lines, metric_helper, stage):
        matched = metric_helper.get_matching_lines(lines)
        value = metric_helper.get_value(lines)
        self.log.info("[%s] %s parsed_value=%s labels=%s",
                      stage, metric_helper.metric_name, value, metric_helper.labels)
        if matched:
            self.log.info("[%s] matching metric line(s): %s", stage, matched[:10])
        else:
            self.log.info("[%s] no matching metric line found for labels=%s",
                          stage, metric_helper.labels)
        return value

    def wait_for_metric_increment(self, get_current_value_fn, metric_helper, expected_floor, sleep_fn,
                                  timeout_sec=120, poll_interval_sec=5, wait_reason=None):
        end = time.time() + timeout_sec
        last_seen = None
        target = float(expected_floor) + 1.0
        while time.time() < end:
            val = get_current_value_fn()
            last_seen = val
            if val >= target:
                return val
            sleep_fn(poll_interval_sec, wait_reason or "Waiting for metric increment")
        raise AssertionError("Timed out waiting for {0}{{type=\"{1}\"}} to reach >= {2}. "
                             "ExpectedFloor={3} LastSeen={4}"
                             .format(metric_helper.metric_name, metric_helper.labels,
                                     target, expected_floor, last_seen))

    @staticmethod
    def block_traffic_between_nodes(node1, node2):
        """Block bidirectional network traffic between two nodes using iptables"""
        shell = RemoteMachineShellConnection(node1)
        shell.execute_command("iptables -A INPUT -s {0} -j DROP".format(node2.ip))
        shell.execute_command("iptables -A OUTPUT -d {0} -j DROP".format(node2.ip))
        shell.disconnect()

    @staticmethod
    def restore_network_connectivity(node1, node2):
        """Restore network connectivity between two nodes by removing iptables rules"""
        shell = RemoteMachineShellConnection(node1)
        shell.execute_command("iptables -D INPUT -s {0} -j DROP || true".format(node2.ip))
        shell.execute_command("iptables -D OUTPUT -d {0} -j DROP || true".format(node2.ip))
        shell.disconnect()

    def remove_all_network_partitions(self, nodes):
        """Remove all iptables rules from specified nodes (cleanup)"""
        for node in nodes:
            try:
                shell = RemoteMachineShellConnection(node)
                shell.execute_command("/sbin/iptables -F")
                shell.disconnect()
            except Exception as e:
                self.log.warning("Failed to clear rules on {0}: {1}".format(node.ip, e))
