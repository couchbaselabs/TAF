"""
Helper module for rate limiting validation across different test categories.
Provides reusable methods for monitoring and verifying rate limiting behavior.
"""
import time

from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from cb_tools.cbstats import Cbstats


class ThrottlingHelper:
    """
    Helper class for rate limiting validation in existing tests.
    Can be used in collections_rebalance, failovertests, etc.
    """

    def __init__(self, cluster_master, logger):
        """
        Initialize throttling helper.

        Args:
            cluster_master: Master node for cbstats queries
            logger: Test logger instance
        """
        self.master = cluster_master
        self.log = logger

    def is_rate_limiting_enabled(self, test_input):
        """
        Check if rate limiting is enabled via test input parameters.

        Args:
            test_input: TestInputSingleton.input instance

        Returns:
            bool: True if rate limiting parameters are present
        """
        return (test_input.param("bucket_throttle_enabled", None) in [True, "true", "True", "1"] or
                test_input.param("bucket_throttle_reserved", None) is not None or
                test_input.param("bucket_throttle_hard_limit", None) is not None)

    def get_throttle_stats(self, bucket_name):
        """
        Fetch throttle-related stats from cbstats.

        Args:
            bucket_name: Name of the bucket to query

        Returns:
            dict: Dictionary of throttle stats
        """
        cb_stat = Cbstats(self.master)
        throttle_stats = {}
        try:
            stats = cb_stat.all_stats(bucket_name)
            required_stats = [
                "throttle_count_total", "reject_count_total",
                "ru_total", "wu_total", "throttle_hard_limit",
                "throttle_reserved"
            ]
            for stat_key in required_stats:
                if stat_key in stats:
                    throttle_stats[stat_key] = stats[stat_key]
        except Exception as e:
            self.log.info(f"Could not fetch throttle stats: {e}")
        finally:
            cb_stat.disconnect()
        return throttle_stats

    def verify_no_client_throttling_during_operation(self, bucket_name, initial_stats,
                                                       final_stats, tolerance=10):
        """
        Verify that client throttling did NOT occur during an operation
        (e.g., rebalance operations should not be throttled).

        Args:
            bucket_name: Name of the bucket
            initial_stats: Initial throttle stats
            final_stats: Final throttle stats
            tolerance: Maximum allowed increase in rejection count

        Returns:
            bool: True if client throttling did not occur
        """
        initial_reject_count = int(initial_stats.get("reject_count_total", 0))
        final_reject_count = int(final_stats.get("reject_count_total", 0))
        reject_delta = final_reject_count - initial_reject_count

        result = reject_delta <= tolerance
        self.log.info(f"No client throttling verification for {bucket_name}: "
                     f"Initial rejects={initial_reject_count}, "
                     f"Final rejects={final_reject_count}, "
                     f"Delta={reject_delta}, "
                     f"Within tolerance={result}")

        return result

    def log_throttle_stats_comparison(self, bucket_name, label, stats):
        """
        Log throttle stats for comparison.

        Args:
            bucket_name: Name of the bucket
            label: Label for the stats (e.g., "Pre-rebalance", "Post-rebalance")
            stats: Throttle stats dict
        """
        self.log.info(f"{label} throttle stats for {bucket_name}:")
        for key, value in stats.items():
            self.log.info(f"  {key}: {value}")

    def validate_memcached_throttle_settings(self, nodes, bucket_name,
                                             reserved, hard_limit, timeout=180,
                                             poll_interval=5, log_interval=30):
        """
        Poll until memcached on every node runs with throttling enabled
        and the given bucket limits.

        throttle_enabled comes from each node's effective memcached settings
        (GET /pools/default/settings/memcached/effective/self), and the
        limits from that node's `cbstats all` for bucket_name.

        Args:
            timeout: seconds to wait for all nodes to match
            poll_interval: seconds between polls
            log_interval: how often (seconds) to log per-node progress

        Returns:
            (bool, dict): True if all nodes match within timeout, and the
            last per-node reading {ip: {stat: value}}
        """
        expected = {"throttle_enabled": True, "throttle_reserved": reserved,
                    "throttle_hard_limit": hard_limit}
        start_time = time.time()
        end_time = start_time + timeout
        next_log = start_time
        while True:
            actual = dict()
            for node in nodes:
                status, effective = \
                    ClusterRestAPI(node).get_effective_memcached_setting()
                stats = ThrottlingHelper(node, self.log).get_throttle_stats(
                    bucket_name)
                actual[node.ip] = {
                    "throttle_enabled":
                        effective.get("throttle_enabled") if status else None,
                    "throttle_reserved": int(stats.get("throttle_reserved", -1)),
                    "throttle_hard_limit": int(stats.get("throttle_hard_limit",
                                                         -1))}
            elapsed = int(time.time() - start_time)
            if actual and all(mc == expected for mc in actual.values()):
                self.log.info(f"memcached throttle settings for bucket "
                              f"{bucket_name} match on all {len(actual)} "
                              f"nodes after {elapsed}s: {actual}")
                return True, actual
            if time.time() > end_time:
                mismatched = [ip for ip, mc in actual.items() if mc != expected]
                self.log.error(f"memcached throttle settings for bucket "
                               f"{bucket_name} still differ after {elapsed}s "
                               f"on {mismatched}. Per node: {actual}, "
                               f"expected: {expected}")
                return False, actual
            if time.time() >= next_log:
                self.log.info(f"Waiting for memcached throttle settings for "
                              f"bucket {bucket_name} ({elapsed}s/{timeout}s). "
                              f"Per node: {actual}, expected: {expected}")
                next_log = time.time() + log_interval
            time.sleep(poll_interval)

