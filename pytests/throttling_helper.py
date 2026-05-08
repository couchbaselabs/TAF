"""
Helper module for rate limiting validation across different test categories.
Provides reusable methods for monitoring and verifying rate limiting behavior.
"""
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

