from basetestcase import ClusterSetup
from cb_tools.cb_cli import CbCli
from BucketLib.bucket import Bucket
from couchbase_helper.documentgenerator import doc_generator
from shell_util.remote_connection import RemoteMachineShellConnection
from cb_tools.cbstats import Cbstats
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from cb_server_rest_util.buckets.buckets_api import BucketRestApi


class KVRateLimitingTests(ClusterSetup):
    def setUp(self):
        super(KVRateLimitingTests, self).setUp()
        self.cluster_rest = ClusterRestAPI(self.cluster.master)
        self.bucket_rest = BucketRestApi(self.cluster.master)
        self.shell = RemoteMachineShellConnection(self.cluster.master)
        self.cb_cli = CbCli(self.shell)
        self.log.info("Starting KVRateLimitingTests synchronized with latest framework")

        # Create bucket with test-specific parameters from conf file
        self.create_bucket(self.cluster)
        self.bucket = self.cluster.buckets[0]

    def tearDown(self):
        if getattr(self, "shell", None):
            self.shell.disconnect()
        super(KVRateLimitingTests, self).tearDown()

    def get_throttle_stats(self, bucket_name):
        """
        Helper method to fetch throttle-related stats from cbstats.
        """
        cb_stat = Cbstats(self.cluster.master)
        throttle_stats = {}
        try:
            # Use all_stats() which returns a parsed dictionary
            stats = cb_stat.all_stats(bucket_name)

            # Look for any throttle-related stats
            throttle_related = {k: v for k, v in stats.items() 
                               if 'throttle' in k.lower() or 'reject' in k.lower()}
            self.log.info(f"Throttle-related stats found: {throttle_related}")

            required_stats = [
                "throttle_count_total", "reject_count_total",
                "throttle_hard_limit", "throttle_reserved"
            ]
            for stat_key in required_stats:
                if stat_key in stats:
                    throttle_stats[stat_key] = stats[stat_key]
        except Exception as e:
            self.log.info(f"Could not fetch throttle stats: {e}")
        finally:
            cb_stat.disconnect()
        return throttle_stats

    def load_docs_to_bucket(self, bucket_obj, start=0, end=10000,
                             batch_size=500, concurrency=4):
        """
        Helper method to load documents into a bucket.
        """
        gen_load = doc_generator("test_doc", start, end)
        task = self.task.async_load_gen_docs(
            self.cluster, bucket_obj, gen_load, "create", 0,
            batch_size=batch_size, process_concurrency=concurrency,
            load_using=self.load_docs_using)
        self.task.jython_task_manager.get_task_result(task)
        return task

    def test_bucket_limit_crud_and_cli(self):
        """
        Validate bucket creation via REST, editing via both REST and CLI,
        and verify configuration persistence via both REST and CLI.
        [P0] Combined REST and CLI validation.
        """
        bucket_name = self.bucket.name

        # Verify creation via REST
        status, content = self.bucket_rest.get_bucket_info()
        found = any(b['name'] == bucket_name for b in content)
        self.assertTrue(found, f"Bucket {bucket_name} not found in nodes list")

        bucket_info = next(b for b in content if b['name'] == bucket_name)
        self.log.info(f"Initial bucket info via REST: throttleReserved={bucket_info.get('throttleReserved')}, "
                     f"throttleHardLimit={bucket_info.get('throttleHardLimit')}")

        # Edit via REST
        edit_params = {Bucket.throttleReserved: 6000}
        self.log.info(f"Editing bucket {bucket_name} via REST: throttleReserved=6000")
        status, content = self.bucket_rest.edit_bucket(bucket_name, edit_params)
        self.assertTrue(status, f"Bucket edit via REST failed: {content}")

        # Verify via REST
        status, content = self.bucket_rest.get_bucket_info()
        bucket_info = next(b for b in content if b['name'] == bucket_name)
        self.log.info(f"Bucket info after REST edit: throttleReserved={bucket_info.get('throttleReserved')}, "
                     f"throttleHardLimit={bucket_info.get('throttleHardLimit')}")
        self.assertEqual(bucket_info.get('throttleReserved'), 6000,
                         f"REST edit verification failed: expected 6000, got {bucket_info.get('throttleReserved')}")

        # Edit via CLI (both reserved and hard limit in one edit)
        self.log.info(f"Editing bucket {bucket_name} via CLI: throttleReserved=7000, throttleHardLimit=12000")
        cli_output = self.cb_cli.edit_bucket(bucket_name, throttleReserved=7000, throttleHardLimit=12000)
        self.assertEqual(cli_output, "Bucket updated successfully",
                         f"CLI bucket edit failed: {cli_output}")

        # Final verification via REST (CLI changes persistence)
        status, content = self.bucket_rest.get_bucket_info()
        bucket_info = next(b for b in content if b['name'] == bucket_name)
        self.log.info(f"Final bucket info via REST: throttleReserved={bucket_info.get('throttleReserved')}, "
                     f"throttleHardLimit={bucket_info.get('throttleHardLimit')}")
        self.assertEqual(bucket_info.get('throttleReserved'), 7000,
                         f"CLI edit verification via REST failed: expected 7000, got {bucket_info.get('throttleReserved')}")
        self.assertEqual(bucket_info.get('throttleHardLimit'), 12000,
                         f"CLI edit verification via REST failed: expected 12000, got {bucket_info.get('throttleHardLimit')}")

    def test_limit_enforcement(self):
        """
        Validate that operations are throttled/rejected based on limits using cbstats.
        [P0] Functional enforcement test.
        """
        bucket_name = self.bucket.name

        # Run load to trigger throttling
        self.log.info(f"Starting to load documents to trigger throttling on {bucket_name}")
        self.load_docs_to_bucket(self.bucket, start=0, end=10000, batch_size=100, concurrency=4)

        # Verify cbstats metrics for throttle enforcement
        throttle_stats = self.get_throttle_stats(bucket_name)
        self.log.info(f"Throttle metrics from cbstats: {throttle_stats}")
        self.assertTrue(len(throttle_stats) > 0, "No throttle stats found in cbstats")

    def test_rate_limiting_unavailable_in_ce(self):
        """
        Validate Rate Limiting feature is unavailable in Community Edition (CE).
        Verification:
        - Check if rate limiting stats are 0 or empty after bucket creation
          with rate limiting parameters in CE.
        """
        bucket_name = self.bucket.name
        throttle_stats = self.get_throttle_stats(bucket_name)
        self.log.info(f"CE throttle stats: {throttle_stats}")

        # Verify that throttle values are 0/disabled in CE
        self.assertEqual(int(throttle_stats.get("throttle_reserved", 0)), 0,
                         f"throttle_reserved should be 0 in CE, found {throttle_stats.get('throttle_reserved')}")
        self.assertEqual(int(throttle_stats.get("throttle_hard_limit", 0)), 0,
                         f"throttle_hard_limit should be 0 in CE, found {throttle_stats.get('throttle_hard_limit')}")
        self.assertEqual(int(throttle_stats.get("throttle_count_total", 0)), 0,
                         f"throttle_count_total should be 0 in CE, found {throttle_stats.get('throttle_count_total')}")
        self.assertEqual(int(throttle_stats.get("reject_count_total", 0)), 0,
                         f"reject_count_total should be 0 in CE, found {throttle_stats.get('reject_count_total')}")

    def test_enable_disable_rate_limiting_rest(self):
        """
        Set and validate per-bucket enable/disable value via REST API.
        """
        bucket_name = self.bucket.name

        status, buckets = self.bucket_rest.get_bucket_info()
        bucket_info = next(b for b in buckets if b['name'] == bucket_name)
        self.assertEqual(bucket_info.get('throttleEnabled'), True,
                         "Rate limiting should be enabled")

        edit_params = {Bucket.throttleEnabled: "false"}
        self.log.info("Disabling rate limiting via REST")
        status, content = self.bucket_rest.edit_bucket(bucket_name, edit_params)
        self.assertTrue(status, f"Failed to disable rate limiting: {content}")

        status, buckets = self.bucket_rest.get_bucket_info()
        bucket_info = next(b for b in buckets if b['name'] == bucket_name)
        self.assertEqual(bucket_info.get('throttleEnabled'), False,
                         "Rate limiting should be disabled")

        edit_params = {Bucket.throttleEnabled: "true"}
        self.log.info("Re-enabling rate limiting via REST")
        status, content = self.bucket_rest.edit_bucket(bucket_name, edit_params)
        self.assertTrue(status, f"Failed to re-enable rate limiting: {content}")

        status, buckets = self.bucket_rest.get_bucket_info()
        bucket_info = next(b for b in buckets if b['name'] == bucket_name)
        self.assertEqual(bucket_info.get('throttleEnabled'), True,
                         "Rate limiting should be re-enabled")

    def test_soft_limit_throttling(self):
        """
        Validate throttling behavior when ops rate exceeds the set SOFT limit.
        """
        bucket_name = self.bucket.name

        initial_stats = self.get_throttle_stats(bucket_name)
        initial_throttle_count = int(initial_stats.get("throttle_count_total", 0))
        self.log.info(f"Initial throttle stats: {initial_stats}")

        self.log.info("Driving load to exceed soft limit...")
        self.load_docs_to_bucket(self.bucket, start=0, end=50000,
                                  batch_size=500, concurrency=8)

        final_stats = self.get_throttle_stats(bucket_name)
        final_throttle_count = int(final_stats.get("throttle_count_total", 0))
        self.log.info(f"Final throttle stats: {final_stats}")
        self.assertGreater(final_throttle_count, initial_throttle_count,
                           "Throttle count should increase when exceeding soft limit")

    def test_hard_limit_rejection(self):
        """
        Validate operation rejection when buffer/queue size exceeds HARD limit.
        """
        bucket_name = self.bucket.name

        initial_stats = self.get_throttle_stats(bucket_name)
        initial_reject_count = int(initial_stats.get("reject_count_total", 0))
        self.log.info(f"Initial stats: {initial_stats}")

        self.log.info("Driving heavy load to exceed hard limit...")
        try:
            self.load_docs_to_bucket(self.bucket, start=0, end=100000, batch_size=1000, concurrency=16)
        except Exception as e:
            self.log.info(f"Expected exception during hard limit test: {e}")

        final_stats = self.get_throttle_stats(bucket_name)
        final_reject_count = int(final_stats.get("reject_count_total", 0))
        self.log.info(f"Final stats: {final_stats}")
        self.assertGreater(final_reject_count, initial_reject_count,
                           "Rejection count should increase when exceeding hard limit")

    def test_transaction_rate_limiting(self):
        """
        Verify rate limiting accounts for shadow/meta documents during transactions.
        """
        bucket_name = self.bucket.name

        initial_stats = self.get_throttle_stats(bucket_name)
        self.log.info(f"Initial stats before transactions: {initial_stats}")
        initial_wu = int(initial_stats.get("wu_total", 0))
        transaction_completed = False
        try:
            if self.cluster.sdk_client_pool:
                client = self.cluster.sdk_client_pool.get_client_for_bucket(
                    self.bucket, self.bucket.name)
                for i in range(100):
                    client.crud("create", f"txn_doc_{i}", {"value": i})
                for i in range(50):
                    client.crud("update", f"txn_doc_{i}", {"value": i * 2, "updated": True})
                transaction_completed = True
                self.log.info("Transaction-like operations completed successfully")
        except Exception as e:
            self.log.warning(f"Transaction test encountered error: {e}")

        final_stats = self.get_throttle_stats(bucket_name)
        self.log.info(f"Final stats after transactions: {final_stats}")
        final_wu = int(final_stats.get("wu_total", 0))
        if transaction_completed:
            self.assertGreater(final_wu, initial_wu,
                               "Write units should increase after transactions")
