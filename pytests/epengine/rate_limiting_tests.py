from basetestcase import ClusterSetup
from BucketLib.BucketOperations_Rest import BucketHelper
from cb_tools.cb_cli import CbCli
from BucketLib.bucket import Bucket
from couchbase_helper.documentgenerator import doc_generator
from shell_util.remote_connection import RemoteMachineShellConnection
from cb_tools.cbstats import Cbstats


class KVRateLimitingTests(ClusterSetup):
    def setUp(self):
        super(KVRateLimitingTests, self).setUp()
        self.bucket_rest = BucketHelper(self.cluster.master)
        self.shell = RemoteMachineShellConnection(self.cluster.master)
        self.cb_cli = CbCli(self.shell)
        self.log.info("Starting KVRateLimitingTests synchronized with latest framework")

    def tearDown(self):
        if getattr(self, "shell", None):
            self.shell.disconnect()
        super(KVRateLimitingTests, self).tearDown()

    def test_bucket_limit_crud_and_cli(self):
        """
        Validate bucket creation via REST, editing via both REST and CLI,
        and verify configuration persistence via both REST and CLI.
        [P0] Combined REST and CLI validation.
        """
        bucket_name = "throttleBuck"
        params = {
            Bucket.name: bucket_name,
            Bucket.ramQuotaMB: 256,
            Bucket.throttleReserved: 5000,
            Bucket.throttleHardLimit: 10000,
            Bucket.throttleEnabled: "true"
        }

        self.log.info(f"Creating bucket {bucket_name} via REST")
        status, content = self.bucket_rest.create_bucket(params)
        self.assertTrue(status, f"Bucket creation failed: {content}")

        # Verify creation via REST
        status, content = self.bucket_rest.get_buckets_json()
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
        status, content = self.bucket_rest.get_buckets_json()
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
        status, content = self.bucket_rest.get_buckets_json()
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
        bucket_name = "limitBuck"
        params = {
            Bucket.name: bucket_name,
            Bucket.ramQuotaMB: 256,
            Bucket.throttleReserved: 100,
            Bucket.throttleHardLimit: 200,
            Bucket.throttleEnabled: "true"
        }
        self.bucket_rest.create_bucket(params)

        # Create a Bucket object representation for TAF task management
        bucket_obj = Bucket(params)
        self.cluster.buckets.append(bucket_obj)

        # Run load to trigger throttling
        gen_load = doc_generator("test_limit", 0, 10000)
        self.log.info(f"Starting to load documents to trigger throttling on {bucket_name}")
        task = self.task.async_load_gen_docs(
            self.cluster, bucket_obj, gen_load, "create", 0,
            batch_size=100, process_concurrency=4)
        self.task.jython_task_manager.get_task_result(task)

        # Verify cbstats metrics for throttle enforcement
        cb_stat = Cbstats(self.cluster.master)

        # Directly fetch specific throttle-related stats
        throttle_stats = {}

        # Fetch specific throttle metrics directly
        try:
            stats = cb_stat.get_stats(bucket_name, "")
            # Extract specific throttle-related metrics directly by key
            required_stats = ["throttle_count_total", "reject_count_total", 
                            "ru_total", "wu_total", "throttle_hard_limit", 
                            "throttle_reserved"]
            for stat_key in required_stats:
                if stat_key in stats:
                    throttle_stats[stat_key] = stats[stat_key]
        except Exception as e:
            self.log.info(f"Could not fetch throttle stats: {e}")

        cb_stat.disconnect()

        self.log.info(f"Throttle metrics from cbstats: {throttle_stats}")
        self.assertTrue(len(throttle_stats) > 0, "No throttle stats found in cbstats")
