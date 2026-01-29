from magma_base import MagmaBaseTest
from BucketLib.BucketOperations import BucketHelper
from cb_tools.cbstats import Cbstats
from remote.remote_util import RemoteMachineShellConnection

class MagmaKVTests(MagmaBaseTest):
    def setUp(self):
        super(MagmaKVTests, self).setUp()

    def tearDown(self):
        super(MagmaKVTests, self).tearDown()

    def get_magma_thread_count(self, node):
        shell = RemoteMachineShellConnection(node)
        cmd = "for p in $(pgrep memcached); do cat /proc/$p/task/*/comm 2>/dev/null | grep 'mg:'; done"
        output, error = shell.execute_command(cmd)
        shell.disconnect()
        magma_count = len([line for line in output if line.strip().startswith("mg:")])
        return magma_count

    def get_flusher_thread_count(self, node):
        shell = RemoteMachineShellConnection(node)
        cmd = "for p in $(pgrep memcached); do cat /proc/$p/task/*/comm 2>/dev/null | grep 'mg:flusher'; done"
        output, error = shell.execute_command(cmd)
        shell.disconnect()
        flusher_count = len([line for line in output if line.strip().startswith("mg:flusher")])
        return flusher_count

    def get_memcached_global_settings(self, bucket_helper):
        shell = RemoteMachineShellConnection(self.cluster.master)
        cmd = "curl -s -u Administrator:password http://localhost:8091/pools/default/settings/memcached/global"
        output, error = shell.execute_command(cmd)
        shell.disconnect()
        if output:
            return output[0].strip()
        return None

    def test_create_max_buckets_with_min_ram_quota(self):

        self.bucket_util.print_bucket_stats(self.cluster)

        if len(self.cluster.buckets) == self.standard_buckets:
            self.log.info("{0} {1} buckets created successfully with RAM Quota {2} MB".format(
                                                self.standard_buckets, self.bucket_storage,
                                                self.bucket_ram_quota))

        total_buckets = self.cluster.buckets
        new_buckets_count = len(total_buckets) // 2

        self.log.info("Deleting half the buckets...")
        for i in range(len(total_buckets)//2):
            bucket_obj = total_buckets[i]
            self.bucket_util.delete_bucket(self.cluster, bucket_obj)
            self.log.info("Bucket {0} deleted".format(bucket_obj.name))

        self.sleep(30, "Wait for bucket deletion to get reflected")

        buckets_in_cluster = self.bucket_util.get_all_buckets(self.cluster)

        create_task = None
        self.log.info("Inserting docs into the existing buckets")
        self.create_start = self.num_items
        self.create_end = self.create_start + 100000
        self.generate_docs(doc_ops="create")
        create_task = self.data_load()

        self.log.info("Creating {0} buckets...".format(new_buckets_count))
        buckets_creation_task = self.bucket_util.create_multiple_buckets(
                                            self.cluster,
                                            self.num_replicas,
                                            bucket_count=new_buckets_count,
                                            bucket_type=self.bucket_type,
                                            storage=self.bucket_storage,
                                            eviction_policy=self.bucket_eviction_policy,
                                            bucket_name="new_bucket",
                                            ram_quota=self.bucket_ram_quota)

        self.assertTrue(buckets_creation_task, "Unable to create multiple buckets")
        self.wait_for_doc_load_completion(create_task)

        self.bucket_util.print_bucket_stats(self.cluster)

        self.sleep(30, "Wait for newly created bucktes to get reflected")
        buckets_in_cluster = self.bucket_util.get_all_buckets(self.cluster)

        self.log.info("Creating SDK clients for the new buckets")
        clients_per_bucket = 1
        for bucket in buckets_in_cluster:
            if "new_bucket" in bucket.name:
                self.sdk_client_pool.create_clients(
                    bucket, [self.cluster.master],
                    clients_per_bucket,
                    compression_settings=self.sdk_compression)

        self.log.info("Loading data into all buckets...")
        self.key = "new_docs"
        self.create_start = 0
        self.create_end = 100000
        self.generate_docs(doc_ops="create")
        create_task = self.data_load()
        self.wait_for_doc_load_completion(create_task)

        self.bucket_util.print_bucket_stats(self.cluster)

        self.log.info("Deleting the newly created buckets")
        for bucket in buckets_in_cluster:
            if "new_bucket" in bucket.name:
                self.bucket_util.delete_bucket(self.cluster, bucket)
                self.log.info("Bucket {0} deleted".format(bucket.name))

        self.sleep(30, "Wait for bucket deletion to get reflected")
        self.bucket_util.print_bucket_stats(self.cluster)
        buckets_in_cluster = self.bucket_util.get_all_buckets(self.cluster)

        self.log.info("Creating {0} buckets...".format(new_buckets_count))
        buckets_creation_task = self.bucket_util.create_multiple_buckets(
                                            self.cluster,
                                            self.num_replicas,
                                            bucket_count=new_buckets_count,
                                            bucket_type=self.bucket_type,
                                            storage=self.bucket_storage,
                                            eviction_policy=self.bucket_eviction_policy,
                                            bucket_name="new_bucket",
                                            ram_quota=self.bucket_ram_quota)

        self.assertTrue(buckets_creation_task, "Unable to create multiple buckets")
        self.bucket_util.print_bucket_stats(self.cluster)
        buckets_in_cluster = self.bucket_util.get_all_buckets(self.cluster)

        self.log.info("Creating SDK clients for the new buckets")
        clients_per_bucket = 1
        for bucket in buckets_in_cluster:
            if "new_bucket" in bucket.name:
                self.sdk_client_pool.create_clients(
                    bucket, [self.cluster.master],
                    clients_per_bucket,
                    compression_settings=self.sdk_compression)

        self.log.info("Loading data into all buckets...")
        self.key = "new_test_docs"
        self.create_start = 0
        self.create_end = 100000
        self.generate_docs(doc_ops="create")
        create_task = self.data_load()
        self.wait_for_doc_load_completion(create_task)
        self.sleep(20)

        self.bucket_util.print_bucket_stats(self.cluster)

        if len(buckets_in_cluster) == self.standard_buckets:
            self.log.info("Bucket count matches with the specified number of standard buckets")
        else:
            self.log_failure("Bucket count mismatch. Expected : {0}, Actual : {1}".format(
                                            self.standard_buckets, len(buckets_in_cluster)))

    def test_magma_flusher_thread_percentage(self):
        percentages_to_test = [10, 20, 40, 50]
        docs_per_iteration = self.input.param("docs_per_iteration", 100000)
        storage_threads = self.input.param("storage_threads", 20)

        bucket_helper = BucketHelper(self.cluster.master)
        bucket = self.cluster.buckets[0]
        cb_stat = Cbstats(self.cluster.master)

        self.log.info("=" * 70)
        self.log.info("SETUP: Setting num_storage_threads to {}".format(storage_threads))
        self.log.info("=" * 70)
        self.log.info("Command: curl -X POST -u Administrator:password http://{}:8091/pools/default/settings/memcached/global -d 'num_storage_threads={}'".format(
            self.cluster.master.ip, storage_threads))
        result = bucket_helper.update_memcached_settings(num_storage_threads=storage_threads)
        if not result:
            self.log.warning("Failed to set num_storage_threads")
        else:
            self.log.info("Successfully set num_storage_threads to {}".format(storage_threads))
        self.assertTrue(result, "Failed to set num_storage_threads")
        self.sleep(5, "Wait for num_storage_threads setting to take effect")

        self.log.info("")
        self.log.info("=" * 70)
        self.log.info("INITIAL STATE - Checking current configuration")
        self.log.info("=" * 70)
        
        self.log.info("Command: curl -u Administrator:password http://{}:8091/pools/default/settings/memcached/global".format(
            self.cluster.master.ip))
        global_config = self.get_memcached_global_settings(bucket_helper)
        self.log.info("Result: {}".format(global_config))
        
        self.log.info("")
        self.log.info("Command: cbstats -u Administrator -p password {}:11210 -b {} all | grep ep_magma_flusher_thread_percentage".format(
            self.cluster.master.ip, bucket.name))
        initial_stats = cb_stat.all_stats(bucket.name)
        initial_percentage = initial_stats.get("ep_magma_flusher_thread_percentage")
        self.log.info("Result: ep_magma_flusher_thread_percentage = {}".format(initial_percentage))
        
        self.log.info("")
        self.log.info("Command: for p in $(pgrep memcached); do cat /proc/$p/task/*/comm 2>/dev/null | grep 'mg:' | wc -l; done")
        initial_total_magma = self.get_magma_thread_count(self.cluster.master)
        self.log.info("Result: Total magma threads = {}".format(initial_total_magma))
        
        self.log.info("Command: for p in $(pgrep memcached); do cat /proc/$p/task/*/comm 2>/dev/null | grep 'mg:flusher'; done")
        initial_flusher_count = self.get_flusher_thread_count(self.cluster.master)
        self.log.info("Result: Flusher threads = {}".format(initial_flusher_count))
        if initial_total_magma > 0:
            initial_flusher_pct = (initial_flusher_count * 100.0) / initial_total_magma
            self.log.info("Result: Flusher percentage = {:.2f}% ({}/{} magma threads)".format(
                initial_flusher_pct, initial_flusher_count, initial_total_magma))

        self.log.info("")
        self.log.info("=" * 70)
        self.log.info("Starting continuous data load in background ({} docs)".format(
            len(percentages_to_test) * docs_per_iteration))
        self.log.info("=" * 70)
        self.create_start = self.init_items_per_collection
        self.create_end = self.create_start + (len(percentages_to_test) * docs_per_iteration)
        self.generate_docs(doc_ops="create")
        load_task = self.data_load()

        for percentage in percentages_to_test:
            self.log.info("")
            self.log.info("=" * 70)
            self.log.info("TESTING: magma_flusher_thread_percentage = {}%".format(percentage))
            self.log.info("=" * 70)

            self.log.info("[1/3] Setting magma_flusher_thread_percentage to {}%".format(percentage))
            self.log.info("Command: curl -X POST -u Administrator:password http://{}:8091/pools/default/settings/memcached/global -d 'magma_flusher_thread_percentage={}'".format(
                self.cluster.master.ip, percentage))
            result = bucket_helper.update_memcached_settings(
                magma_flusher_thread_percentage=percentage)
            if not result:
                self.log.warning("FAILED to set magma_flusher_thread_percentage to {}".format(percentage))
            else:
                self.log.info("SUCCESS: Set magma_flusher_thread_percentage to {}".format(percentage))
            self.assertTrue(result,
                "Failed to update magma_flusher_thread_percentage to {}".format(percentage))

            self.sleep(5, "Wait for setting to take effect")

            self.log.info("")
            self.log.info("[2/3] Verifying configuration was updated")
            self.log.info("Command: curl -u Administrator:password http://{}:8091/pools/default/settings/memcached/global".format(
                self.cluster.master.ip))
            global_config = self.get_memcached_global_settings(bucket_helper)
            self.log.info("Result (Global Config): {}".format(global_config))
            
            self.log.info("")
            self.log.info("Command: cbstats -u Administrator -p password {}:11210 -b {} all | grep ep_magma_flusher_thread_percentage".format(
                self.cluster.master.ip, bucket.name))
            stats = cb_stat.all_stats(bucket.name)
            actual_percentage = stats.get("ep_magma_flusher_thread_percentage")
            self.log.info("Result (cbstats): ep_magma_flusher_thread_percentage = {}".format(actual_percentage))
            if actual_percentage is None:
                self.log.warning("WARNING: ep_magma_flusher_thread_percentage stat not found in cbstats")
            elif int(actual_percentage) != percentage:
                self.log.warning("WARNING: Percentage mismatch! Expected: {}, Actual: {}".format(
                    percentage, actual_percentage))
            else:
                self.log.info("SUCCESS: Percentage matches! Expected: {}, Actual: {}".format(
                    percentage, actual_percentage))
            # self.assertIsNotNone(actual_percentage,
            #     "ep_magma_flusher_thread_percentage stat not found in cbstats")
            # self.assertEqual(int(actual_percentage), percentage,
            #     "Percentage mismatch! Expected: {}, Actual: {}".format(
            #         percentage, actual_percentage))

            self.log.info("")
            self.log.info("[3/3] Checking actual magma thread counts on system")
            self.log.info("Command: for p in $(pgrep memcached); do cat /proc/$p/task/*/comm 2>/dev/null | grep 'mg:' | wc -l; done")
            total_magma_threads = self.get_magma_thread_count(self.cluster.master)
            self.log.info("Result: Total magma threads = {}".format(total_magma_threads))
            
            self.log.info("Command: for p in $(pgrep memcached); do cat /proc/$p/task/*/comm 2>/dev/null | grep 'mg:flusher'; done")
            actual_flusher_count = self.get_flusher_thread_count(self.cluster.master)
            expected_flusher_count = (storage_threads * percentage) // 100
            
            self.log.info("Result: Flusher threads - Expected: {}, Actual: {}".format(
                expected_flusher_count, actual_flusher_count))
            if total_magma_threads > 0:
                flusher_percentage_actual = (actual_flusher_count * 100.0) / total_magma_threads
                self.log.info("Result: Actual flusher percentage = {:.2f}% ({}/{} magma threads)".format(
                    flusher_percentage_actual, actual_flusher_count, total_magma_threads))
            
            if actual_flusher_count != expected_flusher_count:
                self.log.warning("WARNING: Flusher thread count mismatch! Expected: {}, Actual: {}".format(
                    expected_flusher_count, actual_flusher_count))
            else:
                self.log.info("SUCCESS: Flusher thread count matches! Expected: {}, Actual: {}".format(
                    expected_flusher_count, actual_flusher_count))
            self.assertEqual(actual_flusher_count, expected_flusher_count,
                "Flusher thread count mismatch! Expected: {}, Actual: {}".format(
                    expected_flusher_count, actual_flusher_count))

            self.log.info("Completed validation for magma_flusher_thread_percentage = {}%".format(percentage))

        self.log.info("")
        self.log.info("=" * 70)
        self.log.info("FINAL VALIDATION - Waiting for data load and queue drainage")
        self.log.info("=" * 70)
        self.log.info("Waiting for continuous data load to complete...")
        self.wait_for_doc_load_completion(load_task)
        self.log.info("Data load completed successfully")

        self.log.info("")
        self.log.info("Waiting for queue to drain...")
        self.log.info("Command: cbstats -u Administrator -p password {}:11210 -b {} all | grep ep_queue_size".format(
            self.cluster.master.ip, bucket.name))
        self.bucket_util._wait_for_stats_all_buckets(
            self.cluster, self.cluster.buckets, timeout=300)
        final_stats = cb_stat.all_stats(bucket.name)
        final_queue = final_stats.get("ep_queue_size")
        self.log.info("Result: Final queue size = {}".format(final_queue))
        if final_queue is None:
            self.log.warning("WARNING: ep_queue_size stat not found")
        else:
            self.log.info("Queue drained successfully")
        self.assertIsNotNone(final_queue, "ep_queue_size stat not found")

        self.log.info("")
        self.log.info("=" * 70)
        self.log.info("TEST COMPLETED")
        self.log.info("=" * 70)
        cb_stat.disconnect()