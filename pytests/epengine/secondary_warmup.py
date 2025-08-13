import random
import string
import threading
import time

from BucketLib.bucket import Bucket
from basetestcase import BaseTestCase
from bucket_collections.collections_base import CollectionBase
from cb_constants.CBServer import CbServer
from cb_tools.cbstats import Cbstats
from sdk_client3 import SDKClient
from shell_util.remote_connection import RemoteMachineShellConnection
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI

class SecondaryWarmup(BaseTestCase):

    def setUp(self):
        super(SecondaryWarmup, self).setUp()
        self.spec_name = self.input.param("bucket_spec", "single_bucket.default")
        self.data_spec_name = self.input.param("data_spec_name", "initial_load")
        self.ops_rate = self.input.param("ops_rate", 50000)
        self.durability_min_level = self.input.param("durability_min_level",
                            Bucket.DurabilityMinLevel.MAJORITY_AND_PERSIST_ACTIVE)

        self.num_reader_threads = self.input.param("num_reader_threads", 1)

        # warmup triggers -> kill_memcached, change_eviction_policy
        self.warmup_trigger = self.input.param("warmup_trigger", "kill_memcached")
        self.new_eviction_policy = self.input.param("new_eviction_policy",
                                                    Bucket.EvictionPolicy.FULL_EVICTION)

        self.update_warmup_behavior = self.input.param("update_warmup_behavior", False)
        self.validate_warmup = self.input.param("validate_warmup", True)

        if self.nodes_init > 1:
            nodes_in = self.cluster.servers[1:self.nodes_init]
            result = self.task.rebalance(self.cluster, nodes_in, [],
                                        services=["kv"]*len(nodes_in))
            self.assertTrue(result, "Initial rebalance failed")

        self.cluster.kv_nodes = self.cluster_util.get_nodes_from_services_map(
                                    self.cluster,
                                    service_type=CbServer.Services.KV,
                                    get_all_nodes=True)

        # Update num_reader_threads to slow the warmup process
        ClusterRestAPI(self.cluster.master).manage_global_memcached_setting(
            num_reader_threads=self.num_reader_threads)

        # Creating buckets from spec file
        CollectionBase.deploy_buckets_from_spec_file(self)

        for bucket in self.cluster.buckets:
            self.log.info(f"Bucket: {bucket.name}, "
                          f"Warmup Behvaior: {bucket.warmupBehavior}")

        # Create clients in SDK client pool
        CollectionBase.create_clients_for_sdk_pool(self)

        # Initial load
        CollectionBase.load_data_from_spec_file(self, self.data_spec_name,
                                                validate_docs=False)

        self.sleep(30, "Wait for num_items to get reflected")
        self.bucket_util.print_bucket_stats(self.cluster)

    def tearDown(self):
        self.cluster_util.print_cluster_stats(self.cluster)
        super(SecondaryWarmup, self).tearDown()

    def kill_memcached_on_nodes(self, nodes):
        for node in nodes:
            self.log.info(f"Killing memcached on node: {node.ip}")
            shell = RemoteMachineShellConnection(node)
            shell.kill_memcached()
            shell.disconnect()

    def insert_docs(self, key_prefix, bucket, num_docs):

        insert_sdk_client = SDKClient(self.cluster, bucket,
                                      scope=CbServer.default_scope,
                                      collection=CbServer.default_collection)
        document = {
            "num": 1000
        }
        insert_result = list()
        self.log.info(f"Inserting {num_docs} documents to bucket: {bucket.name}")
        for i in range(num_docs):
            key = key_prefix + str(i+1)
            res = insert_sdk_client.insert(key, document, timeout=5)
            insert_result.append(res['status'])
            self.log.info(
                f"Bucket: {bucket.name}, Key: {key}, Result of insert = {res}"
            )
            time.sleep(1)

        insert_sdk_client.close()
        self.insert_bucket_dict[bucket.name] = insert_result

    def read_docs(self, key_prefix, bucket, num_docs):

        read_sdk_client = SDKClient(self.cluster, bucket,
                                    scope=CbServer.default_scope,
                                    collection=CbServer.default_collection)

        read_result = list()
        self.log.info(f"Reading {num_docs} documents from bucket: {bucket.name}")
        for i in range(num_docs):
            key = key_prefix + str(i+1)
            res = read_sdk_client.read(key)
            read_result.append(res['status'])
            self.log.info(
                f"Bucket: {bucket.name}, Key: {key}, Result of read = {res}"
            )
            time.sleep(1)

        read_sdk_client.close()
        self.read_bucket_dict[bucket.name] = read_result

    def upsert_docs(self, key_prefix, bucket, num_docs):

        upsert_sdk_client = SDKClient(self.cluster, bucket,
                                      scope=CbServer.default_scope,
                                      collection=CbServer.default_collection)
        document = {
            "num": 2000
        }
        upsert_result = list()
        self.log.info(f"Upserting {num_docs} documents in bucket: {bucket.name}")
        for i in range(num_docs):
            key = key_prefix + str(i+1)
            res = upsert_sdk_client.upsert(key, document, timeout=5)
            upsert_result.append(res['status'])
            self.log.info(
                f"Bucket: {bucket.name}, Key: {key}, Result of upsert = {res}"
            )
            time.sleep(1)

        upsert_sdk_client.close()
        self.upsert_bucket_dict[bucket.name] = upsert_result

    def perform_cruds(self):

        insert_threads = list()
        read_threads = list()
        upsert_threads = list()

        # Wait until ep_warmup_thread is True on a bucket with background warmup
        for bucket in self.cluster.buckets:
            if bucket.warmupBehavior == Bucket.WarmupBehavior.BACKGROUND:
                self.bucket_util._wait_warmup_completed(bucket, self.cluster.kv_nodes,
                                                        check_for_persistence=False)

        for bucket in self.cluster.buckets:
            random_key_prefix = ''.join(random.choices(
                                        string.ascii_lowercase, k=6))
            insert_th = threading.Thread(target=self.insert_docs, args=[
                                         random_key_prefix, bucket, 20])

            read_th = threading.Thread(target=self.read_docs, args=[
                                       "test_docs", bucket, 20])

            upsert_th = threading.Thread(target=self.upsert_docs, args=[
                                         "test_docs", bucket, 20])

            insert_threads.append(insert_th)
            read_threads.append(read_th)
            upsert_threads.append(upsert_th)

            insert_th.start()
            read_th.start()
            upsert_th.start()

        for th in insert_threads:
            th.join()

        for th in read_threads:
            th.join()

        for th in upsert_threads:
            th.join()

        self.log.info(f"Insert results: {self.insert_bucket_dict}")
        self.log.info(f"Read results: {self.read_bucket_dict}")
        self.log.info(f"Upsert results: {self.upsert_bucket_dict}")

    def trigger_warmup_perform_cruds(self, warmup_trigger):

        monitor_threads = list()

        if warmup_trigger == "kill_memcached":

            self.kill_memcached_on_nodes(self.cluster.kv_nodes)

            for bucket in self.cluster.buckets:
                if bucket.warmupBehavior == Bucket.WarmupBehavior.BACKGROUND:
                    for server in self.cluster.kv_nodes:
                        monitor_th = threading.Thread(
                            target=self.monitor_secondary_warmup_stats,
                            args=[server, bucket])
                        monitor_th.start()
                        monitor_threads.append(monitor_th)

            time.sleep(2.5)
            self.perform_cruds()

        elif warmup_trigger == "change_eviction_policy":

            for bucket in self.cluster.buckets:
                self.log.info(
                    f"Updating evictionPolicy -> {self.new_eviction_policy} "
                    f"for bucket: {bucket.name}"
                )
                self.bucket_util.update_bucket_property(
                    self.cluster.master, bucket,
                    eviction_policy=self.new_eviction_policy)

            time.sleep(15)

            for bucket in self.cluster.buckets:
                if bucket.warmupBehavior == Bucket.WarmupBehavior.BACKGROUND:
                    for server in self.cluster.kv_nodes:
                        monitor_th = threading.Thread(
                            target=self.monitor_secondary_warmup_stats,
                            args=[server, bucket])
                        monitor_th.start()
                        monitor_threads.append(monitor_th)

            self.perform_cruds()

        for th in monitor_threads:
            th.join()

    def monitor_secondary_warmup_stats(self, server, bucket):

        cb_stat = Cbstats(server)
        secondary_warmup_running = True
        while secondary_warmup_running:
            try:
                warmup_stats = cb_stat.warmup_stats(bucket.name)
                self.log.info(
                    f"Secondary warmup stats, "
                    f"Server: {server.ip}, "
                    f"Bucket: {bucket.name}, "
                    f"State: {warmup_stats['ep_secondary_warmup_state']}, "
                    f"Status: {warmup_stats['ep_secondary_warmup_status']}"
                )
                if warmup_stats["ep_secondary_warmup_status"] == "complete":
                    secondary_warmup_running = False
            except Exception:
                pass
            finally:
                self.sleep(1)

    def validate_warmup_behavior(self):

        for bucket in self.cluster.buckets:
            self.log.info(f"Validating warmupBehavior for bucket:{bucket.name}")
            bucket_read_list = self.read_bucket_dict[bucket.name]
            read_res = True if sum(bucket_read_list) == len(bucket_read_list) \
                            else False

            bucket_insert_list = self.insert_bucket_dict[bucket.name]
            if bucket.warmupBehavior == Bucket.WarmupBehavior.BACKGROUND:
                insert_res = True \
                    if sum(bucket_insert_list) == len(bucket_insert_list) \
                                else False
            else:
                insert_res = True if False in bucket_insert_list else False

            bucket_upsert_list = self.upsert_bucket_dict[bucket.name]
            if bucket.warmupBehavior == Bucket.WarmupBehavior.BACKGROUND:
                upsert_res = True \
                    if sum(bucket_upsert_list) == len(bucket_upsert_list) \
                                else False
            else:
                upsert_res = True if False in bucket_upsert_list else False

            if not read_res:
                self.log.error("Some reads failed during warmup")

            if insert_res and upsert_res:
                self.log.info(
                    f"Warmup behavior:{bucket.warmupBehavior} for bucket:{bucket.name} "
                    f"is working as expected"
                )
            else:
                if not insert_res:
                    self.log.error("Writes are not working as expected")
                elif not upsert_res:
                    self.log.error("Upserts are not working as expected")
                self.fail(
                    f"Warmup behavior:{bucket.warmupBehavior} for bucket:{bucket.name} "
                    f"is not working as expected"
                )


    def test_secondary_warmup(self):

        self.insert_bucket_dict = dict()
        self.read_bucket_dict = dict()
        self.upsert_bucket_dict = dict()

        if self.durability_min_level not in [None, "none", "None"]:
            for bucket in self.cluster.buckets:
                self.bucket_util.update_bucket_property(
                    self.cluster.master, bucket,
                    bucket_durability=self.durability_min_level)
            self.sleep(5, "Wait after setting durability")

        # Inserting a few documents with bucket_durability set
        for bucket in self.cluster.buckets:
            self.insert_docs("test_docs", bucket, 20)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        for bucket in self.cluster.buckets:
            self.read_docs("test_docs", bucket, 20)

        # Trigger a bucket warmup and perform cruds during the process
        self.trigger_warmup_perform_cruds(warmup_trigger=self.warmup_trigger)

        # Validate warmup behavior
        if self.validate_warmup:
            self.validate_warmup_behavior()

        # Update warmup_behavior and validate that the new behavior is taking effect
        if self.update_warmup_behavior:
            self.sleep(60, "Wait before updating warmupBehavior")
            for bucket in self.cluster.buckets:
                if bucket.warmupBehavior == Bucket.WarmupBehavior.BLOCKING:
                    new_warmup_behavior = Bucket.WarmupBehavior.BACKGROUND
                else:
                    new_warmup_behavior = Bucket.WarmupBehavior.BLOCKING
                self.log.info(
                    f"Bucket:{bucket.name}, New warmupBehavior:{new_warmup_behavior}"
                )
                self.bucket_util.update_bucket_property(
                        self.cluster.master,
                        bucket,
                        warmup_behavior=new_warmup_behavior)

            self.trigger_warmup_perform_cruds(warmup_trigger="kill_memcached")

            # Validate new warmup behavior
            if self.validate_warmup:
                self.validate_warmup_behavior()