from BucketLib.bucket import Bucket
import math, time
from Cb_constants import CbServer, DocLoading
from cb_tools.cbstats import Cbstats
from membase.api.rest_client import RestConnection
from sdk_client3 import SDKClient
from couchbase_helper.documentgenerator import doc_generator
from BucketLib.BucketOperations import BucketHelper
from storage.guardrails.guardrails_base import GuardrailsBase

class RRGuardrails(GuardrailsBase):
    def setUp(self):
        super(RRGuardrails, self).setUp()
        self.sdk_timeout = self.input.param("sdk_timeout", 60)
        self.deep_copy = self.input.param("deep_copy", False)
        self.bucket_ram_quota = self.input.param("bucket_ram_quota", None)
        self.track_failures = self.input.param("track_failures", True)
        self.monitor_stats = ["doc_ops", "ep_queue_size"]
        self.couch_min_rr = self.input.param("couch_min_rr", 10)
        self.magma_min_rr = self.input.param("magma_min_rr", 1)
        self.retry_get_process_num = self.input.param("retry_get_process_num", 200)
        self.init_items_load = self.input.param("init_items_load", 2500000)
        self.timeout = self.input.param("guardrail_timeout", 300)

        status, content = \
            BucketHelper(self.cluster.master).set_bucket_rr_guardrails(self.couch_min_rr,
                                                                       self.magma_min_rr)
        if status:
            self.log.info("Bucket RR Guardrails set {}".format(content))

        if self.bucket_storage == Bucket.StorageBackend.couchstore:
            self.rr_guardrail_threshold = self.couch_min_rr
        else:
            self.rr_guardrail_threshold = self.magma_min_rr

        self.bucket = self.cluster.buckets[0]
        self.kv_nodes = self.cluster_util.get_kv_nodes(self.cluster,
                                                       self.cluster.nodes_in_cluster)

        self.log.info("Creating SDK clients for the buckets")
        self.create_sdk_clients_for_buckets()


    def create_sdk_clients_for_buckets(self):
        self.init_sdk_pool_object()
        max_clients = min(self.task_manager.number_of_threads, 20)
        if self.standard_buckets > 20:
            max_clients = self.standard_buckets
        clients_per_bucket = int(math.ceil(max_clients / self.standard_buckets))
        for bucket in self.cluster.buckets:
            self.sdk_client_pool.create_clients(
                bucket, [self.cluster.master], clients_per_bucket,
                compression_settings=self.sdk_compression)


    def test_rr_guardrail_with_data_growth(self):

        self.log.info("Starting initial data load...")
        self.initial_data_load_until_guardrail_limit(self.rr_guardrail_threshold)

        self.sleep(30, "Wait for 30 seconds after the RR guardrail is hit")
        self.bucket_util.print_bucket_stats(self.cluster)

        self.log.info("Current node resident ratios = {}".format(
                                    self.check_resident_ratio(self.cluster)))

        bucket_item_count = self.bucket_util.get_bucket_current_item_count(self.cluster,
                                                                           self.bucket)
        self.log.info("Current bucket item count {}".format(bucket_item_count))
        self.bucket_util.print_bucket_stats(self.cluster)

        number_of_docs = self.input.param("number_of_docs", 10)
        result = self.insert_new_docs_sdk(num_docs=number_of_docs,
                                          bucket=self.bucket,
                                          doc_key="new_docs")

        self.log.info("Validating SDK error messages")
        error_code = 'BUCKET_RESIDENT_RATIO_TOO_LOW'
        for res in result:
            exp = res["status"] == False and error_code in res["error"]
            self.assertTrue(exp, "Mutations were not blocked")
        self.log.info("Expected error code {} was seen on all inserts".format(error_code))

        bucket_item_count2 = self.bucket_util.get_bucket_current_item_count(self.cluster,
                                                                            self.bucket)
        self.log.info("Bucket item count after inserts {}".format(bucket_item_count2))
        exp2 = bucket_item_count == bucket_item_count2
        self.assertTrue(exp2, "Bucket item count does not match")


    def test_failover_with_rr_guardrail(self):

        self.log.info("Starting initial data load...")
        self.initial_data_load_until_guardrail_limit(self.rr_guardrail_threshold)

        self.sleep(30, "Wait for 30 seconds after the RR guardrail is hit")
        self.bucket_util.print_bucket_stats(self.cluster)

        rr_current = self.check_resident_ratio(self.cluster)
        self.log.info("Current resident ratios = {}".format(rr_current))

        rest = RestConnection(self.cluster.master)
        rest_nodes = rest.node_statuses()
        node_to_failover = None
        for node in rest_nodes:
            if node.ip != self.cluster.master.ip:
                node_to_failover = node
                break
        failover_res = rest.fail_over(node_to_failover.id, graceful=True)
        self.assertTrue(failover_res, "Failover of node failed")

        rebalance_passed = rest.monitorRebalance()
        self.assertTrue(rebalance_passed, "Failover rebalance failed")

        rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()],
                       ejectedNodes=[])
        rebalance_passed = rest.monitorRebalance()
        self.assertTrue(rebalance_passed, "Rebalance after failover of node failed")

        self.cluster_util.print_cluster_stats(self.cluster)
        self.cluster.nodes_in_cluster = self.cluster_util.get_nodes(self.cluster.master)

        rr_val = self.check_resident_ratio(self.cluster)
        self.log.info("Resident ratio after failover = {}".format(rr_val))

        if self.check_if_rr_guardrail_breached(self.bucket, rr_val, self.rr_guardrail_threshold):
            self.log.info("Resident ratio guardrail threshold is breached")

        number_of_docs = self.input.param("number_of_docs", 20)
        result = self.insert_new_docs_sdk(number_of_docs,
                                          self.bucket,
                                          doc_key="temp_docs")
        self.log.info("Validating SDK error messages")
        error_code = 'BUCKET_RESIDENT_RATIO_TOO_LOW'
        for res in result:
            exp = res["status"] == False and error_code in res["error"]
            self.assertTrue(exp, "Mutations were not blocked")
        self.log.info("Expected error code {} was seen on all inserts".format(error_code))


    def test_rebalance_out_with_rr_guardrail(self):

        self.log.info("Starting initial data load...")
        self.initial_data_load_until_guardrail_limit(self.rr_guardrail_threshold)

        self.sleep(30, "Wait for 30 seconds after the guardrail is hit")
        self.bucket_util.print_bucket_stats(self.cluster)

        number_of_docs = self.input.param("number_of_docs", 10)
        result = self.insert_new_docs_sdk(num_docs=number_of_docs,
                                            bucket=self.bucket,
                                            doc_key="temp_docs")
        self.log.info("Validating SDK error messages")
        error_code = 'BUCKET_RESIDENT_RATIO_TOO_LOW'
        for res in result:
            exp = res["status"] == False and error_code in res["error"]
            self.assertTrue(exp, "Mutations were not blocked")
        self.log.info("Expected error code {} was seen on all inserts".format(error_code))

        node_to_rebalance_out = None
        for node in self.kv_nodes:
            if node.ip != self.cluster.master.ip:
                node_to_rebalance_out = node
                break

        self.log.info("Node to rebalance out = {}".format(node_to_rebalance_out.ip))
        self.log.info("Rebalance-out starting")
        rebalance_task = self.task.async_rebalance(
                            self.cluster,
                            [],
                            to_remove=[node_to_rebalance_out],
                            retry_get_process_num=self.retry_get_process_num)

        self.task_manager.get_task_result(rebalance_task)

        self.assertFalse(rebalance_task.result, "Rebalance-out succeeded which was not expected")
        self.log.info("Rebalance-out was blocked since RR would go below the threshold limit")
        self.cluster_util.print_cluster_stats(self.cluster)


    def test_guardrail_mutations_after_collection_drop(self):

        self.log.info("Starting initial data load...")
        self.initial_data_load_until_guardrail_limit(self.rr_guardrail_threshold)

        self.sleep(30, "Wait for 30 seconds after the guardrail is hit")
        self.bucket_util.print_bucket_stats(self.cluster)

        number_of_docs = self.input.param("number_of_docs", 10)

        # Trying inserts after hitting guardrail
        result = self.insert_new_docs_sdk(num_docs=number_of_docs,
                                          bucket=self.bucket,
                                          doc_key="temp_docs")

        self.log.info("Validating SDK error messages")
        error_code = 'BUCKET_RESIDENT_RATIO_TOO_LOW'
        for res in result:
            exp = res["status"] == False and error_code in res["error"]
            self.assertTrue(exp, "Mutations were not blocked")
        self.log.info("Expected error code {} was seen on all inserts".format(error_code))

        for scope in self.bucket.scopes:
            if scope == "_default" or scope == "_system":
                continue
            for coll in self.bucket.scopes[scope].collections:
                self.log.info("Dropping collection {0} in scope {1}".format(coll, scope))
                self.bucket_util.drop_collection(self.cluster.master,
                                                 self.bucket,
                                                 scope,
                                                 coll)
                break

        self.sleep(60, "Wait for a few seconds after dropping collections")
        bucket_item_count3 = self.bucket_util.get_bucket_current_item_count(self.cluster,
                                                                            self.bucket)
        self.log.info("Bucket item count after dropping collections = {}".format(bucket_item_count3))

        new_rr = self.check_resident_ratio(self.cluster)
        self.log.info("Resident ratio after dropping collection = {}".format(new_rr))

        # Trying inserts after dropping a collection
        result = self.insert_new_docs_sdk(num_docs=number_of_docs,
                                          bucket=self.bucket,
                                          doc_key="new_docs")
        for res in result:
            exp = res["status"] == True
            self.assertTrue(exp, "Mutations are still blocked after deletes")

        self.sleep(20, "Wait for items to get reflected")
        new_bucket_item_count = self.bucket_util.get_bucket_current_item_count(self.cluster,
                                                                               self.bucket)
        self.log.info("Bucket item count = {}".format(new_bucket_item_count))

        exp2 = new_bucket_item_count == bucket_item_count3 + number_of_docs
        self.assertTrue(exp2, "Bucket item count does not match")


    def test_mutations_after_deletes_rr_guardrail(self):

        self.log.info("Starting initial data load...")
        self.initial_data_load_until_guardrail_limit(self.rr_guardrail_threshold)

        self.sleep(30, "Wait for 30 seconds after the guardrail is hit")
        self.bucket_util.print_bucket_stats(self.cluster)

        bucket_item_count = self.bucket_util.get_bucket_current_item_count(self.cluster,
                                                                           self.bucket)

        number_of_docs = self.input.param("number_of_docs", 10)

        # Trying inserts after hitting guardrail
        result = self.insert_new_docs_sdk(num_docs=number_of_docs,
                                          bucket=self.bucket,
                                          doc_key="temp_docs")

        self.log.info("Validating SDK error messages")
        error_code = 'BUCKET_RESIDENT_RATIO_TOO_LOW'
        for res in result:
            exp = res["status"] == False and error_code in res["error"]
            self.assertTrue(exp, "Mutations were not blocked")
        self.log.info("Expected error code {} was seen on all inserts".format(error_code))

        items_to_delete = bucket_item_count // ((self.num_scopes - 1) * self.num_collections * 2)

        self.doc_ops = "delete"
        self.delete_start = 0
        self.delete_end = items_to_delete
        self.create_perc = 0
        self.delete_perc = 100
        self.log.info("Deleting {} items in each collection".format(items_to_delete))
        self.new_loader()
        self.doc_loading_tm.getAllTaskResult()
        self.printOps.end_task()

        bucket_item_count = self.bucket_util.get_bucket_current_item_count(self.cluster,
                                                                           self.bucket)
        self.log.info("Resident ratio after deletes = {}".format(
                                self.check_resident_ratio(self.cluster)))

        result = self.insert_new_docs_sdk(num_docs=100,
                                          bucket=self.bucket,
                                          doc_key="new_docs")
        for res in result:
            exp = res["status"] == True
            self.assertTrue(exp, "Mutations are still blocked after deleting docs")
        self.log.info("Inserts were successful after deletion of a few items")


    def test_delete_recreate_bucket_rr_guardrail(self):

        self.log.info("Starting initial data load...")
        self.initial_data_load_until_guardrail_limit(self.rr_guardrail_threshold)

        self.sleep(30, "Wait for 30 seconds after the guardrail is hit")
        self.bucket_util.print_bucket_stats(self.cluster)

        # Trying inserts after hitting guardrail
        number_of_docs = self.input.param("number_of_docs", 10)
        result = self.insert_new_docs_sdk(num_docs=number_of_docs,
                                          bucket=self.bucket,
                                          doc_key="temp_docs")

        self.log.info("Validating SDK error messages")
        error_code = 'BUCKET_RESIDENT_RATIO_TOO_LOW'
        for res in result:
            exp = res["status"] == False and error_code in res["error"]
            self.assertTrue(exp, "Mutations were not blocked")
        self.log.info("Expected error code {} was seen on all inserts".format(error_code))

        self.log.info("Deleting existing bucket : {}".format(self.bucket.name))
        self.bucket_util.delete_bucket(self.cluster, self.bucket)
        self.sdk_client_pool.shutdown()

        self.log.info("Creating a bucket...")
        self.bucket_util.create_default_bucket(self.cluster, bucket_type=self.bucket_type,
            ram_quota=self.bucket_ram_quota, replica=self.num_replicas,
            storage=self.bucket_storage, eviction_policy=self.bucket_eviction_policy,
            autoCompactionDefined=self.autoCompactionDefined,
            fragmentation_percentage=self.fragmentation,
            flush_enabled=self.flush_enabled,
            magma_key_tree_data_block_size=self.magma_key_tree_data_block_size,
            magma_seq_tree_data_block_size=self.magma_seq_tree_data_block_size,
            history_retention_collection_default=self.bucket_collection_history_retention_default,
            history_retention_seconds=self.bucket_dedup_retention_seconds,
            history_retention_bytes=self.bucket_dedup_retention_bytes,
            weight=self.bucket_weight, width=self.bucket_width)
        self.sleep(10)
        self.cluster.buckets = self.bucket_util.get_all_buckets(self.cluster)
        self.bucket_util.print_bucket_stats(self.cluster)
        new_bucket = self.cluster.buckets[0]

        self.log.info("Creating scopes and collections")
        self.bucket_util.create_scope(self.cluster.master, new_bucket,
                                              {"name": "scope-1"})
        self.bucket_util.create_collection(self.cluster.master, new_bucket,
                                    "scope-1", {"name": "collection-1"})

        self.log.info("Creating SDK clients for the new bucket")
        self.create_sdk_clients_for_buckets()

        self.create_start = 0
        self.create_end = 100000
        self.log.info("Loading data into the newly created bucket")
        self.new_loader()
        self.doc_loading_tm.getAllTaskResult()
        self.printOps.end_task()

        self.sleep(15, "Wait for item count to get reflected")
        self.bucket_util.print_bucket_stats(self.cluster)
        bucket_item_count = self.bucket_util.get_bucket_current_item_count(self.cluster,
                                                                           new_bucket)
        self.assertTrue(bucket_item_count == self.create_end, "Item count does not match")


    def test_rr_guardrail_with_expiry_workload(self):

        self.bucket_ttl = self.input.param("bucket_ttl", 300)
        status = self.bucket_util.update_bucket_property(self.cluster.master,
                                                        self.bucket,
                                                        max_ttl=self.bucket_ttl)
        if status:
            self.log.info("Bucket ttl set to {}".format(self.bucket_ttl))

        self.log.info("Starting initial data load...")
        self.initial_data_load_until_guardrail_limit(self.rr_guardrail_threshold)

        self.sleep(30, "Wait for 30 seconds after the guardrail is hit")
        self.bucket_util.print_bucket_stats(self.cluster)

        bucket_item_count1 = self.bucket_util.get_bucket_current_item_count(self.cluster,
                                                                            self.bucket)
        self.log.info("Bucket item count = {}".format(bucket_item_count1))

        number_of_docs = self.input.param("number_of_docs", 10)
        result = self.insert_new_docs_sdk(num_docs=number_of_docs,
                                          bucket=self.bucket,
                                          doc_key="temp_docs")
        self.log.info("Validating SDK error messages")
        error_code = 'BUCKET_RESIDENT_RATIO_TOO_LOW'
        for res in result:
            exp = res["status"] == False and error_code in res["error"]
            self.assertTrue(exp, "Mutations were not blocked")
        self.log.info("Expected error code {} was seen on all inserts".format(error_code))

        self.bucket_util._expiry_pager(self.cluster, val=1)
        self.sleep(300, "Wait for docs to expire")

        bucket_item_count2 = self.bucket_util.get_bucket_current_item_count(self.cluster,
                                                                            self.bucket)
        self.log.info("Bucket item count after expiry of docs = {}".format(bucket_item_count2))

        result = self.insert_new_docs_sdk(num_docs=number_of_docs,
                                          bucket=self.bucket,
                                          doc_key="new_docs")
        for res in result:
            exp = res["status"] == True
            self.assertTrue(exp, "Mutations are still blocked after expiry of docs")
        self.log.info("Inserts were successful after expiry of docs")


    def test_rr_guardrail_limit_dynamic(self):

        self.new_guardrail_limit = self.input.param("new_guardrail_limit", 60)

        self.log.info("Starting initial data load...")
        self.initial_data_load_until_guardrail_limit(self.new_guardrail_limit)

        # Changing RR guardrail limits
        status, content = \
            BucketHelper(self.cluster.master).set_bucket_rr_guardrails(
                                            couch_min_rr=self.new_guardrail_limit,
                                            magma_min_rr=self.new_guardrail_limit)
        if status:
            self.log.info("Resident ratio guardrail changed to {}".format(content))

        self.sleep(30, "Wait for 30 seconds after the guardrail limit is changed")
        bucket_item_count = self.bucket_util.get_bucket_current_item_count(self.cluster,
                                                                           self.bucket)
        self.log.info("Bucket item count = {}".format(bucket_item_count))

        number_of_docs = self.input.param("number_of_docs", 10)
        result = self.insert_new_docs_sdk(num_docs=number_of_docs,
                                          bucket=self.bucket,
                                          doc_key="temp_docs")
        self.log.info("Validating SDK error messages")
        error_code = 'BUCKET_RESIDENT_RATIO_TOO_LOW'
        for res in result:
            exp = res["status"] == False and error_code in res["error"]
            self.assertTrue(exp, "Mutations were not blocked")
        self.log.info("Expected error code {} was seen on all inserts".format(error_code))

        # Changing the guardrail limit to the original value again
        status, content = \
            BucketHelper(self.cluster.master).set_bucket_rr_guardrails(
                                            couch_min_rr=self.couch_min_rr,
                                            magma_min_rr=self.magma_min_rr)
        if status:
            self.log.info("Guardrail values have been changed to {}".format(content))

        self.sleep(30, "Wait for a few seconds after guardrail limit is changed")
        result = self.insert_new_docs_sdk(num_docs=number_of_docs,
                                          bucket=self.bucket,
                                          doc_key="new_docs")
        for res in result:
            exp = res["status"] == True
            self.assertTrue(exp, "Mutations were not blocked")

        self.sleep(20, "Wait for a few seconds after inserting docs")
        bucket_item_count2 = self.bucket_util.get_bucket_current_item_count(self.cluster,
                                                                           self.bucket)
        self.log.info("Bucket item count = {}".format(bucket_item_count2))

        exp = bucket_item_count2 == bucket_item_count2 + number_of_docs
        self.assertTrue(exp, "Item count does not match")


    def test_rr_guardrail_with_flush_bucket(self):

        self.log.info("Starting initial data load...")
        self.initial_data_load_until_guardrail_limit(self.rr_guardrail_threshold)

        self.sleep(30, "Wait for 30 seconds after the guardrail is hit")
        self.bucket_util.print_bucket_stats(self.cluster)

        number_of_docs = self.input.param("number_of_docs", 10)
        result = self.insert_new_docs_sdk(num_docs=number_of_docs,
                                          bucket=self.bucket,
                                          doc_key="temp_docs")
        self.log.info("Validating SDK error messages")
        error_code = 'BUCKET_RESIDENT_RATIO_TOO_LOW'
        for res in result:
            exp = res["status"] == False and error_code in res["error"]
            self.assertTrue(exp, "Mutations were not blocked")
        self.log.info("Expected error code {} was seen on all inserts".format(error_code))

        self.log.info("Flushing the bucket")
        self.bucket_util.flush_bucket(self.cluster, self.bucket)
        self.sleep(60, "Wait for a few seconds after flushing the bucket")
        bucket_item_count = self.bucket_util.get_bucket_current_item_count(self.cluster,
                                                                           self.bucket)
        exp = bucket_item_count == 0
        self.assertTrue(exp, "All items were not deleted")

        self.create_start = 0
        self.create_end = 20000
        self.log.info("Loading data into the bucket after flushing")
        self.new_loader(wait=True)

        bucket_item_count2 = self.bucket_util.get_bucket_current_item_count(self.cluster,
                                                                           self.bucket)
        final_item_count = self.create_end * (self.num_scopes - 1) * self.num_collections
        exp = bucket_item_count2 == final_item_count
        self.assertTrue(exp, "Item count does not match")


    def test_rr_guardrail_target_vb_on_node(self):

        vbucket_list = {}
        # Getting vbucket list for the nodes in the cluster
        for server in self.cluster.kv_nodes:
            cbstats = Cbstats(server)
            target_vbucket = cbstats.vbucket_list(self.bucket.name)
            vbucket_list[server.ip] = target_vbucket

        self.log.info("Creating docs for inserting into vbuckets of server {}".format(
                                                                self.cluster.master.ip))
        doc_gen = doc_generator(key="new_docs", start=0, end=self.init_items_load,
                                doc_size=self.doc_size,
                                target_vbucket=vbucket_list[self.cluster.master.ip],
                                randomize_value=True)
        self.log.info("Starting data load...")
        task = self.task.async_load_gen_docs(
            self.cluster, self.bucket, doc_gen,
            DocLoading.Bucket.DocOps.CREATE, 0,
            batch_size=self.batch_size,
            process_concurrency=self.process_concurrency,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            sdk_client_pool=self.sdk_client_pool)

        current_rr = self.check_resident_ratio(self.cluster)
        end_time = time.time() + self.timeout

        while (not self.check_if_rr_guardrail_breached(self.bucket, current_rr, self.rr_guardrail_threshold)) and \
                                            time.time() < end_time:
            self.sleep(2, "Wait for a few seconds before next RR check")
            current_rr = self.check_resident_ratio(self.cluster)
            self.log.info("Current resident ratio = {}".format(current_rr))

        self.log.info("Stopping all doc loading tasks")
        task.end_task()
        self.printOps.end_task()
        self.sleep(30, "Wait for 30 seconds after hitting guardrail")

        for server in self.cluster.kv_nodes:
            if server.ip != self.cluster.master.ip:
                non_master_node = server
                break

        number_of_docs = self.input.param("number_of_docs", 20)
        self.log.info("Creating SDK client for inserting new docs")
        self.sdk_client = SDKClient([non_master_node], self.bucket)
        new_docs = doc_generator(key="temp_docs", start=0,
                                end=number_of_docs,
                                target_vbucket=vbucket_list[non_master_node.ip],
                                randomize_value=True)
        result = []
        self.log.info("Inserting {0} docs into vbuckets on the node {1}".format(
                                                number_of_docs, non_master_node.ip))
        for i in range(number_of_docs):
            key_obj, val_obj = new_docs.next()
            res = self.sdk_client.insert(key_obj, val_obj)
            result.append(res)

        self.log.info("Validating SDK error messages")
        error_code = 'BUCKET_RESIDENT_RATIO_TOO_LOW'
        for res in result:
            exp = res["status"] == False and error_code in res["error"]
            self.assertTrue(exp, "Mutations were not blocked")
        self.log.info("Expected error code {} was seen on all inserts".format(error_code))


    def insert_new_docs_sdk(self, num_docs, bucket, doc_key="new_docs"):
        result = []
        self.log.info("Creating SDK client for inserting new docs")
        self.sdk_client = SDKClient([self.cluster.master],
                                    bucket,
                                    scope=CbServer.default_scope,
                                    collection=CbServer.default_collection)
        new_docs = doc_generator(key=doc_key, start=0,
                                end=num_docs,
                                doc_size=1024,
                                doc_type=self.doc_type,
                                vbuckets=self.cluster.vbuckets,
                                key_size=self.key_size,
                                randomize_value=True)
        self.log.info("Inserting {} documents".format(num_docs))
        for i in range(num_docs):
            key_obj, val_obj = new_docs.next()
            res = self.sdk_client.insert(key_obj, val_obj)
            result.append(res)

        self.sdk_client.close()
        return result

    def initial_data_load_until_guardrail_limit(self, guardrail_limit):
        self.create_start = 0
        self.create_end = self.init_items_load
        end_time = time.time() + self.timeout

        doc_loading_tasks = self.new_loader()

        current_rr = self.check_resident_ratio(self.cluster)

        while (not self.check_if_rr_guardrail_breached(self.bucket, current_rr, guardrail_limit)) and \
                                            time.time() < end_time:
            self.sleep(2, "Wait for a few seconds before next RR check")
            current_rr = self.check_resident_ratio(self.cluster)
            self.log.info("Current resident ratio = {}".format(current_rr))

        self.sleep(2)
        self.log.info("Stopping all doc loading tasks after hitting RR guardrail")
        for task in doc_loading_tasks:
            task.stop_work_load()
        self.printOps.end_task()
