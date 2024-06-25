import time

from BucketLib.bucket import Bucket
import time
from cb_constants import CbServer, DocLoading
from cb_tools.cbstats import Cbstats
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
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
        self.timeout = self.input.param("guardrail_timeout", 3600)

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

    def test_rr_guardrail_with_data_growth(self):

        number_of_docs = self.input.param("number_of_docs", 10)
        error_code = 'BUCKET_RESIDENT_RATIO_TOO_LOW'

        self.log.info("Starting initial data load...")
        self.initial_data_load_until_guardrail_limit(self.rr_guardrail_threshold)

        self.sleep(30, "Wait for 30 seconds after the RR guardrail is hit")
        result = self.check_cm_resource_limit_reached(self.cluster, "resident_ratio")
        self.log.info("CM result = {}".format(result))
        result_bucket = result[self.bucket.name]
        self.assertTrue(1 in result_bucket, "CM resource limit for resident ratio is set to 0")
        self.bucket_util.print_bucket_stats(self.cluster)

        self.log.info("Current node resident ratios = {}".format(
                                    self.check_resident_ratio(self.cluster)))

        bucket_item_count = self.bucket_util.get_bucket_current_item_count(self.cluster,
                                                                           self.bucket)
        self.log.info("Current bucket item count {}".format(bucket_item_count))
        self.bucket_util.print_bucket_stats(self.cluster)

        result = self.insert_new_docs_sdk(num_docs=number_of_docs,
                                          bucket=self.bucket,
                                          doc_key="new_docs")

        self.log.info("Validating SDK error messages")
        for res in result:
            exp = res["status"] == False and error_code in res["error"]
            self.assertTrue(exp, "Mutations were not blocked")
        self.log.info("Expected error code {} was seen on all inserts".format(error_code))

        bucket_item_count2 = self.bucket_util.get_bucket_current_item_count(self.cluster,
                                                                            self.bucket)
        self.log.info("Bucket item count after inserts {}".format(bucket_item_count2))
        exp2 = bucket_item_count == bucket_item_count2
        self.assertTrue(exp2, "Bucket item count does not match")

        if self.test_query_mutations:
            result = self.insert_new_docs_query(self.bucket)
            self.assertFalse(result, "Mutations were allowed through query")


    def test_failover_with_rr_guardrail(self):

        self.load_till_rr = self.input.param("load_till_rr", self.rr_guardrail_threshold)
        number_of_docs = self.input.param("number_of_docs", 10)
        error_code = 'BUCKET_RESIDENT_RATIO_TOO_LOW'

        self.log.info("Starting initial data load...")
        self.initial_data_load_until_guardrail_limit(self.load_till_rr)

        if self.load_till_rr == self.rr_guardrail_threshold:
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

        mutations_allowed = not self.check_if_rr_guardrail_breached(self.bucket,
                                                                rr_val,
                                                                self.rr_guardrail_threshold)
        result = self.insert_new_docs_sdk(number_of_docs,
                                          self.bucket,
                                          doc_key="temp_docs")

        self.log.info("Validating SDK insert results")
        if not mutations_allowed:
            for res in result:
                exp = res["status"] == False and error_code in res["error"]
                self.assertTrue(exp, "Mutations were not blocked")
            self.log.info("Expected error code {} was seen on all inserts".format(error_code))
        else:
            for res in result:
                exp = res["status"] == True
                self.assertTrue(exp, "Mutations are not allowed even when RR > threshold")


    def test_rebalance_out_with_rr_guardrail(self):

        self.load_till_rr = self.input.param("load_till_rr", self.rr_guardrail_threshold)
        number_of_docs = self.input.param("number_of_docs", 10)
        error_code = 'BUCKET_RESIDENT_RATIO_TOO_LOW'
        self.include_bucket_priority = self.input.param("include_bucket_priority", False)
        self.lowest_bucket_priority = self.input.param("lowest_bucket_priority", 100)
        self.buckets_to_create = self.input.param("buckets_to_create", 2)

        if self.include_bucket_priority:
            self.bucket_util.update_bucket_property(self.cluster.master, self.bucket,
                                            bucket_rank=self.lowest_bucket_priority)

        self.log.info("Starting initial data load...")
        self.initial_data_load_until_guardrail_limit(self.load_till_rr)

        if self.load_till_rr == self.rr_guardrail_threshold:
            self.sleep(30, "Wait for 30 seconds after the guardrail is hit")
            self.bucket_util.print_bucket_stats(self.cluster)

            result = self.insert_new_docs_sdk(num_docs=number_of_docs,
                                                bucket=self.bucket,
                                                doc_key="temp_docs")
            self.log.info("Validating SDK error messages")
            for res in result:
                exp = res["status"] == False and error_code in res["error"]
                self.assertTrue(exp, "Mutations were not blocked")
            self.log.info("Expected error code {} was seen on all inserts".format(error_code))

        current_rr = self.check_resident_ratio(self.cluster)
        self.log.info("Current RR after loading = {}".format(current_rr))

        if self.include_bucket_priority:
            bucket_name = "new_bucket"
            delta = 100
            self.log.info("Creating {} empty buckets".format(self.buckets_to_create))
            for i in range(self.buckets_to_create):
                self.bucket_util.create_default_bucket(
                    self.cluster, bucket_type=self.bucket_type,
                    ram_quota=self.bucket_ram_quota, replica=self.num_replicas,
                    storage=self.bucket_storage, eviction_policy=self.bucket_eviction_policy,
                    bucket_rank=self.lowest_bucket_priority+(delta*(i+1)),
                    bucket_name=bucket_name+str(i))

        # Calculate new RR after rebalance-out of a node
        new_rr = dict()
        num_nodes = len(self.cluster.kv_nodes)
        for bucket in current_rr:
            new_rr[bucket] = []
            for node_rr in current_rr[bucket]:
                rr = node_rr * (float(num_nodes - 1) / float(num_nodes))
                new_rr[bucket].append(rr)
        self.log.info("Expected Resident ratios after rebalance-out = {}".format(new_rr))

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

        rebalance_allowed = not self.check_if_rr_guardrail_breached(self.bucket,
                                                                new_rr,
                                                                self.rr_guardrail_threshold)
        if rebalance_allowed:
            actual_rr_after_rebalance_out = self.check_resident_ratio(self.cluster)
            self.log.info("Resident ratios after rebalance-out = {}".format(
                                                        actual_rr_after_rebalance_out))

        if rebalance_allowed:
            self.assertTrue(rebalance_task.result, "Rebalance-out failed even when RR > threshold")
            self.log.info("Rebalance-out succeeded since RR would not go below the threshold limit")
        else:
            self.assertFalse(rebalance_task.result, "Rebalance-out succeeded even when RR < threshold")
            self.log.info("Rebalance-out was blocked since RR would go below the threshold limit")
        self.cluster_util.print_cluster_stats(self.cluster)


    def test_guardrail_mutations_after_collection_drop(self):

        number_of_docs = self.input.param("number_of_docs", 10)
        error_code = 'BUCKET_RESIDENT_RATIO_TOO_LOW'

        self.log.info("Starting initial data load...")
        self.initial_data_load_until_guardrail_limit(self.rr_guardrail_threshold)

        self.sleep(30, "Wait for 30 seconds after the guardrail is hit")
        self.bucket_util.print_bucket_stats(self.cluster)

        # Trying inserts after hitting guardrail
        result = self.insert_new_docs_sdk(num_docs=number_of_docs,
                                          bucket=self.bucket,
                                          doc_key="temp_docs")
        self.log.info("Validating SDK error messages")
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

        number_of_docs = self.input.param("number_of_docs", 10)
        error_code = 'BUCKET_RESIDENT_RATIO_TOO_LOW'

        self.log.info("Starting initial data load...")
        self.initial_data_load_until_guardrail_limit(self.rr_guardrail_threshold)

        self.sleep(30, "Wait for 30 seconds after the guardrail is hit")
        self.bucket_util.print_bucket_stats(self.cluster)

        bucket_item_count = self.bucket_util.get_bucket_current_item_count(self.cluster,
                                                                           self.bucket)

        # Trying inserts after hitting guardrail
        result = self.insert_new_docs_sdk(num_docs=number_of_docs,
                                          bucket=self.bucket,
                                          doc_key="temp_docs")
        self.log.info("Validating SDK error messages")
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
        number_of_docs = self.input.param("number_of_docs", 10)
        error_code = 'BUCKET_RESIDENT_RATIO_TOO_LOW'

        self.log.info("Starting initial data load...")
        self.initial_data_load_until_guardrail_limit(self.rr_guardrail_threshold)

        self.sleep(30, "Wait for 30 seconds after the guardrail is hit")
        self.bucket_util.print_bucket_stats(self.cluster)

        # Trying inserts after hitting guardrail
        result = self.insert_new_docs_sdk(num_docs=number_of_docs,
                                          bucket=self.bucket,
                                          doc_key="temp_docs")

        self.log.info("Validating SDK error messages")
        for res in result:
            exp = res["status"] == False and error_code in res["error"]
            self.assertTrue(exp, "Mutations were not blocked")
        self.log.info("Expected error code {} was seen on all inserts".format(error_code))

        self.log.info("Deleting existing bucket : {}".format(self.bucket.name))
        self.bucket_util.delete_bucket(self.cluster, self.bucket)
        self.cluster.sdk_client_pool.shutdown()

        self.log.info("Creating a bucket...")
        self.bucket_util.create_default_bucket(self.cluster, bucket_type=self.bucket_type,
            ram_quota=self.bucket_ram_quota, replica=self.num_replicas,
            storage=self.bucket_storage, eviction_policy=self.bucket_eviction_policy)
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

        self.sleep(30, "Wait for item count to get reflected")
        self.bucket_util.print_bucket_stats(self.cluster)
        bucket_item_count = self.bucket_util.get_bucket_current_item_count(self.cluster,
                                                                           new_bucket)
        self.assertTrue(bucket_item_count == self.create_end, "Item count does not match")


    def test_rr_guardrail_with_expiry_workload(self):

        self.bucket_ttl = self.input.param("bucket_ttl", 300)
        number_of_docs = self.input.param("number_of_docs", 10)
        error_code = 'BUCKET_RESIDENT_RATIO_TOO_LOW'
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

        result = self.insert_new_docs_sdk(num_docs=number_of_docs,
                                          bucket=self.bucket,
                                          doc_key="temp_docs")
        self.log.info("Validating SDK error messages")
        for res in result:
            exp = res["status"] == False and error_code in res["error"]
            self.assertTrue(exp, "Mutations were not blocked")
        self.log.info("Expected error code {} was seen on all inserts".format(error_code))

        self.bucket_util._expiry_pager(self.cluster, val=0.1)
        self.sleep(self.bucket_ttl*3, "Wait for docs to expire")

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
        number_of_docs = self.input.param("number_of_docs", 10)
        error_code = 'BUCKET_RESIDENT_RATIO_TOO_LOW'

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

        result = self.insert_new_docs_sdk(num_docs=number_of_docs,
                                          bucket=self.bucket,
                                          doc_key="temp_docs")
        self.log.info("Validating SDK error messages")
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
        exp = bucket_item_count2 == bucket_item_count + number_of_docs
        self.assertTrue(exp, "Item count does not match")


    def test_rr_guardrail_with_flush_bucket(self):

        number_of_docs = self.input.param("number_of_docs", 10)
        error_code = 'BUCKET_RESIDENT_RATIO_TOO_LOW'

        self.log.info("Starting initial data load...")
        self.initial_data_load_until_guardrail_limit(self.rr_guardrail_threshold)

        self.sleep(30, "Wait for 30 seconds after the guardrail is hit")
        self.bucket_util.print_bucket_stats(self.cluster)

        result = self.insert_new_docs_sdk(num_docs=number_of_docs,
                                          bucket=self.bucket,
                                          doc_key="temp_docs")
        self.log.info("Validating SDK error messages")
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
        self.new_loader()
        self.doc_loading_tm.getAllTaskResult()
        self.printOps.end_task()

        self.sleep(20, "Wait for num_items to get relfected")
        self.bucket_util.print_bucket_stats(self.cluster)
        bucket_item_count2 = self.bucket_util.get_bucket_current_item_count(self.cluster,
                                                                           self.bucket)
        final_item_count = self.create_end * ((self.num_scopes * self.num_collections) - 1)
        exp = bucket_item_count2 == final_item_count
        self.log.info("Expected = {0}, actual = {1}".format(final_item_count, bucket_item_count2))
        self.assertTrue(exp, "Item count does not match.")


    def test_rr_guardrail_target_vb_on_node(self):

        number_of_docs = self.input.param("number_of_docs", 10)
        error_code = 'BUCKET_RESIDENT_RATIO_TOO_LOW'

        vbucket_list = {}
        # Getting vbucket list for the nodes in the cluster
        for server in self.cluster.kv_nodes:
            cbstats = Cbstats(server)
            vbucket_list[server.ip] = cbstats.vbucket_list(self.bucket.name)
            cbstats.disconnect()

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
            durability=self.durability_level, timeout_secs=self.sdk_timeout,
            load_using=self.load_docs_using)

        current_rr = self.check_resident_ratio(self.cluster)
        end_time = time.time() + self.timeout

        while (not self.check_if_rr_guardrail_breached(self.bucket, current_rr, self.rr_guardrail_threshold)) and \
                                            time.time() < end_time:
            self.sleep(2, "Wait for a few seconds before next RR check")
            current_rr = self.check_resident_ratio(self.cluster)
            self.log.info("Current resident ratio = {}".format(current_rr))

        self.log.info("Stopping all doc loading tasks")
        task.end_task()
        self.sleep(30, "Wait for 30 seconds after hitting guardrail")

        for server in self.cluster.kv_nodes:
            if server.ip != self.cluster.master.ip:
                non_master_node = server
                break

        self.log.info("Creating SDK client for inserting new docs")
        self.sdk_client = SDKClient(self.cluster, self.bucket,
                                    servers=[non_master_node])
        new_docs = doc_generator(key="temp_docs", start=0, end=number_of_docs,
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
        for res in result:
            exp = res["status"] == False and error_code in res["error"]
            self.assertTrue(exp, "Mutations were not blocked")
        self.log.info("Expected error code {} was seen on all inserts".format(error_code))


    def test_rr_guardrail_restart_services(self):

        self.restart_service = self.input.param("restart_service", "prometheus")
        number_of_docs = self.input.param("number_of_docs", 10)
        error_code = 'BUCKET_RESIDENT_RATIO_TOO_LOW'

        self.log.info("Starting data load...")
        self.initial_data_load_until_guardrail_limit(self.rr_guardrail_threshold)

        self.sleep(30, "Wait for 30 seconds after the guardrail is hit")
        self.bucket_util.print_bucket_stats(self.cluster)
        bucket_item_count = self.bucket_util.get_bucket_current_item_count(self.cluster,
                                                                           self.bucket)

        result = self.insert_new_docs_sdk(num_docs=number_of_docs, bucket=self.bucket,
                                          doc_key="temp_docs")
        self.log.info("Validating SDK error messages")
        for res in result:
            exp = res["status"] == False and error_code in res["error"]
            self.assertTrue(exp, "Mutations were not blocked")
        self.log.info("Expected error code {} was seen on all inserts".format(error_code))

        self.log.info("Current resident ratio = {}".format(
                            self.check_resident_ratio(self.cluster)))

        count = 0
        iter = self.input.param("restart_iterations", 5)
        # Stopping and re-starting services in a loop
        while count < iter:
            self.log.info("Iteration {}".format(count+1))
            shell = RemoteMachineShellConnection(self.cluster.master)

            self.log.info("Stopping service {}".format(self.restart_service))
            if self.restart_service == "prometheus":
                shell.stop_prometheus()
            elif self.restart_service == "memcached":
                shell.stop_memcached()
            elif self.restart_service == "server":
                shell.stop_couchbase()

            self.sleep(5, "Wait before bringing the service back up")

            self.log.info("Re-starting service {}".format(self.restart_service))
            if self.restart_service == "prometheus":
                shell.start_prometheus()
            elif self.restart_service == "memcached":
                shell.start_memcached()
            elif self.restart_service == "server":
                shell.start_couchbase()

            self.sleep(30, "Wait after bringing the service back up")
            self.log.info("Current resident ratio = {}".format(
                                    self.check_resident_ratio(self.cluster)))
            document_key = "new_docs" + str(count)
            result = self.insert_new_docs_sdk(num_docs=1, bucket=self.bucket,
                                              doc_key=document_key)
            print(result)
            exp = result[0]["status"] == False and error_code in result[0]["error"]
            self.assertTrue(exp, "Mutations were not blocked")
            count += 1

        final_bucket_item_count = self.bucket_util.get_bucket_current_item_count(self.cluster,
                                                                                 self.bucket)
        exp = bucket_item_count == final_bucket_item_count
        self.assertTrue(exp, "Bucket item count does not match")


    def test_backup_restore_with_rr_guardrail(self):

        number_of_docs = self.input.param("number_of_docs", 10)
        self.load_till_rr = self.input.param("load_till_rr", self.rr_guardrail_threshold)
        error_code = 'BUCKET_RESIDENT_RATIO_TOO_LOW'
        archive = self.input.param("archive", "/couchbase_data/backups")
        repo = self.input.param("repo", "example")
        username = self.input.param("username", "Administrator")
        password = self.input.param("password", "password")

        self.log.info("Starting initial data load...")
        self.initial_data_load_until_guardrail_limit(self.load_till_rr)

        self.sleep(30, "Wait for 30 seconds after the RR guardrail is hit")
        self.bucket_util.print_bucket_stats(self.cluster)

        self.log.info("Current node resident ratios = {}".format(
                                    self.check_resident_ratio(self.cluster)))

        shell = RemoteMachineShellConnection(self.cluster.master)
        self.log.info('Delete previous backups')
        command = 'rm -rf %s' % archive
        o, r = shell.execute_command(command)
        shell.log_command_output(o, r)

        self.log.info('Configure backup')
        configure_bkup_cmd = '{0}cbbackupmgr config -a {1} -r {2}'.format(
            shell.return_bin_path_based_on_os(shell.return_os_type()),
            archive, repo)
        o, r = shell.execute_command(configure_bkup_cmd)
        shell.log_command_output(o, r)

        self.log.info("Backing up data")
        if CbServer.use_https:
            bkup_cmd = '{0}cbbackupmgr backup -a {1} -r {2} --cluster couchbases://{3} --username {4} --password {5} --no-ssl-verify'.format(
                shell.return_bin_path_based_on_os(shell.return_os_type()),
                archive, repo, self.cluster.master.ip, username, password)
        else:
            bkup_cmd = '{0}cbbackupmgr backup -a {1} -r {2} --cluster couchbase://{3} --username {4} --password {5}'.format(
                shell.return_bin_path_based_on_os(shell.return_os_type()),
                archive, repo, self.cluster.master.ip, username, password)

        o, r = shell.execute_command(bkup_cmd)
        shell.log_command_output(o, r)
        self.assertTrue('Backup completed successfully' in ''.join(o),
                            msg='Backup was unsuccessful')
        shell.disconnect()
        self.log.info("Backup of the cluster completed successfully")

        self.log.info("Deleting bucket {}".format(self.bucket.name))
        self.bucket_util.delete_bucket(self.cluster, self.bucket)

        self.log.info("Creating a bucket with the same name: {}".format(self.bucket.name))
        self.bucket_util.create_default_bucket(self.cluster, bucket_type=self.bucket_type,
            ram_quota=self.bucket_ram_quota * 2, replica=self.num_replicas,
            storage=self.bucket_storage, eviction_policy=self.bucket_eviction_policy)
        self.bucket = self.cluster.buckets[0]

        self.log.info("Restoring backup into bucket: {}".format(self.bucket.name))
        shell = RemoteMachineShellConnection(self.cluster.master)
        if CbServer.use_https:
            restore_cmd = '{0}cbbackupmgr restore -a {1} -r {2} --cluster couchbases://{3} --username {4} --password {5} --no-ssl-verify'.format(
                shell.return_bin_path_based_on_os(shell.return_os_type()),
                archive, repo, self.cluster.master.ip, username, password)
        else:
            restore_cmd = '{0}cbbackupmgr restore -a {1} -r {2} --cluster couchbase://{3} --username {4} --password {5}'.format(
                shell.return_bin_path_based_on_os(shell.return_os_type()),
                archive, repo, self.cluster.master.ip, username, password)

        o, r = shell.execute_command(restore_cmd)
        shell.log_command_output(o, r)
        shell.disconnect()
        self.assertTrue('Restore completed successfully' in ''.join(o),
                            msg='Restore was unsuccessful')
        self.log.info("Restore completed successfully")
        self.bucket_util.print_bucket_stats(self.cluster)


    def test_rr_guardrail_with_multi_buckets(self):

        number_of_docs = self.input.param("number_of_docs", 10)
        error_code = 'BUCKET_RESIDENT_RATIO_TOO_LOW'

        first_bucket = self.cluster.buckets[0]
        second_bucket = self.cluster.buckets[1]

        self.log.info("Creating docs for inserting into bucket {0}".format(first_bucket.name))
        doc_gen = doc_generator(key="new_docs", start=0, end=self.init_items_load,
                                doc_size=self.doc_size,
                                randomize_value=True)
        self.log.info("Starting initial data load...")
        task = self.task.async_load_gen_docs(
            self.cluster, first_bucket, doc_gen,
            DocLoading.Bucket.DocOps.CREATE, 0,
            batch_size=self.batch_size,
            process_concurrency=self.process_concurrency,
            durability=self.durability_level, timeout_secs=self.sdk_timeout,
            load_using=self.load_docs_using)

        current_rr = self.check_resident_ratio(self.cluster)
        end_time = time.time() + self.timeout

        while (not self.check_if_rr_guardrail_breached(first_bucket, current_rr, self.rr_guardrail_threshold)) and \
                                            time.time() < end_time:
            self.sleep(2, "Wait for a few seconds before next RR check")
            current_rr = self.check_resident_ratio(self.cluster)
            self.log.info("Current resident ratio = {}".format(current_rr))

        self.log.info("Stopping all doc loading tasks")
        task.end_task()
        self.sleep(30, "Wait for 30 seconds after hitting guardrail")
        self.bucket_util.print_bucket_stats(self.cluster)

        self.log.info("Trying inserts on bucket {}".format(first_bucket.name))
        result = self.insert_new_docs_sdk(num_docs=number_of_docs, bucket=first_bucket,
                                          doc_key="temp_docs")
        self.log.info("Validating SDK error messages")
        for res in result:
            exp = res["status"] == False and error_code in res["error"]
            self.assertTrue(exp, "Mutations were not blocked")

        self.log.info("Trying inserts on bucket {}".format(second_bucket.name))
        result = self.insert_new_docs_sdk(num_docs=number_of_docs, bucket=second_bucket,
                                          doc_key="temp_docs")
        self.log.info("Validating SDK insert results")
        for res in result:
            exp = res["status"] == True
            self.assertTrue(exp, "Mutations were blocked on the second bucket")

        self.log.info("Mutations were blocked on bucket {0} and were allowed on bucket {1}".format(
                                                first_bucket.name, second_bucket.name))


    def test_rr_guardrail_with_memory_tuning(self):

        self.new_bucket_quota = self.input.param("new_bucket_quota", 512)
        self.load_till_rr = self.input.param("load_till_rr", self.rr_guardrail_threshold)
        number_of_docs = self.input.param("number_of_docs", 10)
        error_code = 'BUCKET_RESIDENT_RATIO_TOO_LOW'

        self.log.info("Starting initial data load...")
        self.initial_data_load_until_guardrail_limit(self.load_till_rr)

        self.log.info("Current resident ratio = {}".format(
                                    self.check_resident_ratio(self.cluster)))

        self.log.info("Changing bucket memory quota")
        self.bucket_util.update_bucket_property(self.cluster.master,
                                                self.bucket,
                                                ram_quota_mb=self.new_bucket_quota)
        self.bucket_util.print_bucket_stats(self.cluster)

        current_rr = self.check_resident_ratio(self.cluster)
        self.log.info("Current RR after changing bucket RAM quota = {}".format(current_rr))

        threshold_res = self.check_if_rr_guardrail_breached(self.bucket,
                                                            current_rr,
                                                            self.rr_guardrail_threshold)
        result = self.insert_new_docs_sdk(num_docs=number_of_docs,
                                          bucket=self.bucket,
                                          doc_key="temp_docs")
        if threshold_res:
            error_msg = "Mutations were not blocked"
            for res in result:
                exp = res["status"] == False and error_code in res["error"]
                self.assertTrue(exp, error_msg)
        else:
            error_msg = "Mutations are not allowed"
            for res in result:
                exp = res["status"] == True
                self.assertTrue(exp, error_msg)


    def test_rr_guardrail_with_add_nodes(self):

        self.rebalance_pause = self.input.param("rebalance_pause", False)
        self.reb_pause_per = self.input.param("reb_pause_per", 30)
        number_of_docs = self.input.param("number_of_docs", 10)
        error_code = 'BUCKET_RESIDENT_RATIO_TOO_LOW'

        self.log.info("Starting initial data load...")
        self.initial_data_load_until_guardrail_limit(self.rr_guardrail_threshold)

        self.sleep(30, "Wait for 30 seconds after the guardrail is hit")
        self.bucket_util.print_bucket_stats(self.cluster)

        result = self.insert_new_docs_sdk(num_docs=number_of_docs,
                                          bucket=self.bucket,
                                          doc_key="temp_docs")
        self.log.info("Validating SDK error messages")
        for res in result:
            exp = res["status"] == False and error_code in res["error"]
            self.assertTrue(exp, "Mutations were not blocked")
        self.log.info("Expected error code {} was seen on all inserts".format(error_code))

        node_to_add = self.cluster.servers[self.nodes_init]
        rest = RestConnection(self.cluster.master)

        self.log.info("Rebalance-in of node {} starting".format(node_to_add.ip))
        rebalance_in_task = \
            self.task.async_rebalance(self.cluster,
                                    to_add=[node_to_add],
                                    to_remove=[],
                                    check_vbucket_shuffling=False,
                                    services=[CbServer.Services.KV])
        if self.rebalance_pause:
            self.sleep(20)
            reached = self.cluster_util.rebalance_reached(self.cluster.master, self.reb_pause_per)
            if reached:
                stopped = rest.stop_rebalance(wait_timeout=self.wait_timeout / 3)
                self.assertTrue(stopped, "Unable to stop rebalance")
                self.log.info("Rebalance paused at {} percent".format(self.reb_pause_per))

            self.sleep(60, "Wait for a few seconds after pausing rebalance")
            result = self.insert_new_docs_sdk(num_docs=100,
                                              bucket=self.bucket,
                                              doc_key="docs_new")
            cm_result = self.check_cm_resource_limit_reached(self.cluster, "resident_ratio")
            cm_result_bucket = cm_result[self.bucket.name]
            self.log.info("CM result = {}".format(cm_result))
            for res in result:
                if (1 in cm_result_bucket):
                    exp = res["status"] == False and error_code in res["error"]
                    self.assertTrue(exp, "Mutations were not blocked")
                else:
                    exp = res["status"] == True
                    self.assertTrue(exp, "Mutations are still blocked")

            self.log.info("Resuming rebalance-in task")
            rebalance_in_task = self.task.async_rebalance(self.cluster,
                                        to_add=[], to_remove=[],
                                        check_vbucket_shuffling=False)

        self.task_manager.get_task_result(rebalance_in_task)
        self.assertTrue(rebalance_in_task.result, "Rebalance-in of a node failed")

        current_rr = self.check_resident_ratio(self.cluster)
        self.log.info("Resident ratio after adding a node = {}".format(current_rr))

        result = self.insert_new_docs_sdk(num_docs=100,
                                          bucket=self.bucket,
                                          doc_key="random_docs")
        for res in result:
            exp = res["status"] == True
            self.assertTrue(exp, "Mutations are still blocked after adding a node")

        self.log.info("Inserts were allowed after rebalance-in")


    def test_swap_rebalance_rr_guardrail(self):

        number_of_docs = self.input.param("number_of_docs", 10)
        error_code = 'BUCKET_RESIDENT_RATIO_TOO_LOW'

        self.log.info("Starting initial data load...")
        self.initial_data_load_until_guardrail_limit(self.rr_guardrail_threshold)

        self.sleep(30, "Wait for 30 seconds after the guardrail is hit")
        self.bucket_util.print_bucket_stats(self.cluster)

        result = self.insert_new_docs_sdk(num_docs=number_of_docs,
                                          bucket=self.bucket,
                                          doc_key="temp_docs")
        self.log.info("Validating SDK error messages")
        for res in result:
            exp = res["status"] == False and error_code in res["error"]
            self.assertTrue(exp, "Mutations were not blocked")
        self.log.info("Expected error code {} was seen on all inserts".format(error_code))

        node_to_swap = None
        for server in self.cluster.kv_nodes:
            if server.ip != self.cluster.master.ip:
                node_to_swap = server
                break

        self.spare_node = self.cluster.servers[self.nodes_init]
        self.log.info("Swap Rebalance starting")
        swap_reb_task = self.task.async_rebalance(self.cluster,
                                                to_add=[self.spare_node],
                                                to_remove=[node_to_swap],
                                                check_vbucket_shuffling=False,
                                                services=[CbServer.Services.KV])
        self.task_manager.get_task_result(swap_reb_task)
        self.assertTrue(swap_reb_task.result, "Swap Rebalance failed")

        current_rr = self.check_resident_ratio(self.cluster)
        self.log.info("Resident ratio after swap rebalance = {}".format(current_rr))

        result = self.insert_new_docs_sdk(num_docs=100,
                                          bucket=self.bucket,
                                          doc_key="new_docs")
        for res in result:
            exp = res["status"] == False and error_code in res["error"]
            self.assertTrue(exp, "Mutations were not blocked")
        self.log.info("Mutations were blocked as expected even after swap rebalance")


    def test_horizontal_node_scaling_rr_guardrail(self):

        number_of_docs = self.input.param("number_of_docs", 10)
        self.loop_iter = self.input.param("loop_iter", 3)
        error_code = 'BUCKET_RESIDENT_RATIO_TOO_LOW'

        self.log.info("Starting initial data load...")
        self.initial_data_load_until_guardrail_limit(self.rr_guardrail_threshold)

        self.sleep(30, "Wait for 30 seconds after the guardrail is hit")
        self.bucket_util.print_bucket_stats(self.cluster)

        result = self.insert_new_docs_sdk(num_docs=number_of_docs,
                                          bucket=self.bucket,
                                          doc_key="temp_docs")
        self.log.info("Validating SDK error messages")
        for res in result:
            exp = res["status"] == False and error_code in res["error"]
            self.assertTrue(exp, "Mutations were not blocked")
        self.log.info("Expected error code {} was seen on all inserts".format(error_code))

        bucket_item_count = self.bucket_util.get_bucket_current_item_count(self.cluster,
                                                                           self.bucket)
        create_start = self.create_start
        create_end = self.create_end

        # Horizontal scaling (adding nodes) in a loop n times
        for iter in range(self.loop_iter):
            self.log.info("Iteration {} of horizontal scaling".format(iter+1))
            node_to_add = self.cluster.servers[self.nodes_init + iter]

            self.log.info("Rebalancing-in the node {}".format(node_to_add.ip))
            reb_task = self.task.async_rebalance(self.cluster,
                                    to_add=[node_to_add],
                                    to_remove=[],
                                    check_vbucket_shuffling=False,
                                    services=[CbServer.Services.KV])
            self.task_manager.get_task_result(reb_task)
            self.assertTrue(reb_task.result, "Rebalance-in failed")

            current_rr = self.check_resident_ratio(self.cluster)
            self.log.info("Resident ratio after adding a node = {}".format(current_rr))

            self.log.info("Doing data load after adding a node")
            create_start = create_end
            create_end = create_start + self.init_items_load
            self.create_perc = 100
            self.initial_data_load_until_guardrail_limit(self.rr_guardrail_threshold,
                                                         create_start=create_start,
                                                         create_end=create_end)

            self.sleep(30, "Wait for 30 seconds after the guardrail is hit")
            document_key = "new_docs" + str(iter)
            result = self.insert_new_docs_sdk(num_docs=100,
                                          bucket=self.bucket,
                                          doc_key=document_key)
            self.log.info("Validating SDK error messages")
            for res in result:
                exp = res["status"] == False and error_code in res["error"]
                self.assertTrue(exp, "Mutations were not blocked")
            self.log.info("Mutations were blocked after hitting RR guardrail")
            current_bucket_item_count = \
                            self.bucket_util.get_bucket_current_item_count(self.cluster,
                                                                           self.bucket)
            exp = current_bucket_item_count > bucket_item_count
            self.assertTrue(exp, "Bucket count is still the same after data load")
            bucket_item_count = current_bucket_item_count


    def test_breach_rr_guardrail_during_rebalance(self):

        self.load_till_rr = self.input.param("load_till_rr", self.rr_guardrail_threshold + 20)
        number_of_docs = self.input.param("number_of_docs", 10)
        error_code = 'BUCKET_RESIDENT_RATIO_TOO_LOW'

        self.log.info("Starting initial data load")
        self.initial_data_load_until_guardrail_limit(self.load_till_rr)

        node_to_swap = None
        for server in self.cluster.kv_nodes:
            if server.ip != self.cluster.master.ip:
                node_to_swap = server

        self.spare_node = self.cluster.servers[self.nodes_init]
        self.log.info("Rebalance starting")
        reb_task = self.task.async_rebalance(self.cluster,
                                            to_add=[self.spare_node],
                                            to_remove=[node_to_swap],
                                            check_vbucket_shuffling=False,
                                            services=[CbServer.Services.KV])
        self.sleep(25)
        create_start = self.init_items_load
        create_end = create_start + self.init_items_load
        self.initial_data_load_until_guardrail_limit(self.rr_guardrail_threshold,
                                                     create_start=create_start,
                                                     create_end=create_end)
        self.task_manager.get_task_result(reb_task)

        result = self.insert_new_docs_sdk(num_docs=number_of_docs,
                                          bucket=self.bucket,
                                          doc_key="temp_docs")
        self.log.info("Validating SDK error messages")
        for res in result:
            exp = res["status"] == False and error_code in res["error"]
            self.assertTrue(exp, "Mutations were not blocked")
        self.log.info("Mutations were blocked after hitting RR guardrail")


    def initial_data_load_until_guardrail_limit(self, guardrail_limit, create_start=0, create_end=2500000):
        self.create_start = create_start
        self.create_end = create_end
        end_time = time.time() + self.timeout

        doc_loading_tasks = self.new_loader()

        current_rr = self.check_resident_ratio(self.cluster)

        while (not self.check_if_rr_guardrail_breached(self.bucket, current_rr, guardrail_limit)) and \
                                            time.time() < end_time:
            self.sleep(2, "Wait for a few seconds before next RR check")
            current_rr = self.check_resident_ratio(self.cluster)
            self.log.info("Current resident ratio = {}".format(current_rr))

        self.sleep(1)
        self.log.info("Stopping all doc loading tasks after hitting RR guardrail")
        for task in doc_loading_tasks:
            task.stop_work_load()
        self.printOps.end_task()
