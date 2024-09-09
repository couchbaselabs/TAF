import time

from BucketLib.bucket import Bucket
from BucketLib.BucketOperations import BucketHelper
from membase.api.rest_client import RestConnection
from storage.guardrails.guardrails_base import GuardrailsBase


class MaxDataGuardrails(GuardrailsBase):
    def setUp(self):
        super(MaxDataGuardrails, self).setUp()
        self.sdk_timeout = self.input.param("sdk_timeout", 60)
        self.deep_copy = self.input.param("deep_copy", False)
        self.track_failures = self.input.param("track_failures", True)
        self.monitor_stats = ["doc_ops", "ep_queue_size"]
        self.couch_max_data = self.input.param("couch_max_data", 1.6)
        self.magma_max_data = self.input.param("magma_max_data", 16)
        self.retry_get_process_num = self.input.param("retry_get_process_num", 200)
        self.init_items_load = self.input.param("init_items_load", 5000000)
        self.timeout = self.input.param("guardrail_timeout", 3600)

        status, content = \
            BucketHelper(self.cluster.master).set_max_data_per_bucket_guardrails(self.couch_max_data,
                                                                                 self.magma_max_data)
        if status:
            self.log.info("Max data per bucket Guardrails set {}".format(content))

        if self.bucket_storage == Bucket.StorageBackend.couchstore:
            self.data_guardrail_threshold = self.couch_max_data
        else:
            self.data_guardrail_threshold = self.magma_max_data

        self.bucket = self.cluster.buckets[0]
        self.kv_nodes = self.cluster_util.get_kv_nodes(self.cluster,
                                                       self.cluster.nodes_in_cluster)

        self.log.info("Creating SDK clients for the buckets")
        self.create_sdk_clients_for_buckets()


    def test_max_data_per_bucket_with_data_growth(self):

        self.load_till_threshold = self.input.param("load_till_threshold",
                                                    self.data_guardrail_threshold)
        number_of_docs = self.input.param("number_of_docs", 10)
        error_code = 'BUCKET_DATA_SIZE_TOO_BIG'

        if self.test_read_traffic:
            document_key = "initial_docs"
            self.insert_new_docs_sdk(number_of_docs, self.bucket, document_key)

        self.log.info("Starting initial data load...")
        self.initial_data_load_until_guardrail_limit(self.load_till_threshold)

        self.log.info("Current bucket data sizes = {}".format(
                                        self.check_bucket_data_size_per_node(self.cluster)))
        self.bucket_util.print_bucket_stats(self.cluster)

        if self.load_till_threshold == self.data_guardrail_threshold:
            self.sleep(30, "Wait for 30 seconds after hitting the max bucket data size guardrail")

            if self.test_read_traffic:
                result = self.read_docs_sdk(self.bucket)
                for res in result:
                    exp = res["status"] == True
                    self.assertTrue(exp, "Read traffic was blocked")

            result = self.insert_new_docs_sdk(num_docs=number_of_docs,
                                              bucket=self.bucket,
                                              doc_key="new_docs")
            self.log.info("Validating SDK error messages")
            for res in result:
                exp = res["status"] == False and error_code in res["error"]
                self.assertTrue(exp, "Mutations were not blocked")
            self.log.info("Expected error code {} was seen on all inserts".format(error_code))

            if self.test_query_mutations:
                result = self.insert_new_docs_query(self.bucket)
                self.assertFalse(result,
                            "Mutations were allowed through query after hitting guardrail")

        else:
            result = self.check_cm_resource_limit_reached(self.cluster, "data_size")
            for bucket in self.cluster.buckets:
                result_bucket = result[bucket.name]
                self.assertTrue(1 not in result_bucket,
                                "CM resource limit for bucket data size is set to 1")

            if self.test_read_traffic:
                result = self.read_docs_sdk(self.bucket)
                for res in result:
                    exp = res["status"] == True
                    self.assertTrue(exp, "Read traffic was blocked")

            for bucket in self.cluster.buckets:
                result = self.insert_new_docs_sdk(num_docs=number_of_docs,
                                                  bucket=bucket,
                                                  doc_key="new_docs")
                self.log.info("Validating SDK insert results")
                for res in result:
                    exp = res["status"] == True
                    self.assertTrue(exp, "Mutations were blocked")
                self.log.info("Inserts were allowed on bucket {}".format(bucket.name))

                if self.test_query_mutations:
                    result = self.insert_new_docs_query(bucket)
                    self.assertTrue(result,
                        "Mutations are blocked through query on bucket: {}".format(bucket.name))


    def test_mutations_after_deletes_max_data_guardrail(self):

        number_of_docs = self.input.param("number_of_docs", 10)
        error_code = 'BUCKET_DATA_SIZE_TOO_BIG'

        self.log.info("Starting initial data load...")
        self.initial_data_load_until_guardrail_limit(self.data_guardrail_threshold)

        self.sleep(30, "Wait for 30 seconds after hitting the max bucket data size guardrail")
        self.bucket_util.print_bucket_stats(self.cluster)

        self.log.info("Current bucket data sizes = {}".format(
                                    self.check_bucket_data_size_per_node(self.cluster)))

        bucket_item_count = self.bucket_util.get_buckets_item_count(
            self.cluster, self.bucket.name)
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

        self.log.info("Bucket data size after deletes = {}".format(
                                self.check_bucket_data_size_per_node(self.cluster)))

        result = self.insert_new_docs_sdk(num_docs=100,
                                          bucket=self.bucket,
                                          doc_key="new_docs")
        for res in result:
            exp = res["status"] == True
            self.assertTrue(exp, "Mutations are still blocked after deleting docs")
        self.log.info("Inserts were successful after deletion of a few items")


    def test_mutations_after_collection_drop_max_data_guardrail(self):

        number_of_docs = self.input.param("number_of_docs", 10)
        error_code = 'BUCKET_DATA_SIZE_TOO_BIG'

        self.log.info("Starting initial data load...")
        self.initial_data_load_until_guardrail_limit(self.data_guardrail_threshold)

        self.sleep(30, "Wait for 30 seconds after hitting the max bucket data size guardrail")
        self.bucket_util.print_bucket_stats(self.cluster)

        result = self.insert_new_docs_sdk(num_docs=number_of_docs,
                                          bucket=self.bucket,
                                          doc_key="new_docs")
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

        self.sleep(30, "Wait for a few seconds after dropping collections")

        new_data_sizes = self.check_bucket_data_size_per_node(self.cluster)
        self.log.info("Bucket data size after dropping collections = {}".format(
                                                new_data_sizes[self.bucket.name]))

        # Trying inserts after dropping a collection
        result = self.insert_new_docs_sdk(num_docs=number_of_docs,
                                          bucket=self.bucket,
                                          doc_key="new_docs")
        for res in result:
            exp = res["status"] == True
            self.assertTrue(exp, "Mutations are still blocked after deletes")
        self.log.info("Inserts were allowed after dropping collections")


    def test_max_data_guardrail_with_bucket_compaction(self):

        number_of_docs = self.input.param("number_of_docs", 10)
        error_code = 'BUCKET_DATA_SIZE_TOO_BIG'

        self.log.info("Starting initial data load...")
        self.initial_data_load_until_guardrail_limit(self.data_guardrail_threshold)

        self.sleep(30, "Wait for 30 seconds after hitting the max bucket data size guardrail")
        self.bucket_util.print_bucket_stats(self.cluster)

        result = self.insert_new_docs_sdk(num_docs=number_of_docs,
                                          bucket=self.bucket,
                                          doc_key="new_docs")
        self.log.info("Validating SDK error messages")
        for res in result:
            exp = res["status"] == False and error_code in res["error"]
            self.assertTrue(exp, "Mutations were not blocked")
        self.log.info("Expected error code {} was seen on all inserts".format(error_code))

        collection_items = self.bucket_util.get_doc_count_per_collection(self.cluster, self.bucket)
        self.log.info("Collection item count = {}".format(collection_items))

        self.log.info("Compacting bucket {}".format(self.bucket.name))
        compaction_task = self.task.async_compact_bucket(
                self.cluster.master, self.bucket)
        self.task_manager.get_task_result(compaction_task)
        self.assertTrue(compaction_task.result, "Bucket compaction failed")
        self.bucket_util.print_bucket_stats(self.cluster)

        result = self.insert_new_docs_sdk(num_docs=100,
                                          bucket=self.bucket,
                                          doc_key="temp_docs")
        self.log.info("Validating SDK error messages")
        for res in result:
            exp = res["status"] == False and error_code in res["error"]
            self.assertTrue(exp, "Mutations were not blocked")
        self.log.info("Expected error code {} was seen on all inserts".format(error_code))


    def test_rebalance_out_max_data_guardrail(self):

        self.load_till_threshold = \
            self.input.param("load_till_threshold", self.data_guardrail_threshold)
        number_of_docs = self.input.param("number_of_docs", 10)
        error_code = 'BUCKET_DATA_SIZE_TOO_BIG'

        self.log.info("Starting initial data load...")
        self.initial_data_load_until_guardrail_limit(self.load_till_threshold)

        if self.load_till_threshold == self.data_guardrail_threshold:
            self.sleep(30, "Wait for 30 seconds after the max data per bucket guardrail is hit")
            self.bucket_util.print_bucket_stats(self.cluster)

            result = self.insert_new_docs_sdk(num_docs=number_of_docs,
                                            bucket=self.bucket,
                                            doc_key="new_docs")
            self.log.info("Validating SDK error messages")
            for res in result:
                exp = res["status"] == False and error_code in res["error"]
                self.assertTrue(exp, "Mutations were not blocked")
            self.log.info("Expected error code {} was seen on all inserts".format(error_code))

        data_sizes = self.check_bucket_data_size_per_node(self.cluster)
        bucket_data_size = data_sizes[self.bucket.name]
        total_bucket_size = sum(bucket_data_size)
        num_nodes = len(self.kv_nodes)

        new_per_node_bucket_size = total_bucket_size / float(num_nodes-1)
        if new_per_node_bucket_size >= self.data_guardrail_threshold:
            rebalance_not_allowed = True
        else:
            rebalance_not_allowed = False

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

        if rebalance_not_allowed:
            self.assertFalse(rebalance_task.result, "Rebalance-out was allowed")
            self.log.info("Rebalance-out was not allowed since data per node would below the threshold")
        else:
            self.assertTrue(rebalance_task.result, "Rebalance-out was not allowed")
            self.log.info("Rebalance-out was allowed since data per node would not go below the threshold")


    def test_mutations_after_add_nodes_max_data_guardrail(self):

        number_of_docs = self.input.param("number_of_docs", 10)
        error_code = 'BUCKET_DATA_SIZE_TOO_BIG'

        self.log.info("Starting initial data load...")
        self.initial_data_load_until_guardrail_limit(self.data_guardrail_threshold)

        self.sleep(30, "Wait for 30 seconds after hitting max data per bucket guardrail")
        self.bucket_util.print_bucket_stats(self.cluster)

        result = self.insert_new_docs_sdk(num_docs=number_of_docs,
                                          bucket=self.bucket,
                                          doc_key="new_docs")
        self.log.info("Validating SDK error messages")
        for res in result:
            exp = res["status"] == False and error_code in res["error"]
            self.assertTrue(exp, "Mutations were not blocked")
        self.log.info("Expected error code {} was seen on all inserts".format(error_code))

        node_to_add = self.cluster.servers[self.nodes_init]
        rest = RestConnection(self.cluster.master)
        services = rest.get_nodes_services()
        services_on_target_node = services[(self.cluster.master.ip + ":"
                                            + str(self.cluster.master.port))]
        self.log.info("Adding node {}".format(node_to_add.ip))
        rebalance_in_task = self.task.async_rebalance(self.cluster,
                                    to_add=[node_to_add],
                                    to_remove=[],
                                    check_vbucket_shuffling=False,
                                    services=[",".join(services_on_target_node)])
        self.task_manager.get_task_result(rebalance_in_task)
        self.assertTrue(rebalance_in_task.result, "Rebalance-in of a node failed")
        self.sleep(10, "Wait for a few seconds after rebalance-in")

        data_sizes = self.check_bucket_data_size_per_node(self.cluster)
        bucket_data_size = data_sizes[self.bucket.name]
        self.log.info("New bucket data size per node after adding a node = {}".format(bucket_data_size))

        self.log.info("Inserting docs after adding a node")
        result = self.insert_new_docs_sdk(num_docs=100,
                                          bucket=self.bucket,
                                          doc_key="new_docs")
        for res in result:
            exp = res["status"] == True
            self.assertTrue(exp, "Mutations are blocked even after adding a node")
        self.log.info("Inserts were allowed after rebalance-in of a node")


    def test_failover_max_data_guardrail(self):

        self.load_till_threshold = self.input.param("load_till_threshold",
                                                    self.data_guardrail_threshold)
        number_of_docs = self.input.param("number_of_docs", 10)
        error_code = 'BUCKET_DATA_SIZE_TOO_BIG'

        self.log.info("Starting initial data load...")
        self.initial_data_load_until_guardrail_limit(self.load_till_threshold)

        if self.load_till_threshold == self.data_guardrail_threshold:
            self.sleep(30, "Wait for a few seconds after hitting max data per bucket guardrail")
            self.bucket_util.print_bucket_stats(self.cluster)

            result = self.insert_new_docs_sdk(num_docs=number_of_docs,
                                            bucket=self.bucket,
                                            doc_key="new_docs")
            self.log.info("Validating SDK error messages")
            for res in result:
                exp = res["status"] == False and error_code in res["error"]
                self.assertTrue(exp, "Mutations were not blocked")
            self.log.info("Expected error code {} was seen on all inserts".format(error_code))

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
        self.cluster.kv_nodes = self.cluster_util.get_kv_nodes(self.cluster,
                                                               self.cluster.nodes_in_cluster)

        data_sizes = self.check_bucket_data_size_per_node(self.cluster)
        mutations_allowed = not self.check_if_bucket_data_size_guardrail_breached(self.bucket,
                                                                        data_sizes,
                                                                        self.data_guardrail_threshold)
        self.log.info("Inserting new docs")
        result = self.insert_new_docs_sdk(num_docs=100,
                                          bucket=self.bucket,
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


    def test_swap_rebalance_max_data_guardrail(self):

        self.load_till_threshold = self.input.param("load_till_threshold",
                                                    self.data_guardrail_threshold)
        number_of_docs = self.input.param("number_of_docs", 10)
        error_code = 'BUCKET_DATA_SIZE_TOO_BIG'

        self.log.info("Starting initial data load...")
        self.initial_data_load_until_guardrail_limit(self.load_till_threshold)
        self.sleep(30, "Wait for 30 seconds after hitting max data per bucket guardrail")

        self.bucket_util.print_bucket_stats(self.cluster)

        self.log.info("Buckets data sizes after initial loading = {} ".format(
                                self.check_bucket_data_size_per_node(self.cluster)))

        if self.data_guardrail_threshold == self.load_till_threshold:
            result = self.insert_new_docs_sdk(num_docs=number_of_docs,
                                            bucket=self.bucket,
                                            doc_key="new_docs")
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
        rest = RestConnection(self.cluster.master)
        services = rest.get_nodes_services()
        services_on_target_node = services[(self.cluster.master.ip + ":"
                                            + str(self.cluster.master.port))]

        self.log.info("Swap Rebalance starting")
        swap_reb_task = self.task.async_rebalance(self.cluster,
                                                to_add=[self.spare_node],
                                                to_remove=[node_to_swap],
                                                check_vbucket_shuffling=False,
                                                services=[",".join(services_on_target_node)])
        self.task_manager.get_task_result(swap_reb_task)
        self.assertTrue(swap_reb_task.result, "Swap Rebalance failed")

        data_sizes = self.check_bucket_data_size_per_node(self.cluster)
        mutations_allowed = not self.check_if_bucket_data_size_guardrail_breached(self.bucket,
                                                                data_sizes,
                                                                self.data_guardrail_threshold)
        self.log.info("Inserting docs after swap rebalance")
        result = self.insert_new_docs_sdk(num_docs=number_of_docs,
                                          bucket=self.bucket,
                                          doc_key="new_docs")
        for res in result:
            if not mutations_allowed:
                exp = res["status"] == False and error_code in res["error"]
                self.assertTrue(exp, "Mutations were not blocked")
            else:
                exp = res["status"] == True
                self.assertTrue(exp, "Mutations were blocked")


    def check_bucket_data_size_per_node(self, cluster):

        bucket_data_size = dict()

        for server in cluster.kv_nodes:
            _, res = RestConnection(server).query_prometheus("kv_logical_data_size_bytes")

            for item in res["data"]["result"]:
                if item["metric"]["state"] == "active":
                    bucket_name = item["metric"]["bucket"]
                    logical_data_bytes = float(item["value"][1])
                    logical_data_bytes_in_tb = logical_data_bytes / float(1000000000000)

                    if bucket_name not in bucket_data_size:
                        bucket_data_size[bucket_name] = [logical_data_bytes_in_tb]
                    else:
                        bucket_data_size[bucket_name].append(logical_data_bytes_in_tb)

        return bucket_data_size


    def check_if_bucket_data_size_guardrail_breached(self, bucket, buckets_data_size, threshold):

        bucket_data = buckets_data_size[bucket.name]
        for size in bucket_data:
            if size > threshold:
                return True

        return False

    def initial_data_load_until_guardrail_limit(self, guardrail_limit, create_start=0, create_end=5000000):
        self.create_start = create_start
        self.create_end = create_end
        end_time = time.time() + self.timeout

        doc_loading_tasks = self.new_loader()

        current_data_size = self.check_bucket_data_size_per_node(self.cluster)

        while not self.check_if_bucket_data_size_guardrail_breached(self.bucket, current_data_size, guardrail_limit) and \
                                            time.time() < end_time:
            self.sleep(2, "Wait for a few seconds before next bucket data size check")
            current_data_size = self.check_bucket_data_size_per_node(self.cluster)
            self.log.info("Current bucket data sizes = {}".format(current_data_size))

        self.sleep(1)
        self.log.info("Stopping all doc loading tasks after hitting max data guardrail")
        for task in doc_loading_tasks:
            task.stop_work_load()
        self.printOps.end_task()
