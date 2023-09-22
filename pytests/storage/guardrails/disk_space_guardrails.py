import time
from membase.api.rest_client import RestConnection
from BucketLib.BucketOperations import BucketHelper
from storage.guardrails.guardrails_base import GuardrailsBase

class DiskUsageGuardrails(GuardrailsBase):
    def setUp(self):
        super(DiskUsageGuardrails, self).setUp()
        self.sdk_timeout = self.input.param("sdk_timeout", 60)
        self.deep_copy = self.input.param("deep_copy", False)
        self.track_failures = self.input.param("track_failures", True)
        self.monitor_stats = ["doc_ops", "ep_queue_size"]
        self.disk_usage_delta = self.input.param("disk_usage_delta", 1)
        self.retry_get_process_num = self.input.param("retry_get_process_num", 200)
        self.init_items_load = self.input.param("init_items_load", 5000000)
        self.timeout = self.input.param("guardrail_timeout", 5000)

        self.bucket = self.cluster.buckets[0]
        self.kv_nodes = self.cluster_util.get_kv_nodes(self.cluster,
                                                       self.cluster.nodes_in_cluster)
        self.log.info("Creating SDK clients for the buckets")
        self.create_sdk_clients_for_buckets()


    def test_disk_usage_guardrail_with_data_growth(self):

        number_of_docs = self.input.param("number_of_docs", 10)
        self.test_replica_update = self.input.param("test_replica_update", False)
        self.new_replica_number = self.input.param("new_replica_number", 2)
        error_code = 'BUCKET_DISK_SPACE_TOO_LOW'

        if self.test_read_traffic:
            document_key = "initial_docs"
            self.insert_new_docs_sdk(number_of_docs, self.bucket, document_key)

        self.log.info("Starting initial data load...")
        self.create_start = 0
        self.create_end = self.init_items_load
        self.new_loader()
        self.doc_loading_tm.getAllTaskResult()
        self.printOps.end_task()

        current_disk_usage = self.check_disk_usage_per_node(self.cluster)
        self.log.info("Current disk usage of nodes = {}".format(current_disk_usage))
        self.max_disk_usage = max(current_disk_usage) - 0.5
        status, content = \
            BucketHelper(self.cluster.master).set_max_disk_usage_guardrails(self.max_disk_usage)
        if status:
            self.log.info("Max disk usage Guardrails set {}".format(content))

        self.sleep(30, "Wait for 30 seconds after hitting the disk usage guardrail")
        self.bucket_util.print_bucket_stats(self.cluster)

        if self.test_read_traffic:
            result = self.read_docs_sdk(self.bucket)
            for res in result:
                exp = res["status"] == True
                self.assertTrue(exp, "Read traffic was blocked")

        new_bucket_name = "new_bucket"
        self.log.info("Creating a new bucket: {}".format(new_bucket_name))
        self.bucket_util.create_default_bucket(
                self.cluster,
                ram_quota=self.bucket_ram_quota,
                replica=self.num_replicas,
                storage=self.bucket_storage,
                bucket_name=new_bucket_name)
        self.bucket_util.print_bucket_stats(self.cluster)

        for bucket in self.cluster.buckets:
            result = self.insert_new_docs_sdk(num_docs=number_of_docs,
                                              bucket=bucket,
                                              doc_key="new_docs")
            self.log.info("Validating SDK error messages")
            for res in result:
                exp = res["status"] == False and error_code in res["error"]
                self.assertTrue(exp, "Mutations were not blocked")
            self.log.info("Expected error code {0} was seen on bucket {1}".format(error_code,
                                                                                bucket.name))
            if self.test_query_mutations:
                result = self.insert_new_docs_query(bucket)
                self.assertFalse(result, "Mutations were allowed through query")

            if self.test_replica_update:
                self.log.info("Updating replicaNumber to {0} for bucket: {1}".format(
                                                self.new_replica_number, bucket.name))
                try:
                    self.bucket_util.update_bucket_property(self.cluster.master, bucket,
                                                replica_number=self.new_replica_number)
                except Exception as e:
                    self.log.info("Exception = {}. Replica update failed".format(e))
                else:
                    if self.new_replica_number > self.num_replicas:
                        self.fail("Disk space guardrail didn't restrict replica update")

                rebalance_task = self.task.async_rebalance(self.cluster, [], [])
                self.task_manager.get_task_result(rebalance_task)
                self.assertTrue(rebalance_task.result, "Rebalance after replica update failed")


    def test_disk_usage_guardrail_with_data_reduction(self):

        self.reduce_data_action = self.input.param("reduce_data_action", "delete_data")
        self.hit_guardrail_initial = self.input.param("hit_guardrail_initial", True)
        number_of_docs = self.input.param("number_of_docs", 10)
        error_code = 'BUCKET_DISK_SPACE_TOO_LOW'

        self.log.info("Starting initial data load...")
        self.create_start = 0
        self.create_end = self.init_items_load
        self.new_loader()
        self.doc_loading_tm.getAllTaskResult()
        self.printOps.end_task()

        current_disk_usage = self.check_disk_usage_per_node(self.cluster)
        self.log.info("Current disk usage of nodes = {}".format(current_disk_usage))
        if self.hit_guardrail_initial:
            self.max_disk_usage = max(current_disk_usage) - 0.5
            status, content = \
                BucketHelper(self.cluster.master).set_max_disk_usage_guardrails(self.max_disk_usage)
            if status:
                self.log.info("Max disk usage Guardrails set {}".format(content))

            self.sleep(30, "Wait for 30 seconds after hitting the disk usage guardrail")
            self.bucket_util.print_bucket_stats(self.cluster)

            self.log.info("Inserting docs after hitting guardrail")
            result = self.insert_new_docs_sdk(num_docs=number_of_docs,
                                            bucket=self.bucket,
                                            doc_key="new_docs")
            self.log.info("Validating SDK error messages")
            for res in result:
                exp = res["status"] == False and error_code in res["error"]
                self.assertTrue(exp, "Mutations were not blocked")
            self.log.info("Expected error code {} was seen on all inserts".format(error_code))

        bucket_item_count = self.bucket_util.get_bucket_current_item_count(self.cluster,
                                                                           self.bucket)
        self.log.info("Current bucket item count {}".format(bucket_item_count))
        self.bucket_util.print_bucket_stats(self.cluster)

        if self.reduce_data_action == "delete_data":
            items_to_delete = int(self.init_items_load * 0.75)
            self.doc_ops = "delete"
            self.delete_start = 0
            self.delete_end = items_to_delete
            self.create_perc = 0
            self.delete_perc = 100
            self.log.info("Deleting {} items in each collection".format(items_to_delete))
            self.new_loader()
            self.doc_loading_tm.getAllTaskResult()
            self.printOps.end_task()

        elif self.reduce_data_action == "delete_bucket":
            bucket_to_delete = None
            for bucket in self.cluster.buckets:
                if bucket.name != self.bucket.name:
                    bucket_to_delete = bucket
            self.log.info("Deleting existing bucket : {}".format(bucket_to_delete.name))
            self.bucket_util.delete_bucket(self.cluster, bucket_to_delete)

        elif self.reduce_data_action == "compact_bucket":
            items_to_mutate = int(self.init_items_load * 0.5)
            self.doc_ops = "update"
            self.update_start = 0
            self.update_end = items_to_mutate
            self.create_perc = 0
            self.delete_perc = 0
            self.update_perc = 100
            self.log.info("Updating {} items in each collection".format(items_to_mutate))
            self.new_loader()
            self.doc_loading_tm.getAllTaskResult()
            self.printOps.end_task()
            self.sleep(30, "Wait for a few seconds after updating items")

            disk_usage_current = self.check_disk_usage_per_node(self.cluster)
            self.log.info("Current disk usage of nodes = {}".format(disk_usage_current))

            self.max_disk_usage = max(disk_usage_current) - 0.5
            status, content = \
                BucketHelper(self.cluster.master).set_max_disk_usage_guardrails(self.max_disk_usage)
            if status:
                self.log.info("Max disk usage Guardrails set {}".format(content))
            self.sleep(30, "Wait for 30 seconds after hitting the disk usage guardrail")

            self.log.info("Compacting bucket {}".format(self.bucket.name))
            compaction_task = self.task.async_compact_bucket(
                    self.cluster.master, self.bucket)
            self.task_manager.get_task_result(compaction_task)
            self.assertTrue(compaction_task.result, "Bucket compaction failed")

        elif self.reduce_data_action == "drop_collection":
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

        self.sleep(100, "Wait for a few seconds after performing action: {}".format(
                                                                self.reduce_data_action))
        disk_usages = self.check_disk_usage_per_node(self.cluster)
        self.log.info("Disk usage of node after data reduction = {}".format(disk_usages))
        mutations_allowed = not self.check_if_disk_usage_guardrail_breached(disk_usages,
                                                                    self.max_disk_usage)

        # Trying inserts after reducing disk usage
        self.log.info("Inserting docs into bucket {}".format(self.bucket.name))
        result = self.insert_new_docs_sdk(num_docs=number_of_docs,
                                          bucket=self.bucket,
                                          doc_key="new_docs")
        for res in result:
            if mutations_allowed:
                exp = res["status"] == True
                self.assertTrue(exp, "Mutations are still blocked after deletes")
            else:
                exp = res["status"] == False and error_code in res["error"]
                self.assertTrue(exp, "Mutations were not blocked")


    def test_failover_with_disk_usage_guardail(self):

        number_of_docs = self.input.param("number_of_docs", 10)
        error_code = 'BUCKET_DISK_SPACE_TOO_LOW'

        self.log.info("Starting initial data load...")
        self.create_start = 0
        self.create_end = self.init_items_load
        self.new_loader()
        self.doc_loading_tm.getAllTaskResult()
        self.printOps.end_task()

        current_disk_usage = self.check_disk_usage_per_node(self.cluster)
        self.log.info("Current disk usage of nodes = {}".format(current_disk_usage))
        self.max_disk_usage = max(current_disk_usage) - 0.5
        status, content = \
            BucketHelper(self.cluster.master).set_max_disk_usage_guardrails(self.max_disk_usage)
        if status:
            self.log.info("Max disk usage Guardrails set {}".format(content))

        self.sleep(30, "Wait for 30 seconds after hitting the disk usage guardrail")
        self.bucket_util.print_bucket_stats(self.cluster)

        if self.load_till_threshold == self.max_disk_usage:
            self.sleep(30, "Wait for a few seconds after hitting disk usage guardrail")
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

        disk_usages = self.check_disk_usage_per_node(self.cluster)
        mutations_allowed = not self.check_if_disk_usage_guardrail_breached(disk_usages,
                                                                    self.max_disk_usage)
        self.log.info("Inserting docs after swap rebalance")
        result = self.insert_new_docs_sdk(num_docs=100,
                                          bucket=self.bucket,
                                          doc_key="new_docs")
        for res in result:
            if not mutations_allowed:
                exp = res["status"] == False and error_code in res["error"]
                self.assertTrue(exp, "Mutations were not blocked")
            else:
                exp = res["status"] == True
                self.assertTrue(exp, "Mutations were blocked")


    def test_rebalance_out_disk_usage_guardrail(self):

        number_of_docs = self.input.param("number_of_docs", 10)
        error_code = 'BUCKET_DISK_SPACE_TOO_LOW'

        self.log.info("Starting initial data load...")
        self.create_start = 0
        self.create_end = self.init_items_load
        self.new_loader()
        self.doc_loading_tm.getAllTaskResult()
        self.printOps.end_task()

        current_disk_usage = self.check_disk_usage_per_node(self.cluster)
        self.log.info("Current disk usage of nodes = {}".format(current_disk_usage))
        self.max_disk_usage = max(current_disk_usage) - 0.5
        status, content = \
            BucketHelper(self.cluster.master).set_max_disk_usage_guardrails(self.max_disk_usage)
        if status:
            self.log.info("Max disk usage Guardrails set {}".format(content))

        self.sleep(30, "Wait for 30 seconds after hitting the disk usage guardrail")
        self.bucket_util.print_bucket_stats(self.cluster)

        self.log.info("Inserting docs after hitting guardrail")
        result = self.insert_new_docs_sdk(num_docs=number_of_docs,
                                          bucket=self.bucket,
                                          doc_key="new_docs")
        self.log.info("Validating SDK error messages")
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

        self.assertFalse(rebalance_task.result, "Rebalance-out was allowed")
        self.log.info("Rebalance-out was restricted after hitting disk usage guardrail")


    def test_add_node_disk_usage_guardrail(self):

        number_of_docs = self.input.param("number_of_docs", 10)
        error_code = 'BUCKET_DISK_SPACE_TOO_LOW'

        self.log.info("Starting initial data load...")
        self.create_start = 0
        self.create_end = self.init_items_load
        self.new_loader()
        self.doc_loading_tm.getAllTaskResult()
        self.printOps.end_task()

        current_disk_usage = self.check_disk_usage_per_node(self.cluster)
        self.log.info("Current disk usage of nodes = {}".format(current_disk_usage))
        self.max_disk_usage = max(current_disk_usage) - 0.5
        status, content = \
            BucketHelper(self.cluster.master).set_max_disk_usage_guardrails(self.max_disk_usage)
        if status:
            self.log.info("Max disk usage Guardrails set {}".format(content))

        self.sleep(30, "Wait for 30 seconds after hitting the disk usage guardrail")
        self.bucket_util.print_bucket_stats(self.cluster)

        self.log.info("Inserting docs after hitting guardrail")
        result = self.insert_new_docs_sdk(num_docs=number_of_docs,
                                          bucket=self.bucket,
                                          doc_key="new_docs")
        self.log.info("Validating SDK error messages")
        for res in result:
            exp = res["status"] == False and error_code in res["error"]
            self.assertTrue(exp, "Mutations were not blocked")
        self.log.info("Expected error code {} was seen on all inserts".format(error_code))

        spare_node = self.cluster.servers[0]
        rest = RestConnection(self.cluster.master)
        services = rest.get_nodes_services()
        services_on_target_node = services[(self.cluster.master.ip + ":"
                                            + str(self.cluster.master.port))]
        self.log.info("Rebalancing-in the node {}".format_map(spare_node.ip))
        rebalance_in_task = self.task.async_rebalance(self.cluster,
                                    to_add=[spare_node],
                                    to_remove=[],
                                    check_vbucket_shuffling=False,
                                    services=[",".join(services_on_target_node)])
        self.task_manager.get_task_result(rebalance_in_task)
        self.assertTrue(rebalance_in_task.result, "Rebalance-in of a node failed")
        self.sleep(10, "Wait for a few seconds after rebalance-in")

        disk_usages = self.check_disk_usage_per_node(self.cluster)
        self.log.info("Disk usages after adding a node = {}".format(disk_usages))

        mutations_allowed = not self.check_if_disk_usage_guardrail_breached(disk_usages,
                                                                    self.max_disk_usage)
        self.log.info("Inserting docs after adding a node")
        result = self.insert_new_docs_sdk(num_docs=100,
                                          bucket=self.bucket,
                                          doc_key="new_docs")
        for res in result:
            if mutations_allowed:
                exp = res["status"] == True
                self.assertTrue(exp, "Mutations are blocked even after adding a node")
            else:
                exp = res["status"] == False and error_code in res["error"]
                self.assertTrue(exp, "Mutations were not blocked")

    def test_swap_rebalance_with_disk_space_guardrail(self):

        number_of_docs = self.input.param("number_of_docs", 10)
        error_code = 'BUCKET_DISK_SPACE_TOO_LOW'

        self.log.info("Starting initial data load...")
        self.create_start = 0
        self.create_end = self.init_items_load
        self.new_loader()
        self.doc_loading_tm.getAllTaskResult()
        self.printOps.end_task()

        current_disk_usage = self.check_disk_usage_per_node(self.cluster)
        self.log.info("Current disk usage of nodes = {}".format(current_disk_usage))
        self.max_disk_usage = max(current_disk_usage) - 0.5
        status, content = \
            BucketHelper(self.cluster.master).set_max_disk_usage_guardrails(self.max_disk_usage)
        if status:
            self.log.info("Max disk usage Guardrails set {}".format(content))

        self.sleep(30, "Wait for 30 seconds after hitting the disk usage guardrail")
        self.bucket_util.print_bucket_stats(self.cluster)

        self.log.info("Inserting docs after hitting guardrail")
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


    def check_disk_usage_per_node(self, cluster):

        disk_usage_nodes = []
        for server in cluster.kv_nodes:
            _, res = RestConnection(server).query_prometheus("sys_disk_usage_ratio")
            for item in res["data"]["result"]:
                if item["metric"]["disk"] == "/data":
                    disk_usage_nodes.append(float(item["value"][1]) * 100)

        return disk_usage_nodes

    def check_if_disk_usage_guardrail_breached(self, disk_usage_nodes, threshold):

        for disk_usage in disk_usage_nodes:
            if disk_usage > threshold:
                return True
        return False
