from pytests.serverless.serverless_onprem_basetest import \
    ServerlessOnPremBaseTest
from BucketLib.bucket import Bucket
from threading import Thread
from cbas_utils.cbas_utils import CBASRebalanceUtil ,CbasUtil
from collections_helper.collections_spec_constants import MetaCrudParams
from Cb_constants import CbServer
from Jython_tasks.task import ConcurrentFailoverTask
from membase.api.rest_client import RestConnection
from pytests.bucket_collections.collections_base import CollectionBase


class TenantManagementOnPremFailover(ServerlessOnPremBaseTest):

    def setUp(self):
        super(TenantManagementOnPremFailover, self).setUp()
        self.num_node_failures = self.input.param("num_node_failures", 1)
        self.timeout = self.input.param("timeout", 200)
        self.get_from_engaged = self.input.param("get_from_engaged", None)
        self.nodes_in = self.input.param("nodes_in", 0)
        self.validate_bucket_creation = self.input.param("validate_bucket_creation", True)
        self.pick_zone_wise = self.input.param("pick_zone_wise", False)
        self.spec_name = self.input.param("bucket_spec", None)
        self.data_spec_name = self.input.param("data_spec_name", None)
        self.failure_type = self.input.param("failure_type", "stop_memcached")
        self.current_fo_strategy = self.input.param("current_fo_strategy",
                                                    "auto")
        self.async_data_load = self.input.param("async_data_load", True)
        self.max_count = self.input.param("maxCount", 1)
        self.doc_spec_name = self.input.param("doc_spec_name",
                                              "initial_load")
        self.recovery_strategy = self.input.param("recovery_strategy", "full")
        self.validate_stat = self.input.param("validate_stat", False)
        self.enable_data_load = self.input.param("data_loading", True)
        self.cbas_util = CbasUtil(self.task)
        self.rebalance_util = CBASRebalanceUtil(
            self.cluster_util, self.bucket_util, self.task, True,
            self.cbas_util)
        self.servers_to_fail = []
        self.rest = RestConnection(self.cluster.master)
        self.zone_affected = dict()
        self.zone_map = dict()
        self.get_zone_map()
        if not self.pick_zone_wise:
            self.servers_to_fail = self.cluster.servers[1:self.num_node_failures
                                                          + 1]
        if self.spec_name:
            CollectionBase.deploy_buckets_from_spec_file(self)
            self.create_sdk_clients()
        self.bucket_util.print_bucket_stats(self.cluster)

        validation = self.bucket_util.validate_serverless_buckets(
            self.cluster, self.cluster.buckets)
        self.assertTrue(validation, "Bucket validation failed")

    def tearDown(self):
        super(TenantManagementOnPremFailover, self).tearDown()

    @staticmethod
    def data_load_spec():
        spec = {
            # Scope/Collection ops params
            MetaCrudParams.COLLECTIONS_TO_FLUSH: 0,
            MetaCrudParams.COLLECTIONS_TO_DROP: 0,

            MetaCrudParams.SCOPES_TO_DROP: 0,
            MetaCrudParams.SCOPES_TO_ADD_PER_BUCKET: 2,
            MetaCrudParams.COLLECTIONS_TO_ADD_FOR_NEW_SCOPES: 2,

            MetaCrudParams.COLLECTIONS_TO_ADD_PER_BUCKET: 1,

            MetaCrudParams.BUCKET_CONSIDERED_FOR_OPS: "all",
            MetaCrudParams.SCOPES_CONSIDERED_FOR_OPS: "all",
            MetaCrudParams.COLLECTIONS_CONSIDERED_FOR_OPS: "all",

            # Doc loading params
            "doc_crud": {
                MetaCrudParams.DocCrud.COMMON_DOC_KEY: "test_collections",
                MetaCrudParams.DocCrud.CREATE_PERCENTAGE_PER_COLLECTION: 30,
                MetaCrudParams.DocCrud.NUM_ITEMS_FOR_NEW_COLLECTIONS: 1000,
                MetaCrudParams.DocCrud.READ_PERCENTAGE_PER_COLLECTION: 0,
                MetaCrudParams.DocCrud.UPDATE_PERCENTAGE_PER_COLLECTION: 0,
                MetaCrudParams.DocCrud.REPLACE_PERCENTAGE_PER_COLLECTION: 0,
                MetaCrudParams.DocCrud.DELETE_PERCENTAGE_PER_COLLECTION: 0,
            },

        }
        return spec

    def create_sdk_clients(self, buckets=None):
        if not buckets:
            buckets = self.cluster.buckets
        if self.enable_data_load:
            CollectionBase.create_sdk_clients(
                self.task_manager.number_of_threads,
                self.cluster.master,
                buckets,
                self.sdk_client_pool,
                self.sdk_compression)

    def __update_server_obj(self):
        temp_data = self.servers_to_fail
        self.servers_to_fail = dict()
        for node_obj in temp_data:
            if self.nodes_in > 0:
                if self.zone_map[node_obj] in self.zone_affected:
                    self.zone_affected[self.zone_map[node_obj]] += 1
                else:
                    self.zone_affected[self.zone_map[node_obj]] = 1
            self.servers_to_fail[node_obj] = self.failure_type

    def get_zone_map(self):
        for zones in self.rest.get_zone_names():
            servers = []
            nodes = self.rest.get_nodes_in_zone(zones)
            for server in self.cluster.servers:
                if server.ip in nodes:
                    self.zone_map[server] = zones
                    servers.append(server)
            if self.pick_zone_wise and self.cluster.master.ip not in nodes \
                    and len(self.servers_to_fail) == 0:
                self.servers_to_fail = servers

    def run_recovery_rebalance(self):
        nodes_out = []
        if not self.recovery_strategy == "remove":
            for node in self.servers_to_fail:
                self.rest.set_recovery_type(otpNode='ns_1@' + node.ip,
                                            recoveryType=self.recovery_strategy)
        # replacing failed nodes with new one
        nodes_in = self.cluster.servers[
                   self.nodes_init:self.nodes_init + self.nodes_in]
        rebalance_task = self.task.async_rebalance(self.cluster,
                                                   nodes_in, nodes_out,
                                                   retry_get_process_num=2000,
                                                   add_nodes_server_groups =
                                                   self.zone_affected)
        self.task_manager.get_task_result(rebalance_task)
        self.assertTrue(rebalance_task.result, "Rebalance failed")

    def update_failover_nodes(self):
        engaged = set()
        for bucket in self.cluster.buckets:
            for nodes in bucket.servers:
                if nodes not in [self.cluster.master]:
                    engaged.add(nodes)
        engaged = list(engaged)
        if self.get_from_engaged:
            self.servers_to_fail = engaged[: self.num_node_failures]
            return
        self.servers_to_fail = list(set(set(self.cluster.servers[:self.nodes_init])
                                        - set(engaged)) - {self.cluster.master}
                                    )[:self.num_node_failures]

    def test_bucket_creation(self):
        bucket_name = "test_buckets_"
        self.create_serverless_bucket(bucket_name)
        for bucket in self.cluster.buckets:
            if bucket_name in bucket.name:
                for server in bucket.servers:
                    self.assertTrue(server not in self.servers_to_fail,
                                    "New Buckets Allocated "
                                    "v-buckets in failed over nodes")

    def failover_task(self):
        try:
            rest_nodes = self.rest.get_nodes()
            self.__update_server_obj()
            if self.current_fo_strategy == CbServer.Failover.Type.AUTO:
                status = self.rest.update_autofailover_settings(
                    True, self.timeout, maxCount=self.max_count)
                self.assertTrue(status, "Auto-failover enable failed")
                failover_task = ConcurrentFailoverTask(
                    task_manager=self.task_manager, master=self.cluster.master,
                    servers_to_fail=self.servers_to_fail,
                    expected_fo_nodes=self.num_node_failures,
                    task_type="induce_failure")
                self.task_manager.add_new_task(failover_task)
                self.task_manager.get_task_result(failover_task)
                if failover_task.result is False:
                    self.fail("Failure during concurrent failover procedure")
            elif self.current_fo_strategy == CbServer.Failover.Type.GRACEFUL:
                self.rest.monitorRebalance()
                for node in self.servers_to_fail:
                    node = [t_node for t_node in rest_nodes
                            if t_node.ip == node.ip][0]
                    status = self.rest.fail_over(node.id, graceful=True)
                    if status is False:
                        self.fail("Graceful failover failed for %s" % node)
                    self.sleep(5, "Wait for failover to start")
                    reb_result = self.rest.monitorRebalance()
                    self.assertTrue(reb_result, "Graceful failover failed")
            elif self.current_fo_strategy == CbServer.Failover.Type.FORCEFUL:
                self.rest.monitorRebalance()
                for node in self.servers_to_fail:
                    node = [t_node for t_node in rest_nodes
                            if t_node.ip == node.ip][0]
                    status = self.rest.fail_over(node.id, graceful=False)
                    if status is False:
                        self.fail("Hard failover failed for %s" % node)
                    self.sleep(5, "Wait for failover to start")
                    reb_result = self.rest.monitorRebalance()
                    self.assertTrue(reb_result, "Hard failover failed")
        except Exception as e:
            self.log.error("Exception occurred: %s" % str(e))
        finally:
            self.rest.update_autofailover_settings(
                enabled=False, timeout=self.timeout, maxCount=self.max_count)

            if self.current_fo_strategy == CbServer.Failover.Type.AUTO:
                failover_task = ConcurrentFailoverTask(
                    task_manager=self.task_manager, master=self.cluster.master,
                    servers_to_fail=self.servers_to_fail,
                    task_type="revert_failure")
                self.task_manager.add_new_task(failover_task)
                self.task_manager.get_task_result(failover_task)
                if failover_task.result is False:
                    self.fail("Failure during reverting failover operation")

    def create_serverless_bucket(self, name_prefix="bucket_", num=2,
                                 weight=1, wait_for_warmup=True,
                                 replicas=Bucket.ReplicaNum.TWO):
        bucket_objects = []
        def __get_bucket_params(b_name, ram_quota=256, width=1,
                                weight=1):
            self.log.debug("Creating bucket param")
            return {
                Bucket.name: b_name,
                Bucket.replicaNumber: replicas,
                Bucket.ramQuotaMB: ram_quota,
                Bucket.storageBackend: Bucket.StorageBackend.magma,
                Bucket.width: width,
                Bucket.weight: weight
            }
        for i in range(num):
            name = name_prefix + str(i)
            bucket_params = __get_bucket_params(
                b_name=name, weight=weight)
            bucket_obj = Bucket(bucket_params)
            try:
                self.bucket_util.create_bucket(self.cluster, bucket_obj,
                                               wait_for_warmup=wait_for_warmup)
                bucket_objects.append(bucket_obj)
            except Exception as e:
                raise e
        self.create_sdk_clients(buckets=bucket_objects)

    def test_failover_during_update(self):
        data_load_task = None
        if self.validate_stat:
            self.expected_stat = self.bucket_util.get_initial_stats(self.cluster.buckets)
        if self.enable_data_load and self.async_data_load:
            data_load_task = self.rebalance_util.data_load_collection(
                self.cluster, self.doc_spec_name, False, async_load=True)
        for bucket in self.cluster.buckets:
            self.bucket_util.update_bucket_property(self.cluster.master, bucket,
                                                    bucket_width=
                                                    self.desired_width,
                                                    bucket_weight=
                                                    self.desired_weight)
            if self.desired_width:
                bucket.serverless.width = self.desired_width
            if self.desired_weight:
                bucket.serverless.weight = self.desired_weight
        if self.get_from_engaged is not None:
            self.update_failover_nodes()
        rebalance_task = self.task.async_rebalance(self.cluster, [], [],
                                                   retry_get_process_num=3000)
        self.sleep(5, "Wait for rebalance to make progress")
        self.failover_task()
        self.task_manager.get_task_result(rebalance_task)
        if self.validate_bucket_creation:
            self.test_bucket_creation()
        if self.enable_data_load and self.async_data_load:
            self.rebalance_util.wait_for_data_load_to_complete(data_load_task,
                                                               False)
        if self.validate_stat:
            for bucket in self.cluster.buckets:
                self.expected_stat[bucket.name]["wu"] += \
                    self.bucket_util.get_total_items_bucket(bucket)
            self.bucket_util.validate_stats(self.cluster.buckets, self.expected_stat)
        self.run_recovery_rebalance()
        validation = self.bucket_util.validate_serverless_buckets(
            self.cluster, self.cluster.buckets)
        self.assertTrue(validation, "Bucket validation failed")
        if self.validate_stat:
            self.bucket_util.validate_stats(self.cluster.buckets, self.expected_stat)

    def test_failover_non_included_node(self):
        weight = 1000
        initial_bucket = self.cluster.buckets[0]
        bucket_name = "test_bucket"
        for i in range(2):
            self.create_serverless_bucket(bucket_name + str(i), 1, weight)
            weight += 1000
        self.rebalance_util.data_load_collection(
            self.cluster, self.doc_spec_name, False, async_load=True)
        target_bucket = self.cluster.buckets[1]
        second_bucket = self.cluster.buckets[2]
        self.bucket_util.update_bucket_property(self.cluster.master,
                                                target_bucket,
                                                bucket_width=2)
        target_bucket.serverless.width = 2
        rebalance_task = self.task.async_rebalance(self.cluster, [], [],
                                                   retry_get_process_num=3000)

        self.sleep(5, "wait for re-balance to progress")
        self.servers_to_fail = [second_bucket.servers[0]]
        self.failover_task()
        self.run_recovery_rebalance()
        self.task_manager.get_task_result(rebalance_task)
        validation = self.bucket_util.validate_serverless_buckets(
            self.cluster, self.cluster.buckets)
        self.assertTrue(validation, "Bucket validation failed")
        # making sure bucket expanded to nodes with less total weight
        self.assertTrue(set(initial_bucket.servers).
                        issubset(set(target_bucket.servers)),
                        "Target bucket not sub-set of nodes with less weight")
        self.assertTrue(set(second_bucket.servers).
                         isdisjoint(set(target_bucket.servers)),
                         "Target bucket not expected in nodes with more weight")

    def test_create_bucket_during_failover(self):
        self.thread_fail_Exception = None
        self.timeout = 240
        self.create_buckets = True

        def parallel_bucket_creation():
            iter = 0
            while self.create_buckets and \
                    self.rest._rebalance_progress_status() != "running":
                iter += 1
                try:
                    self.create_serverless_bucket("test_bucket" + str(iter),
                                                  1, 30, wait_for_warmup=False)
                except Exception as e:
                    if self.rest._rebalance_progress_status() == "running":
                        break
                    self.thread_fail_Exception = e
                    return
                self.sleep(5, "waiting 5 seconds before next bucket creation")

        data_load_task = self.rebalance_util.data_load_collection(
            self.cluster, "volume_test_load_with_CRUD_on_collections",
            False, async_load=True)

        # start bucket creation thread
        bucket_creation_thread = Thread(target=parallel_bucket_creation)
        bucket_creation_thread.start()
        self.failover_task()
        self.create_serverless_bucket("after_failover", 2, 30,
                                      wait_for_warmup=True)
        self.run_recovery_rebalance()

        # thread stopped
        self.create_buckets = False
        bucket_creation_thread.join()

        for bucket in self.cluster.buckets:
            self.bucket_util._wait_warmup_completed(bucket, wait_time=60)

        # bucket creation after node recovered
        self.create_serverless_bucket("after_recovery", 2, 30,
                                      wait_for_warmup=True)
        if self.thread_fail_Exception is not None:
            raise Exception(self.thread_fail_Exception)
        self.rebalance_util.wait_for_data_load_to_complete(data_load_task,
                                                           False)
        # assertions
        self.bucket_util.validate_doc_loading_results(data_load_task)
        bucket_clients = []
        self.create_sdk_clients(buckets=bucket_clients)
        data_spec = self.data_load_spec()
        self.bucket_util.run_scenario_from_spec(self.task, self.cluster,
                                                self.cluster.buckets,
                                                data_spec)
        validation = self.bucket_util.validate_serverless_buckets(
            self.cluster, self.cluster.buckets)
        self.assertTrue(validation, "Bucket validation failed")

    def bucket_update_during_failover(self):
        init_replicas = self.input.param("init_replicas", 2)
        if self.spec_name:
            doc_loading_spec_name = "initial_load"
            doc_loading_spec = self.bucket_util.get_crud_template_from_package(
                doc_loading_spec_name)
            task = self.bucket_util.run_scenario_from_spec(self.task,
                                                           self.cluster,
                                                           self.cluster.buckets,
                                                           doc_loading_spec,
                                                           mutation_num=0,
                                                           async_load=True)
            self.task.jython_task_manager.get_task_result(task)
            self.bucket_util.validate_doc_loading_results(task)
            if task.result is False:
                raise Exception("doc load/verification failed")
        if self.num_buckets > 0:
            self.create_serverless_bucket("test_buckets", self.num_buckets, 30,
                                          wait_for_warmup=True,
                                          replicas=init_replicas)

        data_spec = self.data_load_spec()
        data_spec[MetaCrudParams.DocCrud.NUM_ITEMS_FOR_NEW_COLLECTIONS] \
            = 100000
        data_task = self.bucket_util.run_scenario_from_spec(self.task,
                                                        self.cluster,
                                                self.cluster.buckets,
                                                data_spec, async_load=True)
        if self.pick_zone_wise:
            self.servers_to_fail = self.servers_to_fail[
                                   :self.num_node_failures]
        failover_thread = Thread(target=self.failover_task)
        failover_thread.start()
        for bucket in self.cluster.buckets:
            self.bucket_util.update_bucket_property(self.cluster.master, bucket,
                                                    bucket_width=
                                                    self.desired_width,
                                                    bucket_weight=
                                                    self.desired_weight)
            if self.desired_width:
                bucket.serverless.width = self.desired_width
            if self.desired_weight:
                bucket.serverless.weight = self.desired_weight

        rebalance_task = self.task.async_rebalance(self.cluster, [], [],
                                                   retry_get_process_num=3000)
        self.task_manager.get_task_result(rebalance_task)
        failover_thread.join()
        self.run_recovery_rebalance()
        self.task.jython_task_manager.get_task_result(data_task)
        validation = self.bucket_util.validate_serverless_buckets(
            self.cluster, self.cluster.buckets)
        self.assertTrue(validation, "Bucket validation failed")
        self.assertTrue(self.rest.is_cluster_balanced(), "Cluster unbalanced")
