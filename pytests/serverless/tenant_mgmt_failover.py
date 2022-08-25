from pytests.serverless.serverless_onprem_basetest import \
    ServerlessOnPremBaseTest
from BucketLib.bucket import Bucket
from cbas_utils.cbas_utils import CBASRebalanceUtil ,CbasUtil
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
        self.validate_bucket_creation = self.input.param("validate_bucket_creation"
                                                         , True)
        self.pick_zone_wise = self.input.param("pick_zone_wise", False)
        self.failure_type = self.input.param("failure_type", "stop_memcached")
        self.current_fo_strategy = self.input.param("current_fo_strategy",
                                                    "auto")
        self.async_data_load = self.input.param("async_data_load", True)
        self.num_nodes_to_be_failover = self.input.param("num_nodes_to_be_failover", 1)
        self.max_count = self.input.param("maxCount", 1)
        self.doc_spec_name = self.input.param("doc_spec_name",
                                              "volume_test_load_with_CRUD_on_collections")
        self.recovery_strategy = self.input.param("recovery_strategy", "full")
        self.cbas_util = CbasUtil(self.task)
        self.rebalance_util = CBASRebalanceUtil(
            self.cluster_util, self.bucket_util, self.task, True,
            self.cbas_util)
        self.servers_to_fail = []
        self.rest = RestConnection(self.cluster.master)
        if not self.pick_zone_wise:
            self.servers_to_fail = self.cluster.servers[1:self.num_node_failures
                                                          + 1]
        else:
            self.get_nodes_from_az()
        self.create_serverless_bucket()

    def tearDown(self):
        super(TenantManagementOnPremFailover, self).tearDown()

    def __update_server_obj(self):
        temp_data = self.servers_to_fail
        self.servers_to_fail = dict()
        for node_obj in temp_data:
            self.servers_to_fail[node_obj] = self.failure_type

    def get_nodes_from_az(self):
        self.servers_to_fail = []
        for zones in self.rest.get_zone_names():
            nodes = self.rest.get_nodes_in_zone(zones)
            if self.cluster.master.ip in nodes:
                continue
            for server in self.cluster.servers:
                if server.ip in nodes:
                    self.servers_to_fail.append(server)
            return

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
        self.servers_to_fail = list(set(set(self.cluster.servers) - set(engaged)) -
                                    {self.cluster.master})[:self.num_node_failures]

    def test_bucket_creation(self):
        bucket_name = "test_buckets_"
        self.create_serverless_bucket(bucket_name, False)
        for bucket in self.cluster.buckets:
            if bucket_name in bucket.name:
                for server in bucket.servers:
                    self.assertTrue(server not in self.servers_to_fail,
                                    "New Buckets Allocated "
                                    "v-buckets in failed over nodes")

    def failover_task(self):
        try:
            rest_nodes = self.rest.get_nodes()
            if self.current_fo_strategy == CbServer.Failover.Type.AUTO:
                status = self.rest.update_autofailover_settings(
                    True, self.timeout, maxCount=self.max_count)
                self.assertTrue(status, "Auto-failover enable failed")
                self.__update_server_obj()
                failover_task = ConcurrentFailoverTask(
                    task_manager=self.task_manager, master=self.cluster.master,
                    servers_to_fail=self.servers_to_fail,
                    expected_fo_nodes=self.num_nodes_to_be_failover,
                    task_type="induce_failure")
                self.task_manager.add_new_task(failover_task)
                self.task_manager.get_task_result(failover_task)
                if failover_task.result is False:
                    self.fail("Failure during concurrent failover procedure")
            elif self.current_fo_strategy == CbServer.Failover.Type.GRACEFUL:
                for node in self.servers_to_fail:
                    node = [t_node for t_node in rest_nodes
                            if t_node.ip == node.ip][0]
                    status = self.rest.fail_over(node.id, graceful=True)
                    if status is False:
                        self.fail("Graceful failover failed for %s" % node)
                    self.sleep(5, "Wait for failover to start")
                    reb_result = self.rest.monitorRebalance()
                    self.assertTrue(reb_result, "Graceful failover failed")
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

    # temporary method
    def create_serverless_bucket(self, name_prefix="bucket_", validate=True):
        def __get_bucket_params(b_name, ram_quota=256, width=1,
                                weight=1):
            self.log.debug("Creating bucket param")
            return {
                Bucket.name: b_name,
                Bucket.replicaNumber: Bucket.ReplicaNum.TWO,
                Bucket.ramQuotaMB: ram_quota,
                Bucket.storageBackend: Bucket.StorageBackend.magma,
                Bucket.width: width,
                Bucket.weight: weight
            }
        for i in range(self.num_buckets):
            name = name_prefix + str(i)
            bucket_params = __get_bucket_params(
                b_name=name,
                width=self.bucket_width, )
            bucket_obj = Bucket(bucket_params)
            self.bucket_util.create_bucket(self.cluster, bucket_obj,
                                           wait_for_warmup=True)
            if validate:
                validation = self.bucket_util.validate_serverless_buckets(
                    self.cluster, self.cluster.buckets)
                self.assertTrue(validation, "Bucket validation failed")
                CollectionBase.create_sdk_clients(
                    self.task_manager.number_of_threads,
                    self.cluster.master,
                    self.cluster.buckets,
                    self.sdk_client_pool,
                    self.sdk_compression)

    def test_failover_during_update(self):
        if self.async_data_load:
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
        self.sleep(3, "Wait for rebalance to make progress")
        self.failover_task()
        self.task_manager.get_task_result(rebalance_task)
        if self.validate_bucket_creation:
            self.test_bucket_creation()
        if self.async_data_load:
            self.rebalance_util.wait_for_data_load_to_complete(data_load_task,
                                                               False)
        for node in self.servers_to_fail:
            self.rest.add_back_node("ns_1@{}".format(node.ip))
            self.rest.set_recovery_type("ns_1@{}".format(node.ip),
                                        self.recovery_strategy)
        rebalance_task = self.task.async_rebalance(self.cluster, [], [],
                                                   retry_get_process_num=2000)
        self.task_manager.get_task_result(rebalance_task)
        self.assertTrue(rebalance_task.result, "Rebalance failed")
        validation = self.bucket_util.validate_serverless_buckets(
            self.cluster, self.cluster.buckets)
        self.assertTrue(validation, "Bucket validation failed")


