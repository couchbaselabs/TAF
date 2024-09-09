import json
import urllib
import time
from random import choice, randint, sample, randrange

from BucketLib.BucketOperations import BucketHelper
from collections_helper.collections_spec_constants import MetaCrudParams
from BucketLib.bucket import Bucket
from cb_constants import CbServer
from threading import Thread
from couchbase_helper.documentgenerator import doc_generator
from membase.api.rest_client import RestConnection
from serverless.serverless_onprem_basetest import ServerlessOnPremBaseTest
from error_simulation.cb_error import CouchbaseError
from pytests.bucket_collections.collections_base import CollectionBase
from shell_util.remote_connection import RemoteMachineShellConnection


class TenantManagementOnPrem(ServerlessOnPremBaseTest):
    def setUp(self):
        super(TenantManagementOnPrem, self).setUp()
        self.b_create_endpoint = "pools/default/buckets"

        self.spec_name = self.input.param("bucket_spec", None)
        self.data_spec_name = self.input.param("data_spec_name", None)
        self.negative_case = self.input.param("negative_case", False)
        self.bucket_util.delete_all_buckets(self.cluster)
        self.nodes_in = self.input.param("nodes_in", 0)
        self.nodes_out = self.input.param("nodes_out", 0)
        if self.spec_name:
            CollectionBase.deploy_buckets_from_spec_file(self)
        elif self.input.param("with_default_bucket", False):
            old_weight = self.bucket_weight
            self.bucket_weight = 1
            self.create_bucket(self.cluster)
            self.bucket_weight = old_weight

        # Load initial data from spec file
        if self.data_spec_name:
            pass

        self.bucket_util.print_bucket_stats(self.cluster)

    def tearDown(self):
        super(TenantManagementOnPrem, self).tearDown()

    @staticmethod
    def data_load_spec():
        spec = {
            # Scope/Collection ops params
            MetaCrudParams.COLLECTIONS_TO_FLUSH: 0,
            MetaCrudParams.COLLECTIONS_TO_DROP: 0,

            MetaCrudParams.SCOPES_TO_DROP: 0,
            MetaCrudParams.SCOPES_TO_ADD_PER_BUCKET: 3,
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

    def get_zone_map(self, rest):
        zone_map = dict()
        for zones in rest.get_zone_names():
            servers = []
            nodes = rest.get_nodes_in_zone(zones)
            for server in self.cluster.servers:
                if server.ip in nodes:
                    servers.append(server)
            zone_map[zones] = servers
        return zone_map

    def __get_bucket_params(self, b_name, ram_quota=256, width=1, weight=1,
                            replica=Bucket.ReplicaNum.TWO):
        self.log.debug("Creating bucket param")
        return {
            Bucket.name: b_name,
            Bucket.replicaNumber: replica,
            Bucket.ramQuotaMB: ram_quota,
            Bucket.storageBackend: Bucket.StorageBackend.magma,
            Bucket.width: width,
            Bucket.weight: weight
        }

    def create_sdk_clients(self, buckets=None):
        if not buckets:
            buckets = self.cluster.buckets
        CollectionBase.create_sdk_clients(
            self.cluster,
            self.task_manager.number_of_threads,
            self.cluster.master,
            buckets,
            self.sdk_compression)

    def test_cluster_scaling(self):
        """
        1. Start with 3 node cluster (3 AZs)
        2. Create default bucket with some data
        3. Add multiple servers in followed by out operations
        4. Make sure the cluster remains 'balanced' after each operation
           and bucket remains on the same set of nodes deployed initially
        """
        iterations = self.input.param("iterations", 1)
        self.create_bucket(self.cluster)
        self.bucket_util.print_bucket_stats(self.cluster)
        for _ in range(iterations):
            num_nodes_scaled = randint(1, 3)
            nodes_scaled = self.cluster.servers[
                self.nodes_init:self.nodes_init+num_nodes_scaled]
            az = dict([(t_az, 1) for t_az in sample(self.server_groups,
                                                    num_nodes_scaled)])

            self.log.info("Rebalance_map: %s" % az)
            reb_task = self.task.async_rebalance(
                cluster=self.cluster, to_add=nodes_scaled, to_remove=[],
                add_nodes_server_groups=az)
            self.task_manager.get_task_result(reb_task)
            self.assertTrue(reb_task.result, "Cluster scaling-up failed")
            self.assertTrue(
                RestConnection(self.cluster.master).is_cluster_balanced(),
                "Cluster is reported as unbalanced")
            self.assertTrue(
                self.bucket_util.validate_serverless_buckets(
                    self.cluster, self.cluster.buckets),
                "Bucket validation failed")

            reb_task = self.task.async_rebalance(
                cluster=self.cluster, to_add=[], to_remove=nodes_scaled)
            self.task_manager.get_task_result(reb_task)
            self.assertTrue(reb_task.result, "Cluster scaling-down failed")
            self.assertTrue(
                RestConnection(self.cluster.master).is_cluster_balanced(),
                "Cluster is reported as unbalanced")
            self.assertTrue(
                self.bucket_util.validate_serverless_buckets(
                    self.cluster, self.cluster.buckets),
                "Bucket validation failed")

    def test_create_bucket(self):
        """
        Creates one bucket at a time with,
        1. Default numVBuckets
        2. Explicit numVBuckets value (min/max/random)
        """
        def run_test():
            self.create_bucket(self.cluster)

            result = self.bucket_util.validate_serverless_buckets(
                self.cluster, self.cluster.buckets)
            self.assertTrue(result, "Bucket validation failed")

            """
            TODO: Following goes into validate_serverless_buckets(possibly)
             - Validate bucket's vBucket distribution
             - Validate weight wrt cluster.
             - Cluster node's max_weight computation
            """

            # Validate cluster's balance with bucket
            self.assertTrue(
                RestConnection(self.cluster.master).is_cluster_balanced(),
                "Cluster is reported as unbalanced")

            self.bucket_util.delete_bucket(self.cluster,
                                           self.cluster.buckets[0])

            # Validate cluster's balance post bucket deletion
            self.assertTrue(
                RestConnection(self.cluster.master).is_cluster_balanced(),
                "Cluster is reported as unbalanced")

        for num_vb in [None, Bucket.vBucket.MIN_VALUE,
                       Bucket.vBucket.MAX_VALUE]:
            self.vbuckets = num_vb
            self.log.info("Creating bucket with vbucket_num=%s"
                          % self.vbuckets)
            run_test()

        self.vbuckets = choice(range(Bucket.vBucket.MIN_VALUE+1,
                                     Bucket.vBucket.MAX_VALUE))
        run_test()

    def test_create_bucket_negative(self):
        """
        1. Create bucket with width=0
        2. Create bucket with unsupported numVBuckets value
        3. Create bucket with width > available sub_clusters
        4. Create bucket with weight > cluster_supported weight
        """

        def create_bucket():
            params = urllib.urlencode(bucket_params)
            status, cont, _ = helper._http_request(api, helper.POST, params)
            self.assertFalse(status, "Bucket created successfully")
            return json.loads(cont)

        bucket_params = self.__get_bucket_params(b_name="bucket_1", width=0)
        helper = BucketHelper(self.cluster.master)
        api = helper.baseUrl + self.b_create_endpoint

        # Known error strings
        err_width_msg = "width must be 1 or more"
        err_more_width = "Need more space in availability zones"
        err_vb = "Number of vbuckets must be an integer between 16 and 1024"

        # Create bucket with invalid width=0
        self.log.info("Creating bucket with invalid width and num_vbuckets")
        content = create_bucket()["errors"]
        self.assertEqual(content["width"], err_width_msg,
                         "Invalid error message for bucket::width")

        # Create bucket with invalid numVbuckets
        self.log.info("Creating bucket with invalid numVbuckets")
        bucket_params[Bucket.width] = 1
        for vb_num in [None, Bucket.vBucket.MIN_VALUE-1,
                       Bucket.vBucket.MAX_VALUE+1]:
            bucket_params[Bucket.numVBuckets] = vb_num
            content = create_bucket()["errors"]
            self.assertEqual(content["numVbuckets"], err_vb,
                             "Invalid error message for bucket::numVbuckets")

        bucket_params.pop(Bucket.numVBuckets)
        # Create with width > available sub-clusters
        self.log.info("Creating bucket with width > len(sub_cluster)")
        bucket_params[Bucket.width] = \
            len(self.cluster.kv_nodes) \
            / CbServer.Serverless.KV_SubCluster_Size \
            + 1

        content = create_bucket()
        self.assertTrue(err_more_width in content["_"],
                        "Invalid error message for bucket::width")

        # Create with weight > cluster_supported weight
        self.log.info("Creating bucket with weight > MAX_SUPPORTED")
        bucket_params[Bucket.width] = 1
        bucket_params[Bucket.weight] = CbServer.Serverless.MAX_WEIGHT + 1
        content = create_bucket()
        self.assertTrue(err_more_width in content["_"],
                        "Invalid error message for bucket::width")

    def test_create_bucket_with_failed_node(self):
        """
        Create bucket when of the node is unavailable/failed over
        """
        b_index = 1
        recovery_type = self.input.param("recovery_type")
        target_node = choice(self.cluster.kv_nodes)

        # Update master if master is going to be failed over
        if target_node == self.cluster.master:
            self.cluster.master = self.cluster.kv_nodes[-1]

        rest = RestConnection(self.cluster.master)
        node = [node for node in rest.get_nodes()
                if node.ip == target_node.ip][0]

        self.log.info("Master: %s, failing over %s"
                      % (self.cluster.master.ip, target_node.ip))
        rest.fail_over(node.id)

        bucket_params = self.__get_bucket_params(b_name="bucket_%s" % b_index,
                                                 width=self.bucket_width)
        params = urllib.urlencode(bucket_params)
        helper = BucketHelper(self.cluster.master)
        api = helper.baseUrl + self.b_create_endpoint
        self.log.info("Attempting to create bucket")
        status, cont, _ = helper._http_request(api, helper.POST, params)
        self.assertFalse(status, "Bucket created successfully")
        err_msg = "Need more space in availability zones [<<\"%s\">>]."
        try:
            self.assertEqual(json.loads(cont)["_"],
                             err_msg % node.server_group,
                             "Mismatch in the error message")
        finally:
            rest.set_recovery_type(otpNode=node.id, recoveryType=recovery_type)
            self.sleep(5, "Wait before starting '%s' add_back rebalance"
                          % recovery_type)

            self.assertTrue(self.cluster_util.rebalance(self.cluster),
                            "Node add_back rebalance failed")

        self.assertTrue(rest.is_cluster_balanced(), "Cluster unbalanced")

        status, cont, _ = helper._http_request(api, helper.POST, params)
        self.assertTrue(status, "Failed to create bucket")

        bucket = self.bucket_util.get_all_buckets(self.cluster)[0]
        self.bucket_util.get_updated_bucket_server_list(self.cluster, bucket)
        self.bucket_util.is_warmup_complete([bucket])

        self.assertTrue(rest.is_cluster_balanced(), "Cluster unbalanced")

    def test_multi_buckets(self):
        """
        1. Create multiple buckets and validate the bucket distribution
        2. Create an extra bucket to ensure the creation fails
        3. Add a KV sub-cluster and create multi-bucket to make sure
           they get created on the new nodes
        """
        vb_nums = [16, 32, 64, 128, 256, 512, 1024]
        bucket_weights = [i*30 for i in range(13)]
        limit_bucket_by = self.input.param("bucket_limit", "memory")
        random_vb_num = self.input.param("random_vb_num", False)
        expected_err = "Need more space in availability zones"

        # Create max_possible buckets for the given sub_cluster
        rest = RestConnection(self.cluster.master)
        cluster_stats = rest.cluster_status()
        if limit_bucket_by == "memory":
            num_buckets = int(cluster_stats["memoryQuota"] / self.bucket_size)
        elif limit_bucket_by == "weight":
            node_stat = rest.get_nodes_self()
            max_weight = node_stat.limits[CbServer.Services.KV]["weight"]
            num_buckets = int(cluster_stats["memoryQuota"] /
                              self.bucket_size) - 2
            bucket_weights = [max_weight/num_buckets]
        else:
            self.fail("Invalid limit_by field: %s" % limit_bucket_by)

        self.log.info("Sub_cluster #1 - Creating %d buckets with ram=%d"
                      % (num_buckets, self.bucket_size))
        for index in range(num_buckets):
            if random_vb_num:
                self.vbuckets = choice(vb_nums)
            name = "bucket_%d" % index
            self.bucket_weight = choice(bucket_weights)
            self.create_bucket(self.cluster, bucket_name=name)

        # Extra bucket to validate failure condition
        bucket_params = self.__get_bucket_params(
            "extra_bucket", ram_quota=256,
            width=1, weight=self.bucket_weight)
        params = urllib.urlencode(bucket_params)
        helper = BucketHelper(self.cluster.master)
        api = helper.baseUrl + self.b_create_endpoint

        self.log.info("Attempting to create an extra bucket")
        status, content, _ = helper._http_request(api, helper.POST, params)
        self.assertFalse(status, "Extra bucket created successfully")
        self.assertTrue(expected_err in json.loads(content)["_"],
                        "Mismatch in the error message")
        self.bucket_util.print_bucket_stats(self.cluster)

        self.log.info("Adding KV sub_cluster")
        nodes_to_add = self.cluster.servers[
            self.nodes_init
            :self.nodes_init + CbServer.Serverless.KV_SubCluster_Size]
        self.task.rebalance(self.cluster, to_add=nodes_to_add, to_remove=[],
                            add_nodes_server_groups=self.kv_distribution_dict)

        self.log.info("Sub_cluster #2 - Creating %d buckets with ram=%d"
                      % (num_buckets, self.bucket_size))
        for index in range(num_buckets, num_buckets*2):
            if random_vb_num:
                self.vbuckets = choice(vb_nums)
            name = "bucket_%d" % index
            self.bucket_weight = choice(bucket_weights)
            self.create_bucket(self.cluster, bucket_name=name)

        self.log.info("Attempting to create an extra bucket")
        status, content, _ = helper._http_request(api, helper.POST, params)
        self.assertFalse(status, "Extra bucket created successfully")
        self.assertTrue(expected_err in json.loads(content)["_"],
                        "Mismatch in the error message")

    def test_change_bucket_width_weight(self):
        """
       - Increase single / multiple / all bucket's width
        at the same time then trigger rebalance
       - Decrease single / multiple / all buckets' width
        and rebalance
        Cases will be executed both with/without data load
       -Simultaneously increase / decrease the bucket's width+weight
        at a same time and check the results
        """
        # bucket creation way to be updated after spec function available
        # at the base class
        def create_serverless_bucket():
            for i in range(self.num_buckets):
                name = "bucket_"+str(i)
                bucket_params = self.__get_bucket_params(
                    b_name=name, width=self.bucket_width,
                    weight=self.bucket_weight)
                bucket_obj = Bucket(bucket_params)
                self.bucket_util.create_bucket(self.cluster, bucket_obj,
                                               wait_for_warmup=True)

        def data_load():
            doc_loading_spec_name = "initial_load"
            doc_loading_spec = self.bucket_util.get_crud_template_from_package(
                doc_loading_spec_name)
            tasks = self.bucket_util.run_scenario_from_spec(
                self.task, self.cluster, buckets, doc_loading_spec,
                mutation_num=0, async_load=True)
            return tasks

        def verify_data_load(load_task):
            self.task.jython_task_manager.get_task_result(load_task)
            self.bucket_util.validate_doc_loading_results(self.cluster,
                                                          load_task)
            if load_task.result is False:
                raise Exception("doc load/verification failed")

        self.validate_stat = self.input.param("validate_stat", False)
        if self.negative_case:
            self.desired_width = (len(self.cluster.servers) /
                                  CbServer.Serverless.KV_SubCluster_Size) + 1

        scale = self.input.param("bucket_scale", "all")
        enable_data_load = self.input.param("data_loading", True)
        data_load_after_rebalance = self.input.param("data_load_after_rebalance",
                                                     True)
        update_during_rebalance = self.input.param("update_during_rebalance",
                                                   False)
        random_scale = self.input.param("random_scale", False)
        async_load = self.input.param("async_load", False)
        nodes_in = self.cluster.servers[
                   self.nodes_init:self.nodes_init + self.nodes_in]
        nodes_out = self.cluster.servers[self.nodes_init -
                                         self.nodes_out:self.nodes_init]

        task = None
        rebalance_task_during_update = None

        create_serverless_bucket()
        buckets_to_consider = buckets = self.cluster.buckets
        validation = self.bucket_util.validate_serverless_buckets(
            self.cluster, self.cluster.buckets)
        self.assertTrue(validation, "Bucket validation failed")

        if len(buckets) < 1:
            self.fail("no bucket found")
        if scale == 'single':
            buckets_to_consider = buckets[:1]
        elif scale == 'multiple':
            if len(buckets) <= 2:
                self.fail("number of buckets should be greater than 2 for"
                          " width change in multiple buckets")
            buckets_to_consider = buckets[:(len(buckets) / 2)]

        if enable_data_load:
            CollectionBase.create_sdk_clients(
                self.cluster,
                self.task_manager.number_of_threads,
                self.cluster.master,
                buckets,
                self.sdk_compression)
            if async_load:
                task = data_load()
        if update_during_rebalance:
            rest = RestConnection(self.cluster.master)
            zone_map = self.get_zone_map(rest)
            nodes_out = []
            track_zones = dict()
            zone_value = list(zone_map.keys())
            for iter in range(self.nodes_out):
                nodes = zone_map[zone_value[iter % 3]]
                if zone_value[iter % 3] not in track_zones:
                    track_zones[zone_value[iter % 3]] = 0
                else:
                    track_zones[zone_value[iter % 3]] += 1
                if nodes[track_zones[zone_value[iter % 3]]].ip == \
                        self.cluster.master.ip:
                    track_zones[zone_value[iter % 3]] += 1
                nodes_out.append(nodes[track_zones[zone_value[iter % 3]]])

            rebalance_task_during_update = self.task.async_rebalance(
                self.cluster, nodes_in, nodes_out,
                retry_get_process_num=3000)
            self.sleep(10, "Wait for rebalance to make progress")

        if self.validate_stat:
            self.expected_stat = self.bucket_util.get_initial_stats(
                self.cluster.buckets)
        for bucket in buckets_to_consider:
            try:
                set_width = self.desired_width
                set_weight = self.desired_weight
                if random_scale:
                    set_width = randrange(1, self.desired_width+1)
                    set_weight = randrange(30, self.desired_weight)

                self.bucket_util.update_bucket_property(
                    self.cluster.master, bucket,
                    bucket_width=set_width,
                    bucket_weight=set_weight)
                if self.desired_width:
                    bucket.serverless.width = set_width
                if self.desired_weight:
                    bucket.serverless.weight = set_weight
            except Exception as e:
                self.log.error("Exception occurred: %s" % str(e))
                if not self.negative_case:
                    raise e
        if self.negative_case:
            for bucket in buckets_to_consider:
                self.assertTrue(bucket.serverless.width != self.desired_width)
                self.assertTrue(bucket.serverless.weight != self.desired_weight)
        if update_during_rebalance:
            self.task_manager.get_task_result(rebalance_task_during_update)
            self.assertTrue(rebalance_task_during_update.result,
                            "Rebalance Failed")
        rebalance_task = self.task.async_rebalance(self.cluster, [], [],
                                                   retry_get_process_num=3000)
        self.task_manager.get_task_result(rebalance_task)
        self.assertTrue(rebalance_task.result, "Rebalance Failed")

        if self.validate_stat:
            self.bucket_util.validate_stats(self.cluster.buckets, self.expected_stat)
        # validations
        if enable_data_load and async_load:
            verify_data_load(task)
        validation = self.bucket_util.validate_serverless_buckets(
            self.cluster, self.cluster.buckets)
        self.assertTrue(validation, "Bucket validation failed")
        if enable_data_load and data_load_after_rebalance:
            task = data_load()
            verify_data_load(task)
            if self.validate_stat:
                for bucket in self.cluster.buckets:
                    self.expected_stat[bucket.name]["wu"] += \
                        self.bucket_util.get_total_items_bucket(bucket)
                self.bucket_util.validate_stats(self.cluster.buckets, self.expected_stat)
        self.bucket_util.print_bucket_stats(self.cluster)

    def test_continuous_width_updates(self):
        num_itr = self.input.param("iterations", 1)
        self.create_bucket(self.cluster)

        bucket = self.cluster.buckets[0]
        self.cluster.sdk_client_pool.create_clients(
            bucket=bucket, servers=bucket.servers, req_clients=1)

        doc_gen = doc_generator(self.key, 0, self.num_items)
        loading_task = self.task.async_continuous_doc_ops(
            self.cluster, bucket, doc_gen)
        for i in range(1, num_itr+1):
            self.log.info("Iteration :: %s" % i)

            for b_width in [2, 3, 2, 1]:
                self.log.info("Updating bucket width=%s" % b_width)
                self.bucket_util.update_bucket_property(
                    self.cluster.master, bucket, bucket_width=b_width)
                bucket.serverless.width = self.desired_width

                rebalance_task = self.task.async_rebalance(
                    self.cluster, [], [], retry_get_process_num=3000)
                self.task_manager.get_task_result(rebalance_task)

                self.log.critical("Doc loading failures: %s"
                                  % loading_task.fail)

                if rebalance_task.result is not True:
                    loading_task.end_task()
                    self.task_manager.get_task_result(loading_task)
                    self.fail("Rebalance Failed")

        loading_task.end_task()
        self.task_manager.get_task_result(loading_task)

    def test_scaling_rebalance_failures(self):
        """
        Scaling failures during re-balance
        nodes added to accommodate scaling
        weight width updated while data-load happens in parallel
        followed by different re-balance failure scenarios and assertions
        """

        def rebalance_failure(rebal_failure, target_buckets=None,
                              error_type=None, num_target_node=1, interval=20,
                              timeout=100, wait_before_Start=20):
            try:
                self.sleep(wait_before_Start,
                           "waiting before inducing failure")
                if rebal_failure == "delete_bucket":
                    bucket_conn = BucketHelper(self.cluster.master)
                    for bucket_obj in target_buckets:
                        status = bucket_conn.delete_bucket(bucket_obj.name)
                        self.assertFalse(status,
                                         "Bucket wasn't expected to get "
                                         "deleted while re-balance running")
                elif rebal_failure == "induce_error":
                    end_time = time.time() + timeout
                    target_nodes = self.cluster.servers[fail_start_server
                                                        :num_target_node]
                    while time.time() <= end_time:
                        error_sim_list = []
                        for node in target_nodes:
                            shell = RemoteMachineShellConnection(node)
                            error_sim = CouchbaseError(self.log,
                                                       shell,
                                                       node=node)
                            error_sim.create(error_type)
                            error_sim_list.append(error_sim)
                        self.sleep(interval, "Error induced waiting before "
                                             "resolving the same")
                        for error in error_sim_list:
                            error.revert(sim_error)
                        self.sleep(5)
                elif rebal_failure == "stop_rebalance":
                    counter = 0
                    expected_progress = [20, 40, 80]
                    for progress in expected_progress:
                        if rest.is_cluster_balanced():
                            break
                        counter += 1
                        reached = self.cluster_util.rebalance_reached(
                            self.cluster, progress)
                        self.assertTrue(reached,
                                        "Rebalance failed or did not reach "
                                        "{0}%".format(progress))
                        stopped = rest.stop_rebalance(wait_timeout=30)
                        self.assertTrue(stopped,
                                        msg="Unable to stop rebalance")
                        self.bucket_util._wait_for_stats_all_buckets(
                            self.cluster, self.cluster.buckets, timeout=1200)
                        self.task.async_rebalance(self.cluster, [], [],
                                                  retry_get_process_num=3000)
                        self.sleep(5, "Waiting before next iteration")
                    if counter < len(expected_progress):
                        self.log.info("Cluster balanced before stopping for "
                                      "progress{0}".format(
                            expected_progress[counter]))
            except Exception as e:
                self.log.info(e)
                self.thread_fail = True

        def data_load(doc_loading_spec_name=None, async_load=False):
            if not doc_loading_spec_name:
                doc_loading_spec_name = "initial_load"
            doc_loading_spec = self.bucket_util.get_crud_template_from_package(
                doc_loading_spec_name)
            task = self.bucket_util.run_scenario_from_spec(self.task,
                                                           self.cluster,
                                                           self.cluster.buckets,
                                                           doc_loading_spec,
                                                           mutation_num=0,
                                                           async_load=True)
            if async_load:
                return task
            self.task.jython_task_manager.get_task_result(task)
            self.bucket_util.validate_doc_loading_results(self.cluster,
                                                          task)
            if task.result is False:
                raise Exception("doc load/verification failed")
        second_rebalance = self.input.param("second_rebalance", True)
        fail_case = self.input.param("fail_case", "induce_error")
        weight_change = self.input.param("weight_add", 100)
        nodes_in = self.cluster.servers[
                   self.nodes_init:self.nodes_init + self.nodes_in]
        num_bucket_update = self.input.param("num_bucket_update",
                                             len(self.cluster.buckets))
        num_target_nodes = self.input.param("num_target_nodes", 2)
        self.thread_fail = self.input.param("thread_fail", False)
        scale_cluster = self.input.param("scale_cluster", False)
        scale_cluster_nodes_in = self.input.param("scale_cluster_nodes_in", 3)
        sim_error = self.input.param("sim_error", None)
        fail_start_server = self.input.param("fail_start_server", 0)
        fail_interval = self.input.param("fail_interval", 20)
        expect_data_fail = self.input.param("expect_data_fail", False)
        fail_timeout = self.input.param("fail_timeout", 150)
        expect_scale_fail = self.input.param("expect_scale_fail", False)
        wait_before_fail = self.input.param("wait_before_fail", 20)
        rest = RestConnection(self.cluster.master)
        target_buckets = self.cluster.buckets[:num_bucket_update]
        status = rest.update_autofailover_settings(True, 300)
        self.assertTrue(status, "Auto-failover enable failed")
        self.create_sdk_clients()
        data_load()
        data_load_task = data_load(
            doc_loading_spec_name="volume_test_load_with_CRUD_on_collections",
            async_load=True)
        # adding a required sub-cluster before scaling to simulate control
        # plane like action
        if nodes_in:
            add_to_nodes = dict()
            for zone in rest.get_zone_names():
                add_to_nodes[zone] = 1
            rebalance_task = self.task.async_rebalance(self.cluster,
                                                       nodes_in, [],
                                                       add_nodes_server_groups=
                                                       add_to_nodes,
                                                       retry_get_process_num=
                                                       3000)
            self.task_manager.get_task_result(rebalance_task)
            self.assertTrue(rebalance_task.result, "Re-balance failed")
            self.nodes_init += self.nodes_in

            # bucket scaling
        failure_thread = Thread(target=rebalance_failure,
                                args=(fail_case, target_buckets, sim_error,
                                      num_target_nodes, fail_interval,
                                      fail_timeout, wait_before_fail))
        failure_thread.start()
        for bucket in target_buckets:
            try:
                self.bucket_util.update_bucket_property(self.cluster.master,
                                                        bucket,
                                                        bucket_width=
                                                        self.desired_width,
                                                        ram_quota_mb=
                                                        self.desired_ram)
                if self.desired_width:
                    bucket.serverless.width = self.desired_width
            except Exception as e:
                self.log.info(e)
                self.assertTrue(expect_scale_fail,
                                "Didn't expect bucket scaling to fail for "
                                "this case")

        if self.desired_weight:
            for bucket in target_buckets:
                while bucket.serverless.weight < self.desired_weight:
                    if bucket.serverless.weight + weight_change > \
                            self.desired_weight:
                        next_weight = self.desired_weight
                    else:
                        next_weight = bucket.serverless.weight + weight_change
                    self.bucket_util.update_bucket_property(
                        self.cluster.master,
                        bucket,
                        bucket_weight=
                        next_weight)
                    bucket.serverless.weight = next_weight
                while bucket.serverless.weight > self.desired_weight:
                    if bucket.serverless.weight - weight_change < \
                            self.desired_weight:
                        next_weight = self.desired_weight
                    else:
                        next_weight = bucket.serverless.weight - weight_change
                    self.bucket_util.update_bucket_property(
                        self.cluster.master,
                        bucket,
                        bucket_weight=
                        next_weight)
                    bucket.serverless.weight = next_weight

        # scaling re-balance
        nodes_in = []
        nodes_out = []
        zone_group = None
        if scale_cluster:
            nodes_in = self.cluster.servers[
                       self.nodes_init:self.nodes_init + scale_cluster_nodes_in]
            nodes_out = self.cluster.servers[self.nodes_init -
                                             self.nodes_out:self.nodes_init]
            if len(nodes_in) > 0:
                zone_group = dict()
                for zone in rest.get_zone_names():
                    zone_group[zone] = 1
        rebalance_task = self.task.async_rebalance(self.cluster, nodes_in,
                                                   nodes_out,
                                                   add_nodes_server_groups=zone_group,
                                                   retry_get_process_num=3000)

        self.task_manager.get_task_result(rebalance_task)
        failure_thread.join()
        self.assertFalse(self.thread_fail, "Rebalance thread failed")
        if second_rebalance:
            rebalance_task = self.task.async_rebalance(self.cluster, [], [],
                                                       retry_get_process_num=3000)
            self.task_manager.get_task_result(rebalance_task)
            self.assertTrue(rebalance_task.result, "Rebalance failed")

        # assertions
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets,
                                                     timeout=1200)
        self.task.jython_task_manager.get_task_result(data_load_task)
        self.bucket_util.validate_doc_loading_results(self.cluster,
                                                      data_load_task)
        if expect_data_fail is False and data_load_task.result is False:
            self.log_failure("Doc CRUDs failed")
        self.assertTrue(rest.is_cluster_balanced(), "Cluster not balanced!")
        validation = self.bucket_util.validate_serverless_buckets(
            self.cluster, self.cluster.buckets)
        self.assertTrue(validation, "Bucket validation failed")

    def test_one_server_group_bucket(self):
        rest = RestConnection(self.cluster.master)
        bucket_params = self.__get_bucket_params(b_name="bucket_1", width=1,
                                                 weight=30, replica=0)
        bucket_obj = Bucket(bucket_params)
        self.bucket_util.create_bucket(self.cluster, bucket_obj,
                                       wait_for_warmup=True)
        self.assertTrue(rest.is_cluster_balanced(), "Cluster unbalanced")
        self.create_sdk_clients()
        data_spec = self.data_load_spec()
        self.bucket_util.run_scenario_from_spec(self.task, self.cluster,
                                                self.cluster.buckets,
                                                data_spec)

    def test_two_server_group_bucket(self):
        bucket_params = self.__get_bucket_params(b_name="bucket_1", width=1,
                                                 weight=30, replica=1)
        rest = RestConnection(self.cluster.master)
        bucket_obj = Bucket(bucket_params)
        self.bucket_util.create_bucket(self.cluster, bucket_obj,
                                       wait_for_warmup=True)
        self.assertTrue(rest.is_cluster_balanced(), "Cluster unbalanced")
        self.create_sdk_clients()
        data_spec = self.data_load_spec()
        self.bucket_util.run_scenario_from_spec(self.task, self.cluster,
                                                self.cluster.buckets,
                                                data_spec)

    def test_recreate_bucket(self):
        """test recreate buckets steps
        1. creating initial buckets for continuous data load
            * with these buckets checking is same clusters are allocated
            when buckets recreated
        2. creating rest buckets till the cluster limit
        3. deleting and recreating all buckets accompanied with
            * swap re-balance of nodes
            * data load in some buckets
        """

        sleep_before_recreate = self.input.param("sleep_before_recreate", 10)
        init_buckets = self.input.param("init_bucket", 3)
        replace_nodes = self.input.param("replace_nodes", True)
        init_weight = self.input.param("init_weight", 30)
        replace_nodes_num = self.input.param("replace_nodes_num", 2)
        rest = RestConnection(self.cluster.master)
        helper = BucketHelper(self.cluster.master)
        expectedError = "Need more space in availability zones"
        buckets = []
        for counter in range(init_buckets):
            name = "init_bucket" + str(counter)
            bucket_params = self.__get_bucket_params(b_name=name,
                                                     width=1,
                                                     weight=30,
                                                     replica=1)
            bucket_obj = Bucket(bucket_params)
            self.bucket_util.create_bucket(self.cluster, bucket_obj,
                                           wait_for_warmup=True)

        def create_bucket_map():
            server_bucket_map = dict()
            for bucket in self.cluster.buckets:
                for server in bucket.servers:
                    if server not in server_bucket_map:
                        server_bucket_map[server] = [bucket]
                    else:
                        server_bucket_map[server].append(bucket)
            return server_bucket_map

        def check_cluster_allocation(bucket):
            server_bucket_map = create_bucket_map()
            self.bucket_util.delete_bucket(self.cluster, bucket)
            self.bucket_util.create_bucket(self.cluster, bucket,
                                           wait_for_warmup=True)
            post_server_bucket_map = create_bucket_map()
            for key in server_bucket_map.keys():
                self.assertTrue(post_server_bucket_map[key].sort() ==
                                server_bucket_map[key].sort(),
                                "Recreated bucket deployed in different nodes")

        check_cluster_allocation(self.cluster.buckets[0])
        self.create_sdk_clients()

        def swap_rebalance():
            zone_map = dict()
            for zone in rest.get_zone_names():
                zone_map[zone] = rest.get_nodes_in_zone(zone)
            key_list = list(zone_map.keys())
            add_to_nodes = dict()
            node_to_replace = []

            # selecting zone-wise nodes to replace in var node_to_replace
            for i in range(replace_nodes_num):
                if key_list[i % len(key_list)] not in add_to_nodes:
                    add_to_nodes[key_list[i % len(key_list)]] = 1
                else:
                    add_to_nodes[key_list[i % len(key_list)]] += 1
                for node in self.cluster.servers:
                    if self.cluster.master.ip != node.ip and node.ip in \
                            zone_map[key_list[i % len(key_list)]]:
                        node_to_replace.append(node)
                        break
            nodes_in = self.cluster.servers[self.nodes_init:
                                            self.nodes_init + replace_nodes_num]

            rebalance_task = self.task.async_rebalance(self.cluster,
                                                       nodes_in,
                                                       node_to_replace,
                                                       retry_get_process_num=3000,
                                                       add_nodes_server_groups=
                                                       add_to_nodes,
                                                       check_vbucket_shuffling=False)
            self.task_manager.get_task_result(rebalance_task)
            self.assertTrue(rebalance_task.result, "Re-balance Failed")

        # hitting API to create bucket in order to validate from response
        def create_bucket(bucket_params):
            api = helper.baseUrl + self.b_create_endpoint
            params = urllib.urlencode(bucket_params)
            status, cont, _ = helper._http_request(api, helper.POST, params)
            if not status:
                return json.loads(cont)
            return True

        iterator = 0
        while iterator < 100:
            iterator += 1
            b_name = "bucket_1" + str(iterator)
            bucket_params = self.__get_bucket_params(b_name=b_name, width=1,
                                                     weight=init_weight)
            cont = create_bucket(bucket_params)
            if not isinstance(cont, bool):
                self.assertTrue(expectedError in cont["_"],
                                "Invalid error message for bucket::Number")
                break
            buckets.append(b_name)
        else:
            self.fail("not expected to create 100 buckets")

        bucket_length = len(buckets)
        # checking bucket recreate
        data_spec = self.data_load_spec()
        data_spec[MetaCrudParams.COLLECTIONS_TO_ADD_FOR_NEW_SCOPES] = 0
        for iter in range(bucket_length):
            async_load_task = self.bucket_util.run_scenario_from_spec(
                self.task, self.cluster, self.cluster.buckets, data_spec,
                async_load=True)
            # deleting bucket here
            helper.delete_bucket(buckets[iter])
            self.sleep(sleep_before_recreate, "waiting after bucket deleted")
            if replace_nodes:
                swap_rebalance()
                replace_nodes = False
            # recreating bucket
            cont = create_bucket(self.__get_bucket_params(
                b_name=buckets[iter], width=1, weight=30))
            self.assertTrue(isinstance(cont, bool), "bucket not recreated")

            # checking bucket creation post limit reached
            cont = create_bucket(self.__get_bucket_params(
                b_name=buckets[iter], width=1, weight=init_weight))
            self.assertFalse(isinstance(cont, bool),
                             "bucket not expected to be created")
            self.task_manager.get_task_result(async_load_task)
            self.bucket_util.validate_doc_loading_results(self.cluster,
                                                          async_load_task)
            if async_load_task.result is False:
                self.log_failure("Doc CRUDs failed")

        self.assertTrue(rest.is_cluster_balanced(), "Cluster unbalanced")

    def tenant_bucket_limit_test(self):
        rest = RestConnection(self.cluster.master)
        rest.set_serverless_bucket_limit(8)
        nodes_in = self.cluster.servers[
                   self.nodes_init:self.nodes_init + 3]
        # creating bucket within limit
        for i in range(8):
            b_name = "test_bucket" + str(i)
            bucket_params = self.__get_bucket_params(b_name=b_name, width=1,
                                                     weight=30)
            bucket_obj = Bucket(bucket_params)
            self.bucket_util.create_bucket(self.cluster, bucket_obj,
                                           wait_for_warmup=True)

        # trying exceeding limit
        b_name = "exceed_limit"
        bucket_params = self.__get_bucket_params(b_name=b_name, width=1,
                                                 weight=30)
        bucket_obj = Bucket(bucket_params)
        try:
            self.bucket_util.create_bucket(self.cluster, bucket_obj,
                                           wait_for_warmup=True)
        except Exception as e:
            self.log.info(e)
        else:
            self.fail("expected bucket creation to fail")

        # reducing limit to less than current value
        rest.set_serverless_bucket_limit(3)
        zone_to_add = dict()
        for zone in rest.get_zone_names():
            zone_to_add[zone] = 1
        try:
            rebalance_task = self.task.async_rebalance(self.cluster,
                                                       nodes_in,
                                                       retry_get_process_num=3000,
                                                       add_nodes_server_groups=zone_to_add)
            self.task_manager.get_task_result(rebalance_task)
            self.assertTrue(rebalance_task.result, "Re-balance Failed")
        except Exception as e:
            self.log.info(e)
        else:
            self.fail("expected re-balance to fail")

        for bucket in self.cluster.buckets:
            self.bucket_util._wait_warmup_completed(bucket, wait_time=20)
        rest.set_serverless_bucket_limit(8)
        rebalance_task = self.task.async_rebalance(self.cluster,
                                                   [], [],
                                                   retry_get_process_num=3000)
        self.task_manager.get_task_result(rebalance_task)
        self.assertTrue(rebalance_task.result, "Re-balance Failed")
        self.assertFalse(rest.set_serverless_bucket_limit(0),
                         "Able to set tenant limit as 0")
        self.assertFalse(rest.set_serverless_bucket_limit(-1),
                         "Able to set a negative tenant limit")
        self.assertTrue(rest.is_cluster_balanced(), "Cluster unbalanced")
