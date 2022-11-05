import json
import urllib
from random import choice, randint, sample

from BucketLib.BucketOperations import BucketHelper
from BucketLib.bucket import Bucket
from Cb_constants import CbServer
from couchbase_helper.documentgenerator import doc_generator
from membase.api.rest_client import RestConnection
from serverless.serverless_onprem_basetest import ServerlessOnPremBaseTest
from error_simulation.cb_error import CouchbaseError
from remote.remote_util import RemoteMachineShellConnection
from pytests.bucket_collections.collections_base import CollectionBase


class TenantManagementOnPrem(ServerlessOnPremBaseTest):
    def setUp(self):
        super(TenantManagementOnPrem, self).setUp()
        self.b_create_endpoint = "pools/default/buckets"

        self.spec_name = self.input.param("bucket_spec", None)
        self.error_sim = None
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

    def __get_bucket_params(self, b_name, ram_quota=256, width=1, weight=1):
        self.log.debug("Creating bucket param")
        return {
            Bucket.name: b_name,
            Bucket.replicaNumber: Bucket.ReplicaNum.TWO,
            Bucket.ramQuotaMB: ram_quota,
            Bucket.storageBackend: Bucket.StorageBackend.magma,
            Bucket.width: width,
            Bucket.weight: weight
        }

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
                    b_name=name, width=self.bucket_width)
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
            self.bucket_util.validate_doc_loading_results(load_task)
            if load_task.result is False:
                raise Exception("doc load/verification failed")

        self.validate_stat = self.input.param("validate_stat", False)
        if self.negative_case:
            self.desired_width = (len(self.cluster.servers) /
                                  CbServer.Serverless.KV_SubCluster_Size) + 1

        scale = self.input.param("bucket_scale", "all")
        enable_data_load = self.input.param("data_loading", True)
        data_load_after_rebalance = self.input.param(
            "data_load_after_rebalance", True)
        update_during_rebalance = self.input.param(
            "update_during_rebalance", False)
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
                self.task_manager.number_of_threads,
                self.cluster.master,
                buckets,
                self.sdk_client_pool,
                self.sdk_compression)
            if async_load:
                task = data_load()
        if update_during_rebalance:
            rebalance_task_during_update = self.task.async_rebalance(
                self.cluster, nodes_in, nodes_out,
                retry_get_process_num=3000)
            self.sleep(10, "Wait for rebalance to make progress")

        if self.validate_stat:
            self.expected_stat = self.bucket_util.get_initial_stats(
                self.cluster.buckets)
        for bucket in buckets_to_consider:
            try:
                self.bucket_util.update_bucket_property(
                    self.cluster.master, bucket,
                    bucket_width=self.desired_width,
                    bucket_weight=self.desired_weight)
                if self.desired_width:
                    bucket.serverless.width = self.desired_width
                if self.desired_weight:
                    bucket.serverless.weight = self.desired_weight
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
        self.sdk_client_pool.create_clients(
            bucket=bucket, servers=bucket.servers, req_clients=1)

        doc_gen = doc_generator(self.key, 0, self.num_items)
        loading_task = self.task.async_continuous_doc_ops(
            self.cluster, bucket, doc_gen,
            sdk_client_pool=self.sdk_client_pool)
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
        def create_sdk_clients():
            CollectionBase.create_sdk_clients(
                self.task_manager.number_of_threads,
                self.cluster.master,
                self.cluster.buckets,
                self.sdk_client_pool,
                self.sdk_compression)

        def rebalance_failure(rebal_failure, target_buckets=None,
                              error_type=None):
            if rebal_failure == "delete_bucket":
                bucket_conn = BucketHelper(self.cluster.master)
                for bucket_obj in target_buckets:
                    status = bucket_conn.delete_bucket(bucket_obj.name)
                    self.assertFalse(status, "Bucket wasn't expected to get "
                                             "deleted while re-balance running")

            elif rebal_failure == "induce_error":
                shell = RemoteMachineShellConnection(self.cluster.servers[1])
                self.error_sim = CouchbaseError(self.log, shell)
                self.error_sim.create(error_type)
            elif rebal_failure == "stop_rebalance":
                counter = 0
                expected_progress = [20, 40, 80]
                for progress in expected_progress:
                    if rest.is_cluster_balanced():
                        break
                    counter += 1
                    reached = self.cluster_util.rebalance_reached(rest,
                                                                  progress)
                    self.assertTrue(reached,
                                    "Rebalance failed or did not reach "
                                    "{0}%".format(progress))
                    stopped = rest.stop_rebalance(wait_timeout=30)
                    self.assertTrue(stopped, msg="Unable to stop rebalance")
                    self.bucket_util._wait_for_stats_all_buckets(
                        self.cluster, self.cluster.buckets, timeout=1200)
                    self.task.async_rebalance(self.cluster, [], [],
                                              retry_get_process_num=3000)
                    self.sleep(5, "Waiting before next iteration")
                if counter < len(expected_progress):
                    self.log.info("Cluster balanced before stopping for "
                                  "progress{0}".format(expected_progress[counter]))

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
            self.bucket_util.validate_doc_loading_results(task)
            if task.result is False:
                raise Exception("doc load/verification failed")
        second_rebalance = self.input.param("second_rebalance", True)
        fail_case = self.input.param("fail_case", "induce_error")
        nodes_in = self.cluster.servers[
                   self.nodes_init:self.nodes_init + self.nodes_in]
        num_bucket_update = self.input.param("num_bucket_update",
                                             len(self.cluster.buckets))
        sim_error = self.input.param("sim_error", None)
        rest = RestConnection(self.cluster.master)
        target_buckets = self.cluster.buckets[:num_bucket_update]
        create_sdk_clients()
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

        # bucket scaling
        for bucket in target_buckets:
            self.bucket_util.update_bucket_property(self.cluster.master, bucket,
                                                    bucket_width=
                                                    self.desired_width,
                                                    bucket_weight=
                                                    self.desired_weight)
            if self.desired_width:
                bucket.serverless.width = self.desired_width
            if self.desired_weight:
                bucket.serverless.weight = self.desired_weight
        # scaling re-balance
        rebalance_task = self.task.async_rebalance(self.cluster, [], [],
                                                   retry_get_process_num=3000)
        self.sleep(10, "Wait for Rebalance to start")
        rebalance_failure(fail_case, target_buckets, sim_error)
        self.task_manager.get_task_result(rebalance_task)
        # induced error removal
        if self.error_sim:
            self.sleep(60, "Waiting before resolving induced error")
            self.error_sim.revert(sim_error)
        if second_rebalance:
            self.sleep(60, "Waiting for error resolved before re-balance")
            rebalance_task = self.task.async_rebalance(self.cluster, [], [],
                                                       retry_get_process_num=3000)
            self.task_manager.get_task_result(rebalance_task)
            self.assertTrue(rebalance_task.result, "Rebalance failed")

        # assertions
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets,
                                                     timeout=1200)
        self.task.jython_task_manager.get_task_result(data_load_task)
        self.bucket_util.validate_doc_loading_results(data_load_task)
        self.assertTrue(rest.is_cluster_balanced(), "Cluster not balanced!")
        validation = self.bucket_util.validate_serverless_buckets(
            self.cluster, self.cluster.buckets)
        self.assertTrue(validation, "Bucket validation failed")
