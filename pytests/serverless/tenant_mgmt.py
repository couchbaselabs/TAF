import json
import urllib
from random import choice

from BucketLib.BucketOperations import BucketHelper
from BucketLib.bucket import Bucket
from Cb_constants import CbServer
from membase.api.rest_client import RestConnection
from serverless.serverless_onprem_basetest import ServerlessOnPremBaseTest


class TenantManagementOnPrem(ServerlessOnPremBaseTest):
    def setUp(self):
        super(TenantManagementOnPrem, self).setUp()
        self.b_create_endpoint = "pools/default/buckets"

        with_default_bucket = self.input.param("with_default_bucket", False)
        if with_default_bucket:
            bucket_params = self.__get_bucket_params(
                b_name="default",
                width=self.bucket_width)
            params = urllib.urlencode(bucket_params)
            helper = BucketHelper(self.cluster.master)
            api = helper.baseUrl + self.b_create_endpoint
            status, _, _ = helper._http_request(api, helper.POST, params)
            self.assertTrue(status, "Bucket creation failed")

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
            bucket_params[Bucket.num_vbuckets] = vb_num
            content = create_bucket()["errors"]
            self.assertEqual(content["numVbuckets"], err_vb,
                             "Invalid error message for bucket::numVbuckets")

        bucket_params.pop(Bucket.num_vbuckets)
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
        random_vb_num = self.input.param("random_vb_num", False)

        # Create max_possible buckets for the given sub_cluster
        rest = RestConnection(self.cluster.master)
        cluster_stats = rest.cluster_status()
        num_buckets = int(cluster_stats["memoryQuota"] / self.bucket_size)
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
        error = json.loads(content)["errors"]["ramQuota"]
        self.assertEqual(error, "RAM quota specified is too large to be "
                                "provisioned into this cluster.",
                         "Mismatch in the error message")

        self.log.info("Adding KV sub_cluster")
        nodes_to_add = self.cluster.servers[
            self.nodes_init+CbServer.Serverless.KV_SubCluster_Size]
        self.task.rebalance(self.cluster, to_add=nodes_to_add, to_remove=[],
                            add_nodes_server_groups=self.kv_distribution_dict)

        self.log.info("Sub_cluster #2 - Creating %d buckets with ram=%d"
                      % (num_buckets, self.bucket_size))
        for index in range(num_buckets):
            if random_vb_num:
                self.vbuckets = choice(vb_nums)
            name = "bucket_%d" % index
            self.bucket_weight = choice(bucket_weights)
            self.create_bucket(self.cluster, bucket_name=name)

        self.log.info("Attempting to create an extra bucket")
        status, content, _ = helper._http_request(api, helper.POST, params)
        self.assertFalse(status, "Extra bucket created successfully")
        error = json.loads(content)["errors"]["ramQuota"]
        self.assertEqual(error, "RAM quota specified is too large to be "
                                "provisioned into this cluster.",
                         "Mismatch in the error message")
