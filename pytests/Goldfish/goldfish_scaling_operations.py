from Goldfish.goldfish_base import GoldFishBaseTest


class GoldfishScalingOperations(GoldFishBaseTest):

    def setUp(self):
        super(GoldfishScalingOperations, self).setUp()
        self.cluster = self.list_all_clusters()[0]

    def tearDown(self):
        super(GoldfishScalingOperations, self).tearDown()

    def test_scale_in_and_scale_out_goldfish_instances(self):
        # increase compute units from 2 to 5
        self.goldfish_utils.scale_goldfish_cluster(self.pod, self.users[0], self.cluster, 5)
        self.goldfish_utils.wait_for_cluster_scaling_operation_to_complete(
            self.pod, self.users[0], self.cluster)
        # decrease compute units from 5 to 2
        self.goldfish_utils.scale_goldfish_cluster(self.pod, self.users[0], self.cluster, 2)
        self.goldfish_utils.wait_for_cluster_scaling_operation_to_complete(
            self.pod, self.users[0], self.cluster)
