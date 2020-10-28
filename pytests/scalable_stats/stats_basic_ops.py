from StatsLib.StatsOperations import StatsHelper
from bucket_collections.collections_base import CollectionBase
from rbac_utils.Rbac_ready_functions import RbacUtils


class StatsBasicOps(CollectionBase):
    def setUp(self):
        super(StatsBasicOps, self).setUp()

    def tearDown(self):
        super(StatsBasicOps, self).tearDown()

    def test_check_low_cardinality_metrics(self):
        """
        Check if _prometheusMetrics returns low cardinality metrics by default
        ie; Low cardinality metrics are collected by default
        Also serves as a check if prometheus is running on all nodes
        """
        for server in self.cluster.servers[:self.nodes_init]:
            try:
                map = StatsHelper(server).get_prometheus_metrics()
                number_of_metrics = len(map)
                if len(map) == 0:
                    self.warn("No metrics were returned")
                self.log.info("Number of metrics returned on {0}: {1}".format(server.ip, number_of_metrics))
            except Exception as e:
                self.fail("Exception in getting _prometheusMetrics: {0}".format(e))

    def test_check_authorization_prometheus_metrics(self):
        """
        Check prometheus metrics endpoint is accessible only by cluster.admin.internal.stats!read
        Check with cluster admin role - it should fail, and then try it with Full admin - it should pass
        """
        # ToDo - check with other endpoints as well
        rbac_util = RbacUtils(self.cluster.master)
        self.log.info("Create a user with role cluster admin")
        rbac_util._create_user_and_grant_role("cluster_admin", "cluster_admin")
        stats_helper_object = StatsHelper(self.cluster.master)
        try:
            stats_helper_object.username = "cluster_admin"
            _ = stats_helper_object.get_prometheus_metrics()
            self.fail("Metrics was accessible without necessary permissions")
        except Exception as e:
            self.log.info("Accessing metrics with cluster admin failed as expected {0}".format(e))
        stats_helper_object.username = "Administrator"
        map = stats_helper_object.get_prometheus_metrics()
        number_of_metrics = len(map)
        self.log.info("Got metrics with user Full admin. Number of metrics: {0}".format(number_of_metrics))

    def test_range_api_metrics(self):
        """
        Example to retrieve range_api_metrics
        """
        # Example 1
        metric_name = "kv_curr_items"
        label_values = {"bucket": self.bucket_util.buckets[0].name, "nodes": self.cluster.master.ip}
        content = StatsHelper(self.cluster.master).get_range_api_metrics(metric_name, label_values=label_values)
        print(content)

        # Example 2
        metric_name = "kv_curr_items"
        label_values = {"bucket": self.bucket_util.buckets[0].name, "aggregationFunction":"max"}
        content = StatsHelper(self.cluster.master).get_range_api_metrics(metric_name, label_values=label_values)
        print(content)

    def test_instant_api_metrics(self):
        """
        API not exposed yet
        """
        pass
