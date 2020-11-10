from BucketLib.bucket import TravelSample, BeerSample
from StatsLib.StatsOperations import StatsHelper
from bucket_collections.collections_base import CollectionBase
from rbac_utils.Rbac_ready_functions import RbacUtils
from membase.api.rest_client import RestConnection

from platform_utils.remote.remote_util import RemoteMachineShellConnection


class StatsBasicOps(CollectionBase):
    def setUp(self):
        super(StatsBasicOps, self).setUp()
        self.rest = RestConnection(self.cluster.master)

    def tearDown(self):
        super(StatsBasicOps, self).tearDown()

    def get_services_from_node(self, server):
        """
        Helper function to return dict of key- ip:port and value- list of services of a node
        """
        services_map = self.rest.get_nodes_services()
        key = "%s:%s" % (str(server.ip), str(server.port))
        server_services = services_map[key]
        return server_services

    def test_check_low_cardinality_metrics(self):
        """
        Check if _prometheusMetrics returns low cardinality metrics by default
        ie; Low cardinality metrics are collected by default
        Also serves as a check if prometheus is running on all nodes
        """
        component = self.input.param("component", "ns_server")
        parse = self.input.param("parse", False)

        self.bucket_util.load_sample_bucket(TravelSample())
        self.bucket_util.load_sample_bucket(BeerSample())
        for server in self.cluster.servers[:self.nodes_init]:
            content = StatsHelper(server).get_prometheus_metrics(component=component, parse=parse)
            if not parse:
                StatsHelper(server)._validate_metrics(content)
        for line in content:
            print(line.strip("\n"))

    def test_check_high_cardinality_metrics(self):
        """
        Check if _prometheusMetrics returns high cardinality metrics by default
        ie; High cardinality metrics are collected by default
        Also serves as a check if prometheus is running on all nodes
        """
        component = self.input.param("component", "kv")
        parse = self.input.param("parse", False)

        self.bucket_util.load_sample_bucket(TravelSample())
        self.bucket_util.load_sample_bucket(BeerSample())
        for server in self.cluster.servers[:self.nodes_init]:
            content = StatsHelper(server).get_prometheus_metrics_high(component=component, parse=parse)
            if not parse:
                StatsHelper(server)._validate_metrics(content)
        for line in content:
            print(line.strip("\n"))

    def test_check_authorization_low_cardinality_metrics(self):
        """
        Check low cardinality prometheus metrics endpoint is accessible only by cluster.admin.internal.stats!read
        Check with cluster admin role - it should fail, and then try it with Full admin - it should pass
        """
        rbac_util = RbacUtils(self.cluster.master)
        self.log.info("Create a user with role cluster admin")
        rbac_util._create_user_and_grant_role("cluster_admin", "cluster_admin")
        for server in self.cluster.servers[:self.nodes_init]:
            stats_helper_object = StatsHelper(server)
            try:
                stats_helper_object.username = "cluster_admin"
                _ = stats_helper_object.get_prometheus_metrics()
                self.fail("Metrics was accessible without necessary permissions")
            except Exception as e:
                self.log.info("Accessing metrics with cluster admin failed as expected {0}".format(e))
            self.log.info("trying again with Administrator privilages")
            stats_helper_object.username = "Administrator"
            map = stats_helper_object.get_prometheus_metrics()
            number_of_metrics = len(map)
            self.log.info("Got metrics with user Full admin. Number of metrics: {0}".format(number_of_metrics))

    def test_check_authorization_high_cardinality_metrics(self):
        """
        Check high cardinality prometheus metrics endpoint is accessible only by cluster.admin.internal.stats!read
        Check with cluster admin role - it should fail, and then try it with Full admin - it should pass
        """
        rbac_util = RbacUtils(self.cluster.master)
        self.log.info("Create a user with role cluster admin")
        rbac_util._create_user_and_grant_role("cluster_admin", "cluster_admin")
        for server in self.cluster.servers[:self.nodes_init]:
            server_services = self.get_services_from_node(server)
            stats_helper_object = StatsHelper(server)
            for component in server_services:
                try:
                    stats_helper_object.username = "cluster_admin"
                    _ = stats_helper_object.get_prometheus_metrics_high(component=component, parse=False)
                    self.fail("Metrics was accessible without necessary permissions on {0} for component {1}"
                              .format(server.ip, component))
                except Exception as e:
                    self.log.info("Accessing metrics with cluster admin failed as expected {0}".format(e))
                self.log.info("trying again with Administrator privilages")
                stats_helper_object.username = "Administrator"
                content = stats_helper_object.get_prometheus_metrics_high(component=component, parse=False)
                StatsHelper(server)._validate_metrics(content)

    def test_check_get_all_metrics(self):
        """
        Test /metrics endpoint. Validate for duplicity and prefix
        """
        self.bucket_util.load_sample_bucket(TravelSample())
        self.bucket_util.load_sample_bucket(BeerSample())
        for server in self.cluster.servers[:self.nodes_init]:
            content = StatsHelper(server).get_all_metrics()
            StatsHelper(server)._validate_metrics(content)
        for line in content:
            print(line.strip("\n"))

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
        label_values = {"bucket": self.bucket_util.buckets[0].name, "aggregationFunction": "max"}
        content = StatsHelper(self.cluster.master).get_range_api_metrics(metric_name, label_values=label_values)
        print(content)

    def test_execute_promql_query(self):
        """
        Does not seem to get it to work yet
        """
        query = "kv_curr_items"
        content = StatsHelper(self.cluster.master).execute_promql_query(query)
        print(content)

    def test_instant_api_metrics(self):
        """
        API not exposed yet
        """
        pass

    def test_configure_stats_settings_from_diag_eval(self):
        """
        Example to change the stats settings.
        """
        shell = RemoteMachineShellConnection(self.cluster.master)
        shell.enable_diag_eval_on_non_local_hosts()
        # Example 1 to change scrape interval
        StatsHelper(self.cluster.master).configure_stats_settings_from_diag_eval("scrape_interval", 30)

        # Example 2 to disable high cardinality
        value = "[{kv,[{high_cardinality_enabled,false}]}, {index,[{high_cardinality_enabled,false}]}]"
        StatsHelper(self.cluster.master).configure_stats_settings_from_diag_eval("services", value)

        # ToDo: Is there a way to reset them all to defaults at once, before running the next test?

    def test_stats_1000_collections(self):
        """
        Call all endpoints, for all components, and validate with 1000 collections in the cluster
        """
        for server in self.cluster.servers[:self.nodes_init]:
            self.log.info("calling low cardinality metrics on {0} with component ns server".format(server.ip))
            content = StatsHelper(server).get_prometheus_metrics(component="ns_server", parse=False)
            StatsHelper(server)._validate_metrics(content)

            self.log.info("calling /metrics on {0}".format(server.ip))
            content = StatsHelper(server).get_all_metrics()
            StatsHelper(server)._validate_metrics(content)

            server_services = self.get_services_from_node(server)
            for component in server_services:
                self.log.info("calling low cardinality metrics on {0} with component {1}".format(server.ip, component))
                content = StatsHelper(server).get_prometheus_metrics(component=component, parse=False)
                StatsHelper(server)._validate_metrics(content)
                self.log.info("calling high cardinality metrics on {0} with component {1}".format(server.ip, component))
                content = StatsHelper(server).get_prometheus_metrics_high(component=component, parse=False)
                StatsHelper(server)._validate_metrics(content)




