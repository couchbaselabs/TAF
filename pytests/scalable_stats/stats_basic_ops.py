from BucketLib.bucket import TravelSample, BeerSample
from StatsLib.StatsOperations import StatsHelper
from bucket_collections.collections_base import CollectionBase
from rbac_utils.Rbac_ready_functions import RbacUtils
from membase.api.rest_client import RestConnection

from ruamel.yaml import YAML # using ruamel.yaml as "import yaml" (pyyaml) doesn't work with jython


class StatsBasicOps(CollectionBase):
    def setUp(self):
        super(StatsBasicOps, self).setUp()
        self.rest = RestConnection(self.cluster.master)

    def tearDown(self):
        self.log.info("Reverting settings to default")
        StatsHelper(self.cluster.master).reset_stats_settings_from_diag_eval()
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

    def test_instant_api_metrics(self):
        """
        API not exposed yet
        """
        pass

    def test_disable_high_cardinality_metrics(self):
        """
        Disable Prometheus from scraping high cardinality metrics
        Validate by querying Prometheus directly for its active targets
        """
        self.bucket_util.load_sample_bucket(TravelSample())
        self.bucket_util.load_sample_bucket(BeerSample())

        self.log.info("Disabling high cardinality metrics of all services")
        value = "[{index,[{high_cardinality_enabled,false}]}, {fts,[{high_cardinality_enabled,false}]},\
                 {kv,[{high_cardinality_enabled,false}]}, {cbas,[{high_cardinality_enabled,false}]}, \
                 {eventing,[{high_cardinality_enabled,false}]}]"
        StatsHelper(self.cluster.master).configure_stats_settings_from_diag_eval("services", value)

        self.log.info("Validating by querying prometheus")
        StatsHelper(self.cluster.master).configure_stats_settings_from_diag_eval("prometheus_auth_enabled", "false")
        StatsHelper(self.cluster.master).configure_stats_settings_from_diag_eval("listen_addr_type", "any")
        self.sleep(10, "Waiting for prometheus federation")
        query = "targets?state=active"
        for server in self.cluster.servers[:self.nodes_init]:
            content = StatsHelper(server).query_prometheus_federation(query)
            active_targets = content["data"]["activeTargets"]
            if len(active_targets) == 0:
                self.fail("Prometheus did not return any active targets")
            for active_targets_dict in active_targets:
                job = active_targets_dict["labels"]["job"]
                self.log.info("Job name {0}".format(job))
                if "high_cardinality" in job:
                    self.fail("Prometheus is still scraping target with job name {0} on {1}".format(job, server.ip))

    def test_disable_external_prometheus_high_cardinality_metrics(self):
        """
        Disable exposition of high cardinality metrics by ns-server's /metrics endpoint
        Validate by checking that there are no high cardinality metrics returned at
        /metrics endpoint ie; check if
        total number low cardinality metrics = total number of metrics at /metrics endpoint
        """
        self.bucket_util.load_sample_bucket(TravelSample())
        self.bucket_util.load_sample_bucket(BeerSample())

        self.log.info("Disabling external prometheus high cardinality metrics of all services")
        value = "[{index,[{high_cardinality_enabled,false}]}, {fts,[{high_cardinality_enabled,false}]},\
                         {kv,[{high_cardinality_enabled,false}]}, {cbas,[{high_cardinality_enabled,false}]}, \
                         {eventing,[{high_cardinality_enabled,false}]}]"
        StatsHelper(self.cluster.master).configure_stats_settings_from_diag_eval("external_prometheus_services", value)
        for server in self.cluster.servers[:self.nodes_init]:
            len_low_cardinality_metrics = 0
            content = StatsHelper(server).get_prometheus_metrics(component="ns_server", parse=False)
            self.log.info("lc count of ns_server on {0} is {1}".format(server.ip, len(content)))
            len_low_cardinality_metrics = len_low_cardinality_metrics + len(content)
            server_services = self.get_services_from_node(server)
            for component in server_services:
                content = StatsHelper(server).get_prometheus_metrics(component=component, parse=False)
                self.log.info("lc count of {2} on {0} is {1}".format(server.ip, len(content), component))
                len_low_cardinality_metrics = len_low_cardinality_metrics + len(content)
            content = StatsHelper(server).get_all_metrics()
            len_metrics = len(content)
            if len_metrics != len_low_cardinality_metrics:
                self.fail("Number mismatch on node {0} , Total lc metrics count {1}, Total metrics count {2}".
                          format(server.ip, len_low_cardinality_metrics, len_metrics))

    def test_change_global_scrape_interval(self):
        """
        Change global scrape interval and verify the prometheus config by querying Prometheus Federation
        """
        scrape_interval = self.input.param("scrape_interval", 15)
        self.bucket_util.load_sample_bucket(TravelSample())
        self.bucket_util.load_sample_bucket(BeerSample())

        self.log.info("Changing scrape interval to {0}".format(scrape_interval))
        StatsHelper(self.cluster.master).configure_stats_settings_from_diag_eval("scrape_interval", scrape_interval)

        self.log.info("Validating by querying prometheus")
        StatsHelper(self.cluster.master).configure_stats_settings_from_diag_eval("prometheus_auth_enabled", "false")
        StatsHelper(self.cluster.master).configure_stats_settings_from_diag_eval("listen_addr_type", "any")
        self.sleep(10, "Waiting for prometheus federation")
        query = "status/config"
        yaml = YAML()
        for server in self.cluster.servers[:self.nodes_init]:
            content = StatsHelper(server).query_prometheus_federation(query)
            yaml_code = yaml.load(content["data"]["yaml"])
            global_scrape_interval = yaml_code["global"]["scrape_interval"]
            if str(global_scrape_interval) != (str(scrape_interval) + "s"):
                self.fail("Expected scrape interval {0}, but Actual {1}"
                          .format(scrape_interval, global_scrape_interval))

    def test_change_global_scrape_timeout(self):
        """
        Change global scrape timeout and verify the prometheus config by querying Prometheus Federation
        (Positive test case as a valid scrape_timeout is always less than scrape_interval)
        """
        scrape_timeout = self.input.param("scrape_timeout", 5)
        self.bucket_util.load_sample_bucket(TravelSample())
        self.bucket_util.load_sample_bucket(BeerSample())

        self.log.info("Changing scrape interval to {0}".format(scrape_timeout))
        StatsHelper(self.cluster.master).configure_stats_settings_from_diag_eval("scrape_timeout", scrape_timeout)

        self.log.info("Validating by querying prometheus")
        StatsHelper(self.cluster.master).configure_stats_settings_from_diag_eval("prometheus_auth_enabled", "false")
        StatsHelper(self.cluster.master).configure_stats_settings_from_diag_eval("listen_addr_type", "any")
        self.sleep(10, "Waiting for prometheus federation")
        query = "status/config"
        yaml = YAML()
        for server in self.cluster.servers[:self.nodes_init]:
            content = StatsHelper(server).query_prometheus_federation(query)
            yaml_code = yaml.load(content["data"]["yaml"])
            global_scrape_timeout = yaml_code["global"]["scrape_timeout"]
            if str(global_scrape_timeout) != (str(scrape_timeout) + "s"):
                self.fail("Expected scrape timeout {0}, but Actual {1}"
                          .format(scrape_timeout, global_scrape_timeout))

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
