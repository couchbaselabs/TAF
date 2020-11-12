from BucketLib.bucket import TravelSample, BeerSample
from StatsLib.StatsOperations import StatsHelper
from bucket_collections.collections_base import CollectionBase
from error_simulation.cb_error import CouchbaseError
from remote.remote_util import RemoteMachineShellConnection


class StatsFailureScenarios(CollectionBase):
    def setUp(self):
        super(StatsFailureScenarios, self).setUp()
        self.components = self.input.param("components","ns_server")
        self.components = self.components.split(":")
        self.parse = self.input.param("parse", False)
        self.metric_name = self.input.param("metric_name", "kv_curr_items")
        self.simulate_error = self.input.param("simulate_error", CouchbaseError.KILL_MEMCACHED)

    def tearDown(self):
        super(StatsFailureScenarios, self).tearDown()

    def test_prometheus_and_ns_server_stats_after_failure_scenarios(self):
        """
        Run all metrics before and after failure scenarios and validate
        both ns_server and prometheus stats
        """
        self.bucket_util.load_sample_bucket(TravelSample())
        target_node = self.servers[0]
        remote = RemoteMachineShellConnection(target_node)
        error_sim = CouchbaseError(self.log, remote)
        self.log.info("Before failure")
        self.get_all_metrics(self.components, self.parse, self.metric_name)
        try:
            # Induce the error condition
            error_sim.create(self.simulate_error)
            self.sleep(20, "Wait before reverting the error condition")
        finally:
            # Revert the simulated error condition and close the ssh session
            error_sim.revert(self.simulate_error)
            remote.disconnect()
        self.log.info("After failure")
        self.get_all_metrics(self.components, self.parse, self.metric_name)
        # TODO: Add a method to compare the stats before and after failure scenarios

    def get_low_cardinality_metrics(self, component, parse):
        content = None
        for server in self.cluster.servers[:self.nodes_init]:
            content = StatsHelper(server).get_prometheus_metrics(component=component, parse=parse)
            if not parse:
                StatsHelper(server)._validate_metrics(content)
        for line in content:
            self.log.info(line.strip("\n"))

    def get_high_cardinality_metrics(self, component, parse):
        content = None
        try:
            for server in self.cluster.servers[:self.nodes_init]:
                content = StatsHelper(server).get_prometheus_metrics_high(component=component, parse=parse)
                if not parse:
                    StatsHelper(server)._validate_metrics(content)
            for line in content:
                self.log.info(line.strip("\n"))
        except:
            pass

    def get_range_api_metrics(self, metric_name):
        label_values = {"bucket": self.bucket_util.buckets[0].name, "nodes": self.cluster.master.ip}
        content = StatsHelper(self.cluster.master).get_range_api_metrics(metric_name, label_values=label_values)
        self.log.info(content)

    def get_instant_api(self, metric_name):
        pass

    def get_all_metrics(self, components, parse, metrics):
        for component in components:
            self.get_low_cardinality_metrics(component, parse)
            self.get_high_cardinality_metrics(component, parse)
        self.get_range_api_metrics(metrics)
        self.get_instant_api(metrics)
