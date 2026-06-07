# -*- coding: utf-8 -*-
"""
LighthouseBase - Base class for multi-cluster Lighthouse tests.

Inheritance: LighthouseBase -> CollectionBase -> ClusterSetup -> OnPremBaseTest

Usage in ini file:
    [cluster1]
    1:_1
    2:_2

    [cluster3]
    3:_3
    4:_4

    [LHPortal]
    5:_5

    [servers]
    1:_1
    2:_2
    3:_3
    4:_4
    5:_5

Usage in conf:
    test_something,nodes_init=2|2,services_init=kv:n1ql-kv:n1ql|kv:cbas-kv:cbas,num_of_clusters=2
"""

from TestInput import TestInputSingleton
from bucket_collections.collections_base import CollectionBase
from membase.api.rest_client import RestConnection
from couchbase_utils.cluster_utils.cluster_ready_functions import ClusterUtils, CBCluster
from global_vars import logger


class LighthouseBase(CollectionBase):

    def setUp(self):
        if not hasattr(self, "input"):
            self.input = TestInputSingleton.input
        self.log = logger.get("test")
        self.log.info("Test parameters: %s" % self.input.test_params)
        self._topology = ClusterUtils.get_cluster_topology(self.input)
        self._is_multi_cluster = self._topology["num_clusters"] > 1
        self.log.info("Cluster topology: %s" % self._topology)
        if self._is_multi_cluster:
            self._restrict_input_to_primary_cluster()
        super(LighthouseBase, self).setUp()
        self.cluster.nodes_in_cluster = \
            self._topology["clusters"][0]["nodes"]
        self.clusters = list()
        self.clusters.append(self.cluster)
        if self._is_multi_cluster:
            self._setup_additional_clusters()
        self.log_setup_status("LighthouseBase", "complete")

    def tearDown(self):
        if hasattr(self, "_original_servers"):
            self.input.servers = self._original_servers
        extra_clusters = []
        if hasattr(self, "cb_clusters"):
            extra_clusters = [name for name in self.cb_clusters
                              if name != "C1"]
            for name in extra_clusters:
                del self.cb_clusters[name]

        super(LighthouseBase, self).tearDown()
        all_servers = getattr(self, "_original_servers",
                              self.input.servers)
        for server in all_servers:
            try:
                rest = RestConnection(server)
                rest.reset_node()
            except Exception as e:
                self.log.warning(
                    "Failed to reset node %s: %s" % (server.ip, e))


    def _restrict_input_to_primary_cluster(self):
        self._original_servers = list(self.input.servers)
        primary = self._topology["clusters"][0]
        self.input.test_params["nodes_init"] = str(len(primary["nodes"]))
        if primary["services"]:
            self.input.test_params["services_init"] = \
                "-".join(s.replace(",", ":") for s in primary["services"])
        self.input.clusters.clear()
        self.input.clusters[0] = primary["nodes"]
        self.input.servers = primary["nodes"]

    def _setup_additional_clusters(self):
        cluster_name_format = "C%s"
        for i in range(1, self._topology["num_clusters"]):
            cluster_info = self._topology["clusters"][i]
            cluster_nodes = cluster_info["nodes"]
            cluster_services = cluster_info["services"]
            cluster_name = cluster_name_format % str(i + 1)

            cluster = CBCluster(name=cluster_name,
                                servers=cluster_nodes)
            self.cb_clusters[cluster_name] = cluster

            master_services = cluster_services[0] \
                if cluster_services else None
            self.initialize_cluster(
                cluster_name, cluster, services=master_services)

            self.enforce_tls_and_set_ports(cluster_nodes)

            cluster.nodes_in_cluster = list(cluster_nodes)

            self._rebalance_in_nodes(cluster, cluster_name,
                                     cluster_nodes, cluster_services)

            self.cluster_util.update_cluster_nodes_service_list(cluster)
            self.clusters.append(cluster)
            self.log.info("Cluster %s ready: %d node(s) [%s]"
                          % (cluster_name, len(cluster_nodes),
                             ", ".join(n.ip for n in cluster_nodes)))

    def _rebalance_in_nodes(self, cluster, cluster_name,
                            cluster_nodes, cluster_services):
        services = cluster_services[1:] \
            if len(cluster_services) > 1 else None
        result = ClusterUtils.rebalance_in_nodes(
            self.task, cluster, services=services)
        if not result:
            self.fail("Initial rebalance failed for %s" % cluster_name)

