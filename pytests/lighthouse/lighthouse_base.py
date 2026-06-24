
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
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from cluster_utils.cluster_ready_functions import ClusterUtils, CBCluster
from global_vars import logger
from unified_control_plane import UnifiedControlPlaneClient
from lighthouse.lighthouse_portal import LighthousePortal

class LighthouseBase(CollectionBase):
    def setUp(self):
        if not hasattr(self, "input"):
            self.input = TestInputSingleton.input
        self.log = logger.get("test")
        self.log.info("Test parameters: %s" % self.input.test_params)
        # Remove LHPortal servers from input.servers so they are
        # never treated as Couchbase Server cluster nodes.
        self._exclude_portal_from_servers()
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
        # Extract UCP portal from [LHPortal] ini section
        self._setup_ucp_portal()
        self.log_setup_status("LighthouseBase", "complete")
    def _exclude_portal_from_servers(self):
        """
        Remove LHPortal nodes from input.servers and input.clusters
        so they are never included in Couchbase cluster operations
        (rebalance, tearDown reset, etc).
        """
        if not self.input.lh_portal:
            return
        portal_ips = {s.ip for s in self.input.lh_portal}
        self.input.servers = [s for s in self.input.servers
                              if s.ip not in portal_ips]
        # Also remove from any cluster group that may contain them
        for key in list(self.input.clusters.keys()):
            self.input.clusters[key] = [
                s for s in self.input.clusters[key]
                if s.ip not in portal_ips]
            # Remove empty cluster entries
            if not self.input.clusters[key]:
                del self.input.clusters[key]
    def _setup_ucp_portal(self):
        """
        Extract the UCP portal server from input.lh_portal and
        build a LighthousePortal config object.
        The IP comes from [LHPortal] ini section.
        Port and credentials can be overridden via test params:
            - ucp_port (default 8080)
            - ucp_username (default from [membase] rest_username)
            - ucp_password (default from [membase] rest_password)
        Exposes:
            self.ucp_portal  - LighthousePortal config object
            self.ucp_client  - UnifiedControlPlaneClient instance
        """
        if not self.input.lh_portal:
            self.fail("No UCP portal server found. "
                      "Ensure [LHPortal] section is defined in the ini file.")
        server = self.input.lh_portal[0]
        self.ucp_portal = LighthousePortal.from_server_and_params(
            server, self.input)
        self.log.info("UCP portal: %s" % self.ucp_portal)
        # Create UCP client using the portal config
        self.ucp_client = UnifiedControlPlaneClient(self.ucp_portal)
    def tearDown(self):
        if hasattr(self, "_original_servers"):
            self.input.servers = self._original_servers
        extra_clusters = []
        if hasattr(self, "cb_clusters"):
            extra_clusters = [name for name in self.cb_clusters
                              if name != "C1"]
            for name in extra_clusters:
                del self.cb_clusters[name]
            try:
                rest = ClusterRestAPI(server)
                status, content = rest.reset_node()
                if not status:
                    self.log.warning(
                        "Reset node %s failed: %s" % (server.ip, content))
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
