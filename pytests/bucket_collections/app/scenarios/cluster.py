from random import choice

from Cb_constants import CbServer
from bucket_collections.app.lib.common_util import \
    get_all_scenarios
from global_vars import logger
from membase.api.rest_client import RestConnection


class Cluster(object):
    scenarios = dict()
    log = logger.get("test")

    def __init__(self, task, cluster, cluster_util):
        super(Cluster, self).__init__()
        self.task = task
        self.cluster = cluster
        self.cluster_util = cluster_util
        self.rebalance_task = None

        self.spare_nodes = list()
        self.node_service_mapping = dict()

        self.update_nodes_in_cluster()

        Cluster.scenarios = get_all_scenarios(Cluster)

    def update_nodes_in_cluster(self):
        """
        1. Fetches current master node
        2. Constructs map of node_services: node_list
        3. Filters out spare nodes to be used for rebalance swap/in scenarios
        """
        self.spare_nodes = list()
        self.node_service_mapping = dict()
        self.cluster.nodes_in_cluster = list()
        cluster_status = RestConnection(self.cluster.master).cluster_status()

        self.cluster_util.find_orchestrator(self.cluster)
        self.log.info("Current master: %s" % self.cluster.master.ip)

        for node in cluster_status["nodes"]:
            node["services"].sort()
            map_key = ",".join(node["services"])
            if map_key not in self.node_service_mapping:
                self.node_service_mapping[map_key] = list()

            host_ip = node["hostname"].split(":")[0]
            for server in self.cluster.servers:
                if server.ip == host_ip:
                    self.cluster.nodes_in_cluster.append(server)
                    self.node_service_mapping[map_key].append(server)
                    break

        for server in self.cluster.servers:
            if server not in self.cluster.nodes_in_cluster:
                self.spare_nodes.append(server)

        self.log.debug("Node service map: %s" % self.node_service_mapping)
        self.log.debug("Nodes in cluster: %s" % self.cluster.nodes_in_cluster)
        self.log.debug("Spare nodes: %s" % self.spare_nodes)

    def get_node_to_remove(self, known_node_list):
        node_to_remove = choice(known_node_list)
        known_node_list.remove(node_to_remove)
        self.cluster.nodes_in_cluster.remove(node_to_remove)
        return node_to_remove

    def scenario_rebalance(self, kwargs):
        self.rebalance_task = self.task.async_rebalance(
            self.cluster, to_add=[], to_remove=[], services=None,
            sleep_before_rebalance=0, retry_get_process_num=25)

    def scenario_rebalance_in(self, kwargs):
        services = kwargs.get("services")
        self.rebalance_task = self.task.async_rebalance(
            self.cluster,
            to_add=self.spare_nodes[:len(services)], to_remove=[],
            services=services,
            sleep_before_rebalance=0, retry_get_process_num=25)

    def scenario_rebalance_out(self, kwargs):
        services = kwargs.get("services")
        nodes_to_remove = list()
        for service in services:
            self.log.debug("Removing node with services: %s" % service)
            service = service.split(",")
            service.sort()
            service = ','.join(service)
            if service in self.node_service_mapping:
                node_to_remove = \
                    self.get_node_to_remove(self.node_service_mapping[service])
                nodes_to_remove.append(node_to_remove)
            else:
                for t_service, node_list in self.node_service_mapping.items():
                    if service in t_service:
                        node_to_remove = self.get_node_to_remove(node_list)
                        nodes_to_remove.append(node_to_remove)
                        break

        self.rebalance_task = self.task.async_rebalance(
            self.cluster, to_add=[], to_remove=nodes_to_remove,
            check_vbucket_shuffling=True,
            sleep_before_rebalance=0, retry_get_process_num=25)

        # Update existing node as master
        for _, node_list in self.node_service_mapping.items():
            if node_list:
                self.cluster.master = node_list[0]
                break

    def scenario_rebalance_swap(self, kwargs):
        services = kwargs.get("services")
        nodes_to_remove = list()
        for service in services:
            self.log.debug("Swap node with services: %s" % service)
            service = service.split(",")
            service.sort()
            service = ','.join(service)
            if service in self.node_service_mapping:
                node_to_remove = \
                    self.get_node_to_remove(self.node_service_mapping[service])
                nodes_to_remove.append(node_to_remove)
            else:
                for t_service, node_list in self.node_service_mapping.items():
                    if service in t_service:
                        node_to_remove = self.get_node_to_remove(node_list)
                        nodes_to_remove.append(node_to_remove)
                        break
        self.rebalance_task = self.task.async_rebalance(
            self.cluster,
            to_add=self.spare_nodes[:len(services)], to_remove=nodes_to_remove,
            check_vbucket_shuffling=False, services=services,
            sleep_before_rebalance=0, retry_get_process_num=25)

        # Update existing node as master
        self.cluster.master = self.spare_nodes[0]

    def scenario_graceful_failover(self, kwargs):
        services = kwargs.get("services")
        nodes_to_failover = list()
        for service in services:
            self.log.debug("Fetching %s nodes to failover" % service)
            service = service.split(",")
            service.sort()
            service = ','.join(service)
            if service in self.node_service_mapping:
                node_to_remove = \
                    self.get_node_to_remove(self.node_service_mapping[service])
                nodes_to_failover.append(node_to_remove)
            else:
                for t_service, node_list in self.node_service_mapping.items():
                    if service in t_service:
                        node_to_remove = self.get_node_to_remove(node_list)
                        nodes_to_failover.append(node_to_remove)
                        break
        self.rebalance_task = self.task.async_failover(
            [self.cluster.master], nodes_to_failover, graceful=True)

        # Update existing node as master
        for _, node_list in self.node_service_mapping.items():
            if node_list:
                self.cluster.master = node_list[0]
                break

    def scenario_failover_node_delta_recovery(self, kwargs):
        services = kwargs.get("services")
        nodes_in_failed_state = list()

        rest = RestConnection(self.cluster.master)
        nodes_status = rest.cluster_status()["nodes"]
        for node in nodes_status:
            if node["clusterMembership"] == "inactiveFailed":
                nodes_in_failed_state.append(node["otpNode"].split("@")[1])

        self.log.info("Nodes in failover state: %s" % nodes_in_failed_state)
        if len(services) > len(nodes_in_failed_state):
            self.log.warning(
                "Failover nodes '%s' < '%s' expected node to recover"
                % (len(nodes_in_failed_state), len(services)))
        for index, service in enumerate(services):
            rest.set_recovery_type(
                otpNode="ns_1@"+nodes_in_failed_state[index],
                recoveryType=CbServer.Failover.RecoveryType.DELTA)

    def run(self, op_type, **kwargs):
        self.log.info("Running cluster scenario: %s" % op_type)
        self.update_nodes_in_cluster()
        Cluster.scenarios["scenario_%s" % op_type](self, kwargs)
