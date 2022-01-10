from collections import defaultdict
from limits.ns_server_num_concurrent_requests import NsServerNumConcurrentRequests
from limits.ns_server_ingress import NsServerIngress
from limits.ns_server_egress import NsServerEgress
from limits.ns_server_num_collections import NsServerNumCollections
from limits.kv_num_connections import KvNumConnections
from limits.kv_egress import KvEgress
from limits.kv_ingress import KvIngress
from limits.kv_ops import KvOps
from limits.kv_data_size import KvDataSize
from cb_tools.mc_stat import McStat
from remote.remote_util import RemoteMachineShellConnection
from StatsLib.StatsOperations import StatsHelper
import re


class UserResourceProducer(object):
    """ A factory for objects related to a specific resource. """

    def __init__(self, resource_name, hints={}):
        self.resource_name, self.hints = resource_name, hints

    def get_resource_task(self, user, node):
        """ Returns a UserResourceTask """
        if self.resource_name == "ns_server_num_concurrent_requests":
            if "streaminguri" not in self.hints:
                raise ValueError("streaminguri is missing from the hints")
            return NsServerNumConcurrentRequests(
                user, node, self.hints["streaminguri"])
        elif self.resource_name == "ns_server_egress":
            return NsServerEgress(user, node)
        elif self.resource_name == "ns_server_ingress":
            return NsServerIngress(user, node)
        elif self.resource_name == "ns_server_num_collections":
            return NsServerNumCollections("default", user.scope, user, node)
        elif self.resource_name == "kv_num_connections":
            return KvNumConnections(user, node)
        elif self.resource_name == "kv_egress":
            return KvEgress(user, node)
        elif self.resource_name == "kv_ingress":
            return KvIngress(user, node)
        elif self.resource_name == "kv_ops":
            return KvOps(user, node)
        elif self.resource_name == "kv_data_size":
            return KvDataSize("default", user.scope, user, node)
        else:
            raise ValueError(
                "unknown resource name '{}'".format(
                    self.resource_name))

    def mc_stats(self, user, node, pattern):
        shell_conn = RemoteMachineShellConnection(node)
        output = McStat(shell_conn).get_user_stat("default", user)
        shell_conn.disconnect()
        return pattern.findall(output[0])

    def ns_server_stat(self, node, pattern):
        content = StatsHelper(node).get_prometheus_metrics_high(component="ns_server")
        output = [re.findall(r'\s\d+', line)[0] for line in content if pattern in line]
        return output

    def get_resource_stat_monitor(self, users, nodes, throughput, above=True):
        """ Returns an object that allows you monitor this statisic for the given user and node """
        self.output = list()
        for node in nodes:
            if self.resource_name == "kv_ingress":
                pattern = re.compile(r"\"ingress_bytes\":(\d*)")
                self.output = self.mc_stats(users[0], node, pattern)
            if self.resource_name == "kv_egress":
                pattern = re.compile(r"\"egress_bytes\":(\d*)")
                self.output = self.mc_stats(users[0], node, pattern)
            if self.resource_name == "kv_num_connections":
                pattern = re.compile(r"\"total\":(\d*)")
                self.output = self.mc_stats(users[0], node, pattern)
            if self.resource_name == "ns_server_num_concurrent_requests":
                self.output = self.ns_server_stat(node, pattern="cm_num_concurrent_requests")
            elif self.resource_name == "ns_server_egress":
                self.output = self.ns_server_stat(node, pattern="cm_egress_1m_max")
            elif self.resource_name == "ns_server_ingress":
                self.output = self.ns_server_stat(node, pattern="cm_ingress_1m_max")
            for value in self.output:
                if above:
                    if int(value) < throughput:
                        raise Exception("{0} didnot exceeed limits actual {1}"
                                        "expected {2}". format(value, throughput,
                                                               self.resource_name))
                else:
                    if int(value) > throughput:
                        raise Exception("{0} above limits actual {1}"
                                        "expected {2}". format(value, throughput,
                                                               self.resource_name))

class LimitConfig:
    def __init__(self):
        self.user_config = defaultdict(dict)
        self.scope_config = defaultdict(dict)

    def set_limit(self, resource_name, value):
        if resource_name == "ns_server_num_concurrent_requests":
            self.user_config["clusterManager"]["num_concurrent_requests"] = value
        elif resource_name == "ns_server_ingress":
            self.user_config["clusterManager"]["ingress_mib_per_min"] = value
        elif resource_name == "ns_server_egress":
            self.user_config["clusterManager"]["egress_mib_per_min"] = value
        elif resource_name == "ns_server_num_collections":
            self.scope_config["clusterManager"]["num_collections"] = value
        elif resource_name == "kv_num_connections":
            self.user_config["kv"]["num_connections"] = value
        elif resource_name == "kv_egress":
            self.user_config["kv"]["egress_mib_per_min"] = value
        elif resource_name == "kv_ingress":
            self.user_config["kv"]["ingress_mib_per_min"] = value
        elif resource_name == "kv_ops":
            self.user_config["kv"]["num_ops_per_min"] = value
        elif resource_name == "kv_data_size":
            self.scope_config["kv"]["data_size"] = value
        else:
            raise ValueError(
                "unknown resource name '{}'".format(resource_name))

    def get_user_config(self):
        return self.user_config

    def get_scope_config(self):
        return self.scope_config
