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

    def get_resource_stat_monitor(self, user, node):
        """ Returns an object that allows you monitor this statisic for the given user and node """
        pass


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
