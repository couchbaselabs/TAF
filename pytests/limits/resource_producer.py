from limits.ns_server_num_concurrent_requests import NsServerNumConcurrentRequests
from limits.ns_server_ingress import NsServerIngress
from limits.ns_server_egress import NsServerEgress


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
        else:
            raise ValueError(
                "unknown resource name '{}'".format(
                    self.resource_name))

    def get_resource_stat_monitor(self, user, node):
        """ Returns an object that allows you monitor this statisic for the given user and node """
        pass
