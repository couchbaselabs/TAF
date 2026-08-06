from cb_server_rest_util.server_groups.manage_cluster_nodes import \
    ManageClusterNodes
from cb_server_rest_util.server_groups.manage_groups import ManageServerGroups


class ServerGroupsAPI(ManageServerGroups, ManageClusterNodes):
    def __init__(self, server):
        """
        Main gateway for all Cluster Rest Operations
        """
        super(ServerGroupsAPI, self).__init__()

        self.set_server_values(server)
        self.set_endpoint_urls(server)
        self.check_if_couchbase_is_active(self, max_retry=5)
