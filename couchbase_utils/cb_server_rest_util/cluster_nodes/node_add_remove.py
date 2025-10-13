"""
https://docs.couchbase.com/server/current/rest-api/rest-adding-and-removing-nodes.html
"""
from cb_server_rest_util.connection import CBRestConnection


class NodeAdditionRemoval(CBRestConnection):
    def __init__(self):
        super(NodeAdditionRemoval).__init__()

    def add_node(self, host_name, username=None, password=None, services=None):
        """
        POST :: /controller/addNode
        docs.couchbase.com/server/current/rest-api/rest-cluster-addnodes.html
        """
        api = self.base_url + "/controller/addNode"
        params = {"hostname": host_name}
        if username:
            params["user"] = username
        if password:
            params["password"] = password
        if services:
            params["services"] = services
        status, content, _ = self.request(api, self.POST, params=params)
        return status, content

    def join_node_to_cluster(self, cluster_member_host_ip,
                             cluster_member_port=None,
                             username=None, password=None, services=None):
        """
        POST :: /node/controller/doJoinCluster
        docs.couchbase.com/server/current/rest-api/rest-cluster-joinnode.html
        """
        api = self.base_url + "/node/controller/doJoinCluster"
        params = {"clusterMemberHostIp": cluster_member_host_ip}
        if cluster_member_port:
            params["clusterMemberPort"] = cluster_member_port
        if username:
            params["user"] = username
        if password:
            params["password"] = password
        if services:
            params["services"] = services
        status, content, _ = self.request(api, self.POST, params=params)
        return status, content

    def eject_node(self, otp_node):
        """
        POST :: /controller/ejectNode
        docs.couchbase.com/server/current/rest-api/rest-cluster-removenode.html
        """
        api = self.base_url + "/controller/ejectNode"
        status, content, _ = self.request(api, self.POST,
                                          params={"otpNode": otp_node})
        return status, content

    def re_add_node(self, otp_node):
        """
        POST :: /controller/reAddNode
        No documentation present
        """
        api = self.base_url + "/controller/reAddNode"
        status, content, _ = self.request(api, self.POST,
                                          params={"otpNode": otp_node})
        return status, content
