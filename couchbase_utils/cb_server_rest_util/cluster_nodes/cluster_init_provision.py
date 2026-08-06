"""
https://docs.couchbase.com/server/current/rest-api/rest-cluster-init-and-provisioning.html
https://docs.couchbase.com/server/current/rest-api/rest-adding-and-removing-nodes.html
"""
from cb_server_rest_util.connection import CBRestConnection


class ClusterInitializationProvision(CBRestConnection):
    def __init__(self):
        super(ClusterInitializationProvision, self).__init__()

    def initialize_cluster(self, hostname, username, password,
                           data_path=None, index_path=None,
                           cbas_path=None, eventing_path=None,
                           java_home=None, send_stats=None,
                           cluster_name="TAF_Cluster", services="kv",
                           memory_quota=None, index_memory_quota=None,
                           eventing_memory_quota=None, fts_memory_quota=None,
                           cbas_memory_quota=None,
                           afamily=None, afamily_only=None,
                           node_encryption=None, indexer_storage_mode=None,
                           port='SAME', allowed_hosts=None):
        """
        POST /clusterInit
        docs.couchbase.com/server/current/rest-api/rest-initialize-cluster.html
        """
        params = {"clusterName": cluster_name, "hostname": hostname,
                  "username": username, "password": password,
                  "port": port}
        if data_path:
            params["dataPath"] = data_path
        if index_path:
            params["indexPath"] = index_path
        if eventing_path:
            params["eventingPath"] = eventing_path
        if cbas_path:
            params["analyticsPath"] = cbas_path
        if java_home:
            params["javaHome"] = java_home
        if send_stats:
            params["sendStats"] = send_stats
        if services:
            params["services"] = services
        if memory_quota:
            params["memoryQuota"] = memory_quota
        if index_memory_quota:
            params["indexMemoryQuota"] = index_memory_quota
        if eventing_memory_quota:
            params["eventingMemoryQuota"] = eventing_memory_quota
        if fts_memory_quota:
            params["ftsMemoryQuota"] = fts_memory_quota
        if cbas_memory_quota:
            params["cbasMemoryQuota"] = cbas_memory_quota
        if afamily:
            params["afamily"] = afamily
        if afamily_only:
            params["afamilyOnly"] = afamily_only
        if node_encryption:
            params["nodeEncryption"] = node_encryption
        if allowed_hosts:
            params["allowedHosts"] = allowed_hosts
        if indexer_storage_mode:
            params["indexerStorageMode"] = indexer_storage_mode

        api = self.base_url + "/clusterInit"
        status, response, _ = self.request(api, CBRestConnection.POST, params)
        return status, response

    def initialize_node(self, username, password,
                        data_path=None, index_path=None,
                        cbas_path=None, eventing_path=None, java_home=None):
        """
        POST /nodes/self/controller/settings
        docs.couchbase.com/server/current/rest-api/rest-initialize-node.html
        """
        api = self.base_url + "/nodes/self/controller/settings"
        params = dict()
        if data_path:
            params["path"] = data_path
        if index_path:
            params["index_path"] = index_path
        if cbas_path:
            params["cbas_path"] = cbas_path
        if eventing_path:
            params["eventing_path"] = eventing_path
        if java_home:
            params["java_home"] = java_home
        headers = self.create_headers(username, password)
        status, content, _ = self.request(api, self.POST, params, headers)
        return status, content

    def establish_credentials(self, username, password, port="SAME"):
        """
        POST /settings/web
        docs.couchbase.com/server/current/rest-api/rest-establish-credentials.html
        """
        api = self.base_url + "/settings/web"
        params = {"username": username,
                  "password": password,
                  "port": port}
        headers = self.create_headers(username, password)
        status, content, _ = self.request(api, CBRestConnection.POST,
                                          params, headers)
        return status, content

    def reset_node(self):
        """
        POST :: /controller/hardResetNode
        Not documented in CB docs
        """
        api = self.base_url + "/controller/hardResetNode"
        status, content, _ = self.request(api, self.POST)
        return status, content

    def rename_node(self, hostname):
        """
        POST /node/controller/rename
        docs.couchbase.com/server/current/rest-api/rest-name-node.html
        """
        api = self.base_url + "/node/controller/rename"
        params = {"hostname": hostname}
        headers = self.create_headers(self.username, self.password)
        status, content, _ = self.request(api, CBRestConnection.POST,
                                          params, headers)
        return status, content

    def configure_memory(self, mem_quota_dict):
        """
        POST /pools/default
        docs.couchbase.com/server/current/rest-api/rest-configure-memory.html
        """
        api = self.base_url + "/pools/default"
        headers = self.create_headers(self.username, self.password)
        status, content, _ = self.request(api, CBRestConnection.POST,
                                          mem_quota_dict, headers)
        return status, content

    def setup_services(self, services):
        """
        POST /node/controller/setupServices
        docs.couchbase.com/server/current/rest-api/rest-set-up-services.html
        :param services: List of services to configure as string
        """
        api = self.base_url + "/node/controller/setupServices"
        params = {"services": ",".join(services)}
        headers = self.create_headers(self.username, self.password)
        status, content, _ = self.request(api, CBRestConnection.POST,
                                          params, headers)
        return status, content

    def naming_the_cluster(self, cluster_name):
        """
        POST /pools/default
        docs.couchbase.com/server/current/rest-api/rest-name-cluster.html
        """
        api = self.base_url + "/node/controller/setupServices"
        params = {"clusterName": cluster_name}
        headers = self.create_headers(self.username, self.password)
        status, content, _ = self.request(api, CBRestConnection.POST,
                                          params, headers)
        return status, content
