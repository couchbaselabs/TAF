import requests
from cb_server_rest_util.connection import CBRestConnection


class XdcrReferencesAPI(CBRestConnection):
    def __init__(self):
        super(XdcrReferencesAPI, self).__init__()

    def get_remote_references(self):
        """
        GET :: /pools/default/remoteClusters
        docs.couchbase.com/server/current/rest-api/rest-xdcr-get-ref.html
        """
        api = self.base_url + "/pools/default/remoteClusters"
        status, content, _ = self.request(api)
        return status, content

    def create_remote_reference(self, target_cluster_local_name=None):
        """
        POST :: /pools/default/remoteClusters
        docs.couchbase.com/server/current/rest-api/rest-xdcr-create-ref.html
        """
        api = self.base_url + "/pools/default/remoteClusters"
        if target_cluster_local_name:
            api += f"/{target_cluster_local_name}"

    def delete_remote_reference(self, target_cluster_name):
        """
        POST ::  /pools/default/remoteClusters/<target_cluster_name>
        docs.couchbase.com/server/current/rest-api/rest-xdcr-delete-ref.html
        """
        target_cluster_name = requests.utils.quote(target_cluster_name)
        api = self.base_url + f'/pools/default/remoteClusters/{target_cluster_name}'
        status, content, _ = self.request(api, self.DELETE)
        return status, content

    def connection_pre_check(self):
        """
        POST :: /xdcr/connectionPreCheck
        docs.couchbase.com/server/current/rest-api/rest-xdcr-connection-precheck.html
        """
