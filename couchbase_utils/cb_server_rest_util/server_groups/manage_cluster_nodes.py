from cb_server_rest_util.connection import CBRestConnection


class ManageClusterNodes(CBRestConnection):
    def __init__(self):
        super(ManageClusterNodes, self).__init__()

    def add_node(self, hostname, port, uuid, username, password,
                 services=None):
        """
        docs.couchbase.com/server/current/rest-api/rest-servergroup-post-add.html
        POST :: /pools/default/serverGroups/<uuid>/addNode
        """
        api = self.base_url + f"/pools/default/serverGroups/{uuid}/addNone"
        params = {'hostname': f"{hostname}:{port}",
                  'user': username,
                  'password': password}
        if services:
            params['services'] = ','.join(services)
        status, content, _ = self.request(api, self.POST, params=params)
        return status, content
