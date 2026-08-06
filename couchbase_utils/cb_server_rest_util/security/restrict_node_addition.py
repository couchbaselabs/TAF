from cb_server_rest_util.connection import CBRestConnection


class NodeInitAddition(CBRestConnection):
    def __init__(self):
        super(NodeInitAddition, self).__init__()

    def cluster_init(self):
        pass

    def set_internal_password_rotation_interval(self, rotation_interval=1800000):
        """
        POST :: /settings/security
        Not documented in CB docs
        """
        api = self.base_url + "/settings/security"
        data = {"intCredsRotationInterval": rotation_interval}
        status, content, response = self.request(
            api, CBRestConnection.POST, params=data)
        if status:
            content = response.json()
        return status, content
