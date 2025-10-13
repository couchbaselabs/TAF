from cb_server_rest_util.connection import CBRestConnection


class AutoReprovisionAPI(CBRestConnection):
    def __init__(self):
        super(AutoReprovisionAPI, self).__init__()

    def get_auto_reprovision_settings(self):
        """
        GET /settings/autoReprovision
        No documentation present
        """
        api = self.base_url + "/settings/autoReprovision"
        status, content, _ = self.request(api, self.GET)
        return status, content

    def update_auto_reprovision_settings(self, enabled, max_nodes=1):
        """
        POST /settings/autoReprovision
        No documentation present
        """
        api = self.base_url + "/settings/autoReprovision"
        params = {"enabled": enabled,
                  "maxNodes": max_nodes}
        status, content, _ = self.request(api, self.POST, params=params)
        return status, content

    def reset_auto_reprovision(self):
        """
        POST /settings/autoReprovision/resetCount
        No documentation present
        """
        api = self.base_url + '/settings/autoReprovision/resetCount'
        status, content, _ = self.request(api, self.POST)
        return status, content
