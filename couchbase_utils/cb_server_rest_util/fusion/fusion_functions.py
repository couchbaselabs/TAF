from cb_server_rest_util.connection import CBRestConnection

class FusionFunctions(CBRestConnection):
    def __init__(self):
        super(FusionFunctions).__init__()

    def get_active_guest_volumes(self):
        """
        GET :: /fusion/activeGuestVolumes
        """
        api = self.base_url + "/fusion/activeGuestVolumes"
        status, content, _ = self.request(api)
        return status, content

    def manage_fusion_settings(self, log_store_uri=None, enable_sync_threshold=None):
        """
        POST / GET :: /settings/fusion
        """
        api = self.base_url + "/settings/fusion"

        params = dict()
        if log_store_uri is not None:
            params["logStoreURI"] = log_store_uri
        if enable_sync_threshold is not None:
            params["enableSyncThresholdMB"] = enable_sync_threshold
        if params:
            # POST method
            status, _, response = self.request(api, CBRestConnection.POST,
                                               params=params)
        else:
            # GET method
            status, _, response = self.request(api, CBRestConnection.GET)
        content = response.json() if status else response.text
        return status, content

    def get_fusion_status(self):
        """
        GET :: /fusion/status
        """
        api = self.base_url + "/fusion/status"
        status, content, _ = self.request(api)
        return status, content

    def enable_fusion(self, buckets=None):
        """
        POST :: /fusion/enable
        """
        api = self.base_url + "/fusion/enable"
        if buckets is not None:
            params = dict()
            params["buckets"] = buckets
            status, content, _ = self.request(api, CBRestConnection.POST, params=params)
        else:
            status, content, _ = self.request(api, CBRestConnection.POST)
        return status, content

    def disable_fusion(self):
        """
        POST :: /fusion/disable
        """
        api = self.base_url + "/fusion/disable"
        status, content, _ = self.request(api, CBRestConnection.POST)
        return status, content

    def stop_fusion(self):
        """
        POST :: /fusion/stop
        """
        api = self.base_url + "/fusion/stop"
        status, content, _ = self.request(api, CBRestConnection.POST)
        return status, content

    def prepare_rebalance(self, keep_nodes):
        """
        POST :: /controller/fusion/prepareRebalance
        """
        keepNodes = ','.join(keep_nodes)
        params = {'keepNodes': keepNodes}

        api = self.base_url + "/controller/fusion/prepareRebalance"
        status, content, _ = self.request(api, CBRestConnection.POST, params=params)
        return status, content