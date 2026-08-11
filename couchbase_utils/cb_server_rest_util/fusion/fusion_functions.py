import json

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

    def prepare_rebalance(self, keep_nodes, snapshot_lifetime_sec=None):
        """
        POST :: /controller/fusion/prepareRebalance
        """
        keepNodes = ','.join(keep_nodes)
        params = {'keepNodes': keepNodes}
        if snapshot_lifetime_sec is not None:
            params["snapshotLifetimeSec"] = snapshot_lifetime_sec

        api = self.base_url + "/controller/fusion/prepareRebalance"
        status, content, _ = self.request(api, CBRestConnection.POST, params=params)
        return status, content

    def sync_log_store(self):
        """
        POST :: /controller/fusion/syncLogStore
        Force the latest snapshot on disk to LogStore.
        """
        api = self.base_url + "/controller/fusion/syncLogStore"
        status, content, _ = self.request(api, CBRestConnection.POST)
        return status, content

    def prepare_snapshot_restore(self, buckets):
        """
        POST :: /controller/fusion/prepareSnapshotRestore

        buckets: list of {"config": {"name", "replicaNumber", "ramQuota"},
                          "manifest": <accelerator-cli generate-manifest output>}
        Returns (status, content); content carries the plan's "planUUID" plus
        the per-node manifest that accelerator-cli split-manifest consumes.
        """
        api = self.base_url + "/controller/fusion/prepareSnapshotRestore"
        headers = self.get_headers_for_content_type_json()
        body = json.dumps({"buckets": buckets})
        status, content, _ = self.request(api, CBRestConnection.POST, body,
                                          headers=headers)
        return status, content

    def restore_snapshot(self, plan_uuid, nodes):
        """
        POST :: /controller/fusion/restoreSnapshot?planUUID=<plan_uuid>

        nodes: list of {"name": <otpNode>, "guestVolumePaths": [<path>, ...]}
        Synchronous: the request blocks until the restore completes.
        """
        api = "{0}/controller/fusion/restoreSnapshot?planUUID={1}".format(
            self.base_url, plan_uuid)
        headers = self.get_headers_for_content_type_json()
        body = json.dumps({"nodes": nodes})
        status, content, _ = self.request(api, CBRestConnection.POST, body,
                                          headers=headers, timeout=3600)
        return status, content