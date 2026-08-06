from cb_server_rest_util.connection import CBRestConnection


class BackupManageAndConfigAPIs(CBRestConnection):
    def __init__(self):
        super(BackupManageAndConfigAPIs, self).__init__()

    def get_cluster_info(self):
        """
        GET /cluster/self
        docs.couchbase.com/server/current/rest-api/backup-get-cluster-info.html
        """
        api = self.base_url + "/_p/backup/api/v1/cluster/self"
        status, content, _ = self.request(api, self.GET)
        return status, content

    def get_current_config(self):
        """
        GET /config
        docs.couchbase.com/server/current/rest-api/backup-manage-config.html
        """
        api = self.base_url + "/_p/backup/api/v1/config"
        status, content, _ = self.request(api, self.GET)
        return status, content

    def update_backup_config(self, history_rotation_period=None,
                             history_rotation_size=None):
        """
        POST /config
        docs.couchbase.com/server/current/rest-api/backup-manage-config.html
        """
        params = dict()
        if history_rotation_size is not None:
            params["history_rotation_size"] = int(history_rotation_size)
        if history_rotation_period is not None:
            params["history_rotation_period"] = int(history_rotation_period)
        api = self.base_url + "/_p/backup/api/v1/config"
        status, content, _ = self.request(api, self.POST, params=params)
        return status, content
