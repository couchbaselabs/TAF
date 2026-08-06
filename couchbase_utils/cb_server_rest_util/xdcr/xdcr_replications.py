from cb_server_rest_util.connection import CBRestConnection


class XdcrReplicationAPI(CBRestConnection):
    def __init__(self):
        super(XdcrReplicationAPI, self).__init__()

    def create_replication(self):
        """
        POST :: /controller/createReplication
        docs.couchbase.com/server/current/rest-api/rest-xdcr-create-replication.html
        """

    def delete_replication(self, replication_id):
        """
        DELETE :: /controller/cancelXDCR/<replication_id>
        docs.couchbase.com/server/current/rest-api/rest-xdcr-delete-replication.html
        """
        api = self.base_url + "/controller/cancelXDCR/" + replication_id
        status, content, _ = self.request(api, self.DELETE)
        return status, content

    def pause_resume_replication(self, replication_id, pause_requested=True):
        """
        POST :: /settings/replications/<settingsURI>
        docs.couchbase.com/server/current/rest-api/rest-xdcr-pause-resume.html
        """
        api = self.base_url + "/settings/replications/" + replication_id
        status, content, _ = self.request(api, self.POST)
        return status, content

    def stop_recovery(self, stop_uri):
        """
        POST :: <STOP_URI> retrieved from ns_server tasks' type='recovery'
        Not documented in CB docs
        """
        api = self.base_url + stop_uri
        status, content, _ = self.request(api, self.POST)
        return status, content
