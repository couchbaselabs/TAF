from cb_server_rest_util.connection import CBRestConnection


class Logging(CBRestConnection):
    def __init__(self):
        super(Logging, self).__init__()

    def start_logs_collection(
            self, nodes="*", log_redaction_level=None, log_redaction_salt=None,
            log_dir=None, tmp_dir=None,
            upload_host=None, upload_proxy=None,
            customer=None, ticket=None):
        """
        POST :: /controller/startLogsCollection
        docs.couchbase.com/server/current/rest-api/rest-manage-log-collection.html
        """
        api = self.base_url + "/controller/startLogsCollection"
        params = {"nodes": nodes}
        if log_redaction_level:
            params["logRedactionLevel"] = log_redaction_level
        if log_redaction_salt:
            params["logRedactionSalt"] = log_redaction_salt
        if log_dir:
            params["logDir"] = log_dir
        if tmp_dir:
            params["tmpDir"] = tmp_dir
        if upload_host:
            params["uploadHost"] = upload_host
        if upload_proxy:
            params["uploadProxy"] = upload_proxy
        if customer:
            params["customer"] = customer
        if ticket:
            params["ticket"] = ticket
        status, content, _ = self.request(api, self.POST, params=params)
        return status, content

    def cancel_logs_collection(self):
        """
        POST :: /controller/cancelLogsCollection
        docs.couchbase.com/server/current/rest-api/rest-manage-log-collection.html
        """
        api = self.base_url + "/controller/cancelLogsCollection"
        status, content, _ = self.request(api, self.POST)
        return status, content

    def log_client_error(self, message):
        """
        POST :: /logClientError
        docs.couchbase.com/server/current/rest-api/rest-client-logs.html
        """
        api = self.base_url + "/logClientError"
        status, content, _ = self.request(api, self.POST, params=message)
        return status, content
