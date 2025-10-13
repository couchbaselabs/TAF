from cb_server_rest_util.index.index_settings import IndexSettings


class IndexRestAPI(IndexSettings):
    def __init__(self, server):
        super(IndexRestAPI).__init__()

        self.set_server_values(server)
        self.set_endpoint_urls(server)
        self.check_if_couchbase_is_active(self, max_retry=5)
