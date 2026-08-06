from cb_server_rest_util.search.search_functions import IndexFunctions


class SearchRestAPI(IndexFunctions):
    def __init__(self, server):
        super(SearchRestAPI).__init__()

        self.set_server_values(server)
        self.set_endpoint_urls(server)
        self.check_if_couchbase_is_active(self, max_retry=5)
