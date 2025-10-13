"""
https://docs.couchbase.com/server/current/rest-api/rest-fts.html
"""

from cb_server_rest_util.connection import CBRestConnection


class IndexFunctions(CBRestConnection):
    def __init__(self):
        super(IndexFunctions).__init__()

    def create_fts_index_from_json(self, index_name, param_data, timeout=60):
        """
        PUT :: /api/index/{index_name}
        docs.couchbase.com/server/current/rest-api/rest-fts-indexing.html
        """
        api = f"{self.fts_url}/api/index/{index_name}"
        headers = self.create_headers(content_type="application/json")
        status, content, _ = self.request(api, self.PUT, param_data, headers=headers, timeout=timeout)
        return status, content

    def run_fts_query(self, index_name, param_data, timeout=120):
        """
        POST :: /api/index/{index_name}/query
        docs.couchbase.com/server/current/rest-api/rest-fts-indexing.html
        """
        api = f"{self.fts_url}/api/index/{index_name}/query"
        headers = self.create_headers(content_type="application/json")
        status, content, _ = self.request(api, self.POST, param_data, headers=headers, timeout=timeout)
        return status, content

    def delete_fts_index(self, index_name, timeout=60):
        """
        DELETE :: /api/index/{index_name}
        docs.couchbase.com/server/current/rest-api/rest-fts-indexing.html
        """
        api = f"{self.fts_url}/api/index/{index_name}"
        status, content, _ = self.request(api, self.DELETE, timeout=timeout)
        return status, content

    def fts_index_item_count(self, index_name, timeout=60):
        """
        GET :: /api/index/{index_name}/count
        docs.couchbase.com/server/current/rest-api/rest-fts-indexing.html
        """
        api = f"{self.fts_url}/api/index/{index_name}/count"
        status, content, _ = self.request(api, self.GET, timeout=timeout)
        return status, content