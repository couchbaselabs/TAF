"""
https://docs.couchbase.com/server/current/rest-api/rest-index-service.html
"""

from cb_server_rest_util.connection import CBRestConnection


class QueryFunctions(CBRestConnection):
    def __init__(self):
        super(QueryFunctions).__init__()

    def run_query(self, query, timeout=None):
        """
        POST :: /query/service
        docs.couchbase.com/server/current/n1ql-rest-query/index.html
        """

        api = self.query_url + "/query/service"
        params = {"statement": query}

        if timeout:
            status, content, _ = self.request(api, self.POST, params, timeout=timeout)
        else:
            status, content, _ = self.request(api, self.POST, params)

        return status, content
