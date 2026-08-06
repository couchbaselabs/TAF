"""
https://docs.couchbase.com/server/current/rest-api/rest-index-service.html
"""

from cb_server_rest_util.connection import CBRestConnection


class QueryFunctions(CBRestConnection):
    def __init__(self):
        super(QueryFunctions).__init__()

    def run_query(self, params, timeout=None):
        """
        POST :: /query/service
        docs.couchbase.com/server/current/n1ql-rest-query/index.html

        :param params: Request parameters (dict for JSON body, or string
                      for form data)
        :param timeout: Request timeout in seconds
        :return: status, content
        """
        api = self.query_url + "/query/service"
        if timeout:
            status, content, _ = self.request(
                api, self.POST, params=params, timeout=timeout)
        else:
            status, content, _ = self.request(api, self.POST, params=params)

        return status, content
