"""
https://docs.couchbase.com/server/current/analytics-rest-service/index.html
"""
import json
from cb_server_rest_util.connection import CBRestConnection


class AnalyticsFunctionsAPI(CBRestConnection):
    def __init__(self):
        super(AnalyticsFunctionsAPI, self).__init__()

    def execute_statement_on_cbas(self, statement, mode=None, pretty=True,
                                  timeout=70, client_context_id=None):
        """
        POST /analytics/service
        https://docs.couchbase.com/server/current/analytics-rest-service/index.html
        """
        api = f"{self.cbas_url}/analytics/service"
        headers = self.get_headers_for_content_type_json()

        params = {'statement': statement, 'pretty': pretty, 'client_context_id': client_context_id}

        if mode is not None:
            params['mode'] = mode

        status, content, _ = self.request(api, self.POST,
                                          headers=headers,
                                          params=json.dumps(params),
                                          timeout=timeout)
        return status, content