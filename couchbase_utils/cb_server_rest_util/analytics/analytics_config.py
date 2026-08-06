"""
https://docs.couchbase.com/enterprise-analytics/current/reference/rest-intro.html#enterprise-analytics-config-api
"""

import json
from cb_server_rest_util.connection import CBRestConnection


class AnalyticsConfigAPI(CBRestConnection):
    def __init__(self):
        super(AnalyticsConfigAPI, self).__init__()

    def get_service_config(self):
        """
        GET /api/v1/config/service
        View Service-Level Parameters

        :return: tuple (status, content, response) where status is boolean and content is response
        """
        api = f"{self.cbas_url}/api/v1/config/service"
        headers = self.create_headers()

        status, result, response = self.request(
            api, self.GET, headers=headers, timeout=300)

        return status, result, response

    def update_service_config(self, config_params):
        """
        PUT /api/v1/config/service
        Modify Service-Level Parameters

        :param config_params: Dictionary containing the service-level parameters to modify
        :return: tuple (status, content, response) where status is boolean and content is response
        """
        api = f"{self.cbas_url}/api/v1/config/service"
        headers = self.get_headers_for_content_type_json()
        params = json.dumps(config_params)

        status, result, response = self.request(
            api, self.PUT, params=params, headers=headers, timeout=300)

        return status, result, response
