"""
https://docs.couchbase.com/enterprise-analytics/current/reference/rest-intro.html#enterprise-analytics-admin-api
"""

import json
from cb_server_rest_util.connection import CBRestConnection


class AnalyticsAdminAPI(CBRestConnection):
    def __init__(self):
        super(AnalyticsAdminAPI, self).__init__()

    def cancel_request(self, request_id):
        """
        DELETE /api/v1/active_requests?request_id={requestID}
        Cancel a running async request

        :param request_id: Request ID of the request to cancel
        :return: tuple (status, content) where status is boolean and content is response
        """
        api = f"{self.cbas_url}/api/v1/active_requests?request_id={request_id}"
        headers = self.create_headers()

        status, result, response = self.request(
            api, self.DELETE, headers=headers, timeout=300)

        return status, result, response

    def restart_analytics_service(self):
        """
        POST /api/v1/service/restart
        Restart the analytics service

        :return: tuple (status, content) where status is boolean and content is response
        """
        api = f"{self.cbas_url}/api/v1/service/restart"
        headers = self.get_headers_for_content_type_json()
        status, result, response = self.request(
            api, self.POST, headers=headers, timeout=300)

        return status, result, response
