"""
https://docs.couchbase.com/enterprise-analytics/current/reference/rest-intro.html#enterprise-analytics-admin-api
"""

import json
import urllib.parse
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

    def cancel_request_by_context_id(self, client_context_id):
        """
        DELETE /analytics/admin/active_requests?client_context_id={id}
        Cancel a running request by its client context id

        :param client_context_id: client_context_id the request was submitted with
        :return: tuple (status, content, response)
        """
        api = (f"{self.cbas_url}/analytics/admin/active_requests"
               f"?client_context_id={client_context_id}")
        headers = self.create_headers()

        status, result, response = self.request(
            api, self.DELETE, headers=headers, timeout=300)

        return status, result, response

    def restart_analytics_node(self):
        """
        POST /api/v1/node/restart
        Restart the analytics service on this node (not the whole service -
        see restart_analytics_service for that)

        :return: tuple (status, content, response)
        """
        api = f"{self.cbas_url}/api/v1/node/restart"
        headers = self.get_headers_for_content_type_json()

        status, result, response = self.request(
            api, self.POST, headers=headers, timeout=300)

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

    def clear_plan_cache(self, username=None, password=None):
        """
        DELETE /api/v1/plan_cache
        Clear the query plan cache (admin-only). Returns (status, content, response).

        :param username/password: optional non-default creds (e.g. to assert a
            non-admin caller is rejected). Uses default admin creds when unset.
        """
        api = f"{self.cbas_url}/api/v1/plan_cache"
        if username:
            headers = self.create_headers(username, password,
                                          "application/json")
        else:
            headers = self.get_headers_for_content_type_json()
        status, result, response = self.request(
            api, self.DELETE, headers=headers, timeout=300)

        return status, result, response

    def get_analytics_samples(self):
        """
        GET /api/v1/samples
        List the sample datasets this Enterprise Analytics cluster can install.

        :return: tuple (status, content, response)
        """
        api = f"{self.cbas_url}/api/v1/samples"
        status, result, response = self.request(
            api, self.GET, headers=self.create_headers(), timeout=300)

        return status, result, response

    def load_analytics_sample(self, sample_name, timeout=60):
        """
        POST /api/v1/samples  (param: sampleName)
        Install a sample dataset natively on Enterprise Analytics.

        :param sample_name: name of the sample, e.g. "travel-sample"
        :param timeout: seconds before the client abandons the connection; keep
            it short - see above
        :return: tuple (status, content, response)
        """
        api = f"{self.cbas_url}/api/v1/samples"
        params = urllib.parse.urlencode({"sampleName": sample_name})
        status, result, response = self.request(
            api, self.POST, params=params, headers=self.create_headers(),
            timeout=timeout)

        return status, result, response
