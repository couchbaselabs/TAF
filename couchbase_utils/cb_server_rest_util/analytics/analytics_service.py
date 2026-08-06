"""
https://docs.couchbase.com/enterprise-analytics/current/reference/rest-intro.html#enterprise-analytics-service-api
"""

import json
from cb_server_rest_util.connection import CBRestConnection


class AnalyticsServiceAPI(CBRestConnection):
    def __init__(self):
        super(AnalyticsServiceAPI, self).__init__()

    def submit_service_request(self, statement, mode=None, client_context_id=None,
                               format="JSON", pretty=True, query_context=None,
                               readonly=None, timeout=None, logical_plan=False,
                               expression_tree=False, rewritten_expression_tree=False,
                               job=False):
        """
        POST /api/v1/request
        Submit an async query request to Enterprise Analytics

        :param statement: SQL++ statement to execute
        :param mode: Query execution mode (must be "async")
        :param client_context_id: Optional client context ID
        :param format: Result format (default: "JSON")
        :param pretty: Whether to pretty print results
        :param query_context: Query context (e.g., "default:Default")
        :param readonly: Whether query is readonly
        :param timeout: Query timeout (e.g., "30s")
        :param logical_plan: Whether to include logical plan in response
        :param expression_tree: Whether to include expression tree in response
        :param rewritten_expression_tree: Whether to include rewritten expression tree
        :param job: Whether to include job information in response
        :return: tuple (status, content) where status is boolean and content is response dict
        """
        api = f"{self.cbas_url}/api/v1/request"
        headers = self.get_headers_for_content_type_json()
        params = {
            "statement": statement,
            "mode": mode
        }

        if client_context_id:
            params["client_context_id"] = client_context_id
        if format:
            params["format"] = format
        if pretty is not None:
            params["pretty"] = str(pretty).lower()
        if query_context:
            params["query_context"] = query_context
        if readonly is not None:
            params["readonly"] = str(readonly).lower()
        if timeout:
            params["timeout"] = timeout
        if logical_plan:
            params["logical-plan"] = "true"
        if expression_tree:
            params["expression-tree"] = "true"
        if rewritten_expression_tree:
            params["rewritten-expression-tree"] = "true"
        if job:
            params["job"] = "true"

        status, result, response = self.request(
            api, self.POST, headers=headers,
            params=json.dumps(params), timeout=300)

        if status and isinstance(result, str):
            try:
                result = json.loads(result)
            except (ValueError, TypeError):
                pass

        return status, result, response

    def get_request_status(self, request_id, handle):
        """
        GET /api/v1/request/status/{requestID}/{handle}
        Check the status of an async request

        :param request_id: Request ID returned from submit_async_request
        :param handle: Handle returned from submit_async_request (format: "jobId-partitionId")
        :return: tuple (status, content) where status is boolean and content is response dict
        """
        api = f"{self.cbas_url}/api/v1/request/status/{request_id}/{handle}"
        headers = self.create_headers()
        status, result, response = self.request(
            api, self.GET, headers=headers, timeout=300)

        if status and isinstance(result, str):
            try:
                result = json.loads(result)
            except (ValueError, TypeError):
                pass

        return status, result, response

    def get_request_result(self, request_id, handle, partition=None):
        """
        GET /api/v1/request/result/{requestID}/{handle}
        GET /api/v1/request/result/{requestID}/{handle}/{partition}
        Fetch results from a completed async request

        :param request_id: Request ID returned from submit_async_request
        :param handle: Handle returned from submit_async_request
        :param partition: Optional partition number to fetch results from specific partition
        :return: tuple (status, content) where status is boolean and content is response dict
        """
        if partition is not None:
            api = f"{self.cbas_url}/api/v1/request/result/{request_id}/{handle}/{partition}"
        else:
            api = f"{self.cbas_url}/api/v1/request/result/{request_id}/{handle}"

        headers = self.create_headers()
        status, result, response = self.request(
            api, self.GET, headers=headers, timeout=300)

        if status and isinstance(result, str):
            try:
                result = json.loads(result)
            except (ValueError, TypeError):
                pass

        return status, result, response

    def discard_request_result(self, request_id, handle):
        """
        DELETE /api/v1/request/result/{requestID}/{handle}
        Discard/delete the results of a completed async request

        :param request_id: Request ID returned from submit_async_request
        :param handle: Handle returned from submit_async_request
        :return: tuple (status, content) where status is boolean and content is response
        """
        api = f"{self.cbas_url}/api/v1/request/result/{request_id}/{handle}"
        headers = self.create_headers()
        status, result, response = self.request(
            api, self.DELETE, headers=headers, timeout=300)

        return status, result, response

    def wait_for_request_completion(self, request_id, handle, timeout=300, poll_interval=1):
        """
        Helper method to poll request status until completion

        :param request_id: Request ID returned from submit_async_request
        :param handle: Handle returned from submit_async_request
        :param timeout: Maximum time to wait in seconds
        :param poll_interval: Time between status checks in seconds
        :return: tuple (status, content) where status is boolean and content is final status response
        """
        import time
        start_time = time.time()

        while time.time() - start_time < timeout:
            status, result, response = self.get_request_status(
                request_id, handle)

            if not status:
                return status, result, response

            if isinstance(result, dict):
                request_status = result.get("status", "").lower()

                # Terminal states
                if request_status in ["success", "failed", "fatal", "timeout"]:
                    return status, result, response

                # Continue polling for queued/running states
                if request_status in ["queued", "running"]:
                    time.sleep(poll_interval)
                    continue

            # If we can't parse the response, return what we have
            return status, result, response

        # Timeout reached
        return False, {"error": "Timeout waiting for request completion"}, None
