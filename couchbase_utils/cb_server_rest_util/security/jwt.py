import json

from cb_server_rest_util.connection import CBRestConnection


class JWTAPI(CBRestConnection):
    """JWT Authentication REST API for Couchbase Server"""

    def __init__(self):
        super(JWTAPI, self).__init__()

    def put_jwt_config(self, config):
        """
        PUT JWT configuration to the cluster.

        Args:
            config: JWT configuration dictionary

        Returns:
            tuple: (status, content, response)
        """
        url = "/settings/jwt"
        api = self.base_url + url
        headers = self.get_headers_for_content_type_json()
        body = json.dumps(config)

        status, content, response = self.request(api, self.PUT, body, headers=headers)

        if not status:
            self.log.error(f"Failed to PUT JWT config: status={status}, content={content}")

        return status, content, response

    def get_jwt_config(self, expected_status_code=200):
        """
        GET JWT configuration from cluster.

        Args:
            expected_status_code: Expected HTTP status code (default: 200)

        Returns:
            tuple: (status, content, response)
        """
        url = "/settings/jwt"
        api = self.base_url + url

        status, content, response = self.request(api, self.GET)

        if expected_status_code is not None and response is not None:
            actual_status = getattr(response, 'status_code', response.status)
            if actual_status is not None:
                assert int(actual_status) == int(expected_status_code), (
                    f"Expected GET /settings/jwt status {expected_status_code}, got {actual_status}"
                )

        self.log.info(f"GET /settings/jwt: status={status}")

        return status, content, response

    def disable_jwt(self):
        """
        Disable JWT authentication on the cluster.
        Returns:
            tuple: (status, content, response)
        """
        try:
            status, content, response = self.get_jwt_config(expected_status_code=None)
            if status and content:
                config = json.loads(content) if isinstance(content, str) else content
                config["enabled"] = False
                if "issuers" not in config:
                    config["issuers"] = []
                return self.put_jwt_config(config)
        except Exception as e:
            self.log.warning(f"GET before disable failed: {e}")
        return self.put_jwt_config({"enabled": False, "issuers": []})
