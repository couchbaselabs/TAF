"""
https://docs.couchbase.com/server/current/eventing-rest-api/index.html
"""

import json
from cb_server_rest_util.connection import CBRestConnection

class EventingFunctions(CBRestConnection):
    def __init__(self):
        super(EventingFunctions).__init__()

    def create_function(self, name, body):
        """
        POST :: /api/v1/functions/{name}
        docs.couchbase.com/server/current/eventing-rest-api/index.html
        """
        api = f"{self.eventing_url}/api/v1/functions/{name}"
        headers = self.create_headers(content_type="application/json")
        status, content, _ = self.request(api, self.POST,
                                          params=json.dumps(body).encode("ascii", "ignore"),
                                          headers=headers)
        return status, content

    def deploy_eventing_function(self, name):
        """
        POST :: api/v1/functions/{name}/deploy
        docs.couchbase.com/server/current/eventing-rest-api/index.html
        """
        api = f"{self.eventing_url}/api/v1/functions/{name}/deploy"
        headers = self.create_headers(content_type="application/json")
        status, content, _ = self.request(api, self.POST, headers=headers)
        return status, content

    def pause_eventing_function(self, name):
        """
        POST :: api/v1/functions/{name}/pause
        docs.couchbase.com/server/current/eventing-rest-api/index.html
        """
        api = f"{self.eventing_url}/api/v1/functions/{name}/pause"
        headers = self.create_headers(content_type="application/json")
        status, content, _ = self.request(api, self.POST, headers=headers)
        return status, content

    def resume_eventing_function(self, name):
        """
        POST :: api/v1/functions/{name}/resume
        docs.couchbase.com/server/current/eventing-rest-api/index.html
        """
        api = f"{self.eventing_url}/api/v1/functions/{name}/resume"
        headers = self.create_headers(content_type="application/json")
        status, content, _ = self.request(api, self.POST, headers=headers)
        return status, content

    def update_function(self, name, body):
        """
        POST :: api/v1/functions/{name}
        docs.couchbase.com/server/current/eventing-rest-api/index.html
        """
        api = f"{self.eventing_url}/api/v1/functions/{name}"
        headers = self.create_headers(content_type="application/json")
        body['appname'] = name
        status, content, _ = self.request(api, self.POST,
                                          params=json.dumps(body).encode("ascii", "ignore"),
                                          headers=headers)
        return status, content

    def get_function_details(self, name):
        """
        GET :: api/v1/functions/{name}
        docs.couchbase.com/server/current/eventing-rest-api/index.html
        """
        api = f"{self.eventing_url}/api/v1/functions/{name}"
        status, content, _ = self.request(api, self.GET)
        return status, content

    def get_list_of_eventing_functions(self):
        """
        GET :: api/v1/list/functions
        docs.couchbase.com/server/current/eventing-rest-api/index.html
        """
        api = f"{self.eventing_url}/api/v1/list/functions"
        status, content, _ = self.request(api, self.GET)
        return status, content