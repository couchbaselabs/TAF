from connections.Rest_Connection import RestConnection
import json


class FtsHelper(RestConnection):
    def __init__(self, fts_node):
        super(FtsHelper, self).__init__(fts_node)

    def create_fts_index_from_json(self, index_name, param_data):
        api = self.ftsUrl + "api/index/%s" % index_name
        json_header = self.get_headers_for_content_type_json()
        status, content, _ = self._http_request(api, "PUT",
                                                params=param_data,
                                                headers=json_header,
                                                timeout=60)
        return status, content

    def run_fts_query_curl(self, index_name, param_data):
        api = self.ftsUrl + "api/index/%s/query" % index_name
        json_header = self.get_headers_for_content_type_json()
        status, content, _ = self._urllib_request(api, "POST",
                                                params=param_data,
                                                headers=json_header,
                                                timeout=120)
        return status, content

    def delete_fts_index(self, index_name):
        api = self.ftsUrl + "api/index/%s" % index_name
        status, content, _ = self._http_request(api, "DELETE",
                                                timeout=60)
        return status, content

    def fts_index_item_count(self, index_name):
        api = self.ftsUrl + "api/index/%s/count" % index_name
        status, content, _ = self._http_request(api, "GET",
                                                timeout=60)
        return status, content

    def capture_memory_profile(self):
        api = self.ftsUrl + "runtime/profile/memory"
        status, content, _ = self._http_request(api, "POST",
                                                timeout=60)
        return status, content
