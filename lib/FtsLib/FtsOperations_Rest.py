import json
import requests

from connections.Rest_Connection import RestConnection


class CBASHelper(RestConnection):
    def __init__(self, fts_node):
        super(CBASHelper, self).__init__(fts_node)

    def create_fts_index_from_json(self, index_name, param_data):
        api = self.ftsUrl + "api/index/%s" % index_name
        json_header = self.get_headers_for_content_type_json()
        status, content, _ = self._http_request(api, "PUT",
                                                params=param_data,
                                                headers=json_header,
                                                timeout=60)
        return status, content

    def delete_fts_index(self, index_name):
        api = self.ftsUrl + "api/index/%s" % index_name
        status, content, _ = self._http_request(api, "DELETE",
                                                timeout=60)
        return status, content
