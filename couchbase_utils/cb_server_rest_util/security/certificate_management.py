from cb_server_rest_util.connection import CBRestConnection


class CertificateMangementAPI(CBRestConnection):
    def __init__(self):
        super(CertificateMangementAPI, self).__init__()

    def get_trusted_root_certificates(self):
        """
        GET:: /pools/default/trustedCAs
        https://docs.couchbase.com/server/current/rest-api/deprecated-security-apis/upload-retrieve-root-cert.html#examples
        """
        url = "/pools/default/trustedCAs"
        api = self.base_url + url
        status, content, _ = self.request(api, self.GET)
        return status, content

    def get_node_certificates(self):
        """
        GET:: /pools/default/certificates
        https://docs.couchbase.com/server/current/rest-api/deprecated-security-apis/upload-retrieve-root-cert.html#examples
        """
        url = "/pools/default/certificates"
        api = self.base_url + url
        status, content, _ = self.request(api, self.GET)
        return status, content
