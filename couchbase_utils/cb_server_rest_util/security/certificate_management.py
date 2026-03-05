import json

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

    def get_client_cert_auth_config(self):
        """
        Retrieve current client certificate authentication configuration.

        Returns:
            dict: Configuration object or None if retrieval fails
        """
        url = "/settings/clientCertAuth"
        api = self.base_url + url

        try:
            status, content, _ = self.request(api, self.GET)

            self.log.info(f"GET settings/clientCertAuth status: {status}")

            if status and content:
                if isinstance(content, (str, bytes)):
                    try:
                        return json.loads(content)
                    except (ValueError, TypeError):
                        self.log.warning(f"Failed to parse content as JSON: {content}")
                        return None
                elif isinstance(content, dict):
                    return content
                else:
                    self.log.warning(f"Unexpected content type: {type(content)}")
                    return None
            else:
                self.log.warning(f"Failed to retrieve client cert config with status: {status}")
                return None
        except Exception as e:
            self.log.error(f"Exception while retrieving client cert config: {e}")
            return None

    def set_client_cert_auth_config(self, state, prefixes=None):
        """
        Upload client certificate authentication configuration.

        Args:
            state: 'enable' or 'disable'
            prefixes: List of prefix dictionaries (optional)

        Returns:
            tuple: (status, content, headers)
        """
        url = "/settings/clientCertAuth"
        api = self.base_url + url

        payload = {
            "state": state,
            "prefixes": prefixes if prefixes is not None else []
        }

        headers = self.get_headers_for_content_type_json()
        return self.request(api, self.POST, json.dumps(payload), headers=headers)

    def cleanup_client_cert_auth(self):
        """
        Disable client certificate authentication configuration.

        Returns:
            tuple: (status, content, headers)
        """
        return self.set_client_cert_auth_config(state='disable', prefixes=[])

    def generate_client_cert_prefixes_json(self, num_prefixes):
        """
        Generate prefix entries for client certificate configuration.

        Valid path types for client certificate authentication:
        - subject.cn: Certificate Common Name
        - san.dnsname: Subject Alternative Name (DNS)
        - san.uri: Subject Alternative Name (URI)

        Creates a list of prefix dictionaries in the format:
        [
            {"path": "subject.cn", "prefix": "CN=client0", "delimiter": "."},
            {"path": "san.dnsname", "prefix": "CN=client1", "delimiter": "."},
            {"path": "san.uri", "prefix": "CN=client2", "delimiter": "."},
            ...
        ]

        Args:
            num_prefixes: Number of prefix entries to generate

        Returns:
            list: List of prefix dictionaries
        """
        path_types = ['subject.cn', 'san.dnsname', 'san.uri']
        prefixes = []

        for i in range(num_prefixes):
            path = path_types[i % len(path_types)]
            prefix = f'CN=client{i}'
            delimiter = '.'

            prefixes.append({
                "path": path,
                "prefix": prefix,
                "delimiter": delimiter
            })

        return prefixes
