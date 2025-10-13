import base64
import logging
import requests
import time

from cb_constants.CBServer import CbServer
from cb_constants.ClusterRun import ClusterRun


class CBRestConnection(object):
    DELETE = "DELETE"
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    PATCH = "PATCH"

    @staticmethod
    def get_auth(headers):
        key = 'Authorization'
        if key in headers:
            val = headers[key]
            if val.startswith("Basic "):
                val = val.encode()
                return str("auth: " + base64.decodebytes(val[6:]).decode())
        return ""

    @staticmethod
    def flatten_param_to_str(value):
        """
        Convert dict/list -> str
        """
        result = ""
        if isinstance(value, dict):
            result = '{'
            for key, val in value.items():
                if isinstance(val, dict) or isinstance(val, list):
                    result += '\"%s\":%s,' % (key, CBRestConnection.flatten_param_to_str(val))
                else:
                    try:
                        val = int(val)
                    except ValueError:
                        val = '\"%s\"' % val
                    result += '\"%s\":%s,' % (key, val)
            if value:
                result = result[:-1] + '}'
            else:
                result += '}'
        elif isinstance(value, list):
            result = '['
            for val in value:
                if isinstance(val, dict) or isinstance(val, list):
                    result += CBRestConnection.flatten_param_to_str(val) + ","
                else:
                    result += '"%s",' % val
            if value:
                result = result[:-1] + ']'
            else:
                result += ']'
        return result

    def set_server_values(self, server):
        self.ip = server.ip
        self.port = server.port
        self.username = server.rest_username
        self.password = server.rest_password
        self.type = "default"
        self.log = logging.getLogger("rest_api")

    def set_endpoint_urls(self, server):
        index_port = CbServer.index_port
        fts_port = CbServer.fts_port
        query_port = CbServer.n1ql_port
        eventing_port = CbServer.eventing_port
        backup_port = CbServer.backup_port
        cbas_port = CbServer.cbas_port
        hostname = None

        if hasattr(server, 'index_port') and server.index_port:
            index_port = server.index_port
        if hasattr(server, 'query_port') and server.query_port:
            query_port = server.query_port
        if hasattr(server, 'fts_port') and server.fts_port:
            fts_port = server.fts_port
        if hasattr(server, 'eventing_port') and server.eventing_port:
            eventing_port = server.eventing_port
        if hasattr(server, 'cbas_port') and server.cbas_port:
            cbas_port = server.cbas_port
        if hasattr(server, 'hostname') and server.hostname \
                and server.hostname.find(self.ip) == -1:
            hostname = server.hostname
        if hasattr(server, 'services'):
            self.services = server.services

        if CbServer.use_https:
            if ClusterRun.is_enabled:
                if int(self.port) < ClusterRun.ssl_port:
                    self.port = int(self.port) + 10000
            else:
                self.port = CbServer.ssl_port
                index_port = CbServer.ssl_index_port
                query_port = CbServer.ssl_n1ql_port
                fts_port = CbServer.ssl_fts_port
                eventing_port = CbServer.ssl_eventing_port
                backup_port = CbServer.ssl_backup_port
                cbas_port = CbServer.ssl_cbas_port

        http_url = "http://{0}:{1}"
        https_url = "https://{0}:{1}"
        generic_url = http_url
        if CbServer.use_https:
            generic_url = https_url
        url_host = "{0}".format(self.ip)
        if hostname:
            url_host = "{0}".format(hostname)

        self.base_url = generic_url.format(url_host, self.port)
        self.index_url = generic_url.format(url_host, index_port)
        self.query_url = generic_url.format(url_host, query_port)
        self.fts_url = generic_url.format(url_host, fts_port)
        self.eventing_url = generic_url.format(url_host, eventing_port)
        self.backup_url = generic_url.format(url_host, backup_port)
        self.cbas_url = generic_url.format(url_host, cbas_port)
        if hasattr(server, "type"):
            self.type = server.type

    @staticmethod
    def check_if_couchbase_is_active(rest, max_retry=5):
        api = rest.base_url + '/nodes/self'
        if rest.type not in ["default", "analytics"] or rest.type == "nebula":
            api = rest.base_url + "/pools/default"
        # for Node is unknown to this cluster error
        node_unknown_msg = "Node is unknown to this cluster"
        unexpected_server_err_msg = "Unexpected server error, request logged"
        headers = rest.create_headers(rest.username, rest.password,
                                      'application/json')
        for iteration in range(max_retry):
            content = None
            success = False
            try:
                status, content, header = rest.request(
                    api, CBRestConnection.GET, headers=headers, timeout=30)
                if status:
                    success = True
                else:
                    rest.log.warning(f"{api} with status {status}: {content}")
            except ValueError as e:
                rest.log.critical(e)
            if not success and type(content) in [bytes, str]\
                    and (content.find(node_unknown_msg) > -1
                         or content.find(unexpected_server_err_msg) > -1):
                rest.log.error("Error {0}, 5 seconds sleep before retry"
                               .format(content))
                time.sleep(5)
                if iteration == 2:
                    rest.log.error("Node {0}:{1} is in a broken state!"
                                   .format(rest.ip, rest.port))
                    raise Exception(f"Server {rest.ip} unreachable")
                continue
            else:
                break

    def __init__(self):
        """
        Contains the place-holders. Need to be initialized by the
        implementing *_api.py file / module
        """
        # Basic info about the server
        self.ip = None
        self.port = None
        self.username = None
        self.password = None
        self.type = None
        self.services = None

        # Valid URL endpoints for reusing
        self.base_url = None
        self.index_url = None
        self.query_url = None
        self.fts_url = None
        self.eventing_url = None
        self.backup_url = None

        # For tracking sessions (if any)
        self.session = None

        self.log = logging.getLogger("rest_api")

    def create_headers(self, username=None, password=None,
                       content_type='application/x-www-form-urlencoded'):
        username = username or self.username
        password = password or self.password
        authorization = base64.b64encode(
            '{}:{}'.format(username, password).encode()).decode()
        return {'Content-Type': content_type,
                'Authorization': 'Basic %s' % authorization,
                'Connection': 'close',
                'Accept': '*/*'}

    def get_headers_for_content_type_json(self):
        authorization = base64.b64encode(
            '{}:{}'.format(self.username, self.password).encode()).decode()
        return {'Content-type': 'application/json',
                'Authorization': 'Basic %s' % authorization}

    def request(self, api, method='GET', params='', headers=None,
                timeout=300, verify=False, session=None):
        """
        :param api:
        :param method:
        :param params:
        :param headers:
        :param timeout:
        :param verify:
        :param session:
        :return:
        """
        session = session or requests.Session()
        headers = headers or self.create_headers()
        end_time = time.time() + timeout
        while True:
            status = False
            try:
                request_args = {
                    "method": method,
                    "url": api,
                    "headers": headers,
                    "timeout": timeout,
                    "verify": verify,
                }
                if method.upper() == "GET" and params:
                    request_args["params"] = params  # Send as query params
                elif method.upper() != "GET":
                    request_args["data"] = params # Send as body payload

                response = session.request(**request_args)

                content = response.content
                if 200 <= response.status_code < 300:
                    status = True
                try:
                    content = response.json()
                except ValueError:
                    content
                    pass
                return status, content, response
            except requests.exceptions.HTTPError as errh:
                self.log.error("HTTPError {0}".format(errh))
            except requests.exceptions.ConnectionError as errc:
                if "Illegal state exception" in str(errc):
                    # Known ssl bug, retry
                    pass
                else:
                    self.log.debug("Error Connecting {0}".format(errc))
                if time.time() > end_time:
                    raise Exception(f"ServerUnavailableException - {self.ip}")
            except requests.exceptions.Timeout as errt:
                self.log.error("Timeout Error: {0}".format(errt))
                if time.time() > end_time:
                    raise Exception(f"ServerUnavailableException - {self.ip}")
            except requests.exceptions.RequestException as err:
                self.log.error("Something else: {0}".format(err))
                if time.time() > end_time:
                    raise Exception(f"ServerUnavailableException - {self.ip}")
            time.sleep(3)
