"""
Created on Sep 25, 2017

@author: riteshagarwal
"""
import base64
import json
import traceback
import socket
import time

from TestInput import TestInputSingleton
from cb_constants import constants, CbServer, ClusterRun
from common_lib import sleep
from global_vars import logger
from custom_exceptions.exception import ServerUnavailableException

import requests


class RestConnection(object):
    DELETE = "DELETE"
    GET = "GET"
    POST = "POST"
    PUT = "PUT"

    def __new__(self, serverInfo={}, node=None):
        # allow port to determine
        # behavior of rest connection
        self.log = logger.get("infra")
        port = None
        if isinstance(serverInfo, dict):
            if 'port' in serverInfo:
                port = serverInfo['port']
        else:
            port = serverInfo.port

        if not port:
            port = constants.port

        if int(port) in range(9091, 9100):
            # return elastic search rest connection
            from membase.api.esrest_client import EsRestConnection
            obj = object.__new__(EsRestConnection, serverInfo)
        else:
            # default
            obj = object.__new__(self)
        return obj

    def __init__(self, serverInfo, timeout=300):
        # serverInfo can be a json object/dictionary
        index_port = constants.index_port
        fts_port = constants.fts_port
        query_port = constants.n1ql_port
        eventing_port = constants.eventing_port
        backup_port = constants.backup_port
        if isinstance(serverInfo, dict):
            self.ip = serverInfo["ip"]
            self.username = serverInfo["username"]
            self.password = serverInfo["password"]
            self.port = serverInfo["port"]
            if "index_port" in serverInfo.keys():
                index_port = serverInfo["index_port"]
            if "fts_port" in serverInfo.keys():
                if serverInfo['fts_port']:
                    fts_port = serverInfo["fts_port"]
            if "eventing_port" in serverInfo.keys():
                if serverInfo['eventing_port']:
                    self.eventing_port = serverInfo["eventing_port"]
            self.hostname = ''
            self.services = ''
            if "hostname" in serverInfo:
                self.hostname = serverInfo["hostname"]
            if "services" in serverInfo:
                self.services = serverInfo["services"]
        else:
            self.ip = serverInfo.ip
            self.username = serverInfo.rest_username
            self.password = serverInfo.rest_password
            self.port = serverInfo.port
            self.hostname = ''
            self.services = "kv"
            if hasattr(serverInfo, "services"):
                self.services = serverInfo.services
            if hasattr(serverInfo, 'index_port') \
                    and serverInfo.index_port:
                index_port = serverInfo.index_port
            if hasattr(serverInfo, 'query_port') \
                    and serverInfo.query_port:
                query_port = serverInfo.query_port
            if hasattr(serverInfo, 'fts_port') \
                    and serverInfo.fts_port:
                fts_port = serverInfo.fts_port
            if hasattr(serverInfo, 'eventing_port') \
                    and serverInfo.eventing_port:
                self.eventing_port = serverInfo.eventing_port
            if hasattr(serverInfo, 'hostname') and serverInfo.hostname \
                    and serverInfo.hostname.find(self.ip) == -1:
                self.hostname = serverInfo.hostname
        try:
            self.type = serverInfo.type
        except:
            self.type = "default"
        if CbServer.use_https:
            if ClusterRun.is_enabled:
                if int(self.port) < ClusterRun.ssl_port:
                    self.port = int(self.port) + 10000
            else:
                if self.type != "columnar":
                    self.port = CbServer.ssl_port
                    index_port = CbServer.ssl_index_port
                    query_port = CbServer.ssl_n1ql_port
                    fts_port = CbServer.ssl_fts_port
                    eventing_port = CbServer.ssl_eventing_port
                    backup_port = CbServer.ssl_backup_port
        self.input = TestInputSingleton.input
        if self.input is not None:
            """ from watson, services param order and format:
                new_services=fts-kv-index-n1ql """
            self.services_node_init = self.input.param("new_services", None)

        http_url = "http://{0}:{1}/"
        https_url = "https://{0}:{1}/"
        generic_url = http_url
        if CbServer.use_https:
            generic_url = https_url
        url_host = "{0}".format(self.ip)
        if self.hostname:
            url_host = "{0}".format(self.hostname)

        self.baseUrl = generic_url.format(url_host, self.port)
        self.indexUrl = generic_url.format(url_host, index_port)
        self.queryUrl = generic_url.format(url_host, query_port)
        self.ftsUrl = generic_url.format(url_host, fts_port)
        self.eventing_baseUrl = generic_url.format(url_host, eventing_port)
        self.backup_url = generic_url.format(url_host, backup_port)
        if self.type != "columnar":
            if self.type != "default" or self.type == "nebula":
                nodes_self_url = self.baseUrl + "pools/default"
            else:
                nodes_self_url = self.baseUrl + 'nodes/self'
            # for Node is unknown to this cluster error
            node_unknown_msg = "Node is unknown to this cluster"
            unexpected_server_err_msg = "Unexpected server error, request logged"
            for iteration in range(5):
                http_res, success = \
                    self.init_http_request(nodes_self_url, timeout)
                if not success and type(http_res) == unicode \
                        and (http_res.find(node_unknown_msg) > -1
                             or http_res.find(unexpected_server_err_msg) > -1):
                    self.log.error("Error {0}, 5 seconds sleep before retry"
                                   .format(http_res))
                    sleep(5, log_type="infra")
                    if iteration == 2:
                        self.log.error("Node {0}:{1} is in a broken state!"
                                       .format(self.ip, self.port))
                        raise ServerUnavailableException(self.ip)
                    continue
                else:
                    break

    def init_http_request(self, api, timeout=300):
        content = None
        try:
            status, content, header = self._http_request(
                api, 'GET', headers=self._create_capi_headers(), timeout=timeout)
            json_parsed = json.loads(content)
            if status:
                return json_parsed, True
            else:
                self.log.debug("{0} with status {1}: {2}".format(api, status, json_parsed))
                return json_parsed, False
        except ValueError as e:
            if content is not None:
                print("{0}: {1}".format(api, content))
            else:
                print(e)
            return content, False

    def _create_capi_headers(self, username=None, password=None, contentType='application/json', connection='close'):
        if username is None:
            username = self.username
        if password is None:
            password = self.password
        authorization = base64.b64encode('{}:{}'.format(username, password).encode()).decode()
        return {'Content-Type': contentType,
                'Authorization': 'Basic %s' % authorization,
                'Connection': connection,
                'Accept': '*/*'}

    @staticmethod
    def get_auth(headers):
        key = 'Authorization'
        if key in headers:
            val = headers[key]
            if val.startswith("Basic "):
                return "auth: " + base64.decodestring(val[6:])
        return ""

    def urllib_request(self, api, method='GET', headers=None,
                       params={}, timeout=300, verify=False):
        session = requests.Session()
        headers = headers or self.get_headers_for_content_type_json()
        params = json.dumps(params)
        try:
            if method == "GET":
                resp = session.get(api, params=params, headers=headers,
                                   timeout=timeout, verify=verify)
            elif method == "POST":
                resp = session.post(api, data=params, headers=headers,
                                    timeout=timeout, verify=verify)
            elif method == "DELETE":
                resp = session.delete(api, data=params, headers=headers,
                                      timeout=timeout, verify=verify)
            elif method == "PUT":
                resp = session.put(api, data=params, headers=headers,
                                   timeout=timeout, verify=verify)
            return resp
        except requests.exceptions.HTTPError as errh:
            self.log.error("HTTP Error {0}".format(errh))
        except requests.exceptions.ConnectionError as errc:
            self.log.error("Error Connecting {0}".format(errc))
        except requests.exceptions.Timeout as errt:
            self.log.error("Timeout Error: {0}".format(errt))
        except requests.exceptions.RequestException as err:
            self.log.error("Something else: {0}".format(err))

    def _urllib_request(self, api, method='GET', params='', headers=None,
                        timeout=300, verify=False, session=None):
        if session is None:
            session = requests.Session()
        end_time = time.time() + timeout
        while True:
            try:
                if method == "GET":
                    response = session.get(api, params=params, headers=headers,
                                           timeout=timeout, verify=verify)
                elif method == "POST":
                    response = session.post(api, data=params, headers=headers,
                                            timeout=timeout, verify=verify)
                elif method == "DELETE":
                    response = session.delete(api, data=params, headers=headers,
                                              timeout=timeout, verify=verify)
                elif method == "PUT":
                    response = session.put(api, data=params, headers=headers,
                                           timeout=timeout, verify=verify)
                elif method == "PATCH":
                    response = session.patch(api, data=params, headers=headers,
                                             timeout=timeout, verify=verify)
                status = response.status_code
                content = response.content
                if status in [200, 201, 202, 204]:
                    return True, content, response
                else:
                    try:
                        json_parsed = json.loads(content)
                    except ValueError:
                        json_parsed = dict()
                        json_parsed["error"] = "status: {0}, content: {1}".format(
                            response.status_code, content)
                    reason = "unknown"
                    if "error" in json_parsed:
                        reason = json_parsed["error"]
                    elif "errors" in json_parsed:
                        reason = json_parsed["errors"]
                    if ("accesskey" in params.lower()) or (
                            "secretaccesskey" in params.lower()) or (
                            "password" in params.lower()) or (
                            "secretkey" in params.lower()):
                        message = '{0} {1} body: {2} headers: {3} ' \
                                  'error: {4} reason: {5} {6} {7}'. \
                            format(method, api,
                                   "Body is being redacted because it contains sensitive info",
                                   headers, response.status_code, reason,
                                   content.rstrip('\n'),
                                   RestConnection.get_auth(headers))
                    else:
                        message = '{0} {1} body: {2} headers: {3} ' \
                                  'error: {4} reason: {5} {6} {7}'. \
                            format(method, api, params, headers,
                                   response.status_code, reason,
                                   content.rstrip('\n'),
                                   RestConnection.get_auth(headers))
                    self.log.debug(message)
                    self.log.debug(''.join(traceback.format_stack()))
                    return False, content, response
            except requests.exceptions.HTTPError as errh:
                self.log.error("HTTP Error {0}".format(errh))
            except requests.exceptions.ConnectionError as errc:
                if "Illegal state exception" in str(errc):
                    # Known ssl bug, retry
                    pass
                else:
                    self.log.debug("Error Connecting {0}".format(errc))
                if time.time() > end_time:
                    raise ServerUnavailableException(ip=self.ip)
            except requests.exceptions.Timeout as errt:
                self.log.error("Timeout Error: {0}".format(errt))
                if time.time() > end_time:
                    raise ServerUnavailableException(ip=self.ip)
            except requests.exceptions.RequestException as err:
                self.log.error("Something else: {0}".format(err))
                if time.time() > end_time:
                    raise ServerUnavailableException(ip=self.ip)
            sleep(3, log_type="infra")

    def _http_request(self, api, method='GET', params='', headers=None,
                      timeout=60):
        if not headers:
            headers = self._create_headers()
        if CbServer.use_https:
            status, content, response = \
                self._urllib_request(api, method=method, params=params, headers=headers,
                                     timeout=timeout, verify=False)
            return status, content, response
        end_time = time.time() + timeout
        while True:
            try:
                response, content = httplib2.Http(timeout=timeout).request(
                    api, method, params, headers)
                if response.status in [200, 201, 202, 204]:
                    return True, content, response
                else:
                    try:
                        json_parsed = json.loads(content)
                    except ValueError:
                        json_parsed = dict()
                        json_parsed["error"] = "status: {0}, content: {1}" \
                            .format(response['status'], content)
                    reason = "unknown"
                    if "error" in json_parsed:
                        reason = json_parsed["error"]
                    if ("accesskey" in params.lower()) or ("secretaccesskey" in params.lower()) or (
                            "password" in params.lower()) or ("secretkey" in params.lower()):
                        message = '{0} {1} body: {2} headers: {3} ' \
                                  'error: {4} reason: {5} {6} {7}'. \
                            format(method, api, "Body is being redacted because it contains sensitive info", headers,
                                   response['status'], reason,
                                   content.rstrip('\n'),
                                   RestConnection.get_auth(headers))
                    else:
                        message = '{0} {1} body: {2} headers: {3} ' \
                                  'error: {4} reason: {5} {6} {7}'. \
                            format(method, api, params, headers,
                                   response['status'], reason,
                                   content.rstrip('\n'),
                                   RestConnection.get_auth(headers))
                    self.log.debug(message)
                    self.log.debug(''.join(traceback.format_stack()))
                    return False, content, response
            except socket.error as e:
                self.log.error("Socket error while connecting to {0}. "
                               "Error {1}".format(api, e))
                if time.time() > end_time:
                    raise ServerUnavailableException(ip=self.ip)
            except Exception as e:
                self.log.error("ServerNotFoundError while connecting to {0}. "
                               "Error {1}".format(api, e))
                if time.time() > end_time:
                    raise ServerUnavailableException(ip=self.ip)
            sleep(3, log_type="infra")

    def _create_headers(self, username=None, password=None):
        if username is None:
            username = self.username
        if password is None:
            password = self.password
        authorization = base64.b64encode('{}:{}'.format(username, password).encode()).decode()
        return {'Content-Type': 'application/x-www-form-urlencoded',
                'Authorization': 'Basic %s' % authorization,
                'Connection': 'close',
                'Accept': '*/*'}

    def get_headers_for_content_type_json(self):
        authorization = base64.b64encode('{}:{}'.format(self.username, self.password).encode()).decode()
        return {'Content-type': 'application/json',
                'Authorization': 'Basic %s' % authorization}
