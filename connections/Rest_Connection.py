'''
Created on Sep 25, 2017

@author: riteshagarwal
'''
from membase.api import httplib2
import base64
import json
import logger
import traceback
import socket
import time
from TestInput import TestInputSingleton
from membase.api.exception import ServerUnavailableException
log = logger.Logger.get_logger()


class RestConnection(object):

    def __new__(self, serverInfo={}, node = None):
        # allow port to determine
        # behavior of restconnection
        port = None
        if isinstance(serverInfo, dict):
            if 'port' in serverInfo:
                port = serverInfo['port']
        else:
            port = serverInfo.port

        if not port:
            port = 8091

        if int(port) in xrange(9091, 9100):
            # return elastic search rest connection
            from membase.api.esrest_client import EsRestConnection
            obj = object.__new__(EsRestConnection, serverInfo)
        else:
            # default
            obj = object.__new__(self, serverInfo)
        return obj

    def __init__(self, serverInfo):
        # serverInfo can be a json object/dictionary
        if isinstance(serverInfo, dict):
            self.ip = serverInfo["ip"]
            self.username = serverInfo["username"]
            self.password = serverInfo["password"]
            self.port = serverInfo["port"]
            self.index_port = 9102
            self.fts_port = 8094
            self.query_port=8093
            if "index_port" in serverInfo.keys():
                self.index_port = serverInfo["index_port"]
            if "fts_port" in serverInfo.keys():
                if serverInfo['fts_port']:
                    self.fts_port = serverInfo["fts_port"]
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
            self.index_port = 9102
            self.fts_port = 8094
            self.query_port = 8093
            self.services = "kv"
            if hasattr(serverInfo, "services"):
                self.services = serverInfo.services
            if hasattr(serverInfo, 'index_port'):
                self.index_port = serverInfo.index_port
            if hasattr(serverInfo, 'query_port'):
                self.query_port = serverInfo.query_port
            if hasattr(serverInfo, 'fts_port'):
                if serverInfo.fts_port:
                    self.fts_port = serverInfo.fts_port
            if hasattr(serverInfo, 'hostname') and serverInfo.hostname and\
               serverInfo.hostname.find(self.ip) == -1:
                self.hostname = serverInfo.hostname
            if hasattr(serverInfo, 'services'):
                self.services = serverInfo.services
        self.input = TestInputSingleton.input
        if self.input is not None:
            """ from watson, services param order and format:
                new_services=fts-kv-index-n1ql """
            self.services_node_init = self.input.param("new_services", None)
        self.baseUrl = "http://{0}:{1}/".format(self.ip, self.port)
        if self.hostname:
            self.baseUrl = "http://{0}:{1}/".format(self.hostname, self.port)

        # for Node is unknown to this cluster error
        for iteration in xrange(5):
            http_res, success = self.init_http_request(self.baseUrl + 'nodes/self')
            if not success and type(http_res) == unicode and\
               (http_res.find('Node is unknown to this cluster') > -1 or \
                http_res.find('Unexpected server error, request logged') > -1):
                log.error("Error {0} was gotten, 5 seconds sleep before retry"\
                                                             .format(http_res))
                time.sleep(5)
                if iteration == 2:
                    log.error("node {0}:{1} is in a broken state!"\
                                        .format(self.ip, self.port))
                    raise ServerUnavailableException(self.ip)
                continue
            else:
                break
            
    def init_http_request(self, api):
        content = None
        try:
            status, content, header = self._http_request(api, 'GET',
                                                         headers=self._create_capi_headers())
            json_parsed = json.loads(content)
            if status:
                return json_parsed, True
            else:
                print("{0} with status {1}: {2}".format(api, status, json_parsed))
                return json_parsed, False
        except ValueError as e:
            if content is not None:
                print("{0}: {1}".format(api, content))
            else:
                print e
            return content, False
        
    def _create_capi_headers(self, username=None, password=None):
        if username==None:
            username = self.username
        if password==None:
            password = self.password            
        authorization = base64.encodestring('%s:%s' % (username, password))
        return {'Content-Type': 'application/json',
                'Authorization': 'Basic %s' % authorization,
                'Connection': 'close',
                'Accept': '*/*'}

    def _get_auth(self, headers):
        key = 'Authorization'
        if key in headers:
            val = headers[key]
            if val.startswith("Basic "):
                return "auth: " + base64.decodestring(val[6:])
        return ""
    
    def _http_request(self, api, method='GET', params='', headers=None, timeout=120):
        if not headers:
            headers = self._create_headers()
        end_time = time.time() + timeout
        while True:
            try:
                response, content = httplib2.Http(timeout=timeout).request(api, method, params, headers)
                if response['status'] in ['200', '201', '202']:
                    return True, content, response
                else:
                    try:
                        json_parsed = json.loads(content)
                    except ValueError as e:
                        json_parsed = {}
                        json_parsed["error"] = "status: {0}, content: {1}".format(response['status'], content)
                    reason = "unknown"
                    if "error" in json_parsed:
                        reason = json_parsed["error"]
                    message = '{0} {1} body: {2} headers: {3} error: {4} reason: {5} {6} {7}'.\
                              format(method, api, params, headers, response['status'], reason,
                                     content.rstrip('\n'), self._get_auth(headers))
                    log.error(message)
                    log.debug(''.join(traceback.format_stack()))
                    return False, content, response
            except socket.error as e:
                log.error("socket error while connecting to {0} error {1} ".format(api, e))
                if time.time() > end_time:
                    raise ServerUnavailableException(ip=self.ip)
            except httplib2.ServerNotFoundError as e:
                log.error("ServerNotFoundError error while connecting to {0} error {1} ".format(api, e))
                if time.time() > end_time:
                    raise ServerUnavailableException(ip=self.ip)
            time.sleep(3)

    def _create_headers(self):
        authorization = base64.encodestring('%s:%s' % (self.username, self.password))
        return {'Content-Type': 'application/x-www-form-urlencoded',
                'Authorization': 'Basic %s' % authorization,
                'Connection': 'close',
                'Accept': '*/*'}

