'''
Created on Sep 25, 2017

@author: riteshagarwal
'''
from connections.Rest_Connection import RestConnection
import logger
import base64
import json
import urllib
from membase.api import httplib2

log = logger.Logger.get_logger()

class CBASHelper(RestConnection):
    
    def __init__(self, master, cbas_node):
        super(CBASHelper, self).__init__(cbas_node)
        self.cbas_base_url = "http://{0}:{1}".format(self.ip, 8095)

    def createConn(self, bucket, username, password):
        pass
    
    def closeConn(self):
        pass
    
    def execute_statement_on_cbas(self, statement, mode, pretty=True,
                                  timeout=70, client_context_id=None,
                                  username=None, password=None,analytics_timeout=120, time_out_unit="s"):
        if not username:
            username = self.username
        if not password:
            password = self.password
        api = self.cbas_base_url + "/analytics/service"
        headers = self._create_capi_headers(username, password)

        params = {'statement': statement, 'mode': mode, 'pretty': pretty,
                  'client_context_id': client_context_id, 'timeout': str(analytics_timeout) + time_out_unit}
        params = json.dumps(params)
        status, content, header = self._http_request(api, 'POST',
                                                     headers=headers,
                                                     params=params,
                                                     timeout=timeout)
        if status:
            return content
        elif str(header['status']) == '503':
            log.info("Request Rejected")
            raise Exception("Request Rejected")
        elif str(header['status']) in ['500','400']:
            json_content = json.loads(content)
            msg = json_content['errors'][0]['msg']
            if "Job requirement" in  msg and "exceeds capacity" in msg:
                raise Exception("Capacity cannot meet job requirement")
            else:
                return content
        else:
            log.error("/analytics/service status:{0},content:{1}".format(
                status, content))
            raise Exception("Analytics Service API failed")

    def execute_parameter_statement_on_cbas(self, statement, mode, pretty=True, timeout=70, client_context_id=None,
                                            username=None, password=None, analytics_timeout=120, parameters=[]):
        if not username:
            username = self.username
        if not password:
            password = self.password
        api = self.cbas_base_url + "/analytics/service"
        headers = self._create_capi_headers(username, password)
        params = {'statement': statement, 'mode': mode, 'pretty': pretty, 'client_context_id': client_context_id,
                  'timeout': str(analytics_timeout) + "s"}
        for i in range(len(parameters)):
            params.update(parameters[i])
        params = json.dumps(params)
        status, content, header = self._http_request(api, 'POST', headers=headers, params=params, timeout=timeout)
        # print("cbas response:{}".format(content))
        if status:
            return content
        elif str(header['status']) == '503':
            log.info("Request Rejected")
            raise Exception("Request Rejected")
        elif str(header['status']) in ['500', '400']:
            json_content = json.loads(content)
            msg = json_content['errors'][0]['msg']
            if "Job requirement" in msg and "exceeds capacity" in msg:
                raise Exception("Capacity cannot meet job requirement")
            else:
                return content
        else:
            log.error("/analytics/service status:{0},content:{1}".format(status, content))
            raise Exception("Analytics Service API failed")

    def delete_active_request_on_cbas(self, payload, username=None, password=None):
        if not username:
            username = self.username
        if not password:
            password = self.password

        api = self.cbas_base_url + "/analytics/admin/active_requests"
        status, content, header = self._http_request(api, 'DELETE', params=payload, timeout=60)
        if status:
            return header['status']
        elif str(header['status']) == '404':
            log.info("Request Not Found")
            return header['status']
        else:
            log.error(
                "/analytics/admin/active_requests status:{0},content:{1}".format(
                    status, content))
            raise Exception("Analytics Admin API failed")
    
    def analytics_tool(self, query, port=8095, timeout=650, query_params={}, is_prepared=False, named_prepare=None,
                   verbose = True, encoded_plan=None, servers=None):
        key = 'prepared' if is_prepared else 'statement'
        headers = None
        content=""
        prepared = json.dumps(query)
        if is_prepared:
            if named_prepare and encoded_plan:
                http = httplib2.Http()
                if len(servers)>1:
                    url = "http://%s:%s/query/service" % (servers[1].ip, port)
                else:
                    url = "http://%s:%s/query/service" % (self.ip, port)

                headers = {'Content-type': 'application/json'}
                body = {'prepared': named_prepare, 'encoded_plan':encoded_plan}

                response, content = http.request(url, 'POST', headers=headers, body=json.dumps(body))

                return eval(content)

            elif named_prepare and not encoded_plan:
                params = 'prepared=' + urllib.quote(prepared, '~()')
                params = 'prepared="%s"'% named_prepare
            else:
                prepared = json.dumps(query)
                prepared = str(prepared.encode('utf-8'))
                params = 'prepared=' + urllib.quote(prepared, '~()')
            if 'creds' in query_params and query_params['creds']:
                headers = self._create_headers_with_auth(query_params['creds'][0]['user'].encode('utf-8'),
                                                         query_params['creds'][0]['pass'].encode('utf-8'))
            api = "%s/analytics/service?%s" % (self.cbas_base_url, params)
            log.info("%s"%api)
        else:
            params = {key : query}
            if 'creds' in query_params and query_params['creds']:
                headers = self._create_headers_with_auth(query_params['creds'][0]['user'].encode('utf-8'),
                                                         query_params['creds'][0]['pass'].encode('utf-8'))
                del query_params['creds']
            params.update(query_params)
            params = urllib.urlencode(params)
            if verbose:
                log.info('query params : {0}'.format(params))
            api = "%s/analytics/service?%s" % (self.cbas_base_url, params)
        status, content, header = self._http_request(api, 'POST', timeout=timeout, headers=headers)
        try:
            return json.loads(content)
        except ValueError:
            return content

    def operation_log_level_on_cbas(self, method, params=None, logger_name=None, log_level=None, timeout=120, username=None,
                                    password=None):
        if not username:
            username = self.username
        if not password:
            password = self.password
        headers = self._create_capi_headers(username, password)
        
        if params is not None:
            api = self.cbas_base_url + "/analytics/cluster/logging"
        else:
            api = self.cbas_base_url + "/analytics/cluster/logging/" + logger_name
        
        # In case of SET action we can set logging level of a specific logger and pass log_level as text string in body 
        if log_level:
           params = log_level    

        status, content, response = self._http_request(api, method=method, params=params, headers=headers,
                                                       timeout=timeout)
        return status, content, response

    def operation_config_on_cbas(self, method="GET", params=None, username=None, password=None):
        if not username:
            username = self.username
        if not password:
            password = self.password
        headers = self._create_capi_headers(username, password)
        api = self.cbas_base_url + "/analytics/node/config"
        status, content, response = self._http_request(api, method=method, params=params, headers=headers)
        return status, content, response

    def restart_cbas(self, username=None, password=None):
        if not username:
            username = self.username
        if not password:
            password = self.password
        headers = self._create_capi_headers(username, password)
        api = self.cbas_base_url + "/analytics/cluster/restart"
        status, content, response = self._http_request(api, method="POST", headers=headers)
        return status, content, response

    def fetch_cbas_stats(self, username=None, password=None):
        if not username:
            username = self.username
        if not password:
            password = self.password
        headers = self._create_capi_headers(username, password)
        cbas_base_url = "http://{0}:{1}".format(self.ip, 9110)
        api = cbas_base_url + "/analytics/node/stats"
        status, content, response = self._http_request(api, method="GET", headers=headers)
        return status, content, response
    
    def operation_service_parameters_configuration_cbas(self, method="GET", params=None, username=None, password=None):
        if not username:
            username = self.username
        if not password:
            password = self.password
        headers = self._create_capi_headers(username, password)
        cbas_base_url = "http://{0}:{1}".format(self.ip, 8095)
        api = cbas_base_url + "/analytics/config/service"
        status, content, response = self._http_request(api, method=method, params=params, headers=headers)
        return status, content, response
    
    def operation_node_parameters_configuration_cbas(self, method="GET", params=None, username=None, password=None):
        if not username:
            username = self.username
        if not password:
            password = self.password
        headers = self._create_capi_headers(username, password)
        cbas_base_url = "http://{0}:{1}".format(self.ip, 8095)
        api = cbas_base_url + "/analytics/config/node"
        status, content, response = self._http_request(api, method=method, params=params, headers=headers)
        return status, content, response
    
    def restart_analytics_cluster_uri(self, username=None, password=None):
        if not username:
            username = self.username
        if not password:
            password = self.password
        headers = self._create_capi_headers(username, password)
        api = self.cbas_base_url + "/analytics/cluster/restart"
        status, content, response = self._http_request(api, method="POST", headers=headers)
        return status, content, response

    def restart_analytics_node_uri(self, node_ip, port=8095, username=None, password=None):
        if not username:
            username = self.username
        if not password:
            password = self.password
        headers = self._create_capi_headers(username, password)
        node_url = "http://{0}:{1}".format(node_ip, port)
        api = node_url + "/analytics/node/restart"
        status, content, response = self._http_request(api, method="POST", headers=headers)
        return status, content, response
    
    def fetch_bucket_state_on_cbas(self, method="GET", username=None, password=None):
        if not username:
            username = self.username
        if not password:
            password = self.password
        headers = self._create_capi_headers(username, password)
        api = self.cbas_base_url + "/analytics/buckets"
        status, content, response = self._http_request(api, method=method, headers=headers)
        return status, content, response

    def fetch_pending_mutation_on_cbas_node(self, node_ip, port=9110, method="GET", username="None", password="None"):
        if not username:
            username = self.username
        if not password:
            password = self.password
        headers = self._create_capi_headers(username, password)
        node_url = "http://{0}:{1}".format(node_ip, port)
        api = node_url + "/analytics/node/stats"
        status, content, response = self._http_request(api, method=method, headers=headers)
        return status, content, response

    def fetch_pending_mutation_on_cbas_cluster(self, port=9110, method="GET", username="None", password="None"):
        if not username:
            username = self.username
        if not password:
            password = self.password
        headers = self._create_capi_headers(username, password)
        node_url = "http://{0}:{1}".format(self.ip, port)
        api = node_url + "/analytics/node/agg/stats/remaining"
        status, content, response = self._http_request(api, method=method, headers=headers)
        return status, content, response
    
    def fetch_dcp_state_on_cbas(self, dataset,  method="GET", dataverse="Default", username=None, password=None):
        if not username:
            username = self.username
        if not password:
            password = self.password
        headers = self._create_capi_headers(username, password)
        api = self.cbas_base_url + "/analytics/dataset/dcp/{0}/{1}".format(dataverse, dataset)
        status, content, response = self._http_request(api, method=method, headers=headers)
        return status, content, response

    # return analytics diagnostics info
    def get_analytics_diagnostics(self, cbas_node, timeout=120):
        analyticsBaseUrl = "http://{0}:{1}/".format(cbas_node.ip, 8095)
        api = analyticsBaseUrl + 'analytics/node/diagnostics'
        status, content, header = self._http_request(api, timeout=timeout)
        if status:
            json_parsed = json.loads(content)
            return json_parsed
        else:
            raise Exception("Unable to get jre path from analytics")