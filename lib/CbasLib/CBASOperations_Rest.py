"""
Created on Sep 25, 2017

@author: riteshagarwal
"""

import json
import urllib
import requests

from connections.Rest_Connection import RestConnection
from membase.api import httplib2


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
                                  username=None, password=None,
                                  analytics_timeout=120, time_out_unit="s",
                                  scan_consistency=None, scan_wait=None):
        if not username:
            username = self.username
        if not password:
            password = self.password
        api = self.cbas_base_url + "/analytics/service"
        headers = self._create_capi_headers(username, password)

        params = {'statement': statement, 'pretty': pretty,
                  'client_context_id': client_context_id,
                  'timeout': str(analytics_timeout) + time_out_unit}

        if mode is not None:
            params['mode'] = mode

        if scan_consistency is not None:
            params['scan_consistency'] = scan_consistency

        if scan_wait is not None:
            params['scan_wait'] = scan_wait
        params = json.dumps(params)
        status, content, header = self._http_request(
            api, 'POST', headers=headers, params=params, timeout=timeout)
        if status:
            return content
        elif str(header['status']) == '503':
            self.log.info("Request Rejected")
            raise Exception("Request Rejected")
        elif str(header['status']) in ['500', '400', '401', '403']:
            json_content = json.loads(content)
            msg = json_content['errors'][0]['msg']
            if "Job requirement" in  msg and "exceeds capacity" in msg:
                raise Exception("Capacity cannot meet job requirement")
            else:
                return content
        else:
            self.log.error("/analytics/service status:{0}, content:{1}"
                           .format(status, content))
            raise Exception("Analytics Service API failed")

    def execute_parameter_statement_on_cbas(self, statement, mode, pretty=True,
                                            timeout=70, client_context_id=None,
                                            username=None, password=None,
                                            analytics_timeout=120,
                                            parameters={}):
        if not username:
            username = self.username
        if not password:
            password = self.password
        api = self.cbas_base_url + "/analytics/service"
        headers = self._create_capi_headers(username, password)

        params = {'statement': statement, 'pretty': pretty,
                  'client_context_id': client_context_id,
                  'timeout': str(analytics_timeout) + "s"}
        if mode is not None:
            params['mode'] = mode
        params.update(parameters)

        params = json.dumps(params)
        status, content, header = self._http_request(
            api, 'POST', headers=headers, params=params, timeout=timeout)
        if status:
            return content
        elif str(header['status']) == '503':
            self.log.info("Request Rejected")
            raise Exception("Request Rejected")
        elif str(header['status']) in ['500', '400', '401', '403']:
            json_content = json.loads(content)
            msg = json_content['errors'][0]['msg']
            if "Job requirement" in msg and "exceeds capacity" in msg:
                raise Exception("Capacity cannot meet job requirement")
            else:
                return content
        else:
            self.log.error("/analytics/service status:{0}, content:{1}"
                           .format(status, content))
            raise Exception("Analytics Service API failed")

    def delete_active_request_on_cbas(self, payload, username=None,
                                      password=None):
        if not username:
            username = self.username
        if not password:
            password = self.password

        api = self.cbas_base_url + "/analytics/admin/active_requests"
        status, content, header = self._http_request(
            api, 'DELETE',params=payload, timeout=60)
        if status:
            return header['status']
        elif str(header['status']) == '404':
            self.log.info("Request Not Found")
            return header['status']
        else:
            self.log.error("/analytics/admin/active_requests "
                           "status:{0}, content:{1}"
                           .format(status, content))
            raise Exception("Analytics Admin API failed")

    def analytics_tool(self, query, port=8095, timeout=650, query_params={},
                       is_prepared=False, named_prepare=None,
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

                response, content = http.request(
                    url, 'POST', headers=headers, body=json.dumps(body))

                return eval(content)

            elif named_prepare and not encoded_plan:
                params = 'prepared=' + urllib.quote(prepared, '~()')
                params = 'prepared="%s"'% named_prepare
            else:
                prepared = json.dumps(query)
                prepared = str(prepared.encode('utf-8'))
                params = 'prepared=' + urllib.quote(prepared, '~()')
            if 'creds' in query_params and query_params['creds']:
                headers = self._create_headers_with_auth(
                    query_params['creds'][0]['user'].encode('utf-8'),
                    query_params['creds'][0]['pass'].encode('utf-8'))
            api = "%s/analytics/service?%s" % (self.cbas_base_url, params)
            self.log.info("%s" % api)
        else:
            params = {key : query}
            if 'creds' in query_params and query_params['creds']:
                headers = self._create_headers_with_auth(
                    query_params['creds'][0]['user'].encode('utf-8'),
                    query_params['creds'][0]['pass'].encode('utf-8'))
                del query_params['creds']
            params.update(query_params)
            params = urllib.urlencode(params)
            if verbose:
                self.log.info('Query params: {0}'.format(params))
            api = "%s/analytics/service?%s" % (self.cbas_base_url, params)
        status, content, header = self._http_request(
            api, 'POST', timeout=timeout, headers=headers)
        try:
            return json.loads(content)
        except ValueError:
            return content

    def operation_log_level_on_cbas(self, method, params=None,
                                    logger_name=None, log_level=None,
                                    timeout=120, username=None,
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

        # In case of SET action we can set logging level of a specific logger
        # and pass log_level as text string in body
        if log_level:
           params = log_level

        status, content, response = self._http_request(
            api, method=method, params=params, headers=headers,
            timeout=timeout)
        return status, content, response

    def operation_config_on_cbas(self, method="GET", params=None,
                                 username=None, password=None):
        if not username:
            username = self.username
        if not password:
            password = self.password
        headers = self._create_capi_headers(username, password)
        api = self.cbas_base_url + "/analytics/node/config"
        status, content, response = self._http_request(
            api, method=method, params=params, headers=headers)
        return status, content, response

    def restart_cbas(self, username=None, password=None):
        if not username:
            username = self.username
        if not password:
            password = self.password
        headers = self._create_capi_headers(username, password)
        api = self.cbas_base_url + "/analytics/cluster/restart"
        status, content, response = self._http_request(
            api, method="POST", headers=headers)
        return status, content, response

    def fetch_cbas_stats(self, username=None, password=None):
        if not username:
            username = self.username
        if not password:
            password = self.password
        headers = self._create_capi_headers(username, password)
        cbas_base_url = "http://{0}:{1}".format(self.ip, 9110)
        api = cbas_base_url + "/analytics/node/stats"
        status, content, response = self._http_request(
            api, method="GET", headers=headers)
        return status, content, response

    def fetch_cbas_storage_stats(self, username=None, password=None):
        if not username:
            username = self.username
        if not password:
            password = self.password
        headers = self._create_capi_headers(username, password)
        cbas_base_url = "http://{0}:{1}".format(self.ip, 9110)
        api = cbas_base_url + "/analytics/node/storage/stats"
        status, content, response = self._http_request(api, method="GET", headers=headers)
        content = json.loads(content)
        return status, content, response
    
    def operation_service_parameters_configuration_cbas(self, method="GET", params=None, username=None, password=None):
        if not username:
            username = self.username
        if not password:
            password = self.password
        headers = self._create_capi_headers(username, password)
        cbas_base_url = "http://{0}:{1}".format(self.ip, 8095)
        api = cbas_base_url + "/analytics/config/service"
        status, content, response = self._http_request(
            api, method=method, params=params, headers=headers)
        return status, content, response

    def operation_node_parameters_configuration_cbas(self, method="GET",
                                                     params=None,
                                                     username=None,
                                                     password=None):
        if not username:
            username = self.username
        if not password:
            password = self.password
        headers = self._create_capi_headers(username, password)
        cbas_base_url = "http://{0}:{1}".format(self.ip, 8095)
        api = cbas_base_url + "/analytics/config/node"
        status, content, response = self._http_request(
            api, method=method, params=params, headers=headers)
        return status, content, response

    def restart_analytics_cluster_uri(self, username=None, password=None):
        if not username:
            username = self.username
        if not password:
            password = self.password
        headers = self._create_capi_headers(username, password)
        api = self.cbas_base_url + "/analytics/cluster/restart"
        status, content, response = self._http_request(
            api, method="POST", headers=headers)
        return status, content, response

    def restart_analytics_node_uri(self, node_ip, port=8095, username=None,
                                   password=None):
        if not username:
            username = self.username
        if not password:
            password = self.password
        headers = self._create_capi_headers(username, password)
        node_url = "http://{0}:{1}".format(node_ip, port)
        api = node_url + "/analytics/node/restart"
        status, content, response = self._http_request(
            api, method="POST", headers=headers)
        return status, content, response

    def fetch_bucket_state_on_cbas(self, method="GET", username=None,
                                   password=None):
        if not username:
            username = self.username
        if not password:
            password = self.password
        headers = self._create_capi_headers(username, password)
        api = self.cbas_base_url + "/analytics/buckets"
        status, content, response = self._http_request(
            api, method=method, headers=headers)
        return status, content, response

    def fetch_pending_mutation_on_cbas_node(self, node_ip, port=9110,
                                            method="GET", username="None",
                                            password="None"):
        if not username:
            username = self.username
        if not password:
            password = self.password
        headers = self._create_capi_headers(username, password)
        node_url = "http://{0}:{1}".format(node_ip, port)
        api = node_url + "/analytics/node/stats"
        status, content, response = self._http_request(
            api, method=method, headers=headers)
        return status, content, response

    def fetch_pending_mutation_on_cbas_cluster(self, port=9110, method="GET",
                                               username="None",
                                               password="None"):
        if not username:
            username = self.username
        if not password:
            password = self.password
        headers = self._create_capi_headers(username, password)
        node_url = "http://{0}:{1}".format(self.ip, port)
        api = node_url + "/analytics/node/agg/stats/remaining"
        status, content, response = self._http_request(
            api, method=method, headers=headers)
        return status, content, response

    def fetch_dcp_state_on_cbas(self, dataset,  method="GET",
                                dataverse="Default", username=None,
                                password=None):
        if not username:
            username = self.username
        if not password:
            password = self.password
        headers = self._create_capi_headers(username, password)
        api = self.cbas_base_url + "/analytics/dataset/dcp/{0}/{1}" \
            .format(dataverse, dataset)
        status, content, response = self._http_request(
            api, method=method, headers=headers)
        return status, content, response

    # return analytics diagnostics info
    def get_analytics_diagnostics(self, cbas_node, timeout=120):
        analytics_base_url = "http://{0}:{1}/".format(cbas_node.ip, 8095)
        api = analytics_base_url + 'analytics/node/diagnostics'
        status, content, header = self._http_request(api, timeout=timeout)
        if status:
            json_parsed = json.loads(content)
            return json_parsed
        else:
            raise Exception("Unable to get jre path from analytics")

    # Backup Analytics metadata
    def backup_cbas_metadata(self, bucket_name, username=None, password=None):
        if not username:
            username = self.username
        if not password:
            password = self.password
        url = self.cbas_base_url + "/analytics/backup?bucket={0}".format(bucket_name)
        response = requests.get(url=url, auth=(username, password))
        return response

    # Restore Analytics metadata
    def restore_cbas_metadata(self, metadata, bucket_name, username=None, password=None):
        if not username:
            username = self.username
        if not password:
            password = self.password
        url = self.cbas_base_url + "/analytics/backup?bucket={0}".format(bucket_name)
        response = requests.post(url, data=json.dumps(metadata), auth=(username, password))
        return response

    # Set Analytics config parameter
    def set_global_compression_type(self, compression_type="snappy", username=None, password=None):
        if not username:
            username = self.username
        if not password:
            password = self.password
        headers = self._create_capi_headers(username, password)
        url = self.cbas_base_url + "/analytics/config/service"
        setting = {'storageCompressionBlock': compression_type}
        setting = json.dumps(setting)

        status, content, header = self._http_request(url, 'PUT', setting, headers=headers)
        return status
    
    def analytics_link_operations(self,method="GET", params="", timeout=120, username=None, password=None):
        if not username:
            username = self.username
        if not password:
            password = self.password
        api = self.cbas_base_url + "/analytics/link"
        headers = self._create_headers(username, password)
        if method.lower() == "get":
            api += "?{0}".format(params)
            params = ""
        try:
            status, content, header = self._http_request(
                api, method, headers=headers, params=params, timeout=timeout)
            try:
                content = json.loads(content)
            except Exception:
                pass
            errors = list()
            if not status:
                if not content:
                    errors.append({"msg": "Request Rejected", "code": 0 })
                else:
                    if isinstance(content,dict):
                        if "errors" in content:
                            if isinstance(content["errors"], list):
                                errors.extend(content["errors"])
                            else:
                                errors.append({"msg": content["errors"], "code": 0 })
                        elif "error" in content:
                            errors.append({"msg": content["error"], "code": 0 })
                    else:
                        content = content.split(":")
                        errors.append({"msg": content[1], "code": content[0] })
            return status, header['status'], content, errors
        except Exception as err:
            self.log.error("Exception occured while calling rest APi through httplib2.")
            self.log.error("Exception msg - (0)".format(str(err)))
            self.log.info("Retrying again with requests module")
            response = requests.request(method,api,headers=headers,data=params)
            try:
                content = response.json()
            except Exception:
                content = response.content
            errors = list()
            if response.status_code in [200, 201, 202]:
                return True, response.status_code, content, errors
            else:
                return False, response.status_code, content, content["errors"]
            
        
