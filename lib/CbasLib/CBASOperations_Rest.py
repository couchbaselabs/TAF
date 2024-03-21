"""
Created on Sep 25, 2017

@author: riteshagarwal
"""

import json
import urllib
import requests

from Cb_constants import CbServer
from connections.Rest_Connection import RestConnection
from membase.api import httplib2


class CBASHelper(RestConnection):
    def __init__(self, cbas_node):
        super(CBASHelper, self).__init__(cbas_node)
        if cbas_node.type == "columnar":
            self.cbas_base_url = "https://{0}:{1}".format(
                self.ip, cbas_node.nebula_rest_port)
        else:
            self.cbas_base_url = "http://{0}:{1}".format(self.ip, CbServer.cbas_port)
            if CbServer.use_https:
                self.cbas_base_url = "https://{0}:{1}".format(self.ip, CbServer.ssl_cbas_port)


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
        status, content, response = self._http_request(
            api, 'POST', headers=headers, params=params, timeout=timeout)
        if hasattr(response, "status"):
            status_code = response.status
        elif hasattr(response, "status_code"):
            status_code = response.status_code

        if status:
            return content
        elif status_code == 503:
            self.log.info("Request Rejected")
            raise Exception("Request Rejected")
        elif status_code in [500, 400, 401, 403, 409]:
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
        status, content, response = self._http_request(
            api, 'POST', headers=headers, params=params, timeout=timeout)

        if hasattr(response, "status"):
            status_code = response.status
        elif hasattr(response, "status_code"):
            status_code = response.status_code

        if status:
            return content
        elif status_code == 503:
            self.log.info("Request Rejected")
            raise Exception("Request Rejected")
        elif status_code in [500, 400, 401, 403, 409]:
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
        headers = self._create_headers(username, password)
        status, content, response = self._http_request(
            api, 'DELETE', params=payload, headers=headers, timeout=600)

        if hasattr(response, "status"):
            status_code = response.status
        elif hasattr(response, "status_code"):
            status_code = response.status_code

        if status:
            return status_code
        elif status_code == 404:
            self.log.info("Request Not Found")
            return status_code
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
        status, content, reponse = self._http_request(
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
        api = self.cbas_base_url + "/analytics/node/stats"
        status, content, response = self._http_request(
            api, method="GET", headers=headers)
        return status, content, response

    def fetch_cbas_storage_stats(self, username=None, password=None):
        if not username:
            username = self.username
        if not password:
            password = self.password
        headers = self._create_capi_headers(username, password)
        api = self.cbas_base_url + "/analytics/node/storage/stats"
        status, content, response = self._http_request(api, method="GET", headers=headers)
        content = json.loads(content)
        return status, content, response

    def operation_service_parameters_configuration_cbas(self, method="GET", params=None, username=None, password=None):
        if not username:
            username = self.username
        if not password:
            password = self.password
        headers = self._create_capi_headers(username, password)
        api = self.cbas_base_url + "/analytics/config/service"
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
        api = self.cbas_base_url + "/analytics/config/node"
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
        api = self.cbas_base_url + "/analytics/node/restart"
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
        api = self.cbas_base_url + "/analytics/node/stats"
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
        api = self.cbas_base_url + "/analytics/node/agg/stats/remaining"
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
    def get_analytics_diagnostics(self, timeout=120):
        api = self.cbas_base_url + '/analytics/cluster/diagnostics'
        status, content, response = self._http_request(api, timeout=timeout)
        if status:
            json_parsed = json.loads(content)
            return json_parsed
        else:
            raise Exception("Unable to get analytics diagnostics")

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

    def backup_cbas(self, username=None, password=None, bucket="", include="",
                    exclude="", level="cluster"):
        """
        Cluster level: GET/POST: /api/v1/backup
        Bucket level: GET/POST: /api/v1/bucket/[BUCKET]/backup
        params:
            include: str path[,path]
                path format is bucket.scope.collection for cluster level and \
                scope.collection for bucket level
            exclude: str path[,path]
                path format is bucket.scope.collection for cluster level and \
                scope.collection for bucket level
        """
        if not username:
            username = self.username
        if not password:
            password = self.password
        api = self.cbas_base_url
        if level.lower() == "cluster":
            api += "/api/v1/backup"
        elif level.lower() == "bucket":
            if not bucket:
                raise Exception(
                    "Bucket name is not specified for bucket level backup api")
            api += "/api/v1/bucket/{0}/backup".format(urllib.quote(bucket))
        else:
            raise Exception("Un-known backup level")
        if include or exclude:
            api += "?"
        if include:
            api += "include={0}".format(include)
        if exclude:
            if include:
                api += "&"
            api += "exclude={0}".format(exclude)
        self.log.info("Backup: " + api)
        return self._http_request(api)

    def restore_cbas(self, username=None, password=None, bucket="", include="",
                     exclude="", remap="",
                     level="cluster", backup={}):
        """
        Cluster level: GET/POST: /api/v1/backup
        Bucket level: GET/POST: /api/v1/bucket/[BUCKET]/backup
        params:
            bucket: str : name of bucket
            level: str : cluster, bucket
            remap: str : path:path[, path:path]
                path format is bucket.scope.collection for cluster level and \
                scope.collection for bucket level
            backup: dict : response from backup api
        """
        api = self.cbas_base_url
        if level.lower() == "cluster":
            api += "/api/v1/backup"
        elif level.lower() == "bucket":
            if not bucket:
                raise Exception(
                    "Bucket name is not specified for bucket level backup api")
            api += "/api/v1/bucket/{0}/backup".format(urllib.quote(bucket))
        else:
            raise Exception("Un-known backup level")
        if include or exclude or remap:
            api += "?"
        if include:
            api += "include={0}".format(include)
        if exclude:
            if not api.endswith("&"):
                api += "&"
            api += "exclude={0}".format(exclude)
        if remap:
            if not api.endswith("&"):
                api += "&"
            api += "remap={0}".format(remap)
        self.log.info("Restore: " + api + " Body: " + str(backup))
        return self._http_request(api, method="POST", params=json.dumps(backup))

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

        status, content, response = self._http_request(url, 'PUT', setting,
                                                headers=headers)
        return status

    def analytics_link_operations(self, method="GET", uri="", params="",
                                  timeout=120, username=None, password=None):
        if not username:
            username = self.username
        if not password:
            password = self.password
        api = self.cbas_base_url + "/analytics/link" + uri
        headers = self._create_headers(username, password)
        if method.lower() == "get":
            api += "?{0}".format(params)
            params = ""
        try:
            status, content, response = self._http_request(
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
            if hasattr(response, "status"):
                status_code = response.status
            elif hasattr(response, "status_code"):
                status_code = response.status_code
            return status, status_code, content, errors
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

    @staticmethod
    def analytics_replica_settings(logger, node, method="GET", params="",
                                   timeout=120, username=None, password=None):
        """
        This method is used to get or set analytics replica number from
        cluster settings page.
        """
        rest_conn = RestConnection(node)
        if not username:
            username = rest_conn.username
        if not password:
            password = rest_conn.password

        api = rest_conn.baseUrl + "/settings/analytics"
        headers = rest_conn._create_headers(username, password)

        try:
            status, content, response = rest_conn._http_request(
                api, method, headers=headers, params=params, timeout=timeout)
            try:
                content = json.loads(content)
            except Exception:
                pass
            errors = list()
            if not status:
                if not content:
                    errors.append({"msg": "Request Rejected", "code": 0})
                else:
                    if isinstance(content, dict):
                        if "errors" in content:
                            if isinstance(content["errors"], list):
                                errors.extend(content["errors"])
                            elif isinstance(content["errors"], dict):
                                for v in content["errors"].values():
                                    errors.append(
                                        {"msg": v.encode("utf-8"), "code": 0})
                            else:
                                errors.append({"msg": content["errors"], "code": 0})
                        elif "error" in content:
                            errors.append({"msg": content["error"], "code": 0})
                        elif "message" in content:
                            errors.append(
                                {"msg": content["message"], "code": 0})
                    else:
                        content = content.split(":")
                        errors.append({"msg": content[1], "code": content[0]})
            if hasattr(response, "status_code"):
                return status, response.status_code, content, errors
            else:
                return status, response["status"], content, errors
        except Exception as err:
            logger.error("Exception occured while calling rest APi through httplib2.")
            logger.error("Exception msg - (0)".format(str(err)))
            logger.info("Retrying again with requests module")
            response = requests.request(
                method, api, headers=headers, data=params)
            try:
                content = response.json()
            except Exception:
                content = response.content
            errors = list()
            if response.status_code in [200, 201, 202]:
                return True, response.status_code, content, errors
            else:
                return False, response.status_code, content, content["errors"]
