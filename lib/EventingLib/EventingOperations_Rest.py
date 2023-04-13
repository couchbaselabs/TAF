import base64
import json
from connections.Rest_Connection import RestConnection

class EventingHelper(RestConnection):
    def __init__(self, server):
        super(EventingHelper, self).__init__(server)

    '''
            Save the Function so that it is visible in UI
    '''
    def save_function(self, name, body):
        authorization = base64.encodestring('%s:%s' % (self.username, self.password))
        url = "_p/event/saveAppTempStore/?name=" + name
        api = self.baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'POST', headers=headers,
                                                     params=json.dumps(body).encode("ascii", "ignore"))
        if not status:
            raise Exception(content)
        return content

    '''
            Deploy the Function
    '''

    def deploy_function(self, name, body):
        authorization = base64.encodestring('%s:%s' % (self.username, self.password))
        url = "_p/event/setApplication/?name=" + name
        api = self.baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'POST', headers=headers,
                                                     params=json.dumps(body).encode("ascii", "ignore"))
        if not status:
            raise Exception(content)
        return content

    '''
            GET all the Functions
    '''

    def get_all_functions(self):
        authorization = base64.encodestring('%s:%s' % (self.username, self.password))
        url = "api/v1/functions"
        api = self.eventing_baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'GET', headers=headers)
        if not status:
            raise Exception(content)
        return content

    '''
            Undeploy the Function
    '''

    def set_settings_for_function(self, name, body):
        authorization = base64.encodestring('%s:%s' % (self.username, self.password))
        url = "api/v1/functions/" + name + "/settings"
        api = self.eventing_baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'POST', headers=headers,
                                                     params=json.dumps(body).encode("ascii", "ignore"))

        if not status:
            raise Exception(content)
        return content

    '''
        undeploy the Function
    '''

    def undeploy_function(self, name):
        authorization = base64.encodestring('%s:%s' % (self.username, self.password))
        url = "api/v1/functions/" + name + "/settings"
        body = {"deployment_status": False, "processing_status": False}
        api = self.eventing_baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'POST', headers=headers,
                                                     params=json.dumps(body).encode("ascii", "ignore"))
        if not status:
            raise Exception(content)
        return content

    '''
        Delete all the functions
    '''

    def delete_all_function(self):
        url = "api/v1/functions"
        api = self.eventing_baseUrl + url
        status, content, _ = self._http_request(api, 'DELETE')
        return status, content

    '''
            Delete single function
    '''

    def delete_single_function(self, name):
        authorization = base64.encodestring('%s:%s' % (self.username, self.password))
        url = "api/v1/functions/" + name
        api = self.eventing_baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'DELETE', headers=headers)
        if not status:
            raise Exception(content)
        return content

    '''
            Delete the Function from UI
    '''

    def delete_function_from_temp_store(self, name):
        authorization = base64.encodestring('%s:%s' % (self.username, self.password))
        url = "_p/event/deleteAppTempStore/?name=" + name
        api = self.baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'DELETE', headers=headers)
        if not status:
            raise Exception(content)
        return content

    '''
            Delete the Function
    '''

    def delete_function(self, name):
        authorization = base64.encodestring('%s:%s' % (self.username, self.password))
        url = "_p/event/deleteApplication/?name=" + name
        api = self.baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'DELETE', headers=headers)
        if not status:
            raise Exception(content)
        return content

    '''
            Export the Function
    '''

    def export_function(self, name):
        export_map = {}
        authorization = base64.encodestring('%s:%s' % (self.username, self.password))
        url = "api/v1/export/" + name
        api = self.eventing_baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'GET', headers=headers)
        if not status:
            raise Exception(content)
        if status:
            json_parsed = json.loads(content)
            for key in json_parsed[0].keys():  # returns an array
                tokens = key.split(":")
                val = json_parsed[0][key]
                if len(tokens) == 1:
                    field = tokens[0]
                    export_map[field] = val
        return export_map

    '''
             Ensure that the eventing node is out of bootstrap node
    '''

    def get_deployed_eventing_apps(self):
        authorization = base64.encodestring('%s:%s' % (self.username, self.password))
        url = "getDeployedApps"
        api = self.eventing_baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'GET', headers=headers)
        if not status:
            raise Exception(content)
        return json.loads(content)

    '''
            Ensure that the eventing node is out of bootstrap node
    '''

    def get_running_eventing_apps(self):
        authorization = base64.encodestring('%s:%s' % (self.username, self.password))
        url = "getRunningApps"
        api = self.eventing_baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'GET', headers=headers)
        if not status:
            raise Exception(content)
        return json.loads(content)

    '''
            composite status of a handler
    '''

    def get_composite_eventing_status(self):
        url = "api/v1/status"
        api = self.eventing_baseUrl + url
        status, content, _ = self._http_request(api, 'GET')
        return status, json.loads(content)

    '''
             Get Eventing processing stats
    '''

    def get_event_processing_stats(self, name, eventing_map=None):
        if eventing_map is None:
            eventing_map = {}
        authorization = base64.encodestring('%s:%s' % (self.username, self.password))
        url = "getEventProcessingStats?name=" + name
        api = self.eventing_baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'GET', headers=headers)
        if status:
            json_parsed = json.loads(content)
            for key in json_parsed.keys():
                tokens = key.split(":")
                val = json_parsed[key]
                if len(tokens) == 1:
                    field = tokens[0]
                    eventing_map[field] = val
        return eventing_map

    '''
            Get Aggregate Eventing processing stats
    '''

    def get_aggregate_event_processing_stats(self, name, eventing_map=None):
        if eventing_map is None:
            eventing_map = {}
        authorization = base64.encodestring('%s:%s' % (self.username, self.password))
        url = "getAggEventProcessingStats?name=" + name
        api = self.eventing_baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'GET', headers=headers)
        if status:
            json_parsed = json.loads(content)
            for key in json_parsed.keys():
                tokens = key.split(":")
                val = json_parsed[key]
                if len(tokens) == 1:
                    field = tokens[0]
                    eventing_map[field] = val
        return eventing_map

    '''
            Get Eventing execution stats
    '''

    def get_event_execution_stats(self, name, eventing_map=None):
        if eventing_map is None:
            eventing_map = {}
        authorization = base64.encodestring('%s:%s' % (self.username, self.password))
        url = "getExecutionStats?name=" + name
        api = self.eventing_baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'GET', headers=headers)
        if status:
            json_parsed = json.loads(content)
            for key in json_parsed.keys():
                tokens = key.split(":")
                val = json_parsed[key]
                if len(tokens) == 1:
                    field = tokens[0]
                    eventing_map[field] = val
        return eventing_map

    '''
            Get Eventing failure stats
    '''

    def get_event_failure_stats(self, name, eventing_map=None):
        if eventing_map is None:
            eventing_map = {}
        authorization = base64.encodestring('%s:%s' % (self.username, self.password))
        url = "getFailureStats?name=" + name
        api = self.eventing_baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'GET', headers=headers)
        if status:
            json_parsed = json.loads(content)
            for key in json_parsed.keys():
                tokens = key.split(":")
                val = json_parsed[key]
                if len(tokens) == 1:
                    field = tokens[0]
                    eventing_map[field] = val
        return eventing_map

    '''
            Get all eventing stats
    '''

    def get_all_eventing_stats(self, seqs_processed=False, eventing_map=None):
        if eventing_map is None:
            eventing_map = {}
        url = "api/v1/stats"
        if seqs_processed:
            url += "?type=full"
        api = self.eventing_baseUrl + url
        status, content, _ = self._http_request(api, 'GET')
        return status, json.loads(content)

    '''
            Cleanup eventing
    '''

    def cleanup_eventing(self):
        authorization = base64.encodestring('%s:%s' % (self.username, self.password))
        url = "cleanupEventing"
        api = self.eventing_baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'GET', headers=headers)
        if not status:
            raise Exception(content)
        return content

    '''
               enable debugger
    '''

    def enable_eventing_debugger(self):
        authorization = base64.encodestring('%s:%s' % (self.username, self.password))
        url = "_p/event/api/v1/config"
        api = self.baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        body = "{\"enable_debugger\": true}"
        status, content, header = self._http_request(api, 'POST', headers=headers, params=body)
        if not status:
            raise Exception(content)
        return content

    '''
                   disable debugger
    '''

    def disable_eventing_debugger(self):
        authorization = base64.encodestring('%s:%s' % (self.username, self.password))
        url = "_p/event/api/v1/config"
        api = self.baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        body = "{\"enable_debugger\": false}"
        status, content, header = self._http_request(api, 'POST', headers=headers, params=body)
        if not status:
            raise Exception(content)
        return content

    '''
            Start debugger
    '''

    def start_eventing_debugger(self, name):
        authorization = base64.encodestring('%s:%s' % (self.username, self.password))
        url = "/pools/default"
        api = self.baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'GET', headers=headers)
        url = "_p/event/startDebugger/?name=" + name
        api = self.baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'POST', headers=headers, params=content)
        if not status:
            raise Exception(content)
        return content

    '''
            Stop debugger
    '''

    def stop_eventing_debugger(self, name):
        authorization = base64.encodestring('%s:%s' % (self.username, self.password))
        url = "_p/event/stopDebugger/?name=" + name
        api = self.baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'POST', headers=headers)
        if not status:
            raise Exception(content)
        return content

    '''
            Get debugger url
    '''

    def get_eventing_debugger_url(self, name):
        authorization = base64.encodestring('%s:%s' % (self.username, self.password))
        url = "_p/event/getDebuggerUrl/?name=" + name
        api = self.baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'POST', headers=headers)
        if not status:
            raise Exception(content)
        return content

    '''
          Get eventing rebalance status
    '''

    def get_eventing_rebalance_status(self):
        authorization = base64.encodestring('%s:%s' % (self.username, self.password))
        url = "getAggRebalanceStatus"
        api = self.eventing_baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'GET', headers=headers)
        if status:
            return content

    def import_function(self, body):
        url = "api/v1/import"
        api = self.eventing_baseUrl + url
        status, content, _ = self._http_request(api, 'POST', params=body)
        return status, content

    def create_function(self, name, body):
        authorization = base64.encodestring('%s:%s' % (self.username, self.password))
        url = "api/v1/functions/" + name
        api = self.eventing_baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'POST', headers=headers,
                                                     params=json.dumps(body).encode("ascii", "ignore"))
        if not status:
            raise Exception(content)
        return content

    def update_function(self, name, body):
        authorization = base64.encodestring('%s:%s' % (self.username, self.password))
        url = "api/v1/functions/" + name
        api = self.eventing_baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        body['appname'] = name
        status, content, header = self._http_request(api, 'POST', headers=headers,
                                                     params=json.dumps(body).encode("ascii", "ignore"))
        if not status:
            raise Exception(content)
        return content

    def get_function_details(self, name):
        url = "api/v1/functions/" + name
        api = self.eventing_baseUrl + url
        status, content, _ = self._http_request(api, 'GET')
        return status, json.loads(content)

    def get_eventing_go_routine_dumps(self):
        authorization = base64.encodestring('%s:%s' % (self.username, self.password))
        url = "debug/pprof/goroutine?debug=1"
        api = self.eventing_baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'GET', headers=headers)
        if not status:
            raise Exception(content)
        return content

    def set_eventing_retry(self, name, body):
        authorization = base64.encodestring('%s:%s' % (self.username, self.password))
        url = "api/v1/functions/" + name + "/retry"
        api = self.eventing_baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'POST', headers=headers,
                                                     params=json.dumps(body).encode("ascii", "ignore"))
        if not status:
            raise Exception(content)
        return content

    def get_cpu_count(self):
        authorization = base64.encodestring(
            '%s:%s' % (self.username, self.password))
        url = "getCpuCount"
        api = self.eventing_baseUrl + url
        headers = {'Content-type': 'application/json',
                   'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'GET',
                                                     headers=headers)
        if not status:
            raise Exception(content)
        return content

    def get_list_of_eventing_functions(self):
         url = "api/v1/list/functions"
         api = self.eventing_baseUrl + url
         status, content, _ = self._http_request(api, 'GET')
         return status, json.loads(content)

    def lifecycle_operation(self, name, operation):
         url = "api/v1/functions/" + name +"/"+ operation
         api = self.eventing_baseUrl + url
         status, content, _ = self._http_request(api, 'POST')
         return status, content
