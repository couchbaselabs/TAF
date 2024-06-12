import json
import urllib
import re
import base64

from py_constants import CbServer
from connections.Rest_Connection import RestConnection
from membase.api.rest_client import RestConnection as RestClientConnection
from platform_constants.os_constants import Windows
from platform_utils.remote.remote_util import RemoteMachineShellConnection
from global_vars import logger
from pytests.scalable_stats import constants


class StatsHelper(RestConnection):
    def __init__(self, server):
        super(StatsHelper, self).__init__(server)
        self.server = server
        protocol = "https" if CbServer.use_https else "http"
        rest_port = CbServer.ssl_port_map.get(
            str(CbServer.port), CbServer.port) if CbServer.use_https else CbServer.port
        fts_port = CbServer.ssl_port_map.get(str(CbServer.fts_port), CbServer.fts_port) \
            if CbServer.use_https else CbServer.fts_port
        n1ql_port = CbServer.ssl_port_map.get(str(CbServer.n1ql_port), CbServer.n1ql_port) \
            if CbServer.use_https else CbServer.n1ql_port
        cbas_port = CbServer.ssl_port_map.get(str(CbServer.cbas_port), CbServer.cbas_port) \
            if CbServer.use_https else CbServer.cbas_port
        eventing_port = CbServer.ssl_port_map.get(str(CbServer.eventing_port), CbServer.eventing_port) \
            if CbServer.use_https else CbServer.eventing_port
        index_port = CbServer.ssl_port_map.get(str(CbServer.index_port), CbServer.index_port) \
            if CbServer.use_https else CbServer.index_port

        self.base_url = "{0}://{1}:{2}".format(protocol, self.ip, rest_port)
        self.fts_base_url = "{0}://{1}:{2}".format(protocol, self.ip, fts_port)
        self.n1ql_base_url = "{0}://{1}:{2}".format(protocol, self.ip, n1ql_port)
        self.cbas_base_url = "{0}://{1}:{2}".format(protocol, self.ip, cbas_port)
        self.eventing_base_url = "{0}://{1}:{2}".format(protocol, self.ip, eventing_port)
        self.index_base_url = "{0}://{1}:{2}".format(protocol, self.ip, index_port)
        self.memcached_ssl_base_url = "http://{0}:{1}".format(self.ip, 11207)
        # Prometheus scrapes from KV metrics from this port, and not 11210.
        # Look at: /opt/couchbase/var/lib/couchbase/config/prometheus.yaml for ports
        self.memcached_base_url = "http://{0}:{1}".format(self.ip, 11280)
        self.prometheus_base_url = "http://{0}:{1}".format(self.ip, 9123)
        self.rest = RestClientConnection(server)

        self.curl_path = "curl"
        shell = RemoteMachineShellConnection(self.server)
        type = shell.extract_remote_info().distribution_type
        if type.lower() == 'windows':
            self.path = Windows.COUCHBASE_BIN_PATH
            self.curl_path = "%scurl" % self.path
        shell.disconnect()

    def get_prometheus_metrics(self, component="ns_server", parse=False):
        """
        API to get low cardinality metrics from the cluster
        :component: component specific metrics to retrieve
        :parse: whether to parse the response or not?
        :returns parsed dictionary or the response content as a list
        """
        if component == "kv":
            # For KV the endpoint is not accessible from non-localhost. So we do the following
            cmd = "{3} -i -o - --silent -u {0}:{1} http://{2}:11280/_prometheusMetrics". \
                format(self.username, self.password, "localhost", self.curl_path)
            content = self._run_curl_command_from_localhost(cmd)

        else:
            url = self._get_url_from_service(component)
            api = '%s%s' % (url, '/_prometheusMetrics')
            status, content, _ = self._http_request(api)
            if not status:
                raise Exception(content)
            content = content.splitlines()
        if parse:
            return self._call_component_parser(content=content, component=component, cardinality="low")
        else:
            return content

    def get_prometheus_metrics_high(self, component="kv", parse=False):
        """
        API to get high cardinality metrics from the cluster
        :component: component specific metrics to retrieve
        :returns response content as a list
        """
        if component == "kv":
            # For KV the endpoint is not accessible from non-localhost. So we do the following
            cmd = "{3} -i -o - --silent -u {0}:{1} http://{2}:11280/_prometheusMetricsHigh". \
                format(self.username, self.password, "localhost", self.curl_path)
            content = self._run_curl_command_from_localhost(cmd)
        else:
            url = self._get_url_from_service(component)
            api = '%s%s' % (url, '/_prometheusMetricsHigh')
            status, content, _ = self._http_request(api)
            if not status:
                raise Exception(content)
            content = content.splitlines()
        # ToDo - Think of a way to a parse this "high cardinality metrics", instead of returning the entire content
        return content

    def metering(self, component="kv"):
        """
        API to get high cardinality metrics from the cluster
        :component: component specific metrics to retrieve
        :returns response content as a list
        """
        if component == "kv":
            # For KV the endpoint is not accessible from non-localhost. So we do the following
            cmd = "{3} -i -o - --silent -u {0}:{1} http://{2}:11280/_metering". \
                format(self.username, self.password, "localhost", self.curl_path)
            content = self._run_curl_command_from_localhost(cmd)
        else:
            url = self._get_url_from_service(component)
            api = '%s%s' % (url, '/_metering')
            status, content, _ = self._http_request(api)
            if not status:
                raise Exception(content)
            content = content.splitlines()
        # ToDo - Think of a way to a parse this "high cardinality metrics", instead of returning the entire content
        return content

    def get_all_metrics(self):
        """
        API that returns all the metrics for all the services on that node in Prometheus format
        """
        api = '%s%s' % (self.base_url, '/metrics')
        status, content, _ = self._http_request(api)
        if not status:
            raise Exception(content)
        content = content.splitlines()
        # ToDo - Think of a way to a parse this "all metrics", instead of returning the entire content
        return content

    def get_range_api_metrics(self, metric_name, function=None, label_values=None, optional_params=None):
        """
        :metric_name: metric_name to query
        :function: function such as rate, avg_over_time
        :label_values: dict of label as key and it's value as value
        :optional_params: optional params like start,end,step,nodes,time_window,nodesAggregation
        :return json.loads(content): dictionary of returned metrics
        """
        api = '%s%s%s' % (self.base_url, '/pools/default/stats/range/', urllib.quote_plus("%s" % metric_name))
        if function:
            api = api + "/" + function
        if label_values:
            api = api + "?" + self._build_params_for_get_request(label_values)
        if optional_params:
            api = api + "?" + self._build_params_for_get_request(optional_params)
        status, content, _ = self._http_request(api)
        if not status:
            raise Exception(content)
        # ToDo - Think of a way to a parse time series, instead of returning the entire content
        return json.loads(content)

    def post_range_api_metrics(self, bucket_name):
        params = self._get_ui_params(bucket_name)
        authorization = base64.encodestring('%s:%s' % (self.username, self.password))
        api = self.base_url + '/pools/default/stats/range/'
        headers = {'Content-Type': 'text/plain', 'Authorization': 'Basic %s' % authorization}
        # We are using it because there are lot of code in _http_request which works only when output is json
        # In this case response in list, apart from that there is few more checks like we do for redaction
        # that doesnt work when the body is list
        # TODO : fix _http_request to work with lists(both body and repsonse)
        response, content = httplib2.Http(timeout=120).request(api, 'POST', params, headers)
        if not response['status']:
            raise Exception(content)
        return json.loads(content)

    def get_instant_api_metrics(self, metric_name, label_values=None, optional_params=None):
        """
        :metric_name: metric_name to query
        :function: function such as rate, avg_over_time
        :label_values: dict of label as key and it's value as value
        :optional_params: optional params like ,nodes,nodesAggregation
        :return json.loads(content): dictionary of returned metrics
        """
        api = '%s%s%s' % (self.base_url, '/pools/default/stats/instant/', urllib.quote_plus("%s" % metric_name))
        if label_values:
            api = api + "?" + self._build_params_for_get_request(label_values)
        if optional_params:
            api = api + "?" + self._build_params_for_get_request(optional_params)
        status, content, _ = self._http_request(api)
        if not status:
            raise Exception(content)
        # ToDo - Think of a way to a parse time series, instead of returning the entire content dictionary
        return json.loads(content)

    def configure_stats_settings_from_diag_eval(self, key, value):
        """
        To change stats config settings through diag/eval
        :key:  scrape_interval, retention_size, prometheus_metrics_scrape_interval etc
        :value: new_value to be set for the above key.
        """
        shell = RemoteMachineShellConnection(self.server)
        shell.enable_diag_eval_on_non_local_hosts()
        shell.disconnect()
        key_value = '{%s, %s}' % (key, str(value))
        status, content = self.rest.diag_eval('ns_config:set_sub(stats_settings, [%s])' % key_value)
        if not status:
            raise Exception(content)

    def reset_stats_settings_from_diag_eval(self, key=None):
        """
        To restore stats config to defaults through diag/eval
        :key: Specific key to restore eg: retention_size
        """
        shell = RemoteMachineShellConnection(self.server)
        shell.enable_diag_eval_on_non_local_hosts()
        shell.disconnect()
        default_config_dict = constants.stats_default_config
        if key:
            value = default_config_dict[key]
            key_value = '{%s, %s}' % (key, str(value))
            status, content = self.rest.diag_eval('ns_config:set_sub(stats_settings, [%s])' % key_value)
            if not status:
                raise Exception(content)
        else:
            # Reset all
            for key, value in default_config_dict.items():
                key_value = '{%s, %s}' % (key, str(value))
                status, content = self.rest.diag_eval('ns_config:set_sub(stats_settings, [%s])' % key_value)
                if not status:
                    raise Exception(content)

    def configure_stats_settings_from_api(self, metrics_data):
        """
        uses ns_server's rest api to change stats settings
        :param: metrics_data (json_string): metrics to update in the form of json string
        :returns (dict): all metrics settings
        """
        api = '%s%s' % (self.base_url, '/internal/settings/metrics/')
        headers = self.get_headers_for_content_type_json()
        status, content, _ = self._http_request(api, method="POST",
                                                params=metrics_data,
                                                headers=headers)
        if not status:
            raise Exception(content)
        return json.loads(content)

    def change_scrape_interval(self, scrape_interval):
        """
        changes scrape_interval via rest api
        :param: scrape_interval (int) - scrape_interval
        :returns: all metrics settings as a dict
        """
        stats_dict = dict()
        stats_dict["scrapeInterval"] = scrape_interval
        metrics_data = json.dumps(stats_dict)
        settings = self.configure_stats_settings_from_api(metrics_data)
        return settings

    def change_scrape_timeout(self, scrape_timeout):
        """
        changes scrape_interval via rest api
        :param: scrape timeout (int) - scrape timeout
        :returns: all metrics settings as a dict
        """
        stats_dict = dict()
        stats_dict["scrapeTimeout"] = scrape_timeout
        metrics_data = json.dumps(stats_dict)
        settings = self.configure_stats_settings_from_api(metrics_data)
        return settings

    def query_prometheus_federation(self, query):
        """
        Queries prometheus directly which runs at 9123 port
        :query: Query to be executed eg: targets?state=active
        :return json.loads(content): dictionary of returned content
        (requires auth_enabled=false and listen_addr_type=any from ns_config)
        """
        api = '%s%s%s' % (self.prometheus_base_url, '/api/v1/', query)
        status, content, _ = self._http_request(api)
        if not status:
            raise Exception(content)
        return json.loads(content)

    @staticmethod
    def _build_params_for_get_request(params_dict):
        """
        To build a string to be passed for GET request
        :params_dict: dictionary of key value pairs
        :return return_string: "key1=value1?key2=value2?..." that gets appended to GET API request
        """
        return_string = ""
        for key, value in params_dict.items():
            return_string = return_string + key + "=" + urllib.quote_plus("%s" % value) + "&"
        return_string = return_string[:-1]  # To remove the last '&" character
        return return_string

    def _get_url_from_service(self, component):
        if component == "kv":
            return self.memcached_base_url
        elif component == "ns_server":
            return self.base_url
        elif component == "index":
            return self.index_base_url
        elif component == "n1ql":
            return self.n1ql_base_url
        elif component == "cbas":
            return self.cbas_base_url
        # ToDo enumerate other services that support exposition of prometheus metrics
        else:
            raise Exception("unknown component name : {0}".format(component))

    def _run_curl_command_from_localhost(self, cmd):
        """
        Method to run curl cmd get request from localhost by SSHing into self.server
        :cmd: curl command  (get request)
        :returns True, body(content) as a list
        :Raises exception if response code not in 200 series
        """
        shell = RemoteMachineShellConnection(self.server)
        output, error = shell.execute_command(cmd)
        shell.disconnect()

        if error:
            self.log.error("Error making Curl request on server {0} {1}".format(self.server.ip, error))

        output_index = 0
        for line in output:
            if line.startswith("HTTP"):
                status_code_line = line.split(" ")
                status_code = status_code_line[1]
                status_msg = " ".join(status_code_line[2:])
                if status_code not in ['200', '201', '202']:
                    raise Exception("Exception {0} {1}".format(status_code, status_msg))
            elif not line.strip():
                content = output[output_index + 1:]
                return content
            output_index = output_index + 1

    def _call_component_parser(self, content, component="ns_server", cardinality="low"):
        """
        Helper method that calls the metrics parser
        :content: Response content from low/high cardinality metrics endpoint
        :component:  Respective component parser to call
        :cardinality: low or high cardinality content to be parsed
        :returns parsed dictionary from the respective parser
        """
        if cardinality == "low":
            if component == "ns_server":
                map = self._prometheus_low_cardinality_stats_parser_ns_server(content)
                return map
            else:
                raise Exception("Low cardinality parser for component {0} not implemented".format(component))

    @staticmethod
    def _prometheus_low_cardinality_stats_parser_ns_server(content):
        """
        Method to parse content from _prometheusMetrics (low cardinality metrics) of ns server
        :returns dict which looks nested like below
            a. For audit: metric_name->metric_value
            b: For sysproc: metric_name->process_name->metric_value
            c: For sys: metric_name->metric_value
            d: for couch: metric_name->bucket_name->metric_value
            e: for cm: metric_name->metric_value
        :Raises exception
            a. If a metric name does not start with the above prefixes
            b. If duplicate metrics are returned
            c. If no metrics were there to parse
        """
        map = dict()
        # Format of metrics exposed to prometheus
        # metric_name["{"label_name"="`"`label_value`"`{","label_name"="`"` label_value `"`} [","]"}"] value[timestamp]
        results = content
        match_pattern = "(.*){(.*)}(.*)"
        count_lines = 0
        for line in results:
            count_lines = count_lines + 1
            matched = re.match(match_pattern, line)
            if matched:
                metric_name = matched.group(1)
                if metric_name not in map:
                    map[metric_name] = dict()

                if metric_name.startswith("audit"):
                    value = matched.group(3)
                    if map[metric_name]:
                        raise Exception("Duplicate stats of type audit {0}".format(metric_name))
                    map[metric_name] = value
                elif metric_name.startswith("sysproc"):
                    value = matched.group(3)
                    labels_list = matched.group(2)
                    labels_name_values_list = labels_list.split(",")
                    for labels_name_values in labels_name_values_list:
                        labels_name_value = labels_name_values.split("=")
                        if labels_name_value[0] == "proc":
                            process_name = labels_name_value[1]
                            if process_name in map[metric_name]:
                                raise Exception("Duplicate stats of type sysproc {0}->{1}->{2}".
                                                format(metric_name, process_name, value))
                            map[metric_name][process_name] = value
                            break
                elif metric_name.startswith("sys"):
                    value = matched.group(3)
                    if map[metric_name]:
                        raise Exception("Duplicate stats of type sys {0}".format(metric_name))
                    map[metric_name] = value
                elif metric_name.startswith("couch"):
                    value = matched.group(3)
                    labels_list = matched.group(2)
                    labels_name_values_list = labels_list.split(",")
                    for labels_name_values in labels_name_values_list:
                        labels_name_value = labels_name_values.split("=")
                        if labels_name_value[0] == "bucket":
                            bucket_name = labels_name_value[1]
                            if bucket_name in map[metric_name]:
                                raise Exception("Duplicate stats of type couch {0}->{1}->{2}".
                                                format(metric_name, bucket_name, value))
                            map[metric_name][bucket_name] = value
                            break
                elif metric_name.startswith("cm"):
                    value = matched.group(3)
                    if map[metric_name]:
                        raise Exception("Duplicate stats of type cm {0}".format(metric_name))
                    map[metric_name] = value
                else:
                    raise Exception("Found some other metric type {0}".format(line))
        if len(map) == 0:
            raise Exception("NS server did not return nay low cardinality metrics")
        return map

    @staticmethod
    def _validate_metrics(content):
        """
        Method to validate exposition of metrics in /_getPrometheusMetrics, /_getPrometheusMetricsHigh, /metrics endpoints
            1. Check for duplicate entries (for component other than ns-server
            2. Check if entries are prefixed with service name
        :content: content response from the above endpoints, in the form of list
        Raises Exception if:
            1. Duplicate metrics are seen
            2. If a metric is not prefixed appropriately
        #TODo Not sure how it will behave for very large content - may result in stack overflow while checking duplicity ...
        #TODo...for example 20 metrics per index translates to a max of 200,000 metrics and hence should do not be ...
        #ToDo...called when cluster is heavy
        """

        def check_prefixes(line_to_check):
            allowed_prefixes = ["audit", "sysproc", "sys", "couch",
                                "exposer", "cm", "kv",
                                "index", "n1ql", "fts", "eventing", "xdcr"
                                ]
            for prefix in allowed_prefixes:
                if line_to_check.startswith(prefix):
                    return True
            return False

        log = logger.get("test")
        log.info("Validating metrics")
        lines_seen = set()
        for line in content:
            if not line.startswith("#"):
                if line not in lines_seen:
                    lines_seen.add(line)
                else:
                    raise Exception(
                        "Duplicate metrics entry {0}".format(line))
                if not check_prefixes(line):
                    raise Exception(
                        "Invalid prefix for metric {0}".format(line))
        if len(content) == 0:
            log.error("No metrics are present to validate")

    @staticmethod
    def _get_ui_params(bucket_name):
        """
        Method that returns important ui params, given a bucket_name
        """
        ui_params = [[{"step": 1, "start": -60, "metric": {"name": "kv_ops", "bucket": bucket_name},
                       "nodesAggregation": "sum", "applyFunctions": ["irate", "sum"]},
                      {"step": 1, "start": -60, "metric": {"name": "n1ql_requests"}, "nodesAggregation": "sum",
                       "applyFunctions": ["irate"]}, {"step": 1, "start": -60, "metric": {"name": "fts_total_queries",
                                                                                          "bucket":
                                                                                              bucket_name},
                                                      "nodesAggregation": "sum", "applyFunctions": ["irate"]},
                      {"step": 1, "start": -60,
                       "metric": {"name": "kv_ep_tmp_oom_errors", "bucket": bucket_name},
                       "nodesAggregation": "sum", "applyFunctions": ["irate"]}, {"step": 1, "start": -60, "metric": {
                "name": "kv_ep_cache_miss_ratio", "bucket": bucket_name},
                                                                                    "nodesAggregation": "sum"},
                      {"step": 1, "start": -60,
                       "metric": {"name": "kv_ops", "op": "get", "bucket": bucket_name},
                       "nodesAggregation": "sum", "applyFunctions": ["irate", "sum"]}, {"step": 1, "start": -60,
                                                                                           "metric": {"name": "kv_ops",
                                                                                                      "op": "set",
                                                                                                      "bucket":
                                                                                                          bucket_name},
                                                                                           "nodesAggregation": "sum",
                                                                                           "applyFunctions": ["irate",
                                                                                                              "sum"]},
                      {"step": 1, "start": -60, "metric": {"name": "kv_ops", "op": "delete", "result": "hit",
                                                           "bucket": bucket_name},
                       "nodesAggregation": "sum", "applyFunctions": ["irate"]},
                      {"step": 1, "start": -60, "metric": {"name": "accesses", "bucket": bucket_name},
                       "nodesAggregation": "sum"}, {"step": 1, "start": -60, "metric": {"name": "kv_mem_used_bytes",
                                                                                           "bucket": bucket_name},
                                                       "nodesAggregation": "sum"},
                      {"step": 1, "start": -60, "metric": {
                          "name": "kv_ep_mem_low_wat", "bucket": bucket_name}, "nodesAggregation": "sum"},
                      {"step": 1, "start": -60,
                       "metric": {"name": "kv_ep_mem_high_wat", "bucket": bucket_name},
                       "nodesAggregation": "sum"}, {"step": 1, "start": -60, "metric": {"name": "kv_curr_items",
                                                                                           "bucket": bucket_name},
                                                       "nodesAggregation": "sum"},
                      {"step": 1, "start": -60, "metric": {
                          "name": "kv_vb_replica_curr_items", "bucket": bucket_name},
                       "nodesAggregation": "sum"},
                      {"step": 1, "start": -60,
                       "metric": {"name": "kv_vb_active_resident_items_ratio", "bucket": bucket_name},
                       "nodesAggregation": "sum"}, {"step": 1, "start": -60,
                                                       "metric": {"name": "kv_vb_replica_resident_items_ratio",
                                                                  "bucket": bucket_name},
                                                       "nodesAggregation": "sum"},
                      {"step": 1, "start": -60, "metric": {
                          "name": "kv_disk_write_queue", "bucket": bucket_name}}, {"step": 1, "start": -60,
                                                                              "metric": {
                                                                                  "name": "kv_ep_data_read_failed",
                                                                                  "bucket":
                                                                                      bucket_name},
                                                                              "nodesAggregation": "sum"},
                      {"step": 1, "start": -60,
                       "metric": {"name": "kv_ep_data_write_failed", "bucket": bucket_name},
                       "nodesAggregation": "sum"},
                      {"step": 1, "start": -60, "metric": {"name": "n1ql_errors"}, "nodesAggregation": "sum",
                       "applyFunctions": ["irate"]},
                      {"step": 1, "start": -60, "metric": {"name": "eventing_failed_count"},
                       "nodesAggregation": "sum"},
                      {"step": 1, "start": -60, "metric": {"name": "n1ql_requests_250ms"}, "nodesAggregation": "sum",
                       "applyFunctions": ["irate"]},
                      {"step": 1, "start": -60, "metric": {"name": "n1ql_requests_500ms"}, "nodesAggregation": "sum",
                       "applyFunctions": ["irate"]},
                      {"step": 1, "start": -60, "metric": {"name": "n1ql_requests_1000ms"},
                       "nodesAggregation": "sum",
                       "applyFunctions": ["irate"]},
                      {"step": 1, "start": -60, "metric": {"name": "n1ql_requests_5000ms"},
                       "nodesAggregation": "sum",
                       "applyFunctions": ["irate"]}, {"step": 1, "start": -60,
                                                      "metric": {"name": "replication_changes_left",
                                                                 "bucket": bucket_name}},
                      {"step": 1, "start": -60,
                       "metric": {"name": "index_num_docs_pending+queued", "bucket": bucket_name,
                                  "index": "gsi-0"}}, {"step": 1, "start": -60,
                                                       "metric": {"name": "fts_num_mutations_to_index",
                                                                  "bucket": bucket_name}},
                      {"step": 1, "start": -60, "metric": {"name": "eventing_dcp_backlog"}, "applyFunctions": ["sum"]}]]

        return json.dumps(ui_params)
