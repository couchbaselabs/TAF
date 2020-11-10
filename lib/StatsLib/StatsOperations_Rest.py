import json
import urllib
import re

import testconstants
from connections.Rest_Connection import RestConnection
from platform_utils.remote.remote_util import RemoteMachineShellConnection
from global_vars import logger


class StatsHelper(RestConnection):
    def __init__(self, server):
        super(StatsHelper, self).__init__(server)
        self.server = server
        self.base_url = "http://{0}:{1}".format(self.ip, 8091)
        self.fts_base_url = "http://{0}:{1}".format(self.ip, 8092)
        self.n1ql_base_url = "http://{0}:{1}".format(self.ip, 8093)
        self.cbas_base_url = "http://{0}:{1}".format(self.ip, 8095)
        self.eventing_base_url = "http://{0}:{1}".format(self.ip, 8096)
        self.index_base_url = "http://{0}:{1}".format(self.ip, 9102)
        self.memcached_ssl_base_url = "http://{0}:{1}".format(self.ip, 11207)
        # Prometheus scrapes from KV metrics from this port, and not 11210.
        # Look at: /opt/couchbase/var/lib/couchbase/config/prometheus.yaml for ports
        self.memcached_base_url = "http://{0}:{1}".format(self.ip, 11280)

        self.curl_path = "curl"
        shell = RemoteMachineShellConnection(self.server)
        type = shell.extract_remote_info().distribution_type
        if type.lower() == 'windows':
            self.path = testconstants.WIN_COUCHBASE_BIN_PATH
            self.curl_path = "%scurl" % self.path

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
        :optional_params: optional params like start,end,step,nodes,time_window,aggregationFunction
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

    def execute_promql_query(self, query):
        api = '%s%s%s' % (self.base_url, '/pools/default/stats?query=', urllib.quote_plus("%s" % query))
        status, content, _ = self._http_request(api)
        if not status:
            raise Exception(content)

    def get_instant_api_metrics(self, metric_name, label_values=None, optional_params=None):
        """
        :metric_name: metric_name to query
        :function: function such as rate, avg_over_time
        :label_values: dict of label as key and it's value as value
        :optional_params: optional params like ,nodes,aggregationFunction
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
        To change stats config settings thorugh diag/eval
        :key:  scrape_interval, retention_size, prometheus_metrics_scrape_interval etc
        :value: new_value to be set for the above key.
        """
        def diag_eval(code, print_log=True):
            api = '{0}{1}'.format(self.baseUrl, 'diag/eval/')
            status_i, content_i, header = self._http_request(api, "POST", code)
            if print_log:
                self.log.debug(
                    "/diag/eval status on {0}:{1}: {2} content: {3} command: {4}"
                        .format(self.ip, self.port, status_i, content_i, code))
            return status_i, content_i

        key_value = "{%s, %s}" % (key, str(value))
        status, content = diag_eval("ns_config:set_sub(stats_settings, [%s])" % key_value)
        if not status:
            raise Exception(content)

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
            raise Exception("unknown component name")

    def _run_curl_command_from_localhost(self, cmd):
        """
        Method to run curl cmd get request from localhost by SSHing into self.server
        :cmd: curl command  (get request)
        :returns True, body(content) as a list
        :Raises exception if response code not in 200 series
        """
        shell = RemoteMachineShellConnection(self.server)
        output, error = shell.execute_command(cmd)

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
                                "exposer", "kv",
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
                    raise Exception("Duplicate metrics entry {0}".format(line))
                if not check_prefixes(line):
                    raise Exception("Invalid prefix for metric {0}".format(line))

        if len(content) == 0:
            log.error("No metrics are present to validate")
