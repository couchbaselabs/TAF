import json
import urllib
import re

from connections.Rest_Connection import RestConnection


class StatsHelper(RestConnection):
    def __init__(self, server):
        super(StatsHelper, self).__init__(server)
        self.base_url = "http://{0}:{1}".format(self.ip, 8091)
        self.fts_base_url = "http://{0}:{1}".format(self.ip, 8092)
        self.n1ql_base_url = "http://{0}:{1}".format(self.ip, 8093)
        self.cbas_base_url = "http://{0}:{1}".format(self.ip, 8095)
        self.eventing_base_url = "http://{0}:{1}".format(self.ip, 8096)
        self.index_base_url = "http://{0}:{1}".format(self.ip, 9102)
        self.memcached_ssl_base_url = "http://{0}:{1}".format(self.ip, 11207)
        self.memcached_base_url = "http://{0}:{1}".format(self.ip, 11210)

    def get_prometheus_metrics(self):
        """
        API to get low cardinality metrics from the cluster
        """
        api = '%s%s' % (self.base_url, '/_prometheusMetrics')
        status, content, _ = self._http_request(api)
        if not status:
            raise Exception(content)
        map = self._promtheus_low_cardinality_stats_parser(content)
        return map

    def get_prometheus_metrics_high(self):
        """
        API to get high cardinality metrics from the cluster
        """
        api = '%s%s' % (self.base_url, '/_prometheusMetricsHigh')
        status, content, _ = self._http_request(api)
        if not status:
            raise Exception(content)
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

    def _build_params_for_get_request(self, params_dict):
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

    def _promtheus_low_cardinality_stats_parser(self, content):
        """
        Method to parse content from _prometheusMetrics (low cardinality metrics)
        :returns dict which looks nested like below
            a. For audit: metric_name->metric_value
            b: For sysproc: metric_name->process_name->metric_value
            c: For sys: metric_name->metric_value
            d: for couch: metric_name->bucket_name->metric_value
        """
        map = dict()
        # Format of metrics exposed to prometheus
        # metric_name["{"label_name"="`"`label_value`"`{","label_name"="`"` label_value `"`} [","]"}"] value[timestamp]
        results = content.split("\n")
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
        return map
