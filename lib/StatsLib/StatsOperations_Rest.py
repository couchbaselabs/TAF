import json
import urllib
import requests
import re

from connections.Rest_Connection import RestConnection
from membase.api import httplib2


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
        api = '%s%s' % (self.base_url, '/_prometheusMetrics')
        status, content, _ = self._http_request(api)
        if not status:
            raise Exception(content)
        map = self._promtheus_stats_parser(content)
        return map

    def get_prometheus_metrics_high(self):
        api = '%s%s' % (self.base_url, '/_prometheusMetricsHigh')
        status, content, _ = self._http_request(api)
        if not status:
            raise Exception(content)
        map = self._promtheus_stats_parser(content)
        return map

    def _promtheus_stats_parser(self, content):
        map = dict()
        # Format of metrics exposed to prometheus
        # metric_name["{"label_name"="`"`label_value`"`{","label_name"="`"` label_value `"`} [","]"}"] value[timestamp]
        results = content.split("\n")
        match_pattern = "(.*){(.*)}(.*)"
        for line in results:
            matched = re.match(match_pattern, line)
            if matched:
                metric_name = matched.group(1)
                if metric_name not in map:
                    map[metric_name] = dict()
                # self.log.info("Metric Name : {0}".format(metric_name))
                labels_list = matched.group(2)
                labels_name_values_list = labels_list.split(",")
                value = matched.group(3)
                system_processes = False
                label = None
                for labels_name_values in labels_name_values_list:
                    labels_name_value = labels_name_values.split("=")
                    # self.log.info("Label Name : {0} Label Value".format(labels_name_value[0],labels_name_value[1]))
                    if labels_name_value[0] == "proc" or labels_name_value[0] == "bucket":
                        system_processes = True
                        label = labels_name_value[1]
                        map[metric_name][label] = dict()
                        map[metric_name][label]["value"] = value
                    else:
                        if system_processes:
                            map[metric_name][label][labels_name_value[0]] = labels_name_value[1]
                        else:
                            map[metric_name][labels_name_value[0]] = labels_name_value[1]
                            map[metric_name]["value"] = value
                system_processes = False
                label = None
        return map





