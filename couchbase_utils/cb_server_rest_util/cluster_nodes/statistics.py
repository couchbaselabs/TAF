from cb_server_rest_util.connection import CBRestConnection

class StatisticsAPI(CBRestConnection):
    def __init__(self):
        super(StatisticsAPI, self).__init__()

    def get_prometheus_sd_config(self):
        """
        GET /prometheus_sd_config
        docs.couchbase.com/server/current/rest-api/rest-discovery-api.html
        """
        api = self.base_url + "/prometheus_sd_config"
        status, content, _ = self.request(api, CBRestConnection.GET)
        return status, content

    def get_stats_for_metric(self, metric_name, function_expression=None, label_values=None):
        """
        GET /pools/default/stats/range/<metric_name>/[function-expression]
        docs.couchbase.com/server/current/rest-api/rest-statistics-single.html
        """
        api = self.base_url + f"/pools/default/stats/range/{metric_name}"
        if function_expression:
            api += f"/{function_expression}"
        status, content, _ = self.request(api, CBRestConnection.GET, params=label_values)
        return status, content

    def get_multiple_stats(self, data):
        """
        POST /pools/default/stats/range
        docs.couchbase.com/server/current/rest-api/rest-statistics-multiple.html
        """
        api = self.base_url + "/pools/default/stats/range"
        status, content, _ = self.request(api, CBRestConnection.POST,
                                          params=data)
        return status, content

    def query_prometheus(self, query):
        """
        GET /_prometheus/api/v1/query?query={query}
        """
        api = self.base_url + f"/_prometheus/api/v1/query?query={query}"
        status, content, _ = self.request(api, CBRestConnection.GET)
        return status, content
