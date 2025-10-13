from cb_server_rest_util.analytics.analytics_settings import AnalyticsSettingsAPI
from cb_server_rest_util.analytics.analytics_functions import AnalyticsFunctionsAPI


class AnalyticsRestAPI(AnalyticsSettingsAPI, AnalyticsFunctionsAPI):
    def __init__(self, server):
        super(AnalyticsRestAPI).__init__()

        self.set_server_values(server)
        self.set_endpoint_urls(server)
        self.check_if_couchbase_is_active(self, max_retry=5)
