from cb_server_rest_util.analytics.analytics_settings import AnalyticsSettingsAPI
from cb_server_rest_util.analytics.analytics_functions import AnalyticsFunctionsAPI
from cb_server_rest_util.analytics.analytics_service import AnalyticsServiceAPI
from cb_server_rest_util.analytics.analytics_admin import AnalyticsAdminAPI
from cb_server_rest_util.analytics.analytics_config import AnalyticsConfigAPI


class AnalyticsRestAPI(AnalyticsSettingsAPI, AnalyticsFunctionsAPI, AnalyticsServiceAPI, AnalyticsAdminAPI, AnalyticsConfigAPI):
    def __init__(self, server):
        super(AnalyticsRestAPI).__init__()

        self.set_server_values(server)
        self.set_endpoint_urls(server)
        self.check_if_couchbase_is_active(self, max_retry=5)
