from cb_constants.CBServer import CbServer
from cb_server_rest_util.analytics.analytics_api import AnalyticsRestAPI
from cb_server_rest_util.backup.backup_api import BackupRestApi
from cb_server_rest_util.buckets.buckets_api import BucketRestApi
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from cb_server_rest_util.index.index_api import IndexRestAPI
from cb_server_rest_util.query.query_api import QueryRestAPI
from cb_server_rest_util.security.security_api import SecurityRestAPI
from cb_server_rest_util.server_groups.server_groups_api import ServerGroupsAPI
from cb_server_rest_util.xdcr.xdcr_api import XdcrRestAPI


class RestConnection(object):
    def __init__(self, server):
        self.server = server

        self.cluster = ClusterRestAPI(self.server)
        self.bucket = BucketRestApi(self.server)

        self.index = None
        self.query = None
        self.analytics = None
        self.eventing = None
        self.fts = None
        self.backup = None

        self.security = None
        self.server_group = None
        self.xdcr = None

    def activate_service_api(self, service_list):
        if CbServer.Services.INDEX in service_list:
            self.index = IndexRestAPI(self.server)
        if CbServer.Services.N1QL in service_list:
            self.query = QueryRestAPI(self.server)
        if CbServer.Services.CBAS in service_list:
            self.analytics = AnalyticsRestAPI(self.server)
        if CbServer.Services.EVENTING in service_list:
            pass
        if CbServer.Services.FTS in service_list:
            pass
        if CbServer.Services.BACKUP in service_list:
            self.backup = BackupRestApi(self.server)
        if "security" in service_list:
            self.security = SecurityRestAPI(self.server)
        if "server_group" in service_list:
            self.server_group = ServerGroupsAPI(self.server)
        if "xdcr" in service_list:
            self.xdcr = XdcrRestAPI(self.server)
