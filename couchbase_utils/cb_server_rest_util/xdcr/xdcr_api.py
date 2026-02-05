from cb_server_rest_util.xdcr.xdcr_references import XdcrReferencesAPI
from cb_server_rest_util.xdcr.xdcr_replications import XdcrReplicationAPI
from cb_server_rest_util.xdcr.xdcr_stats import XDCRStatisticsAPI


class XdcrRestAPI(XdcrReferencesAPI, XdcrReplicationAPI, XDCRStatisticsAPI):
    def __init__(self, server):
        """
        Main gateway for all Cluster Rest Operations
        """
        super(XdcrRestAPI, self).__init__()

        self.set_server_values(server)
        self.set_endpoint_urls(server)
