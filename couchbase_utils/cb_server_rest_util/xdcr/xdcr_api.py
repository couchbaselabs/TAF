from cb_server_rest_util.xdcr.xdcr_references import XdcrReferencesAPI
from cb_server_rest_util.xdcr.xdcr_replications import XdcrReplicationAPI


class XdcrRestAPI(XdcrReferencesAPI, XdcrReplicationAPI):
    def __init__(self, server):
        """
        Main gateway for all Cluster Rest Operations
        """
        super(XdcrRestAPI, self).__init__()

        self.set_server_values(server)
        self.set_endpoint_urls(server)
