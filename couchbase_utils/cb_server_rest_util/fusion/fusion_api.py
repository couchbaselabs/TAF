from cb_server_rest_util.fusion.fusion_functions import FusionFunctions

class FusionRestAPI(FusionFunctions):
    def __init__(self, server):
        super(FusionRestAPI).__init__()

        self.set_server_values(server)
        self.set_endpoint_urls(server)
        self.check_if_couchbase_is_active(self, max_retry=5)
