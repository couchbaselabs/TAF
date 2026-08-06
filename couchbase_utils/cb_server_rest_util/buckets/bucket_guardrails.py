from cb_server_rest_util.connection import CBRestConnection


class BucketGuardrailsAPI(CBRestConnection):
    def __init__(self):
        super(BucketGuardrailsAPI, self).__init__()

    def set_bucket_rr_guardrails(self, couch_min_rr=None, magma_min_rr=None):
        """
        POST /settings/resourceManagement/bucket/residentRatio
        Sets resident-ratio guardrails for couchstore and/or magma buckets.
        """
        api = self.base_url + "/settings/resourceManagement/bucket/residentRatio"
        params = {}
        if couch_min_rr is not None:
            params['couchstoreMinimum'] = couch_min_rr
        if magma_min_rr is not None:
            params['magmaMinimum'] = magma_min_rr
        status, content, _ = self.request(api, self.POST, params=params)
        return status, content

    def set_max_data_per_bucket_guardrails(self, couch_max_data=None, magma_max_data=None):
        """
        POST /settings/resourceManagement/bucket/dataSizePerNode
        Sets max data-size-per-node guardrails for couchstore and/or magma buckets.
        """
        api = self.base_url + "/settings/resourceManagement/bucket/dataSizePerNode"
        params = {}
        if couch_max_data is not None:
            params['couchstoreMaximum'] = couch_max_data
        if magma_max_data is not None:
            params['magmaMaximum'] = magma_max_data
        status, content, _ = self.request(api, self.POST, params=params)
        return status, content

    def set_max_disk_usage_guardrails(self, max_disk_usage):
        """
        POST /settings/resourceManagement/diskUsage
        Sets the maximum disk usage guardrail.
        """
        api = self.base_url + "/settings/resourceManagement/diskUsage"
        params = {'maximum': max_disk_usage}
        status, content, _ = self.request(api, self.POST, params=params)
        return status, content
