from cb_server_rest_util.buckets.bucket_info import BucketInfo
from cb_server_rest_util.buckets.bucket_stats import BucketStats
from cb_server_rest_util.buckets.doc_ops import DocOpAPI
from cb_server_rest_util.buckets.manage_bucket import BucketManageAPI
from cb_server_rest_util.buckets.scope_and_collections import \
    ScopeAndCollectionsAPI


class BucketRestApi(BucketManageAPI, BucketInfo, BucketStats,
                    DocOpAPI,
                    ScopeAndCollectionsAPI):
    def __init__(self, server):
        super(BucketRestApi, self).__init__()

        self.set_server_values(server)
        self.set_endpoint_urls(server)
