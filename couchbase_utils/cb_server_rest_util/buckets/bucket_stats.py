from requests.utils import quote
from cb_server_rest_util.connection import CBRestConnection


class BucketStats(CBRestConnection):
    def __init__(self):
        super(BucketStats, self).__init__()

    def get_bucket_stats(self, bucket_name, zoom=None):
        """
        :param bucket_name: Bucket name to fetch stats
        :param zoom: minute | hour | day | week | month | year
        GET :: /pools/default/buckets/<bucket-name>/stats
        docs.couchbase.com/server/current/rest-api/rest-bucket-stats.html
        """
        bucket_name = quote(bucket_name)
        api = self.base_url + f"/pools/default/buckets/{bucket_name}/stats"
        if zoom:
            api += f"?zoom={zoom}"
        status, content, _ = self.request(api)
        return status, content

    def get_bucket_stats_from_node(self, bucket_name, target_node, zoom=None):
        """
        GET :: /pools/default/buckets/<bucket_name>/nodes/<node-ip>:<port>/stats
        :param bucket_name: Bucket name to fetch stats
        :param target_node: The node containing the bucket
        :param zoom: minute | hour | day | week | month | year
        docs.couchbase.com/server/current/rest-api/rest-bucket-stats.html
        """
        bucket_name = quote(bucket_name)
        api = self.base_url + f"/pools/default/buckets/{bucket_name}/nodes/" \
                              f"{target_node.ip}:{target_node.port}/stats"
        if zoom:
            api += f"?zoom={zoom}"
        status, content, _ = self.request(api)
        return status, content

    def get_stats_range(self, params):
        """
        GET /pools/default/stats/range
        :param params: String format for a valid dict to represent the required
                       stats
        No documentation present
        """
        params = self.flatten_param_to_str(params)
        api = self.base_url + "/pools/default/stats/range"
        status, content, _ = self.request(api, params=params)
        return status, content
