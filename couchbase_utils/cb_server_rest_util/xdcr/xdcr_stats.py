from requests.utils import quote
from cb_server_rest_util.connection import CBRestConnection


class XDCRStatisticsAPI(CBRestConnection):
    def __init__(self):
        super(XDCRStatisticsAPI, self).__init__()

    def get_xdcr_stats(self, bucket_name, zoom="minute"):
        """
        GET :: /pools/default/buckets/@xdcr-<bucket_name>/stats
        :param bucket_name: Bucket name to fetch XDCR stats
        :param zoom: minute | hour | day | week | month | year
        Returns XDCR replication statistics including replication_changes_left
        """
        bucket_name = quote(bucket_name)
        api = self.base_url + f"/pools/default/buckets/@xdcr-{bucket_name}/stats"
        if zoom:
            api += f"?zoom={zoom}"
        status, content, _ = self.request(api)
        return status, content