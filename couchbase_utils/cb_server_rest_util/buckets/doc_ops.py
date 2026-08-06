from requests.utils import quote

from cb_server_rest_util.connection import CBRestConnection


class DocOpAPI(CBRestConnection):
    def __init__(self):
        super(DocOpAPI, self).__init__()

    def get_random_key(self, bucket_name):
        """
        GET /pools/default/buckets/<bucket_name>/localRandomKey
        No documentation present
        """
        bucket_name = quote(bucket_name)
        header = self.create_headers(content_type="application/json")
        api = (f'{self.base_url}/pools/default/buckets'
               f'/{bucket_name}/localRandomKey')
        status, content, _ = self.request(api, self.GET, headers=header)
        return status, content
