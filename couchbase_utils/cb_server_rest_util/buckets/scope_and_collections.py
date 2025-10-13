from requests.utils import quote

from cb_server_rest_util.connection import CBRestConnection


class ScopeAndCollectionsAPI(CBRestConnection):
    def __init__(self):
        super(ScopeAndCollectionsAPI, self).__init__()

    def create_scope(self, bucket_name, scope_name):
        """
        docs.couchbase.com/server/current/rest-api/creating-a-scope.html
        POST :: /pools/default/buckets/<bucket_name>/scopes
        """
        bucket_name = quote(bucket_name)
        api = self.base_url + f"/pools/default/buckets/{bucket_name}/scopes"
        status, content, _ = self.request(api, self.POST,
                                          params={"name": scope_name})
        return status, content

    def drop_scope(self, bucket_name, scope_name):
        """
        docs.couchbase.com/server/current/rest-api/dropping-a-scope.html
        DELETE :: /pools/default/buckets/<bucket_name>/scopes
        """
        bucket_name = quote(bucket_name)
        scope_name = quote(scope_name)
        api = self.base_url \
            + f"/pools/default/buckets/{bucket_name}/scopes/{scope_name}"
        status, content, _ = self.request(api, self.DELETE)
        return status, content

    def create_collection(self, bucket_name, scope_name, collection_spec):
        """
        docs.couchbase.com/server/current/rest-api/creating-a-collection.html
        POST :: /pools/default/buckets/<bucket_name>/scopes/<scope_name>/collections
        """
        bucket_name = quote(bucket_name)
        scope_name = quote(scope_name)
        api = self.base_url + "/pools/default/buckets" \
            + f"/{bucket_name}/scopes/{scope_name}/collections"
        status, content, _ = self.request(api, self.POST,
                                          params=collection_spec)
        return status, content

    def drop_collection(self, bucket_name, scope_name, collection_name):
        """
        docs.couchbase.com/server/current/rest-api/dropping-a-collection.html
        DELETE :: /pools/default/buckets/<bucket_name>/scopes/<scope_name>/collections/<collection_name>
        """
        bucket_name = quote(bucket_name)
        scope_name = quote(scope_name)
        collection_name = quote(collection_name)
        api = self.base_url + f"/pools/default/buckets/{bucket_name}" \
            + f"/scopes/{scope_name}/collections/{collection_name}"
        status, content, _ = self.request(api, self.DELETE)
        return status, content

    def edit_collection(self, bucket_name, scope_name, collection_name,
                        collection_spec):
        """
        docs.couchbase.com/server/current/rest-api/creating-a-collection.html
        PATCH :: /pools/default/buckets/<bucket_name>/scopes/<scope_name>/collections/<collection_name>
        """
        bucket_name = quote(bucket_name)
        scope_name = quote(scope_name)
        api = self.base_url + f"/pools/default/buckets/{bucket_name}" \
            + f"/scopes/{scope_name}/collections/{collection_name}"
        status, content, _ = self.request(api, self.PATCH,
                                          params=collection_spec)
        return status, content

    def list_scope_collections(self, bucket_name):
        """
        docs.couchbase.com/server/current/rest-api/listing-scopes-and-collections.html
        GET :: /pools/default/buckets/<bucket_name>/scopes/
        """
        bucket_name = quote(bucket_name)
        api = self.base_url + f"/pools/default/buckets/{bucket_name}/scopes"
        status, content, _ = self.request(api, self.GET)
        return status, content

    def import_collection_using_manifest(self, bucket_name, manifest_data):
        """
        PUT /pools/default/buckets/<bucket_name>/scopes
        No documentation available
        """
        bucket_name = quote(bucket_name)
        header = self.create_headers(content_type="application/json")
        api = self.base_url + "/pools/default/buckets/%s/scopes" % bucket_name
        status, content, _ = self.request(api, self.PUT, manifest_data,
                                          headers=header)
        return status, content

    def wait_for_collections_warmup(self, bucket_name, uid, session=None):
        """
        POST /pools/default/buckets/<bucket_name>/scopes/@ensureManifest/<uid>
        No documentation available
        Will wait till the bucket manifest reaches the 'uid' (>=)
        """
        bucket_name = quote(bucket_name)
        api = self.base_url \
              + "/pools/default/buckets/%s/scopes/@ensureManifest/%s" \
              % (bucket_name, uid)
        status, content, _ = self.request(api, self.POST)
        return status, content
