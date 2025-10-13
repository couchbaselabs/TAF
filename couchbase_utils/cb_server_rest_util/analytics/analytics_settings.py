"""
https://docs.couchbase.com/server/current/analytics/rest-settings.html
"""
from cb_server_rest_util.connection import CBRestConnection


class AnalyticsSettingsAPI(CBRestConnection):
    def __init__(self):
        super(AnalyticsSettingsAPI, self).__init__()

    def set_analytics_debug_settings_in_metakv(
            self, setting_name, setting_value):
        """
        PUT /_metakv/cbas/debug/settings/
        This is an undocumented API.
        """
        api = self.base_url + "/_metakv/cbas/debug/settings/{0}".format(
            setting_name)
        headers = {'Content-Type': "application/x-www-form-urlencoded"}
        param = {"value": setting_value}
        status, content, _ = self.request(
            api=api, method=self.PUT, params=param, headers=headers)
        return status, content

    def set_blob_storage_access_key_id(self, access_key_id):
        """
        Method sets up credentials for accessing blob storage.
        This is an undocumented API.
        """
        self.log.info("Adding blob storage access key")
        return self.set_analytics_debug_settings_in_metakv(
            setting_name="blob_storage_access_key_id",
            setting_value=access_key_id)

    def set_blob_storage_secret_access_key(self, secret_access_key):
        """
        Method sets up credentials for accessing blob storage.
        This is an undocumented API.
        """
        self.log.info("Adding blob storage secret access key")
        return self.set_analytics_debug_settings_in_metakv(
            setting_name="blob_storage_secret_access_key",
            setting_value=secret_access_key)

    def get_analytics_settings(self):
        """
        GET /settings/analytics
        https://docs.couchbase.com/server/current/analytics/rest-settings.html
        """
        api = self.base_url + "/settings/analytics"
        status, content, _ = self.request(api, self.GET)
        return status, content

    def update_analytics_settings(
            self, num_replicas=None, blob_storage_region=None,
            blob_storage_prefix=None, blob_storage_bucket=None,
            blob_storage_scheme=None, profile=None,
            endpoint_url=None, blob_storage_list_eventually_consistent=False,
            blob_storage_force_path_style=False):
        """
        POST /settings/analytics
        https://docs.couchbase.com/server/current/analytics/rest-settings.html
        Following setting are not released, but are used to configure
        compute storage separation on columnar server builds.
        blob_storage_region : Region in which the blob storage is present.
        For AWS it is the AWS region in which the S3 bucket is present.
        blob_storage_prefix : Empty string.
        blob_storage_bucket : Name of the blob storage bucket. For AWS it is
        the S3 bucket name.
        blob_storage_scheme : For now only "s3" is supported.
        """
        api = self.base_url + "/settings/analytics"
        params = {}
        if endpoint_url:
            params["blobStorageEndpoint"] = endpoint_url
        if num_replicas:
            params["numReplicas"] = num_replicas
        if blob_storage_region:
            params["blobStorageRegion"] = blob_storage_region
        if blob_storage_prefix is not None:
            params["blobStoragePrefix"] = blob_storage_prefix
        if blob_storage_bucket:
            params["blobStorageBucket"] = blob_storage_bucket
        if blob_storage_scheme:
            params["blobStorageScheme"] = blob_storage_scheme
        if blob_storage_list_eventually_consistent:
            params["blobStorageListEventuallyConsistent"] = "true"
        if blob_storage_force_path_style:
            params["blobStoragePathStyleAddressing"] = "true"
        status, content, _ = self.request(api, self.POST, params=params)
        return status, content
