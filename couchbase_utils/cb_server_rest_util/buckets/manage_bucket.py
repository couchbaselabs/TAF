from requests.utils import quote

from cb_server_rest_util.connection import CBRestConnection


class BucketManageAPI(CBRestConnection):
    def __init__(self):
        super(BucketManageAPI, self).__init__()

    def get_available_sample_buckets(self):
        """
        docs.couchbase.com/server/current/rest-api/rest-sample-buckets.html
        GET :: /sampleBuckets
        """
        api = f"{self.base_url}/sampleBuckets"
        status, content, _ = self.request(api)
        return status, content

    def enable_bucket_encryption(self, bucket, secret_id):
        """
        POST :: /pools/default/buckets/<bucket>
        """
        api = f"{self.base_url}/pools/default/buckets/{bucket}"
        params = {'encryptionAtRestSecretId': secret_id}
        status, json_parsed, _ = self.request(api, method='POST',
                                              params=params)
        return status, json_parsed

    def disable_bucket_encryption(self, bucket):
        """
        POST :: /pools/default/buckets/<bucket>
        """
        api = f"{self.base_url}/pools/default/buckets/{bucket}"
        params = {'encryptionAtRestSecretId': '-1'}
        status, json_parsed, _ = self.request(api, method='POST',
                                              params=params)
        return status, json_parsed

    def load_sample_bucket(self, bucket_name_list):
        """
        docs.couchbase.com/server/current/rest-api/rest-sample-buckets.html
        POST :: /sampleBuckets/install
        """
        param = self.flatten_param_to_str(bucket_name_list)
        api = f"{self.base_url}/sampleBuckets/install"
        status, content, _ = self.request(api, self.POST, params=param)
        return status, content

    def create_bucket(self, bucket_params):
        """
        POST :: /pools/default/buckets
        docs.couchbase.com/server/current/rest-api/rest-bucket-create.html
        """
        api = f"{self.base_url}/pools/default/buckets"
        status, _, response = self.request(api, self.POST,
                                           params=bucket_params)
        return status, response

    def edit_bucket(self, bucket_name, bucket_params):
        """
        POST :: /pools/default/buckets/<bucket_name>
        docs.couchbase.com/server/current/rest-api/rest-bucket-create.html
        """
        bucket_name = quote(bucket_name)
        api = self.base_url + f"/pools/default/buckets/{bucket_name}"
        status, content, _ = self.request(api, self.POST,
                                          params=bucket_params)
        return status, content

    def delete_bucket(self, bucket_name):
        """
        DELETE :: /pools/default/buckets/<bucket_name>
        docs.couchbase.com/server/current/rest-api/rest-bucket-delete.html
        """
        bucket_name = quote(bucket_name)
        api = self.base_url + f"/pools/default/buckets/{bucket_name}"
        status, content, _ = self.request(api, self.DELETE)
        return status, content

    def compact_bucket(self, bucket_name):
        """
        No documentation present
        POST :: /pools/default/buckets/<bucket_name>/controller/compactBucket
        """
        bucket_name = quote(bucket_name)
        api = self.base_url + f"/pools/default/buckets/{bucket_name}" \
            + "/controller/compactBucket"
        status, content, _ = self.request(api, self.POST)
        return status, content

    def cancel_compaction(self, bucket_name):
        """
        No documentation present
        POST :: /pools/default/buckets/<bucket_name>/controller/cancelBucketCompaction
        """
        bucket_name = quote(bucket_name)
        api = self.base_url + f"/pools/default/buckets/{bucket_name}" \
            + "/controller/cancelBucketCompaction"
        status, content, _ = self.request(api, self.POST)
        return status, content

    def flush_bucket(self, bucket_name):
        """
        docs.couchbase.com/server/current/rest-api/rest-bucket-flush.html
        POST :: /pools/default/buckets/<bucket_name>/controller/
        """
        bucket_name = quote(bucket_name)
        api = self.base_url \
            + f"/pools/default/buckets/{bucket_name}/controller/doFlush"
        status, content, _ = self.request(api, self.POST)
        return status, content

    def set_auto_compaction(self, bucket_name,
                            parallel_db_and_vc="false",
                            db_fragment_threshold=None,
                            view_fragment_threshold=None,
                            db_fragment_threshold_percentage=None,
                            view_fragment_threshold_percentage=None,
                            allowed_time_period_from_hour=None,
                            allowed_time_period_from_min=None,
                            allowed_time_period_to_hour=None,
                            allowed_time_period_to_min=None,
                            allowed_time_period_abort=None):
        """
        POST /pools/default/buckets/<bucket_name>
        docs.couchbase.com/server/current/rest-api/rest-bucket-create.html#auto-compaction-parameters
        """
        params = dict()
        if bucket_name:
            # overriding per bucket compaction setting
            api = f"{self.base_url}/pools/default/buckets/{quote(bucket_name)}"
            params["autoCompactionDefined"] = "true"
        else:
            # setting is cluster wide
            api = f"{self.base_url}/controller/setAutoCompaction"

        params["parallelDBAndViewCompaction"] = parallel_db_and_vc
        # Need to verify None because the value could be = 0
        if db_fragment_threshold is not None:
            params["databaseFragmentationThreshold[size]"] = \
                db_fragment_threshold
        if view_fragment_threshold is not None:
            params[
                "viewFragmentationThreshold[size]"] = view_fragment_threshold
        if db_fragment_threshold_percentage is not None:
            params["databaseFragmentationThreshold[percentage]"] = \
                db_fragment_threshold_percentage
        if view_fragment_threshold_percentage is not None:
            params["viewFragmentationThreshold[percentage]"] = \
                view_fragment_threshold_percentage
        if allowed_time_period_from_hour is not None:
            params[
                "allowedTimePeriod[fromHour]"] = allowed_time_period_from_hour
        if allowed_time_period_from_min is not None:
            params[
                "allowedTimePeriod[fromMinute]"] = allowed_time_period_from_min
        if allowed_time_period_to_hour is not None:
            params["allowedTimePeriod[toHour]"] = allowed_time_period_to_hour
        if allowed_time_period_to_min is not None:
            params["allowedTimePeriod[toMinute]"] = allowed_time_period_to_min
        if allowed_time_period_abort is not None:
            params[
                "allowedTimePeriod[abortOutside]"] = allowed_time_period_abort

        status, content, _ = self.request(api, self.POST, params)
        return status, content
