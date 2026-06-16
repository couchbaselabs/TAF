"""
Created on June 04, 2026
"""

from pytests.Capella.RestAPIv4.Buckets.get_buckets import GetBucket


class EventingFunctionBase(GetBucket):

    def setUp(self, nomenclature="EventingFunctions_Base", create_function=False):
        self.created_function_names = set()
        GetBucket.setUp(self, nomenclature)
        self.eventing_functions_endpoint_default = \
            "/v4/organizations/{}/projects/{}/clusters/{}/eventingFunctions"
        self.eventing_function_endpoint_default = \
            self.eventing_functions_endpoint_default + "/{}"
        self.eventing_function_activation_state_endpoint_default = \
            self.eventing_function_endpoint_default + "/activationState"
        self.function_name = "evf_{}".format(
            self.generate_random_string(12, special_characters=False).lower())
        self.default_code = (
            "function OnUpdate(doc, meta, xattrs) { return; }\n"
            "function OnDelete(meta, options) { return; }"
        )
        self.metadata_bucket_name = "meta_{}".format(
            self.generate_random_string(10, special_characters=False).lower())
        self._create_metadata_bucket()
        if create_function:
            self.ensure_function_exists(self.function_name)

    def tearDown(self):
        self.update_auth_with_api_token(self.curr_owner_key)
        for function_name in list(self.created_function_names):
            self.cleanup_eventing_function(function_name)
        super(EventingFunctionBase, self).tearDown()

    def api_call_with_retry(self, method, *args, **kwargs):
        result = method(*args, **kwargs)
        if result.status_code == 429:
            self.handle_rate_limit(int(result.headers["Retry-After"]))
            result = method(*args, **kwargs)
        return result

    def _create_metadata_bucket(self):
        result = self.api_call_with_retry(
            self.capellaAPI.cluster_ops_apis.create_bucket,
            self.organisation_id, self.project_id, self.cluster_id,
            self.metadata_bucket_name,
            self.expected_res["type"],
            self.expected_res["storageBackend"],
            self.expected_res["memoryAllocationInMb"],
            self.expected_res["bucketConflictResolution"],
            self.expected_res["durabilityLevel"],
            self.expected_res["replicas"],
            self.expected_res["flush"],
            self.expected_res["timeToLiveInSeconds"]
        )
        if result.status_code != 201:
            self.log.error(result.content)
            self.fail("Unable to create metadata bucket for eventing test")
        self.buckets.append(result.json()["id"])

    def build_payload(self, function_name=None, code=None):
        function_name = function_name or self.function_name
        return {
            "name": function_name,
            "description": "Eventing API automation",
            "code": code or self.default_code,
            "eventSource": {
                "bucket": self.expected_res["name"],
                "scope": "_default",
                "collection": "_default"
            },
            "eventMetadataStorage": {
                "bucket": self.metadata_bucket_name,
                "scope": "_default",
                "collection": "_default"
            },
            "settings": {
                "workerCount": 1,
                "scriptTimeout": 60,
                "sqlConsistency": "none",
                "languageCompatibility": "7.2.0",
                "feedBoundary": "from_now",
                "maxTimerContextSize": 1024,
                "allowSyncDocuments": True,
                "cursorAware": False
            },
            "bindings": {
                "buckets": [],
                "urls": [],
                "constants": []
            }
        }

    def create_function(self, function_name=None, payload=None, headers=None):
        function_name = function_name or self.function_name
        payload = payload or self.build_payload(function_name=function_name)
        result = self.api_call_with_retry(
            self.capellaAPI.cluster_ops_apis.create_eventing_function_v4,
            self.organisation_id, self.project_id, self.cluster_id,
            payload, headers=headers)
        if result.status_code in [201, 409]:
            self.created_function_names.add(function_name)
        return result

    def ensure_function_exists(self, function_name):
        result = self.create_function(function_name=function_name)
        if result.status_code not in [201, 409]:
            self.log.error(result.content)
            self.fail("Unable to create eventing function")

    def cleanup_eventing_function(self, function_name):
        self.api_call_with_retry(
            self.capellaAPI.cluster_ops_apis.update_eventing_function_activation_state_v4,
            self.organisation_id, self.project_id, self.cluster_id,
            function_name, "undeploy")
        self.api_call_with_retry(
            self.capellaAPI.cluster_ops_apis.delete_eventing_function_v4,
            self.organisation_id, self.project_id, self.cluster_id,
            function_name)

    @staticmethod
    def expected_invalid_uuid_error():
        return {
            "code": 1000,
            "hint": "Check if you have provided a valid URL and all "
                    "the required params are present in the request body.",
            "httpStatusCode": 400,
            "message": "The server cannot or will not process the request "
                       "due to something that is perceived to be a client "
                       "error."
        }
