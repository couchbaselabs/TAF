"""
Created on June 09, 2026
"""

from pytests.Capella.RestAPIv4.api_base import APIBase


class ModelApiKeysBase(APIBase):

    def setUp(self, nomenclature="ModelApiKeys_Base"):
        APIBase.setUp(self, nomenclature)
        self.model_api_keys_endpoint_default = \
            "/v4/organizations/{}/aiServices/models/apiKeys"
        self.model_api_key_endpoint_default = \
            self.model_api_keys_endpoint_default + "/{}"
        self.region = "us-east-1"
        self.created_key_ids = set()
        self.key_prefix = "mk_{}".format(
            self.generate_random_string(8, special_characters=False).lower())

    def tearDown(self):
        self.update_auth_with_api_token(self.curr_owner_key)
        for key_id in list(self.created_key_ids):
            self.cleanup_model_api_key(key_id)
        super(ModelApiKeysBase, self).tearDown()

    def api_call_with_retry(self, method, *args, **kwargs):
        result = method(*args, **kwargs)
        if result.status_code == 429:
            self.handle_rate_limit(int(result.headers["Retry-After"]))
            result = method(*args, **kwargs)
        return result

    def build_payload(self, name=None, description=None, expiry=180,
                      allowed_cidrs=None, allowed_models=None,
                      region=None):
        return {
            "name": name or self.key_prefix,
            "description": description or "Automation test model API key",
            "expiry": expiry,
            "allowedCIDRs": allowed_cidrs or ["0.0.0.0/0"],
            "region": region or self.region,
            "allowedModels": allowed_models if allowed_models is not None
            else []
        }

    def create_model_api_key(self, name=None, payload=None, headers=None):
        payload = payload or self.build_payload(name=name)
        result = self.api_call_with_retry(
            self.capellaAPI.org_ops_apis.create_model_api_key,
            self.organisation_id, payload, headers=headers)
        if result.status_code == 201:
            key_id = result.json().get("id")
            if key_id:
                self.created_key_ids.add(key_id)
        return result

    def cleanup_model_api_key(self, key_id):
        self.api_call_with_retry(
            self.capellaAPI.org_ops_apis.delete_model_api_key,
            self.organisation_id, key_id)

    def ensure_model_api_key_exists(self, name=None):
        result = self.create_model_api_key(name=name)
        if result.status_code not in [201, 409]:
            self.log.error(result.content)
            self.fail("Unable to create model API key")
        return result.json().get("id")

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
