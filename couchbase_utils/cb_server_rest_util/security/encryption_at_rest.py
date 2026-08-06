"""
https://docs.couchbase.com/server/current/rest-api/security/encryption-at-rest/encryption-at-rest.html
"""
from cb_server_rest_util.connection import CBRestConnection


class EncryptionAtRest(CBRestConnection):
    def __init__(self):
        super(EncryptionAtRest, self).__init__()

    def list_encryption_at_rest_keys(self):
        """
        GET /settings/encryptionKeys/
        https://docs.couchbase.com/server/current/rest-api/security/encryption-at-rest/manage-encryption-keys.html#list-keys
        """
        api = self.base_url + "/settings/encryptionKeys"
        status, content, _ = self.request(api, self.GET)
        return status, content

    def get_encryption_at_rest_key(self, key_id):
        """
        GET /settings/encryptionKeys/{KEY_ID}
        https://docs.couchbase.com/server/current/rest-api/security/encryption-at-rest/manage-encryption-keys.html#list-keys
        """
        api = self.base_url + f"/settings/encryptionKeys/{key_id}"
        status, content, _ = self.request(api, self.GET)
        return status, content

    def create_encryption_at_rest_key(self, params):
        """
        POST /settings/encryptionKeys
        https://docs.couchbase.com/server/current/rest-api/security/encryption-at-rest/manage-encryption-keys.html#create-key
        """
        api = self.base_url + '/settings/encryptionKeys'
        status, content, _ = self.request(api, method=self.POST, params=params)
        return status, content

    def update_encryption_at_rest_key(self, key_id, params):
        """
        PUT /settings/encryptionKeys/{KEY_ID}
        https://docs.couchbase.com/server/current/rest-api/security/encryption-at-rest/manage-encryption-keys.html#create-key
        """
        api = self.base_url + f'/settings/encryptionKeys/{key_id}'
        status, content, _ = self.request(api, method=self.PUT, params=params)
        return status, content

    def delete_encryption_at_rest_key(self, key_id):
        """
        DELETE /settings/encryptionKeys/{KEY_ID}
        https://docs.couchbase.com/server/current/rest-api/security/encryption-at-rest/manage-encryption-keys.html#delete-key
        """
        api = self.base_url + f'/settings/encryptionKeys/{key_id}'
        status, content, _ = self.request(api, method=self.DELETE)
        return status, content

    def test_encryption_at_rest_key(self, key_id):
        """
        POST /settings/encryptionKeys/{KEY_ID}/test
        https://docs.couchbase.com/server/current/rest-api/security/encryption-at-rest/manage-encryption-keys.html#test-key
        """
        api = self.base_url + f'/settings/encryptionKeys/{key_id}/test'
        status, content, _ = self.request(api, method=self.POST)
        return status, content

    def test_encryption_at_rest_key_changes(self, key_id, params):
        """
        PUT /settings/encryptionKeys/{KEY_ID}/test
        https://docs.couchbase.com/server/current/rest-api/security/encryption-at-rest/manage-encryption-keys.html#test-key-changes
        :param params: updated key config (name, type, data, usage)
        """
        api = self.base_url + f'/settings/encryptionKeys/{key_id}/test'
        status, content, _ = self.request(api, method=self.PUT, params=params)
        return status, content

    def get_encryption_at_rest_settings(self):
        """
        GET /settings/security/encryptionAtRest
        https://docs.couchbase.com/server/current/rest-api/security/encryption-at-rest/manage-system-encryption-at-rest.html#get-settings
        Returns audit, config, and log encryption-at-rest settings.
        """
        api = self.base_url + "/settings/security/encryptionAtRest"
        status, content, _ = self.request(api, self.GET)
        return status, content

    def set_encryption_at_rest_settings(self, params):
        """
        POST /settings/security/encryptionAtRest
        https://docs.couchbase.com/server/current/rest-api/security/encryption-at-rest/manage-system-encryption-at-rest.html#change-settings
        :param params: dict with keys like audit.encryptionMethod, config.encryptionKeyId, etc.
        """
        api = self.base_url + "/settings/security/encryptionAtRest"
        status, content, _ = self.request(api, method=self.POST, params=params)
        return status, content

    def drop_encryption_deks_for_bucket(self, bucket_name):
        """
        POST /controller/dropEncryptionAtRestDeks/bucket/{BUCKET_NAME}
        https://docs.couchbase.com/server/current/rest-api/security/encryption-at-rest/drop-encryption-deks.html#drop-bucket
        Rotates DEKs for a bucket and re-encrypts its data.
        """
        api = self.base_url + f"/controller/dropEncryptionAtRestDeks/bucket/{bucket_name}"
        status, content, _ = self.request(api, method=self.POST)
        return status, content

    def drop_encryption_deks_for_type(self, data_type):
        """
        POST /controller/dropEncryptionAtRestDeks/{TYPE}
        https://docs.couchbase.com/server/current/rest-api/security/encryption-at-rest/drop-encryption-deks.html#drop-type
        :param data_type: "audit" | "config" | "log"
        """
        api = self.base_url + f"/controller/dropEncryptionAtRestDeks/{data_type}"
        status, content, _ = self.request(api, method=self.POST)
        return status, content

    def rotate_encryption_at_rest_key(self, key_id):
        """
        POST /controller/rotateEncryptionKey/{KEY_ID}
        https://docs.couchbase.com/server/current/rest-api/security/encryption-at-rest/rotate-encryption-at-rest-key.html#rotate-key
        Creates a new key version and re-encrypts all DEKs encrypted with the previous version.
        """
        api = self.base_url + f"/controller/rotateEncryptionKey/{key_id}"
        status, content, _ = self.request(api, method=self.POST)
        return status, content

    def force_encryption_at_rest_for_bucket(self, bucket_name):
        """
        POST /controller/forceEncryptionAtRest/bucket/{BUCKET_NAME}
        https://docs.couchbase.com/server/current/rest-api/security/encryption-at-rest/force-encryption-at-rest.html#bucket
        Forces encryption of unencrypted data in a bucket.
        """
        api = self.base_url + f"/controller/forceEncryptionAtRest/bucket/{bucket_name}"
        status, content, _ = self.request(api, method=self.POST)
        return status, content

    def force_encryption_at_rest_for_type(self, data_type):
        """
        POST /controller/forceEncryptionAtRest/{TYPE}
        https://docs.couchbase.com/server/current/rest-api/security/encryption-at-rest/force-encryption-at-rest.html#type
        :param data_type: "audit" | "config" | "log"
        """
        api = self.base_url + f"/controller/forceEncryptionAtRest/{data_type}"
        status, content, _ = self.request(api, method=self.POST)
        return status, content
