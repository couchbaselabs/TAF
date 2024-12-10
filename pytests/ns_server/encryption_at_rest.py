import json
from pytests.bucket_collections.collections_base import CollectionBase
from BucketLib.BucketOperations import BucketHelper
from remote.remote_util import RemoteMachineShellConnection
from datetime import datetime
from membase.api.rest_client import RestConnection


class EncryptionAtRest(CollectionBase):
    def setUp(self):
        super(EncryptionAtRest, self).setUp()

    def tearDown(self):
        super(EncryptionAtRest, self).tearDown()

    def is_encrypted(self, file_path, shell):
        command = "cat " + file_path
        output, _ = shell.execute_command(command)
        data = ''.join(output)
        entropy = CollectionBase.calculate_entropy(data)
        return entropy > 7.5

    def test_bucket_encryption_auto_rotation(self):
        bucket_helper = BucketHelper(self.cluster.master)
        rest = RestConnection(self.cluster.master)
        for bucket in self.cluster.buckets:
            self.log.info(
                "Creating a new secret with auto-rotation off for bucket: %s" % bucket.name)

            # Create secret params with auto-rotation off and next rotation set to a minute later
            params = bucket_helper.create_secret_params(
                secret_type="auto-generated-aes-key-256",
                name="UTestSecretAutoRotationOff",
                usage=["bucket-encryption-*"],
                autoRotation=False,
                rotationIntervalInSeconds=60
            )
            initial_creation_time = datetime.now().isoformat()
            status, response = rest.create_secret(params)
            generated_key = ""
            secret_id = None
            if status:
                response_dict = json.loads(response)
                generated_key = \
                response_dict.get('data', {}).get('keys', [{}])[0].get('id')
                secret_id = response_dict.get('id')

            self.log.info(
                "Setting encryption values for bucket: %s" % bucket.name)
            bucket_helper.change_bucket_props(
                bucket,
                encryptionAtRestSecretId=generated_key,
                encryptionAtRestDekRotationInterval=60,
                encryptionAtRestDekLifetime=7776000
            )

            self.log.info("Waiting for 1 minute to check auto-rotation")
            self.sleep(60)

            status, secrets = rest.get_all_secrets()
            self.assertTrue(status, "Failed to fetch secrets")

            secret_found = False
            # might need logic revaluation once rotation is working
            for secret in secrets:
                if secret['id'] == secret_id:
                    secret_found = True
                    self.assertTrue(
                        secret['data']['autoRotation'] == False,
                        "Auto-rotation status mismatch for secret: %s" %
                        secret['id'])
                    # Check if a new key has been generated
                    keys = secret['data']['keys']
                    self.assertTrue(len(keys) > 1,
                                    "Key rotation did not occur as expected for secret: %s" %
                                    secret['id'])

                    latest_creation_time = keys[-1]['creationDateTime']
                    self.assertTrue(
                        latest_creation_time > initial_creation_time,
                        "Key rotation did not occur as expected for secret: %s" %
                        secret['id'])
                    break

            self.assertTrue(secret_found,
                            "Secret with ID %s not found" % secret_id)

            # Delete the secret created for the test
            if secret_id is not None:
                self.log.info(
                    "Deleting the secret created for the test: %s" % secret_id)
                delete_status, _ = rest.delete_secret(secret_id)
                self.assertTrue(delete_status,
                                "Failed to delete the secret: %s" % secret_id)

            self.log.info(
                "Completed auto-rotation test for bucket: %s" % bucket.name)

    def test_bucket_encryption_manual_rotation(self):
        bucket_helper = BucketHelper(self.cluster.master)
        rest = RestConnection(self.cluster.master)
        for bucket in self.cluster.buckets:
            self.log.info(
                "Creating a new secret with auto-rotation off for bucket: %s" % bucket.name)

            # Create secret params with auto-rotation off
            params = bucket_helper.create_secret_params(
                secret_type="auto-generated-aes-key-256",
                name="UTestSecretManualRotation",
                usage=["bucket-encryption-*"],
                autoRotation=False,
                rotationIntervalInSeconds=600
            )
            initial_creation_time = datetime.now().isoformat()
            status, response = rest.create_secret(params)
            generated_key = ""
            secret_id = None
            if status:
                response_dict = json.loads(response)
                generated_key = \
                    response_dict.get('data', {}).get('keys', [{}])[0].get(
                        'id')
                secret_id = response_dict.get('id')

            self.log.info(
                "Setting encryption values for bucket: %s" % bucket.name)
            bucket_helper.change_bucket_props(
                bucket,
                encryptionAtRestSecretId=generated_key,
                encryptionAtRestDekRotationInterval=600,
                encryptionAtRestDekLifetime=7776000
            )

            self.log.info(
                "Triggering manual rotation for secret: %s" % secret_id)
            rotation_status, _ = rest.trigger_kek_rotation(secret_id)
            self.assertTrue(rotation_status,
                            "Failed to trigger manual rotation for secret: %s" % secret_id)

            self.log.info(
                "Waiting for a few seconds to allow rotation to complete")
            self.sleep(10)

            status, secrets = rest.get_all_secrets()
            self.assertTrue(status, "Failed to fetch secrets")

            secret_found = False
            for secret in secrets:
                if secret['id'] == secret_id:
                    secret_found = True
                    self.assertTrue(
                        secret['data']['autoRotation'] == False,
                        "Auto-rotation status mismatch for secret: %s" %
                        secret['id'])
                    # Check if a new key has been generated
                    keys = secret['data']['keys']
                    self.assertTrue(len(keys) > 1,
                                    "Key rotation did not occur as expected for secret: %s" %
                                    secret['id'])

                    latest_creation_time = keys[-1]['creationDateTime']
                    self.assertTrue(
                        latest_creation_time > initial_creation_time,
                        "Key rotation did not occur as expected for secret: %s" %
                        secret['id'])
                    break

            self.assertTrue(secret_found,
                            "Secret with ID %s not found" % secret_id)

            # Delete the secret created for the test
            if secret_id is not None:
                self.log.info(
                    "Deleting the secret created for the test: %s" % secret_id)
                delete_status, _ = rest.delete_secret(secret_id)
                self.assertTrue(delete_status,
                                "Failed to delete the secret: %s" % secret_id)

            self.log.info(
                "Completed manual rotation test for bucket: %s" % bucket.name)

    def test_entropy_for_encrypted_files(self):
        bucket_helper = BucketHelper(self.cluster.master)
        rest = RestConnection(self.cluster.master)
        for bucket in self.cluster.buckets:
            self.log.info(
                "Creating a new secret with auto-rotation off for bucket: %s" % bucket.name)

            params = bucket_helper.create_secret_params(
                secret_type="auto-generated-aes-key-256",
                name="UTestSecretManualRotation",
                usage=["bucket-encryption-*"],
                autoRotation=False,
                rotationIntervalInSeconds=600
            )
            initial_creation_time = datetime.now().isoformat()
            status, response = rest.create_secret(params)
            generated_key = ""
            secret_id = None
            if status:
                response_dict = json.loads(response)
                generated_key = \
                    response_dict.get('data', {}).get('keys', [{}])[0].get(
                        'id')
                secret_id = response_dict.get('id')

            self.log.info(
                "Setting encryption values for bucket: %s" % bucket.name)
            bucket_helper.change_bucket_props(
                bucket,
                encryptionAtRestSecretId=generated_key,
                encryptionAtRestDekRotationInterval=600,
                encryptionAtRestDekLifetime=7776000
            )
        for node in self.cluster.nodes_in_cluster:
            shell = RemoteMachineShellConnection(node)
            data_dir = "/opt/couchbase/var/lib/couchbase/data"
            command = "find " + data_dir + " -type f"
            output, _ = shell.execute_command(command)
            files = output.splitlines()
            for file_path in files:
                if self.is_encrypted(file_path, shell):
                    print("The file {} is likely encrypted.".format(file_path))
                else:
                    print("The file {} is not encrypted.".format(file_path))
            shell.close()

