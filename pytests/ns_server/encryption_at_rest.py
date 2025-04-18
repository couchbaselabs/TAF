import json
import threading
from datetime import datetime
from collections_helper.collections_spec_constants import MetaCrudParams
from BucketLib.bucket import Bucket
from pytests.bucket_collections.collections_base import CollectionBase
from SystemEventLogLib.SystemEventOperations import SystemEventRestHelper
from BucketLib.BucketOperations import BucketHelper
from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection


class EncryptionAtRest(CollectionBase):
    def setUp(self):
        super(EncryptionAtRest, self).setUp()
        self.enable_log_and_config_encryption()

    def tearDown(self):
        super(EncryptionAtRest, self).tearDown()

    def enable_log_and_config_encryption(self):
        rest = RestConnection(self.cluster.master)
        bucket_helper = BucketHelper(self.cluster.master)
        self.log_secret_name = "TestSecretLogEncryption"
        self.conf_secret_name = "TestSecretConfigEncryption"
        # Create secret for log encryption
        log_params = bucket_helper.create_secret_params(
            secret_type="auto-generated-aes-key-256",
            name=self.log_secret_name,
            usage=["log-encryption"],
            autoRotation=True,
            rotationIntervalInSeconds=60
        )
        status, response = rest.create_secret(log_params)
        if status:
            response_dict = json.loads(response)
            self.temp_log_secret_id = response_dict.get('id')
        else:
            self.fail("Failed to create log encryption secret: %s" % response)

        # Create secret for config encryption
        config_params = bucket_helper.create_secret_params(
            secret_type="auto-generated-aes-key-256",
            name=self.conf_secret_name,
            usage=["config-encryption"],
            autoRotation=True,
            rotationIntervalInSeconds=60
        )
        status, response = rest.create_secret(config_params)
        if status:
            response_dict = json.loads(response)
            self.temp_config_secret_id = response_dict.get('id')
        else:
            self.fail("Failed to create config encryption secret: %s" % response)

        # Enable log and config encryption
        params = {
            "log.encryptionMethod": "encryptionKey",
            "log.encryptionKeyId": self.temp_log_secret_id,
            "config.encryptionMethod": "encryptionKey",
            "config.encryptionKeyId": self.temp_config_secret_id
        }
        status, response = rest.configure_encryption_at_rest(params)
        if not status:
            self.fail("Failed to enable log and config encryption: %s" % response)
        self.sleep(10, "Sleep after enabling log and config encryption")

    def crash(self, nodes=None, kill_itr=2, graceful=False):
        nodes = nodes or self.cluster.nodes_in_cluster
        count = kill_itr
        loop_itr = 0
        connections = dict()
        for node in nodes:
            shell = RemoteMachineShellConnection(node)
            connections.update({node: shell})

        while count > 0:
            loop_itr += 1
            second_iter = 5
            for node, shell in connections.items():
                if graceful:
                    shell.restart_couchbase()
                else:
                    shell.kill_memcached()
                    self.sleep(second_iter,
                               "Sleep before killing memcached on same node again.")
            count -= 1
            second_iter += 3

        for node, shell in connections.items():
            shell.disconnect()

    def __perform_doc_ops(self, durability=None, validate_num_items=False,
                          async_load=False):
        load_spec = \
            self.bucket_util.get_crud_template_from_package(
                "def_add_collection")
        load_spec["doc_crud"][
            MetaCrudParams.DocCrud.NUM_ITEMS_FOR_NEW_COLLECTIONS] = 10000
        if durability and self.num_replicas != Bucket.ReplicaNum.THREE:
            load_spec[MetaCrudParams.DURABILITY_LEVEL] = durability

        self.log.info("Performing doc_ops with durability level=%s"
                      % load_spec[MetaCrudParams.DURABILITY_LEVEL])
        doc_loading_task = \
            self.bucket_util.run_scenario_from_spec(
                self.task,
                self.cluster,
                self.cluster.buckets,
                load_spec,
                mutation_num=0,
                async_load=async_load,
                validate_task=False,
                batch_size=self.batch_size,
                process_concurrency=self.process_concurrency)

        if validate_num_items is True and doc_loading_task.result is False:
            self.fail("Collection CRUDs failure")

        if validate_num_items:
            self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                         self.cluster.buckets,
                                                         timeout=1200)
            self.bucket_util.validate_docs_per_collections_all_buckets(
                self.cluster)

    def test_bucket_encryption_auto_rotation(self):
        bucket_helper = BucketHelper(self.cluster.master)
        rest = RestConnection(self.cluster.master)

        # Create secret params with auto-rotation on
        params = bucket_helper.create_secret_params(
            secret_type="auto-generated-aes-key-256",
            name="TestSecretAutoRotationOn",
            usage=["bucket-encryption-*"],
            autoRotation=True,
            rotationIntervalInSeconds=60
        )
        initial_creation_time = datetime.now().isoformat()
        status, response = rest.create_secret(params)
        secret_id = None
        if status:
            response_dict = json.loads(response)
            secret_id = response_dict.get('id')
        params = bucket_helper.create_secret_params(
            secret_type="auto-generated-aes-key-256",
            name=self.log_secret_name,
            usage=["log-encryption"],
            autoRotation=True,
            rotationIntervalInSeconds=60
        )
        rest.modify_secret(self.temp_log_secret_id, params)

        params = bucket_helper.create_secret_params(
            secret_type="auto-generated-aes-key-256",
            name=self.conf_secret_name,
            usage=["config-encryption"],
            autoRotation=True,
            rotationIntervalInSeconds=60
        )
        rest.modify_secret(self.temp_config_secret_id, params)

        for bucket in self.cluster.buckets:
            bucket_helper.change_bucket_props(
                bucket,
                encryptionAtRestKeyId=secret_id,
                encryptionAtRestDekRotationInterval=3,
                encryptionAtRestDekLifetime=2
            )

        self.sleep(60)
        status, secrets_response = rest.get_all_secrets()
        secrets = json.loads(secrets_response)
        self.assertTrue(status, "Failed to fetch secrets")

        secret_found = False
        for secret in secrets:
            if secret['id'] == secret_id or secret['id'] == \
                    self.temp_config_secret_id or secret['id'] == self.temp_log_secret_id:
                secret_found = True
                self.assertTrue(secret['data']['autoRotation'] == True, "Auto-rotation status mismatch")
                keys = secret['data']['keys']
                self.assertTrue(len(keys) > 1, "Key rotation did not occur")

                # Identify the new and old keys based on creationDateTime
                keys_sorted = sorted(keys, key=lambda k: k['creationDateTime'])
                old_key = keys_sorted[0]
                new_key = keys_sorted[-1]

                self.assertTrue(new_key['creationDateTime'] > initial_creation_time, "Key rotation did not occur")

                # Check if the new key is active and the original key is inactive
                self.assertTrue(new_key['active'], "New key is not active")
                self.assertFalse(old_key['active'], "Original key is still active")
                delete_status, _ = rest.delete_secret(secret['id'])
                self.assertFalse(delete_status,
                                 "Secret gets deleted even though it's in use for ID: %s" % secret_id)

        self.assertTrue(secret_found, "Secret with ID %s not found" % secret_id)

        for bucket in self.cluster.buckets:
            bucket_helper.change_bucket_props(
                bucket,
                encryptionAtRestKeyId=-1,
            )
        delete_status, _ = rest.delete_secret(secret_id)
        self.assertTrue(delete_status, "Failed to delete the secret: %s" % secret_id)

    def test_bucket_encryption_at_rest(self):
        bucket_helper = BucketHelper(self.cluster.master)
        rest = RestConnection(self.cluster.master)

        params = bucket_helper.create_secret_params(
            secret_type="auto-generated-aes-key-256",
            name="TestSecretEncryptionAtRest",
            usage=["bucket-encryption-*"],
            autoRotation=True,
            rotationIntervalInSeconds=60
        )
        doc_id = "test_collections-"
        for node in self.cluster.nodes_in_cluster:
            shell = RemoteMachineShellConnection(node)
            rest_obj = RestConnection(node)
            node_config = rest_obj.get_nodes_self_unparsed()
            data_path = node_config['storage']['hdd'][0]['path']
            data_path_command = "grep -r '{}' {}".format(doc_id, data_path)
            output, error = shell.execute_command(data_path_command)
            self.assertTrue(len(output) != 0, "No document IDs found in the data path, test won't be accurate")
            shell.disconnect()

        status, response = rest.create_secret(params)
        secret_id = None
        if status:
            response_dict = json.loads(response)
            generated_key = response_dict.get('data', {}).get('keys', [{}])[0].get('id')
            secret_id = response_dict.get('id')

        for bucket in self.cluster.buckets:
            bucket_helper.change_bucket_props(
                bucket,
                encryptionAtRestKeyId=secret_id,
                encryptionAtRestDekRotationInterval=60,
                encryptionAtRestDekLifetime=7776000
            )

        for bucket in self.cluster.buckets:
            bucket_helper.compact_bucket(bucket.name)
            self.sleep(10)

        for node in self.cluster.nodes_in_cluster:
            shell = RemoteMachineShellConnection(node)
            rest_obj = RestConnection(node)
            node_config = rest_obj.get_nodes_self_unparsed()
            data_path = node_config['storage']['hdd'][0]['path']
            data_path_command = "grep -r '{}' {}".format(doc_id, data_path)
            output, error = shell.execute_command(data_path_command)
            self.assertEqual(len(output), 0, "Found document IDs in the data path, encryption at rest might not be working")
            shell.disconnect()

    def test_rapid_dek_rotation_with_crud_and_recovery(self):
        bucket_helper = BucketHelper(self.cluster.master)
        rest = RestConnection(self.cluster.master)
        self.graceful = self.input.param("graceful", "true")
        if self.graceful == "true":
            self.graceful = True
        else:
            self.graceful = False
        params = bucket_helper.create_secret_params(
            secret_type="auto-generated-aes-key-256",
            name="TestSecretRapidDEKRotation",
            usage=["bucket-encryption-*"],
            autoRotation=True,
            rotationIntervalInSeconds=120
        )

        status, response = rest.create_secret(params)
        secret_id = None
        if status:
            response_dict = json.loads(response)
            secret_id = response_dict.get('id')

        for bucket in self.cluster.buckets:
            bucket_helper.change_bucket_props(
                bucket,
                encryptionAtRestKeyId=secret_id,
                encryptionAtRestDekRotationInterval=1,
                encryptionAtRestDekLifetime=1
            )
        for bucket in self.cluster.buckets:
            self.bucket_util.flush_bucket(self.cluster, bucket)

        self.crash_th = threading.Thread(target=self.crash,
                                         kwargs=dict(graceful=self.graceful))
        self.crash_th.start()
        self.__perform_doc_ops("PERSIST_TO_MAJORITY")
        self.crash_th.join()

    def test_system_event_logs(self):

        def check_keys_values(event, search_json):
            for key, value in search_json.items():
                if isinstance(value, dict):
                    if not check_keys_values(event.get(key, {}), value):
                        return False
                elif event.get(key) != value:
                    return False
            return True

        def search_and_verify(events, to_verify):
            for verify in to_verify:
                flag = 0
                search_key = verify["search_key"]
                search_value = verify["search_value"]
                for event in events:
                    if event.get(search_key) == search_value:
                        if check_keys_values(event, verify["attributes"]):
                            flag = 1
                            break
                if flag == 0:
                    not_find.append(verify)

        self.event_rest_helper = SystemEventRestHelper(
            self.cluster.nodes_in_cluster)

        to_verify = [
            {
                "search_key": "description",
                "search_value": "Encryption at rest DEK rotated",
                "attributes": {
                    "extra_attributes": {
                        "kind": "logDek"
                    }
                }
            },
            {
                "search_key": "description",
                "search_value": "Encryption key changed",
                "attributes": {
                    "extra_attributes": {
                        "encryption_key_name": self.log_secret_name,
                        "new_settings": {
                            "data": {
                                "autoRotation": False,
                            }
                        }
                    }
                }
            },
            {
                "search_key": "description",
                "search_value": "Encryption key changed",
                "attributes": {
                    "extra_attributes": {
                        "encryption_key_name": self.conf_secret_name,
                        "new_settings": {
                            "data": {
                                "autoRotation": False,
                            }
                        }
                    }
                }
            },
            {
                "search_key": "description",
                "search_value": "Encryption key changed",
                "attributes": {
                    "extra_attributes": {
                        "encryption_key_name": "renamed_key",
                    }
                }
            },
            {
                "search_key": "description",
                "search_value": "Encryption at rest DEK rotated",
                "attributes": {
                    "extra_attributes": {
                        "kind": "configDek"
                    }
                }
            },
            {
                "search_key": "description",
                "search_value": "Encryption key created",
                "attributes": {
                    "extra_attributes": {
                        "encryption_key_name": self.conf_secret_name
                    }
                }
            },
            {
                "search_key": "description",
                "search_value": "Encryption key created",
                "attributes": {
                    "extra_attributes": {
                        "encryption_key_name": self.log_secret_name
                    }
                }
            },
            {
                "search_key": "description",
                "search_value": "Encryption key created",
                "attributes": {
                    "extra_attributes": {
                        "settings": {
                         "name": "EncryptionSecret"
                        }
                    }
                }
            },
            {
                "search_key": "description",
                "search_value": "Encryption at rest settings changed",
                "attributes": {
                    "extra_attributes": {
                        "new_settings": {
                            "config": {
                                "dekLifetime": 100,
                                "dekRotationInterval": 100,
                            },
                            "log": {
                                "dekLifetime": 0,
                                "dekRotationInterval": 100,
                            }
                        }
                    }
                }
            },
            {
                "search_key": "description",
                "search_value": "Encryption at rest settings changed",
                "attributes": {
                    "extra_attributes": {
                        "new_settings": {
                            "log": {
                                "encryptionMethod": "disabled"
                            }
                        }
                    }
                }
            },
            {
                "search_key": "description",
                "search_value": "Bucket configuration changed",
                "attributes": {
                    "extra_attributes": {
                        "new_settings": {
                            "encryption_dek_lifetime": 60,
                            "encryption_dek_rotation_interval": 60
                        }
                    }
                }
            }
        ]

        bucket_helper = BucketHelper(self.cluster.master)
        not_find = []
        rest = RestConnection(self.cluster.master)
        for bucket in self.cluster.buckets:
            bucket_helper.change_bucket_props(
                bucket,
                encryptionAtRestKeyId=0,
                encryptionAtRestDekRotationInterval=60,
                encryptionAtRestDekLifetime=60
            )
        params = bucket_helper.create_secret_params(
            secret_type="auto-generated-aes-key-256",
            name=self.log_secret_name,
            usage=["log-encryption"],
            autoRotation=False,
            rotationIntervalInSeconds=60
        )
        status, response = rest.modify_secret(self.temp_log_secret_id, params)
        self.assertTrue(status, "Failed to modify log secret")

        params = bucket_helper.create_secret_params(
            secret_type="auto-generated-aes-key-256",
            name=self.conf_secret_name,
            usage=["config-encryption"],
            autoRotation=False,
            rotationIntervalInSeconds=60
        )
        status, response = rest.modify_secret(self.temp_config_secret_id,
                                              params)
        self.assertTrue(status, "Failed to modify config secret")

        params = bucket_helper.create_secret_params(
            secret_type="auto-generated-aes-key-256",
            name="renamed_key",
            usage=["bucket-encryption-*"],
            autoRotation=False,
            rotationIntervalInSeconds=60
        )
        status, response = rest.modify_secret(0, params)
        self.assertTrue(status, "Failed to modify renamed key secret")

        params = {
            "log.encryptionMethod": "disabled",
            "log.encryptionKeyId": -1,
        }
        status, response = rest.configure_encryption_at_rest(params)
        self.assertTrue(status, "Failed to configure encryption at rest")

        for bucket in self.cluster.buckets:
            bucket_helper.change_bucket_props(
                bucket,
                encryptionAtRestKeyId=-1,
                encryptionAtRestDekRotationInterval=60,
                encryptionAtRestDekLifetime=60
            )
        status, response = rest.delete_secret(0)
        self.assertFalse(status, "Should not be able to delete default secret")
        status, response = rest.delete_secret(self.temp_log_secret_id)
        self.assertFalse(status,
                         "Should not be able to delete current log secret")

        self.sleep(100, "waiting for event to be generated")
        events = self.event_rest_helper.get_events(server=self.cluster.master,
                                                   events_count=40000)
        events = events["events"]
        search_and_verify(events, to_verify)
        self.assertTrue(len(not_find) == 0, "Not found: %s" % not_find)