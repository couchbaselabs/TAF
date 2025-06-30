import requests

import global_vars
from cb_constants import DocLoading
from sirius_client_framework.sirius_setup import SiriusSetup
from table_view import TableView
from cb_constants.CBServer import CbServer


class SiriusJavaDocGen(object):
    __val = {"key": "val"}

    def __init__(self, start=0, end=1,
                 key_prefix="test_doc-", key_size=10,
                 doc_size=64, mutate=0):
        self.itr = 0
        self.name = key_prefix
        self.keys_len = end - start
        self.doc_size = doc_size
        self.key_size = key_size
        self.key_prefix = key_prefix
        self.start = start
        self.end = end
        self.keys = SiriusCouchbaseLoader.get_keys_from_sirius(
            self.key_prefix, self.key_size, "RandomKey",
            self.start, self.end, mutate)

    def has_next(self):
        return self.itr < self.keys_len

    def next(self):
        if not self.has_next():
            raise StopIteration
        key = self.keys[self.itr]
        self.itr += 1
        # Returning (k,v) format to align with in-built doc_loader return type
        return key, SiriusJavaDocGen.__val


class BaseSiriusLoader:
    """Base class containing common functionality for Sirius loaders"""

    @staticmethod
    def get_headers():
        return {'Content-Type': 'application/json',
                'Connection': 'close',
                'Accept': '*/*'}

    @staticmethod
    def _flatten_param_to_str(value):
        """
        Convert dict/list -> str
        """
        result = ""
        if isinstance(value, dict):
            result = '{'
            for key, val in value.items():
                if isinstance(val, dict):
                    result += BaseSiriusLoader._flatten_param_to_str(val)
                elif isinstance(val, list):
                    result += '\"%s\":%s,' % (
                        key, BaseSiriusLoader._flatten_param_to_str(val))
                else:
                    if val is None:
                        continue
                    try:
                        val = int(val)
                    except ValueError:
                        val = '\"%s\"' % val
                    result += '\"%s\":%s,' % (key, val)
            result = result[:-1] + '}'
        elif isinstance(value, list):
            result = '['
            for val in value:
                if isinstance(val, dict) or isinstance(val, list):
                    result += BaseSiriusLoader._flatten_param_to_str(val)
                else:
                    try:
                        val = int(val)
                        result += '%s,' % val
                    except ValueError:
                        result += '"%s",' % val
            result = result[:-1] + ']'
        return result

    @staticmethod
    def _print_error_table(bucket, scope, collection, failed_dict):
        log = global_vars.logger.get("test")
        failed_item_view = TableView(log.critical)
        failed_item_view.set_headers(["Read Key", "Exception"])
        for key, result_dict in failed_dict.items():
            failed_item_view.add_row([key, result_dict["error"]])
        failed_item_view.display("Keys failed in %s:%s:%s"
                               % (bucket, scope, collection))

    def _make_api_request(self, api_endpoint, data, timeout=10):
        """Common method to make API requests"""
        url = f"{SiriusSetup.sirius_url}/{api_endpoint}"
        response = requests.post(url, data,
                               headers=self.get_headers(), timeout=timeout)
        json_response = None
        if response.ok:
            json_response = response.json()
        return response.ok, json_response

    def _make_task_request(self, task_ids, api_endpoint, timeout=10):
        """Common method to make requests for multiple task IDs"""
        ok = True
        response = None
        for task_id in task_ids:
            data = self._flatten_param_to_str({"task_id": task_id})
            success, resp = self._make_api_request(api_endpoint, data, timeout)
            ok = ok and success
            response = resp
        return ok, response

    def _reset_indexes(self):
        """Reset all operation indexes to 0"""
        indexes = [
            'create_percent', 'read_percent', 'update_percent', 'delete_percent',
            'expiry_percent', 'create_start_index', 'create_end_index',
            'read_start_index', 'read_end_index', 'update_start_index',
            'update_end_index', 'delete_start_index', 'delete_end_index',
            'touch_start_index', 'touch_end_index', 'replace_start_index',
            'replace_end_index', 'expiry_start_index', 'expiry_end_index',
            'subdoc_insert_start_index', 'subdoc_insert_end_index',
            'subdoc_upsert_start_index', 'subdoc_upsert_end_index',
            'subdoc_remove_start_index', 'subdoc_remove_end_index',
            'subdoc_read_start_index', 'subdoc_read_end_index'
        ]
        for index in indexes:
            if hasattr(self, index):
                setattr(self, index, 0)


class SiriusCouchbaseLoader(BaseSiriusLoader):
    def __init__(self, server_ip, server_port,
                 generator=None, op_type=None,
                 username="Administrator", password="password",
                 bucket=None,
                 scope_name="_default", collection_name="_default",
                 key_prefix="test_doc-", key_size=10, doc_size=256,
                 key_type="SimpleKey", value_type="SimpleValue",
                 create_percent=0, read_percent=0, update_percent=0,
                 delete_percent=0, expiry_percent=0,
                 create_start_index=0, create_end_index=0,
                 read_start_index=0, read_end_index=0,
                 update_start_index=0, update_end_index=0,
                 delete_start_index=0, delete_end_index=0,
                 touch_start_index=0, touch_end_index=0,
                 replace_start_index=0, replace_end_index=0,
                 expiry_start_index=0, expiry_end_index=0,
                 subdoc_percent=0,
                 subdoc_insert_start_index=0, subdoc_insert_end_index=0,
                 subdoc_upsert_start_index=0, subdoc_upsert_end_index=0,
                 subdoc_remove_start_index=0, subdoc_remove_end_index=0,
                 subdoc_read_start_index=0, subdoc_read_end_index=0,
                 durability="NONE",
                 exp=0, exp_unit="seconds",
                 timeout=10, time_unit="seconds",
                 process_concurrency=1, task_identifier="", ops=None,
                 suppress_error_table=False,
                 track_failures=True,
                 iterations=1,
                 validate_docs=False, validate_deleted_docs=False,
                 mutate=0,
                 create_path=False, is_xattr=False, is_sys_xattr=False,
                 elastic=False, es_server=None, es_api_key=None,
                 es_similarity=None,
                 base_vectors_file_path=None, sift_url=None,
                 model=None, mockVector=False, dim=128, base64=False):
        """
        Gateway to start doc loading using Java SDK.
        Have common params for both storage_tests and regular test.
        If 'generator' and 'op_type' is specified, we will extract all data
        from it. Else we will consider data for storage_tests' perspective
        """

        self.task_ids = list()
        self.thread_name = None

        # Server params
        self.ip = server_ip
        self.port = server_port
        self.username = username
        self.password = password
        # Bucket / collection params
        self.bucket = bucket
        self.scope = scope_name
        self.collection = collection_name

        # SDK load parameters
        self.exp = exp
        self.exp_unit = exp_unit
        self.timeout = timeout
        self.time_unit = time_unit
        self.durability = durability
        self.create_path = create_path
        self.is_xattr = is_xattr
        self.is_sys_xattr = is_sys_xattr

        # Load parameters
        self.process_concurrency = process_concurrency
        self.task_identifier = task_identifier
        self.suppress_error_table = suppress_error_table
        self.track_failures = track_failures
        self.ops = ops
        self.gtm = False
        self.iterations = iterations
        self.target_vbuckets = None
        self.vbuckets = CbServer.total_vbuckets

        # Key / Doc properties
        self.key_type = key_type
        self.key_prefix = key_prefix
        self.key_size = key_size
        self.doc_size = doc_size
        self.value_type = value_type
        self.mutate = mutate

        # Set indexes for regular load
        self.create_percent = create_percent
        self.read_percent = read_percent
        self.update_percent = update_percent
        self.delete_percent = delete_percent
        self.expiry_percent = expiry_percent
        self.subdoc_percent = subdoc_percent
        self.create_start_index = create_start_index
        self.create_end_index = create_end_index
        self.read_start_index = read_start_index
        self.read_end_index = read_end_index
        self.update_start_index = update_start_index
        self.update_end_index = update_end_index
        self.delete_start_index = delete_start_index
        self.delete_end_index = delete_end_index
        self.touch_start_index = touch_start_index
        self.touch_end_index = touch_end_index
        self.replace_start_index = replace_start_index
        self.replace_end_index = replace_end_index
        self.expiry_start_index = expiry_start_index
        self.expiry_end_index = expiry_end_index
        self.subdoc_insert_start_index = subdoc_insert_start_index
        self.subdoc_insert_end_index = subdoc_insert_end_index
        self.subdoc_upsert_start_index = subdoc_upsert_start_index
        self.subdoc_upsert_end_index = subdoc_upsert_end_index
        self.subdoc_remove_start_index = subdoc_remove_start_index
        self.subdoc_remove_end_index = subdoc_remove_end_index
        self.subdoc_read_start_index = subdoc_read_start_index
        self.subdoc_read_end_index = subdoc_read_end_index

        self.elastic = elastic
        self.es_server = es_server
        self.es_api_key = es_api_key
        self.es_similarity = es_similarity
        self.base_vectors_file_path = base_vectors_file_path
        self.sift_url = sift_url
        self.model = model
        self.mockVector = mockVector
        self.dim = dim
        self.base64 = base64
        self.mutate_field = ""
        self.mutation_timeout = 0

        # Flags for validation
        self.validate_docs = validate_docs
        self.validate_deleted_docs = validate_deleted_docs

        self.op_type = op_type
        self.generator = generator

        # Result params
        self.success = dict()
        self.fail = dict()
        self.fail_count = 0

        if ops is None:
            self.ops = 20000
            self.gtm = True

        if generator is not None:
            # Read all required data from the given Generator itself
            # Useful to load from regular async_load_task()
            self._reset_indexes()

            self.doc_size = generator.doc_size
            self.key_size = generator.key_size
            self.key_prefix = generator.name
            self.vbuckets = 1024

            if hasattr(generator, "vbuckets"):
                self.vbuckets = generator.vbuckets
            if hasattr(generator, "target_vbuckets"):
                self.target_vbuckets = generator.target_vbuckets

            if op_type == DocLoading.Bucket.DocOps.CREATE:
                self.create_percent = 100
                self.create_start_index = generator.start
                self.create_end_index = generator.end
            elif op_type == DocLoading.Bucket.DocOps.UPDATE:
                self.update_percent = 100
                self.update_start_index = generator.start
                self.update_end_index = generator.end
            elif op_type == DocLoading.Bucket.DocOps.READ:
                self.read_percent = 100
                self.read_start_index = generator.start
                self.read_end_index = generator.end
            elif op_type == DocLoading.Bucket.DocOps.DELETE:
                self.delete_percent = 100
                self.delete_start_index = generator.start
                self.delete_end_index = generator.end
            elif op_type == DocLoading.Bucket.DocOps.REPLACE:
                self.update_percent = 100
                self.replace_start_index = generator.start
                self.replace_end_index = generator.end
            elif op_type == DocLoading.Bucket.DocOps.TOUCH:
                self.update_percent = 100
                self.touch_start_index = generator.start
                self.touch_end_index = generator.end
            elif op_type == DocLoading.Bucket.SubDocOps.INSERT:
                self.subdoc_percent = 100
                self.subdoc_insert_start_index = generator.start
                self.subdoc_insert_end_index = generator.end
            elif op_type == DocLoading.Bucket.SubDocOps.UPSERT:
                self.subdoc_percent = 100
                self.subdoc_upsert_start_index = generator.start
                self.subdoc_upsert_end_index = generator.end
            elif op_type == DocLoading.Bucket.SubDocOps.LOOKUP:
                self.subdoc_percent = 100
                self.subdoc_read_start_index = generator.start
                self.subdoc_read_end_index = generator.end
            elif op_type == DocLoading.Bucket.SubDocOps.REMOVE:
                self.subdoc_percent = 100
                self.subdoc_remove_start_index = generator.start
                self.subdoc_remove_end_index = generator.end
            else:
                raise Exception(f"Unsupported {op_type} with generator")

        if self.key_size + 1 <= len(self.key_prefix):
            raise Exception(f"key_size ({self.key_size}) <= key_prefix "
                          f"({self.key_prefix})")

    @staticmethod
    def create_clients_in_pool(server, username, password, bucket_name,
                              req_clients=1):
        data = {
            "server_ip": server.ip,
            "server_port": server.memcached_port,
            "username": username,
            "password": password,
            "bucket_name": bucket_name,
            "req_clients": req_clients
        }
        data = BaseSiriusLoader._flatten_param_to_str(data)
        url = f"{SiriusSetup.sirius_url}/create_clients"
        response = requests.post(url, data,
                               headers=BaseSiriusLoader.get_headers(),
                               timeout=10)
        json_response = None
        if response.ok:
            json_response = response.json()
        return response.ok, json_response

    def create_doc_load_task(self):
        if self.iterations != 1:
            self.key_type = "CircularKey"
        if self.subdoc_percent != 0:
            self.value_type = "SimpleSubDocValue"

        api_endpoint = "doc_load"
        data = {
            "bucket_name": self.bucket.name,
            "scope_name": self.scope,
            "collection_name": self.collection,

            "num_vbuckets": self.vbuckets,
            "target_vbuckets": self.target_vbuckets,

            "create_percent": self.create_percent,
            "delete_percent": self.delete_percent,
            "update_percent": self.update_percent,
            "read_percent": self.read_percent,
            "expiry_percent": self.expiry_percent,
            "subdoc_percent": self.subdoc_percent,

            "create_start": self.create_start_index,
            "create_end": self.create_end_index,

            "delete_start": self.delete_start_index,
            "delete_end": self.delete_end_index,

            "update_start": self.update_start_index,
            "update_end": self.update_end_index,

            "read_start": self.read_start_index,
            "read_end": self.read_end_index,

            "touch_start": self.touch_start_index,
            "touch_end": self.touch_end_index,

            "replace_start": self.replace_start_index,
            "replace_end": self.replace_end_index,

            "expiry_start": self.expiry_start_index,
            "expiry_end": self.expiry_end_index,

            "sd_insert_start": self.subdoc_insert_start_index,
            "sd_insert_end": self.subdoc_insert_end_index,

            "sd_upsert_start": self.subdoc_upsert_start_index,
            "sd_upsert_end": self.subdoc_upsert_end_index,

            "sd_remove_start": self.subdoc_remove_start_index,
            "sd_remove_end": self.subdoc_remove_end_index,

            "sd_read_start": self.subdoc_read_start_index,
            "sd_read_end": self.subdoc_read_end_index,

            "key_prefix": self.key_prefix,
            "key_size": self.key_size,
            "doc_size": self.doc_size,
            "key_type": self.key_type,
            "value_type": self.value_type,
            "mutate": self.mutate,
            "create_path": self.create_path,
            "is_subdoc_xattr": self.is_xattr,
            "is_subdoc_sys_xattr": self.is_sys_xattr,

            "timeout": self.timeout,
            "timeout_unit": self.time_unit,
            "doc_ttl": self.exp,
            "doc_ttl_unit": self.exp_unit,
            "durability_level": self.durability,

            "ops": self.ops,
            "gtm": self.gtm,
            "process_concurrency": self.process_concurrency,
            "iterations": self.iterations,

            "validate_docs": self.validate_docs,
            "validate_deleted_docs": self.validate_deleted_docs,

            "elastic": self.elastic,
            "es_server": self.es_server,
            "es_api_key": self.es_api_key,
            "es_similarity": self.es_similarity,
            "base_vectors_file_path": self.base_vectors_file_path,
            "sift_url": self.sift_url,
            "model": self.model,
            "mockVector": self.mockVector,
            "dim": self.dim,
            "base64": self.base64,
            "mutate_field": self.mutate_field,
            "mutation_timeout": self.mutation_timeout
        }

        if self.value_type == "siftBigANN":
            api_endpoint = "sift_doc_load"

        data = BaseSiriusLoader._flatten_param_to_str(data)
        success, json_response = self._make_api_request(api_endpoint, data, 10)

        if success and json_response:
            self.task_ids = json_response["tasks"]
            self.thread_name = '_'.join(self.task_ids)
        return success, json_response

    def start_task(self):
        return self._make_task_request(self.task_ids, "submit_task", 10)[0]

    def end_task(self):
        # Graceful way of stopping a task
        return self._make_task_request(self.task_ids, "stop_task", 10)

    def cancel_task(self):
        return self._make_task_request(self.task_ids, "cancel_task", 10)

    def get_task_result(self):
        ok = True
        for task_id in self.task_ids:
            try:
                data = BaseSiriusLoader._flatten_param_to_str({"task_id": task_id})
                success, json_resp = self._make_api_request("get_task_result",
                                                          data, None)
                ok = ok and success and json_resp["status"]
                if "fail" in json_resp:
                    self.fail.update(json_resp["fail"])
                    self.fail_count = len(self.fail)
            except Exception as e:
                print(e)

        if not self.suppress_error_table:
            self._print_error_table(self.bucket, self.scope, self.collection,
                                   self.fail)
        return ok

    @staticmethod
    def get_keys_from_sirius(key_prefix, key_size, key_type,
                            start, end, mutate):
        data = {"key_prefix": key_prefix,
                "key_size": key_size,
                "key_type": key_type,
                "mutate": mutate,

                # Using delete here since that can generate Keys without values
                "delete_percent": 100,
                "delete_start": start,
                "delete_end": end,
                "ops": 1000,

                # Following values are required for validation to pass
                "server_ip": "dummy",
                "username": "dummy",
                "password": "dummy",
                "bucket_name": "dummy",
                }
        data = BaseSiriusLoader._flatten_param_to_str(data)
        url = f"{SiriusSetup.sirius_url}/get_doc_keys"
        response = requests.post(url, data,
                               headers=BaseSiriusLoader.get_headers(),
                               timeout=None)
        return response.json()["keys"] if response.ok else list()


class SiriusJavaMongoLoader(BaseSiriusLoader):

    def __init__(self, server_ip, server_port, username, password,
                 bucket_name, collection_name, is_atlas,
                 process_concurrency=1, task_identifier="",
                 suppress_error_table=False, track_failures=True, ops=None,
                 iterations=1,
                 key_type="SimpleKey", key_prefix="test_doc-", key_size=10,
                 doc_size=256, value_type="SimpleValue", mutate=0,
                 create_percent=0, read_percent=0, update_percent=0,
                 delete_percent=0, expiry_percent=0, subdoc_percent=0,
                 create_start_index=0, create_end_index=0, read_start_index=0,
                 read_end_index=0, update_start_index=0, update_end_index=0,
                 delete_start_index=0, delete_end_index=0, touch_start_index=0,
                 touch_end_index=0, replace_start_index=0, replace_end_index=0,
                 expiry_start_index=0, expiry_end_index=0):
        self.server_ip = server_ip
        self.server_port = server_port
        self.username = username
        self.password = password
        self.bucket_name = bucket_name
        self.collection_name = collection_name
        self.is_atlas = is_atlas

        # Result params
        self.success = dict()
        self.fail = dict()
        self.fail_count = 0
        self.success_count = 0
        self.total_count = 0

        # Load parameters
        self.process_concurrency = process_concurrency
        self.task_identifier = task_identifier
        self.suppress_error_table = suppress_error_table
        self.track_failures = track_failures
        self.ops = ops
        self.iterations = iterations

        # Key / Doc properties
        self.key_type = key_type
        self.key_prefix = key_prefix
        self.key_size = key_size
        self.doc_size = doc_size
        self.value_type = value_type
        self.mutate = mutate

        # Set indexes for regular load
        self.create_percent = create_percent
        self.read_percent = read_percent
        self.update_percent = update_percent
        self.delete_percent = delete_percent
        self.expiry_percent = expiry_percent
        self.subdoc_percent = subdoc_percent
        self.create_start_index = create_start_index
        self.create_end_index = create_end_index
        self.read_start_index = read_start_index
        self.read_end_index = read_end_index
        self.update_start_index = update_start_index
        self.update_end_index = update_end_index
        self.delete_start_index = delete_start_index
        self.delete_end_index = delete_end_index
        self.touch_start_index = touch_start_index
        self.touch_end_index = touch_end_index
        self.replace_start_index = replace_start_index
        self.replace_end_index = replace_end_index
        self.expiry_start_index = expiry_start_index
        self.expiry_end_index = expiry_end_index

    def create_doc_load_task(self):
        data = {
            "mongo_server_ip": self.server_ip,
            "mongo_server_port": self.server_port,
            "mongo_username": self.username,
            "mongo_password": self.password,
            "mongo_bucket_name": self.bucket_name,
            "mongo_collection_name": self.collection_name,
            "mongo_is_atlas": self.is_atlas,

            "create_percent": self.create_percent,
            "delete_percent": self.delete_percent,
            "update_percent": self.update_percent,
            "read_percent": self.read_percent,
            "expiry_percent": self.expiry_percent,
            "subdoc_percent": self.subdoc_percent,

            "create_start": self.create_start_index,
            "create_end": self.create_end_index,

            "delete_start": self.delete_start_index,
            "delete_end": self.delete_end_index,

            "update_start": self.update_start_index,
            "update_end": self.update_end_index,

            "read_start": self.read_start_index,
            "read_end": self.read_end_index,

            "expiry_start": self.expiry_start_index,
            "expiry_end": self.expiry_end_index,

            "key_prefix": self.key_prefix,
            "key_size": self.key_size,
            "doc_size": self.doc_size,
            "key_type": self.key_type,
            "value_type": self.value_type,
            "mutate": self.mutate,

            "ops": self.ops,
            "process_concurrency": self.process_concurrency,
            "iterations": self.iterations,
        }
        data = self._flatten_param_to_str(data)
        success, json_response = self._make_api_request("doc_load_mongo", data,
                                                       10)

        if success and json_response:
            self.task_ids = json_response["tasks"]
            self.thread_name = '_'.join(self.task_ids)
        return success, json_response

    def start_task(self):
        return self._make_task_request(self.task_ids, "submit_task_mongo",
                                      10)[0]

    def get_task_result(self):
        ok = True
        for task_id in self.task_ids:
            try:
                data = BaseSiriusLoader._flatten_param_to_str({"task_id": task_id})
                success, json_resp = self._make_api_request("get_task_result_mongo",
                                                          data, None)
                ok = ok and success and json_resp["status"]
                if "fail" in json_resp:
                    self.fail.update(json_resp["fail"])
                    self.fail_count = len(self.fail)
            except Exception as e:
                print(e)

        if not self.suppress_error_table:
            self._print_error_table(self.bucket_name, self.collection_name,
                                   self.collection_name, self.fail)
        return ok

    def end_task(self):
        return self._make_task_request(self.task_ids, "stop_task", 10)

    def cancel_task(self):
        return self._make_task_request(self.task_ids, "cancel_task", 10)