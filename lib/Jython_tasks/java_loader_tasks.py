import requests

import global_vars
from cb_constants import DocLoading
from sirius_client_framework.sirius_setup import SiriusSetup
from table_view import TableView


class SiriusJavaDocGen(object):
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
        return key, None


class SiriusCouchbaseLoader(object):
    def __init__(self, server_ip, server_port,
                 generator=None, op_type=None,
                 username="Administrator", password="password",
                 bucket=None,
                 scope_name="_default", collection_name="_default",
                 key_prefix="test_doc-", key_size=10, doc_size=256,
                 create_percent=0, read_percent=0, update_percent=0,
                 delete_percent=0, expiry_percent=0,
                 create_start_index=0, create_end_index=0,
                 read_start_index=0, read_end_index=0,
                 update_start_index=0, update_end_index=0,
                 delete_start_index=0, delete_end_index=0,
                 touch_start_index=0, touch_end_index=0,
                 replace_start_index=0, replace_end_index=0,
                 expiry_start_index=0, expiry_end_index=0,
                 durability="NONE",
                 exp=0, exp_unit="seconds",
                 timeout=10, time_unit="seconds",
                 process_concurrency=1, task_identifier="", ops=None,
                 suppress_error_table=False,
                 track_failures=True,
                 iterations=1,
                 validate_docs=False, validate_deleted_docs=False,
                 mutate=0):
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

        # Load parameters
        self.process_concurrency = process_concurrency
        self.task_identifier = task_identifier
        self.suppress_error_table = suppress_error_table
        self.track_failures = track_failures
        self.ops = ops
        self.gtm = False
        self.iterations = iterations

        # Key / Doc properties
        self.key_prefix = key_prefix
        self.key_size = key_size
        self.doc_size = doc_size
        self.mutate = mutate

        # Set indexes for regular load
        self.create_percent = create_percent
        self.read_percent = read_percent
        self.update_percent = update_percent
        self.delete_percent = delete_percent
        self.expiry_percent = expiry_percent
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

        # Flags for validation
        self.validate_docs = validate_docs
        self.validate_deleted_docs = validate_deleted_docs

        self.op_type = op_type
        self.generator = generator

        # Result params
        self.success = dict()
        self.fail = dict()

        if ops is None:
            self.ops = 20000
            self.gtm = True

        if generator is not None:
            # Read all required data from the given Generator itself
            # Useful to load from regular async_load_task()
            self.__reset_indexes()

            self.doc_size = generator.doc_size
            self.key_size = generator.key_size
            self.key_prefix = generator.name

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
            elif self.exp > 0:
                # Expiry case str is not available in constants, so we do this
                self.update_percent = 100
                self.expiry_start_index = generator.start
                self.expiry_end_index = generator.end
            else:
                raise Exception(f"Unsupported {op_type} with generator")

        if self.key_size+1 <= len(self.key_prefix):
            raise Exception(f"key_size ({self.key_size}) <= key_prefix "
                            f"({self.key_prefix})")

    def __reset_indexes(self):
        self.create_percent = 0
        self.read_percent = 0
        self.update_percent = 0
        self.delete_percent = 0
        self.expiry_percent = 0
        self.create_start_index = 0
        self.create_end_index = 0
        self.read_start_index = 0
        self.read_end_index = 0
        self.update_start_index = 0
        self.update_end_index = 0
        self.delete_start_index = 0
        self.delete_end_index = 0
        self.touch_start_index = 0
        self.touch_end_index = 0
        self.replace_start_index = 0
        self.replace_end_index = 0
        self.expiry_start_index = 0
        self.expiry_end_index = 0

    @staticmethod
    def __print_error_table(bucket, scope, collection, failed_dict):
        log = global_vars.logger.get("test")
        failed_item_view = TableView(log.critical)
        failed_item_view.set_headers(["Read Key", "Exception"])
        for key, result_dict in failed_dict.items():
            failed_item_view.add_row([key, result_dict["error"]])
        failed_item_view.display("Keys failed in %s:%s:%s"
                                 % (bucket, scope, collection))

    @staticmethod
    def get_headers():
        return {'Content-Type': 'application/json',
                'Connection': 'close',
                'Accept': '*/*'}

    @staticmethod
    def __flatten_param_to_str(value):
        """
        Convert dict/list -> str
        """
        result = ""
        if isinstance(value, dict):
            result = '{'
            for key, val in value.items():
                if isinstance(val, dict) or isinstance(val, list):
                    result += SiriusCouchbaseLoader.__flatten_param_to_str(val)
                else:
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
                    result += SiriusCouchbaseLoader.__flatten_param_to_str(val)
                else:
                    result += '"%s",' % val
            result = result[:-1] + ']'
        return result

    def create_doc_load_task(self):
        key_type = "RandomKey"
        if self.iterations != 1:
            key_type = "CircularKey"

        api = f"{SiriusSetup.sirius_url}/doc_load"
        data = {
            "server_ip": self.ip,
            "server_port": self.port,
            "username": self.username,
            "password": self.password,
            "bucket_name": self.bucket.name,
            "scope_name": self.scope,
            "collection_name": self.collection,

            "create_percent": self.create_percent,
            "delete_percent": self.delete_percent,
            "update_percent": self.update_percent,
            "read_percent": self.read_percent,
            "expiry_percent": self.expiry_percent,

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

            "key_prefix": self.key_prefix,
            "key_size": self.key_size,
            "doc_size": self.doc_size,
            "key_type": key_type,
            "value_type": "SimpleValue",
            "mutate": self.mutate,

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
        }
        data = self.__flatten_param_to_str(data)
        response = requests.post(api, data,
                                 headers=self.get_headers(), timeout=10)
        json_response = None
        if response.ok:
            json_response = response.json()
            self.task_ids = json_response["tasks"]
            self.thread_name = '_'.join(self.task_ids)
        return response.ok, json_response

    def start_task(self):
        ok = True
        api = f"{SiriusSetup.sirius_url}/submit_task"
        for task_id in self.task_ids:
            response = requests.post(
                api, self.__flatten_param_to_str({"task_id": task_id}),
                headers=self.get_headers(), timeout=10)
            ok = ok and response.ok
        return ok

    def end_task(self):
        # Graceful way of stopping a task
        ok = True
        response = None
        api = f"{SiriusSetup.sirius_url}/stop_task"
        for task_id in self.task_ids:
            response = requests.post(
                api, self.__flatten_param_to_str({"task_id": task_id}),
                headers=self.get_headers(), timeout=10)
            ok = ok and response.ok
            response = response.json()
        return ok, response

    def cancel_task(self):
        ok = True
        response = None
        api = f"{SiriusSetup.sirius_url}/cancel_task"
        for task_id in self.task_ids:
            response = requests.post(
                api, self.__flatten_param_to_str({"task_id": task_id}),
                headers=self.get_headers(), timeout=10)
            ok = ok and response.ok
            response = response.json()
        return ok, response

    def get_task_result(self):
        ok = True
        url = f"{SiriusSetup.sirius_url}/get_task_result"
        for task_id in self.task_ids:
            try:
                response = requests.post(
                    url, self.__flatten_param_to_str({"task_id": task_id}),
                    headers=self.get_headers(), timeout=None)
                json_resp = response.json()
                ok = ok and response.ok and json_resp["status"]
                if "fail" in json_resp:
                    self.fail.update(json_resp["fail"])
            except Exception as e:
                print(e)

        if not self.suppress_error_table:
            self.__print_error_table(self.bucket, self.scope, self.collection,
                                     self.fail)
        return ok

    @staticmethod
    def get_keys_from_sirius(key_prefix, key_size, key_type,
                             start, end, mutate):
        url = f"{SiriusSetup.sirius_url}/get_doc_keys"
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
        response = requests.post(
            url, SiriusCouchbaseLoader.__flatten_param_to_str(data),
            headers=SiriusCouchbaseLoader.get_headers(), timeout=None)
        return response.json()["keys"] if response.ok else list()
