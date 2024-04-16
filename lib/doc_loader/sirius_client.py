import json
import random
import time
import requests

from cb_constants import CbServer, DocLoading
from common_lib import IDENTIFIER_TOKEN
from doc_loader.sirius import SiriusClient
from global_vars import logger
from TestInput import TestInputSingleton


class RESTClient(object):
    def __init__(self, servers, bucket,
                 scope=CbServer.default_scope,
                 collection=CbServer.default_collection,
                 username="Administrator", password="password",
                 compression_settings=None, sirius_base_url="http://0.0.0.0:4000"):
        """
        :param servers: List of servers for SDK to establish initial
                        connections with
        :param bucket: Bucket object to which the SDK connection will happen
        :param scope:  Name of the scope to connect.
                       Default: '_default'
        :param collection: Name of the collection to connect.
                           Default: _default
        :param username: User name using which to establish the connection
        :param password: Password for username authentication
        :param compression_settings: Dict of compression settings. Format:
                                     {
                                      "enabled": Bool,
                                      "minRatio": Double int (None to default),
                                      "minSize": int (None to default)
                                     }
        :param cert_path: Path of certificate file to establish connection
        """
        input = TestInputSingleton.input
        self.sirius_base_url = sirius_base_url
        self.sirius_base_url = input.param("sirius_url", self.sirius_base_url)
        self.sirius_base_url_list = self.sirius_base_url.split("|")
        self.sirius_base_url = random.choice(self.sirius_base_url_list)
        self.servers = list()
        self.hosts = list()
        self.scope_name = scope
        self.collection_name = collection
        self.username = username
        self.password = password

        self.default_timeout = 0
        self.cluster = None
        self.bucket = bucket
        self.bucketObj = None
        self.collection = None
        self.compression = compression_settings

        self.cluster_config = None
        self.timeouts_config = None
        self.compression_config = None
        self.connection_string = ""
        self.retry = 1000
        self.retry_interval = 0.2
        self.delete_record = False
        self.op_type = ""

        self.log = logger.get("test")
        for server in servers:
            self.servers.append((server.ip, int(server.port)))
            if server.ip == "127.0.0.1":
                if CbServer.use_https:
                    self.scheme = "https"
                else:
                    self.scheme = "http"
            else:
                self.hosts.append(server.ip)
                if CbServer.use_https:
                    self.scheme = "couchbases"
                else:
                    self.scheme = "couchbase"

        if not SiriusClient().is_sirius_online(self.sirius_base_url):
            raise Exception("sirius not online")

        self.create_cluster_config()

    def create_cluster_config(self):
        self.log.debug("Creating Cluster Config connection for '%s'" % self.bucket)

        self.connection_string = "{0}://{1}".format(self.scheme, ", ".
                                                    join(self.hosts).
                                                    replace(" ", ""))

        self.cluster_config = self.create_payload_cluster_config()

    def send_request(self, path, method, payload=None):
        headers = {
            "Content-Type": "application/json"
        }

        url = self.sirius_base_url + path
        print(url, path, payload)
        exception = None
        for i in range(5):
            try:
                response = requests.post(url, headers=headers, json=payload)
                return response
            except Exception as e:
                print(str(e))
                exception = e

        if exception is not None:
            raise exception

    def create_payload_cluster_config(self):
        self.compression_config = self.create_payload_compression_config()
        self.timeouts_config = self.create_payload_timeouts_config()
        payload = {
            "clusterConfig": {
                "username": self.username,
                "password": self.password,
                "connectionString": self.connection_string
            }
        }

        if self.compression_config:
            payload["clusterConfig"].update(self.compression_config)

        if self.timeouts_config:
            payload["clusterConfig"].update(self.timeouts_config)

        return payload

    def create_payload_compression_config(self):
        payload = {
            "compressionConfig": {}
        }
        if type(self.compression) == dict and "minSize" in self.compression and "minRatio" in self.compression:
            payload["compressionConfig"]["disabled"] = self.compression["enabled"]

            payload["compressionConfig"]["minSize"] = self.compression_config["minSize"]

            payload["compressionConfig"]["minRatio"] = self.compression_config["minRatio"]

        return payload

    def create_payload_timeouts_config(self, connect_timeout=20, kv_timeout=10, kv_durable_timeout=None):
        payload = {
            "timeoutsConfig": {}
        }

        if connect_timeout is not None:
            payload["timeoutsConfig"]["connectTimeout"] = connect_timeout

        if kv_timeout is not None:
            payload["timeoutsConfig"]["KVTimeout"] = kv_timeout

        if kv_durable_timeout is not None:
            payload["timeoutsConfig"]["KVDurableTimeout"] = kv_durable_timeout

        return payload

    def create_payload_insert_options(self, expiry=None, persist_to=None, replicate_to=None, durability=None,
                                      timeout=None):
        payload = {
            "insertOptions": {}
        }

        if expiry is not None:
            payload["insertOptions"]["expiry"] = expiry

        if persist_to is not None:
            payload["insertOptions"]["persistTo"] = persist_to

        if replicate_to is not None:
            payload["insertOptions"]["replicateTo"] = replicate_to

        if durability is not None:
            payload["insertOptions"]["durability"] = durability

        if timeout is not None:
            payload["insertOptions"]["timeout"] = timeout

        return payload

    def create_payload_remove_options(self, cas=None, persist_to=None, replicate_to=None, durability=None,
                                      timeout=None):
        payload = {
            "removeOptions": {}
        }

        if cas is not None:
            payload["removeOptions"]["cas"] = cas

        if persist_to is not None:
            payload["removeOptions"]["persistTo"] = persist_to

        if replicate_to is not None:
            payload["removeOptions"]["replicateTo"] = replicate_to

        if durability is not None:
            payload["removeOptions"]["durability"] = durability

        if timeout is not None:
            payload["removeOptions"]["timeout"] = timeout

        return payload

    def create_payload_replace_options(self, expiry=None, cas=None, persist_to=None, replicate_to=None,
                                       durability=None, timeout=None):
        payload = {
            "replaceOptions": {}
        }

        if expiry is not None:
            payload["replaceOptions"]["expiry"] = expiry

        if cas is not None:
            payload["replaceOptions"]["cas"] = cas

        if persist_to is not None:
            payload["replaceOptions"]["persistTo"] = persist_to

        if replicate_to is not None:
            payload["replaceOptions"]["replicateTo"] = replicate_to

        if durability is not None:
            payload["replaceOptions"]["durability"] = durability

        if timeout is not None:
            payload["replaceOptions"]["timeout"] = timeout

        return payload

    def create_payload_touch_options(self, timeout):
        payload = {
            "touchOptions": {}
        }

        if timeout is not None:
            payload["touchOptions"]["timeout"] = timeout

        return payload

    def create_payload_operation_config(self, count=None, doc_size=512, doc_type="json", key_size="100", key_prefix="",
                                        key_suffix="", random_doc_size=False, random_key_size=False,
                                        read_your_own_write=False, template_name="Person", start=0, end=0,
                                        fields_to_change=[], ignore_exceptions=[], retry_exception=[],
                                        retry_attempts=0):
        payload = {
            "operationConfig": {
                "docSize": doc_size,
                "keyPrefix": key_prefix,
                "keySuffix": key_suffix,
                "template": template_name,
                "start": start,
                "end": end
            }
        }

        if count is not None:
            payload["operationConfig"]["count"] = count

        if doc_type is not None:
            payload["operationConfig"]["docType"] = doc_type

        if key_size is not None:
            payload["operationConfig"]["keySize"] = key_size

        if read_your_own_write is not None:
            payload["operationConfig"]["readYourOwnWrite"] = read_your_own_write

        if fields_to_change is not None:
            payload["operationConfig"]["fieldsToChange"] = fields_to_change

        ignore_exceptions = []
        retry_exception = []

        payload["operationConfig"]["exceptions"] = {
            "retryExceptions": retry_exception,
            "ignoreExceptions": ignore_exceptions,
            "retryAttempts": retry_attempts,
        }

        return payload

    def build_sub_doc_operation_config(self, start=0, end=0, ignore_exceptions=[], retry_exception=[],
                                       retry_attempts=0):
        payload = {
            "subDocOperationConfig": {
                "start": start,
                "end": end
            }
        }

        ignore_exceptions = []
        retry_exception = []

        payload["subDocOperationConfig"]["exceptions"] = {
            "retryExceptions": retry_exception,
            "ignoreExceptions": ignore_exceptions,
            "retryAttempts": retry_attempts,
        }

        return payload

    def build_insert_spec_options(self, create_path=None, is_xattr=None):

        payload = {
            "insertSpecOptions": {}
        }
        if create_path is not None:
            payload["insertSpecOptions"]["createPath"] = create_path

        if is_xattr is not None:
            payload["insertSpecOptions"]["isXattr"] = is_xattr

        return payload

    def build_remove_spec_options(self, is_xattr=None):

        payload = {
            "removeSpecOptions": {}
        }

        if is_xattr is not None:
            payload["removeSpecOptions"]["isXattr"] = is_xattr

        return payload

    def build_replace_spec_options(self, is_xattr=None):

        payload = {
            "replaceSpecOptions": {}
        }

        if is_xattr is not None:
            payload["replaceSpecOptions"]["isXattr"] = is_xattr

        return payload

    def build_mutate_in_options(self, expiry=None, cas=None, persist_to=None, replicate_to=None, durability=None,
                                store_semantic=None, timeout=None,
                                preserve_expiry=None):

        payload = {
            "mutateInOptions": {}
        }

        if expiry is not None:
            payload["mutateInOptions"]["expiry"] = expiry

        if cas is not None:
            payload["mutateInOptions"]["cas"] = cas

        if persist_to is not None:
            payload["mutateInOptions"]["persistTo"] = persist_to

        if replicate_to is not None:
            payload["mutateInOptions"]["replicateTo"] = replicate_to

        if durability is not None:
            payload["mutateInOptions"]["durability"] = durability

        if timeout is not None:
            payload["mutateInOptions"]["timeout"] = timeout

        if store_semantic is not None:
            payload["mutateInOptions"]["storeSemantic"] = store_semantic

        if preserve_expiry is not None:
            payload["mutateInOptions"]["preserveExpiry"] = preserve_expiry

        return payload

    def build_lookup_in_options(self, timeout=None):

        payload = {
            "lookupInOptions": {}
        }

        if timeout is not None:
            payload["lookupInOptions"]["timeout"] = timeout

        return payload

    def build_get_spec_options(self, is_xattr=None):

        payload = {
            "getSpecOptions": {}
        }

        if is_xattr is not None:
            payload["getSpecOptions"]["isXattr"] = is_xattr

        return payload

    def create_payload_single_operation_config(self, key, value=None, doc_size=512, template="Person"):

        payload = {
            "singleOperationConfig": {
                "keys": [key],
                "template": template,
                "docSize": doc_size,
            }
        }

        return payload

    def create_payload_single_sub_doc_operation_config(self, key, path, doc_size=None, value=None):

        payload = {
            "singleSubDocOperationConfig": {
                "key": key,
                "paths": [path],
            }
        }

        if doc_size is not None:
            payload["singleSubDocOperationConfig"]["docSize"] = doc_size

        return payload

    def create_payload_key_path_value(self, key, path, value=None):
        payload = {
            "key": key,
            "pathValue": [
                {
                    "path": path,
                    "value": value,
                }
            ]
        }
        return payload

    def get_result_using_seed(self, response, load_type="bulk"):
        fail = {}

        if response.status_code != 200:
            print(f"unable to start operation (Bad/Malformed): "
                  f"{self.op_type} {response.status_code}")
            print(json.loads(response.content))
            raise Exception("Bad HTTP status at doc loading")

        result_from_request = json.loads(response.content)
        request_using_seed = {}
        result_using_seed = {}

        if not result_from_request["error"]:
            request_using_seed = {
                "seed": result_from_request["data"]["seed"],
                "deleteRecord": self.delete_record,
            }
        else:
            print("error in initiating task")
            raise Exception("Bad/Malformed operation request")

        for i in range(0, self.retry):
            response = self.send_request("/result", "POST", request_using_seed)
            result_using_seed = json.loads(response.content)
            if not result_using_seed["error"]:
                break
            time.sleep(self.retry_interval * 60)

        if response.status_code != 200:
            print(self.sirius_base_url + "/result", request_using_seed, response.status_code, result_using_seed)
            print("unable to retrieve the result of operation :" + self.op_type + " " + str(response.status_code))
            raise Exception("Bad HTTP status at fetching result")

        if not result_using_seed["error"]:
            if "otherErrors" in result_using_seed["data"].keys() and result_using_seed["data"]["otherErrors"] != "":
                print(result_using_seed["data"]["otherErrors"])

            if load_type == "bulk":
                for error_name, failed_documents in result_using_seed["data"]["bulkErrors"].items():
                    for doc_failed in failed_documents:
                        key = doc_failed["key"]
                        fail[key] = dict()
                        fail[key]['error'] = doc_failed["errorString"]
                        fail[key]['value'] = {}
                        fail[key]['status'] = False
                        fail[key]['cas'] = 0

                return fail, result_using_seed["data"]["success"], result_using_seed["data"]["failure"], \
                    result_from_request["data"]["seed"]
            else:
                return result_using_seed["data"]["singleResult"], result_using_seed["data"]["success"], \
                    result_using_seed["data"]["failure"], result_from_request["data"]["seed"]
        else:
            print(result_using_seed["message"])
            raise Exception(result_using_seed["message"])

    def do_bulk_operation(self, op_type, identifier_token=IDENTIFIER_TOKEN, insert_options=None,
                          remove_options=None, touch_options=None, operation_config=None,
                          expiry=None, retry=10, retry_interval=1, delete_record=False):

        self.retry = retry
        self.retry_interval = retry_interval
        self.delete_record = delete_record
        self.op_type = op_type

        payload = {"identifierToken": identifier_token, "bucket": self.bucket.name, "scope": self.scope_name,
                   "collection": self.collection_name}

        payload.update(self.cluster_config)

        if operation_config is not None:
            payload.update(operation_config)

        path = ""
        method = "POST"

        if op_type == DocLoading.Bucket.DocOps.CREATE:
            path = "/bulk-create"
            if insert_options is not None:
                payload.update(insert_options)

        elif op_type == DocLoading.Bucket.DocOps.UPDATE or op_type == DocLoading.Bucket.DocOps.REPLACE:
            path = "/bulk-upsert"
            if insert_options is not None:
                payload.update(insert_options)

        elif op_type == DocLoading.Bucket.DocOps.DELETE:
            path = "/bulk-delete"
            if remove_options is not None:
                payload.update(remove_options)

        elif op_type == DocLoading.Bucket.DocOps.READ:
            path = "/bulk-read"

        elif op_type == DocLoading.Bucket.DocOps.TOUCH:
            path = "/bulk-touch"
            if touch_options is not None:
                payload.update(touch_options)

            if expiry is not None:
                payload.update({"expiry": expiry})

        elif self.op_type == "validate":
            path = "/validate"

        else:
            self.log.debug("unknown rest doc loading operation " + self.op_type)
            raise Exception("unknown rest doc loading operation " + self.op_type)

        response = self.send_request(path=path, method=method, payload=payload)
        return self.get_result_using_seed(response=response, load_type="bulk")

    def do_bulk_sub_doc_operation(self, op_type, identifier_token=IDENTIFIER_TOKEN, insert_spec_options=None,
                                  remove_spec_options=None,
                                  replace_spec_options=None, get_spec_options=None, lookup_in_options=None,
                                  mutate_in_options=None, sub_doc_operation_config=None, retry=10, retry_interval=1,
                                  delete_record=False):
        self.retry = retry
        self.retry_interval = retry_interval
        self.delete_record = delete_record
        self.op_type = op_type

        payload = {"identifierToken": identifier_token, "bucket": self.bucket.name, "scope": self.scope_name,
                   "collection": self.collection_name}

        payload.update(self.cluster_config)

        if sub_doc_operation_config is not None:
            payload.update(sub_doc_operation_config)

        path = ""
        method = "POST"

        if op_type == DocLoading.Bucket.SubDocOps.INSERT:
            path = "/sub-doc-bulk-insert"
            if insert_spec_options is not None:
                payload.update(insert_spec_options)

            if mutate_in_options is not None:
                payload.update(mutate_in_options)

        elif op_type == DocLoading.Bucket.SubDocOps.UPSERT:
            path = "/sub-doc-bulk-upsert"
            if insert_spec_options is not None:
                payload.update(insert_spec_options)

            if mutate_in_options is not None:
                payload.update(mutate_in_options)

        elif op_type == DocLoading.Bucket.SubDocOps.REMOVE:
            path = "/sub-doc-bulk-delete"
            if remove_spec_options is not None:
                payload.update(remove_spec_options)

            if mutate_in_options is not None:
                payload.update(mutate_in_options)

        elif op_type == DocLoading.Bucket.SubDocOps.REPLACE:
            path = "/sub-doc-bulk-replace"
            if replace_spec_options is not None:
                payload.update(replace_spec_options)

            if mutate_in_options is not None:
                payload.update(mutate_in_options)

        elif op_type == DocLoading.Bucket.SubDocOps.LOOKUP or op_type == DocLoading.Bucket.DocOps.READ:
            path = "/sub-doc-bulk-read"
            if get_spec_options is not None:
                payload.update(get_spec_options)

            if lookup_in_options is not None:
                payload.update(lookup_in_options)

        else:
            self.log.debug("unknown rest doc loading operation " + self.op_type)
            raise Exception("unknown rest doc loading operation " + self.op_type)

        response = self.send_request(path=path, method=method, payload=payload)
        return self.get_result_using_seed(response=response, load_type="bulk")

    def do_single_sub_doc_operation(self, op_type, identifier_token=IDENTIFIER_TOKEN, insert_spec_options=None,
                                    remove_spec_options=None,
                                    replace_spec_options=None, get_spec_options=None, lookup_in_options=None,
                                    mutate_in_options=None, increment_in_options=None,
                                    single_sub_doc_operation_config=None, retry=10,
                                    retry_interval=1,
                                    delete_record=False):
        self.retry = retry
        self.retry_interval = retry_interval
        self.delete_record = delete_record
        self.op_type = op_type

        payload = {"identifierToken": identifier_token, "bucket": self.bucket.name, "scope": self.scope_name,
                   "collection": self.collection_name}

        payload.update(self.cluster_config)

        if single_sub_doc_operation_config is not None:
            payload.update(single_sub_doc_operation_config)

        path = ""
        method = "POST"

        if op_type == DocLoading.Bucket.SubDocOps.INSERT:
            path = "/single-sub-doc-insert"
            if insert_spec_options is not None:
                payload.update(insert_spec_options)

            if mutate_in_options is not None:
                payload.update(mutate_in_options)

        elif op_type == DocLoading.Bucket.SubDocOps.UPSERT:
            path = "/single-sub-doc-upsert"
            if insert_spec_options is not None:
                payload.update(insert_spec_options)

            if mutate_in_options is not None:
                payload.update(mutate_in_options)

        elif op_type == DocLoading.Bucket.SubDocOps.REMOVE:
            path = "/single-sub-doc-delete"
            if remove_spec_options is not None:
                payload.update(remove_spec_options)

            if mutate_in_options is not None:
                payload.update(mutate_in_options)

        elif op_type == DocLoading.Bucket.SubDocOps.REPLACE:
            path = "/single-sub-doc-replace"
            if replace_spec_options is not None:
                payload.update(replace_spec_options)

            if mutate_in_options is not None:
                payload.update(mutate_in_options)

        elif op_type == DocLoading.Bucket.SubDocOps.LOOKUP:
            path = "/single-sub-doc-read"
            if get_spec_options is not None:
                payload.update(get_spec_options)

            if lookup_in_options is not None:
                payload.update(lookup_in_options)

        elif op_type == DocLoading.Bucket.SubDocOps.COUNTER:
            path = "/single-sub-doc-increment"
            if insert_spec_options is not None:
                payload.update(insert_spec_options)

            if mutate_in_options is not None:
                payload.update(mutate_in_options)

        else:
            self.log.debug("unknown rest doc loading operation " + self.op_type)
            raise Exception("unknown rest doc loading operation " + self.op_type)

        response = self.send_request(path=path, method=method, payload=payload)
        return self.get_result_using_seed(response=response, load_type="single")

    def validate_doc(self, key, identifier_token=IDENTIFIER_TOKEN, retry=1000, retry_interval=0.2, delete_record=False):
        result = dict()
        self.retry = retry
        self.retry_interval = retry_interval
        self.delete_record = delete_record

        payload = {"identifierToken": identifier_token, "bucket": self.bucket.name, "scope": self.scope_name,
                   "collection": self.collection_name}

        single_operation_config = self.create_payload_single_operation_config(key=key)

        payload.update(self.cluster_config)
        payload.update(single_operation_config)
        method = "POST"
        path = "/single-doc-validate"

        response = self.send_request(path=path, method=method, payload=payload)
        data, _, failure_count, _ = self.get_result_using_seed(response, load_type="single")
        if not data[key]['status']:
            result.update({"key": key, "value": "", "error": data[key]["errorString"],
                           "status": False, "cas": 0})
        else:
            result.update({"key": key, "value": "", "error": "",
                           "status": True, "cas": data[key]["cas"]})
        return result

    def do_single_operation(self, op_type, identifier_token=IDENTIFIER_TOKEN, insert_options=None, remove_options=None,
                            replace_options=None, single_operation_config=None, retry=1000, retry_interval=0.2,
                            delete_record=False):

        self.retry = retry
        self.retry_interval = retry_interval
        self.delete_record = delete_record
        self.op_type = op_type

        payload = {"identifierToken": identifier_token, "bucket": self.bucket.name, "scope": self.scope_name,
                   "collection": self.collection_name}

        payload.update(self.cluster_config)

        if single_operation_config is not None:
            payload.update(single_operation_config)

        path = ""
        method = "POST"

        if op_type == DocLoading.Bucket.DocOps.CREATE:
            path = "/single-create"
            if insert_options is not None:
                payload.update(insert_options)

        elif op_type == DocLoading.Bucket.DocOps.UPDATE:
            path = "/single-upsert"
            if insert_options is not None:
                payload.update(insert_options)

        elif op_type == DocLoading.Bucket.DocOps.DELETE:
            path = "/single-delete"
            if remove_options is not None:
                payload.update(remove_options)

        elif op_type == DocLoading.Bucket.DocOps.READ:
            path = "/single-read"

        elif op_type == DocLoading.Bucket.DocOps.REPLACE:
            path = "/single-replace"
            if replace_options is not None:
                payload.update(replace_options)

        elif op_type == DocLoading.Bucket.DocOps.TOUCH:
            path = "/single-touch"
            if insert_options is not None:
                payload.update(insert_options)
        else:
            self.log.debug("unknown rest doc loading operation")
            raise Exception("unknown rest doc loading operation")

        response = self.send_request(path=path, method=method, payload=payload)
        return self.get_result_using_seed(response, load_type="single")

    def create_payload_exception_handling(self, resultSeed, identifierToken=IDENTIFIER_TOKEN, retryExceptions=[],
                                          ignoreExceptions=[],
                                          retryAttempts=0):
        data = {
            "identifierToken": identifierToken,
            "resultSeed": resultSeed,
            "exceptions": {
                "retryExceptions": retryExceptions,
                "ignoreExceptions": ignoreExceptions,
                "retryAttempts": retryAttempts
            }
        }
        return data

    def retry_exceptions(self, exception_payload, retry=100, retry_interval=1, delete_record=False):
        method = "POST"
        path = "/retry-exceptions"
        self.retry = retry
        self.retry_interval = retry_interval
        self.delete_record = delete_record
        self.op_type = "retry_exception"
        response = self.send_request(path, method, exception_payload)
        return self.get_result_using_seed(response, load_type="bulk")

    def translate_to_json_object(value, doc_type="json"):
        if type(value) == dict:
            return value

    def replace(self, key, value,
                exp=0, exp_unit="seconds",
                persist_to=0, replicate_to=0,
                timeout=5, time_unit="seconds",
                durability="", cas=0, sdk_retry_strategy=None,
                preserve_expiry=None,
                doc_size=512, identifier_token=IDENTIFIER_TOKEN):

        result = dict()

        try:
            replace_options = self.create_payload_replace_options(expiry=exp, cas=cas, persist_to=persist_to,
                                                                  replicate_to=replicate_to,
                                                                  durability=durability, timeout=timeout)
            single_operation_config = self.create_payload_single_operation_config(key=key, value=value,
                                                                                  doc_size=doc_size)
            data, _, failure_count, _ = self.do_single_operation(DocLoading.Bucket.DocOps.REPLACE,
                                                                 identifier_token=identifier_token,
                                                                 replace_options=replace_options,
                                                                 single_operation_config=single_operation_config,
                                                                 delete_record=False)
            if not data[key]['status']:
                result.update({"key": key, "value": "", "error": data[key]["errorString"],
                               "status": False, "cas": 0})
            else:
                result.update({"key": key, "value": "", "error": None,
                               "status": True, "cas": data[key]["cas"]})

        except Exception as e:
            self.log.error("Something else happened: " + str(e))
            result.update({"key": key, "value": "",
                           "error": str(e), "cas": 0, "status": False})
        return result

    def sub_doc_replace(self, key, sub_key, create_path, xattr,
                        exp, preserve_expiry,
                        persist_to, replicate_to,
                        durability, store_semantics,
                        timeout, time_unit,
                        sdk_retry_strategy, cas, value=None,
                        doc_size=100, identifier_token=IDENTIFIER_TOKEN):

        failed = dict()
        success = dict()

        try:
            replace_spec_options = self.build_replace_spec_options(is_xattr=xattr)

            single_sub_doc_operation_config = self.create_payload_single_sub_doc_operation_config(
                key=key, path=sub_key, value=value, doc_size=doc_size)

            mutate_in_options = self.build_mutate_in_options(
                expiry=exp, cas=cas, persist_to=persist_to, replicate_to=replicate_to,
                durability=durability, store_semantic=store_semantics, timeout=timeout,
                preserve_expiry=preserve_expiry)

            data, _, failure_count, _ = self.do_single_sub_doc_operation(
                op_type=DocLoading.Bucket.SubDocOps.REPLACE, identifier_token=identifier_token,
                replace_spec_options=replace_spec_options, mutate_in_options=mutate_in_options,
                single_sub_doc_operation_config=single_sub_doc_operation_config, delete_record=False)

            if not data[key]['status']:
                failed[key] = dict()
                failed.update({"key": key, "value": "",
                               "error": data[key]["errorString"], "status": False,
                               "cas": 0, "path_val": [(sub_key, "")]})
            else:
                success[key] = dict()
                success[key].update({"key": key, "value": "", "error": None,
                                     "status": True, "cas": data[key]["cas"],
                                     "path_val": [(sub_key, "")]})

        except Exception as e:
            self.log.error("Something else happened: " + str(e))
            failed[key] = dict()
            failed[key].update({"key": key, "value": "",
                                "error": str(e), "cas": 0, "status": False,
                                "path_val": [(sub_key, "")]})
        return success, failed

    def touch(self, key, exp=0, exp_unit="seconds",
              persist_to=0, replicate_to=0,
              durability="",
              timeout=5, time_unit="seconds",
              sdk_retry_strategy=None,
              doc_size=512, identifier_token=IDENTIFIER_TOKEN):

        result = {}

        try:
            insert_options = self.create_payload_insert_options(expiry=exp, persist_to=persist_to,
                                                                replicate_to=replicate_to,
                                                                durability=durability, timeout=timeout)

            single_operation_config = self.create_payload_single_operation_config(key=key)

            data, _, failure_count, _ = self.do_single_operation(DocLoading.Bucket.DocOps.TOUCH,
                                                                 identifier_token=identifier_token,
                                                                 insert_options=insert_options,
                                                                 single_operation_config=single_operation_config,
                                                                 delete_record=False)
            if not data[key]['status']:
                result.update({"key": key, "value": None, "error": data[key]["errorString"], "status": False,
                               "cas": 0})
            else:
                result.update({"key": key, "status": True, "value": "", "error": None, "cas": data[key]["cas"]})

        except Exception as e:
            self.log.error("Something else happened: " + str(e))
            result.update({"key": key, "value": "",
                           "error": str(e), "cas": 0, "status": False})
            return result

    def read(self, key, timeout=5, time_unit="seconds",
             sdk_retry_strategy=None, access_deleted=False, identifier_token=IDENTIFIER_TOKEN):
        result = {}
        try:

            single_operation_config = self.create_payload_single_operation_config(key=key)

            data, _, failure_count, _ = self.do_single_operation(DocLoading.Bucket.DocOps.READ,
                                                                 identifier_token=identifier_token,
                                                                 single_operation_config=single_operation_config,
                                                                 delete_record=False)
            if not data[key]['status']:
                result.update({"key": key, "value": None,
                               "error": data[key]["errorString"], "status": False,
                               "cas": 0})
            else:
                result.update({"key": key, "value": None, "error": None,
                               "status": True, "cas": data[key]["cas"]})

        except Exception as e:
            self.log.error("Something else happened: " + str(e))
            result.update({"key": key, "value": None,
                           "error": str(e), "cas": 0, "status": False})
        return result

    def upsert(self, key, value,
               exp=0, exp_unit="seconds",
               persist_to=0, replicate_to=0,
               timeout=5, time_unit="seconds",
               durability="", sdk_retry_strategy=None, preserve_expiry=None,
               doc_size=512, identifier_token=IDENTIFIER_TOKEN):

        result = dict()

        try:
            insert_options = self.create_payload_insert_options(expiry=exp, persist_to=persist_to,
                                                                replicate_to=replicate_to,
                                                                durability=durability, timeout=timeout)

            single_operation_config = self.create_payload_single_operation_config(key=key, value=value,
                                                                                  doc_size=doc_size)

            data, _, failure_count, _ = self.do_single_operation(DocLoading.Bucket.DocOps.UPDATE,
                                                                 identifier_token=identifier_token,
                                                                 insert_options=insert_options,
                                                                 single_operation_config=single_operation_config,
                                                                 delete_record=False)
            if not data[key]['status']:
                result.update({"key": key, "value": "",
                               "error": data[key]["errorString"], "status": False,
                               "cas": 0})
            else:
                result.update({"key": key, "value": "", "error": None,
                               "status": True, "cas": data[key]["cas"]})
        except Exception as e:
            self.log.error("Something else happened: " + str(e))
            result.update({"key": key, "value": "",
                           "error": str(e), "cas": 0, "status": False})
        return result

    def sub_doc_upsert(self, key, sub_key, create_path, xattr,
                       exp, preserve_expiry,
                       persist_to, replicate_to,
                       durability, store_semantics,
                       timeout, time_unit,
                       sdk_retry_strategy, cas, value=None,
                       doc_size=100, identifier_token=IDENTIFIER_TOKEN):

        failed = dict()
        success = dict()

        try:
            insert_spec_options = self.build_insert_spec_options(create_path=create_path, is_xattr=xattr)

            single_sub_doc_operation_config = self.create_payload_single_sub_doc_operation_config(
                key=key, path=sub_key, value=value, doc_size=doc_size)

            mutate_in_options = self.build_mutate_in_options(
                expiry=exp, cas=cas, persist_to=persist_to, replicate_to=replicate_to,
                durability=durability, store_semantic=store_semantics, timeout=timeout,
                preserve_expiry=preserve_expiry)

            data, _, failure_count, _ = self.do_single_sub_doc_operation(
                op_type=DocLoading.Bucket.SubDocOps.UPSERT, identifier_token=identifier_token,
                insert_spec_options=insert_spec_options, mutate_in_options=mutate_in_options,
                single_sub_doc_operation_config=single_sub_doc_operation_config, delete_record=False)

            if not data[key]['status']:
                failed[key] = dict()
                failed[key].update({"key": key, "value": "",
                                    "error": data[key]["errorString"], "status": False,
                                    "cas": 0,
                                    "path_val": [(sub_key, "")]})
            else:
                success[key] = dict()
                success[key].update({"key": key, "value": "", "error": None,
                                     "status": True, "cas": data[key]["cas"],
                                     "path_val": [(sub_key, "")]})

        except Exception as e:
            self.log.error("Something else happened: " + str(e))
            failed[key] = dict()
            failed[key].update({"key": key, "value": "",
                                "error": str(e), "cas": 0, "status": False,
                                "path_val": [(sub_key, "")]})
        return success, failed

    def insert(self, key, value,
               exp=0, exp_unit="seconds",
               persist_to=0, replicate_to=0,
               timeout=5, time_unit="seconds",
               durability="", sdk_retry_strategy=None, preserve_expiry=None,
               doc_size=512, identifier_token=IDENTIFIER_TOKEN):

        result = dict()

        try:
            insert_options = self.create_payload_insert_options(expiry=exp, persist_to=persist_to,
                                                                replicate_to=replicate_to,
                                                                durability=durability, timeout=timeout)

            single_operation_config = self.create_payload_single_operation_config(key=key, value=value,
                                                                                  doc_size=doc_size)

            data, _, failure_count, _ = self.do_single_operation(DocLoading.Bucket.DocOps.CREATE,
                                                                 identifier_token=identifier_token,
                                                                 insert_options=insert_options,
                                                                 single_operation_config=single_operation_config,
                                                                 delete_record=False)
            if not data[key]['status']:
                result.update({"key": key, "value": "",
                               "error": data[key]["errorString"], "status": False,
                               "cas": 0})
            else:
                result.update({"key": key, "value": "", "error": None,
                               "status": True, "cas": data[key]["cas"]})

        except Exception as e:
            self.log.error("Something else happened: " + str(e))
            result.update({"key": key, "value": "",
                           "error": str(e), "cas": 0, "status": False})
        return result

    def sub_doc_insert(self, key, sub_key, create_path, xattr,
                       exp, preserve_expiry,
                       persist_to, replicate_to,
                       durability, store_semantics,
                       timeout, time_unit,
                       sdk_retry_strategy, cas, value=None,
                       doc_size=100, identifier_token=IDENTIFIER_TOKEN):

        failed = dict()
        success = dict()

        try:
            insert_spec_options = self.build_insert_spec_options(create_path=create_path, is_xattr=xattr)

            single_sub_doc_operation_config = self.create_payload_single_sub_doc_operation_config(
                key=key, path=sub_key, value=value, doc_size=doc_size)

            mutate_in_options = self.build_mutate_in_options(
                expiry=exp, cas=cas, persist_to=persist_to, replicate_to=replicate_to,
                durability=durability, store_semantic=store_semantics, timeout=timeout,
                preserve_expiry=preserve_expiry)

            data, _, failure_count, _ = self.do_single_sub_doc_operation(
                op_type=DocLoading.Bucket.SubDocOps.INSERT, identifier_token=identifier_token,
                insert_spec_options=insert_spec_options, mutate_in_options=mutate_in_options,
                single_sub_doc_operation_config=single_sub_doc_operation_config, delete_record=False)

            if not data[key]['status']:
                failed[key] = dict()
                failed[key].update({"key": key, "value": "",
                                    "error": data[key]["errorString"], "status": False,
                                    "cas": 0,
                                    "path_val": [(sub_key, "")]})
            else:
                success[key] = dict()
                success[key].update({"key": key, "value": "", "error": None,
                                     "status": True, "cas": data[key]["cas"],
                                     "path_val": [(sub_key, "")]})

        except Exception as e:
            self.log.error("Something else happened: " + str(e))
            failed[key] = dict()
            failed.update({"key": key, "value": None,
                           "error": str(e), "cas": 0, "status": False,
                           "path_val": [(sub_key, "")]})
        return success, failed

    def delete(self, key, persist_to=0, replicate_to=0,
               timeout=5, time_unit="seconds",
               durability="", cas=0, sdk_retry_strategy=None, identifier_token=IDENTIFIER_TOKEN):

        result = dict()
        result["cas"] = 0
        try:
            remove_options = self.create_payload_remove_options(cas=cas, persist_to=persist_to,
                                                                replicate_to=replicate_to,
                                                                durability=durability, timeout=timeout)

            single_operation_config = self.create_payload_single_operation_config(key=key)

            data, _, failure_count, _ = self.do_single_operation(DocLoading.Bucket.DocOps.DELETE,
                                                                 identifier_token=identifier_token,
                                                                 remove_options=remove_options,
                                                                 single_operation_config=single_operation_config,
                                                                 delete_record=False)
            if not data[key]['status']:
                result.update({"key": key, "error": data[key]["errorString"],
                               "status": False, "cas": 0})
            else:
                result.update({"key": key, "status": True, "error": None, "cas": data[key]["cas"]})

        except Exception as e:
            self.log.error("Something else happened: " + str(e))
            result.update({"key": key, "value": None,
                           "error": str(e), "cas": 0, "status": False})
        return result

    def sub_doc_read(self, key, sub_key, xattr,
                     timeout=5, time_unit=None, sdk_retry_strategy=None, value=None, identifier_token=IDENTIFIER_TOKEN):

        failed = dict()
        success = dict()

        try:
            get_spec_options = self.build_get_spec_options(is_xattr=xattr)

            single_sub_doc_operation_config = self.create_payload_single_sub_doc_operation_config(
                key=key, path=sub_key, value=value)

            lookup_in_option = self.build_lookup_in_options(timeout=timeout)

            data, _, failure_count, _ = self.do_single_sub_doc_operation(
                op_type=DocLoading.Bucket.SubDocOps.LOOKUP, identifier_token=identifier_token,
                get_spec_options=get_spec_options, lookup_in_options=lookup_in_option,
                single_sub_doc_operation_config=single_sub_doc_operation_config, delete_record=False)

            if not data[key]['status']:
                failed[key] = dict()
                failed[key].update({"key": key, "value": "",
                                    "error": data[key]["errorString"], "status": False,
                                    "cas": 0,
                                    "path_val": [(sub_key, "")]})
            else:
                success[key] = dict()
                success[key].update({"key": key, "value": "", "error": None,
                                     "status": True, "cas": data[key]["cas"],
                                     "path_val": [(sub_key, "")]})

        except Exception as e:
            self.log.error("Something else happened: " + str(e))
            failed[key] = dict()
            failed[key].update({"key": key, "value": None,
                                "error": str(e), "cas": 0, "status": False,
                                "path_val": [(sub_key, "")]})
        return success, failed

    def sub_doc_delete(self, key, sub_key, create_path, xattr,
                       exp, preserve_expiry,
                       persist_to, replicate_to,
                       durability, store_semantics,
                       timeout, time_unit,
                       sdk_retry_strategy, cas, value=None, identifier_token=IDENTIFIER_TOKEN):

        failed = dict()
        success = dict()

        try:
            remove_spec_options = self.build_remove_spec_options(is_xattr=xattr)

            single_sub_doc_operation_config = self.create_payload_single_sub_doc_operation_config(
                key=key, path=sub_key, value=value)

            mutate_in_options = self.build_mutate_in_options(
                expiry=exp, cas=cas, persist_to=persist_to, replicate_to=replicate_to,
                durability=durability, store_semantic=store_semantics, timeout=timeout,
                preserve_expiry=preserve_expiry)

            data, _, failure_count, _ = self.do_single_sub_doc_operation(
                op_type=DocLoading.Bucket.SubDocOps.REMOVE, identifier_token=identifier_token,
                remove_spec_options=remove_spec_options, mutate_in_options=mutate_in_options,
                single_sub_doc_operation_config=single_sub_doc_operation_config, delete_record=False)

            if not data[key]['status']:
                failed[key] = dict
                failed[key].update({"key": key, "value": "",
                                    "error": data[key]["errorString"], "status": False,
                                    "cas": 0,
                                    "path_val": [(sub_key, "")]})
            else:
                success[key] = dict()
                success[key].update({"key": key, "value": "", "error": None,
                                     "status": True, "cas": data[key]["cas"],
                                     "path_val": [(sub_key, "")]})

        except Exception as e:
            self.log.error("Something else happened: " + str(e))
            failed[key] = dict
            failed[key].update({"key": key, "value": None,
                                "error": str(e), "cas": 0, "status": False,
                                "path_val": [(sub_key, "")]})
        return success, failed

    def sub_doc_increment(self, key, sub_key, create_path, xattr,
                          exp, preserve_expiry,
                          persist_to, replicate_to,
                          durability, store_semantics,
                          timeout, time_unit,
                          sdk_retry_strategy, cas, value=None, identifier_token=IDENTIFIER_TOKEN):

        failed = dict()
        success = dict()

        try:
            insert_spec_options = self.build_insert_spec_options(is_xattr=xattr)

            single_sub_doc_operation_config = self.create_payload_single_sub_doc_operation_config(
                key=key, path=sub_key, value=value)

            mutate_in_options = self.build_mutate_in_options(
                expiry=exp, cas=cas, persist_to=persist_to, replicate_to=replicate_to,
                durability=durability, store_semantic=store_semantics, timeout=timeout,
                preserve_expiry=preserve_expiry)

            data, _, failure_count, _ = self.do_single_sub_doc_operation(
                op_type=DocLoading.Bucket.SubDocOps.COUNTER, identifier_token=identifier_token,
                insert_spec_options=insert_spec_options, mutate_in_options=mutate_in_options,
                single_sub_doc_operation_config=single_sub_doc_operation_config, delete_record=False)
            if not data[key]['status']:
                failed[key] = dict
                failed[key].update({"key": key, "value": "",
                                    "error": data[key]["errorString"], "status": False,
                                    "cas": 0,
                                    "path_val": [(sub_key, "")]})
            else:
                success[key] = dict()
                success[key].update({"key": key, "value": "",
                                     "status": True, "cas": data[key]["cas"],
                                     "path_val": [(sub_key, "")]})

        except Exception as e:
            self.log.error("Something else happened: " + str(e))
            failed[key] = dict
            failed[key].update({"key": key, "value": None,
                                "error": str(e), "cas": 0, "status": False,
                                "path_val": [(sub_key, "")]})
        return success, failed

    def crud(self, op_type, key, value=None, exp=0, replicate_to=0,
             persist_to=0, durability="",
             timeout=5, time_unit="seconds",
             create_path=True, xattr=False, cas=0, sdk_retry_strategy=None,
             store_semantics=None, preserve_expiry=None, access_deleted=False,
             create_as_deleted=False, identifier_token=IDENTIFIER_TOKEN, doc_size=512,
             sub_value_size=100):
        result = None
        if op_type == DocLoading.Bucket.DocOps.UPDATE:
            result = self.upsert(
                key, value, exp=exp,
                persist_to=persist_to, replicate_to=replicate_to,
                durability=durability,
                timeout=timeout, time_unit=time_unit,
                sdk_retry_strategy=sdk_retry_strategy,
                preserve_expiry=preserve_expiry,
                identifier_token=identifier_token, doc_size=doc_size)
        elif op_type == DocLoading.Bucket.DocOps.CREATE:
            result = self.insert(
                key, value, exp=exp,
                persist_to=persist_to, replicate_to=replicate_to,
                durability=durability,
                timeout=timeout, time_unit=time_unit,
                sdk_retry_strategy=sdk_retry_strategy,
                identifier_token=IDENTIFIER_TOKEN, doc_size=doc_size)
        elif op_type == DocLoading.Bucket.DocOps.DELETE:
            result = self.delete(
                key,
                persist_to=persist_to, replicate_to=replicate_to,
                durability=durability,
                timeout=timeout, time_unit=time_unit,
                sdk_retry_strategy=sdk_retry_strategy,
                identifier_token=IDENTIFIER_TOKEN)
        elif op_type == DocLoading.Bucket.DocOps.REPLACE:
            result = self.replace(
                key, value, exp=exp,
                persist_to=persist_to, replicate_to=replicate_to,
                durability=durability,
                timeout=timeout, time_unit=time_unit,
                cas=cas,
                sdk_retry_strategy=sdk_retry_strategy,
                preserve_expiry=preserve_expiry,
                identifier_token=IDENTIFIER_TOKEN, doc_size=doc_size)
        elif op_type == DocLoading.Bucket.DocOps.TOUCH:
            result = self.touch(
                key, exp=exp,
                persist_to=persist_to, replicate_to=replicate_to,
                durability=durability,
                timeout=timeout, time_unit=time_unit,
                sdk_retry_strategy=sdk_retry_strategy,
                identifier_token=IDENTIFIER_TOKEN, doc_size=doc_size)
        elif op_type == DocLoading.Bucket.DocOps.READ:
            result = self.read(
                key, timeout=timeout, time_unit=time_unit,
                access_deleted=access_deleted,
                sdk_retry_strategy=sdk_retry_strategy)
        elif op_type in [DocLoading.Bucket.SubDocOps.INSERT, "subdoc_insert"]:
            sub_key, value = value[0], value[1]
            path_val = dict()
            path_val[key] = [(sub_key, value)]
            return self.sub_doc_insert(key=key, sub_key=sub_key, value=value, create_path=create_path, xattr=xattr,
                                       exp=exp, preserve_expiry=preserve_expiry,
                                       persist_to=persist_to, replicate_to=replicate_to,
                                       durability=durability, store_semantics=store_semantics,
                                       timeout=timeout, time_unit=time_unit,
                                       sdk_retry_strategy=sdk_retry_strategy, cas=cas,
                                       identifier_token=IDENTIFIER_TOKEN, doc_size=sub_value_size)
        elif op_type in [DocLoading.Bucket.SubDocOps.UPSERT, "subdoc_upsert"]:
            sub_key, value = value[0], value[1]
            path_val = dict()
            path_val[key] = [(sub_key, value)]
            return self.sub_doc_upsert(key=key, sub_key=sub_key, value=value, create_path=create_path, xattr=xattr,
                                       exp=exp, preserve_expiry=preserve_expiry,
                                       persist_to=persist_to, replicate_to=replicate_to,
                                       durability=durability, store_semantics=store_semantics,
                                       timeout=timeout, time_unit=time_unit,
                                       sdk_retry_strategy=sdk_retry_strategy, cas=cas,
                                       identifier_token=IDENTIFIER_TOKEN, doc_size=sub_value_size)
        elif op_type in [DocLoading.Bucket.SubDocOps.REMOVE, "subdoc_delete"]:
            path_val = dict()
            path_val[key] = [(value, '')]
            return self.sub_doc_delete(key=key, sub_key=value, value='', create_path=create_path, xattr=xattr,
                                       exp=exp, preserve_expiry=preserve_expiry,
                                       persist_to=persist_to, replicate_to=replicate_to,
                                       durability=durability, store_semantics=store_semantics,
                                       timeout=timeout, time_unit=time_unit,
                                       sdk_retry_strategy=sdk_retry_strategy, cas=cas,
                                       identifier_token=IDENTIFIER_TOKEN)
        elif op_type == "subdoc_replace" or op_type == DocLoading.Bucket.SubDocOps.REPLACE:
            sub_key, value = value[0], value[1]
            path_val = dict()
            path_val[key] = [(sub_key, value)]
            return self.sub_doc_replace(key=key, sub_key=sub_key, value=value, create_path=create_path, xattr=xattr,
                                        exp=exp, preserve_expiry=preserve_expiry,
                                        persist_to=persist_to, replicate_to=replicate_to,
                                        durability=durability, store_semantics=store_semantics,
                                        timeout=timeout, time_unit=time_unit,
                                        sdk_retry_strategy=sdk_retry_strategy, cas=cas,
                                        identifier_token=IDENTIFIER_TOKEN, doc_size=sub_value_size)
        elif op_type in [DocLoading.Bucket.SubDocOps.LOOKUP, "subdoc_read"]:
            path_val = dict()
            path_val[key] = [(value, '')]
            return self.sub_doc_read(key=key, sub_key=value, xattr=xattr)
        elif op_type == DocLoading.Bucket.DocOps.SINGLE_VALIDATE:
            result = self.validate_doc(key=key)
        else:
            self.log.error("Unsupported operation %s" % op_type)
        return result

    def close(self):
        pass

    # def insert_binary_document(self, keys, sdk_retry_strategy=None):
    #     options = \
    #         SDKOptions.get_insert_options(
    #             sdk_retry_strategy=sdk_retry_strategy) \
    #             .transcoder(RawBinaryTranscoder.INSTANCE)
    #     for key in keys:
    #         binary_value = Unpooled.copiedBuffer('{value":"' + key + '"}',
    #                                              CharsetUtil.UTF_8)
    #         self.collection.upsert(key, binary_value, options)
    #
    def insert_string_document(self, keys, sdk_retry_strategy=None):
        for key in keys:
            self.crud(DocLoading.Bucket.DocOps.UPDATE, key, '{value":"' + key + '"}')

    def insert_xattr_attribute(self, document_id, path, value, xattr=True,
                               create_parents=True):
        self.crud("subdoc_insert",
                  document_id,
                  [path, value],
                  create_path=create_parents,
                  xattr=xattr)

    def update_xattr_attribute(self, document_id, path, value, xattr=True,
                               create_parents=True):
        self.crud("subdoc_upsert",
                  document_id,
                  [path, value],
                  create_path=create_parents,
                  xattr=xattr)

    # def insert_json_documents(self, key_prefix, documents):
    #     for index, data in enumerate(documents):
    #         self.collection.insert(key_prefix + str(index),
    #                                JsonObject.fromJson(data))

    def warmup_bucket(self, identifier_token=IDENTIFIER_TOKEN):
        payload = {"identifierToken": identifier_token,
                   "bucket": self.bucket.name}
        payload.update(self.cluster_config)
        path = "/warmup-bucket"
        method = "POST"
        _ = self.send_request(path=path, method=method, payload=payload)
