import json
import random
import time
from global_vars import logger
import requests

from constants.cloud_constants import DocLoading
from TestInput import TestInputSingleton
from doc_loader.sirius import SiriusClient

from pytests.sdk_workloads.go_workload import IDENTIFIER_TOKEN

path_map_loading = {
    DocLoading.DocOpCodes.DocOps.CREATE: ["/create", "single"],
    DocLoading.DocOpCodes.DocOps.BULKCREATE: ["/bulk-create", "bulk"],
    DocLoading.DocOpCodes.DocOps.UPDATE: ["/upsert", "single"],
    DocLoading.DocOpCodes.DocOps.BULKUPDATE: ["/bulk-upsert", "bulk"],
    DocLoading.DocOpCodes.DocOps.READ: ["/read", "single"],
    DocLoading.DocOpCodes.DocOps.BULKREAD: ["/bulk-read", "bulk"],
    DocLoading.DocOpCodes.SubDocOps.INSERT: ["/sub-doc-insert", "single"],
    DocLoading.DocOpCodes.SubDocOps.UPSERT: ["/sub-doc-upsert", "single"],
    DocLoading.DocOpCodes.SubDocOps.LOOKUP: ["/sub-doc-read", "single"],
    DocLoading.DocOpCodes.SubDocOps.REMOVE: ["/sub-doc-delete", "single"],
    DocLoading.DocOpCodes.DocOps.DELETE: ["/delete", "single"],
    DocLoading.DocOpCodes.DocOps.BULKDELETE: ["/bulk-delete", "bulk"],
    DocLoading.DocOpCodes.DocOps.TOUCH: ["/touch", "single"],
    DocLoading.DocOpCodes.DocOps.BULKTOUCH: ["/bulk-touch", "bulk"],
    DocLoading.DocOpCodes.DocOps.VALIDATECOLUMNAR: ["/validate-columnar", "single"],
    DocLoading.DocOpCodes.DocOps.RETRY: ["/retry-exceptions", "bulk"],
    DocLoading.DocOpCodes.DocOps.VALIDATE: ["/validate", "single"],
}

path_map_mgmt = {
    DocLoading.DocOpCodes.DBMgmtOps.CREATE: "/create-database",
    DocLoading.DocOpCodes.DBMgmtOps.DELETE: "/delete-database",
    DocLoading.DocOpCodes.DBMgmtOps.LIST: "/list-database",
    DocLoading.DocOpCodes.DBMgmtOps.COUNT: "/count",
    DocLoading.DocOpCodes.DBMgmtOps.WARMUP: "/warmup-bucket",
    DocLoading.DocOpCodes.DBMgmtOps.CLEAR: "/clear_data",
}

path_map_blob = {
    DocLoading.DocOpCodes.S3MgmtOps.CREATE: "/create-s3-bucket",
    DocLoading.DocOpCodes.S3MgmtOps.DELETE: "/delete-s3-bucket",
    DocLoading.DocOpCodes.S3MgmtOps.INFO: "/get-info",
    DocLoading.DocOpCodes.S3DocOps.CREATEDIR: "/create-folder",
    DocLoading.DocOpCodes.S3DocOps.DELETEDIR: "/delete-folder",
    DocLoading.DocOpCodes.S3DocOps.CREATE: "/create-file",
    DocLoading.DocOpCodes.S3DocOps.UPDATE: "/update-file",
    DocLoading.DocOpCodes.S3DocOps.DELETE: "/delete-file",
    DocLoading.DocOpCodes.S3DocOps.LOAD: "/create-files-in-folders",
    DocLoading.DocOpCodes.S3DocOps.UPDATEFILES: "/update-files-in-folder",
    DocLoading.DocOpCodes.S3DocOps.DELETEFILES: "/delete-files-in-folder",
}


class RESTClient(object):

    def __init__(self, sirius_base_url="http://0.0.0.0:4000"):

        input = TestInputSingleton.input
        self.sirius_base_url = sirius_base_url
        self.sirius_base_url = input.param("sirius_url", self.sirius_base_url)
        self.sirius_base_url_list = self.sirius_base_url.split("|")
        self.sirius_base_url = random.choice(self.sirius_base_url_list)

        self.retry = 1000
        self.retry_interval = 0.2
        self.delete_record = False
        self.log = logger.get("test")

        if not SiriusClient().is_sirius_online(self.sirius_base_url):
            raise Exception("sirius not online")

    def send_request(self, path, payload=None):
        headers = {"Content-Type": "application/json"}

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

    def get_result_using_seed(self, response, load_type):

        fail = {}

        if response.status_code != 200:
            print(
                "unable to start operation (Bad/Malformed) :"
                + " "
                + str(response.status_code)
            )
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
            response = self.send_request("/result", request_using_seed)
            result_using_seed = json.loads(response.content)
            if not result_using_seed["error"]:
                break
            time.sleep(self.retry_interval * 60)

        if response.status_code != 200:
            print(
                self.sirius_base_url + "/result",
                request_using_seed,
                response.status_code,
                result_using_seed,
            )
            print(
                "unable to retrieve the result of operation :"
                + self.op_type
                + " "
                + str(response.status_code)
            )
            raise Exception("Bad HTTP status at fetching result")

        if not result_using_seed["error"]:
            if (
                "otherErrors" in result_using_seed["data"].keys()
                and result_using_seed["data"]["otherErrors"] != ""
            ):
                print(result_using_seed["data"]["otherErrors"])

            if load_type == "bulk":
                for error_name, failed_documents in result_using_seed["data"][
                    "bulkErrors"
                ].items():
                    for doc_failed in failed_documents:
                        key = doc_failed["key"]
                        fail[key] = dict()
                        fail[key]["error"] = doc_failed["errorString"]
                        fail[key]["value"] = {}
                        fail[key]["status"] = False
                        fail[key]["offset"] = doc_failed["Offset"]

                return fail, \
                    result_using_seed["data"]["success"], \
                    result_using_seed["data"]["failure"], \
                    result_from_request["data"]["seed"],
            else:
                return result_using_seed["data"]["singleResult"], \
                    result_using_seed["data"]["success"], \
                    result_using_seed["data"]["failure"], \
                    result_from_request["data"]["seed"],

        else:
            print(result_using_seed["message"])
            raise Exception(result_using_seed["message"])

    def do_data_loading(
        self,
        op_type,
        db_type,
        connection_string,
        username,
        password,
        identifier_token=IDENTIFIER_TOKEN,
        extra=None,
        operation_config=None,
        retry=10,
        retry_interval=1,
        delete_record=False,
    ):
        self.retry = retry
        self.retry_interval = retry_interval
        self.delete_record = delete_record
        self.op_type = op_type
        endpoint = path_map_loading[op_type][0]
        payload = {
            "identifierToken": identifier_token,
            "dbType": db_type,
            "connectionString": connection_string,
            "username": username,
            "password": password,
        }
        if operation_config is not None:
            payload["operationConfig"] = operation_config
        if extra is not None:
            payload["extra"] = extra
        response = self.send_request(path=endpoint, payload=payload)
        return self.get_result_using_seed(
            response=response, load_type=path_map_loading[op_type][1]
        )

    def do_db_mgmt(
        self,
        op_type,
        db_type,
        connection_string,
        username,
        password,
        operation_config,
        identifier_token=IDENTIFIER_TOKEN,
        extra=None,
    ):
        endpoint = path_map_mgmt[op_type]
        payload = {
            "identifierToken": identifier_token,
            "dbType": db_type,
            "connectionString": connection_string,
            "username": username,
            "password": password,
            "operationConfig": operation_config,
        }
        if extra is not None:
            payload["extra"] = extra
        try:
            response = self.send_request(path=endpoint, payload=payload)
            response.raise_for_status()  # Raise error for non-200 status
            data = response.json()
            if data.get("Error"):
                raise Exception(data["message"])
            return data
        except requests.exceptions.RequestException as e:
            raise Exception("An error occurred: {}".format(str(e)))

    def do_blob_loading(
        self,
        op_type,
        external_storage_extras,
        identifier_token=IDENTIFIER_TOKEN,
        extra=None,
        operation_config=None,
        retry=10,
        retry_interval=1,
        delete_record=False,
    ):
        self.retry = retry
        self.retry_interval = retry_interval
        self.delete_record = delete_record
        self.op_type = op_type
        endpoint = path_map_blob[op_type]
        payload = {
            "identifierToken": identifier_token,
            "dbType": "awsS3",
            "externalStorageExtras": external_storage_extras,
        }
        if operation_config is not None:
            payload["operationConfig"] = operation_config
        if extra is not None:
            payload["extra"] = extra
        response = self.send_request(path=endpoint, payload=payload)
        return self.get_result_using_seed(response=response, load_type="bulk")

    def build_rest_payload_extra(
        self,
        compressionDisabled=None,
        compressionMinSize=None,
        compressionMinRatio=None,
        connectionTimeout=None,
        KVTimeout=None,
        KVDurableTimeout=None,
        bucket=None,
        scope=None,
        collection=None,
        expiry=None,
        persistTo=None,
        replicateTo=None,
        durability=None,
        operationTimeout=None,
        cas=None,
        isXattr=None,
        storeSemantic=None,
        preserveExpiry=None,
        createPath=None,
        SDKBatchSize=None,
        database=None,
        query=None,
        connStr=None,
        username=None,
        password=None,
        columnarBucket=None,
        columnarScope=None,
        columnarCollection=None,
        provisioned=None,
        readCapacity=None,
        writeCapacity=None,
        keyspace=None,
        table=None,
        numOfConns=None,
        subDocPath=None,
        replicationFactor=None,
        cassandraClass=None,
        port=None,
        maxIdleConnections=None,
        maxOpenConnections=None,
        maxIdleTime=None,
        maxLifeTime=None,
    ):
        payload = {}
        for param_name, param_value in locals().items():
            if param_name == "self":
                continue
            if param_name != "payload" and param_value is not None:
                payload[param_name] = param_value
        return payload

    def build_rest_payload_operation_config(
        self, start=None, end=None, docSize=None, template=None, fieldToChange=None
    ):
        payload = {}
        for param_name, param_value in locals().items():
            if param_name == "self":
                continue
            if param_name != "payload" and param_value is not None:
                payload[param_name] = param_value
        return payload

    def build_rest_payload_s3_external_storage_extras(
        self,
        awsAccessKey,
        awsSecretKey,
        awsRegion,
        bucket,
        awsSessionToken="empty",
        folderPath=None,
        fileFormat=None,
        filePath=None,
        numFolders=None,
        foldersPerDepth=None,
        filesPerFolder=None,
        folderLevelNames=None,
        maxFolderDepth=None,
        minFileSize=None,
        maxFileSize=None,
    ):
        payload = {}
        for param_name, param_value in locals().items():
            if param_name == "self":
                continue
            if param_name != "payload" and param_value is not None:
                payload[param_name] = param_value
        return payload
