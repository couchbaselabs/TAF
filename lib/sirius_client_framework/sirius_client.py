import json
import random
import time
from global_vars import logger
import requests

from TestInput import TestInputSingleton
from sirius_client_framework import sirius_constants
from sirius_client_framework.sirius_setup import SiriusSetup


class SiriusClient(object):
    def __init__(self, default_sirius_base_url="http://0.0.0.0:4000",
                 identifier_token="UNIQUE_STRING", retry=1000,
                 retry_interval=0.2,
                 delete_record=False):
        self.log = logger.get("test")
        self.sirius_base_urls = TestInputSingleton.input.param(
            "sirius_url", default_sirius_base_url)
        self.sirius_base_url_list = self.sirius_base_urls.split("|")
        self.sirius_base_url = random.choice(self.sirius_base_url_list)
        self.identifier_token = identifier_token
        self.retry = retry
        self.retry_interval = retry_interval
        self.delete_record = delete_record

        if not SiriusSetup().is_sirius_online(self.sirius_base_url):
            raise Exception("sirius not online")

    def send_request_to_sirius(self, path, payload=None):
        headers = {"Content-Type": "application/json"}
        url = self.sirius_base_url + path
        exception = None
        for i in range(5):
            try:
                response = requests.post(url, headers=headers, json=payload)
                return response
            except requests.exceptions.RequestException as erred:
                self.log.critical(erred)
            except Exception as e:
                exception = e

        if exception is not None:
            raise exception

    def workload_result(self, response):
        fail = {}
        if response.status_code != 200:
            raise Exception("unable to initialize load on sirius")

        result_from_request = json.loads(response.content)
        result_using_seed = {}
        if not result_from_request["error"]:
            request_using_seed = {
                "seed": result_from_request["data"]["seed"],
                "deleteRecord": self.delete_record
            }
        else:
            self.log.critical("error in initiating task on sirius")
            raise Exception("bad/malformed payload in request")

        for i in range(0, self.retry):
            response = self.send_request_to_sirius("/result",
                                                   request_using_seed)
            result_using_seed = json.loads(response.content)
            if not result_using_seed["error"]:
                break
            time.sleep(self.retry_interval * 60)

        if response.status_code != 200:
            self.log.critical(str(request_using_seed["seed"]))
            raise Exception("bad http status on retrieving result from sirius")
        if not result_using_seed["error"]:
            if (
                    "otherErrors" in result_using_seed["data"].keys()
                    and result_using_seed["data"]["otherErrors"] != ""
            ):
                self.log.critical(result_using_seed["data"]["otherErrors"])
                raise Exception(result_using_seed["data"]["otherErrors"])

            for error_name, failed_documents in result_using_seed["data"][
                    "bulkErrors"].items():
                for doc_failed in failed_documents:
                    key = doc_failed["key"]
                    fail[key] = dict()
                    fail[key]["error"] = error_name
                    fail[key]["value"] = {}
                    fail[key]["status"] = False
                    fail[key]["offset"] = doc_failed["Offset"]

            return fail, \
                result_using_seed["data"]["success"], \
                result_using_seed["data"]["failure"], \
                result_from_request["data"]["seed"],

        else:
            self.log.critical(result_using_seed["message"])
            raise Exception(result_using_seed["message"])

    def start_workload(self, op_type, database_info, operation_config):
        endpoint = sirius_constants.WORKLOAD_PATH[op_type]
        payload = {"identifierToken": self.identifier_token,
                   "dbType": database_info.db_type,
                   "connectionString": database_info.connection_string,
                   "username": database_info.username,
                   "password": database_info.password,
                   "operationConfig": operation_config.get_parameters(),
                   "extra": database_info.get_parameters()}
        response = self.send_request_to_sirius(path=endpoint, payload=payload)
        return self.workload_result(response=response)

    def database_management(self, op_type, database_info,
                            operation_config):
        endpoint = sirius_constants.DB_MGMT_PATH[op_type]
        payload = {"identifierToken": self.identifier_token,
                   "dbType": database_info.db_type,
                   "connectionString": database_info.connection_string,
                   "username": database_info.username,
                   "password": database_info.password,
                   "operationConfig": operation_config.get_parameters(),
                   "extra": database_info.get_parameters()}
        try:
            response = self.send_request_to_sirius(path=endpoint,
                                                   payload=payload)
            # Raise error for non-200 status
            if response.status_code != 200:
                raise Exception(response.json())

            data = response.json()
            return data["error"], data["message"], data["data"]
        except requests.exceptions.RequestException as e:
            raise Exception("An error occurred: {}".format(str(e)))

    def start_blob_workload(self, op_type, operation_config,
                            external_storage_config):
        endpoint = sirius_constants.BLOB_PATH[op_type]
        payload = {"identifierToken": self.identifier_token,
                   "dbType": external_storage_config.cloud_provider,
                   "operationConfig": operation_config.get_parameters(),
                   "externalStorageExtras":
                       external_storage_config.get_parameters()}
        try:
            response = self.send_request_to_sirius(path=endpoint,
                                                   payload=payload)
        except requests.exceptions.RequestException as e:
            raise Exception("An error occurred: {}".format(str(e)))
        return self.workload_result(response=response)
