import requests
import json
import time
import logging
from basetestcase import BaseTestCase

headers = {
  "Content-Type": "application/json"
}

buildIndex = True


def clear_test_information(base_url, identifier_token, username, password, bucket_list):
    json_data_request = {
        'username': username,
        'password': password,
        'identifierToken': identifier_token,
    }

    for bucket in bucket_list.keys():
        scopes_and_collection = bucket_list[bucket]
        scope = scopes_and_collection['scope']
        collection_list = scopes_and_collection['collection']
        json_data_request['bucket'] = bucket
        json_data_request['scope'] = scope
        for collection in collection_list:
            json_data_request['collection'] = collection
            exception = None

            for i in range(5):
                try:
                    path = "/clear_data"
                    response = requests.post(url=base_url + path, headers=headers, json=json_data_request)
                    response_data = json.loads(response.content)
                    print("cleaning", response_data)
                    if not response_data["error"]:
                        print(response_data)
                    return
                except Exception as e:
                    print(str(e))
                    exception = e

            raise exception

def read_doc(bucket, domain, scope, collection, start, end, doc_size = 1024, body={}):
    url = domain + "/bulk-read"
    read_body = body
    read_body["operationConfig"] = {
        "start": start,
        "end": end,
        "docSize": doc_size
    }
    read_body["bucket"] = bucket
    read_body["scope"] = scope
    read_body["collection"] = collection
    data = json.dumps(read_body)
    return requests.post(url=url, auth=None, headers=headers, data=data)


def create_doc(bucket, domain, scope, collection, number_of_doc, key_size, doc_size, body={}):
    url = domain + "/bulk-create"
    create_body = body
    create_body["operationConfig"] = {
        "count": number_of_doc,
        "keySize": key_size,
        "docSize": doc_size
    }
    create_body["insertOptions"] = {
        "timeout": 120
    }
    create_body["bucket"] = bucket
    create_body["scope"] = scope
    create_body["collection"] = collection
    data = json.dumps(create_body)
    return requests.post(url=url, auth=None, headers=headers, data=data)


def upsert_doc(bucket, domain, scope, collection, start, end, doc_size=1024, body={}):
    url = domain + "/bulk-upsert"
    upsert_body = body
    upsert_body["operationConfig"] = {
        "start": start,
        "end": end,
        "docSize": doc_size
    }
    upsert_body["insertOptions"] = {
        "timeout": 120
    }
    upsert_body["bucket"] = bucket
    upsert_body["scope"] = scope
    upsert_body["collection"] = collection
    data = json.dumps(upsert_body)
    return requests.post(url=url, auth=None, headers=headers, data=data)


def delete_doc(bucket, domain, scope, collection, start, end, doc_size=1024, body={}):
    url = domain + "/bulk-delete"
    delete_body = body
    delete_body["operationConfig"] = {
        "start": start,
        "end": end,
        "docSize": doc_size
    }
    delete_body["removeOptions"] = {
        "timeout": 120
    }
    delete_body["bucket"] = bucket
    delete_body["scope"] = scope
    delete_body["collection"] = collection
    data = json.dumps(delete_body)
    return requests.post(url=url, auth=None, headers=headers, data=data)


def run_query(bucket, domain, scope, collection, duration, body={}):
    url = domain + "/run-template-query"
    query_body = body
    query_body['operationConfig'] = {
        "buildIndex": buildIndex,
        "template": "Person",
        "duration": duration,
        "BuildIndexViaSDK": False
    }
    query_body["bucket"] = bucket
    query_body["scope"] = scope
    query_body["collection"] = collection
    data = json.dumps(query_body)
    return requests.post(url=url, auth=None, headers=headers, data=data)


def validate_doc(bucket, domain, scope, collection, doc_size=1024, body={}):
    url = domain + "/validate"
    validate_body = body
    validate_body['bucket'] = bucket
    validate_body['operationConfig'] = {
        "docSize": doc_size
    }
    validate_body["scope"] = scope
    validate_body["collection"] = collection
    data = json.dumps(validate_body)
    return requests.post(url=url, auth=None, headers=headers, data=data)


def waitUntilTaskFinish(result_seed,  domain):
    print("I am in wait for result function")
    result_body = {
        "seed": result_seed,
        "deleteRecord": False
    }
    url = domain + "/result"
    data = json.dumps(result_body)
    flag = True
    while flag:
        response = requests.post(url=url, auth=None, headers=headers, data=data)
        error = json.loads(response.content)['error']
        if not error:
            return response
        time.sleep(10)


class GoDocLoader(BaseTestCase):
    def setUp(self):
        super(GoDocLoader, self).setUp()
        self.duration = self.input.capella.get("duration", 1)
        self.bucket_list = self.input.capella.get("bucket_list", [])
        self.username = self.input.capella.get("username", "Administrator")
        self.password = self.input.capella.get("password", "password")
        self.connection_string = self.input.capella.get("connection_string", "")
        self.num_items = self.input.param("num_items", 100000)
        self.indentifier_token = self.input.param("indentifier_token", "Workload-test")
        self.create_body()
        self.convert_bucket_list()
        self.sirius_url = self.input.capella.get("sirius_url", "http://localhost:4000")

    def tearDown(self):
        pass

    def create_body(self):
        self.body = dict()
        self.body['identifierToken'] = self.indentifier_token
        self.body['clusterConfig'] = {
            "username": self.username,
            "password": self.password,
            "connectionString": self.connection_string
        }

    def convert_bucket_list(self):
        self.bucket_list = json.loads(self.bucket_list)

    def run_workload(self):

        def log_results(seed_list, operation):
            for seed in seed_list:
                response = waitUntilTaskFinish(seed, self.sirius_url )
                error = json.loads(response.content)['error']
                success = json.loads(response.content)['data']['success']
                failure = json.loads(response.content)['data']['failure']
                if operation != 'query':
                    bulk_message = json.loads(response.content)['data']['bulkErrors']
                else:
                    bulk_message = json.loads(response.content)['data']['queryErrors']
                print("Success: {}, failure: {} for {}".format(success, failure, operation))
                print("Message: {}".format(bulk_message))

        def validate_doc_thread(bucket_list, doc_size=1024, body={}, header={}):
            seed_list = list()
            for bucket in bucket_list.keys():
                scopes_and_collection = bucket_list[bucket]
                scope = scopes_and_collection['scope']
                collection_list = scopes_and_collection['collection']
                for collection in collection_list:
                    response = validate_doc(bucket, self.sirius_url, scope, collection, 1024, body)
                    error = json.loads(response.content)['error']
                    if not error:
                        seed = json.loads(response.content)['data']['seed']
                        seed_list.append(seed)

            log_results(seed_list, "validate")

        def mutate_doc_thread(bucket_list, mutate_type, body):
            seed_list = list()
            for bucket in bucket_list.keys():
                scopes_and_collection = bucket_list[bucket]
                scope = scopes_and_collection['scope']
                collection_list = scopes_and_collection['collection']
                for collection in collection_list:
                    if mutate_type == "create":
                        response = create_doc(bucket, self.sirius_url, scope, collection, self.num_items, 512, 1024, body)
                    elif mutate_type == "delete":
                        response = delete_doc(bucket, self.sirius_url, scope, collection, 0, self.num_items, 1024, body)
                    if mutate_type == "upsert":
                        response = upsert_doc(bucket, self.sirius_url, scope, collection, 0, self.num_items, 1024, body)
                    elif mutate_type == "read":
                        response = read_doc(bucket, self.sirius_url, scope, collection, 0, self.num_items, 1024, body)
                    elif mutate_type == 'query':
                        response = run_query(bucket, self.sirius_url, scope, collection, 60, body)
                    error = json.loads(response.content)['error']
                    if not error:
                        seed = json.loads(response.content)['data']['seed']
                        seed_list.append(seed)
                    else:
                        print(error)

            log_results(seed_list, mutate_type)

        t_end = time.time() + int(self.duration)
        while time.time() < t_end:
            mutate_doc_thread(self.bucket_list, "create", self.body)
            mutate_doc_thread(self.bucket_list, "upsert", self.body)
            mutate_doc_thread(self.bucket_list, "read", self.body)
            validate_doc_thread(self.bucket_list, "validate", self.body)
            mutate_doc_thread(self.bucket_list, "query", self.body)
            mutate_doc_thread(self.bucket_list, "delete", self.body)
            clear_test_information(self.sirius_url, self.indentifier_token, self.username, self.password, self.bucket_list )
            buildIndex = False
