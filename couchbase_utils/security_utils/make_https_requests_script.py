"""
A standalone requests library created to make https requests with certs
Takes input from file ./couchbase_utils/security_utils/https_input.py
as a string of dictionary.
Writes a suitable output as a string of dictionary to file
./couchbase_utils/security_utils/https_output.py
"""


import requests
import time
from ast import literal_eval


def https_request(api, verb='GET', params='', headers=None, timeout=100,
                  cert=None, verify=False, try_count=3):
    res = dict()
    tries = 0
    while tries < try_count:
        try:
            if verb == 'GET':
                response = requests.get(api, params=params, headers=headers,
                                        timeout=timeout, cert=cert,
                                        verify=verify)
            elif verb == 'POST':
                response = requests.post(api, data=params, headers=headers,
                                         timeout=timeout, cert=cert,
                                         verify=verify)
            elif verb == 'DELETE':
                response = requests.delete(api, data=params, headers=headers,
                                           timeout=timeout, cert=cert,
                                           verify=verify)
            elif verb == "PUT":
                response = requests.put(api, data=params, headers=headers,
                                        timeout=timeout, cert=cert,
                                        verify=verify)
            res["status"] = False
            res["status_code"] = response.status_code
            res["content"] = response.content
            res["reason"] = response.reason
            res["exception"] = None
            if res["status_code"] in [200, 201, 202]:
                res["status"] = True
            return res
        except Exception as e:
            tries = tries + 1
            if tries >= try_count:
                res["exception"] = e
                return res
            else:
                time.sleep(5)


with open('./couchbase_utils/security_utils/https_input.py', 'r') as file:
    data = file.read().replace('\n', '')
input_params = literal_eval(data)

api = input_params["api"]
verb = input_params["verb"]
params = input_params["params"]
headers = input_params["headers"]
timeout = input_params["timeout"]
cert = input_params["cert"]
verify = input_params["verify"]

res = https_request(api=api, verb=verb, params=params,
                    headers=headers, timeout=timeout,
                    cert=cert, verify=verify)

with open('./couchbase_utils/security_utils/https_output.py', 'w') as file:
    file.write(str(res))
