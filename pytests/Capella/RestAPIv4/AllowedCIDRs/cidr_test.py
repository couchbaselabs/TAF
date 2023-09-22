
import random
import string
import requests
from pytests.basetestcase import BaseTestCase
from capellaAPI.capella.dedicated.CapellaAPI_v4 import CapellaAPI
from pytests.Capella.RestAPIv4.clusterBase import ClusterBase
from couchbase_utils.capella_utils.dedicated import CapellaUtils


class CIDRTest(ClusterBase):
    def setUp(self):
        ClusterBase.setUp(self)


    def tearDown(self):
        ClusterBase.tearDown(self)


    def test_create_allowed_cidr(self):
        urltestCases =[
            {
                "description": "valid test case",
                "url": "/v4/organizations" + \
                       "/{}/projects/{}/clusters/{}/allowedcidrs",
                "expected_status_code":201,
                "message_contains": "{\"id\":"
            },
            {
                "description":"changing the resource URL segments from singular to plural",
                "url": "/v4/organizations" + \
                                    "/{}/projects/{}/clusters/{}/allowedcidr",
                "expected_status_code":404,
                "expected_error": "404 page not found\n"
            },
            {
                "description":"adding invalid segment in the url",
                "url":"/v4/organizations" + \
                                    "/{}/projects/{}/clusters/{}/allowedcidrs/cidr",
                "expected_status_code":405,
                "expected_error": ""
            },
            {
                "description": "Replace v4 with v3",
                "url": "/v3/organizations" + \
                       "/{}/projects/{}/clusters/{}/allowedcidrs",
                "expected_status_code": 404,
                "expected_error":"{\"errorType\":\"RouteNotFound\",\"message\":\"Not found\"}",
            },
        ]
        cidr = CapellaUtils.get_next_cidr()+"/20"
        self.log.info("The cidr to be added is {}".format(cidr))
        endpointFunctionParams=(self.organisation_id, self.project_id, self.cluster_id, cidr, "Temp cidr")
        self.check_testcases(urltestCases,self.capellaAPI.cluster_ops_apis.add_CIDR_to_allowed_CIDRs_list,endpointFunctionParams)



    def check_testcases(self,testcases,endpointFunction, endpointFunctionParams):
        failures = list()
        for testcase in testcases:
            self.log.info("Executing test: {}".format(testcase["description"]))
            if "url" in testcase:
                self.capellaAPI.cluster_ops_apis.allowedCIDR_endpoint=testcase["url"]
            resp = endpointFunction(*endpointFunctionParams)
            expected_status_code=testcase["expected_status_code"]


            if resp.status_code != expected_status_code:
                failures.append(testcase["description"])

            elif "expected_error" in testcase:
                if resp.content != testcase["expected_error"]:
                    failures.append(testcase["description"])
                    self.log.warning("Result: {}".format(resp))
                    self.log.info(testcase["description"])
                    self.log.info(testcase["expected_error"])
                    self.log.info(resp.content)
            elif "message_contains" in testcase:
                if testcase["message_contains"] not in resp.content:
                    failures.append(testcase["description"])

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests".format(
                len(failures), len(testcases)))



