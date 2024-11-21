'''
Author - @koushal.sharma
'''
import time
import json
import base64
import random
import string
import requests
from pytests.security.security_base import SecurityBase
from capellaAPI.capella.dedicated.CapellaAPI import CapellaAPI
from capellaAPI.capella.common.CapellaAPI import CommonCapellaAPI
from couchbase_utils.capella_utils.dedicated import CapellaUtils
from TestInput import TestInputSingleton
from platform_utils.remote.remote_util import RemoteMachineShellConnection

class SecurityTest(SecurityBase):

    def setUp(self):
        try:
            SecurityBase.setUp(self)
        except Exception as e:
            self.tearDown()
            self.fail("Base Setup Failed with error as - {}".format(e))

        self.rest_username = TestInputSingleton.input.membase_settings.rest_username
        self.rest_password = TestInputSingleton.input.membase_settings.rest_password

    def tearDown(self):
        super(SecurityTest, self).tearDown()

    def generate_health_report(self):
        self.log.info("Generating a health report for the cluster")
        report_id = ""
        capella_api = CapellaAPI("https://" + self.url, self.secret_key, self.access_key, self.user,
                                 self.passwd)
        resp = capella_api.generate_health_report(self.tenant_id, self.project_id, self.cluster_id)
        self.log.info("The generate health report response is - {}, Status Code - {}".format(
                                                                        resp.content,
                                                                        resp.status_code))
        if resp.status_code == 429:
            resp = self.capellaAPIv2.list_health_reports(self.tenant_id, self.project_id,
                                                         self.cluster_id)
            self.assertEqual(resp.status_code, 200,
                             msg="FAIL. Outcome: {}, Expected: {}, Reason: {}".format(
                             resp.status_code, 200, resp.content))
            resp = resp.json()
            report_id = resp["data"][0]["id"]
        else:
            self.assertEqual(resp.status_code, 202,
                             msg="FAIL. Outcome: {}, Expected: {}, Reason: {}".format(
                             resp.status_code, 202, resp.content))

            resp = self.capellaAPIv2.list_health_reports(self.tenant_id, self.project_id,
                                                         self.cluster_id)
            self.assertEqual(resp.status_code, 200,
                             msg="FAIL. Outcome: {}, Expected: {}, Reason: {}".format(
                                 resp.status_code, 200, resp.content))
            resp = resp.json()
            report_id = resp["data"][0]["id"]
            self.log.info("Generated a health report for the cluster")

        return report_id

    def test_list_health_reports(self):
        self.log.info("Test Started for the List Health Reports Endpoint")
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/health-advisor'.format(
            self.capellaAPIv2.internal_url, self.tenant_id, self.project_id, self.cluster_id)
        url = url + '?page=1&perPage=10&sortBy=createdAt&sortDirection=desc'
        result, error = self.test_authentication(url, method="GET")
        if not result:
            self.fail("Auth test failed. Error: {}".format(error))

        # Test with different tenant ids
        test_method_args = {
            'project_id': self.project_id,
            'cluster_id': self.cluster_id
        }
        result, error = self.test_tenant_ids(self.capellaAPIv2.list_health_reports,
                                             test_method_args, 'tenant_id', 200)
        if not result:
            self.fail("Tenant ids test failed. Error: {}".format(error))

        # Test with different project ids
        test_method_args = {
            'tenant_id': self.tenant_id,
            'cluster_id': self.cluster_id
        }
        result, error = self.test_project_ids(self.capellaAPIv2.list_health_reports,
                                              test_method_args, 'project_id', 200)
        if not result:
            self.fail("Project ids test failed. Error: {}".format(error))

        # Test with org roles
        test_method_args = {
            'tenant_id': self.tenant_id,
            'project_id': self.project_id,
            'cluster_id': self.cluster_id
        }
        result, error = self.test_with_org_roles("list_health_reports",
                                                 test_method_args,
                                                 200, None, "provisioned")
        if not result:
            self.fail("Org roles test failed. Error: {}".format(error))

        # Test with project roles
        result, error = self.test_with_project_roles("list_health_reports",
                                                     test_method_args,
                                                     ["projectOwner", "projectClusterViewer",
                                                      "projectClusterManager", "projectDataWriter",
                                                      "projectDataViewer"],
                                                     200, None, "provisioned")
        if not result:
            self.fail("Project roles test failed. Error: {}".format(error))

        self.log.info("Test Completed for the List Health Reports Endpoint")

    def test_list_health_reports_check(self):
        self.log.info("Started test for List Health Reports Check Endpoint")

        report_id = self.generate_health_report()
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/health-advisor/{}'.format(
                self.capellaAPIv2.internal_url, self.tenant_id, self.project_id, self.cluster_id,
                report_id)
        url = url + '?page=1&perPage=10&sortBy=severity&sortDirection=desc&category=data&severity=good'
        result, error = self.test_authentication(url, method="GET")
        if not result:
            self.fail("Auth test failed. Error: {}".format(error))

        # Test with different tenant ids
        test_method_args = {
            'project_id': self.project_id,
            'cluster_id': self.cluster_id,
            'report_id': report_id
        }
        result, error = self.test_tenant_ids(self.capellaAPIv2.list_health_reports_check,
                                             test_method_args, 'tenant_id', 200)
        if not result:
            self.fail("Tenant ids test failed. Error: {}".format(error))

        # Test with different project ids
        test_method_args = {
            'tenant_id': self.tenant_id,
            'cluster_id': self.cluster_id,
            'report_id': report_id
        }
        result, error = self.test_project_ids(self.capellaAPIv2.list_health_reports_check,
                                              test_method_args, 'project_id', 200)
        if not result:
            self.fail("Project ids test failed. Error: {}".format(error))

        # Test with org roles
        test_method_args = {
            'tenant_id': self.tenant_id,
            'project_id': self.project_id,
            'cluster_id': self.cluster_id,
            'report_id': report_id
        }
        result, error = self.test_with_org_roles("list_health_reports_check",
                                                 test_method_args,
                                                 200, None, "provisioned")
        if not result:
            self.fail("Org roles test failed. Error: {}".format(error))

        # Test with project roles
        result, error = self.test_with_project_roles("list_health_reports_check",
                                                     test_method_args,
                                                     ["projectOwner", "projectClusterViewer",
                                                      "projectClusterManager", "projectDataWriter",
                                                      "projectDataViewer"],
                                                     200, None, "provisioned")
        if not result:
            self.fail("Project roles test failed. Error: {}".format(error))

        self.log.info("Test Completed for List Health Reports Check Endpoint")

    def test_generate_health_report(self):
        self.log.info("Test started for Generating Health Reports Endpoint")

        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/health-advisor'.format(
            self.capellaAPIv2.internal_url, self.tenant_id, self.project_id, self.cluster_id)
        result, error = self.test_authentication(url, method="GET")
        if not result:
            self.fail("Auth test failed. Error: {}".format(error))

        # Test with different tenant ids
        test_method_args = {
            'project_id': self.project_id,
            'cluster_id': self.cluster_id
        }
        result, error = self.test_tenant_ids(self.capellaAPIv2.generate_health_report,
                                             test_method_args, 'tenant_id', 202)
        if not result:
            self.fail("Tenant ids test failed. Error: {}".format(error))

        self.sleep(310, "Waiting for 5 min before the next report generation")
        # Test with different project ids
        test_method_args = {
            'tenant_id': self.tenant_id,
            'cluster_id': self.cluster_id
        }
        result, error = self.test_project_ids(self.capellaAPIv2.generate_health_report,
                                              test_method_args, 'project_id', 202)
        if not result:
            self.fail("Project ids test failed. Error: {}".format(error))

        self.sleep(310, "Waiting for 5 min before the next report generation")
        # Test with org roles
        test_method_args = {
            'tenant_id': self.tenant_id,
            'project_id': self.project_id,
            'cluster_id': self.cluster_id
        }
        result, error = self.test_with_org_roles("generate_health_report",
                                                 test_method_args,
                                                 202, None, "provisioned")
        if not result:
            self.fail("Org roles test failed. Error: {}".format(error))

        # Test with project roles
        result, error = self.test_with_project_roles("generate_health_report",
                                                     test_method_args,
                                                     ["projectOwner", "projectClusterViewer",
                                                      "projectClusterManager",
                                                      "projectDataWriter",
                                                      "projectDataViewer"],
                                                     429, None, "provisioned")
        if not result:
            self.fail("Project roles test failed. Error: {}".format(error))

        self.log.info("Test Completed for Generating Health Reports Endpoint")

    def test_health_report_generation_progress(self):
        self.log.info("Test started for Health Report Generation Progress Endpoint")

        _ = self.generate_health_report()
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/health-advisor/progress'.format(
            self.capellaAPIv2.internal_url, self.tenant_id, self.project_id, self.cluster_id)
        result, error = self.test_authentication(url, method="GET")
        if not result:
            self.fail("Auth test failed. Error: {}".format(error))

        # Test with different tenant ids
        test_method_args = {
            'project_id': self.project_id,
            'cluster_id': self.cluster_id
        }
        result, error = self.test_tenant_ids(self.capellaAPIv2.get_health_report_generation_progress,
                                             test_method_args, 'tenant_id', 200)
        if not result:
            self.fail("Tenant ids test failed. Error: {}".format(error))

        # Test with different project ids
        test_method_args = {
            'tenant_id': self.tenant_id,
            'cluster_id': self.cluster_id
        }
        result, error = self.test_project_ids(self.capellaAPIv2.get_health_report_generation_progress,
                                              test_method_args, 'project_id', 200)
        if not result:
            self.fail("Project ids test failed. Error: {}".format(error))

        # Test with org roles
        test_method_args = {
            'tenant_id': self.tenant_id,
            'project_id': self.project_id,
            'cluster_id': self.cluster_id
        }
        result, error = self.test_with_org_roles("get_health_report_generation_progress",
                                                 test_method_args,
                                                 200, None, "provisioned")
        if not result:
            self.fail("Org roles test failed. Error: {}".format(error))

        # Test with project roles
        result, error = self.test_with_project_roles("get_health_report_generation_progress",
                                                     test_method_args,
                                                     ["projectOwner", "projectClusterViewer",
                                                      "projectClusterManager",
                                                      "projectDataWriter",
                                                      "projectDataViewer"],
                                                     200, None, "provisioned")
        if not result:
            self.fail("Project roles test failed. Error: {}".format(error))

        self.log.info("Test Completed for Health Report Generation Progress Endpoint")

    def test_get_health_advisor_settings(self):
        self.log.info("Test started for Get Health Advisor Settings Endpoint")

        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/health-advisor/settings'.format(
            self.capellaAPIv2.internal_url, self.tenant_id, self.project_id, self.cluster_id)
        result, error = self.test_authentication(url, method="GET")
        if not result:
            self.fail("Auth test failed. Error: {}".format(error))

        # Test with different tenant ids
        test_method_args = {
            'project_id': self.project_id,
            'cluster_id': self.cluster_id
        }
        result, error = self.test_tenant_ids(self.capellaAPIv2.get_health_advisor_settings,
                                             test_method_args, 'tenant_id', 200)
        if not result:
            self.fail("Tenant ids test failed. Error: {}".format(error))

        # Test with different project ids
        test_method_args = {
            'tenant_id': self.tenant_id,
            'cluster_id': self.cluster_id
        }
        result, error = self.test_project_ids(self.capellaAPIv2.get_health_advisor_settings,
                                              test_method_args, 'project_id', 200)
        if not result:
            self.fail("Project ids test failed. Error: {}".format(error))

        # Test with org roles
        test_method_args = {
            'tenant_id': self.tenant_id,
            'project_id': self.project_id,
            'cluster_id': self.cluster_id
        }
        result, error = self.test_with_org_roles("get_health_advisor_settings",
                                                 test_method_args,
                                                 200, None, "provisioned")
        if not result:
            self.fail("Org roles test failed. Error: {}".format(error))

        # Test with project roles
        result, error = self.test_with_project_roles("get_health_advisor_settings",
                                                     test_method_args,
                                                     ["projectOwner", "projectClusterViewer",
                                                      "projectClusterManager",
                                                      "projectDataWriter",
                                                      "projectDataViewer"],
                                                     200, None, "provisioned")
        if not result:
            self.fail("Project roles test failed. Error: {}".format(error))

        self.log.info("Test Completed for Get Health Advisor Settings Endpoint")

    def test_get_health_report_overall_stats(self):
        self.log.info("Test started for Get Health Report Overall Stats Endpoint")

        report_id = self.generate_health_report()
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/health-advisor/{}/stats'.format(
               self.capellaAPIv2.internal_url, self.tenant_id, self.project_id, self.cluster_id,
               report_id)
        result, error = self.test_authentication(url, method="GET")
        if not result:
            self.fail("Auth test failed. Error: {}".format(error))

        # Test with different tenant ids
        self.sleep(310, "Waiting for 5 minutes before generating next report")
        test_method_args = {
            'project_id': self.project_id,
            'cluster_id': self.cluster_id,
            'report_id': report_id
        }
        result, error = self.test_tenant_ids(self.capellaAPIv2.get_health_report_overall_stats,
                                             test_method_args, 'tenant_id', 200)
        if not result:
            self.fail("Tenant ids test failed. Error: {}".format(error))

        # Test with different project ids
        self.sleep(310, "Waiting for 5 minutes before generating next report")
        test_method_args = {
            'tenant_id': self.tenant_id,
            'cluster_id': self.cluster_id,
            'report_id': report_id
        }
        result, error = self.test_project_ids(self.capellaAPIv2.get_health_report_overall_stats,
                                              test_method_args, 'project_id', 200)
        if not result:
            self.fail("Project ids test failed. Error: {}".format(error))

        # Test with org roles
        self.sleep(310, "Waiting for 5 minutes before generating next report")
        test_method_args = {
            'tenant_id': self.tenant_id,
            'project_id': self.project_id,
            'cluster_id': self.cluster_id,
            'report_id': report_id
        }
        result, error = self.test_with_org_roles("get_health_report_overall_stats",
                                                 test_method_args,
                                                 200, None, "provisioned")
        if not result:
            self.fail("Org roles test failed. Error: {}".format(error))

        # Test with project roles
        self.sleep(310, "Waiting for 5 minutes before generating next report")
        result, error = self.test_with_project_roles("get_health_report_overall_stats",
                                                     test_method_args,
                                                     ["projectOwner", "projectClusterViewer",
                                                      "projectClusterManager",
                                                      "projectDataWriter",
                                                      "projectDataViewer"],
                                                     200, None, "provisioned")
        if not result:
            self.fail("Project roles test failed. Error: {}".format(error))

        self.log.info("Test Completed for Get Health Report Overall Stats Endpoint")

    def test_update_health_advisor_settings(self):
        self.log.info("Test started for Update Health Advisor Settings Endpoint")

        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/health-advisor/settings'.format(
            self.capellaAPIv2.internal_url, self.tenant_id, self.project_id, self.cluster_id)
        body = {"generateReport":True,"sendWeeklyReport":True,"emailSettings":{"type":"projectRole",
                "users":[],"roles":["projectOwner","projectClusterManager"]}}
        body = {"generateReport": True, "sendWeeklyReport": True,
         "emailSettings": {"type": "projectRole", "users": [],
                           "roles": ["projectOwner", "projectClusterManager"]}}
        result, error = self.test_authentication(url, method="PATCH", payload=body)
        if not result:
            self.fail("Auth test failed. Error: {}".format(error))

        # Test with different tenant ids
        test_method_args = {
            'project_id': self.project_id,
            'cluster_id': self.cluster_id,
            'payload' : json.dumps({
                "generateReport":True,
                "sendWeeklyReport":True,
                "emailSettings": {
                    "type":"projectRole",
                    "users":[],
                    "roles":["projectOwner","projectClusterManager"]
                }
            })
        }
        result, error = self.test_tenant_ids(self.capellaAPIv2.update_health_advisor_settings,
                                             test_method_args, 'tenant_id', 200)
        if not result:
            self.fail("Tenant ids test failed. Error: {}".format(error))

        # Test with different project ids
        test_method_args = {
            'tenant_id': self.tenant_id,
            'cluster_id': self.cluster_id,
            'payload' : json.dumps({
                "generateReport":True,
                "sendWeeklyReport":True,
                "emailSettings": {
                    "type":"projectRole",
                    "users":[],
                    "roles":["projectOwner","projectClusterManager"]
                }
            })
        }
        result, error = self.test_project_ids(self.capellaAPIv2.update_health_advisor_settings,
                                              test_method_args, 'project_id', 200)
        if not result:
            self.fail("Project ids test failed. Error: {}".format(error))

        # Test with org roles
        test_method_args = {
            'tenant_id': self.tenant_id,
            'project_id': self.project_id,
            'cluster_id': self.cluster_id,
            'payload' : json.dumps({
                "generateReport":True,
                "sendWeeklyReport":True,
                "emailSettings": {
                    "type":"projectRole",
                    "users":[],
                    "roles":["projectOwner","projectClusterManager"]
                }
            })
        }
        result, error = self.test_with_org_roles("update_health_advisor_settings",
                                                 test_method_args,
                                                 200, None, "provisioned")
        if not result:
            self.fail("Org roles test failed. Error: {}".format(error))

        # Test with project roles
        result, error = self.test_with_project_roles("update_health_advisor_settings",
                                                     test_method_args,
                                                     ["projectOwner", "projectClusterManager"],
                                                     200, None, "provisioned")
        if not result:
            self.fail("Project roles test failed. Error: {}".format(error))

        self.log.info("Test Completed for Update Health Advisor Settings Endpoint")

    def test_get_info_health_report(self):
        self.log.info("Test started for Get Info Health Report Endpoint")

        report_id = self.generate_health_report()
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/health-advisor/{}/info'.format(
               self.capellaAPIv2.internal_url, self.tenant_id, self.project_id, self.cluster_id,
               report_id)
        url = url + '?category=data'
        result, error = self.test_authentication(url, method="GET")
        if not result:
            self.fail("Auth test failed. Error: {}".format(error))

        # Test with different tenant ids
        test_method_args = {
            'project_id': self.project_id,
            'cluster_id': self.cluster_id,
            'report_id': report_id
        }
        result, error = self.test_tenant_ids(self.capellaAPIv2.get_info_health_report,
                                             test_method_args, 'tenant_id', 200)
        if not result:
            self.fail("Tenant ids test failed. Error: {}".format(error))

        # Test with different project ids
        test_method_args = {
            'tenant_id': self.tenant_id,
            'cluster_id': self.cluster_id,
            'report_id': report_id
        }
        result, error = self.test_project_ids(self.capellaAPIv2.get_info_health_report,
                                              test_method_args, 'project_id', 200)
        if not result:
            self.fail("Project ids test failed. Error: {}".format(error))

        # Test with org roles
        test_method_args = {
            'tenant_id': self.tenant_id,
            'project_id': self.project_id,
            'cluster_id': self.cluster_id,
            'report_id': report_id
        }
        result, error = self.test_with_org_roles("get_info_health_report",
                                                 test_method_args,
                                                 200, None, "provisioned")
        if not result:
            self.fail("Org roles test failed. Error: {}".format(error))

        # Test with project roles
        result, error = self.test_with_project_roles("get_info_health_report",
                                                     test_method_args,
                                                     ["projectOwner", "projectClusterViewer",
                                                      "projectClusterManager",
                                                      "projectDataWriter",
                                                      "projectDataViewer"],
                                                     200, None, "provisioned")
        if not result:
            self.fail("Project roles test failed. Error: {}".format(error))

        self.log.info("Test Completed for Get Info Health Report Endpoint")

    def test_get_pdf_health_report(self):
        self.log.info("Test started for Get PDF Health Report Endpoint")

        report_id = self.generate_health_report()
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/health-advisor/{}/pdf'.format(
            self.capellaAPIv2.internal_url, self.tenant_id, self.project_id, self.cluster_id,
            report_id)
        result, error = self.test_authentication(url, method="GET")
        if not result:
            self.fail("Auth test failed. Error: {}".format(error))

        # Test with different tenant ids
        test_method_args = {
            'project_id': self.project_id,
            'cluster_id': self.cluster_id,
            'report_id': report_id
        }
        result, error = self.test_tenant_ids(self.capellaAPIv2.get_pdf_health_report,
                                             test_method_args, 'tenant_id', 200)
        if not result:
            self.fail("Tenant ids test failed. Error: {}".format(error))

        # Test with different project ids
        test_method_args = {
            'tenant_id': self.tenant_id,
            'cluster_id': self.cluster_id,
            'report_id': report_id
        }
        result, error = self.test_project_ids(self.capellaAPIv2.get_pdf_health_report,
                                              test_method_args, 'project_id', 200)
        if not result:
            self.fail("Project ids test failed. Error: {}".format(error))

        # Test with org roles
        test_method_args = {
            'tenant_id': self.tenant_id,
            'project_id': self.project_id,
            'cluster_id': self.cluster_id,
            'report_id': report_id
        }
        result, error = self.test_with_org_roles("get_pdf_health_report",
                                                 test_method_args,
                                                 200, None, "provisioned")
        if not result:
            self.fail("Org roles test failed. Error: {}".format(error))

        # Test with project roles
        result, error = self.test_with_project_roles("get_pdf_health_report",
                                                     test_method_args,
                                                     ["projectOwner", "projectClusterViewer",
                                                      "projectClusterManager",
                                                      "projectDataWriter",
                                                      "projectDataViewer"],
                                                     200, None, "provisioned")
        if not result:
            self.fail("Project roles test failed. Error: {}".format(error))

        self.log.info("Test Completed for Get PDF Health Report Endpoint")
