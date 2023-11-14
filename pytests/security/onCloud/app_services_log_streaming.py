import time
import json
import base64
import random
import string
import requests
from pytests.basetestcase import BaseTestCase
from capellaAPI.capella.dedicated.CapellaAPI import CapellaAPI
from capellaAPI.capella.common.CapellaAPI import CommonCapellaAPI


class AppServicesTest(BaseTestCase):

    def setUp(self):
        BaseTestCase.setUp(self)
        self.url = self.input.capella.get("pod")
        self.user = self.input.capella.get("capella_user")
        self.passwd = self.input.capella.get("capella_pwd")
        self.tenant_id = self.input.capella.get("tenant_id")
        self.secret_key = self.input.capella.get("secret_key")
        self.access_key = self.input.capella.get("access_key")
        self.project_id = self.tenant.project_id
        self.cluster_id = self.cluster.id
        self.invalid_id = "00000000-0000-0000-0000-000000000000"
        self.test_users = {}
        roles = ["organizationOwner", "projectCreator", "organizationMember"]
        setup_capella_api = CapellaAPI("https://" + self.url, self.secret_key, self.access_key,
                                       self.user, self.passwd)

        num = 1
        for role in roles:
            usrname = self.user.split('@')
            username = usrname[0] + "+" + str(num) + "@" + usrname[1]
            name = "Test_User_"  + str(num)
            create_user_resp = setup_capella_api.create_user(self.tenant_id,
                                                             name,
                                                             username,
                                                             self.passwd,
                                                             [role])
            if create_user_resp.status_code == 200:
                self.test_users["User" + str(num)] = {
                    "name": create_user_resp.json()["data"]["name"],
                    "mailid": create_user_resp.json()["data"]["email"],
                    "role": role,
                    "password": self.passwd,
                    "userid": create_user_resp.json()["data"]["id"]
                }

            elif create_user_resp.status_code == 422:
                msg = "is already in use. Please sign-in."
                if msg in create_user_resp.json()["message"]:
                    num = num + 1
                    continue
                else:
                    self.fail("Not able to create user. Reason -".format(create_user_resp.content))

            else:
                self.fail("Not able to create user. Reason -".format(create_user_resp.content))

            num = num + 1

    def tearDown(self):
        super(AppServicesTest, self).tearDown()

    def get_log_streaming_current_status(self, tenant_id, project_id, cluster_id, sgw_id,
                                         header=None):
        capella_api_role = CapellaAPI("https://" + self.url, self.secret_key, self.access_key,
                                 self.user, self.passwd)
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/backends/{}/logstreaming'.format(
            capella_api_role.internal_url, tenant_id, project_id, cluster_id, sgw_id)
        resp = capella_api_role._urllib_request(url, method="GET", params='', headers=header)
        return resp

    def enable_log_streaming(self, tenant_id, project_id, cluster_id, sgw_id, header=None):
        capella_api_role = CapellaAPI("https://" + self.url, self.secret_key, self.access_key,
                                      self.user, self.passwd)
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/backends/{}/logstreaming/enable'.\
            format(capella_api_role.internal_url, tenant_id, project_id, cluster_id, sgw_id)
        resp = capella_api_role._urllib_request(url, method="POST", params='', headers=header)
        return resp

    def pause_log_streaming(self, tenant_id, project_id, cluster_id, sgw_id, header=None):
        capella_api_role = CapellaAPI("https://" + self.url, self.secret_key, self.access_key,
                                      self.user, self.passwd)
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/backends/{}/logstreaming/pause'.\
            format(capella_api_role.internal_url, tenant_id, project_id, cluster_id, sgw_id)
        resp = capella_api_role._urllib_request(url, method="POST", params='', headers=header)
        return resp

    def disable_log_streaming(self, tenant_id, project_id, cluster_id, sgw_id, header=None):
        capella_api_role = CapellaAPI("https://" + self.url, self.secret_key, self.access_key,
                                      self.user, self.passwd)
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/backends/{}/logstreaming/disable'.\
            format(capella_api_role.internal_url, tenant_id, project_id, cluster_id, sgw_id)
        resp = capella_api_role._urllib_request(url, method="POST", params='', headers=header)
        return resp

    def get_all_valid_collector_config(self, tenant_id, project_id, cluster_id, sgw_id,
                                       header=None):
        capella_api_role = CapellaAPI("https://" + self.url, self.secret_key, self.access_key,
                                      self.user, self.passwd)
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/backends/{}/' \
              'logstreaming/collector-options'.\
            format(capella_api_role.internal_url, tenant_id, project_id, cluster_id, sgw_id)
        resp = capella_api_role._urllib_request(url, method="GET", params='', headers=header)
        return resp

    def get_all_logging_options(self, tenant_id, project_id, cluster_id, sgw_id, header=None):
        capella_api_role = CapellaAPI("https://" + self.url, self.secret_key, self.access_key,
                                      self.user, self.passwd)
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/backends/{}/logging-options'.\
            format(capella_api_role.internal_url, tenant_id, project_id, cluster_id, sgw_id)
        resp = capella_api_role._urllib_request(url, method="GET", params='', headers=header)
        return resp

    def get_current_config_logging_options(self, tenant_id, project_id, cluster_id, sgw_id,
                                    app_endpoint_id, header=None):
        capella_api_role = CapellaAPI("https://" + self.url, self.secret_key, self.access_key,
                                      self.user, self.passwd)
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/backends/{}/databases/' \
              '{}/logging'.\
            format(capella_api_role.internal_url, tenant_id, project_id, cluster_id, sgw_id,
                   app_endpoint_id)
        resp = capella_api_role._urllib_request(url, method="GET", params='', headers=header)
        return resp

    def put_current_config_logging_options(self, tenant_id, project_id, cluster_id, sgw_id,
                                    app_endpoint_id, params, header=None):
        capella_api_role = CapellaAPI("https://" + self.url, self.secret_key, self.access_key,
                                      self.user, self.passwd)
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/backends/{}/databases/' \
              '{}/logging'.\
            format(capella_api_role.internal_url, tenant_id, project_id, cluster_id, sgw_id,
                   app_endpoint_id)
        resp = capella_api_role._urllib_request(url, method="POST", params=params, headers=header)
        return resp

    def test_get_log_streaming_current_status(self):
        self.log.info("Verifying status code for getting the log streaming status")

        capella_api = CapellaAPI("https://" + self.url, self.secret_key, self.access_key,
                                 self.user, self.passwd)

        app_service_config =  {
            "clusterId": self.cluster_id,
            "compute": {"type": "c5.2xlarge"},
            "desired_capacity": 2,
            "name": "test-app-service"
        }

        resp = capella_api.create_sgw_backend(self.tenant_id, app_service_config)
        self.assertEqual(202, resp.status_code,
                         msg="FAIL: Outcome: {}, Expected: {}".format(resp.status_code, 202))

        sgw_id = resp.json()["id"]
        status = "deploying"
        while status != "healthy":
            resp = capella_api.get_sgw_backend(self.tenant_id, self.project_id, self.cluster_id,
                                               sgw_id)
            status = resp.json()["data"]["status"]["state"]
            self.sleep(15, "Waiting for App Services to be in healthy state")

        # Trying out with different headers to bypass authentication
        self.log.info("Calling API with various headers to bypass authentication")
        headers = {
            "invalid_header": 'abcdefgh',
            "empty_header": ''
        }

        for header in headers:
            cbc_api_request_header = {
                'Authorization': 'Bearer %s' % headers[header],
                'Length': ''
            }

            resp = self.get_log_streaming_current_status(self.tenant_id, self.project_id,
                                                         self.cluster_id, sgw_id,
                                                         header=cbc_api_request_header)
            self.assertEqual(resp.status_code, 401,
                             msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code, 401))

        # Verifying the get log streaming status for different tenants
        self.log.info("Verifying the log streaming status for different tenants")
        tenant_ids = {
            'valid_tenant_id': self.tenant_id,
            # 'invalid_tenant_id': self.invalid_id
        }
        for tenant_id in tenant_ids:
            resp = capella_api.get_log_streaming_current_status(tenant_ids[tenant_id],
                                                                self.project_id,
                                                                self.cluster_id,
                                                                sgw_id)

            if tenant_id == "valid_tenant_id":
                self.assertEqual(resp.status_code, 200,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,200))

            else:
                self.assertEqual(resp.status_code, 404,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              404))

        # Verifying the get log streaming status for different projects
        self.log.info("Verifying the log streaming status for different projects")
        project_ids = {
            'valid_project_id': self.project_id,
            'invalid_project_id': self.invalid_id
        }
        for project_id in project_ids:
            resp = capella_api.get_log_streaming_current_status(self.tenant_id,
                                                                project_ids[project_id],
                                                                self.cluster_id,
                                                                sgw_id)

            if project_id == "valid_project_id":
                self.assertEqual(resp.status_code, 200,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              200))

            else:
                self.assertEqual(resp.status_code, 403,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              403))

        # Verifying the get log streaming status for different clusters
        self.log.info("Verifying the get log streaming status for different clusters")
        cluster_ids = {
            'valid_cluster_id': self.cluster_id,
            'invalid_cluster_id': self.invalid_id
        }
        for cluster_id in cluster_ids:
            resp = capella_api.get_log_streaming_current_status(self.tenant_id,
                                                                self.project_id,
                                                                cluster_ids[cluster_id],
                                                                sgw_id)

            if cluster_id == "valid_cluster_id":
                self.assertEqual(resp.status_code, 200,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              200))

            else:
                self.assertEqual(resp.status_code, 403,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              403))

        # Verifying the get log streaming status for organization RBAC roles
        self.log.info("Verifying the get log streaming status for organization roles")
        for user in self.test_users:
            self.log.info("Checking for role - {}".format(self.test_users[user]["role"]))

            capellaAPIRole = CapellaAPI("https://" + self.url, '', '', self.test_users[
                user]["mailid"], self.test_users[user]["password"])
            role_response = capellaAPIRole.get_log_streaming_current_status(self.tenant_id,
                                                                                 self.project_id,
                                                                                 self.cluster_id,
                                                                                 sgw_id)

            if self.test_users[user]["role"] == "organizationOwner":
                self.assertEqual(role_response.status_code, 200,
                                 msg='FAIL: Outcome:{}, Expected:{}'.format(
                                     role_response.status_code, 200))

            else:
                self.assertEqual(role_response.status_code, 403,
                                 msg='FAIL: Outcome:{}, Expected:{}'.format(
                                     role_response.status_code, 403))

        # Testing for Project Level RBAC roles
        self.log.info("Verifying endpoint for different roles under project - RBAC")
        project_roles = ["projectDataViewer", "projectOwner", "projectClusterManager",
                         "projectClusterViewer", "projectDataWriter"]
        user = self.test_users["User3"]
        for role in project_roles:
            self.log.info("Adding user to project with role as {}".format(role))
            config = {
                "resourceId": self.project_id,
                "resourceType": "project",
                "roles": [role], "users": [user["userid"]]
            }
            capella_api.add_user_to_project(self.tenant_id, json.dumps(config))

            capellaAPIRole = CapellaAPI("https://" + self.url, '', '', user["mailid"],
                                        user["password"])
            role_response = capellaAPIRole.get_log_streaming_current_status(self.tenant_id,
                                                                            self.project_id,
                                                                            self.cluster_id,
                                                                            sgw_id)

            self.assertEqual(role_response.status_code, 200,
                             msg='FAIL, Outcome:{}, Expected:{}'
                             .format(role_response.status_code, 200))

            self.log.info("Removing user from project")
            capella_api.remove_user_from_project(self.tenant_id, user["userid"],
                                                        self.project_id)

    def test_pause_log_streaming(self):
        self.log.info("Verifying status code for pausing log streaming")
        capella_api = CapellaAPI("https://" + self.url, self.secret_key, self.access_key, self.user,
                                 self.passwd)

        app_service_config = {
            "clusterId": self.cluster_id,
            "compute": {"type": "c5.2xlarge"},
            "desired_capacity": 2,
            "name": "test-app-service"
        }

        resp = capella_api.create_sgw_backend(self.tenant_id, app_service_config)
        self.assertEqual(202, resp.status_code,
                         msg="FAIL: Outcome: {}, Expected: {}".format(resp.status_code, 202))

        sgw_id = resp.json()["id"]
        status = "deploying"
        while status != "healthy":
            resp = capella_api.get_sgw_backend(self.tenant_id, self.project_id, self.cluster_id,
                                               sgw_id)
            status = resp.json()["data"]["status"]["state"]
            self.sleep(15, "Waiting for App Services to be in healthy state")

        config = {
            "output_type": "datadog",
            "url": "https://http-intake.logs.datadoghq.eu",
            "api_key": "12345"
        }
        resp = capella_api.create_upsert_log_streaming(self.tenant_id,
                                                       self.project_id,
                                                       self.cluster_id,
                                                       sgw_id, config)
        self.assertEqual(200, resp.status_code,
                         msg="FAIL, Outcome: {0}, Expected: {1}"
                                 .format(resp.status_code, 200))

        # Enabling the log streaming again because it is paused
        resp = capella_api.enable_log_streaming(self.tenant_id,
                                                self.project_id,
                                                self.cluster_id,
                                                sgw_id)
        self.assertEqual(resp.status_code, 200,
                         msg='FAIL: Outcome: {}, Expected: {}'.format(
                             resp.status_code, 200))
        status = "enabling"
        while status != "enabled":
            resp = capella_api.get_log_streaming_current_status(self.tenant_id,
                                                                self.project_id,
                                                                self.cluster_id,
                                                                sgw_id)
            status = resp.json()["data"]["status"]
            self.sleep(10, "Waiting for log streaming to be in healthy state")

        # Trying out with different headers to bypass authentication
        self.log.info("Calling API with various headers to bypass authentication")
        headers = {
            "invalid_header": 'abcdefgh',
            "empty_header": ''
        }

        for header in headers:
            cbc_api_request_header = {
                'Authorization': 'Bearer %s' % headers[header],
                'Length': ''
            }

            resp = self.pause_log_streaming(self.tenant_id, self.project_id,
                                             self.cluster_id, sgw_id,
                                             header=cbc_api_request_header)
            self.assertEqual(resp.status_code, 401,
                             msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code, 401))

        # Verifying the pause log streaming status for different tenants
        self.log.info("Verifying the pause log streaming for different tenants")
        tenant_ids = {
            'valid_tenant_id': self.tenant_id,
            # 'invalid_tenant_id': self.invalid_id
        }
        for tenant_id in tenant_ids:
            resp = capella_api.pause_log_streaming(tenant_ids[tenant_id],
                                                   self.project_id,
                                                   self.cluster_id,
                                                   sgw_id)

            if tenant_id == "valid_tenant_id":
                self.assertEqual(resp.status_code, 200,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              200))

            else:
                self.assertEqual(resp.status_code, 404,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              404))

        status = "pausing"
        while status != "paused":
            resp = capella_api.get_log_streaming_current_status(self.tenant_id,
                                                                self.project_id,
                                                                self.cluster_id,
                                                                sgw_id)
            status = resp.json()["data"]["status"]
            self.sleep(10, "Waiting for log streaming to be in paused state")
        # Enabling the log streaming again because it is paused
        resp = capella_api.enable_log_streaming(self.tenant_id,
                                                self.project_id,
                                                self.cluster_id,
                                                sgw_id)
        self.assertEqual(resp.status_code, 200,
                         msg='FAIL: Outcome: {}, Expected: {}'.format(
                             resp.status_code, 200))
        status = "enabling"
        while status != "enabled":
            resp = capella_api.get_log_streaming_current_status(self.tenant_id,
                                                                self.project_id,
                                                                self.cluster_id,
                                                                sgw_id)
            status = resp.json()["data"]["status"]
            self.sleep(10, "Waiting for log streaming to be in healthy state")

        # Verifying the get log streaming status for different projects
        self.log.info("Verifying the log streaming status for different projects")
        project_ids = {
            'valid_project_id': self.project_id,
            'invalid_project_id': self.invalid_id
        }
        for project_id in project_ids:
            resp = capella_api.pause_log_streaming(self.tenant_id,
                                                   project_ids[project_id],
                                                   self.cluster_id,
                                                   sgw_id)

            if project_id == "valid_project_id":
                self.assertEqual(resp.status_code, 200,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(
                                     resp.status_code,
                                     200))

            else:
                self.assertEqual(resp.status_code, 403,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(
                                     resp.status_code,
                                     403))

        status = "pausing"
        while status != "paused":
            resp = capella_api.get_log_streaming_current_status(self.tenant_id,
                                                                self.project_id,
                                                                self.cluster_id,
                                                                sgw_id)
            status = resp.json()["data"]["status"]
            self.sleep(10, "Waiting for log streaming to be in paused state")
        # Enabling the log streaming again because it is paused
        resp = capella_api.enable_log_streaming(self.tenant_id,
                                                self.project_id,
                                                self.cluster_id,
                                                sgw_id)
        self.assertEqual(resp.status_code, 200,
                         msg='FAIL: Outcome: {}, Expected: {}'.format(
                             resp.status_code, 200))
        status = "enabling"
        while status != "enabled":
            resp = capella_api.get_log_streaming_current_status(self.tenant_id,
                                                                self.project_id,
                                                                self.cluster_id,
                                                                sgw_id)
            status = resp.json()["data"]["status"]
            self.sleep(10, "Waiting for log streaming to be in healthy state")

        # Verifying the get log streaming status for different clusters
        self.log.info("Verifying the get log streaming status for different clusters")
        cluster_ids = {
            'valid_cluster_id': self.cluster_id,
            'invalid_cluster_id': self.invalid_id
        }
        for cluster_id in cluster_ids:
            resp = capella_api.pause_log_streaming(self.tenant_id,
                                                   self.project_id,
                                                   cluster_ids[cluster_id],
                                                   sgw_id)

            if cluster_id == "valid_cluster_id":
                self.assertEqual(resp.status_code, 200,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(
                                     resp.status_code, 200))

            else:
                self.assertEqual(resp.status_code, 403,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(
                                     resp.status_code, 403))

        status = "pausing"
        while status != "paused":
            resp = capella_api.get_log_streaming_current_status(self.tenant_id,
                                                                self.project_id,
                                                                self.cluster_id,
                                                                sgw_id)
            status = resp.json()["data"]["status"]
            self.sleep(10, "Waiting for log streaming to be in paused state")
        # Enabling the log streaming again because it is paused
        resp = capella_api.enable_log_streaming(self.tenant_id,
                                                self.project_id,
                                                self.cluster_id,
                                                sgw_id)
        self.assertEqual(resp.status_code, 200,
                         msg='FAIL: Outcome: {}, Expected: {}'.format(
                             resp.status_code, 200))
        status = "enabling"
        while status != "enabled":
            resp = capella_api.get_log_streaming_current_status(self.tenant_id,
                                                                self.project_id,
                                                                self.cluster_id,
                                                                sgw_id)
            status = resp.json()["data"]["status"]
            self.sleep(10, "Waiting for log streaming to be in healthy state")

        # Verifying the pause log streaming for organization RBAC roles
        self.log.info("Verifying the get log streaming status for organization roles")
        for user in self.test_users:
            self.log.info("Checking for role - {}".format(self.test_users[user]["role"]))

            capellaAPIRole = CapellaAPI("https://" + self.url, '', '', self.test_users[
                user]["mailid"], self.test_users[user]["password"])
            role_response = capellaAPIRole.pause_log_streaming(self.tenant_id,
                                                               self.project_id,
                                                               self.cluster_id,
                                                               sgw_id)

            if self.test_users[user]["role"] == "organizationOwner":
                self.assertEqual(role_response.status_code, 200,
                                 msg='FAIL: Outcome:{}, Expected:{}'.format(
                                     role_response.status_code, 200))

            else:
                self.assertEqual(role_response.status_code, 403,
                                 msg='FAIL: Outcome:{}, Expected:{}'.format(
                                     role_response.status_code, 403))

        status = "pausing"
        while status != "paused":
            resp = capella_api.get_log_streaming_current_status(self.tenant_id,
                                                                self.project_id,
                                                                self.cluster_id,
                                                                sgw_id)
            status = resp.json()["data"]["status"]
            self.sleep(10, "Waiting for log streaming to be in paused state")
        # Enabling the log streaming again because it is paused
        resp = capella_api.enable_log_streaming(self.tenant_id,
                                                self.project_id,
                                                self.cluster_id,
                                                sgw_id)
        self.assertEqual(resp.status_code, 200,
                         msg='FAIL: Outcome: {}, Expected: {}'.format(
                             resp.status_code, 200))
        status = "enabling"
        while status != "enabled":
            resp = capella_api.get_log_streaming_current_status(self.tenant_id,
                                                                self.project_id,
                                                                self.cluster_id,
                                                                sgw_id)
            status = resp.json()["data"]["status"]
            self.sleep(10, "Waiting for log streaming to be in healthy state")

        # Testing for Project Level RBAC roles
        self.log.info("Verifying endpoint for different roles under project - RBAC")
        project_roles = ["projectDataViewer", "projectOwner", "projectClusterManager",
                         "projectClusterViewer", "projectDataWriter"]
        user = self.test_users["User3"]
        for role in project_roles:
            self.log.info("Adding user to project with role as {}".format(role))
            config = {
                "resourceId": self.project_id,
                "resourceType": "project",
                "roles": [role], "users": [user["userid"]]
            }
            capella_api.add_user_to_project(self.tenant_id, json.dumps(config))

            capellaAPIRole = CapellaAPI("https://" + self.url, '', '', user["mailid"],
                                        user["password"])
            role_response = capellaAPIRole.pause_log_streaming(self.tenant_id,
                                                               self.project_id,
                                                               self.cluster_id,
                                                               sgw_id)
            if role in ["projectOwner", "projectClusterManager"]:
                self.assertEqual(role_response.status_code, 200,
                                 msg='FAIL, Outcome:{}, Expected:{}'
                                 .format(role_response.status_code, 200))

                status = "pausing"
                while status != "paused":
                    resp = capella_api.get_log_streaming_current_status(self.tenant_id,
                                                                        self.project_id,
                                                                        self.cluster_id,
                                                                        sgw_id)
                    status = resp.json()["data"]["status"]
                    self.sleep(10, "Waiting for log streaming to be in paused state")
                # Enabling the log streaming again because it is paused
                resp = capella_api.enable_log_streaming(self.tenant_id,
                                                        self.project_id,
                                                        self.cluster_id,
                                                        sgw_id)
                self.assertEqual(resp.status_code, 200,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(
                                     resp.status_code, 200))
                status = "enabling"
                while status != "enabled":
                    resp = capella_api.get_log_streaming_current_status(self.tenant_id,
                                                                        self.project_id,
                                                                        self.cluster_id,
                                                                        sgw_id)
                    status = resp.json()["data"]["status"]
                    self.sleep(10, "Waiting for log streaming to be in enabled state")

            else:
                self.assertEqual(role_response.status_code, 403,
                                 msg='FAIL, Outcome:{}, Expected:{}'
                                 .format(role_response.status_code, 403))

            self.log.info("Removing user from project")
            capella_api.remove_user_from_project(self.tenant_id, user["userid"],
                                                 self.project_id)

    def test_enable_log_streaming(self):
        self.log.info("Verifying status code for enabling log streaming")
        capella_api = CapellaAPI("https://" + self.url, self.secret_key, self.access_key, self.user,
                                 self.passwd)

        app_service_config = {
            "clusterId": self.cluster_id,
            "compute": {"type": "c5.2xlarge"},
            "desired_capacity": 2,
            "name": "test-app-service"
        }

        resp = capella_api.create_sgw_backend(self.tenant_id, app_service_config)
        self.assertEqual(202, resp.status_code,
                         msg="FAIL: Outcome: {}, Expected: {}".format(resp.status_code, 202))

        sgw_id = resp.json()["id"]
        status = "deploying"
        while status != "healthy":
            resp = capella_api.get_sgw_backend(self.tenant_id, self.project_id, self.cluster_id,
                                               sgw_id)
            status = resp.json()["data"]["status"]["state"]
            self.sleep(15, "Waiting for App Services to be in healthy state")

        config = {
            "output_type": "datadog",
            "url": "https://http-intake.logs.datadoghq.eu",
            "api_key": "12345"
        }
        resp = capella_api.create_upsert_log_streaming(self.tenant_id,
                                                       self.project_id,
                                                       self.cluster_id,
                                                       sgw_id, config)
        self.assertEqual(200, resp.status_code,
                         msg="FAIL, Outcome: {0}, Expected: {1}"
                                 .format(resp.status_code, 200))

        # Pausing the log streaming again because it is paused
        resp = capella_api.pause_log_streaming(self.tenant_id,
                                               self.project_id,
                                               self.cluster_id,
                                               sgw_id)
        self.assertEqual(resp.status_code, 200,
                         msg='FAIL: Outcome: {}, Expected: {}'.format(
                             resp.status_code, 200))
        status = "pausing"
        while status != "paused":
            resp = capella_api.get_log_streaming_current_status(self.tenant_id,
                                                                self.project_id,
                                                                self.cluster_id,
                                                                sgw_id)
            status = resp.json()["data"]["status"]
            self.sleep(10, "Waiting for log streaming to be in paused state")

        # Trying out with different headers to bypass authentication
        self.log.info("Calling API with various headers to bypass authentication")
        headers = {
            "invalid_header": 'abcdefgh',
            "empty_header": ''
        }

        for header in headers:
            cbc_api_request_header = {
                'Authorization': 'Bearer %s' % headers[header],
                'Length': ''
            }

            resp = self.enable_log_streaming(self.tenant_id, self.project_id,
                                             self.cluster_id, sgw_id,
                                             header=cbc_api_request_header)
            self.assertEqual(resp.status_code, 401,
                             msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code, 401))

        # Verifying the pause log streaming status for different tenants
        self.log.info("Verifying the enable log streaming for different tenants")
        tenant_ids = {
            'valid_tenant_id': self.tenant_id,
            # 'invalid_tenant_id': self.invalid_id
        }
        for tenant_id in tenant_ids:
            resp = capella_api.enable_log_streaming(tenant_ids[tenant_id],
                                                   self.project_id,
                                                   self.cluster_id,
                                                   sgw_id)

            if tenant_id == "valid_tenant_id":
                self.assertEqual(resp.status_code, 200,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              200))

            else:
                self.assertEqual(resp.status_code, 404,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              404))

        status = "enabling"
        while status != "enabled":
            self.sleep(10, "Waiting for log streaming to be in enabled state")
            resp = capella_api.get_log_streaming_current_status(self.tenant_id,
                                                                self.project_id,
                                                                self.cluster_id,
                                                                sgw_id)
            status = resp.json()["data"]["status"]

        # Pausing the log streaming again because it is enabled
        resp = capella_api.pause_log_streaming(self.tenant_id,
                                               self.project_id,
                                               self.cluster_id,
                                               sgw_id)
        self.assertEqual(resp.status_code, 200,
                         msg='FAIL: Outcome: {}, Expected: {}'.format(
                             resp.status_code, 200))
        status = "pausing"
        while status != "paused":
            resp = capella_api.get_log_streaming_current_status(self.tenant_id,
                                                                self.project_id,
                                                                self.cluster_id,
                                                                sgw_id)
            status = resp.json()["data"]["status"]
            self.sleep(10, "Waiting for log streaming to be in paused state")

        # Verifying the get log streaming status for different projects
        self.log.info("Verifying the enable log streaming status for different projects")
        project_ids = {
            'valid_project_id': self.project_id,
            'invalid_project_id': self.invalid_id
        }
        for project_id in project_ids:
            resp = capella_api.enable_log_streaming(self.tenant_id,
                                                   project_ids[project_id],
                                                   self.cluster_id,
                                                   sgw_id)

            if project_id == "valid_project_id":
                self.assertEqual(resp.status_code, 200,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(
                                     resp.status_code,
                                     200))

            else:
                self.assertEqual(resp.status_code, 403,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(
                                     resp.status_code,
                                     403))

        status = "enabling"
        while status != "enabled":
            resp = capella_api.get_log_streaming_current_status(self.tenant_id,
                                                                self.project_id,
                                                                self.cluster_id,
                                                                sgw_id)
            status = resp.json()["data"]["status"]
            self.sleep(10, "Waiting for log streaming to be in paused state")

        # Enabling the log streaming again because it is paused
        resp = capella_api.pause_log_streaming(self.tenant_id,
                                               self.project_id,
                                               self.cluster_id,
                                               sgw_id)
        self.assertEqual(resp.status_code, 200,
                         msg='FAIL: Outcome: {}, Expected: {}'.format(
                             resp.status_code, 200))
        status = "pausing"
        while status != "paused":
            resp = capella_api.get_log_streaming_current_status(self.tenant_id,
                                                                self.project_id,
                                                                self.cluster_id,
                                                                sgw_id)
            status = resp.json()["data"]["status"]
            self.sleep(10, "Waiting for log streaming to be in healthy state")

        # Verifying the get log streaming status for different clusters
        self.log.info("Verifying the enable log streaming status for different clusters")
        cluster_ids = {
            'valid_cluster_id': self.cluster_id,
            'invalid_cluster_id': self.invalid_id
        }
        for cluster_id in cluster_ids:
            resp = capella_api.enable_log_streaming(self.tenant_id,
                                                   self.project_id,
                                                   cluster_ids[cluster_id],
                                                   sgw_id)

            if cluster_id == "valid_cluster_id":
                self.assertEqual(resp.status_code, 200,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(
                                     resp.status_code, 200))

            else:
                self.assertEqual(resp.status_code, 403,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(
                                     resp.status_code, 403))

        status = "enabling"
        while status != "enabled":
            resp = capella_api.get_log_streaming_current_status(self.tenant_id,
                                                                self.project_id,
                                                                self.cluster_id,
                                                                sgw_id)
            status = resp.json()["data"]["status"]
            self.sleep(10, "Waiting for log streaming to be in paused state")
        # Enabling the log streaming again because it is paused
        resp = capella_api.pause_log_streaming(self.tenant_id,
                                               self.project_id,
                                               self.cluster_id,
                                               sgw_id)
        self.assertEqual(resp.status_code, 200,
                         msg='FAIL: Outcome: {}, Expected: {}'.format(
                             resp.status_code, 200))
        status = "pausing"
        while status != "paused":
            resp = capella_api.get_log_streaming_current_status(self.tenant_id,
                                                                self.project_id,
                                                                self.cluster_id,
                                                                sgw_id)
            status = resp.json()["data"]["status"]
            self.sleep(10, "Waiting for log streaming to be in healthy state")

        # Verifying the pause log streaming for organization RBAC roles
        self.log.info("Verifying the enable log streaming status for organization roles")
        for user in self.test_users:
            self.log.info("Checking for role - {}".format(self.test_users[user]["role"]))

            capellaAPIRole = CapellaAPI("https://" + self.url, '', '', self.test_users[
                user]["mailid"], self.test_users[user]["password"])
            role_response = capellaAPIRole.enable_log_streaming(self.tenant_id,
                                                                self.project_id,
                                                                self.cluster_id,
                                                                sgw_id)

            if self.test_users[user]["role"] == "organizationOwner":
                self.assertEqual(role_response.status_code, 200,
                                 msg='FAIL: Outcome:{}, Expected:{}'.format(
                                     role_response.status_code, 200))

            else:
                self.assertEqual(role_response.status_code, 403,
                                 msg='FAIL: Outcome:{}, Expected:{}'.format(
                                     role_response.status_code, 403))

        status = "enabling"
        while status != "enabled":
            resp = capella_api.get_log_streaming_current_status(self.tenant_id,
                                                                self.project_id,
                                                                self.cluster_id,
                                                                sgw_id)
            status = resp.json()["data"]["status"]
            self.sleep(10, "Waiting for log streaming to be in paused state")
        # Enabling the log streaming again because it is paused
        resp = capella_api.pause_log_streaming(self.tenant_id,
                                               self.project_id,
                                               self.cluster_id,
                                               sgw_id)
        self.assertEqual(resp.status_code, 200,
                         msg='FAIL: Outcome: {}, Expected: {}'.format(
                             resp.status_code, 200))
        status = "pausing"
        while status != "paused":
            resp = capella_api.get_log_streaming_current_status(self.tenant_id,
                                                                self.project_id,
                                                                self.cluster_id,
                                                                sgw_id)
            status = resp.json()["data"]["status"]
            self.sleep(10, "Waiting for log streaming to be in healthy state")

        # Testing for Project Level RBAC roles
        self.log.info("Verifying endpoint for different roles under project - RBAC")
        project_roles = ["projectDataViewer", "projectOwner", "projectClusterManager",
                         "projectClusterViewer", "projectDataWriter"]
        user = self.test_users["User3"]
        for role in project_roles:
            self.log.info("Adding user to project with role as {}".format(role))
            config = {
                "resourceId": self.project_id,
                "resourceType": "project",
                "roles": [role], "users": [user["userid"]]
            }
            capella_api.add_user_to_project(self.tenant_id, json.dumps(config))

            capellaAPIRole = CapellaAPI("https://" + self.url, '', '', user["mailid"],
                                        user["password"])
            role_response = capellaAPIRole.enable_log_streaming(self.tenant_id,
                                                               self.project_id,
                                                               self.cluster_id,
                                                               sgw_id)

            if role in ["projectOwner", "projectClusterManager"]:
                self.assertEqual(role_response.status_code, 200,
                                 msg='FAIL, Outcome:{}, Expected:{}'
                                 .format(role_response.status_code, 200))

                status = "enabling"
                while status != "enabled":
                    resp = capella_api.get_log_streaming_current_status(self.tenant_id,
                                                                        self.project_id,
                                                                        self.cluster_id,
                                                                        sgw_id)
                    status = resp.json()["data"]["status"]
                    self.sleep(10, "Waiting for log streaming to be in paused state")
                # Enabling the log streaming again because it is paused
                resp = capella_api.pause_log_streaming(self.tenant_id,
                                                        self.project_id,
                                                        self.cluster_id,
                                                        sgw_id)
                self.assertEqual(resp.status_code, 200,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(
                                     resp.status_code, 200))
                status = "pausing"
                while status != "paused":
                    resp = capella_api.get_log_streaming_current_status(self.tenant_id,
                                                                        self.project_id,
                                                                        self.cluster_id,
                                                                        sgw_id)
                    status = resp.json()["data"]["status"]
                    self.sleep(10, "Waiting for log streaming to be in enabled state")

            else:
                self.assertEqual(role_response.status_code, 403,
                                 msg='FAIL, Outcome:{}, Expected:{}'
                                 .format(role_response.status_code, 403))

            self.log.info("Removing user from project")
            capella_api.remove_user_from_project(self.tenant_id, user["userid"],
                                                 self.project_id)

    def test_disable_log_streaming(self):
        self.log.info("Verifying status code for disabling log streaming")
        capella_api = CapellaAPI("https://" + self.url, self.secret_key, self.access_key, self.user,
                                 self.passwd)

        app_service_config = {
            "clusterId": self.cluster_id,
            "compute": {"type": "c5.2xlarge"},
            "desired_capacity": 2,
            "name": "test-app-service"
        }

        resp = capella_api.create_sgw_backend(self.tenant_id, app_service_config)
        self.assertEqual(202, resp.status_code,
                         msg="FAIL: Outcome: {}, Expected: {}".format(resp.status_code, 202))

        sgw_id = resp.json()["id"]
        status = "deploying"
        while status != "healthy":
            resp = capella_api.get_sgw_backend(self.tenant_id, self.project_id, self.cluster_id,
                                               sgw_id)
            status = resp.json()["data"]["status"]["state"]
            self.sleep(15, "Waiting for App Services to be in healthy state")

        config = {
            "output_type": "datadog",
            "url": "https://http-intake.logs.datadoghq.eu",
            "api_key": "12345"
        }
        resp = capella_api.create_upsert_log_streaming(self.tenant_id,
                                                       self.project_id,
                                                       self.cluster_id,
                                                       sgw_id, config)
        self.assertEqual(200, resp.status_code,
                         msg="FAIL, Outcome: {0}, Expected: {1}"
                                 .format(resp.status_code, 200))

        status = "enabling"
        while status != "enabled":
            resp = capella_api.get_log_streaming_current_status(self.tenant_id,
                                                                self.project_id,
                                                                self.cluster_id,
                                                                sgw_id)
            status = resp.json()["data"]["status"]
            self.sleep(10, "Waiting for log streaming to be in paused state")

        # Trying out with different headers to bypass authentication
        self.log.info("Calling API with various headers to bypass authentication")
        headers = {
            "invalid_header": 'abcdefgh',
            "empty_header": ''
        }

        for header in headers:
            cbc_api_request_header = {
                'Authorization': 'Bearer %s' % headers[header],
                'Length': ''
            }

            resp = self.disable_log_streaming(self.tenant_id, self.project_id,
                                             self.cluster_id, sgw_id,
                                             header=cbc_api_request_header)
            self.assertEqual(resp.status_code, 401,
                             msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code, 401))

        # Verifying disable log streaming status for different tenants
        self.log.info("Verifying the disable log streaming for different tenants")
        tenant_ids = {
            'valid_tenant_id': self.tenant_id,
            # 'invalid_tenant_id': self.invalid_id
        }
        for tenant_id in tenant_ids:
            resp = capella_api.disable_log_streaming(tenant_ids[tenant_id],
                                                    self.project_id,
                                                    self.cluster_id,
                                                    sgw_id)

            if tenant_id == "valid_tenant_id":
                self.assertEqual(resp.status_code, 200,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              200))

            else:
                self.assertEqual(resp.status_code, 404,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              404))

        status = "disabling"
        while status != "disabled":
            self.sleep(10, "Waiting for log streaming to be in enabled state")
            resp = capella_api.get_log_streaming_current_status(self.tenant_id,
                                                                self.project_id,
                                                                self.cluster_id,
                                                                sgw_id)
            status = resp.json()["data"]["status"]

        config = {
            "output_type": "datadog",
            "url": "https://http-intake.logs.datadoghq.eu",
            "api_key": "12345"
        }
        resp = capella_api.create_upsert_log_streaming(self.tenant_id,
                                                       self.project_id,
                                                       self.cluster_id,
                                                       sgw_id, config)
        self.assertEqual(200, resp.status_code,
                         msg="FAIL, Outcome: {0}, Expected: {1}"
                                 .format(resp.status_code, 200))
        status = "enabling"
        while status != "enabled":
            resp = capella_api.get_log_streaming_current_status(self.tenant_id,
                                                                self.project_id,
                                                                self.cluster_id,
                                                                sgw_id)
            status = resp.json()["data"]["status"]
            self.sleep(10, "Waiting for log streaming to be in paused state")

        # Verifying the disable log streaming for different projects
        self.log.info("Verifying the disable log streaming status for different projects")
        project_ids = {
            'valid_project_id': self.project_id,
            'invalid_project_id': self.invalid_id
        }
        for project_id in project_ids:
            resp = capella_api.disable_log_streaming(self.tenant_id,
                                                    project_ids[project_id],
                                                    self.cluster_id,
                                                    sgw_id)

            if project_id == "valid_project_id":
                self.assertEqual(resp.status_code, 200,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(
                                     resp.status_code,
                                     200))

            else:
                self.assertEqual(resp.status_code, 403,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(
                                     resp.status_code,
                                     403))

        status = "disabling"
        while status != "disabled":
            resp = capella_api.get_log_streaming_current_status(self.tenant_id,
                                                                self.project_id,
                                                                self.cluster_id,
                                                                sgw_id)
            status = resp.json()["data"]["status"]
            self.sleep(10, "Waiting for log streaming to be in paused state")
        config = {
            "output_type": "datadog",
            "url": "https://http-intake.logs.datadoghq.eu",
            "api_key": "12345"
        }
        resp = capella_api.create_upsert_log_streaming(self.tenant_id,
                                                       self.project_id,
                                                       self.cluster_id,
                                                       sgw_id, config)
        self.assertEqual(200, resp.status_code,
                         msg="FAIL, Outcome: {0}, Expected: {1}"
                                 .format(resp.status_code, 200))
        status = "enabling"
        while status != "enabled":
            resp = capella_api.get_log_streaming_current_status(self.tenant_id,
                                                                self.project_id,
                                                                self.cluster_id,
                                                                sgw_id)
            status = resp.json()["data"]["status"]
            self.sleep(10, "Waiting for log streaming to be in healthy state")

        # Verifying the get log streaming status for different clusters
        self.log.info("Verifying the disable log streaming status for different clusters")
        cluster_ids = {
            'valid_cluster_id': self.cluster_id,
            'invalid_cluster_id': self.invalid_id
        }
        for cluster_id in cluster_ids:
            resp = capella_api.disable_log_streaming(self.tenant_id,
                                                    self.project_id,
                                                    cluster_ids[cluster_id],
                                                    sgw_id)

            if cluster_id == "valid_cluster_id":
                self.assertEqual(resp.status_code, 200,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(
                                     resp.status_code, 200))

            else:
                self.assertEqual(resp.status_code, 403,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(
                                     resp.status_code, 403))

        status = "disabling"
        while status != "disabled":
            resp = capella_api.get_log_streaming_current_status(self.tenant_id,
                                                                self.project_id,
                                                                self.cluster_id,
                                                                sgw_id)
            status = resp.json()["data"]["status"]
            self.sleep(10, "Waiting for log streaming to be in paused state")

        config = {
            "output_type": "datadog",
            "url": "https://http-intake.logs.datadoghq.eu",
            "api_key": "12345"
        }
        resp = capella_api.create_upsert_log_streaming(self.tenant_id,
                                                       self.project_id,
                                                       self.cluster_id,
                                                       sgw_id, config)
        self.assertEqual(200, resp.status_code,
                         msg="FAIL, Outcome: {0}, Expected: {1}"
                         .format(resp.status_code, 200))
        status = "enabling"
        while status != "enabled":
            resp = capella_api.get_log_streaming_current_status(self.tenant_id,
                                                                self.project_id,
                                                                self.cluster_id,
                                                                sgw_id)
            status = resp.json()["data"]["status"]
            self.sleep(10, "Waiting for log streaming to be in healthy state")

        # Verifying the pause log streaming for organization RBAC roles
        self.log.info("Verifying the disable log streaming status for organization roles")
        for user in self.test_users:
            self.log.info("Checking for role - {}".format(self.test_users[user]["role"]))

            capellaAPIRole = CapellaAPI("https://" + self.url, '', '', self.test_users[
                user]["mailid"], self.test_users[user]["password"])
            role_response = capellaAPIRole.disable_log_streaming(self.tenant_id,
                                                                 self.project_id,
                                                                 self.cluster_id,
                                                                 sgw_id)

            if self.test_users[user]["role"] == "organizationOwner":
                self.assertEqual(role_response.status_code, 200,
                                 msg='FAIL: Outcome:{}, Expected:{}'.format(
                                     role_response.status_code, 200))

            else:
                self.assertEqual(role_response.status_code, 403,
                                 msg='FAIL: Outcome:{}, Expected:{}'.format(
                                     role_response.status_code, 403))

        status = "disabling"
        while status != "disabled":
            resp = capella_api.get_log_streaming_current_status(self.tenant_id,
                                                                self.project_id,
                                                                self.cluster_id,
                                                                sgw_id)
            status = resp.json()["data"]["status"]
            self.sleep(10, "Waiting for log streaming to be in paused state")

        config = {
            "output_type": "datadog",
            "url": "https://http-intake.logs.datadoghq.eu",
            "api_key": "12345"
        }
        resp = capella_api.create_upsert_log_streaming(self.tenant_id,
                                                       self.project_id,
                                                       self.cluster_id,
                                                       sgw_id, config)
        self.assertEqual(200, resp.status_code,
                         msg="FAIL, Outcome: {0}, Expected: {1}"
                         .format(resp.status_code, 200))
        status = "enabling"
        while status != "enabled":
            resp = capella_api.get_log_streaming_current_status(self.tenant_id,
                                                                self.project_id,
                                                                self.cluster_id,
                                                                sgw_id)
            status = resp.json()["data"]["status"]
            self.sleep(10, "Waiting for log streaming to be in healthy state")

        # Testing for Project Level RBAC roles
        self.log.info("Verifying endpoint for different roles under project - RBAC")
        project_roles = ["projectDataViewer", "projectOwner", "projectClusterManager",
                         "projectClusterViewer", "projectDataWriter"]
        user = self.test_users["User3"]
        for role in project_roles:
            self.log.info("Adding user to project with role as {}".format(role))
            config = {
                "resourceId": self.project_id,
                "resourceType": "project",
                "roles": [role], "users": [user["userid"]]
            }
            capella_api.add_user_to_project(self.tenant_id, json.dumps(config))

            capellaAPIRole = CapellaAPI("https://" + self.url, '', '', user["mailid"],
                                        user["password"])
            role_response = capellaAPIRole.disable_log_streaming(self.tenant_id,
                                                                 self.project_id,
                                                                 self.cluster_id,
                                                                 sgw_id)

            if role in ["projectOwner", "projectClusterManager"]:
                self.assertEqual(role_response.status_code, 200,
                                 msg='FAIL, Outcome:{}, Expected:{}'
                                 .format(role_response.status_code, 200))

                status = "disabling"
                while status != "disabled":
                    resp = capella_api.get_log_streaming_current_status(self.tenant_id,
                                                                        self.project_id,
                                                                        self.cluster_id,
                                                                        sgw_id)
                    status = resp.json()["data"]["status"]
                    self.sleep(10, "Waiting for log streaming to be in paused state")

                config = {
                    "output_type": "datadog",
                    "url": "https://http-intake.logs.datadoghq.eu",
                    "api_key": "12345"
                }
                resp = capella_api.create_upsert_log_streaming(self.tenant_id,
                                                               self.project_id,
                                                               self.cluster_id,
                                                               sgw_id, config)
                self.assertEqual(200, resp.status_code,
                                 msg="FAIL, Outcome: {0}, Expected: {1}"
                                 .format(resp.status_code, 200))
                status = "enabling"
                while status != "enabled":
                    resp = capella_api.get_log_streaming_current_status(self.tenant_id,
                                                                        self.project_id,
                                                                        self.cluster_id,
                                                                        sgw_id)
                    status = resp.json()["data"]["status"]
                    self.sleep(10, "Waiting for log streaming to be in enabled state")

            else:
                self.assertEqual(role_response.status_code, 403,
                                 msg='FAIL, Outcome:{}, Expected:{}'
                                 .format(role_response.status_code, 403))

            self.log.info("Removing user from project")
            capella_api.remove_user_from_project(self.tenant_id, user["userid"],
                                                 self.project_id)

    def test_get_all_collector_config(self):
        self.log.info("Verifying status code for getting the log streaming collector config status")

        capella_api = CapellaAPI("https://" + self.url, self.secret_key, self.access_key,
                                 self.user, self.passwd)

        app_service_config = {
            "clusterId": self.cluster_id,
            "compute": {"type": "c5.2xlarge"},
            "desired_capacity": 2,
            "name": "test-app-service"
        }

        resp = capella_api.create_sgw_backend(self.tenant_id, app_service_config)
        self.assertEqual(202, resp.status_code,
                         msg="FAIL: Outcome: {}, Expected: {}".format(resp.status_code, 202))

        sgw_id = resp.json()["id"]
        status = "deploying"
        while status != "healthy":
            resp = capella_api.get_sgw_backend(self.tenant_id, self.project_id, self.cluster_id,
                                               sgw_id)
            status = resp.json()["data"]["status"]["state"]
            self.sleep(15, "Waiting for App Services to be in healthy state")

        # Trying out with different headers to bypass authentication
        self.log.info("Calling API with various headers to bypass authentication")
        headers = {
            "invalid_header": 'abcdefgh',
            "empty_header": ''
        }

        for header in headers:
            cbc_api_request_header = {
                'Authorization': 'Bearer %s' % headers[header],
                'Length': ''
            }

            resp = self.get_all_valid_collector_config(self.tenant_id, self.project_id,
                                                         self.cluster_id, sgw_id,
                                                         header=cbc_api_request_header)
            self.assertEqual(resp.status_code, 401,
                             msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code, 401))

        # Verifying the get log streaming status for different tenants
        self.log.info("Verifying the log streaming status for different tenants")
        tenant_ids = {
            'valid_tenant_id': self.tenant_id,
            # 'invalid_tenant_id': self.invalid_id
        }
        for tenant_id in tenant_ids:
            resp = capella_api.get_all_valid_collector_config(tenant_ids[tenant_id],
                                                              self.project_id,
                                                              self.cluster_id,
                                                              sgw_id)

            if tenant_id == "valid_tenant_id":
                self.assertEqual(resp.status_code, 200,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              200))

            else:
                self.assertEqual(resp.status_code, 404,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              404))

        # Verifying the get log streaming status for different projects
        self.log.info("Verifying the log streaming status for different projects")
        project_ids = {
            'valid_project_id': self.project_id,
            'invalid_project_id': self.invalid_id
        }
        for project_id in project_ids:
            resp = capella_api.get_all_valid_collector_config(self.tenant_id,
                                                              project_ids[project_id],
                                                              self.cluster_id,
                                                              sgw_id)

            if project_id == "valid_project_id":
                self.assertEqual(resp.status_code, 200,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              200))

            else:
                self.assertEqual(resp.status_code, 403,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              403))

        # Verifying the get log streaming status for different clusters
        self.log.info("Verifying the get log streaming status for different clusters")
        cluster_ids = {
            'valid_cluster_id': self.cluster_id,
            'invalid_cluster_id': self.invalid_id
        }
        for cluster_id in cluster_ids:
            resp = capella_api.get_all_valid_collector_config(self.tenant_id,
                                                              self.project_id,
                                                              cluster_ids[cluster_id],
                                                              sgw_id)

            if cluster_id == "valid_cluster_id":
                self.assertEqual(resp.status_code, 200,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              200))

            else:
                self.assertEqual(resp.status_code, 403,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              403))

        # Verifying the get log streaming status for organization RBAC roles
        self.log.info("Verifying the get log streaming status for organization roles")
        for user in self.test_users:
            self.log.info("Checking for role - {}".format(self.test_users[user]["role"]))

            capellaAPIRole = CapellaAPI("https://" + self.url, '', '', self.test_users[
                user]["mailid"], self.test_users[user]["password"])
            role_response = capellaAPIRole.get_all_valid_collector_config(self.tenant_id,
                                                                          self.project_id,
                                                                          self.cluster_id,
                                                                          sgw_id)

            if self.test_users[user]["role"] == "organizationOwner":
                self.assertEqual(role_response.status_code, 200,
                                 msg='FAIL: Outcome:{}, Expected:{}'.format(
                                     role_response.status_code, 200))

            else:
                self.assertEqual(role_response.status_code, 403,
                                 msg='FAIL: Outcome:{}, Expected:{}'.format(
                                     role_response.status_code, 403))

        # Testing for Project Level RBAC roles
        self.log.info("Verifying endpoint for different roles under project - RBAC")
        project_roles = ["projectDataViewer", "projectOwner", "projectClusterManager",
                         "projectClusterViewer", "projectDataWriter"]
        user = self.test_users["User3"]
        for role in project_roles:
            self.log.info("Adding user to project with role as {}".format(role))
            config = {
                "resourceId": self.project_id,
                "resourceType": "project",
                "roles": [role], "users": [user["userid"]]
            }
            capella_api.add_user_to_project(self.tenant_id, json.dumps(config))

            capellaAPIRole = CapellaAPI("https://" + self.url, '', '', user["mailid"],
                                        user["password"])
            role_response = capellaAPIRole.get_all_valid_collector_config(self.tenant_id,
                                                                          self.project_id,
                                                                          self.cluster_id,
                                                                          sgw_id)

            self.assertEqual(role_response.status_code, 200,
                             msg='FAIL, Outcome:{}, Expected:{}'
                             .format(role_response.status_code, 200))

            self.log.info("Removing user from project")
            capella_api.remove_user_from_project(self.tenant_id, user["userid"],
                                                 self.project_id)

    def test_get_all_logging_options(self):
        self.log.info("Verifying status code for getting the log streaming collector config status")

        capella_api = CapellaAPI("https://" + self.url, self.secret_key, self.access_key,
                                 self.user, self.passwd)

        app_service_config = {
            "clusterId": self.cluster_id,
            "compute": {"type": "c5.2xlarge"},
            "desired_capacity": 2,
            "name": "test-app-service"
        }

        resp = capella_api.create_sgw_backend(self.tenant_id, app_service_config)
        self.assertEqual(202, resp.status_code,
                         msg="FAIL: Outcome: {}, Expected: {}".format(resp.status_code, 202))

        sgw_id = resp.json()["id"]
        status = "deploying"
        while status != "healthy":
            resp = capella_api.get_sgw_backend(self.tenant_id, self.project_id, self.cluster_id,
                                               sgw_id)
            status = resp.json()["data"]["status"]["state"]
            self.sleep(15, "Waiting for App Services to be in healthy state")

        # Trying out with different headers to bypass authentication
        self.log.info("Calling API with various headers to bypass authentication")
        headers = {
            "invalid_header": 'abcdefgh',
            "empty_header": ''
        }

        for header in headers:
            cbc_api_request_header = {
                'Authorization': 'Bearer %s' % headers[header],
                'Length': ''
            }

            resp = self.get_all_logging_options(self.tenant_id,
                                                self.project_id,
                                                self.cluster_id, sgw_id,
                                                header=cbc_api_request_header)
            self.assertEqual(resp.status_code, 401,
                             msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code, 401))

        # Verifying the get log streaming status for different tenants
        self.log.info("Verifying the log streaming status for different tenants")
        tenant_ids = {
            'valid_tenant_id': self.tenant_id,
            # 'invalid_tenant_id': self.invalid_id
        }
        for tenant_id in tenant_ids:
            resp = capella_api.get_all_logging_options(tenant_ids[tenant_id],
                                                       self.project_id,
                                                       self.cluster_id,
                                                       sgw_id)

            if tenant_id == "valid_tenant_id":
                self.assertEqual(resp.status_code, 200,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              200))

            else:
                self.assertEqual(resp.status_code, 404,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              404))

        # Verifying the get log streaming status for different projects
        self.log.info("Verifying the log streaming status for different projects")
        project_ids = {
            'valid_project_id': self.project_id,
            'invalid_project_id': self.invalid_id
        }
        for project_id in project_ids:
            resp = capella_api.get_all_logging_options(self.tenant_id,
                                                       project_ids[project_id],
                                                       self.cluster_id,
                                                       sgw_id)

            if project_id == "valid_project_id":
                self.assertEqual(resp.status_code, 200,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              200))

            else:
                self.assertEqual(resp.status_code, 403,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              403))

        # Verifying the get log streaming status for different clusters
        self.log.info("Verifying the get log streaming status for different clusters")
        cluster_ids = {
            'valid_cluster_id': self.cluster_id,
            'invalid_cluster_id': self.invalid_id
        }
        for cluster_id in cluster_ids:
            resp = capella_api.get_all_logging_options(self.tenant_id,
                                                       self.project_id,
                                                       cluster_ids[cluster_id],
                                                       sgw_id)

            if cluster_id == "valid_cluster_id":
                self.assertEqual(resp.status_code, 200,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              200))

            else:
                self.assertEqual(resp.status_code, 403,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              403))

        # Verifying the get log streaming status for organization RBAC roles
        self.log.info("Verifying the get log streaming status for organization roles")
        for user in self.test_users:
            self.log.info("Checking for role - {}".format(self.test_users[user]["role"]))

            capellaAPIRole = CapellaAPI("https://" + self.url, '', '', self.test_users[
                user]["mailid"], self.test_users[user]["password"])
            role_response = capellaAPIRole.get_all_logging_options(self.tenant_id,
                                                                   self.project_id,
                                                                   self.cluster_id,
                                                                   sgw_id)

            if self.test_users[user]["role"] == "organizationOwner":
                self.assertEqual(role_response.status_code, 200,
                                 msg='FAIL: Outcome:{}, Expected:{}'.format(
                                     role_response.status_code, 200))

            else:
                self.assertEqual(role_response.status_code, 403,
                                 msg='FAIL: Outcome:{}, Expected:{}'.format(
                                     role_response.status_code, 403))

        # Testing for Project Level RBAC roles
        self.log.info("Verifying endpoint for different roles under project - RBAC")
        project_roles = ["projectDataViewer", "projectOwner", "projectClusterManager",
                         "projectClusterViewer", "projectDataWriter"]
        user = self.test_users["User3"]
        for role in project_roles:
            self.log.info("Adding user to project with role as {}".format(role))
            config = {
                "resourceId": self.project_id,
                "resourceType": "project",
                "roles": [role], "users": [user["userid"]]
            }
            capella_api.add_user_to_project(self.tenant_id, json.dumps(config))

            capellaAPIRole = CapellaAPI("https://" + self.url, '', '', user["mailid"],
                                        user["password"])
            role_response = capellaAPIRole.get_all_logging_options(self.tenant_id,
                                                                   self.project_id,
                                                                   self.cluster_id,
                                                                   sgw_id)

            self.assertEqual(role_response.status_code, 200,
                             msg='FAIL, Outcome:{}, Expected:{}'
                             .format(role_response.status_code, 200))

            self.log.info("Removing user from project")
            capella_api.remove_user_from_project(self.tenant_id, user["userid"],
                                                 self.project_id)

    def test_get_current_config_logging_options(self):
        self.log.info("Verifying status code for getting the log streaming collector config status")

        capella_api = CapellaAPI("https://" + self.url, self.secret_key, self.access_key,
                                 self.user, self.passwd)

        app_service_config = {
            "clusterId": self.cluster_id,
            "compute": {"type": "c5.2xlarge"},
            "desired_capacity": 2,
            "name": "test-app-service"
        }

        resp = capella_api.create_sgw_backend(self.tenant_id, app_service_config)
        self.assertEqual(202, resp.status_code,
                         msg="FAIL: Outcome: {}, Expected: {}".format(resp.status_code, 202))

        sgw_id = resp.json()["id"]
        status = "deploying"
        while status != "healthy":
            resp = capella_api.get_sgw_backend(self.tenant_id, self.project_id, self.cluster_id,
                                               sgw_id)
            status = resp.json()["data"]["status"]["state"]
            self.sleep(15, "Waiting for App Services to be in healthy state")

        sgw_id = "b602be29-7887-4e87-b70c-78e725f030a2"

        self.log.info("Creating an app endpoint for the app service")
        endpoint_config = {
            "name": "endpoint1",
            "bucket": "beer-sample",
            "delta_sync": True,
            "import_filter": "",
            "sync":""
        }
        resp = capella_api.create_sgw_database(self.tenant_id,
                                               self.project_id,
                                               self.cluster_id,
                                               sgw_id,
                                               endpoint_config)
        self.assertEqual(202, resp.status_code,
                         msg="FAIL: Outcome: {}, Expected: {}".format(resp.status_code, 202))

        resp = capella_api.resume_sgw_database(self.tenant_id,
                                               self.project_id,
                                               self.cluster_id,
                                               sgw_id,
                                               endpoint_config,
                                               'endpoint1')
        self.assertEqual(200, resp.status_code,
                         msg="FAIL: Outcome: {}, Expected: {}".format(resp.status_code, 200))

        # Trying out with different headers to bypass authentication
        self.log.info("Calling API with various headers to bypass authentication")
        headers = {
            "invalid_header": 'abcdefgh',
            "empty_header": ''
        }

        for header in headers:
            cbc_api_request_header = {
                'Authorization': 'Bearer %s' % headers[header],
                'Length': ''
            }

            resp = self.get_current_config_logging_options(self.tenant_id,
                                                           self.project_id,
                                                           self.cluster_id, sgw_id, "endpoint1",
                                                           header=cbc_api_request_header)
            self.assertEqual(resp.status_code, 401,
                             msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code, 401))

        # Verifying the get log streaming status for different tenants
        self.log.info("Verifying the log streaming status for different tenants")
        tenant_ids = {
            'valid_tenant_id': self.tenant_id,
            # 'invalid_tenant_id': self.invalid_id
        }
        for tenant_id in tenant_ids:
            resp = capella_api.get_current_config_logging_options(tenant_ids[tenant_id],
                                                                  self.project_id,
                                                                  self.cluster_id,
                                                                  sgw_id,
                                                                  "endpoint1")

            if tenant_id == "valid_tenant_id":
                self.assertEqual(resp.status_code, 200,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              200))

            else:
                self.assertEqual(resp.status_code, 404,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              404))

        # Verifying the get log streaming status for different projects
        self.log.info("Verifying the log streaming status for different projects")
        project_ids = {
            'valid_project_id': self.project_id,
            'invalid_project_id': self.invalid_id
        }
        for project_id in project_ids:
            resp = capella_api.get_current_config_logging_options(self.tenant_id,
                                                                  project_ids[project_id],
                                                                  self.cluster_id,
                                                                  sgw_id,
                                                                  "endpoint1")

            if project_id == "valid_project_id":
                self.assertEqual(resp.status_code, 200,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              200))

            else:
                self.assertEqual(resp.status_code, 403,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              403))

        # Verifying the get log streaming status for different clusters
        self.log.info("Verifying the get log streaming status for different clusters")
        cluster_ids = {
            'valid_cluster_id': self.cluster_id,
            'invalid_cluster_id': self.invalid_id
        }
        for cluster_id in cluster_ids:
            resp = capella_api.get_current_config_logging_options(self.tenant_id,
                                                                  self.project_id,
                                                                  cluster_ids[cluster_id],
                                                                  sgw_id,
                                                                  "endpoint1")

            if cluster_id == "valid_cluster_id":
                self.assertEqual(resp.status_code, 200,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              200))

            else:
                self.assertEqual(resp.status_code, 403,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              403))

        # Verifying the get log streaming status for organization RBAC roles
        self.log.info("Verifying the get log streaming status for organization roles")
        for user in self.test_users:
            self.log.info("Checking for role - {}".format(self.test_users[user]["role"]))

            capellaAPIRole = CapellaAPI("https://" + self.url, '', '', self.test_users[
                user]["mailid"], self.test_users[user]["password"])
            role_response = capellaAPIRole.get_current_config_logging_options(
                                                                  self.tenant_id,
                                                                  self.project_id,
                                                                  self.cluster_id,
                                                                  sgw_id,
                                                                  "endpoint1")

            if self.test_users[user]["role"] == "organizationOwner":
                self.assertEqual(role_response.status_code, 200,
                                 msg='FAIL: Outcome:{}, Expected:{}'.format(
                                     role_response.status_code, 200))

            else:
                self.assertEqual(role_response.status_code, 403,
                                 msg='FAIL: Outcome:{}, Expected:{}'.format(
                                     role_response.status_code, 403))

        # Testing for Project Level RBAC roles
        self.log.info("Verifying endpoint for different roles under project - RBAC")
        project_roles = ["projectDataViewer", "projectOwner", "projectClusterManager",
                         "projectClusterViewer", "projectDataWriter"]
        user = self.test_users["User3"]
        for role in project_roles:
            self.log.info("Adding user to project with role as {}".format(role))
            config = {
                "resourceId": self.project_id,
                "resourceType": "project",
                "roles": [role], "users": [user["userid"]]
            }
            capella_api.add_user_to_project(self.tenant_id, json.dumps(config))

            capellaAPIRole = CapellaAPI("https://" + self.url, '', '', user["mailid"],
                                        user["password"])
            role_response = capellaAPIRole.get_current_config_logging_options(
                                                                  self.tenant_id,
                                                                  self.project_id,
                                                                  self.cluster_id,
                                                                  sgw_id,
                                                                  "endpoint1")

            self.assertEqual(role_response.status_code, 200,
                             msg='FAIL, Outcome:{}, Expected:{}'
                             .format(role_response.status_code, 200))

            self.log.info("Removing user from project")
            capella_api.remove_user_from_project(self.tenant_id, user["userid"],
                                                 self.project_id)

    def test_upsert_app_endpoint_config(self):
        self.log.info("Verifying status code for upserting app endpoint config")

        capella_api = CapellaAPI("https://" + self.url, self.secret_key, self.access_key,
                                 self.user, self.passwd)

        app_service_config = {
            "clusterId": self.cluster_id,
            "compute": {"type": "c5.2xlarge"},
            "desired_capacity": 2,
            "name": "test-app-service"
        }

        resp = capella_api.create_sgw_backend(self.tenant_id, app_service_config)
        self.assertEqual(202, resp.status_code,
                         msg="FAIL: Outcome: {}, Expected: {}".format(resp.status_code, 202))

        sgw_id = resp.json()["id"]
        status = "deploying"
        while status != "healthy":
            resp = capella_api.get_sgw_backend(self.tenant_id, self.project_id, self.cluster_id,
                                               sgw_id)
            status = resp.json()["data"]["status"]["state"]
            self.sleep(15, "Waiting for App Services to be in healthy state")

        self.log.info("Creating an app endpoint for the app service")
        endpoint_config = {
            "name": "endpoint1",
            "bucket": "beer-sample",
            "delta_sync": True,
            "import_filter": "",
            "sync":""
        }
        resp = capella_api.create_sgw_database(self.tenant_id,
                                               self.project_id,
                                               self.cluster_id,
                                               sgw_id,
                                               endpoint_config)
        self.assertEqual(202, resp.status_code,
                         msg="FAIL: Outcome: {}, Expected: {}".format(resp.status_code, 202))

        resp = capella_api.resume_sgw_database(self.tenant_id,
                                               self.project_id,
                                               self.cluster_id,
                                               sgw_id,
                                               endpoint_config,
                                               'endpoint1')
        self.assertEqual(200, resp.status_code,
                         msg="FAIL: Outcome: {}, Expected: {}".format(resp.status_code, 200))

        # Trying out with different headers to bypass authentication
        self.log.info("Calling API with various headers to bypass authentication")
        headers = {
            "invalid_header": 'abcdefgh',
            "empty_header": ''
        }

        for header in headers:
            cbc_api_request_header = {
                'Authorization': 'Bearer %s' % headers[header],
                'Length': ''
            }
            config = {
                "log_level": "info",
                "log_keys": ["Bucket", "Sync"]
             }

            resp = self.put_current_config_logging_options(self.tenant_id,
                                                           self.project_id,
                                                           self.cluster_id, sgw_id, "endpoint1",
                                                           config,
                                                           header=cbc_api_request_header)
            self.assertEqual(resp.status_code, 401,
                             msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code, 401))

        # Verifying the get log streaming status for different tenants
        self.log.info("Verifying the log streaming status for different tenants")
        tenant_ids = {
            'valid_tenant_id': self.tenant_id,
            # 'invalid_tenant_id': self.invalid_id
        }
        for tenant_id in tenant_ids:
            config = {
                "log_level": "info",
                "log_keys": ["Bucket", "Sync"]
             }
            resp = capella_api.put_current_config_logging_options(tenant_ids[tenant_id],
                                                                  self.project_id,
                                                                  self.cluster_id,
                                                                  sgw_id,
                                                                  "endpoint1",
                                                                  config)

            if tenant_id == "valid_tenant_id":
                self.assertEqual(resp.status_code, 200,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              200))

            else:
                self.assertEqual(resp.status_code, 404,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              404))

        # Verifying the get log streaming status for different projects
        self.log.info("Verifying the log streaming status for different projects")
        project_ids = {
            'valid_project_id': self.project_id,
            'invalid_project_id': self.invalid_id
        }
        for project_id in project_ids:
            config = {
                "log_level": "info",
                "log_keys": ["Bucket", "Sync"]
             }
            resp = capella_api.put_current_config_logging_options(self.tenant_id,
                                                                  project_ids[project_id],
                                                                  self.cluster_id,
                                                                  sgw_id,
                                                                  "endpoint1",
                                                                  config)

            if project_id == "valid_project_id":
                self.assertEqual(resp.status_code, 200,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              200))

            else:
                self.assertEqual(resp.status_code, 403,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              403))

        # Verifying the get log streaming status for different clusters
        self.log.info("Verifying the get log streaming status for different clusters")
        cluster_ids = {
            'valid_cluster_id': self.cluster_id,
            'invalid_cluster_id': self.invalid_id
        }
        for cluster_id in cluster_ids:
            config = {
                "log_level": "info",
                "log_keys": ["Bucket", "Sync"]
             }
            resp = capella_api.put_current_config_logging_options(self.tenant_id,
                                                                  self.project_id,
                                                                  cluster_ids[cluster_id],
                                                                  sgw_id,
                                                                  "endpoint1",
                                                                  config)

            if cluster_id == "valid_cluster_id":
                self.assertEqual(resp.status_code, 200,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              200))

            else:
                self.assertEqual(resp.status_code, 403,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              403))

        # Verifying the get log streaming status for organization RBAC roles
        self.log.info("Verifying the get log streaming status for organization roles")
        for user in self.test_users:
            self.log.info("Checking for role - {}".format(self.test_users[user]["role"]))

            capellaAPIRole = CapellaAPI("https://" + self.url, '', '', self.test_users[
                user]["mailid"], self.test_users[user]["password"])
            config = {
                "log_level": "info",
                "log_keys": ["Bucket", "Sync"]
            }
            role_response = capellaAPIRole.put_current_config_logging_options(
                                                                        self.tenant_id,
                                                                        self.project_id,
                                                                        self.cluster_id,
                                                                        sgw_id,
                                                                        "endpoint1",
                                                                        config)

            if self.test_users[user]["role"] == "organizationOwner":
                self.assertEqual(role_response.status_code, 200,
                                 msg='FAIL: Outcome:{}, Expected:{}'.format(
                                     role_response.status_code, 200))

            else:
                self.assertEqual(role_response.status_code, 403,
                                 msg='FAIL: Outcome:{}, Expected:{}'.format(
                                     role_response.status_code, 403))

        # Testing for Project Level RBAC roles
        self.log.info("Verifying endpoint for different roles under project - RBAC")
        project_roles = ["projectDataViewer", "projectOwner", "projectClusterManager",
                         "projectClusterViewer", "projectDataWriter"]
        user = self.test_users["User3"]
        for role in project_roles:
            self.log.info("Adding user to project with role as {}".format(role))
            config = {
                "resourceId": self.project_id,
                "resourceType": "project",
                "roles": [role], "users": [user["userid"]]
            }
            capella_api.add_user_to_project(self.tenant_id, json.dumps(config))

            capellaAPIRole = CapellaAPI("https://" + self.url, '', '', user["mailid"],
                                        user["password"])
            config = {
                "log_level": "info",
                "log_keys": ["Bucket", "Sync"]
             }
            role_response = capellaAPIRole.get_current_config_logging_options(
                                                                            self.tenant_id,
                                                                            self.project_id,
                                                                            self.cluster_id,
                                                                            sgw_id,
                                                                            "endpoint1",
                                                                            config)

            if role in ["projectOwner", "projectClusterManager"]:
                self.assertEqual(role_response.status_code, 200,
                                 msg='FAIL, Outcome:{}, Expected:{}'
                                 .format(role_response.status_code, 200))

            else:
                self.assertEqual(role_response.status_code, 403,
                                 msg='FAIL, Outcome:{}, Expected:{}'
                                 .format(role_response.status_code, 403))

            self.log.info("Removing user from project")
            capella_api.remove_user_from_project(self.tenant_id, user["userid"],
                                                 self.project_id)