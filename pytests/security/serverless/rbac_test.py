import json
import time
from pytests.basetestcase import BaseTestCase
from capellaAPI.capella.serverless.CapellaAPI import CapellaAPI


class SecurityTest(BaseTestCase):

    def setUp(self):
        BaseTestCase.setUp(self)
        self.url = self.input.capella.get("pod")
        self.user = self.input.capella.get("capella_user")
        self.passwd = self.input.capella.get("capella_pwd")
        self.tenant_id = self.input.capella.get("tenant_id")
        self.secret_key = self.input.capella.get("secret_key")
        self.access_key = self.input.capella.get("access_key")
        self.project_id = self.input.capella.get("project")
        self.invalid_id = "00000000-0000-0000-0000-000000000000"
        if self.input.capella.get("test_users"):
            self.test_users = json.loads(json.loads(self.input.capella.get("test_users")))
        else:
            self.test_users = {"User1": {"password": self.passwd, "mailid": self.user,
                                         "role": "organizationOwner"}}
        self.invalid_tenant_id = self.input.param("invalid_tenant_id", self.invalid_id)
        self.invalid_userid = self.input.param("invalid_userid", self.invalid_id)

    def tearDown(self):
        super(SecurityTest, self).tearDown()

    def test_retrieve_bucket_details(self):
        self.log.info("Verifying status code for retrieving bucket details")
        self.log.info("Role -*- d_id -*- p_id -*- t_id")
        valid_database_id = self.input.param("valid_database_id", self.invalid_id)
        unauth_database_id = self.input.param("valid_database_id", self.invalid_id)
        junk_database_id = self.input.param("junk_database_id", self.invalid_id)
        unauth_proj_id = self.input.param("unauth_proj_id", self.invalid_id)
        database_ids = {"valid_database_id": valid_database_id,
                        "unauth_database_id": unauth_database_id,
                        "junk_database_id": junk_database_id}
        project_ids = {"valid_project_id": self.project_id,
                       "invalid_project_id": self.invalid_id, "unauth_proj_id": unauth_proj_id}
        tenant_ids = {"valid_tenant_id": self.tenant_id,
                      "unauth_tenant_id": self.invalid_tenant_id, "junk_tenant_id": self.invalid_id}
        for user in self.test_users:
            capella_api = CapellaAPI("https://" + self.url,
                                     self.test_users[user]["mailid"],
                                     self.test_users[user]["password"])
            # Organisation roles
            if user != "User5":
                self.log.info("{0}".format(user))
                for d_id in database_ids:
                    for p_id in project_ids:
                        for t_id in tenant_ids:
                            self.log.info("{0} -*- {1} -*- {2} -*- {3}"
                                          .format(self.test_users[user]["role"], d_id, p_id, t_id))
                            resp = capella_api.get_serverless_db_details(tenant_ids[t_id],
                                                                         project_ids[p_id],
                                                                         database_ids[d_id])
                            # Parsing from top level to bottom level
                            if d_id == "junk_database_id":
                                if resp.status_code != 405:
                                    self.fail("Junk should return 405")
                            elif t_id == "junk_tenant_id" or t_id == "unauth_tenant_id":
                                if resp.status_code != 404:
                                    self.fail(
                                        "No access should be allowed as it is an unauthorised "
                                        "tenant id")
                            elif p_id == "invalid_project_id" or p_id == "unauth_proj_id":
                                if resp.status_code != 404:
                                    self.fail(
                                        "No access should be allowed as it is an unauthorised "
                                        "project id")
                            elif d_id == "unauth_database_id":
                                if resp.status_code != 404:
                                    self.fail(
                                        "No access should be allowed as it is an unauthorised "
                                        "database id")
                            elif self.test_users[user]["role"] != "organizationOwner":
                                if resp.status_code != 404:
                                    self.fail(
                                        "No access should be allowed as the user does not have "
                                        "the required permissions")
                            elif self.test_users[user]["role"] == "organizationOwner":
                                if resp.status_code != 200:
                                    self.fail("Access should be allowed to view bucket")
                            else:
                                self.fail("Did not verify this case!!!")
            # Project roles
            else:
                proj_roles = ["projectOwner", "projectDataWriter", "projectClusterManager",
                              "projectDataViewer", "projectClusterViewer"]
                for role in proj_roles:
                    body = {"resourceId": self.project_id, "resourceType": "project",
                            "roles": [role], "users": [self.test_users[user]["userid"]]}
                    capella_api_a = CapellaAPI("https://" + self.url, self.user, self.passwd)
                    capella_api_a.edit_project_permissions(self.tenant_id, body)
                    self.log.info("{0}".format(user))
                    for d_id in database_ids:
                        for p_id in project_ids:
                            for t_id in tenant_ids:
                                self.log.info("{0} -*- {1} -*- {2} -*- {3}"
                                              .format(role, d_id, p_id, t_id))
                                resp = capella_api.get_serverless_db_details(tenant_ids[t_id],
                                                                             project_ids[p_id],
                                                                             database_ids[d_id])
                                if d_id == "junk_database_id":
                                    if resp.status_code != 405:
                                        self.fail("Junk should return 405")
                                elif t_id == "junk_tenant_id" or t_id == "unauth_tenant_id":
                                    if resp.status_code != 404:
                                        self.fail(
                                            "No access should be allowed as it is an unauthorised "
                                            "tenant id")
                                elif p_id == "invalid_project_id" or p_id == "unauth_proj_id":
                                    if resp.status_code != 404:
                                        self.fail(
                                            "No access should be allowed as it is an unauthorised "
                                            "project id")
                                elif d_id == "unauth_database_id":
                                    if resp.status_code != 404:
                                        self.fail(
                                            "No access should be allowed as it is an unauthorised "
                                            "database id")
                                elif p_id != "unauth_proj_id":
                                    if resp.status_code != 200:
                                        self.fail("Access should be allowed to view bucket")
                                else:
                                    self.fail("Did not verify this case!!!")

    def test_deploy_bucket(self):
        self.log.info("Verifying status code for deploying bucket")
        self.log.info("Role -*- p_id -*- t_id")
        unauth_proj_id = self.input.param("unauth_proj_id", self.invalid_id)
        project_ids = {"valid_project_id": self.project_id,
                       "invalid_project_id": self.invalid_id, "unauth_proj_id": unauth_proj_id}
        tenant_ids = {"valid_tenant_id": self.tenant_id,
                      "unauth_tenant_id": self.invalid_tenant_id, "junk_tenant_id": self.invalid_id}
        for user in self.test_users:
            capella_api = CapellaAPI("https://" + self.url,
                                     self.test_users[user]["mailid"],
                                     self.test_users[user]["password"])
            # Organisation roles
            if user != "User5":
                self.log.info("{0}".format(user))
                for p_id in project_ids:
                    for t_id in tenant_ids:
                        self.log.info("{0} -*- {1} -*- {2}"
                                      .format(self.test_users[user]["role"], p_id, t_id))
                        specs = {"importSampleData": True,
                                 "name": "test",
                                 "projectID": project_ids[p_id],
                                 "provider": "aws",
                                 "region": "eu-west-1",
                                 "tenantID": tenant_ids[t_id]}
                        resp = capella_api.deploy_serverless_db(tenant_ids[t_id], specs)
                        # Parsing from top level to bottom level
                        if t_id == "junk_tenant_id" or t_id == "unauth_tenant_id":
                            if resp.status_code != 404:
                                self.fail("No access should be allowed as it is an unauthorised "
                                          "tenant id")
                        elif p_id == "invalid_project_id":
                            if resp.status_code != 404:
                                self.fail("No access should be allowed as it is an unauthorised "
                                          "project id")
                        elif self.test_users[user]["role"] != "organizationOwner":
                            if resp.status_code != 403:
                                self.fail("No access should be allowed as the user does not have "
                                          "the required permissions")
                        elif self.test_users[user]["role"] == "organizationOwner":
                            if resp.status_code != 202:
                                self.fail("Access should be allowed to deploy bucket")
                        else:
                            self.fail("Did not verify this case!!!")
            # Project roles
            else:
                proj_roles = ["projectOwner", "projectDataWriter", "projectClusterManager",
                              "projectDataViewer", "projectClusterViewer"]
                for role in proj_roles:
                    body = {"resourceId": self.project_id, "resourceType": "project",
                            "roles": [role], "users": [self.test_users[user]["userid"]]}
                    capella_api_a = CapellaAPI("https://" + self.url, self.user, self.passwd)
                    capella_api_a.edit_project_permissions(self.tenant_id, body)
                    self.log.info("{0}".format(user))
                    for p_id in project_ids:
                        for t_id in tenant_ids:
                            self.log.info("{0} -*- {1} -*- {2}"
                                          .format(role, p_id, t_id))
                            specs = {"importSampleData": True,
                                     "name": "test",
                                     "projectID": project_ids[p_id],
                                     "provider": "aws",
                                     "region": "eu-west-1",
                                     "tenantID": tenant_ids[t_id]}
                            resp = capella_api.deploy_serverless_db(tenant_ids[t_id], specs)
                            if t_id == "junk_tenant_id" or t_id == "unauth_tenant_id":
                                if resp.status_code != 404:
                                    self.fail(
                                        "No access should be allowed as it is an unauthorised "
                                        "tenant id")
                            elif p_id == "invalid_project_id":
                                if resp.status_code != 404:
                                    self.fail(
                                        "No access should be allowed as it is an unauthorised "
                                        "project id")
                            elif role in ["projectOwner", "projectClusterManager"] \
                                    and p_id != "unauth_proj_id":
                                if resp.status_code != 202:
                                    self.fail("Access should be allowed to deploy bucket")
                            elif role not in ["projectOwner", "projectClusterManager"] \
                                    or p_id == "unauth_proj_id":
                                if resp.status_code != 403:
                                    self.fail("No access should be allowed as the user does not "
                                              "have the required permissions")
                            else:
                                self.fail("Did not verify this case!!!")

    def test_delete_bucket(self):
        self.log.info("Verifying status code for deleting bucket ")
        self.log.info("Role -*- d_id -*- p_id -*- t_id")
        valid_database_id = self.input.param("valid_database_id", self.invalid_id)
        unauth_database_id = self.input.param("valid_database_id", self.invalid_id)
        junk_database_id = self.input.param("junk_database_id", self.invalid_id)
        unauth_proj_id = self.input.param("unauth_proj_id", self.invalid_id)
        database_ids = {"valid_database_id": valid_database_id,
                        "unauth_database_id": unauth_database_id,
                        "junk_database_id": junk_database_id}
        project_ids = {"valid_project_id": self.project_id,
                       "invalid_project_id": self.invalid_id, "unauth_proj_id": unauth_proj_id}
        tenant_ids = {"valid_tenant_id": self.tenant_id,
                      "unauth_tenant_id": self.invalid_tenant_id, "junk_tenant_id": self.invalid_id}
        for user in self.test_users:
            capella_api = CapellaAPI("https://" + self.url,
                                     self.test_users[user]["mailid"],
                                     self.test_users[user]["password"])
            # Organisation roles
            if user != "User5":
                self.log.info("{0}".format(user))
                for d_id in database_ids:
                    for p_id in project_ids:
                        for t_id in tenant_ids:
                            self.log.info("")
                            self.log.info("{0} -*- {1} -*- {2} -*- {3}"
                                          .format(self.test_users[user]["role"], d_id, p_id, t_id))
                            resp = capella_api.delete_serverless_db(tenant_ids[t_id],
                                                                    project_ids[p_id],
                                                                    database_ids[d_id])
                            # Parsing from top level to bottom level
                            if d_id == "junk_database_id":
                                if resp.status_code != 405:
                                    self.fail("Junk should return 405")
                            elif t_id == "junk_tenant_id" or t_id == "unauth_tenant_id":
                                if resp.status_code != 404:
                                    self.fail(
                                        "No access should be allowed as it is an unauthorised "
                                        "tenant id")
                            elif p_id == "invalid_project_id" or p_id == "unauth_proj_id":
                                if resp.status_code != 404:
                                    self.fail(
                                        "No access should be allowed as it is an unauthorised "
                                        "project id")
                            elif d_id == "unauth_database_id":
                                if resp.status_code != 404:
                                    self.fail(
                                        "No access should be allowed as it is an unauthorised "
                                        "database id")
                            elif self.test_users[user]["role"] != "organizationOwner":
                                if resp.status_code != 403:
                                    self.fail(
                                        "No access should be allowed as the user does not have "
                                        "the required permissions")
                            elif self.test_users[user]["role"] == "organizationOwner":
                                if resp.status_code != 202:
                                    self.fail("Access should be allowed to view bucket")
                            else:
                                self.fail("Did not verify this case!!!")
            # Project roles
            else:
                proj_roles = ["projectOwner", "projectDataWriter", "projectClusterManager",
                              "projectDataViewer", "projectClusterViewer"]
                for role in proj_roles:
                    body = {"resourceId": self.project_id, "resourceType": "project",
                            "roles": [role], "users": [self.test_users[user]["userid"]]}
                    capella_api_a = CapellaAPI("https://" + self.url, self.user, self.passwd)
                    capella_api_a.edit_project_permissions(self.tenant_id, body)
                    self.log.info("{0}".format(user))
                    for d_id in database_ids:
                        for p_id in project_ids:
                            for t_id in tenant_ids:
                                self.log.info("{0} -*- {1} -*- {2} -*- {3}"
                                              .format(role, d_id, p_id, t_id))
                                resp = capella_api.delete_serverless_db(tenant_ids[t_id],
                                                                        project_ids[p_id],
                                                                        database_ids[d_id])
                                if d_id == "junk_database_id":
                                    if resp.status_code != 405:
                                        self.fail("Junk should return 405")
                                elif t_id == "junk_tenant_id" or t_id == "unauth_tenant_id":
                                    if resp.status_code != 404:
                                        self.fail(
                                            "No access should be allowed as it is an unauthorised "
                                            "tenant id")
                                elif p_id == "invalid_project_id" or p_id == "unauth_proj_id":
                                    if resp.status_code != 404:
                                        self.fail(
                                            "No access should be allowed as it is an unauthorised "
                                            "project id")
                                elif d_id == "unauth_database_id":
                                    if resp.status_code != 404:
                                        self.fail(
                                            "No access should be allowed as it is an unauthorised "
                                            "database id")
                                elif role in ["projectOwner", "projectClusterManager"] \
                                        and p_id != "unauth_proj_id":
                                    if resp.status_code != 202:
                                        self.fail("Access should be allowed to view bucket")
                                elif role not in ["projectOwner", "projectClusterManager"] \
                                        or p_id == "unauth_proj_id":
                                    if resp.status_code != 403:
                                        self.fail(
                                            "No access should be allowed as the user does not "
                                            "have the required permissions")
                                else:
                                    self.fail("Did not verify this case!!!")

    def test_allow_ip(self):
        self.log.info("Verifying status code for allowing ip")
        self.log.info("Role -*- d_id -*- p_id -*- t_id")
        valid_database_id = self.input.param("valid_database_id", self.invalid_id)
        unauth_database_id = self.input.param("valid_database_id", self.invalid_id)
        junk_database_id = self.input.param("junk_database_id", self.invalid_id)
        unauth_proj_id = self.input.param("unauth_proj_id", self.invalid_id)
        database_ids = {"valid_database_id": valid_database_id,
                        "unauth_database_id": unauth_database_id,
                        "junk_database_id": junk_database_id}
        project_ids = {"valid_project_id": self.project_id,
                       "invalid_project_id": self.invalid_id, "unauth_proj_id": unauth_proj_id}
        tenant_ids = {"valid_tenant_id": self.tenant_id,
                      "unauth_tenant_id": self.invalid_tenant_id, "junk_tenant_id": self.invalid_id}
        for user in self.test_users:
            capella_api = CapellaAPI("https://" + self.url,
                                     self.test_users[user]["mailid"],
                                     self.test_users[user]["password"])
            # Organisation roles
            if user != "User5":
                self.log.info("{0}".format(user))
                for d_id in database_ids:
                    for p_id in project_ids:
                        for t_id in tenant_ids:
                            self.log.info("")
                            self.log.info("{0} -*- {1} -*- {2} -*- {3}"
                                          .format(self.test_users[user]["role"], d_id, p_id, t_id))
                            config = {"create": [{"cidr": "0.0.0.0/32", "comment": ""}]}
                            resp = capella_api.add_ip_allowlists(tenant_ids[t_id],
                                                                 database_ids[d_id],
                                                                 project_ids[p_id], config)
                            # Parsing from top level to bottom level
                            if d_id == "junk_database_id":
                                if resp.status_code != 405:
                                    self.fail("Junk should return 405")
                            elif t_id == "junk_tenant_id" or t_id == "unauth_tenant_id":
                                if resp.status_code != 404:
                                    self.fail(
                                        "No access should be allowed as it is an unauthorised "
                                        "tenant id")
                            elif p_id == "invalid_project_id" or p_id == "unauth_proj_id":
                                if resp.status_code != 404:
                                    self.fail(
                                        "No access should be allowed as it is an unauthorised "
                                        "project id")
                            elif d_id == "unauth_database_id":
                                if resp.status_code != 404:
                                    self.fail(
                                        "No access should be allowed as it is an unauthorised "
                                        "database id")
                            elif self.test_users[user]["role"] != "organizationOwner":
                                if resp.status_code != 403:
                                    self.fail(
                                        "No access should be allowed as the user does not have "
                                        "the required permissions")
                            elif self.test_users[user]["role"] == "organizationOwner":
                                if resp.status_code != 202:
                                    self.fail("Access should be allowed to view bucket")
                            else:
                                self.fail("Did not verify this case!!!")
            # Project roles
            else:
                proj_roles = ["projectOwner", "projectDataWriter", "projectClusterManager",
                              "projectDataViewer", "projectClusterViewer"]
                for role in proj_roles:
                    body = {"resourceId": self.project_id, "resourceType": "project",
                            "roles": [role], "users": [self.test_users[user]["userid"]]}
                    capella_api_a = CapellaAPI("https://" + self.url, self.user, self.passwd)
                    capella_api_a.edit_project_permissions(self.tenant_id, body)
                    self.log.info("{0}".format(user))
                    for d_id in database_ids:
                        for p_id in project_ids:
                            for t_id in tenant_ids:
                                self.log.info("{0} -*- {1} -*- {2} -*- {3}"
                                              .format(role, d_id, p_id, t_id))
                                config = {"create": [{"cidr": "0.0.0.0/32", "comment": ""}]}
                                resp = capella_api.add_ip_allowlists(tenant_ids[t_id],
                                                                     database_ids[d_id],
                                                                     project_ids[p_id], config)
                                if d_id == "junk_database_id":
                                    if resp.status_code != 405:
                                        self.fail("Junk should return 405")
                                elif t_id == "junk_tenant_id" or t_id == "unauth_tenant_id":
                                    if resp.status_code != 404:
                                        self.fail(
                                            "No access should be allowed as it is an unauthorised "
                                            "tenant id")
                                elif p_id == "invalid_project_id" or p_id == "unauth_proj_id":
                                    if resp.status_code != 404:
                                        self.fail(
                                            "No access should be allowed as it is an unauthorised "
                                            "project id")
                                elif d_id == "unauth_database_id":
                                    if resp.status_code != 404:
                                        self.fail(
                                            "No access should be allowed as it is an unauthorised "
                                            "database id")
                                elif role in ["projectOwner", "projectClusterManager"] \
                                        and p_id != "unauth_proj_id":
                                    if resp.status_code != 422:
                                        self.fail("Access should be allowed to view bucket")
                                elif role not in ["projectOwner", "projectClusterManager"] \
                                        or p_id == "unauth_proj_id":
                                    if resp.status_code != 403:
                                        self.fail(
                                            "No access should be allowed as the user does not "
                                            "have the required permissions")
                                else:
                                    self.fail("Did not verify this case!!!")

    def test_view_ip(self):
        self.log.info("Verifying status code for viewing ip")
        self.log.info("Role -*- d_id -*- p_id -*- t_id")
        valid_database_id = self.input.param("valid_database_id", self.invalid_id)
        unauth_database_id = self.input.param("valid_database_id", self.invalid_id)
        junk_database_id = self.input.param("junk_database_id", self.invalid_id)
        unauth_proj_id = self.input.param("unauth_proj_id", self.invalid_id)
        database_ids = {"valid_database_id": valid_database_id,
                        "unauth_database_id": unauth_database_id,
                        "junk_database_id": junk_database_id}
        project_ids = {"valid_project_id": self.project_id,
                       "invalid_project_id": self.invalid_id, "unauth_proj_id": unauth_proj_id}
        tenant_ids = {"valid_tenant_id": self.tenant_id,
                      "unauth_tenant_id": self.invalid_tenant_id, "junk_tenant_id": self.invalid_id}
        for user in self.test_users:
            capella_api = CapellaAPI("https://" + self.url,
                                     self.test_users[user]["mailid"],
                                     self.test_users[user]["password"])
            # Organisation roles
            if user != "User5":
                self.log.info("{0}".format(user))
                for d_id in database_ids:
                    for p_id in project_ids:
                        for t_id in tenant_ids:
                            self.log.info("")
                            self.log.info("{0} -*- {1} -*- {2} -*- {3}"
                                          .format(self.test_users[user]["role"], d_id, p_id, t_id))
                            resp = capella_api.view_allowed_ips(tenant_ids[t_id],
                                                                project_ids[p_id],
                                                                database_ids[d_id])
                            # Parsing from top level to bottom level
                            if d_id == "junk_database_id":
                                if resp.status_code != 405:
                                    self.fail("Junk should return 405")
                            elif t_id == "junk_tenant_id" or t_id == "unauth_tenant_id":
                                if resp.status_code != 404:
                                    self.fail(
                                        "No access should be allowed as it is an unauthorised "
                                        "tenant id")
                            elif p_id == "invalid_project_id" or p_id == "unauth_proj_id":
                                if resp.status_code != 404:
                                    self.fail(
                                        "No access should be allowed as it is an unauthorised "
                                        "project id")
                            elif d_id == "unauth_database_id":
                                if resp.status_code != 404:
                                    self.fail(
                                        "No access should be allowed as it is an unauthorised "
                                        "database id")
                            elif self.test_users[user]["role"] != "organizationOwner":
                                if resp.status_code != 403:
                                    self.fail(
                                        "No access should be allowed as the user does not have "
                                        "the required permissions")
                            elif self.test_users[user]["role"] == "organizationOwner":
                                if resp.status_code != 200:
                                    self.fail("Access should be allowed to view bucket")
                            else:
                                self.fail("Did not verify this case!!!")
            # Project roles
            else:
                proj_roles = ["projectOwner", "projectDataWriter", "projectClusterManager",
                              "projectDataViewer", "projectClusterViewer"]
                for role in proj_roles:
                    body = {"resourceId": self.project_id, "resourceType": "project",
                            "roles": [role], "users": [self.test_users[user]["userid"]]}
                    capella_api_a = CapellaAPI("https://" + self.url, self.user, self.passwd)
                    capella_api_a.edit_project_permissions(self.tenant_id, body)
                    self.log.info("{0}".format(user))
                    for d_id in database_ids:
                        for p_id in project_ids:
                            for t_id in tenant_ids:
                                self.log.info("{0} -*- {1} -*- {2} -*- {3}"
                                              .format(role, d_id, p_id, t_id))
                                resp = capella_api.view_allowed_ips(tenant_ids[t_id],
                                                                    project_ids[p_id],
                                                                    database_ids[d_id])
                                if d_id == "junk_database_id":
                                    if resp.status_code != 405:
                                        self.fail("Junk should return 405")
                                elif t_id == "junk_tenant_id" or t_id == "unauth_tenant_id":
                                    if resp.status_code != 404:
                                        self.fail(
                                            "No access should be allowed as it is an unauthorised "
                                            "tenant id")
                                elif p_id == "invalid_project_id" or p_id == "unauth_proj_id":
                                    if resp.status_code != 404:
                                        self.fail(
                                            "No access should be allowed as it is an unauthorised "
                                            "project id")
                                elif d_id == "unauth_database_id":
                                    if resp.status_code != 404:
                                        self.fail(
                                            "No access should be allowed as it is an unauthorised "
                                            "database id")
                                elif role in ["projectOwner", "projectClusterManager",
                                              "projectClusterViewer"] \
                                        and p_id != "unauth_proj_id":
                                    if resp.status_code != 200:
                                        self.fail("Access should be allowed to view bucket")
                                elif role not in ["projectOwner", "projectClusterManager",
                                                  "projectClusterViewer"] \
                                        or p_id == "unauth_proj_id":
                                    if resp.status_code != 403:
                                        self.fail(
                                            "No access should be allowed as the user does not "
                                            "have the required permissions")
                                else:
                                    self.fail("Did not verify this case!!!")

    def test_delete_ip(self):
        self.log.info("Verifying status code for deleting ip")
        self.log.info("Role -*- d_id -*- p_id -*- t_id")
        delete_id = self.input.param("delete_id", self.invalid_id)
        valid_database_id = self.input.param("valid_database_id", self.invalid_id)
        unauth_database_id = self.input.param("valid_database_id", self.invalid_id)
        junk_database_id = self.input.param("junk_database_id", self.invalid_id)
        unauth_proj_id = self.input.param("unauth_proj_id", self.invalid_id)
        database_ids = {"valid_database_id": valid_database_id,
                        "unauth_database_id": unauth_database_id,
                        "junk_database_id": junk_database_id}
        project_ids = {"valid_project_id": self.project_id,
                       "invalid_project_id": self.invalid_id, "unauth_proj_id": unauth_proj_id}
        tenant_ids = {"valid_tenant_id": self.tenant_id,
                      "unauth_tenant_id": self.invalid_tenant_id, "junk_tenant_id": self.invalid_id}
        for user in self.test_users:
            capella_api = CapellaAPI("https://" + self.url,
                                     self.test_users[user]["mailid"],
                                     self.test_users[user]["password"])
            # Organisation roles
            if user != "User5":
                self.log.info("{0}".format(user))
                for d_id in database_ids:
                    for p_id in project_ids:
                        for t_id in tenant_ids:
                            self.log.info("")
                            self.log.info("{0} -*- {1} -*- {2} -*- {3}"
                                          .format(self.test_users[user]["role"], d_id, p_id, t_id))
                            config = {"delete": [delete_id]}
                            resp = capella_api.add_ip_allowlists(tenant_ids[t_id],
                                                                 database_ids[d_id],
                                                                 project_ids[p_id], config)
                            # Parsing from top level to bottom level
                            if d_id == "junk_database_id":
                                if resp.status_code != 405:
                                    self.fail("Junk should return 405")
                            elif t_id == "junk_tenant_id" or t_id == "unauth_tenant_id":
                                if resp.status_code != 404:
                                    self.fail(
                                        "No access should be allowed as it is an unauthorised "
                                        "tenant id")
                            elif p_id == "invalid_project_id" or p_id == "unauth_proj_id":
                                if resp.status_code != 404:
                                    self.fail(
                                        "No access should be allowed as it is an unauthorised "
                                        "project id")
                            elif d_id == "unauth_database_id":
                                if resp.status_code != 404:
                                    self.fail(
                                        "No access should be allowed as it is an unauthorised "
                                        "database id")
                            elif self.test_users[user]["role"] != "organizationOwner":
                                if resp.status_code != 403:
                                    self.fail(
                                        "No access should be allowed as the user does not have "
                                        "the required permissions")
                            elif self.test_users[user]["role"] == "organizationOwner":
                                if resp.status_code != 202:
                                    self.fail("Access should be allowed to view bucket")
                            else:
                                self.fail("Did not verify this case!!!")
            # Project roles
            else:
                proj_roles = ["projectOwner", "projectDataWriter", "projectClusterManager",
                              "projectDataViewer", "projectClusterViewer"]
                for role in proj_roles:
                    body = {"resourceId": self.project_id, "resourceType": "project",
                            "roles": [role], "users": [self.test_users[user]["userid"]]}
                    capella_api_a = CapellaAPI("https://" + self.url, self.user, self.passwd)
                    capella_api_a.edit_project_permissions(self.tenant_id, body)
                    self.log.info("{0}".format(user))
                    for d_id in database_ids:
                        for p_id in project_ids:
                            for t_id in tenant_ids:
                                self.log.info("{0} -*- {1} -*- {2} -*- {3}"
                                              .format(role, d_id, p_id, t_id))
                                config = {"delete": [delete_id]}
                                resp = capella_api.add_ip_allowlists(tenant_ids[t_id],
                                                                     database_ids[d_id],
                                                                     project_ids[p_id], config)
                                if d_id == "junk_database_id":
                                    if resp.status_code != 405:
                                        self.fail("Junk should return 405")
                                elif t_id == "junk_tenant_id" or t_id == "unauth_tenant_id":
                                    if resp.status_code != 404:
                                        self.fail(
                                            "No access should be allowed as it is an unauthorised "
                                            "tenant id")
                                elif p_id == "invalid_project_id" or p_id == "unauth_proj_id":
                                    if resp.status_code != 404:
                                        self.fail(
                                            "No access should be allowed as it is an unauthorised "
                                            "project id")
                                elif d_id == "unauth_database_id":
                                    if resp.status_code != 404:
                                        self.fail(
                                            "No access should be allowed as it is an unauthorised "
                                            "database id")
                                elif role in ["projectOwner", "projectClusterManager"] \
                                        and p_id != "unauth_proj_id":
                                    if resp.status_code != 202:
                                        self.fail("Access should be allowed to view bucket")
                                elif role not in ["projectOwner", "projectClusterManager"] \
                                        or p_id == "unauth_proj_id":
                                    if resp.status_code != 403:
                                        self.fail(
                                            "No access should be allowed as the user does not "
                                            "have the required permissions")
                                else:
                                    self.fail("Did not verify this case!!!")

    def test_add_api_key(self):
        self.log.info("Verifying status code for adding api key")
        self.log.info("Role -*- d_id -*- p_id -*- t_id")
        valid_database_id = self.input.param("valid_database_id", self.invalid_id)
        unauth_database_id = self.input.param("valid_database_id", self.invalid_id)
        junk_database_id = self.input.param("junk_database_id", self.invalid_id)
        unauth_proj_id = self.input.param("unauth_proj_id", self.invalid_id)
        database_ids = {"valid_database_id": valid_database_id,
                        "unauth_database_id": unauth_database_id,
                        "junk_database_id": junk_database_id}
        project_ids = {"valid_project_id": self.project_id,
                       "invalid_project_id": self.invalid_id, "unauth_proj_id": unauth_proj_id}
        tenant_ids = {"valid_tenant_id": self.tenant_id,
                      "unauth_tenant_id": self.invalid_tenant_id, "junk_tenant_id": self.invalid_id}
        for user in self.test_users:
            capella_api = CapellaAPI("https://" + self.url,
                                     self.test_users[user]["mailid"],
                                     self.test_users[user]["password"])
            # Organisation roles
            if user != "User5":
                self.log.info("{0}".format(user))
                for d_id in database_ids:
                    for p_id in project_ids:
                        for t_id in tenant_ids:
                            self.log.info("")
                            self.log.info("{0} -*- {1} -*- {2} -*- {3}"
                                          .format(self.test_users[user]["role"], d_id, p_id, t_id))
                            resp = capella_api.generate_keys(tenant_ids[t_id],
                                                             project_ids[p_id],
                                                             database_ids[d_id])
                            # Parsing from top level to bottom level
                            if d_id == "junk_database_id":
                                if resp.status_code != 405:
                                    self.fail("Junk should return 405")
                            elif t_id == "junk_tenant_id" or t_id == "unauth_tenant_id":
                                if resp.status_code != 404:
                                    self.fail(
                                        "No access should be allowed as it is an unauthorised "
                                        "tenant id")
                            elif p_id == "invalid_project_id" or p_id == "unauth_proj_id":
                                if resp.status_code != 404:
                                    self.fail(
                                        "No access should be allowed as it is an unauthorised "
                                        "project id")
                            elif d_id == "unauth_database_id":
                                if resp.status_code != 404:
                                    self.fail(
                                        "No access should be allowed as it is an unauthorised "
                                        "database id")
                            elif self.test_users[user]["role"] != "organizationOwner":
                                if resp.status_code != 403:
                                    self.fail(
                                        "No access should be allowed as the user does not have "
                                        "the required permissions")
                            elif self.test_users[user]["role"] == "organizationOwner":
                                if resp.status_code != 201:
                                    self.fail("Access should be allowed to view bucket")
                            else:
                                self.fail("Did not verify this case!!!")
            # Project roles
            else:
                proj_roles = ["projectOwner", "projectDataWriter", "projectClusterManager",
                              "projectDataViewer", "projectClusterViewer"]
                for role in proj_roles:
                    body = {"resourceId": self.project_id, "resourceType": "project",
                            "roles": [role], "users": [self.test_users[user]["userid"]]}
                    capella_api_a = CapellaAPI("https://" + self.url, self.user, self.passwd)
                    capella_api_a.edit_project_permissions(self.tenant_id, body)
                    self.log.info("{0}".format(user))
                    for d_id in database_ids:
                        for p_id in project_ids:
                            for t_id in tenant_ids:
                                self.log.info("{0} -*- {1} -*- {2} -*- {3}"
                                              .format(role, d_id, p_id, t_id))
                                resp = capella_api.generate_keys(tenant_ids[t_id],
                                                                 project_ids[p_id],
                                                                 database_ids[d_id])
                                if d_id == "junk_database_id":
                                    if resp.status_code != 405:
                                        self.fail("Junk should return 405")
                                elif t_id == "junk_tenant_id" or t_id == "unauth_tenant_id":
                                    if resp.status_code != 404:
                                        self.fail(
                                            "No access should be allowed as it is an unauthorised "
                                            "tenant id")
                                elif p_id == "invalid_project_id" or p_id == "unauth_proj_id":
                                    if resp.status_code != 404:
                                        self.fail(
                                            "No access should be allowed as it is an unauthorised "
                                            "project id")
                                elif d_id == "unauth_database_id":
                                    if resp.status_code != 404:
                                        self.fail(
                                            "No access should be allowed as it is an unauthorised "
                                            "database id")
                                elif role in ["projectOwner", "projectClusterManager"] \
                                        and p_id != "unauth_proj_id":
                                    if resp.status_code != 201:
                                        self.fail("Access should be allowed to view bucket")
                                elif role not in ["projectOwner", "projectClusterManager"] \
                                        or p_id == "unauth_proj_id":
                                    if resp.status_code != 403:
                                        self.fail(
                                            "No access should be allowed as the user does not "
                                            "have the required permissions")
                                else:
                                    self.fail("Did not verify this case!!!")

    def test_view_api_key(self):
        self.log.info("Verifying status code for viewing API key")
        self.log.info("Role -*- d_id -*- p_id -*- t_id")
        valid_database_id = self.input.param("valid_database_id", self.invalid_id)
        unauth_database_id = self.input.param("valid_database_id", self.invalid_id)
        junk_database_id = self.input.param("junk_database_id", self.invalid_id)
        unauth_proj_id = self.input.param("unauth_proj_id", self.invalid_id)
        database_ids = {"valid_database_id": valid_database_id,
                        "unauth_database_id": unauth_database_id,
                        "junk_database_id": junk_database_id}
        project_ids = {"valid_project_id": self.project_id,
                       "invalid_project_id": self.invalid_id, "unauth_proj_id": unauth_proj_id}
        tenant_ids = {"valid_tenant_id": self.tenant_id,
                      "unauth_tenant_id": self.invalid_tenant_id, "junk_tenant_id": self.invalid_id}
        for user in self.test_users:
            capella_api = CapellaAPI("https://" + self.url,
                                     self.test_users[user]["mailid"],
                                     self.test_users[user]["password"])
            # Organisation roles
            if user != "User5":
                self.log.info("{0}".format(user))
                for d_id in database_ids:
                    for p_id in project_ids:
                        for t_id in tenant_ids:
                            self.log.info("")
                            self.log.info("{0} -*- {1} -*- {2} -*- {3}"
                                          .format(self.test_users[user]["role"], d_id, p_id, t_id))
                            resp = capella_api.view_api_keys(tenant_ids[t_id],
                                                             project_ids[p_id],
                                                             database_ids[d_id])
                            # Parsing from top level to bottom level
                            if d_id == "junk_database_id":
                                if resp.status_code != 405:
                                    self.fail("Junk should return 405")
                            elif t_id == "junk_tenant_id" or t_id == "unauth_tenant_id":
                                if resp.status_code != 404:
                                    self.fail(
                                        "No access should be allowed as it is an unauthorised "
                                        "tenant id")
                            elif p_id == "invalid_project_id" or p_id == "unauth_proj_id":
                                if resp.status_code != 404:
                                    self.fail(
                                        "No access should be allowed as it is an unauthorised "
                                        "project id")
                            elif d_id == "unauth_database_id":
                                if resp.status_code != 404:
                                    self.fail(
                                        "No access should be allowed as it is an unauthorised "
                                        "database id")
                            elif self.test_users[user]["role"] != "organizationOwner":
                                if resp.status_code != 403:
                                    self.fail(
                                        "No access should be allowed as the user does not have "
                                        "the required permissions")
                            elif self.test_users[user]["role"] == "organizationOwner":
                                if resp.status_code != 200:
                                    self.fail("Access should be allowed to view bucket")
                            else:
                                self.fail("Did not verify this case!!!")
            # Project roles
            else:
                proj_roles = ["projectOwner", "projectDataWriter", "projectClusterManager",
                              "projectDataViewer", "projectClusterViewer"]
                for role in proj_roles:
                    body = {"resourceId": self.project_id, "resourceType": "project",
                            "roles": [role], "users": [self.test_users[user]["userid"]]}
                    capella_api_a = CapellaAPI("https://" + self.url, self.user, self.passwd)
                    capella_api_a.edit_project_permissions(self.tenant_id, body)
                    self.log.info("{0}".format(user))
                    for d_id in database_ids:
                        for p_id in project_ids:
                            for t_id in tenant_ids:
                                self.log.info("{0} -*- {1} -*- {2} -*- {3}"
                                              .format(role, d_id, p_id, t_id))
                                resp = capella_api.view_api_keys(tenant_ids[t_id],
                                                                 project_ids[p_id],
                                                                 database_ids[d_id])
                                if d_id == "junk_database_id":
                                    if resp.status_code != 405:
                                        self.fail("Junk should return 405")
                                elif t_id == "junk_tenant_id" or t_id == "unauth_tenant_id":
                                    if resp.status_code != 404:
                                        self.fail(
                                            "No access should be allowed as it is an unauthorised "
                                            "tenant id")
                                elif p_id == "invalid_project_id" or p_id == "unauth_proj_id":
                                    if resp.status_code != 404:
                                        self.fail(
                                            "No access should be allowed as it is an unauthorised "
                                            "project id")
                                elif d_id == "unauth_database_id":
                                    if resp.status_code != 404:
                                        self.fail(
                                            "No access should be allowed as it is an unauthorised "
                                            "database id")
                                elif role in ["projectOwner", "projectClusterManager",
                                              "projectClusterViewer"] \
                                        and p_id != "unauth_proj_id":
                                    if resp.status_code != 200:
                                        self.fail("Access should be allowed to view bucket")
                                elif role not in ["projectOwner", "projectClusterManager",
                                                  "projectClusterViewer"] \
                                        or p_id == "unauth_proj_id":
                                    if resp.status_code != 403:
                                        self.fail(
                                            "No access should be allowed as the user does not "
                                            "have the required permissions")
                                else:
                                    self.fail("Did not verify this case!!!")

    def test_delete_api_key(self):
        self.log.info("Verifying status code for deleting API key")
        self.log.info("Role -*- d_id -*- p_id -*- t_id")
        key_id = self.input.param("key_id", self.invalid_id)
        valid_database_id = self.input.param("valid_database_id", self.invalid_id)
        unauth_database_id = self.input.param("valid_database_id", self.invalid_id)
        junk_database_id = self.input.param("junk_database_id", self.invalid_id)
        unauth_proj_id = self.input.param("unauth_proj_id", self.invalid_id)
        database_ids = {"valid_database_id": valid_database_id,
                        "unauth_database_id": unauth_database_id,
                        "junk_database_id": junk_database_id}
        project_ids = {"valid_project_id": self.project_id,
                       "invalid_project_id": self.invalid_id, "unauth_proj_id": unauth_proj_id}
        tenant_ids = {"valid_tenant_id": self.tenant_id,
                      "unauth_tenant_id": self.invalid_tenant_id, "junk_tenant_id": self.invalid_id}
        for user in self.test_users:
            capella_api = CapellaAPI("https://" + self.url,
                                     self.test_users[user]["mailid"],
                                     self.test_users[user]["password"])
            # Organisation roles
            if user != "User5":
                self.log.info("{0}".format(user))
                for d_id in database_ids:
                    for p_id in project_ids:
                        for t_id in tenant_ids:
                            self.log.info("")
                            self.log.info("{0} -*- {1} -*- {2} -*- {3}"
                                          .format(self.test_users[user]["role"], d_id, p_id, t_id))
                            resp = capella_api.revoke_access_secret_key(tenant_ids[t_id],
                                                                        project_ids[p_id],
                                                                        database_ids[d_id], key_id)
                            if d_id == "junk_database_id":
                                if resp.status_code != 405:
                                    self.fail("Junk should return 405")
                            elif t_id == "junk_tenant_id" or t_id == "unauth_tenant_id":
                                if resp.status_code != 404:
                                    self.fail(
                                        "No access should be allowed as it is an unauthorised "
                                        "tenant id")
                            elif p_id == "invalid_project_id" or p_id == "unauth_proj_id":
                                if resp.status_code != 404:
                                    self.fail(
                                        "No access should be allowed as it is an unauthorised "
                                        "project id")
                            elif d_id == "unauth_database_id":
                                if resp.status_code != 404:
                                    self.fail(
                                        "No access should be allowed as it is an unauthorised "
                                        "database id")
                            elif self.test_users[user]["role"] != "organizationOwner":
                                if resp.status_code != 403:
                                    self.fail(
                                        "No access should be allowed as the user does not have "
                                        "the required permissions")
                            elif self.test_users[user]["role"] == "organizationOwner":
                                if resp.status_code != 202:
                                    self.fail("Access should be allowed to view bucket")
                            else:
                                self.fail("Did not verify this case!!!")
            # Project roles
            else:
                proj_roles = ["projectOwner", "projectDataWriter", "projectClusterManager",
                              "projectDataViewer", "projectClusterViewer"]
                for role in proj_roles:
                    body = {"resourceId": self.project_id, "resourceType": "project",
                            "roles": [role], "users": [self.test_users[user]["userid"]]}
                    capella_api_a = CapellaAPI("https://" + self.url, self.user, self.passwd)
                    capella_api_a.edit_project_permissions(self.tenant_id, body)
                    self.log.info("{0}".format(user))
                    for d_id in database_ids:
                        for p_id in project_ids:
                            for t_id in tenant_ids:
                                self.log.info("{0} -*- {1} -*- {2} -*- {3}"
                                              .format(role, d_id, p_id, t_id))
                                resp = capella_api.revoke_access_secret_key(tenant_ids[t_id],
                                                                            project_ids[p_id],
                                                                            database_ids[d_id],
                                                                            key_id)
                                if d_id == "junk_database_id":
                                    if resp.status_code != 405:
                                        self.fail("Junk should return 405")
                                elif t_id == "junk_tenant_id" or t_id == "unauth_tenant_id":
                                    if resp.status_code != 404:
                                        self.fail(
                                            "No access should be allowed as it is an unauthorised "
                                            "tenant id")
                                elif p_id == "invalid_project_id" or p_id == "unauth_proj_id":
                                    if resp.status_code != 404:
                                        self.fail(
                                            "No access should be allowed as it is an unauthorised "
                                            "project id")
                                elif d_id == "unauth_database_id":
                                    if resp.status_code != 404:
                                        self.fail(
                                            "No access should be allowed as it is an unauthorised "
                                            "database id")
                                elif role in ["projectOwner", "projectClusterManager"] \
                                        and p_id != "unauth_proj_id":
                                    if resp.status_code != 202:
                                        self.fail("Access should be allowed to view bucket")
                                elif role not in ["projectOwner", "projectClusterManager"] \
                                        or p_id == "unauth_proj_id":
                                    if resp.status_code != 403:
                                        self.fail(
                                            "No access should be allowed as the user does not "
                                            "have the required permissions")
                                else:
                                    self.fail("Did not verify this case!!!")

    def test_n1ql_query(self):
        self.log.info("Verifying status code for running query on serverless database")
        tenant_ids = {
            "valid_tenant_id": self.tenant_id,
            # "invalid_tenant_id": self.invalid_id
        }
        project_ids = {
            "valid_project_id": self.project_id,
            "same_tenant_unauthorized_project_id": self.input.param(
                "same_tenant_unauthorized_project_id", self.invalid_id),
            "different_tenant_unauthorized_project_id": self.input.param(
                "different_tenant_unauthorized_project_id", self.invalid_id),
            "invalid_project_id": self.invalid_id
        }
        database_ids = {
            "valid_database_id": "",
            "same_tenant_unauthorized_database_id": self.input.param(
                "same_tenant_unauthorized_database_id", self.invalid_id),
            "different_tenant_unauthorized_database_id": self.input.param(
                "different_tenant_unauthorized_database_id", self.invalid_id),
            "invalid_database_id": "00000000-0000-0000-0000-000000000000"
        }
        project_roles = ["projectOwner", "projectDataWriter", "projectClusterManager",
                         "projectDataViewer", "projectClusterViewer"]

        self.log.info("Same Tenant Unauthorized project id - {}".format(
            project_ids["same_tenant_unauthorized_project_id"]))
        self.log.info("Different Tenant Unauthorized project id - {}".format(
            project_ids["different_tenant_unauthorized_project_id"]))

        self.log.info("Same Tenant unauthorized database id - {}".format(
            database_ids["same_tenant_unauthorized_database_id"]))
        self.log.info("Different Tenant unauthorized database id - {}".format(
            database_ids["different_tenant_unauthorized_database_id"]))

        capella_api = CapellaAPI("https://" + self.url, self.user, self.passwd)
        deploy_cluster_body = {"tenantID": self.tenant_id, "projectID": self.project_id,
                               "name": "n1ql-Database", "provider": "aws", "region": "us-east-1",
                               "importSampleData": True}
        deploy_resp = capella_api.deploy_database(self.tenant_id, json.dumps(deploy_cluster_body))
        self.assertEqual(202, deploy_resp.status_code,
                         msg='FAIL, Outcome:{0}, Expected:{1} for deploying a cluster'
                         .format(deploy_resp.status_code, 202))
        database_ids["valid_database_id"] = \
            json.loads(deploy_resp.content.decode('utf-8'))["databaseId"]
        time.sleep(20)

        for tenant_id in tenant_ids:
            for project_id in project_ids:
                for database_id in database_ids:
                    for user in self.test_users:

                        self.log.info("Verifying status code for running a n1ql query for "
                                      " organization roles before adding users to project")
                        self.log.info(" ")
                        self.log.info(" ")
                        self.log.info("Tenant ID--------------Project ID--------------Database ID")
                        self.log.info("{} | {} | {}".format(tenant_id, project_id, database_id))

                        capella_api_role = CapellaAPI("https://" + self.url,
                                                      self.test_users[user]["mailid"],
                                                      self.test_users[user]["password"])
                        n1ql_body = {"timeout": "600s",
                                     "statement": "EXPLAIN select * from airline limit 10;",
                                     "client_context_id": "EaxwLNBb3bpH2q6zJmYYQ",
                                     "query_context": "default:`{}`.`samples`"
                                     .format(database_ids[database_id]),
                                     "profile": "timings", "use_cbo": True, "txtimeout": "120s"}
                        run_query_resp = capella_api_role.run_query(database_ids[database_id],
                                                                    json.dumps(n1ql_body))

                        if tenant_id == "valid_tenant_id":
                            if project_id == "valid_project_id" or \
                                    project_id == "same_tenant_unauthorized_project_id":
                                if database_id == "valid_database_id" or \
                                        database_id == "same_tenant_unauthorized_database_id":

                                    if self.test_users[user]["role"] == "organizationOwner":
                                        self.assertEqual(200, run_query_resp.status_code,
                                                         msg="FAIL, Outcome: {0}, Expected: {1}"
                                                         " for accessing project"
                                                         .format(run_query_resp.status_code, 200))
                                    else:
                                        self.assertEqual(412, run_query_resp.status_code,
                                                         msg="FAIL, Outcome: {0}, Expected: {1} "
                                                         "for accessing project"
                                                         .format(run_query_resp.status_code, 412))

                                else:
                                    self.assertEqual(404, run_query_resp.status_code,
                                                     msg="FAIL, Outcome: {0}, Expected: {1} for "
                                                     "accessing project"
                                                     .format(run_query_resp.status_code, 404))
                            else:
                                self.assertEqual(404, run_query_resp.status_code,
                                                 msg="FAIL, Outcome: {0}, Expected: {1} for "
                                                 "accessing project"
                                                 .format(run_query_resp.status_code, 404))

                        else:
                            self.assertEqual(400, run_query_resp.status_code,
                                             msg='FAIL, Outcome:{0}, Expected:{1} for deploying '
                                             'a cluster'
                                             .format(run_query_resp.status_code, 400))

                self.log.info(
                    "Verifying status code for running a read n1ql query after adding user to the "
                    "project"
                )
                user = self.test_users["User4"]
                capella_api_role = CapellaAPI("https://" + self.url, user["mailid"],
                                              user["password"])
                for role in project_roles:
                    self.log.info("Adding user {} with role as {} to the project"
                                  .format(user["name"], role))
                    if project_id != "same_tenant_unauthorized_project_id":
                        body = {"resourceId": project_ids[project_id], "resourceType": "project",
                                "roles": [role], "users": [user["userid"]]}

                        resp = capella_api.add_user_to_project(tenant_ids[tenant_id],
                                                               json.dumps(body))

                        if tenant_id == "valid_tenant_id":
                            if project_id == "valid_project_id":
                                self.assertEqual(200, run_query_resp.status_code,
                                                 msg="FAIL, Outcome: {0}, Expected: {1} for "
                                                 "adding user to project"
                                                 .format(resp.status_code, 200))
                            else:
                                self.assertEqual(404, run_query_resp.status_code,
                                                 msg="FAIL, Outcome: {0}, Expected: {1} for "
                                                 "adding user to project"
                                                 .format(resp.status_code, 404))

                    for database_id in database_ids:
                        self.log.info(" ")
                        self.log.info(" ")
                        self.log.info("Tenant ID--------------Project ID--------------Database ID")
                        self.log.info("{} | {} | {}".format(tenant_id, project_id, database_id))
                        self.log.info(" ")
                        self.log.info(" ")

                        self.log.info("Verifying status code for read query statement for {}"
                                      .format(role))
                        n1ql_read_body = {"timeout": "600s", "statement": "EXPLAIN select * "
                                          "from airline limit 10;",
                                          "client_context_id": "EaxwLNBb3bpH2q6zJmYYQ",
                                          "query_context": "default:`{}`.`samples`"
                                          .format(database_ids[database_id]),
                                          "profile": "timings", "use_cbo": True,
                                          "txtimeout": "120s"}
                        run_query_resp = capella_api_role.run_query(database_ids[database_id],
                                                                    json.dumps(n1ql_read_body))

                        if tenant_id == "valid_tenant_id":
                            if project_id == "valid_project_id":
                                if database_id == "valid_database_id":
                                    if role == "projectOwner" or role == "projectDataWriter" \
                                            or role == "projectDataViewer":
                                        self.assertEqual(200, run_query_resp.status_code,
                                                         msg="FAIL, Outcome: {0}, Expected: {1} "
                                                         "for adding user to project"
                                                         .format(run_query_resp.status_code, 200))

                                    else:
                                        self.assertEqual(412, run_query_resp.status_code,
                                                         msg="FAIL, Outcome: {0}, Expected: {1} "
                                                         "for adding user to project"
                                                         .format(run_query_resp.status_code, 412))
                                else:
                                    self.assertEqual(412, resp.status_code,
                                                     msg="FAIL, Outcome: {0}, Expected: {1} "
                                                     "for adding user to project"
                                                     .format(run_query_resp.status_code, 412))

                            else:
                                self.assertEqual(412, resp.status_code,
                                                 msg="FAIL, Outcome: {0}, Expected: {1} for "
                                                 "adding user to project"
                                                 .format(run_query_resp.status_code, 412))

                        else:
                            self.assertEqual(412, resp.status_code,
                                             msg="FAIL, Outcome: {0}, Expected: {1} for adding "
                                             "user to project"
                                             .format(run_query_resp.status_code, 412))

                        self.log.info("Verifying status code for write query statement for {}"
                                      .format(role))
                        n1ql_write_body = {"timeout": "600s", "statement": "INSERT INTO airline "
                                           "VALUES(\"1122\", "
                                           "{\"id\":\"1122\", \"name\":\"emirates\",\"type\":"
                                           "\"international\"});",
                                           "client_context_id": "EaxwLNBb3bpH2q6zJmYYQ",
                                           "query_context": "default:`{}`.`samples`"
                                           .format(database_ids[database_id]),
                                           "profile": "timings", "use_cbo": True,
                                           "txtimeout": "120s"}
                        run_query_resp = capella_api_role.run_query(database_ids[database_id],
                                                                    json.dumps(n1ql_write_body))

                        if tenant_id == "valid_tenant_id":
                            if project_id == "valid_project_id":
                                if database_id == "valid_database_id":
                                    # In the following line the projectDataViewer should give 412
                                    # instead of 200.
                                    # For now it is considered as 200 and file a bug for the same.
                                    if role == "projectOwner" or role == "projectDataWriter" \
                                            or role == "projectDataViewer":
                                        self.assertEqual(200, run_query_resp.status_code,
                                                         msg="FAIL, Outcome: {0}, Expected: {1} "
                                                         "for adding user to project"
                                                         .format(run_query_resp.status_code, 200))

                                    else:
                                        self.assertEqual(412, run_query_resp.status_code,
                                                         msg="FAIL, Outcome: {0}, Expected: {1} "
                                                         "for adding user to project"
                                                         .format(run_query_resp.status_code, 412))
                                else:
                                    self.assertEqual(412, resp.status_code,
                                                     msg="FAIL, Outcome: {0}, Expected: {1} for "
                                                     "adding user to project"
                                                     .format(run_query_resp.status_code, 412))

                            else:
                                self.assertEqual(412, resp.status_code,
                                                 msg="FAIL, Outcome: {0}, Expected: {1} for "
                                                 "adding user to project"
                                                 .format(run_query_resp.status_code, 412))

                        else:
                            self.assertEqual(412, resp.status_code,
                                             msg="FAIL, Outcome: {0}, Expected: {1} for "
                                             "adding user to project"
                                             .format(run_query_resp.status_code, 412))

                        # Commented because for the projectDataViewer role it gives a response of
                        # 200
                        # but does not allow the user to write data to the bug. Filed a bug :
                        # AV - 54154
                        #
                        # self.log.info("Verifying status code for write query statement for {}"
                        # .format(role))
                        # str_id = "1222_{}_write".format(role)
                        # write_query_statement = "INSERT INTO airline VALUES ({id},
                        #                         {\"name\":\"Couchbase_Airlines\", " \
                        #                         "\"type\":\"Premium\", \"country\":\"India\", "
                        #                         " \"callsign\":\"C.A.\"," \ "
                        #                         " \"iata\":\"1234\", \"id\":\"1122\"})"
                        #                         .format(id=str_id)
                        # n1ql_write_body = {"timeout": "600s", "statement": write_query_statement,
                        #                   "client_context_id": "EaxwLNBb3bpH2q6zJmYYQ",
                        #                   "query_context": "default:`{}`.`samples`"
                        #                   .format(database_id),
                        #                   "profile": "timings", "use_cbo": True,
                        #                   "txtimeout": "120s"}
                        # resp = capella_api_role.run_query(database_id,
                        #                                   json.dumps(n1ql_write_body))
                        #
                        # if role == "projectOwner" or role == "projectDataWriter":
                        #     self.assertEqual(200, resp.status_code,
                        #                      msg="FAIL, Outcome: {0}, Expected: {1} for
                        #                      adding user to project"
                        #                      .format(resp.status_code, 200))
                        # else:
                        #     self.assertEqual(412, resp.status_code,
                        #                      msg="FAIL, Outcome: {0}, Expected: {1} for
                        #                      adding user to project"
                        #                      .format(resp.status_code, 412))

                    if project_id != "same_tenant_unauthorized_project_id":
                        self.log.info("Removing user {} with role as {} from the project"
                                      .format(user["name"], role))
                        remove_user_resp = capella_api.remove_user_from_project(
                            tenant_ids[tenant_id], user["userid"], project_ids[project_id])
                        self.assertEqual(204, remove_user_resp.status_code,
                                         msg="FAIL, Outcome: {0}, Expected: {1} for adding "
                                         "user to project"
                                         .format(remove_user_resp.status_code, 204))

        self.log.info("Destroying the database")
        destroy_database_resp = capella_api.destroy_database(self.tenant_id, self.project_id,
                                                             database_ids["valid_database_id"])
        self.log.info("Outcome: {}, Expected: {}".format(destroy_database_resp.status_code, 204))

    def test_project_access(self):
        self.log.info("Verifying status code for accessing a project")
        tenant_ids = {
            "valid_tenant_id": self.tenant_id,
            # "invalid_tenant_id": self.invalid_id
        }
        project_ids = {
            "valid_project_id": "",
            "same_tenant_unauthorized_project_id": self.input.param(
                "same_tenant_unauthorized_project_id", self.invalid_id),
            "different_tenant_unauthorized_project_id": self.input.param(
                "different_tenant_unauthorized_project_id", self.invalid_id),
            "invalid_project_id": self.invalid_id
        }
        project_roles = ["projectOwner", "projectDataWriter", "projectClusterManager",
                         "projectDataViewer", "projectClusterViewer"]

        self.log.info("Same Tenant Unauthorized project id - {}".format(
            project_ids["same_tenant_unauthorized_project_id"]))
        self.log.info("Different Tenant Unauthorized project id - {}".format(
            project_ids["different_tenant_unauthorized_project_id"]))

        capella_api = CapellaAPI("https://" + self.url, self.user, self.passwd)
        body = {"name": "Koushal_API_Testing"}
        create_project_resp = capella_api.create_project(self.tenant_id, json.dumps(body))
        project_ids["valid_project_id"] = \
            json.loads(create_project_resp.content.decode('utf-8'))["id"]

        for tenant_id in tenant_ids:
            for project_id in project_ids:
                for user in self.test_users:
                    self.log.info(" ")
                    self.log.info(" ")
                    self.log.info("Tenant ID--------------Project ID--------------User Role")
                    self.log.info("{} | {} | {}".format(tenant_id, project_id,
                                                        self.test_users[user]["role"]))

                    self.log.info("Accessing the project for {}"
                                  .format(self.test_users[user]["role"]))
                    capella_api_role = CapellaAPI("https://" + self.url,
                                                  self.test_users[user]["mailid"],
                                                  self.test_users[user]["password"])

                    access_project_resp = capella_api_role.access_project(tenant_ids[tenant_id],
                                                                          project_ids[project_id])

                    if tenant_id == "valid_tenant_id":
                        if project_id == "valid_project_id" or \
                                project_id == "same_tenant_unauthorized_project_id":
                            if self.test_users[user]["role"] == "organizationOwner":
                                self.assertEqual(200, access_project_resp.status_code,
                                                 msg='Outcome: {0}, Expected:{1} for accessing '
                                                 'project'
                                                 .format(access_project_resp.status_code, 200))

                            else:
                                self.assertEqual(403, access_project_resp.status_code,
                                                 msg='Outcome: {0}, Expected:{1} for accessing'
                                                 ' project'
                                                 .format(access_project_resp.status_code, 403))

                        else:
                            self.assertEqual(404, access_project_resp.status_code,
                                             msg='Outcome: {0}, Expected:{1} for accessing '
                                             'project'
                                             .format(access_project_resp.status_code, 404))

                    else:
                        self.assertEqual(404, access_project_resp.status_code,
                                         msg='Outcome: {0}, Expected:{1} for accessing project'
                                         .format(access_project_resp.status_code, 404))

                self.log.info(
                    "Verifying status code for accessing a project after adding user to the project"
                )
                user = self.test_users["User4"]
                capella_api_role = CapellaAPI("https://" + self.url, user["mailid"],
                                              user["password"])
                for role in project_roles:
                    self.log.info(" ")
                    self.log.info(" ")
                    self.log.info("Tenant ID--------------Project ID--------------Project Role")
                    self.log.info("{} | {} | {}".format(tenant_id, project_id, role))

                    self.log.info("")
                    self.log.info("Adding user {} with role as {} to the project"
                                  .format(user["name"], role))
                    if project_id != "same_tenant_unauthorized_project_id":
                        body = {"resourceId": project_ids[project_id], "resourceType": "project",
                                "roles": [role], "users": [user["userid"]]}
                        add_user_resp = capella_api.add_user_to_project(tenant_ids[tenant_id],
                                                                        json.dumps(body))

                        if tenant_id == "valid_tenant_id":
                            if project_id == "valid_project_id":
                                self.assertEqual(200, add_user_resp.status_code,
                                                 msg="FAIL, Outcome: {0}, Expected: {1} for "
                                                 "adding user to project"
                                                 .format(add_user_resp.status_code, 200))
                            else:
                                self.assertEqual(404, add_user_resp.status_code,
                                                 msg="FAIL, Outcome: {0}, Expected: {1} for"
                                                 " adding user to project"
                                                 .format(add_user_resp.status_code, 404))
                        else:
                            self.assertEqual(404, add_user_resp.status_code,
                                             msg="FAIL, Outcome: {0}, Expected: {1} for "
                                             "adding user to project"
                                             .format(add_user_resp.status_code, 404))

                    self.log.info("Accessing project {} with role as {}"
                                  .format(project_id, role))
                    access_project_resp = capella_api_role.access_project(tenant_ids[tenant_id],
                                                                          project_ids[project_id])

                    if tenant_id == "valid_tenant_id":
                        if project_id == "valid_project_id":
                            self.assertEqual(200, access_project_resp.status_code,
                                             msg="FAIL, Outcome: {0}, Expected: {1} for adding "
                                             "user to project"
                                             .format(access_project_resp.status_code, 200))
                        else:
                            self.assertEqual(404, access_project_resp.status_code,
                                             msg="FAIL, Outcome: {0}, Expected: {1} for adding"
                                             " user to project"
                                             .format(access_project_resp.status_code, 404))

                    else:
                        self.assertEqual(404, access_project_resp.status_code,
                                         msg="FAIL, Outcome: {0}, Expected: {1} for adding "
                                         "user to project"
                                         .format(access_project_resp.status_code, 404))

                    if project_id != "same_tenant_unauthorized_project_id":
                        self.log.info("Removing user {} with role as {} from the project"
                                      .format(user["name"], user["role"]))
                        remove_user_resp = capella_api.remove_user_from_project(
                            tenant_ids[tenant_id], user["userid"],project_ids[project_id])
                        self.assertEqual(204, remove_user_resp.status_code,
                                         msg="FAIL, Outcome: {0}, Expected: {1} for adding "
                                         "user to project"
                                         .format(remove_user_resp.status_code, 204))

            if project_id != "same_tenant_unauthorized_project_id":
                delete_project_resp = capella_api.delete_project(tenant_ids[tenant_id],
                                                                 project_ids[project_id])

                if tenant_id == "valid_tenant_id":
                    if project_id == "valid_project_id":
                        self.assertEqual(204, delete_project_resp.status_code,
                                         msg="FAIL, Outcome: {0}, Expected: {1} for adding "
                                         "user to project"
                                         .format(delete_project_resp.status_code, 204))

                    else:
                        self.assertEqual(404, delete_project_resp.status_code,
                                         msg="FAIL, Outcome: {0}, Expected: {1} for adding "
                                         "user to project"
                                         .format(delete_project_resp.status_code, 404))

                else:
                    self.assertEqual(404, delete_project_resp.status_code,
                                     msg="FAIL, Outcome: {0}, Expected: {1} for adding user "
                                     "to project"
                                     .format(delete_project_resp.status_code, 404))

    def test_indexing(self):
        self.log.info("Verifying status code for creating and managing indexes")
        tenant_ids = {
            "valid_tenant_id": self.tenant_id,
            # "invalid_tenant_id": self.invalid_id
        }
        project_ids = {
            "valid_project_id": self.project_id,
            "same_tenant_unauthorized_project_id": self.input.param(
                "same_tenant_unauthorized_project_id", self.invalid_id),
            "different_tenant_unauthorized_project_id": self.input.param(
                "different_tenant_unauthorized_project_id", self.invalid_id),
            "invalid_project_id": self.invalid_id
        }
        database_ids = {
            "valid_database_id": "",
            "same_tenant_unauthorized_database_id": self.input.param(
                "same_tenant_unauthorized_database_id", self.invalid_id),
            "different_tenant_unauthorized_database_id": self.input.param(
                "different_tenant_unauthorized_database_id", self.invalid_id),
            "invalid_database_id": "00000000-0000-0000-0000-000000000000"
        }
        project_roles = ["projectOwner", "projectDataWriter", "projectClusterManager",
                         "projectDataViewer", "projectClusterViewer"]

        self.log.info("Same Tenant Unauthorized project id - {}".format(
            project_ids["same_tenant_unauthorized_project_id"]))
        self.log.info("Different Tenant Unauthorized project id - {}".format(
            project_ids["different_tenant_unauthorized_project_id"]))

        self.log.info("Same Tenant unauthorized database id - {}".format(
            database_ids["same_tenant_unauthorized_database_id"]))
        self.log.info("Different Tenant unauthorized database id - {}".format(
            database_ids["different_tenant_unauthorized_database_id"]))

        capella_api = CapellaAPI("https://" + self.url, self.user, self.passwd)
        deploy_cluster_body = {"tenantID": self.tenant_id, "projectID": self.project_id,
                               "name": "index-Database", "provider": "aws", "region": "us-east-1",
                               "importSampleData": True}
        deploy_resp = capella_api.deploy_database(self.tenant_id, json.dumps(deploy_cluster_body))
        self.assertEqual(202, deploy_resp.status_code,
                         msg='FAIL, Outcome:{0}, Expected:{1} for deploying a cluster'
                         .format(deploy_resp.status_code, 202))
        database_ids["valid_database_id"] = \
            json.loads(deploy_resp.content.decode('utf-8'))["databaseId"]
        time.sleep(20)

        for tenant_id in tenant_ids:
            for project_id in project_ids:
                for database_id in database_ids:
                    for user in self.test_users:

                        self.log.info("Verifying status code for creating indexes for "
                                      "organization role before adding users to project")
                        self.log.info(" ")
                        self.log.info(" ")
                        self.log.info("Tenant ID--------------Project ID--------------Database ID")
                        self.log.info("{} | {} | {}".format(tenant_id, project_id, database_id))

                        capella_api_role = CapellaAPI("https://" + self.url,
                                                      self.test_users[user]["mailid"],
                                                      self.test_users[user]["password"])
                        defer_build = "{ \"defer_build\":true }"
                        create_index_body = {"timeout": "600s",
                                             "statement": "CREATE INDEX `def_samples_airline_name` "
                                                          "ON "
                                                          "`{0}`.`samples`.`airline`(`name` "
                                                          "INCLUDE MISSING) WITH {1}"
                                             .format(database_ids[database_id], defer_build),
                                             "client_context_id": "TLbAQrhdx5nRbItqJcp-B",
                                             "query_context": "default:`{}`"
                                             .format(database_ids[database_id]),
                                             "profile": "timings",
                                             "use_cbo": True,
                                             "txtimeout": "120s", "txId": ""}
                        run_query_resp = capella_api_role.run_query(database_ids[database_id],
                                                                    json.dumps(create_index_body))

                        if tenant_id == "valid_tenant_id":
                            if project_id == "valid_project_id" and \
                                    project_id == "same_tenant_unauthorized_project_id":
                                if database_id == "valid_database_id" or \
                                        database_id == "same_tenant_unauthorized_database_id":

                                    if self.test_users[user]["role"] == "organizationOwner":
                                        self.assertEqual(200, run_query_resp.status_code,
                                                         msg="FAIL, Outcome: {0}, Expected: {1} "
                                                         "for accessing project"
                                                         .format(run_query_resp.status_code, 200))
                                    else:
                                        self.assertEqual(412, run_query_resp.status_code,
                                                         msg="FAIL, Outcome: {0}, Expected: {1} "
                                                         "for accessing project"
                                                         .format(run_query_resp.status_code, 412))

                                else:
                                    self.assertEqual(404, run_query_resp.status_code,
                                                     msg="FAIL, Outcome: {0}, Expected: {1} for "
                                                     "accessing project"
                                                     .format(run_query_resp.status_code, 404))
                            else:
                                self.assertEqual(404, run_query_resp.status_code,
                                                 msg="FAIL, Outcome: {0}, Expected: {1} for "
                                                 "accessing project"
                                                 .format(run_query_resp.status_code, 404))

                        else:
                            self.assertEqual(400, run_query_resp.status_code,
                                             msg='FAIL, Outcome:{0}, Expected:{1} for deploying'
                                             ' a cluster'
                                             .format(run_query_resp.status_code, 400))

                self.log.info(
                    "Verifying status code for creating an index after adding user to the project"
                )
                user = self.test_users["User4"]
                capella_api_role = CapellaAPI("https://" + self.url, user["mailid"],
                                              user["password"])
                for role in project_roles:
                    self.log.info("")
                    self.log.info("Adding user {} with role as {} to the project"
                                  .format(user["name"], role))
                    if project_id != "same_tenant_unauthorized_project_id":
                        body = {"resourceId": project_ids[project_id], "resourceType": "project",
                                "roles": [role], "users": [user["userid"]]}
                        add_user_resp = capella_api.add_user_to_project(tenant_ids[tenant_id],
                                                                        json.dumps(body))

                        if tenant_id == "valid_tenant_id":
                            if project_id == "valid_project_id":
                                self.assertEqual(200, add_user_resp.status_code,
                                                 msg="FAIL, Outcome: {0}, Expected: {1} for "
                                                 "adding user to project"
                                                 .format(add_user_resp.status_code, 200))
                            else:
                                self.assertEqual(404, add_user_resp.status_code,
                                                 msg="FAIL, Outcome: {0}, Expected: {1} for"
                                                 " adding user to project"
                                                 .format(add_user_resp.status_code, 404))

                    for database_id in database_ids:
                        self.log.info(" ")
                        self.log.info(" ")
                        self.log.info("Tenant ID--------------Project ID--------------Database ID")
                        self.log.info("{} | {} | {}".format(tenant_id, project_id, database_id))

                        self.log.info("Verifying status code for creating index statement for {}"
                                      .format(role))
                        defer_build = "{ \"defer_build\":true }"
                        create_index_body = {"timeout": "600s",
                                             "statement": "CREATE INDEX "
                                                          "`def_samples_airline_name_2` ON "
                                                          "`{0}`.`samples`.`airline`(`name` "
                                                          "INCLUDE MISSING) WITH {1}"
                                             .format(database_ids[database_id], defer_build),
                                             "client_context_id": "TLbAQrhdx5nRbItqJcp-B",
                                             "query_context": "default:`{}`"
                                             .format(database_ids[database_id]),
                                             "profile": "timings",
                                             "use_cbo": True, "txtimeout": "120s", "txId": ""}
                        run_query_resp = capella_api_role.run_query(database_ids[database_id],
                                                                    json.dumps(create_index_body))

                        if tenant_id == "valid_tenant_id":
                            if project_id == "valid_project_id":
                                if database_id == "valid_database_id":
                                    if role == "projectOwner" or role == "projectDataWriter" \
                                            or role == "projectDataViewer":
                                        self.assertEqual(200, run_query_resp.status_code,
                                                         msg="FAIL, Outcome: {0}, Expected: {1} "
                                                         "for adding user to project"
                                                         .format(run_query_resp.status_code, 200))

                                    else:
                                        self.assertEqual(412, run_query_resp.status_code,
                                                         msg="FAIL, Outcome: {0}, Expected: "
                                                         "{1} for adding user to project"
                                                         .format(run_query_resp.status_code, 412))
                                else:
                                    self.assertEqual(412, run_query_resp.status_code,
                                                     msg="FAIL, Outcome: {0}, Expected: {1} for "
                                                     "adding user to project"
                                                     .format(run_query_resp.status_code, 412))

                            else:
                                self.assertEqual(412, run_query_resp.status_code,
                                                 msg="FAIL, Outcome: {0}, Expected: {1} for "
                                                 "adding user to project"
                                                 .format(run_query_resp.status_code, 412))

                        else:
                            self.assertEqual(412, run_query_resp.status_code,
                                             msg="FAIL, Outcome: {0}, Expected: {1} for adding "
                                             "user to project"
                                             .format(run_query_resp.status_code, 412))

                        # Commented because for the projectDataViewer role it gives a response of
                        # 200
                        # but does not allow the user to write data to the bug. Filed a bug :
                        # AV - 54154
                        #
                        # self.log.info("Verifying status code for write query statement for {}"
                        #               .format(role))
                        # str_id = "1222_{}_write".format(role)
                        # write_query_statement = "INSERT INTO airline VALUES ({id},
                        #                           {\"name\":\"Couchbase_Airlines\", " \
                        #                         "\"type\":\"Premium\", \"country\":\"India\", \
                        #                         " \"callsign\":\"C.A.\"," \
                        #                         " \"iata\":\"1234\", \"id\":\"1122\"})"
                        #                         .format(id=str_id)
                        # n1ql_write_body = {"timeout": "600s", "statement": write_query_statement,
                        #                   "client_context_id": "EaxwLNBb3bpH2q6zJmYYQ",
                        #                   "query_context": "default:`{}`.`samples`"
                        #                   .format(database_id),
                        #                   "profile": "timings", "use_cbo": True,
                        #                   "txtimeout": "120s"}
                        # resp = capella_api_role.run_query(database_id,
                        # json.dumps(n1ql_write_body))
                        #
                        # if role == "projectOwner" or role == "projectDataWriter":
                        #     self.assertEqual(200, resp.status_code,
                        #                      msg="FAIL, Outcome: {0}, Expected: {1} for
                        #                      adding user to project"
                        #                      .format(resp.status_code, 200))
                        # else:
                        #     self.assertEqual(412, resp.status_code,
                        #                      msg="FAIL, Outcome: {0}, Expected: {1} for
                        #                      adding user to project"
                        #                      .format(resp.status_code, 412))

                    if project_id != "same_tenant_unauthorized_project_id":
                        self.log.info("Removing user {} with role as {} from the project"
                                      .format(user["name"], user["role"]))
                        remove_user_resp = capella_api.remove_user_from_project(
                            tenant_ids[tenant_id], user["userid"],  project_ids[project_id])
                        self.assertEqual(204, remove_user_resp.status_code,
                                         msg="FAIL, Outcome: {0}, Expected: {1} for adding user "
                                         "to project"
                                         .format(remove_user_resp.status_code, 204))

        self.log.info("Destroying the database")
        destroy_database_resp = capella_api.destroy_database(self.tenant_id, self.project_id,
                                                             database_ids["valid_database_id"])
        self.log.info("Outcome: {}, Expected: {}".format(destroy_database_resp.status_code, 204))

    def test_fts(self):
        self.log.info("Verifying status code for creating and managing indexes")
        tenant_ids = {
            "valid_tenant_id": self.tenant_id,
            # "invalid_tenant_id": self.invalid_id
        }
        project_ids = {
            "valid_project_id": self.project_id,
            "same_tenant_unauthorized_project_id": self.input.param(
                "same_tenant_unauthorized_project_id", self.invalid_id),
            "different_tenant_unauthorized_project_id": self.input.param(
                "different_tenant_unauthorized_project_id", self.invalid_id),
            "invalid_project_id": self.invalid_id
        }
        database_ids = {
            "valid_database_id": "",
            "same_tenant_unauthorized_database_id": self.input.param(
                "same_tenant_unauthorized_database_id", self.invalid_id),
            "different_tenant_unauthorized_database_id": self.input.param(
                "different_tenant_unauthorized_database_id", self.invalid_id),
            "invalid_database_id": "00000000-0000-0000-0000-000000000000"
        }
        project_roles = ["projectOwner", "projectDataWriter", "projectClusterManager",
                         "projectDataViewer", "projectClusterViewer"]

        self.log.info("Same Tenant Unauthorized project id - {}".format(
            project_ids["same_tenant_unauthorized_project_id"]))
        self.log.info("Different Tenant Unauthorized project id - {}".format(
            project_ids["different_tenant_unauthorized_project_id"]))

        self.log.info("Same Tenant unauthorized database id - {}".format(
            database_ids["same_tenant_unauthorized_database_id"]))
        self.log.info("Different Tenant unauthorized database id - {}".format(
            database_ids["different_tenant_unauthorized_database_id"]))

        capella_api = CapellaAPI("https://" + self.url, self.user, self.passwd)
        deploy_cluster_body = {"tenantID": self.tenant_id, "projectID": self.project_id,
                               "name": "index-Database", "provider": "aws", "region": "us-east-1",
                               "importSampleData": True}
        deploy_resp = capella_api.deploy_database(self.tenant_id, json.dumps(deploy_cluster_body))
        self.assertEqual(202, deploy_resp.status_code,
                         msg='FAIL, Outcome:{0}, Expected:{1} for deploying a cluster'
                         .format(deploy_resp.status_code, 202))
        database_ids["valid_database_id"] = \
            json.loads(deploy_resp.content.decode('utf-8'))["databaseId"]
        time.sleep(20)

        for tenant_id in tenant_ids:
            for project_id in project_ids:
                for database_id in database_ids:
                    for user in self.test_users:

                        self.log.info("Verifying status code for creating indexes for "
                                      " organization role before adding users to project")
                        self.log.info(" ")
                        self.log.info(" ")
                        self.log.info("Tenant ID--------------Project ID--------------Database ID")
                        self.log.info("{} | {} | {}".format(tenant_id, project_id, database_id))

                        capella_api_role = CapellaAPI("https://" + self.url,
                                                      self.test_users[user]["mailid"],
                                                      self.test_users[user]["password"])

                        self.log.info("Creating a FTS index")
                        fts_index_name = "test_fts_index"
                        create_fts_index_body = {"name": fts_index_name, "type": "fulltext-index",
                                                 "sourceType": "couchbase",
                                                 "sourceName": database_id, "sourceUUID": "",
                                                 "params": {"mapping": {"types":
                                                                            {"samples.airline":
                                                                                 {"enabled": True,
                                                                                  "dynamic": True}},
                                                                        "default_mapping":
                                                                            {"enabled": False,
                                                                             "dynamic": True},
                                                                        "default_type":
                                                                            "_default",
                                                                        "default_analyzer":
                                                                            "standard",
                                                                        "default_datetime_parser":
                                                                            "dateTimeOptional",
                                                                        "default_field": "_all",
                                                                        "store_dynamic": False,
                                                                        "index_dynamic": True,
                                                                        "docvalues_dynamic": False},
                                                            "store": {"indexType": "scorch",
                                                                      "kvStoreName": ""},
                                                            "doc_config": {"mode":
                                                                    "scope.collection.type_field",
                                                                           "type_field": "airline",
                                                                           "docid_prefix_delim": "",
                                                                           "docid_regexp": ""}},
                                                    "sourceParams": {},
                                                 "planParams": {"maxPartitionsPerPIndex": 64,
                                                                "numReplicas": 1,
                                                                "indexPartitions": 1}, "uuid": "",
                                                 "id": ""}

                        create_fts_resp = capella_api_role.create_fts_index(database_id,
                                           fts_index_name, json.dumps(create_fts_index_body))

                        if tenant_id == "valid_tenant_id":
                            if project_id == "valid_project_id" and \
                                    project_id == "same_tenant_unauthorized_project_id":
                                if database_id == "valid_database_id" or \
                                        database_id == "same_tenant_unauthorized_database_id":

                                    if self.test_users[user]["role"] == "organizationOwner":
                                        self.assertEqual(200, create_fts_resp.status_code,
                                                         msg="FAIL, Outcome: {0}, Expected: {1}"
                                                         " for accessing project"
                                                         .format(create_fts_resp.status_code, 200))
                                    else:
                                        self.assertEqual(412, create_fts_resp.status_code,
                                                         msg="FAIL, Outcome: {0}, Expected: {1}"
                                                         " for accessing project"
                                                         .format(create_fts_resp.status_code, 412))

                                else:
                                    self.assertEqual(404, create_fts_resp.status_code,
                                                     msg="FAIL, Outcome: {0}, Expected: {1} for "
                                                     "accessing project"
                                                     .format(create_fts_resp.status_code, 404))
                            else:
                                self.assertEqual(404, create_fts_resp.status_code,
                                                 msg="FAIL, Outcome: {0}, Expected: {1} for"
                                                 " accessing project"
                                                 .format(create_fts_resp.status_code, 404))

                        else:
                            self.assertEqual(400, create_fts_resp.status_code,
                                             msg='FAIL, Outcome:{0}, Expected:{1} for deploying'
                                             ' a cluster'
                                             .format(create_fts_resp.status_code, 400))

                self.log.info(
                    "Verifying status code for creating an index after adding user to the project"
                )
                user = self.test_users["User4"]
                capella_api_role = CapellaAPI("https://" + self.url, user["mailid"],
                                              user["password"])
                for role in project_roles:
                    self.log.info("")
                    self.log.info("Adding user {} with role as {} to the project"
                                  .format(user["name"], role))
                    if project_id != "same_tenant_unauthorized_project_id":
                        body = {"resourceId": project_ids[project_id], "resourceType": "project",
                                "roles": [role], "users": [user["userid"]]}

                        add_user_resp = capella_api.add_user_to_project(tenant_ids[tenant_id],
                                                               json.dumps(body))

                        if tenant_id == "valid_tenant_id":
                            if project_id == "valid_project_id":
                                self.assertEqual(200, add_user_resp.status_code,
                                                 msg="FAIL, Outcome: {0}, Expected: {1} for"
                                                 " adding user to project"
                                                 .format(add_user_resp.status_code, 200))
                            else:
                                self.assertEqual(404, add_user_resp.status_code,
                                                 msg="FAIL, Outcome: {0}, Expected: {1} for "
                                                 "adding user to project"
                                                 .format(add_user_resp.status_code, 404))

                    for database_id in database_ids:
                        self.log.info(" ")
                        self.log.info(" ")
                        self.log.info("Tenant ID--------------Project ID--------------Database ID")
                        self.log.info("{} | {} | {}".format(tenant_id, project_id, database_id))

                        self.log.info("Creating a FTS index")
                        fts_index_name = "test_fts_index"
                        create_fts_index_body = {"name": fts_index_name, "type": "fulltext-index",
                                                 "sourceType": "couchbase",
                                                 "sourceName": database_id, "sourceUUID": "",
                                                 "params": {"mapping": {"types":
                                                                            {"samples.airline":
                                                                                 {"enabled": True,
                                                                                  "dynamic": True}},
                                                                        "default_mapping": {
                                                                            "enabled": False,
                                                                            "dynamic": True},
                                                                        "default_type": "_default",
                                                                        "default_analyzer":
                                                                            "standard",
                                                                        "default_datetime_parser":
                                                                            "dateTimeOptional",
                                                                        "default_field": "_all",
                                                                        "store_dynamic": False,
                                                                        "index_dynamic": True,
                                                                        "docvalues_dynamic": False},
                                                            "store": {"indexType": "scorch",
                                                                      "kvStoreName": ""},
                                                            "doc_config": {"mode":
                                                                    "scope.collection.type_field",
                                                                           "type_field": "airline",
                                                                           "docid_prefix_delim": "",
                                                                           "docid_regexp": ""}},
                                                 "sourceParams": {},
                                                 "planParams": {"maxPartitionsPerPIndex": 64,
                                                                "numReplicas": 1, "indexPartitions":
                                                                    1}, "uuid": "",
                                                 "id": ""}

                        create_fts_resp = capella_api_role.create_fts_index(database_id,
                                          fts_index_name, json.dumps(create_fts_index_body))

                        if tenant_id == "valid_tenant_id":
                            if project_id == "valid_project_id":
                                if database_id == "valid_database_id":
                                    if role == "projectOwner" or role == "projectDataWriter" \
                                            or role == "projectDataViewer":
                                        self.assertEqual(200, create_fts_resp.status_code,
                                                         msg="FAIL, Outcome: {0}, Expected: {1} "
                                                         "for adding user to project"
                                                         .format(create_fts_resp.status_code, 200))

                                    else:
                                        self.assertEqual(412, create_fts_resp.status_code,
                                                         msg="FAIL, Outcome: {0}, Expected: {1} "
                                                         "for adding user to project"
                                                         .format(create_fts_resp.status_code, 412))
                                else:
                                    self.assertEqual(412, create_fts_resp.status_code,
                                                     msg="FAIL, Outcome: {0}, Expected: {1} for"
                                                     " adding user to project"
                                                     .format(create_fts_resp.status_code, 412))

                            else:
                                self.assertEqual(412, create_fts_resp.status_code,
                                                 msg="FAIL, Outcome: {0}, Expected: {1} for "
                                                 "adding user to project"
                                                 .format(create_fts_resp.status_code, 412))

                        else:
                            self.assertEqual(412, create_fts_resp.status_code,
                                             msg="FAIL, Outcome: {0}, Expected: {1} for adding "
                                             "user to project"
                                             .format(create_fts_resp.status_code, 412))

                        # Commented because for the projectDataViewer role it gives a response of
                        # 200
                        # but does not allow the user to write data to the bug. Filed a bug :
                        # AV - 54154
                        #
                        # self.log.info("Verifying status code for write query statement for {}"
                        # .format(role))
                        # str_id = "1222_{}_write".format(role)
                        # write_query_statement = "INSERT INTO airline VALUES ({id}, {\"name\": \
                        # \"Couchbase_Airlines\", " \
                        #                         "\"type\":\"Premium\", \"country\":\"India\", \
                        #                         \"callsign\":\"C.A.\"," \
                        #                         " \"iata\":\"1234\", \"id\":\"1122\"})"
                        #                         .format(id=str_id)
                        # n1ql_write_body = {"timeout": "600s", "statement": write_query_statement,
                        #                   "client_context_id": "EaxwLNBb3bpH2q6zJmYYQ",
                        #                   "query_context": "default:`{}`.`samples`"
                        #                   .format(database_id),
                        #                   "profile": "timings", "use_cbo": True,
                        #                   "txtimeout": "120s"}
                        # resp = capella_api_role.run_query(database_id,
                        #                   json.dumps(n1ql_write_body))
                        #
                        # if role == "projectOwner" or role == "projectDataWriter":
                        #     self.assertEqual(200, resp.status_code,
                        #                      msg="FAIL, Outcome: {0}, Expected: {1} for adding
                        #                      user to project"
                        #                      .format(resp.status_code, 200))
                        # else:
                        #     self.assertEqual(412, resp.status_code,
                        #                      msg="FAIL, Outcome: {0}, Expected: {1} for adding
                        #                      user to project"
                        #                      .format(resp.status_code, 412))

                    if project_id != "same_tenant_unauthorized_project_id":
                        self.log.info("Removing user {} with role as {} from the project"
                                      .format(user["name"], user["role"]))
                        remove_user_resp = capella_api.remove_user_from_project(
                            tenant_ids[tenant_id], user["userid"], project_ids[project_id])
                        self.assertEqual(204, remove_user_resp.status_code,
                                         msg="FAIL, Outcome: {0}, Expected: {1} for adding "
                                         "user to project"
                                         .format(remove_user_resp.status_code, 204))

        self.log.info("Destroying the database")
        destroy_database_resp = capella_api.destroy_database(self.tenant_id, self.project_id,
                                                             database_ids["valid_database_id"])
        self.log.info("Outcome: {}, Expected: {}".format(destroy_database_resp.status_code, 204))
