import json
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
            self.test_users = json.loads(self.input.capella.get("test_users"))
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
