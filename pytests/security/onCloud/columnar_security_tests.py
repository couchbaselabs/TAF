from pytests.security.security_base import SecurityBase

class ColumnarSecurityTest(SecurityBase):

    def setUp(self):

        SecurityBase.setUp(self)

    def tearDown(self):
        super(ColumnarSecurityTest, self).tearDown()

    def create_cluster_success_callback(self, resp):
        instance_id = resp.json()["id"]
        self.wait_for_columnar_instance_to_deploy(instance_id)
        delete_resp = self.columnarAPI.delete_columnar_instance(self.tenant_id,
                                                                self.project_id,
                                                                instance_id)
        if delete_resp.status_code != 202:
            self.fail("Failed to delete columanr cluster {}".format(instance_id))


    def test_create_columnar_cluster(self):
        #Authentication test
        create_cluster_url = "{}/v2/organizations/{}/projects/{}/instance".format(
            self.capellaAPI.internal_url, self.tenant_id, self.project_id)
        create_cluster_payload = self.get_columnar_cluster_payload("Security_Test_Columnar")
        result, error = self.test_authentication(create_cluster_url, "POST",
                                                 create_cluster_payload)
        if not result:
            self.fail("Auth test failed. Error: {}".format(error))

        #Test with different tenant ids
        test_method_args = {
            'project_id': self.project_id,
            'config': create_cluster_payload
        }
        result, error = self.test_tenant_ids(self.columnarAPI.create_columnar_instance, test_method_args,
                            'tenant_id', 201, self.create_cluster_success_callback)
        if not result:
            self.fail("Tenant ids test failed. Error: {}".format(error))

        #Test with different project ids
        test_method_args = {
            'tenant_id': self.tenant_id,
            'config': create_cluster_payload
        }
        result, error = self.test_project_ids(self.columnarAPI.create_columnar_instance, test_method_args,
                            'project_id', 201, self.create_cluster_success_callback,
                            include_different_project=False)
        if not result:
            self.fail("Project ids test failed. Error: {}".format(error))

        #Test with with org roles
        test_method_args = {
            'tenant_id': self.tenant_id,
            'project_id': self.project_id,
            'config': create_cluster_payload
        }
        result, error = self.test_with_org_roles("create_columnar_instance", test_method_args,
                                                 201, self.create_cluster_success_callback)
        if not result:
            self.fail("Org roles test failed. Error: {}".format(error))
        #Test with project roles
        result, error = self.test_with_project_roles("create_columnar_instance", test_method_args,
                                                     ["projectOwner", "projectClusterManager"], 201,
                                                     self.create_cluster_success_callback)
        if not result:
            self.fail("Project roles test failed. Error: {}".format(error))