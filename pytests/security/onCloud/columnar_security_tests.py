import random
from pytests.security.security_base import SecurityBase

class ColumnarSecurityTest(SecurityBase):

    def setUp(self):

        SecurityBase.setUp(self)
        self.columnar_instance_url = "{}/v2/organizations/{}/projects/{}/instance"
        self.columnar_allow_ip_url = self.columnar_instance_url + "/{}/allowlists"
        self.columnar_on_url = self.columnar_instance_url + "{}/on"
        self.columnar_off_url = self.columnar_instance_url + "{}/off"

    def tearDown(self):
        super(ColumnarSecurityTest, self).tearDown()

    def create_cluster_success_callback(self, resp, test_method_args=None):
        instance_id = resp.json()["id"]
        self.wait_for_columnar_instance_to_deploy(instance_id)
        delete_resp = self.columnarAPI.delete_columnar_instance(self.tenant_id,
                                                                self.project_id,
                                                                instance_id)
        if delete_resp.status_code != 202:
            self.fail("Failed to delete columnar cluster {}".format(instance_id))


    def test_create_columnar_cluster(self):
        #Authentication test
        create_cluster_url = self.columnar_instance_url.format(
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

    def test_get_columnar_clusters(self):
        #Authentication test
        create_cluster_url = self.columnar_instance_url.format(
            self.capellaAPI.internal_url, self.tenant_id, self.project_id)
        result, error = self.test_authentication(create_cluster_url, "GET")
        if not result:
            self.fail("Auth test failed. Error: {}".format(error))

        #Test with different tenant ids
        test_method_args = {
            'project_id': self.project_id
        }
        result, error = self.test_tenant_ids(self.columnarAPI.get_columnar_instances, test_method_args,
                            'tenant_id', 200, None)
        if not result:
            self.fail("Tenant ids test failed. Error: {}".format(error))

        #Test with different project ids
        test_method_args = {
            'tenant_id': self.tenant_id
        }
        result, error = self.test_project_ids(self.columnarAPI.get_columnar_instances, test_method_args,
                                              'project_id', 200, None,
                                              include_different_project=False)
        if not result:
            self.fail("Project ids test failed. Error: {}".format(error))

        #Test with org roles
        test_method_args = {
            'tenant_id': self.tenant_id,
            'project_id': self.project_id
        }
        result, error = self.test_with_org_roles("get_columnar_instances", test_method_args,
                                                 200, None)
        if not result:
            self.fail("Org roles test failed. Error: {}".format(error))
        result, error = self.test_with_project_roles("get_columnar_instances", test_method_args,
                                                     ["projectOwner", "projectClusterViewer", "projectClusterManager",
                                                     "projectDataWriter", "projectDataViewer"],
                                                     200, None)
        if not result:
            self.fail("Project roles test failed. Error: {}".format(error))

    def test_fetch_columnar_cluster(self):
        #Authentication test
        fetch_columnar_cluster_url = "{}/{}".format(
            self.columnar_instance_url.format(self.capellaAPI.internal_url, self.tenant_id, self.project_id),
            self.instance_id)
        result, error = self.test_authentication(fetch_columnar_cluster_url, "GET")
        if not result:
            self.fail("Auth test failed. Error: {}".format(error))

        #Test with different tenant ids
        test_method_args = {
            'project_id': self.project_id,
            'instance_id': self.instance_id
        }
        result, error = self.test_tenant_ids(self.columnarAPI.get_specific_columnar_instance, test_method_args,
                            'tenant_id', 200, None)
        if not result:
            self.fail("Tenant ids test failed. Error: {}".format(error))

        #Test with different project ids
        test_method_args = {
            'tenant_id': self.tenant_id,
            'instance_id': self.instance_id
        }
        result, error = self.test_project_ids(self.columnarAPI.get_specific_columnar_instance, test_method_args,
                                              'project_id', 200, None)
        if not result:
            self.fail("Project ids test failed. Error: {}".format(error))

        #Test with org roles
        test_method_args = {
            'tenant_id': self.tenant_id,
            'project_id': self.project_id,
            'instance_id': self.instance_id
        }
        result, error = self.test_with_org_roles("get_specific_columnar_instance", test_method_args,
                                                 200, None)
        if not result:
            self.fail("Org roles test failed. Error: {}".format(error))

        #Test with project roles
        result, error = self.test_with_project_roles("get_specific_columnar_instance", test_method_args,
                                                     ["projectOwner", "projectClusterViewer", "projectClusterManager",
                                                     "projectDataWriter", "projectDataViewer"],
                                                     200, None)
        if not result:
            self.fail("Project roles test failed. Error: {}".format(error))

    def delete_cluster_success_callback(self, resp, test_method_args=None):
        if resp.status_code != 202:
            self.fail("Failed to delete columnar cluster: {}. Error: {}. Status code: {}.".
                      format(self.instance_id, resp.content, resp.status_code))
        create_cluster_payload = self.get_columnar_cluster_payload("Security_Test_Columnar")
        create_cluster_resp = self.columnarAPI.create_columnar_instance(self.tenant_id,
                                                                        self.project_id,
                                                                        create_cluster_payload)
        if create_cluster_resp.status_code != 201:
            self.fail("Failed to deploy columnar cluster. Error: {}. Status code: {}".
                      format(create_cluster_resp.content, create_cluster_resp.status_code))
        self.instance_id = create_cluster_resp.json()["id"]
        self.wait_for_columnar_instance_to_deploy(self.instance_id)
        if test_method_args:
            test_method_args['instance_id'] = self.instance_id

    def test_delete_columnar_cluster(self):
        #Authentication test
        delete_columnar_cluster_url = "{}/{}".format(
            self.columnar_instance_url.format(self.capellaAPI.internal_url, self.tenant_id, self.project_id),
            self.instance_id)
        result, error = self.test_authentication(delete_columnar_cluster_url, "DELETE")
        if not result:
            self.fail("Auth test failed. Erorr: {}".format(error))

        #Test with different tenant ids
        test_method_args = {
            'project_id': self.project_id,
            'instance_id': self.instance_id
        }
        result, error = self.test_tenant_ids(self.columnarAPI.delete_columnar_instance, test_method_args,
                                             'tenant_id', 202, self.delete_cluster_success_callback)
        if not result:
            self.fail("Tenant ids test failed. Error: {}".format(error))

        #Test with different project ids
        test_method_args = {
            'tenant_id': self.tenant_id,
            'instance_id': self.instance_id
        }
        result, error = self.test_project_ids(self.columnarAPI.delete_columnar_instance, test_method_args,
                                              'project_id', 202, self.delete_cluster_success_callback)
        if not result:
            self.fail("Project ids test failed. Error: {}".format(error))

        #Test with org roles
        test_method_args = {
            'tenant_id': self.tenant_id,
            'project_id': self.project_id,
            'instance_id': self.instance_id
        }
        result, error = self.test_with_org_roles("delete_columnar_instance", test_method_args,
                                                 202, self.delete_cluster_success_callback)
        if not result:
            self.fail("Org roles test failed. Error: {}".format(error))

        #Test with project roles
        result, error = self.test_with_project_roles("delete_columnar_instance", test_method_args,
                                                     ["projectOwner", "projectClusterManager"], 202,
                                                     self.delete_cluster_success_callback)
        if not result:
            self.fail("Project roles test failed. Error: {}".format(error))

    def update_cluster_success_callback(self, resp, test_method_args=None):
        if resp.status_code != 202:
            self.fail("Failed to scale up columnar instance {}. Error: {}. Status code: {}".
                      format(self.instance_id, resp.content, resp.status_code))
        self.wait_for_columnar_instance_to_scale(self.instance_id)
        update_resp = self.columnarAPI.update_columnar_instance(self.tenant_id, self.project_id,
                                                                self.instance_id, nodes=1,
                                                                name='Security_Test_Cluster',
                                                                description='')
        if update_resp.status_code != 202:
            self.fail("Failed to scale down columnar instance {}. Error: {}. Status code: {}".
                      format(self.instance_id, update_resp.content, update_resp.status_code))
        self.wait_for_columnar_instance_to_scale(self.instance_id)

    def test_update_columnar_cluster(self):
        #Authentication test
        update_columnar_cluster_url = "{}/{}".format(
            self.columnar_instance_url.format(self.capellaAPI.internal_url, self.tenant_id, self.project_id),
            self.instance_id)
        result, error = self.test_authentication(update_columnar_cluster_url, "PATCH")
        if not result:
            self.fail("Auth test failed. Erorr: {}".format(error))

        #Test with different tenant ids
        test_method_args = {
            'project_id': self.project_id,
            'instance_id': self.instance_id,
            'nodes': 2,
            'name': "Security_Test_Cluster",
            'description': ""
        }
        result, error = self.test_tenant_ids(self.columnarAPI.update_columnar_instance, test_method_args,
                                             'tenant_id', 202, self.update_cluster_success_callback)
        if not result:
            self.fail("Tenant ids test failed. Error: {}".format(error))

        #Test with different project ids
        test_method_args = {
            'tenant_id': self.tenant_id,
            'instance_id': self.instance_id,
            'nodes': 2,
            'name': "Security_Test_Cluster",
            'description': ""
        }
        result, error = self.test_project_ids(self.columnarAPI.update_columnar_instance, test_method_args,
                                              'project_id', 202, self.update_cluster_success_callback)
        if not result:
            self.fail("Project ids test failed. Error: {}".format(error))

        #Test with org roles
        test_method_args = {
            'tenant_id': self.tenant_id,
            'project_id': self.project_id,
            'instance_id': self.instance_id,
            'nodes': 2,
            'name': "Security_Test_Cluster",
            'description': ""
        }
        result, error = self.test_with_org_roles("update_columnar_instance", test_method_args,
                                                 202, self.update_cluster_success_callback)
        if not result:
            self.fail("Org roles test failed. Error: {}".format(error))

        #Test with project roles
        result, error = self.test_with_project_roles("update_columnar_instance", test_method_args,
                                                     ["projectOwner", "projectClusterManager"], 202,
                                                     self.update_cluster_success_callback)
        if not result:
            self.fail("Project roles test failed. Error: {}".format(error))

        #Scale cluster to an invalid value and verify that the backend nodes actually don't change
        update_resp = self.columnarAPI.update_columnar_instance(self.tenant_id, self.project_id,
                                                                self.instance_id, nodes=3,
                                                                name='Security_Test_Cluster',
                                                                description='')
        if update_resp.status_code != 422:
            self.fail("Expected 422 error response for scaling to 3 nodes but got response {}".
                      format(update_resp.status_code))
        self.sleep(120, "Wait before checking number of nodes")
        servers = self.get_columnar_cluster_nodes(self.instance_id)
        if len(servers) == 3:
            self.fail("No. of nodes in the columnar cluster are updated to 3 even after API returned error")

    def allow_ip_success_callback(self, resp, test_method_args=None):
        if resp.status_code != 201:
            self.fail("Failed to allow IP to cluster. Error: {}. Status code: {}".
                      format(resp.content, resp.status_code))
        ip_entries = self.columnarAPI.get_allowlist_entries(self.tenant_id,
                                                            self.project_id,
                                                            self.instance_id)
        ip_entries = ip_entries.json()["data"]
        for ip_allow_entry in ip_entries:
            ip_allow_id = ip_allow_entry["data"]["id"]
            delete_resp = self.columnarAPI.delete_allowlist_entry(self.tenant_id,
                                                                  self.project_id,
                                                                  self.instance_id,
                                                                  ip_allow_id)
            if delete_resp.status_code != 202:
                self.fail("Failed to delete allow list entry: {}. Error: {}. Status code: {}".
                          format(ip_allow_id, delete_resp.content, delete_resp.status_code))

    def test_columnar_create_allow_ip(self):
        #Auth test
        allow_ip_url = self.columnar_allow_ip_url.format(self.capellaAPI.internal_url, self.tenant_id,
                                                         self.project_id, self.instance_id)
        result, error = self.test_authentication(allow_ip_url, "POST")
        if not result:
            self.fail("Auth test failed. Error: {}".format(error))

        #Test with different tenant ids
        test_method_args = {
            'project_id': self.project_id,
            'instance_id': self.instance_id,
            'cidr': "0.0.0.0/0"
        }
        result, error = self.test_tenant_ids(self.columnarAPI.allow_ip, test_method_args,
                                             'tenant_id', 201, self.allow_ip_success_callback)
        if not result:
            self.fail("Tenant ids test failed. Error: {}".format(error))

        #Test with different project ids
        test_method_args = {
            'tenant_id': self.tenant_id,
            'instance_id': self.instance_id,
            'cidr': "0.0.0.0/0"
        }
        result, error = self.test_project_ids(self.columnarAPI.allow_ip, test_method_args,
                                              'project_id', 201, self.allow_ip_success_callback)
        if not result:
            self.fail("Project ids test failed: {}".format(error))

        #Test with different org roles
        test_method_args = {
            'tenant_id': self.tenant_id,
            'project_id': self.project_id,
            'instance_id': self.instance_id,
            'cidr': "0.0.0.0/0"
        }
        result, error = self.test_with_org_roles("allow_ip", test_method_args, 201,
                                                 self.allow_ip_success_callback)
        if not result:
            self.fail("Org roles test failed: {}".format(error))
        result, error = self.test_with_project_roles("allow_ip", test_method_args,
                                                     ["projectOwner", "projectClusterManager"],
                                                     201, self.allow_ip_success_callback)
        if not result:
            self.fail("Project roles test failed. {}".format(error))

    def test_columnar_get_allow_ip(self):
        #Auth test
        allow_ip_url = self.columnar_allow_ip_url.format(self.capellaAPI.internal_url, self.tenant_id,
                                                         self.project_id, self.instance_id)
        result, error = self.test_authentication(allow_ip_url, "GET")
        if not result:
            self.fail("Auth test failed. Error: {}".format(error))

        #test with different tenant ids
        test_method_args = {
            'project_id': self.project_id,
            'instance_id': self.instance_id
        }
        result, error = self.test_tenant_ids(self.columnarAPI.get_allowlist_entries, test_method_args,
                                             'tenant_id', 200, None)
        if not result:
            self.fail("Tenant ids test failed. Error: {}".format(error))

        #Test with different project ids
        test_method_args = {
            'tenant_id': self.tenant_id,
            'instance_id': self.instance_id
        }
        result, error = self.test_project_ids(self.columnarAPI.get_allowlist_entries, test_method_args,
                                              'project_id', 200, None)
        if not result:
            self.fail("Project ids test failed. Error: {}".format(error))

        #Test with different org roles
        test_method_args = {
            'tenant_id': self.tenant_id,
            'project_id': self.project_id,
            'instance_id': self.instance_id
        }
        result, error = self.test_with_org_roles("get_allowlist_entries", test_method_args,
                                                 200, None)
        if not result:
            self.fail("Org roles test failed. Error: {}".format(error))

        #Test wtth different project roles
        result, error = self.test_with_project_roles("get_allowlist_entries", test_method_args,
                                                     ["projectOwner", "projectClusterManager", "projectClusterViewer"],
                                                     200, None)
        if not result:
            self.fail("Project roles test failed. Error: {}".format(error))

    def delete_ip_success_callback(self, resp, test_method_args=None):
        if resp.status_code != 202:
            self.fail("Failed delete allowlist entry. Error: {}. Status code: {}".
                      format(resp.content, resp.status_code))

        allow_ip_resp = self.columnarAPI.allow_ip(self.tenant_id, self.project_id,
                                                  self.instance_id, "0.0.0.0/0")
        if allow_ip_resp.status_code != 201:
            self.fail("Failed to allow IP. Error: {}. Status code: {}".
                      format(allow_ip_resp.content, allow_ip_resp.status_code))
        ip_entries = self.columnarAPI.get_allowlist_entries(self.tenant_id,
                                                        self.project_id,
                                                        self.instance_id)
        ip_entries = ip_entries.json()["data"]
        allow_ip_id = ip_entries[0]["data"]["id"]
        if test_method_args:
            test_method_args['allowlist_id'] = allow_ip_id


    def test_delete_allow_ip(self):
        allow_ip_resp = self.columnarAPI.allow_ip(self.tenant_id, self.project_id,
                                                  self.instance_id, "0.0.0.0/0")
        if allow_ip_resp.status_code != 201:
            self.fail("Failed to allow IP. Error: {}. Status code: {}".
                      format(allow_ip_resp.content, allow_ip_resp.status_code))
        ip_entries = self.columnarAPI.get_allowlist_entries(self.tenant_id,
                                                            self.project_id,
                                                            self.instance_id)
        ip_entries = ip_entries.json()["data"]
        allow_ip_id = ip_entries[0]["data"]["id"]
        #Auth test
        delete_allow_ip_url = "{}/{}".format(self.columnar_allow_ip_url.format(
            self.capellaAPI.internal_url, self.tenant_id,
            self.project_id, self.instance_id), self.instance_id)
        result, error = self.test_authentication(delete_allow_ip_url, "DELETE")
        if not result:
            self.fail("Auth test failed. Error: {}".format(error))

        #Test with different tenant ids
        test_method_args = {
            'project_id': self.project_id,
            'instance_id': self.instance_id,
            'allowlist_id': allow_ip_id
        }
        result, error = self.test_tenant_ids(self.columnarAPI.delete_allowlist_entry, test_method_args,
                                             'tenant_id', 202, self.delete_ip_success_callback)
        if not result:
            self.fail("Tenant ids test failed. Error: {}".format(error))

        #Test with different project ids
        test_method_args = {
            'tenant_id': self.tenant_id,
            'instance_id': self.instance_id,
            'allowlist_id': allow_ip_id
        }
        result, error = self.test_project_ids(self.columnarAPI.delete_allowlist_entry, test_method_args,
                                              'project_id', 202, self.delete_ip_success_callback)
        if not result:
            self.fail("Project ids test failed. Error: {}".format(error))

        #Test with different org roles
        test_method_args = {
            'tenant_id': self.tenant_id,
            'project_id': self.project_id,
            'instance_id': self.instance_id,
            'allowlist_id': allow_ip_id
        }
        result, error = self.test_with_org_roles("delete_allowlist_entry", test_method_args,
                                                 202, self.delete_ip_success_callback)
        if not result:
            self.fail("Org roles test failed. Error: {}".format(error))

        #Testi with different project roles
        result, error = self.test_with_project_roles("delete_allowlist_entry", test_method_args,
                                                     ["projectOwner", "projectClusterManager"],
                                                     202, self.delete_ip_success_callback)
        if not result:
            self.fail("Project roles test failed. Error: {}".format(error))

    def test_connect_node_ip_allowlist(self):
        #Allow IP
        allow_ip_resp = self.columnarAPI.allow_ip(self.tenant_id, self.project_id,
                                                  self.instance_id, "0.0.0.0/0")
        if allow_ip_resp.status_code != 201:
            self.fail("Failed to allow IP. Error: {}. Status code: {}".
                      format(allow_ip_resp.content, allow_ip_resp.status_code))

        nodes = self.get_columnar_cluster_nodes(self.instance_id)
        valid_ports = ["18091", "18095"]
        for node in nodes:
            self.connect_node_port(node, valid_ports, expect_to_connect=True)
            invalid_ports = ["3389"]
            random_ports = random.sample(range(0, 65536), 5)
            s = set(valid_ports)
            invalid_ports.extend([str(x) for x in random_ports if str(x) not in s])
            self.connect_node_port(node, invalid_ports, expect_to_connect=False)

        #Delete IPs
        ip_entries = self.columnarAPI.get_allowlist_entries(self.tenant_id,
                                                            self.project_id,
                                                            self.instance_id)
        ip_entries = ip_entries.json()["data"]
        for ip_allow_entry in ip_entries:
            ip_allow_id = ip_allow_entry["data"]["id"]
            delete_resp = self.columnarAPI.delete_allowlist_entry(self.tenant_id,
                                                                  self.project_id,
                                                                  self.instance_id,
                                                                  ip_allow_id)
            if delete_resp.status_code != 202:
                self.fail("Failed to delete allow list entry: {}. Error: {}. Status code: {}".
                          format(ip_allow_id, delete_resp.content, delete_resp.status_code))

        for node in nodes:
            self.connect_node_port(node, valid_ports, expect_to_connect=False)

    def cluster_on_success_callback(self, resp, test_method_args):
        if resp.status_code != 202:
            self.fail("Failed to turn on cluster. Error: {}. Status code: {}".
                      format(resp.content, resp.status_code))
        turn_off_resp = self.columnarAPI.turn_off_instance(self.tenant_id,
                                                           self.project_id,
                                                           self.instance_id)
        if turn_off_resp.status_code != 202:
            self.fail("Failed to turn off cluster. Error: {}. Status code: {}".
                      format(turn_off_resp.content, turn_off_resp.status_code))
        self.wait_for_columnar_instance_to_turn_off(self.instance_id)

    def test_columnar_on(self):
        #Auth test
        on_api = self.columnar_on_url.format(self.capellaAPI.internal_url,
                    self.tenant_id, self.project_id, self.instance_id)
        result, error = self.test_authentication(on_api, "POST")
        if not result:
            self.fail("Auth test failed for cluster on. Error: {}".format(error))

        #test with tenant ids
        test_method_args = {
            'project_id': self.project_id,
            'instance_id': self.instance_id
        }
        result, error = self.test_tenant_ids(self.columnarAPI.turn_on_instance, test_method_args,
                                             'tenant_id', 202, self.cluster_on_success_callback)
        if not result:
            self.fail("Tenant ids test failed. Error: {}".format(error))

        #test with project ids
        test_method_args = {
            'tenant_id': self.tenant_id,
            'instance_id': self.instance_id
        }
        result, error = self.test_project_ids(self.columnarAPI.turn_on_instance, test_method_args,
                                              'project_id', 202, self.cluster_on_success_callback)
        if not result:
            self.fail("Project ids test failed. Error: {}".format(error))

        #test with org roles
        test_method_args = {
            'tenant_id': self.tenant_id,
            'project_id': self.project_id,
            'instance_id': self.instance_id
        }
        result, error = self.test_with_org_roles("turn_on_instance", test_method_args,
                                                 202, self.cluster_on_success_callback)
        if not result:
            self.fail("Org roles test failed. Error: {}".format(error))

        #test with project roles
        result, error = self.test_with_project_roles("turn_on_instance", test_method_args,
                                                     ["projectOwner", "projectClusterManager"],
                                                     202, self.cluster_on_success_callback)
        if not result:
            self.fail("Project roles test failed. Error: {}".format(error))

    def cluster_off_success_callback(self, resp, test_method_args):
        if resp.status_code != 202:
            self.fail("Failed to turn off cluster. Error: {}. Status code: {}".
                      format(resp.content, resp.status_code))
        turn_on_resp = self.columnarAPI.turn_on_instance(self.tenant_id,
                                                           self.project_id,
                                                           self.instance_id)
        if turn_on_resp.status_code != 202:
            self.fail("Failed to turn off cluster. Error: {}. Status code: {}".
                      format(turn_on_resp.content, turn_on_resp.status_code))
        self.wait_for_columnar_instance_to_turn_off(self.instance_id)

    def test_columnar_off(self):
        #Auth test
        off_api = self.columnar_off_url.format(self.capellaAPI.internal_url,
                    self.tenant_id, self.project_id, self.instance_id)
        result, error = self.test_authentication(off_api, "POST")
        if not result:
            self.fail("Auth test failed for cluster on. Error: {}".format(error))

        #test with tenant ids
        test_method_args = {
            'project_id': self.project_id,
            'instance_id': self.instance_id
        }
        result, error = self.test_tenant_ids(self.columnarAPI.turn_off_instance, test_method_args,
                                             'tenant_id', 202, self.cluster_off_success_callback)
        if not result:
            self.fail("Tenant ids test failed. Error: {}".format(error))

        #test with project ids
        test_method_args = {
            'tenant_id': self.tenant_id,
            'instance_id': self.instance_id
        }
        result, error = self.test_project_ids(self.columnarAPI.turn_off_instance, test_method_args,
                                              'project_id', 202, self.cluster_off_success_callback)
        if not result:
            self.fail("Project ids test failed. Error: {}".format(error))

        #test with org roles
        test_method_args = {
            'tenant_id': self.tenant_id,
            'project_id': self.project_id,
            'instance_id': self.instance_id
        }
        result, error = self.test_with_org_roles("turn_off_instance", test_method_args,
                                                 202, self.cluster_off_success_callback)
        if not result:
            self.fail("Org roles test failed. Error: {}".format(error))

        #test with project roles
        result, error = self.test_with_project_roles("turn_off_instance", test_method_args,
                                                     ["projectOwner", "projectClusterManager"],
                                                     202, self.cluster_off_success_callback)
        if not result:
            self.fail("Project roles test failed. Error: {}".format(error))

        self.columnarAPI.create_backup()

