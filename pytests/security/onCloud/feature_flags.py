import random
from pytests.security.security_base import SecurityBase


class TestFeatureFlagsAPI(SecurityBase):
    def setUp(self):
        try:
            SecurityBase.setUp(self)
            self.flag_name_exist = "sso"
            self.flag_name_not_exist = "dummy_flag"
        except Exception as e:
            self.tearDown()
            self.fail("Base Setup Failed with error as - {}".format(e))

    def tearDown(self):
        super(TestFeatureFlagsAPI, self).tearDown()

    def test_list_global_feature_flags(self):
        # Auth test
        url = "{}/v2/features/flags".format(self.capellaAPI_internal.internal_url)
        result, error = self.test_authentication(url, method="GET")
        if result:
            self.fail("Auth test should have failed. Error: {}".format(error))

    def test_list_global_feature_flags_filtered(self):
        # Auth test
        url = "{}/v2/features/flags?flags={}".format(self.capellaAPI_internal.internal_url, self.flag_name_exist)
        result, error = self.test_authentication(url, method="GET")
        if result:
            self.fail("Auth test should have failed. Error: {}".format(error))

        url = "{}/v2/features/flags?flags={}".format(self.capellaAPI_internal.internal_url,
                                                     self.flag_name_not_exist)
        result, error = self.test_authentication(url, method="GET")
        if result:
            self.fail("Auth test should have failed. Error: {}".format(error))

    def test_list_tenant_feature_flags(self):
        # Auth test
        tenant_id = self.tenant_id
        url = "{}/v2/features/{}/flags".format(self.capellaAPI_internal.internal_url, tenant_id)
        result, error = self.test_authentication(url, method="GET")
        if not result:
            self.fail("Auth test failed. Error: {}".format(error))

        # Test with different tenant IDs
        test_method_args = {}
        result, error = self.test_tenant_ids(self.capellaAPI_internal.list_tenant_feature_flags,
                                             test_method_args, 'tenant_id', 200)
        if not result:
            self.fail("Tenant IDs test failed. Error: {}".format(error))

    def test_list_tenant_feature_flags_filtered(self):
        # Auth test
        tenant_id = self.tenant_id
        url = "{}/v2/features/{}/flags?flags={}".format(self.capellaAPI_internal.internal_url, tenant_id,
                                                        self.flag_name_exist)
        result, error = self.test_authentication(url, method="GET")
        if not result:
            self.fail("Auth test failed. Error: {}".format(error))

        url = "{}/v2/features/{}/flags?flags={}".format(self.capellaAPI_internal.internal_url, tenant_id,
                                                        self.flag_name_exist)
        result, error = self.test_authentication(url, method="GET")
        if not result:
            self.fail("Auth test failed. Error: {}".format(error))

    def test_list_global_feature_flags_internal(self):
        # Auth test
        url = "{}/internal/support/features/flags".format(self.capellaAPI_internal.internal_url)
        result, error = self.test_authentication(url, method="GET")
        if not result:
            self.fail("Auth test failed. Error: {}".format(error))

    def test_list_global_feature_flags_filtered_internal(self):
        # Auth test
        url = "{}/internal/support/features/flags?flags=name".format(self.capellaAPI_internal.internal_url)
        result, error = self.test_authentication(url, method="GET")
        if not result:
            self.fail("Auth test failed. Error: {}".format(error))

        # Test with org roles
        test_method_args = {'flags': 'name'}
        result, error = self.test_with_org_roles("list_global_feature_flags_internal_specific",
                                                 test_method_args, 200)
        if result:
            self.fail("Org roles test failed. Error: {}".format(error))

    def test_create_global_feature_flag(self):
        flag_name = "test_flag_{}".format(random.randint(1, 10000))
        payload = {"value": True}

        # Auth test
        url = "{}/internal/support/features/flags/{}".format(self.capellaAPI_internal.internal_url, flag_name)
        result, error = self.test_authentication(url, method="POST", payload=payload)
        if not result:
            self.fail("Auth test failed. Error: {}".format(error))

    def test_update_global_feature_flag(self):
        flag_name = "test_flag_{}".format(random.randint(1, 10000))
        create_payload = {"value": True}
        update_payload = {"value": False, "endUserSessions": True}

        # Create the flag first
        resp = self.capellaAPI_internal.create_global_feature_flag(flag_name, create_payload)
        if resp.status_code != 204:
            self.fail("Failed to create global feature flag for update test. Error: {}. Status code: {}".
                      format(resp.content, resp.status_code))

        # Auth test
        url = "{}/internal/support/features/flags/{}".format(self.capellaAPI_internal.internal_url, flag_name)
        result, error = self.test_authentication(url, method="PUT", payload=update_payload)
        if not result:
            self.fail("Auth test failed. Error: {}".format(error))

    def test_delete_global_feature_flag(self):
        flag_name = "test_flag_{}".format(random.randint(1, 10000))
        payload = {"value": True}

        # Create the flag first
        resp = self.capellaAPI_internal.create_global_feature_flag(flag_name, payload)
        if resp.status_code != 204:
            self.fail("Failed to create global feature flag for delete test. Error: {}. Status code: {}".
                      format(resp.content, resp.status_code))

        # Auth test
        url = "{}/internal/support/features/flags/{}".format(self.capellaAPI_internal.internal_url, flag_name)
        result, error = self.test_authentication(url, method="DELETE")
        if not result:
            self.fail("Auth test failed. Error: {}".format(error))

    def test_list_tenant_feature_flags_internal(self):
        # Auth test
        tenant_id = self.tenant_id
        url = "{}/internal/support/features/{}/flags".format(self.capellaAPI_internal.internal_url, tenant_id)
        result, error = self.test_authentication(url, method="GET")
        if not result:
            self.fail("Auth test failed. Error: {}".format(error))

        # Test with different tenant IDs
        test_method_args = {}
        result, error = self.test_tenant_ids(self.capellaAPI_internal.list_tenant_feature_flags_internal,
                                             test_method_args, 'tenant_id', 200)
        if not result:
            self.fail("Tenant IDs test failed. Error: {}".format(error))

    def test_create_tenant_feature_flag(self):
        flag_name = "test_tenant_flag_{}".format(random.randint(1, 10000))
        payload = {"value": True}

        # Create the flag first globally
        resp = self.capellaAPI_internal.create_global_feature_flag(flag_name, payload)
        if resp.status_code != 204:
            self.fail("Failed to create global feature flag for create tenant test. Error: {}. Status code: {}".
                      format(resp.content, resp.status_code))

        # Auth test
        tenant_id = self.tenant_id
        url = "{}/internal/support/features/{}/flags/{}".format(self.capellaAPI_internal.internal_url,
                                                                tenant_id, flag_name)
        result, error = self.test_authentication(url, method="POST", payload=payload)
        if not result:
            self.fail("Auth test failed. Error: {}".format(error))

        # Test with different tenant IDs
        test_method_args = {
            'flag_name': flag_name,
            'payload': payload
        }
        result, error = self.test_tenant_ids(self.capellaAPI_internal.create_tenant_feature_flag,
                                             test_method_args, 'tenant_id', 204)
        if not result:
            self.fail("Tenant IDs test failed. Error: {}".format(error))

    def test_update_tenant_feature_flag(self):
        flag_name = "test_tenant_flag_{}".format(random.randint(1, 10000))
        create_payload = {"value": True}
        update_payload = {"value": False, "endUserSessions": True}

        # Create the flag first
        resp = self.capellaAPI_internal.create_global_feature_flag(flag_name, create_payload)
        if resp.status_code != 204:
            self.fail("Failed to create global feature flag for update test. Error: {}. Status code: {}".
                      format(resp.content, resp.status_code))

        tenant_id = self.tenant_id
        resp = self.capellaAPI_internal.create_tenant_feature_flag(tenant_id, flag_name, create_payload)
        if resp.status_code != 204:
            self.fail("Failed to create tenant feature flag for update test. Error: {}. Status code: {}".
                      format(resp.content, resp.status_code))

        # Auth test
        url = "{}/internal/support/features/{}/flags/{}".format(self.capellaAPI_internal.internal_url,
                                                                tenant_id, flag_name)
        result, error = self.test_authentication(url, method="PUT", payload=update_payload)
        if not result:
            self.fail("Auth test failed. Error: {}".format(error))

        # Test with different tenant IDs
        test_method_args = {
            'flag_name': flag_name,
            'payload': update_payload
        }
        result, error = self.test_tenant_ids(self.capellaAPI_internal.update_tenant_feature_flag,
                                             test_method_args, 'tenant_id', 204)
        if not result:
            self.fail("Tenant IDs test failed. Error: {}".format(error))

    def test_delete_tenant_feature_flag(self):
        flag_name = "test_tenant_flag_{}".format(random.randint(1, 10000))
        payload = {"value": True}

        # Create the flag first
        resp = self.capellaAPI_internal.create_global_feature_flag(flag_name, payload)
        if resp.status_code != 204:
            self.fail("Failed to create global feature flag for delete test. Error: {}. Status code: {}".
                      format(resp.content, resp.status_code))

        tenant_id = self.tenant_id
        resp = self.capellaAPI_internal.create_tenant_feature_flag(tenant_id, flag_name, payload)
        if resp.status_code != 204:
            self.fail("Failed to create tenant feature flag for delete test. Error: {}. Status code: {}".
                      format(resp.content, resp.status_code))

        # Auth test
        url = "{}/internal/support/features/{}/flags/{}".format(self.capellaAPI_internal.internal_url, tenant_id, flag_name)
        result, error = self.test_authentication(url, method="DELETE")
        if not result:
            self.fail("Auth test failed. Error: {}".format(error))

        # Test with different tenant IDs
        test_method_args = {
            'flag_name': flag_name
        }
        result, error = self.test_tenant_ids(self.capellaAPI_internal.delete_tenant_feature_flag,
                                             test_method_args, 'tenant_id', 204)
        if not result:
            self.fail("Tenant IDs test failed. Error: {}".format(error))

    def test_initialize_feature_flags_from_launchdarkly(self):
        # Auth test
        url = "{}/internal/support/features/flags/initialize".format(self.capellaAPI_internal.internal_url)
        result, error = self.test_authentication(url, method="POST")
        if not result:
            self.fail("Auth test failed. Error: {}".format(error))
