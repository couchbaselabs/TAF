import json
import threading
import time
import random
import string
from pytests.security.security_base import SecurityBase
#from pytests.basetestcase import BaseTestCase
from capellaAPI.capella.dedicated.CapellaAPI_v4 import CapellaAPI


def get_random_secret_key():
    # return ''.join(random.choices(string.ascii_letters + string.digits, k=64))
    return ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(64))

def get_random_dummy_email():
    # import random
    return 'dummy.user+' + str(random.random()) + '@couchbase.com'


class SecurityTest(SecurityBase):

    def setUp(self):
        SecurityBase.setUp(self)

        resp = self.capellaAPI.org_ops_apis.create_project(self.tenant_id, 'Secondary project')
        self.log.info(resp.json())
        self.secondary_project_id = resp.json()['id']
        self.test_api_keys = []


    def tearDown(self):
        failures = []
        self.reset_api_keys()
        for api_key in self.test_api_keys:
            resp = self.capellaAPI.org_ops_apis.delete_api_key(
                organizationId=api_key['tenant'],
                accessKey=api_key["id"]
            )
            if resp.status_code != 204:
                failures.append("Error while deleting api key {}"
                                .format(api_key['id']))

        resp = self.capellaAPI.org_ops_apis.delete_project(self.tenant_id, self.secondary_project_id)
        if resp.status_code != 204:
            failures.append("Error while deleting project with id {}"
                            .format(self.secondary_project_id))

        if failures:
            self.fail("Following error occurred in teardown - {}".format(
                failures))
        super(SecurityTest, self).tearDown()

    def reset_access_keys_to_default(self):
        # resetting keys to default
        self.capellaAPI.org_ops_apis.ACCESS = self.access_key_ini
        self.capellaAPI.org_ops_apis.SECRET = self.secret_key_ini

    def set_access_keys(self, bearer_token):
        self.capellaAPI.org_ops_apis.bearer_token = bearer_token

    def append_to_api_keys(self, id, tenant):
        self.test_api_keys.append({
            'id': id,
            'tenant': tenant
        })

    def test_delete_api_key(self):
        # Test with a valid API access key + valid, different and invalid orgID.
        resp = self.capellaAPI.org_ops_apis.create_api_key(self.tenant_id, "name",
                                                    ["organizationMember"], "description",
                                                    allowedCIDRs=['0.0.0.0/0'])
        content = resp.json()
        print("Content: {}".format(content))
        self.assertEqual(resp.status_code, 201)

        resp = self.capellaAPI.org_ops_apis.delete_api_key(self.invalid_id, content['id'])
        self.assertEqual(resp.status_code, 403)

        # resp = self.capellaAPI.org_ops_apis.delete_api_key(self.diff_tenant_id, content['id'])
        # self.assertEqual(resp.status_code, 403)

        resp = self.capellaAPI.org_ops_apis.delete_api_key(self.tenant_id, content['id'])
        self.assertEqual(resp.status_code, 204)

        # Deleting using Invalid Auth
        self.log.info("Deleting access key using invalid auth")
        self.capellaAPI.org_ops_apis.bearer_token = get_random_secret_key()

        resp = self.capellaAPI.org_ops_apis.delete_api_key(self.tenant_id, content['id'])
        self.assertEqual(resp.status_code, 401)

        self.reset_api_keys()

        # Session Expiry Test

        self.log.info("Token exp test.")
        resp = self.capellaAPI.org_ops_apis.create_api_key(self.tenant_id, "name",
                                                    ["organizationOwner"],
                                                    "description", expiry=0.001)

        content = resp.json()
        self.set_access_keys(content['token'])

        time.sleep(90)
        resp = self.capellaAPI.org_ops_apis.delete_api_key(self.tenant_id, content['id'])
        self.assertEqual(resp.status_code, 401)

        self.reset_api_keys()

        # RBAC
        org_roles = ["organizationOwner", "organizationMember", "projectCreator"]
        project_roles = ["projectOwner", "projectManager", "projectViewer",
                         "projectDataReaderWriter", "projectDataReader"]

        for user in org_roles:
            for role in project_roles:
                self.log.info("Rbac test for {}, {} role".format(user, role))
                resp = self.capellaAPI.org_ops_apis.create_api_key(self.tenant_id, "name",
                                                            [user], "description",
                                                            resources=[{"id": self.project_id,
                                                                        "roles": [role]}])
                content = resp.json()

                self.assertEqual(resp.status_code, 201)
                self.set_access_keys(content['token'])

                resp = self.capellaAPI.org_ops_apis.delete_api_key(self.tenant_id, content['id'])

                if user == "organizationOwner" or role == "projectOwner":
                    self.assertEqual(resp.status_code, 204)
                else:
                    self.assertEqual(resp.status_code, 403)
                    self.append_to_api_keys(content['id'], self.tenant_id)
                    self.assertEqual(resp.json()['message'], "Access Denied.")

                self.reset_api_keys()

        # Rate limit test

        # resp = self.capellaAPI.org_ops_apis.create_api_key(self.tenant_id, "name",
        #                                                    ["organizationMember"], "description",
        #                                                    allowedCIDRs=['0.0.0.0/0'])
        # content = resp.json()
        # result = self.rate_limit_wrapper(self.capellaAPI.org_ops_apis.delete_api_key,
        #                                  organizationId=self.tenant_id,
        #                                  accessKey=content['accessKey'])

        # self.log.info("Delete api key rate limit response : {}".format(result))
        # self.assertTrue(result["pass"])

    def test_retrieve_api_key(self):
        self.log.info("Retrieve api keys")

        resp = self.capellaAPI.org_ops_apis.create_api_key(self.tenant_id, "name",
                                                    ["organizationMember"], "description",
                                                    allowedCIDRs=['0.0.0.0/0'])
        content = resp.json()

        self.assertEqual(resp.status_code, 201)
        self.append_to_api_keys(content['id'], self.tenant_id)

        access = content['id']

        resp = self.capellaAPI.org_ops_apis.fetch_api_key_info(self.invalid_id, access)
        self.assertEqual(resp.status_code, 403)

        # creating in one organization and accessing using different valid organization
        # resp = self.capellaAPI.org_ops_apis.fetch_api_key_info(self.diff_tenant_id, access)
        # self.assertEqual(resp.status_code, 403)

        resp = self.capellaAPI.org_ops_apis.fetch_api_key_info(self.tenant_id, access[:-3] + "abc")
        self.assertEqual(resp.status_code, 404)

        self.log.info("Retrieving access key using invalid auth")

        self.capellaAPI.org_ops_apis.bearer_token = get_random_secret_key()

        resp = self.capellaAPI.org_ops_apis.fetch_api_key_info(self.tenant_id, access)
        self.assertEqual(resp.status_code, 401)

        self.reset_api_keys()

        self.log.info("Token exp test.")
        resp = self.capellaAPI.org_ops_apis.create_api_key(self.tenant_id, "name",
                                                 ["organizationOwner"], "description", expiry=0.001)
        content = resp.json()
        self.set_access_keys(content['token'])

        time.sleep(90)

        resp = self.capellaAPI.org_ops_apis.fetch_api_key_info(self.tenant_id, access)
        self.assertEqual(resp.status_code, 401)      # AV-57787
        self.reset_api_keys()

        # RBAC
        org_roles = ["organizationOwner", "organizationMember", "projectCreator"]
        project_roles = ["projectOwner", "projectManager", "projectViewer",
                         "projectDataReaderWriter", "projectDataReader"]

        orgMember_with_project_access_resp = self.capellaAPI.org_ops_apis.create_api_key(self.tenant_id, "name",
                                                    ["organizationMember"], "description",
                                                    resources=[{"id": self.project_id,
                                                                "roles": ["projectViewer"]}])
        orgMember_with_project_access_content = orgMember_with_project_access_resp.json()
        self.append_to_api_keys(orgMember_with_project_access_content['id'], self.tenant_id)

        orgMember_without_project_access_resp = self.capellaAPI.org_ops_apis.create_api_key(self.tenant_id, "name",
                                                    ["organizationMember"], "description",
                                                    resources=[{"id": self.project_id,
                                                                "roles": ["projectViewer"]}])

        orgMember_without_project_access_content = orgMember_without_project_access_resp.json()
        self.append_to_api_keys(orgMember_without_project_access_content['id'], self.tenant_id)

        for user in org_roles:
            for role in project_roles:
                self.log.info("Rbac test for {}, {} role".format(user, role))
                resp = self.capellaAPI.org_ops_apis.create_api_key(self.tenant_id, "name",
                                                            [user], "description",
                                                            resources=[{"id": self.project_id,
                                                                        "roles": [role]}])
                content = resp.json()
                self.assertEqual(resp.status_code, 201)
                self.append_to_api_keys(content['id'], self.tenant_id)
                self.set_access_keys(content['token'])

                resp1 = self.capellaAPI.org_ops_apis.fetch_api_key_info(self.tenant_id,
                                                                orgMember_with_project_access_content['id'])
                content1 = resp1.json()

                self.reset_api_keys()
                resp = self.capellaAPI.org_ops_apis.create_api_key(self.tenant_id, "name",
                                                            [user], "description")
                content = resp.json()
                self.assertEqual(resp.status_code, 201)
                self.append_to_api_keys(content['id'], self.tenant_id)
                self.set_access_keys(content['token'])

                resp2 = self.capellaAPI.org_ops_apis.fetch_api_key_info(self.tenant_id,
                                                                orgMember_without_project_access_content['id'])
                content2 = resp2.json()

                if user == "organizationOwner":
                    self.assertEqual(resp1.status_code, 200)
                    self.assertEqual(resp2.status_code, 200)
                    self.assertSetEqual(set(content1.keys()), {"id", "name",
                                                              "description", "expiry",
                                                              "allowedCIDRs", "organizationRoles",
                                                              "audit", "resources"})
                    self.assertSetEqual(set(content2.keys()), {"id", "name",
                                                               "description", "expiry",
                                                               "allowedCIDRs", "organizationRoles",
                                                               "audit", "resources"})
                elif role == "projectOwner":
                    self.assertEqual(resp1.status_code, 200)
                    self.assertEqual(resp2.status_code, 403)
                    self.assertSetEqual(set(content1.keys()), {"id", "name",
                                                              "description", "expiry",
                                                              "allowedCIDRs", "organizationRoles",
                                                              "audit", "resources"})
                    self.assertEqual(content2['message'], "Access Denied.")
                else:
                    self.assertEqual(resp1.status_code, 403)
                    self.assertEqual(resp2.status_code, 403)
                    self.assertEqual(content1['message'], "Access Denied.")
                    self.assertEqual(content2['message'], "Access Denied.")

                self.reset_api_keys()

        # Rate limit test
        # result = self.rate_limit_wrapper(self.capellaAPI.org_ops_apis.fetch_api_key_info,
        #                                  organizationId=self.tenant_id,
        #                                  accessKey=access)

        # self.log.info("Retrieve access key rate limit response : {}".format(result))
        # self.assertTrue(result["pass"])

    def test_create_api_key(self):
        self.log.info("Create api keys")

        resp = self.capellaAPI.org_ops_apis.create_api_key(self.tenant_id, "name",
                                                 ["organizationOwner"], "description",
                                                    allowedCIDRs=['0.0.0.0/0'])
        content = resp.json()
        self.assertEqual(resp.status_code, 201)
        self.append_to_api_keys(content['id'], self.tenant_id)

        # response should not contain extra fields
        content = resp.json()
        self.assertSetEqual(set(content.keys()), {"id", "token"})

        resp = self.capellaAPI.org_ops_apis.create_api_key(self.invalid_id, "name", ["organizationOwner"],
                                                    "description", allowedCIDRs=['0.0.0.0/0'])
        # self.assertEqual(resp.status_code, 401)    # Issue No. :- AV-58166
        # self.assertSetEqual(set(content.keys()), {"errorType", "message"})

        self.log.info("Verifying the status code")

        invalid_values_for_mandatory_string_field = [
            {'value': '', 'expected_code': 422},
            {'value': None, 'expected_code': 422},
            {'value': 'Lorem ipsum' * 30, 'expected_code': 422},
            {'value': 123, 'expected_code': 400},
            {'value': 32.1, 'expected_code': 400}
        ]

        invalid_values_for_optional_string_field = [
            {'value': '', 'expected_code': 201},
            {'value': None, 'expected_code': 201},
            {'value': 'Lorem ipsum' * 30, 'expected_code': 422},
            {'value': 123, 'expected_code': 400},
            {'value': 32.1, 'expected_code': 400}
        ]

        invalid_values_for_optional_numeric_field = [
            {'value': None, 'expected_code': 201},
            {'value': '', 'expected_code': 400},
            {'value': 'null', 'expected_code': 400},
            {'value': '@#$%^&*()!', 'expected_code': 400},
            {'value': 'lorem ipsum', 'expected_code': 400},
            {'value': 'Lorem ipsum' * 30, 'expected_code': 400},
            {'value': '123', 'expected_code': 400},
            {'value': '32.1', 'expected_code': 400}
        ]

        for invalid in invalid_values_for_mandatory_string_field:
            resp = self.capellaAPI.org_ops_apis.create_api_key(self.tenant_id, name=invalid['value'],
                                                        organizationRoles=['organizationOwner'])
            self.assertEqual(resp.status_code, invalid['expected_code'])

            resp = self.capellaAPI.org_ops_apis.create_api_key(self.tenant_id, name='valid',
                                                        organizationRoles=[invalid['value']])
            self.assertEqual(resp.status_code, invalid['expected_code'])

            resp = self.capellaAPI.org_ops_apis.create_api_key(self.tenant_id, 'valid',
                                                        ['organizationOwner'],
                                                        allowedCIDRs=[invalid['value']])
            self.assertEqual(resp.status_code, invalid['expected_code'])

        for invalid in invalid_values_for_optional_string_field:
            resp = self.capellaAPI.org_ops_apis.create_api_key(self.tenant_id, name='valid',
                                                        description=invalid['value'],
                                                        organizationRoles=['organizationOwner'])
            self.assertEqual(resp.status_code, invalid['expected_code'])

        for invalid in invalid_values_for_optional_numeric_field:
            resp = self.capellaAPI.org_ops_apis.create_api_key(self.tenant_id, name='valid',
                                                        expiry=invalid['value'],
                                                        organizationRoles=['organizationOwner'])
            self.assertEqual(resp.status_code, invalid['expected_code'])

        self.log.info("Creating access key using invalid auth")
        self.capellaAPI.org_ops_apis.bearer_token = get_random_secret_key()

        resp = self.capellaAPI.org_ops_apis.create_api_key(self.tenant_id, name="name",
                                                    organizationRoles=["organizationOwner"],
                                                    description="description")

        self.assertEqual(resp.status_code, 401)
        self.reset_api_keys()

        self.log.info("Accessing using different cidr")
        resp = self.capellaAPI.org_ops_apis.create_api_key(self.tenant_id, "name",
                                                 ["organizationOwner"], "description",
                                                    allowedCIDRs=['10.254.254.254/20'])

        content = resp.json()
        self.set_access_keys(content['token'])

        resp = self.capellaAPI.org_ops_apis.create_api_key(self.tenant_id, "name",
                                                 ["organizationOwner"], "description")
        self.assertEqual(resp.status_code, 401)     # Still in development
        self.reset_api_keys()

        self.log.info("Token Expiry Test.")
        resp = self.capellaAPI.org_ops_apis.create_api_key(self.tenant_id, "name",
                                                 ["organizationOwner"], "description", expiry=0.001)

        content = resp.json()
        self.set_access_keys(content['token'])

        time.sleep(90)

        resp = self.capellaAPI.org_ops_apis.create_api_key(self.tenant_id, "name",
                                                 ["organizationOwner"], "description")
        self.assertEqual(resp.status_code, 401)
        self.reset_api_keys()

        # RBAC
        org_roles = ["organizationOwner", "projectCreator", "organizationMember"]
        project_roles = ["projectOwner", "projectManager", "projectViewer",
                         "projectDataReaderWriter", "projectDataReader"]

        for user in org_roles:
            for role in project_roles:
                self.log.info("Rbac test for {}, {} role".format(user, role))
                self.log.info("Using Project level access")

                resp = self.capellaAPI.org_ops_apis.create_api_key(self.tenant_id, "name",
                                                            [user], "description",
                                                            resources=[{"id": self.project_id,
                                                                        "roles": [role]}])
                content = resp.json()
                self.assertEqual(resp.status_code, 201)
                self.append_to_api_keys(content['id'], self.tenant_id)
                self.set_access_keys(content['token'])

                resp1 = self.capellaAPI.org_ops_apis.create_api_key(self.tenant_id, "name",
                                                            [user], "description")

                resp2 = self.capellaAPI.org_ops_apis.create_api_key(self.tenant_id, "name",
                                                             ["projectCreator"], "description",
                                                             resources=[
                                                                 {"id": self.project_id, "roles":
                                                                     ["projectOwner"]}])

                resp3 = self.capellaAPI.org_ops_apis.create_api_key(self.tenant_id, "name",
                                                             ["organizationMember"], "description",
                                                             resources=[
                                                             {"id": self.project_id, "roles":
                                                                 ["projectOwner"]}])

                content1 = resp1.json()
                content2 = resp2.json()
                content3 = resp3.json()

                self.log.info("Using Organization level access")
                self.reset_api_keys()

                resp = self.capellaAPI.org_ops_apis.create_api_key(self.tenant_id, "name",
                                                            [user], "description")
                content = resp.json()
                self.assertEqual(resp.status_code, 201)
                self.append_to_api_keys(content['id'], self.tenant_id)
                self.set_access_keys(content['token'])

                resp4 = self.capellaAPI.org_ops_apis.create_api_key(self.tenant_id, "name",
                                                            [user], "description")
                resp5 = self.capellaAPI.org_ops_apis.create_api_key(self.tenant_id, "name",
                                                             [user], "description",
                                                             resources=[{"id": self.project_id,
                                                                         "roles": [role]}])
                content4 = resp4.json()
                content5 = resp5.json()

                if user == "organizationOwner":
                    self.assertEqual(resp1.status_code, 201)
                    self.assertEqual(resp2.status_code, 201)
                    self.assertEqual(resp3.status_code, 201)
                    self.assertEqual(resp4.status_code, 201)
                    self.assertEqual(resp5.status_code, 201)
                    self.append_to_api_keys(content1['id'], self.tenant_id)
                    self.append_to_api_keys(content2['id'], self.tenant_id)
                    self.append_to_api_keys(content3['id'], self.tenant_id)
                    self.append_to_api_keys(content4['id'], self.tenant_id)
                    self.append_to_api_keys(content5['id'], self.tenant_id)

                elif role == "projectOwner":
                    self.assertEqual(resp1.status_code, 403)
                    self.assertEqual(resp2.status_code, 403)
                    self.assertEqual(resp3.status_code, 201)
                    self.assertEqual(resp4.status_code, 403)
                    self.assertEqual(resp5.status_code, 403)
                    self.append_to_api_keys(content3['id'], self.tenant_id)

                else:
                    self.assertEqual(resp1.status_code, 403)
                    self.assertEqual(resp2.status_code, 403)
                    self.assertEqual(resp3.status_code, 403)
                    self.assertEqual(resp4.status_code, 403)
                    self.assertEqual(resp5.status_code, 403)
                    self.assertEqual(content1['message'], "Access Denied.")
                    self.assertEqual(content2['message'], "Access Denied.")
                    self.assertEqual(content3['message'], "Access Denied.")
                    self.assertEqual(content4['message'], "Access Denied.")
                    self.assertEqual(content5['message'], "Access Denied.")

                self.reset_api_keys()

        # Rate limit test
        # passing invalid organizationRoles so it give 422.
        # result = self.rate_limit_wrapper(method=self.capellaAPI.org_ops_apis.create_api_key,
        #                                  organizationId=self.tenant_id, name="name",
        #                                  organizationRoles=["projectViewer"],
        #                                  description="description",
        #                                  allowedCIDRs=['0.0.0.0/0'])

        # self.log.info("Create ApiKey rate limit response : {}".format(result))
        # self.assertTrue(result["pass"])

    def test_list_api_keys(self):

        self.log.info("list api test")

        resp = self.capellaAPI.org_ops_apis.list_api_keys(self.tenant_id)
        content = resp.json()

        self.assertLessEqual(len(content["data"]), 10)
        self.assertEqual(content['cursor']['pages']['page'], 1)

        page_sizes = [3, 35, 99]  # Specify the different page sizes to test
        total_items = content["cursor"]["pages"]["totalItems"]

        # Iterate through each page size
        for page_size in page_sizes:
            # Calculate total pages based on total items and page size
            total_pages = (total_items + page_size - 1) // page_size

            print("\nTotal items: {}\n".format(total_items))
            print("\nTotal Pages: {}\n".format(total_pages))
            # Randomly select a few pages
            selected_pages = random.sample(range(1, total_pages+1), min(total_pages, 3))
            selected_pages.extend([1, total_pages])

            # Iterate through the selected pages and verify the expected results
            for page in selected_pages:
                expected_items = min(page_size, total_items - (page - 1) * page_size)
                resp = self.capellaAPI.org_ops_apis.list_api_keys(self.tenant_id, perPage=page_size, page=page)
                content = resp.json()
                self.assertEqual(len(content["data"]), expected_items)

        sort_dir_list = ['asc', 'desc']
        sort_by_list = ['name', 'description', 'expiry']

        for sort_dirn in sort_dir_list:
            for sort_by in sort_by_list:
                self.log.info("sorting test for {} in {} order".format(sort_dirn, sort_by))
                resp = self.capellaAPI.org_ops_apis.list_api_keys(self.tenant_id, perPage=1, page=1, sortBy=sort_by,
                                                            sortDirection=sort_dirn)
                content = resp.json()
                expected_items = sorted(content['data'], key=lambda item: item[sort_by],
                                        reverse=(sort_by != 'expiry' and sort_dirn == 'desc' ) |
                                                (sort_by == 'expiry' and sort_dirn == 'asc'))
                self.assertListEqual(content["data"], expected_items)

        # perPage-> invalid -> null
        perPage_test_cases = [
            {
                'perPage': 'invalid',
                'expected_status_code': 400
            },
            {
                'perPage': -1,
                'expected_status_code': 400
            },
            {
                'perPage': 101,
                'expected_status_code': 400
            },
            {
                'perPage': 1.2,
                'expected_status_code': 400
            },
        ]

        for perPage_test_case in perPage_test_cases:
            resp = self.capellaAPI.org_ops_apis.list_api_keys(self.tenant_id, perPage=perPage_test_case['perPage'])

            self.assertEqual(resp.status_code, perPage_test_case['expected_status_code'])

        page_test_cases = [
            {
                'page': 'invalid',
                'expected_status_code': 400
            },
            {
                'page': -1,
                'expected_status_code': 400
            },
            {
                'page': 1.2,
                'expected_status_code': 400
            },
        ]

        for page_test_case in page_test_cases:
            resp = self.capellaAPI.org_ops_apis.list_api_keys(self.tenant_id, page=page_test_case['page'])

            self.assertEqual(resp.status_code, page_test_case['expected_status_code'])

        # test sortBy and sortDirection
        sortBy_test_cases = [
            {
                'sortBy': 'invalid',
                'expected_status_code': 422
            },
            {
                'sortBy': -1,
                'expected_status_code': 422
            },
            {
                'sortBy': 1000000,
                'expected_status_code': 422
            },
            {
                'sortBy': 1.2,
                'expected_status_code': 422
            },
            {
                'sortBy': 'expiresAt',
                'expected_status_code': 422
            },
            {
                'sortBy': 'status',
                'expected_status_code': 422
            },

        ]

        for sortBy_test_case in sortBy_test_cases:
            self.log.info("sorting test for {}.".format(sortBy_test_case['sortBy']))
            resp = self.capellaAPI.org_ops_apis.list_api_keys(self.tenant_id, perPage=25, sortBy=sortBy_test_case['sortBy'])
            # self.assertEqual(resp.status_code, sortBy_test_case['expected_status_code'])

            self.log.info("sorting test for sortDirection.")
            resp = self.capellaAPI.org_ops_apis.list_api_keys(self.tenant_id, perPage=25, sortBy='name',
                                                        sortDirection=sortBy_test_case['sortBy'])
            self.assertEqual(resp.status_code, sortBy_test_case['expected_status_code'])

        # testing different tenant ids
        resp = self.capellaAPI.org_ops_apis.list_api_keys(self.invalid_id)
        self.assertEqual(resp.status_code, 403)

        # resp = self.capellaAPI.org_ops_apis.list_api_keys(self.diff_tenant_id)
        # self.assertEqual(resp.status_code, 403)

        self.log.info("Retrieving access key using invalid auth")

        self.capellaAPI.org_ops_apis.bearer_token = get_random_secret_key()

        resp = self.capellaAPI.org_ops_apis.list_api_keys(self.tenant_id)
        self.assertEqual(resp.status_code, 401)

        self.reset_api_keys()

        self.log.info("Token exp test.")
        resp = self.capellaAPI.org_ops_apis.create_api_key(self.tenant_id, "name",
                                                    ["organizationOwner"], "description", expiry=0.001)
        content = resp.json()
        self.set_access_keys(content['token'])

        time.sleep(90)

        resp = self.capellaAPI.org_ops_apis.list_api_keys(self.tenant_id)
        self.assertEqual(resp.status_code, 401)
        self.reset_api_keys()

        # RBAC
        org_roles = ["organizationOwner", "organizationMember", "projectCreator"]
        project_roles = ["projectOwner", "projectManager", "projectViewer",
                         "projectDataReaderWriter", "projectDataReader"]
        # new user -> project b[organizationMember, projectViewer]
        name_to_sort_at_the_beginning = '~'*20
        # name is 'zzzzzzzzzzzzzzzzzzz' for sorting it to the back
        resp = self.capellaAPI.org_ops_apis.create_api_key(organizationId=self.tenant_id, name=name_to_sort_at_the_beginning,
                                                    organizationRoles=["organizationOwner"], description="description",
                                                    allowedCIDRs=['0.0.0.0/0'],
                                                    resources=[{"id": self.secondary_project_id,
                                                                "roles": ['projectViewer']}],
                                                    )
        content = resp.json()
        self.assertEqual(resp.status_code, 201)
        self.append_to_api_keys(content['id'], self.tenant_id)
        for user in org_roles:
            for role in project_roles:
                self.log.info("Rbac test for {}, {} role".format(user, role))
                resp = self.capellaAPI.org_ops_apis.create_api_key(self.tenant_id, "name",
                                                            [user], "description",
                                                            resources=[{"id": self.project_id,
                                                                        "roles": [role]}])
                content = resp.json()
                self.assertEqual(resp.status_code, 201)
                self.append_to_api_keys(content['id'], self.tenant_id)
                self.set_access_keys(content['token'])

                resp = self.capellaAPI.org_ops_apis.list_api_keys(self.tenant_id, page=1, perPage=100, sortBy='name',
                                                            sortDirection='desc')
                content = resp.json()

                self.assertEqual(resp.status_code, 200)
                filtered_list = filter(lambda d: d.get("name") == name_to_sort_at_the_beginning, content['data'])

                self.log.info("filter: {}".format(filtered_list))

                if user == "organizationOwner":
                    self.assertEqual(len(filtered_list), 1)
                else:
                    self.assertEqual(len(filtered_list), 0)

                self.reset_api_keys()

        # Rate limit test
        # result = self.rate_limit_wrapper(self.capellaAPI.org_ops_apis.list_api_keys, organizationId=self.tenant_id)
        # self.log.info("Rotate rate limit response : {}".format(result))
        # self.assertTrue(result["pass"])

    def test_rotate_api_key(self):
        self.log.info("Rotate api keys")

        resp = self.capellaAPI.org_ops_apis.create_api_key(self.tenant_id, "name",
                                                 ["organizationOwner"], "description",
                                                    allowedCIDRs=['0.0.0.0/0'])
        organizationOwner_apiKey = resp.json()

        self.set_access_keys(organizationOwner_apiKey['token'])
        self.append_to_api_keys(organizationOwner_apiKey['id'], self.tenant_id)

        resp = self.capellaAPI.org_ops_apis.rotate_api_key(self.tenant_id, organizationOwner_apiKey['id'])

        # response should not contain extra fields
        content = resp.json()
        self.assertEqual(resp.status_code, 201)
        print("Content keys: {}".format(content.keys()))
        self.assertSetEqual(set(content.keys()), {"secretKey"})

        # since secret key is changed this request should be unauthenticated.
        resp = self.capellaAPI.org_ops_apis.rotate_api_key(self.tenant_id, organizationOwner_apiKey['id'])
        self.assertEqual(resp.status_code, 401)
        self.reset_api_keys()

        resp = self.capellaAPI.org_ops_apis.rotate_api_key(self.invalid_id, organizationOwner_apiKey['id'])
        self.assertEqual(resp.status_code, 403)

        # resp = self.capellaAPI.org_ops_apis.rotate_api_key(self.diff_tenant_id, organizationOwner_apiKey['id'])
        # self.assertEqual(resp.status_code, 403)

        self.log.info("Creating access key using invalid auth")
        self.capellaAPI.org_ops_apis.bearer_token = get_random_secret_key()

        resp = self.capellaAPI.org_ops_apis.rotate_api_key(self.tenant_id, organizationOwner_apiKey['id'])
        self.assertEqual(resp.status_code, 401)
        self.reset_api_keys()

        self.log.info("Accessing using different cidr")
        resp = self.capellaAPI.org_ops_apis.create_api_key(self.tenant_id, "name",
                                                 ["organizationOwner"], "description",
                                                    allowedCIDRs=['10.254.254.254/20'])

        content = resp.json()
        self.set_access_keys(content['token'])

        resp = self.capellaAPI.org_ops_apis.rotate_api_key(self.tenant_id, organizationOwner_apiKey['id'])
        self.assertEqual(resp.status_code, 401)
        self.reset_api_keys()

        self.log.info("Token Expiry Test.")
        resp = self.capellaAPI.org_ops_apis.create_api_key(self.tenant_id, "name",
                                                 ["organizationOwner"], "description", expiry=0.001)

        content = resp.json()
        self.set_access_keys(content['token'])

        time.sleep(90)

        resp = self.capellaAPI.org_ops_apis.rotate_api_key(self.tenant_id, organizationOwner_apiKey['id'])
        self.assertEqual(resp.status_code, 401)
        self.reset_api_keys()

        # RBAC
        org_roles = ["organizationOwner", "projectCreator", "organizationMember"]
        project_roles = ["projectOwner", "projectManager", "projectViewer",
                         "projectDataReaderWriter", "projectDataReader"]

        resp = self.capellaAPI.org_ops_apis.create_api_key(self.tenant_id, "name",
                                                    ["organizationMember"], "description",
                                                    resources=[{"id": self.project_id,
                                                                "roles": ["projectViewer"]}])
        projectViewer_apiKey = resp.json()
        self.assertEqual(resp.status_code, 201)
        self.append_to_api_keys(projectViewer_apiKey['id'], self.tenant_id)

        for user in org_roles:
            for role in project_roles:
                self.log.info("Rbac test for {}, {} role".format(user, role))
                self.log.info("Using Project level access")

                resp = self.capellaAPI.org_ops_apis.create_api_key(self.tenant_id, "name",
                                                            [user], "description",
                                                            resources=[{"id": self.project_id,
                                                                        "roles": [role]}])
                content = resp.json()
                self.assertEqual(resp.status_code, 201)
                self.append_to_api_keys(content['id'], self.tenant_id)
                self.set_access_keys(content['token'])

                projApiKey_rotates_orgApiKey = self.capellaAPI.org_ops_apis.rotate_api_key(self.tenant_id,
                                                                                organizationOwner_apiKey['id'])
                projApiKey_rotates_projApiKey = self.capellaAPI.org_ops_apis.rotate_api_key(self.tenant_id,
                                                                                    projectViewer_apiKey[
                                                                                        'id'])

                self.log.info("Using Organization level access")
                self.reset_api_keys()

                resp = self.capellaAPI.org_ops_apis.create_api_key(self.tenant_id, "name",
                                                            [user], "description")
                content = resp.json()
                self.assertEqual(resp.status_code, 201)
                self.append_to_api_keys(content['id'], self.tenant_id)
                self.set_access_keys(content['token'])

                orgApiKey_rotates_orgApiKey = self.capellaAPI.org_ops_apis.rotate_api_key(self.tenant_id,
                                                                                organizationOwner_apiKey['id'])
                orgApiKey_rotates_projApiKey = self.capellaAPI.org_ops_apis.rotate_api_key(self.tenant_id,
                                                                                    projectViewer_apiKey[
                                                                                        'id'])

                if user == "organizationOwner":
                    self.assertEqual(projApiKey_rotates_orgApiKey.status_code, 201)

                    self.assertEqual(projApiKey_rotates_projApiKey.status_code, 201)

                    self.assertEqual(orgApiKey_rotates_orgApiKey.status_code, 201)

                    self.assertEqual(orgApiKey_rotates_projApiKey.status_code, 201)

                else:
                    self.assertEqual(projApiKey_rotates_orgApiKey.status_code, 403)
                    self.assertEqual(projApiKey_rotates_orgApiKey.json()['message'], "Access Denied.")

                    self.assertEqual(projApiKey_rotates_projApiKey.status_code, 403)
                    self.assertEqual(projApiKey_rotates_projApiKey.json()['message'], "Access Denied.")

                    self.assertEqual(orgApiKey_rotates_orgApiKey.status_code, 403)
                    self.assertEqual(orgApiKey_rotates_orgApiKey.json()['message'], "Access Denied.")

                    self.assertEqual(orgApiKey_rotates_projApiKey.status_code, 403)
                    self.assertEqual(orgApiKey_rotates_projApiKey.json()['message'], "Access Denied.")

                self.reset_api_keys()

        # Rate limit test
        # result = self.rate_limit_wrapper(self.capellaAPI.org_ops_apis.rotate_api_key, organizationId=self.tenant_id,
        #                         accessKey=organizationOwner_apiKey['id'])

        # self.log.info("Rotate rate limit response : {}".format(result))
        # self.assertTrue(result["pass"])

    def rate_limit_wrapper(self, method=None, **kwargs):

        self.log.info("Rate limit test for method {} \nkwargs :{}".format(method.__name__, kwargs))

        self.reset_api_keys()

        resp_list = [None] * 101

        def call_endpoint(idx):

            for i in range(5):
                resp = method(**kwargs)
                resp_list[idx*5 + i] = resp

        self.log.info("sleep for 1 min, to avoid any effect of the previous test.")
        time.sleep(60)

        st_time = time.time()

        threads = []
        for i in range(20):
            t1 = threading.Thread(target=call_endpoint, args=[i])
            threads.append(t1)

        for t in threads:
            t.start()

        for t in threads:
            t.join()

        end_time = time.time()

        result = {"time taken": end_time - st_time}

        if end_time - st_time <= 60:
            resp = method(**kwargs)
            resp_list[100] = resp
            if resp.status_code == 429:
                result["pass"] = True
            else:
                result["pass"] = False
        else:
            resp = method(**kwargs)
            resp_list[100] = resp
            if resp.status_code == 429:
                result["pass"] = False
            else:
                result["pass"] = True

        self.log.info("sleep for 1 min, to avoid any effect of the current rate limit on other tests.")
        time.sleep(60)

        result['response list'] = resp_list
        return result
