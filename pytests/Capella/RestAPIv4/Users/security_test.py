import json
import threading
import time
import random
import string
from pytests.security.security_base import SecurityBase
from pytests.basetestcase import BaseTestCase
from capellaAPI.capella.dedicated.CapellaAPI_v4 import CapellaAPI


def get_random_dummy_email():
    return 'dummy.user+' + str(random.random()) + '@couchbase.com'


def get_random_secret_key():
    # import random
    return ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(64))


class SecurityTest(SecurityBase):

    def setUp(self):
        SecurityBase.setUp(self)
        # self.url = self.input.capella.get("pod")
        # self.user = self.input.capella.get("capella_user")
        # self.passwd = self.input.capella.get("capella_pwd")
        # self.tenant_id = self.input.capella.get("tenant_id")

        # if self.input.capella.get("diff_tenant_id"):
        #     self.diff_tenant_id = self.input.capella.get("diff_tenant_id")
        # else:
        #     for tenant in self.tenants:
        #         self.diff_tenant_id = tenant.id.encode('utf-8')
        #         # self.diff_tenant_project_ids = tenant.project_id.encode('utf-8')

        # self.capellaAPI = CapellaAPI("https://" + self.url, '',
        #                                       '', self.user, self.passwd)

        # resp = self.capellaAPI.create_control_plane_api_key(self.tenant_id, 'init api keys')
        # resp = resp.json()

        # self.capellaAPI.org_ops_apis.SECRET = resp['secretKey']
        # self.capellaAPI.org_ops_apis.ACCESS = resp['accessKey']

        # self.secret_key_ini = resp['secretKey']
        # self.access_key_ini = resp['accessKey']

        # # create_control_plane_api_key

        # self.project_id = self.tenant.project_id
        resp = self.capellaAPI.org_ops_apis.create_project(self.tenant_id, 'Secondary project')
        self.log.info(resp.json())
        self.secondary_project_id = resp.json()['id']

        # self.cluster_id = self.cluster.id
        # self.invalid_id = "00000000-0000-0000-0000-000000000000"
        self.test_api_keys = []
        self.users = []
        # if self.input.capella.get("test_users"):
        #     self.test_users = json.loads(self.input.capella.get("test_users"))
        # else:
        #     self.test_users = {"User1": {"password": self.passwd, "mailid": self.user,
        #                                  "role": "organizationOwner"}}

    def tearDown(self):
        failures = []
        self.reset_api_keys()

        for user in self.users:
            resp = self.capellaAPI.org_ops_apis.delete_user(
                organizationId=user['tenant'],
                userId=user["user"]
            )
            if resp.status_code != 204:
                failures.append("Error while deleting User {}"
                                .format(user['user']))

        for api_key in self.test_api_keys:
            resp = self.capellaAPI.org_ops_apis.delete_api_key(
                organizationId=api_key['tenant'],
                accessKey=api_key["access_key"]
            )
            if resp.status_code != 204:
                failures.append("Error while deleting api key {}"
                                .format(api_key['access_key']))

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

    def append_to_api_keys(self, acc, tenant):
        self.test_api_keys.append({
            'access_key': acc,
            'tenant': tenant
        })

    def append_to_users(self, user, tenant):
        self.users.append({
            'user': user,
            'tenant': tenant
        })

    def test_invite_user_to_organization(self):
        self.log.info("Create User endpoint")

        resp = self.capellaAPI.org_ops_apis.invite_users_to_organization(
            organizationId=self.tenant_id,
            email=get_random_dummy_email(),
            organizationRoles=['organizationMember'],
            name='valid')

        self.assertEqual(resp.status_code, 201)
        self.append_to_users(resp.json()['id'], self.tenant_id)

        # should not contain extra fields
        self.assertSetEqual(set(resp.json().keys()), {"id"})

        self.log.info("Creating using invalid tenant id")
        resp = self.capellaAPI.org_ops_apis.invite_users_to_organization(
                                                                organizationId=self.invalid_id,
                                                                email=get_random_dummy_email(),
                                                                organizationRoles=['organizationMember'],
                                                                name='valid')

        self.assertEqual(resp.status_code, 403)

        # create with valid
        test_cases = {
            'test 1': {
                'organizationId': self.tenant_id,
                'email': get_random_dummy_email(),
                'organizationRoles': ['organizationMember'],
                'name': 'valid',
                'resources': [],
                'expected_result': {
                    'status_code': 201,
                    'message': 'user created',
                    'fields': ['status_code', 'message']
                }
            },
            'test 2': {
                'organizationId': self.invalid_id,
                'email': get_random_dummy_email(),
                'organizationRoles': ['organizationMember'],
                'name': 'valid',
                'resources': [],
                'expected_result': {
                    'status_code': 403,
                    'message': 'invalid input',
                    'fields': ['status_code', 'message']
                }
            },
            'test 3': {
                'organizationId': self.tenant_id,
                'email': '',
                'organizationRoles': ['organizationMember'],
                'name': 'valid',
                'resources': [],
                'expected_result': {
                    'status_code': 422,
                    'message': 'invalid input',
                    'fields': ['status_code', 'message']
                }
            },
            'test 4': {
                'organizationId': self.tenant_id,
                'email': get_random_dummy_email(),
                'organizationRoles': [],
                'name': 'valid',
                'resources': [],
                'expected_result': {
                    'status_code': 422,
                    'message': 'invalid input',
                    'fields': ['status_code', 'message']
                }
            },
            'test 5': {
                'organizationId': self.tenant_id,
                'email': get_random_dummy_email(),
                'organizationRoles': ['organizationMember'],
                'name': '',
                'resources': [],
                'expected_result': {
                    'status_code': 201,
                    'message': 'invalid input',
                    'fields': ['status_code', 'message']
                }
            },
            'test 6': {
                'organizationId': self.tenant_id,
                'email': 'valid@example.com',
                'organizationRoles': ['organizationMember'],
                'name': 'valid',
                'resources': 'invalid_type',
                'expected_result': {
                    'status_code': 400,
                    'message': 'invalid input',
                    'fields': ['status_code', 'message']
                }
            }
        }
        for key, test in test_cases.items():

            resp = self.capellaAPI.org_ops_apis.invite_users_to_organization(
                                                                    organizationId=test['organizationId'],
                                                                    email=test['email'],
                                                                    organizationRoles=test['organizationRoles'],
                                                                    name=test['name'],
                                                                    resources=test['resources'])
            self.assertEqual(resp.status_code, test['expected_result']['status_code'])
            if test['expected_result']['status_code'] == 201:
                self.append_to_users(resp.json()['id'], self.tenant_id)

        # invalid body
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
            {'value': 123, 'expected_code': 400},
            {'value': 32.1, 'expected_code': 400}
        ]

        for invalid in invalid_values_for_mandatory_string_field:
            resp = self.capellaAPI.org_ops_apis.invite_users_to_organization(self.tenant_id, email=invalid['value'],
                                                                      organizationRoles=['organizationMember'])
            self.assertEqual(resp.status_code, invalid['expected_code'])

            resp = self.capellaAPI.org_ops_apis.invite_users_to_organization(self.tenant_id, email=get_random_dummy_email(),
                                                                      organizationRoles=[invalid['value']])
            self.assertEqual(resp.status_code, invalid['expected_code'])

        for invalid in invalid_values_for_optional_string_field:
            resp = self.capellaAPI.org_ops_apis.invite_users_to_organization(self.tenant_id, email=get_random_dummy_email(),
                                                                      organizationRoles=['organizationMember'],
                                                                      name=invalid['value'])
            self.assertEqual(resp.status_code, invalid['expected_code'])

        # # invalid auth
        self.log.info("Creating User using invalid auth")
        self.capellaAPI.org_ops_apis.bearer_token += '2'
        self.capellaAPI.org_ops_apis.bearer_token = get_random_secret_key()

        resp = self.capellaAPI.org_ops_apis.invite_users_to_organization(
            organizationId=self.tenant_id,
            email=get_random_dummy_email(),
            organizationRoles=['organizationMember'],
            name='valid')

        self.assertEqual(resp.status_code, 401)
        self.reset_api_keys()

        # expired token

        self.log.info("Token Expiry Test.")
        resp = self.capellaAPI.org_ops_apis.create_api_key(self.tenant_id, "name",
                                                 ["organizationOwner"], "description", expiry=0.001)

        content = resp.json()
        self.set_access_keys(content['token'])

        time.sleep(90)

        resp = self.capellaAPI.org_ops_apis.invite_users_to_organization(
            organizationId=self.tenant_id,
            email=get_random_dummy_email(),
            organizationRoles=['organizationMember'],
            name='valid')

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

                # setting up the role is done now the actual test.
                resp1 = self.capellaAPI.org_ops_apis.invite_users_to_organization(organizationId=self.tenant_id,
                                                                           email=get_random_dummy_email(),
                                                                           organizationRoles=[user],
                                                                           name='dummy user')

                resp2 = self.capellaAPI.org_ops_apis.invite_users_to_organization(organizationId=self.tenant_id,
                                                                           email=get_random_dummy_email(),
                                                                           organizationRoles=["projectCreator"],
                                                                           name='dummy user',
                                                                           resources=[
                                                                               {"id": self.project_id,
                                                                                "roles": ["projectOwner"]
                                                                                }])

                resp3 = self.capellaAPI.org_ops_apis.invite_users_to_organization(organizationId=self.tenant_id,
                                                                           email=get_random_dummy_email(),
                                                                           organizationRoles=["organizationMember"],
                                                                           name='dummy user',
                                                                           resources=[
                                                                               {"id": self.project_id,
                                                                                "roles": ["projectOwner"]
                                                                                }])

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

                resp4 = self.capellaAPI.org_ops_apis.invite_users_to_organization(organizationId=self.tenant_id,
                                                                           email=get_random_dummy_email(),
                                                                           organizationRoles=[user],
                                                                           name='dummy user')

                resp5 = self.capellaAPI.org_ops_apis.invite_users_to_organization(organizationId=self.tenant_id,
                                                                           email=get_random_dummy_email(),
                                                                           organizationRoles=[user],
                                                                           name='dummy user',
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
                    self.append_to_users(content1['id'], self.tenant_id)
                    self.append_to_users(content2['id'], self.tenant_id)
                    self.append_to_users(content3['id'], self.tenant_id)
                    self.append_to_users(content4['id'], self.tenant_id)
                    self.append_to_users(content5['id'], self.tenant_id)

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
        # passing 'projectViewer' role, so it won't create new user.
        # result = self.rate_limit_wrapper(method=self.capellaAPI.org_ops_apis.invite_users_to_organization,
        #                                  organizationId=self.tenant_id, email=get_random_dummy_email(),
        #                                  organizationRoles=['projectViewer'],
        #                                  name='valid')

        # self.log.info("invite user rate limit response : {}".format(result))
        # self.assertTrue(result["pass"])

    def test_fetch_user_info(self):
        # fetch_user_info

        self.log.info("Fetch user info")

        resp = self.capellaAPI.org_ops_apis.invite_users_to_organization(
            organizationId=self.tenant_id,
            email=get_random_dummy_email(),
            organizationRoles=['projectCreator'],
            name='valid')

        self.assertEqual(resp.status_code, 201)
        content = resp.json()
        self.append_to_users(content['id'], self.tenant_id)

        user_id = content['id']

        resp = self.capellaAPI.org_ops_apis.fetch_user_info(self.invalid_id, user_id)
        self.assertEqual(resp.status_code, 403)

        resp = self.capellaAPI.org_ops_apis.fetch_user_info(self.tenant_id, user_id[:-3] + "abc")
        self.assertEqual(resp.status_code, 404)

        self.log.info("Retrieving access key using invalid auth")

        self.capellaAPI.org_ops_apis.bearer_token = get_random_secret_key()

        resp = self.capellaAPI.org_ops_apis.fetch_api_key_info(self.tenant_id, user_id)
        self.assertEqual(resp.status_code, 401)

        self.reset_api_keys()

        self.log.info("Token exp test.")
        resp = self.capellaAPI.org_ops_apis.create_api_key(self.tenant_id, "name",
                                                    ["organizationOwner"], "description", expiry=0.001)
        content = resp.json()
        self.set_access_keys(content['token'])

        time.sleep(90)

        resp = self.capellaAPI.org_ops_apis.fetch_user_info(self.tenant_id, user_id)
        self.assertEqual(resp.status_code, 401)  # AV-57787
        self.reset_api_keys()

        # RBAC
        org_roles = ["organizationOwner", "organizationMember", "projectCreator"]
        project_roles = ["projectOwner", "projectManager", "projectViewer",
                         "projectDataReaderWriter", "projectDataReader"]

        resp = self.capellaAPI.org_ops_apis.invite_users_to_organization(
            organizationId=self.tenant_id,
            email=get_random_dummy_email(),
            organizationRoles=['organizationMember'],
            name='valid', resources=[{"id": self.project_id,
                                      "roles": ["projectViewer"]}])

        self.assertEqual(resp.status_code, 201)

        content = resp.json()
        self.append_to_users(content['id'], self.tenant_id)

        project_user_id = content['id']

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

                resp = self.capellaAPI.org_ops_apis.fetch_user_info(self.tenant_id, user_id)
                content = resp.json()

                resp2 = self.capellaAPI.org_ops_apis.fetch_user_info(self.tenant_id, project_user_id)
                content2 = resp2.json()

                if user == "organizationOwner":
                    self.assertEqual(resp.status_code, 200)
                    self.assertEqual(resp2.status_code, 200)
                    self.assertSetEqual(set(content.keys()), {"name", "audit", "inactive",
                                                              "email", "expiresAt",
                                                              "id", "organizationRoles",
                                                              "organizationId", "status"})
                    self.assertSetEqual(set(content2.keys()), {"name", "audit", "inactive",
                                                               "email", "expiresAt", "resources",
                                                               "id", "organizationRoles",
                                                               "organizationId", "status"})
                else:
                    # self.assertEqual(resp.status_code, 403)  # Bug
                    self.assertEqual(resp2.status_code, 200)
                    # self.assertEqual(content['message'], "Access Denied.")

                self.reset_api_keys()

        # Rate limit test
        # result = self.rate_limit_wrapper(method=self.capellaAPI.org_ops_apis.fetch_user_info,
        #                                  organizationId=self.tenant_id, userId=user_id)

        # self.log.info("Fetch user rate limit response : {}".format(result))
        # self.assertTrue(result["pass"])

    def test_list_users_info(self):

        resp = self.capellaAPI.org_ops_apis.list_org_users(self.tenant_id)
        content = resp.json()
        total_users = content['cursor']['pages']['totalItems']
        self.assertLessEqual(len(content["data"]), 10)
        self.assertEqual(content['cursor']['pages']['page'], 1)

        page_sizes = [3, 35, 99]  # Specify the different page sizes to test
        total_items = content["cursor"]["pages"]["totalItems"]

        # Iterate through each page size
        for page_size in page_sizes:
            # Calculate total pages based on total items and page size
            total_pages = (total_items + page_size - 1) // page_size

            # Randomly select a few pages
            selected_pages = random.sample(range(1, total_pages+1), min(total_pages, 3))
            selected_pages.extend([1, total_pages])

            # Iterate through the selected pages and verify the expected results
            for page in selected_pages:
                expected_items = min(page_size, total_items - (page - 1) * page_size)
                resp = self.capellaAPI.org_ops_apis.list_org_users(self.tenant_id, perPage=page_size, page=page)
                content = resp.json()
                self.assertEqual(len(content["data"]), expected_items)

        sort_dir_list = ['asc', 'desc']
        sort_by_list = ['name', 'id', 'email']

        for sort_dirn in sort_dir_list:
            for sort_by in sort_by_list:
                self.log.info("sorting test for {} in {} order".format(sort_dirn, sort_by))
                resp = self.capellaAPI.org_ops_apis.list_org_users(self.tenant_id, perPage=100, page=1, sortBy=sort_by,
                                                            sortDirection=sort_dirn)
                content = resp.json()
                expected_items = sorted(content['data'], key=lambda item: item[sort_by], reverse=(sort_dirn == 'desc'))
                self.assertListEqual(content["data"], expected_items)

        # project based filtering
        resp = self.capellaAPI.org_ops_apis.list_org_users(self.tenant_id, projectId=self.project_id)
        content = resp.json()

        # self.assertNotEquals(content['cursor']['pages']['totalItems'], total_users)       Bug

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
            resp = self.capellaAPI.org_ops_apis.list_org_users(self.tenant_id, perPage=perPage_test_case['perPage'])

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
                'page': 1000000,
                'expected_status_code': 200
            },
            {
                'page': 1.2,
                'expected_status_code': 400
            },
        ]

        for page_test_case in page_test_cases:
            resp = self.capellaAPI.org_ops_apis.list_org_users(self.tenant_id, page=page_test_case['page'])

            self.assertEqual(resp.status_code, page_test_case['expected_status_code'],
                             "Failed for {}".format(page_test_case['page']))

        # test sortBy and sortDirection
        sortBy_test_cases = [
            {
                'sortBy': 'invalid',
                'expected_status_code': 400,
                'expected_status_code_direction': 422
            },
            {
                'sortBy': -1,
                'expected_status_code': 400,
                'expected_status_code_direction': 422
            },
            {
                'sortBy': 1000000,
                'expected_status_code': 400,
                'expected_status_code_direction': 422
            },
            {
                'sortBy': 1.2,
                'expected_status_code': 400,
                'expected_status_code_direction': 422
            },
            {
                'sortBy': 'expiresAt',
                'expected_status_code': 400,
                'expected_status_code_direction': 422
            },
            {
                'sortBy': 'status',
                'expected_status_code': 200,
                'expected_status_code_direction': 422
            },

        ]

        for sortBy_test_case in sortBy_test_cases:
            self.log.info("sorting test for {}.".format(sortBy_test_case['sortBy']))
            resp = self.capellaAPI.org_ops_apis.list_org_users(self.tenant_id, perPage=25, sortBy=sortBy_test_case['sortBy'])
            self.assertEqual(resp.status_code, sortBy_test_case['expected_status_code'],
                             "Failed for {}, Returned: {}, Expected: {}".format(sortBy_test_case['sortBy'],
                                                                                resp.status_code,
                                                                                sortBy_test_case['expected_status_code']))

            self.log.info("sorting test for sortDirection.")
            resp = self.capellaAPI.org_ops_apis.list_org_users(self.tenant_id, perPage=25, sortBy='name',
                                                        sortDirection=sortBy_test_case['sortBy'])
            self.assertEqual(resp.status_code, sortBy_test_case['expected_status_code_direction'],
                             "Failed for {}, Returned: {}, Expected: {}".format(sortBy_test_case['sortBy'],
                                                                                resp.status_code,
                                                                                sortBy_test_case['expected_status_code_direction']))

        # testing different tenant ids
        resp = self.capellaAPI.org_ops_apis.list_org_users(self.invalid_id)
        self.assertEqual(resp.status_code, 403)

        self.log.info("Retrieving access key using invalid auth")

        self.capellaAPI.org_ops_apis.bearer_token = get_random_secret_key()

        resp = self.capellaAPI.org_ops_apis.list_org_users(self.tenant_id)
        self.assertEqual(resp.status_code, 401)

        self.reset_api_keys()

        self.log.info("Token exp test.")
        resp = self.capellaAPI.org_ops_apis.create_api_key(self.tenant_id, "name",
                                                    ["organizationOwner"], "description", expiry=0.001)
        content = resp.json()
        self.set_access_keys(content['token'])

        time.sleep(90)

        resp = self.capellaAPI.org_ops_apis.list_org_users(self.tenant_id)
        self.assertEqual(resp.status_code, 401)
        self.reset_api_keys()

        # RBAC
        org_roles = ["organizationOwner", "organizationMember", "projectCreator"]
        project_roles = ["projectOwner", "projectManager", "projectViewer",
                         "projectDataReaderWriter", "projectDataReader"]
        # new user -> project b[organizationMember, projectViewer]
        dummy_email = get_random_dummy_email()
        # name is 'zzzzzzzzzzzzzzzzzzz' for sorting it to the back
        resp = self.capellaAPI.org_ops_apis.invite_users_to_organization(
            organizationId=self.tenant_id,
            email=dummy_email,
            organizationRoles=['organizationMember'],
            resources=[{"id": self.secondary_project_id,
                        "roles": ['projectViewer']}],
            name='zzzzzzzzzzzzzzzzzzz'
        )
        self.append_to_users(resp.json()['id'], self.tenant_id)

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

                resp = self.capellaAPI.org_ops_apis.list_org_users(self.tenant_id, perPage=100, sortBy='name',
                                                            sortDirection='desc')
                content = resp.json()

                self.assertEqual(resp.status_code, 200)
                filtered_list = filter(lambda d: d.get("email") == dummy_email, content['data'])

                if user == "organizationOwner":
                    self.assertIn('resources', list(filtered_list[0].keys()))
                else:
                    self.assertNotIn('resources', list(filtered_list[0].keys()))

                self.reset_api_keys()

        # Rate limit test
        # result = self.rate_limit_wrapper(method=self.capellaAPI.org_ops_apis.list_org_users,
        #                                  organizationId=self.tenant_id)

        # self.log.info("List user rate limit response : {}".format(result))
        # self.assertTrue(result["pass"])

    def test_delete_user(self):
        # Test with a valid API access key + valid, different and invalid orgID.
        resp = self.capellaAPI.org_ops_apis.invite_users_to_organization(
            organizationId=self.tenant_id,
            email=get_random_dummy_email(),
            organizationRoles=['organizationMember'],
            name='valid')

        content = resp.json()
        test_user = content['id']
        self.assertEqual(resp.status_code, 201)

        resp = self.capellaAPI.org_ops_apis.delete_user(self.invalid_id, content['id'])
        self.assertEqual(resp.status_code, 403)

        resp = self.capellaAPI.org_ops_apis.delete_user(self.tenant_id, content['id'])
        self.assertEqual(resp.status_code, 204)

        # Deleting using Invalid Auth
        resp = self.capellaAPI.org_ops_apis.invite_users_to_organization(
            organizationId=self.tenant_id,
            email=get_random_dummy_email(),
            organizationRoles=['organizationMember'],
            name='valid')
        self.append_to_users(resp.json()['id'], self.tenant_id)

        content = resp.json()
        self.assertEqual(resp.status_code, 201)

        self.log.info("Deleting access key using invalid auth")
        self.capellaAPI.org_ops_apis.bearer_token = get_random_secret_key()

        resp = self.capellaAPI.org_ops_apis.delete_user(self.tenant_id, content['id'])
        self.assertEqual(resp.status_code, 401)

        self.reset_api_keys()

        self.log.info("Token exp test.")
        resp = self.capellaAPI.org_ops_apis.create_api_key(self.tenant_id, "name",
                                                    ["organizationOwner"],
                                                    "description", expiry=0.001)

        api_key_resp = resp.json()
        self.set_access_keys(api_key_resp['token'])

        time.sleep(90)
        resp = self.capellaAPI.org_ops_apis.delete_user(self.tenant_id, content['id'])
        self.assertEqual(resp.status_code, 401)

        self.reset_api_keys()

        # RBAC
        org_roles = ["organizationOwner", "organizationMember", "projectCreator"]
        project_roles = ["projectOwner", "projectManager", "projectViewer",
                         "projectDataReaderWriter", "projectDataReader"]

        for user in org_roles:
            for role in project_roles:
                self.log.info("Rbac test for {}, {} role".format(user, role))

                resp = self.capellaAPI.org_ops_apis.invite_users_to_organization(
                    organizationId=self.tenant_id,
                    email=get_random_dummy_email(),
                    organizationRoles=['organizationMember'],
                    name='valid')
                resp2 = self.capellaAPI.org_ops_apis.invite_users_to_organization(organizationId=self.tenant_id,
                                                                           email=get_random_dummy_email(),
                                                                           organizationRoles=["organizationMember"],
                                                                           name='dummy user',
                                                                           resources=[
                                                                               {"id": self.project_id,
                                                                                "roles": ["projectOwner"]
                                                                                }])

                invite_user_org_role_resp = resp.json()
                invite_user_proj_role_resp = resp2.json()
                self.assertEqual(resp.status_code, 201)
                self.assertEqual(resp2.status_code, 201)

                # Creating api key with given access.
                resp = self.capellaAPI.org_ops_apis.create_api_key(self.tenant_id, "name",
                                                            [user], "description",
                                                            resources=[{"id": self.project_id,
                                                                        "roles": [role]}])
                content = resp.json()

                self.assertEqual(resp.status_code, 201)
                self.append_to_api_keys(content['id'], self.tenant_id)
                self.set_access_keys(content['token'])

                resp = self.capellaAPI.org_ops_apis.delete_user(self.tenant_id, invite_user_org_role_resp['id'])
                resp2 = self.capellaAPI.org_ops_apis.delete_user(self.tenant_id, invite_user_proj_role_resp['id'])

                if user == "organizationOwner":
                    self.assertEqual(resp.status_code, 204)
                    self.assertEqual(resp2.status_code, 204)

                else:
                    self.assertEqual(resp.status_code, 403)
                    self.assertEqual(resp2.status_code, 403)

                    self.append_to_users(invite_user_org_role_resp['id'], self.tenant_id)
                    self.append_to_users(invite_user_proj_role_resp['id'], self.tenant_id)

                    self.assertEqual(resp.json()['message'], "Access Denied.")

                self.reset_api_keys()

        # Rate limit test
        # result = self.rate_limit_wrapper(method=self.capellaAPI.org_ops_apis.delete_user,
        #                                  organizationId=self.tenant_id, userId=test_user)

        # self.log.info("Delete user rate limit response : {}".format(result))
        # self.assertTrue(result["pass"])

    def test_update_user(self):
        self.log.info("Create User endpoint")

        resp = self.capellaAPI.org_ops_apis.invite_users_to_organization(
            organizationId=self.tenant_id,
            email=get_random_dummy_email(),
            organizationRoles=['organizationMember'],
            name='valid')
        self.assertEqual(resp.status_code, 201)

        test_user = resp.json()['id']
        self.append_to_users(test_user, self.tenant_id)

        # update with valid
        test_cases = [
            {
                'organizationId': self.tenant_id,
                'id': test_user,
                "update_info": [{
                    "op": "add",
                    "path": "/organizationRoles",
                    "value": ["projectCreator"]
                }]
            },
            {
                'organizationId': self.tenant_id,
                'id': test_user,
                "update_info": [{
                    "op": "remove",
                    "path": "/organizationRoles",
                    "value": ["projectCreator"]
                }]
            },
            {
                'organizationId': self.tenant_id,
                'id': test_user,
                "update_info": [{
                    "op": "add",
                    "path": "/resources/{0}".format(self.project_id),
                    "value": {
                        "id": self.project_id,
                        "type": "project",
                        "roles": [
                            "projectOwner"
                        ]
                    }
                }]
            },
            {
                'organizationId': self.tenant_id,
                'id': test_user,
                "update_info": [{
                    "op": "add",
                    "path": "/resources/{0}/roles".format(self.project_id),
                    "value": ["projectOwner"]
                }]
            },
            {
                'organizationId': self.tenant_id,
                'id': test_user,
                "update_info": [
                    {
                        "op": "add",
                        "path": "/resources/{0}/roles".format(self.project_id),
                        "value": ["projectOwner"]
                    },
                    {
                        "op": "add",
                        "path": "/resources/{0}/roles".format(self.project_id),
                        "value": ["projectViewer"]
                    }
                ]
            },
            {
                'organizationId': self.tenant_id,
                'id': test_user,
                "update_info": [
                    {
                        "op": "remove",
                        "path": "/resources/{0}/roles".format(self.project_id),
                        "value": ["projectOwner"]
                    },
                    {
                        "op": "remove",
                        "path": "/resources/{0}/roles".format(self.project_id),
                        "value": ["projectViewer"]
                    }
                ]
            },
            {
                'organizationId': self.tenant_id,
                'id': test_user,
                "update_info": [{
                    "op": "remove",
                    "path": "/resources/{0}/roles".format(self.project_id),
                    "value": ["projectOwner"]
                }]
            },
            {
                'organizationId': self.tenant_id,
                'id': test_user,
                "update_info": [{
                    "op": "remove",
                    "path": "/resources/{0}".format(self.project_id)
                }]
            }
        ]

        for test in test_cases:

            self.log.info("Update request for {0}".format(test))

            self.log.info("Update request with valid body and tenant")
            resp = self.capellaAPI.org_ops_apis.update_user(
                                                    organizationId=test['organizationId'],
                                                    userId=test['id'],
                                                    update_info=test['update_info'])
            self.assertEqual(resp.status_code, 200)

            expected_keys = {
                "audit", "email","expiresAt", "id",
                "inactive", "name", "organizationId",
                "organizationRoles", "status", "resources"
            }

            self.assertTrue(set(resp.json().keys()).issubset(expected_keys))

            self.log.info("Updating using invalid tenant id")
            resp = self.capellaAPI.org_ops_apis.update_user(organizationId=self.invalid_id, userId=test_user,
                                                     update_info=test['update_info'])

            self.assertEqual(resp.status_code, 403)

        # test for invalid body
        self.log.info("Test for invalid body")

        invalid_values_for_mandatory_string_field = ['', None, 'Lorem ipsum' * 30, 123, 32.1]

        for invalid_str in invalid_values_for_mandatory_string_field:
            self.log.info("Update request with invalid string: {0}".format(invalid_str))

            resp = self.capellaAPI.org_ops_apis.update_user(
                organizationId=self.tenant_id,
                userId=test_user,
                update_info=[{
                    "op": str(invalid_str),
                    "path": "/organizationRoles",
                    "value": ["projectCreator"]
                }])
            # self.assertEqual(resp.status_code, 422)   Bug

            resp = self.capellaAPI.org_ops_apis.update_user(
                organizationId=self.tenant_id,
                userId=test_user,
                update_info=[{
                    "op": "add",
                    "path": str(invalid_str),
                    "value": ["projectCreator"]
                }])
            self.assertEqual(resp.status_code, 422)

            resp = self.capellaAPI.org_ops_apis.update_user(
                organizationId=self.tenant_id,
                userId=test_user,
                update_info=[{
                    "op": "add",
                    "path": "/organizationRoles",
                    "value": [str(invalid_str)]
                }])
            self.assertEqual(resp.status_code, 422)

        # # expired token

        self.log.info("Token Expiry Test.")
        resp = self.capellaAPI.org_ops_apis.create_api_key(self.tenant_id, "name",
                                                 ["organizationOwner"], "description", expiry=0.001)

        content = resp.json()
        self.set_access_keys(content['token'])

        time.sleep(90)

        resp = self.capellaAPI.org_ops_apis.update_user(
            organizationId=self.tenant_id,
            userId=test_user,
            update_info=[{
                "op": "add",
                "path": "/organizationRoles",
                "value": ["projectCreator"]
            }])
        self.assertEqual(resp.status_code, 401)
        self.reset_api_keys()

        # RBAC
        org_roles = ["organizationOwner", "projectCreator", "organizationMember"]
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
                self.append_to_api_keys(content['id'], self.tenant_id)
                self.set_access_keys(content['token'])

                # setting up the role is done now the actual test.
                resp1 = self.capellaAPI.org_ops_apis.update_user(organizationId=self.tenant_id,
                                                        userId=test_user,
                                                        update_info=[{
                                                            "op": "add",
                                                            "path": "/organizationRoles",
                                                            "value": ["organizationMember"]}])

                resp2 = self.capellaAPI.org_ops_apis.update_user(organizationId=self.tenant_id,
                                                        userId=test_user,
                                                        update_info=[{
                                                                    "op": "add",
                                                                    "path": "/resources/{0}".format(self.project_id),
                                                                    "value": {
                                                                        "id": self.project_id,
                                                                        "type": "project",
                                                                        "roles": [
                                                                            "projectOwner"
                                                                    ]}}
                                                        ])

                resp3 = self.capellaAPI.org_ops_apis.update_user(organizationId=self.tenant_id,
                                                        userId=test_user,
                                                        update_info=[{
                                                                        "op": "add",
                                                                        "path": "/resources/{0}/roles".format(
                                                                            self.project_id),
                                                                        "value": ["projectOwner"]
                                                                    }])

                resp4 = self.capellaAPI.org_ops_apis.update_user(organizationId=self.tenant_id,
                                                          userId=test_user,
                                                          update_info=[{
                                                              "op": "remove",
                                                              "path": "/resources/{0}".format(self.project_id),
                                                              "value": {
                                                                  "id": self.project_id,
                                                                  "type": "project",
                                                                  "roles": [
                                                                      "projectOwner"
                                                                  ]}}
                                                          ])

                resp5 = self.capellaAPI.org_ops_apis.update_user(organizationId=self.tenant_id,
                                                        userId=test_user,
                                                        update_info=[{
                                                                        "op": "remove",
                                                                        "path": "/resources/{0}/roles".format(
                                                                            self.project_id),
                                                                        "value": ["projectOwner"]
                                                                    }])

                content1 = resp1.json()
                content2 = resp2.json()
                content3 = resp3.json()
                content4 = resp4.json()
                content5 = resp5.json()

                if user == "organizationOwner":
                    self.assertEqual(resp1.status_code, 200)
                    self.assertEqual(resp2.status_code, 200)
                    self.assertEqual(resp3.status_code, 200)
                    self.assertEqual(resp4.status_code, 200)
                    self.assertEqual(resp5.status_code, 200)

                elif role == "projectOwner":
                    self.assertEqual(resp1.status_code, 403)
                    self.assertEqual(resp2.status_code, 200)
                    self.assertEqual(resp3.status_code, 200)
                    self.assertEqual(resp4.status_code, 200)
                    self.assertEqual(resp5.status_code, 200)

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
        # result = self.rate_limit_wrapper(method=self.capellaAPI.org_ops_apis.update_user,
        #                                  organizationId=self.tenant_id, userId=test_user,
        #                                  update_info=[{
        #                                      "op": "add",
        #                                      "path": "/organizationRoles",
        #                                      "value": ["organizationMember"]}])

        # self.log.info("Update user rate limit response : {}".format(result))
        # self.assertTrue(result["pass"])

    def rate_limit_wrapper(self, method=None, **kwargs):

        self.log.info("Rate limit test for method {} \nkwargs :{}".format(method.__name__, kwargs))

        self.reset_access_keys_to_default()

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

