import json
import threading
import time
import random
import string
import uuid

from pytests.Capella.RestAPIv4.security_base import SecurityBase
from pytests.basetestcase import BaseTestCase
from capellaAPI.capella.dedicated.CapellaAPI_v4 import CapellaAPI


def get_random_dummy_email():
    return 'dummy.user+' + str(random.random()) + '@couchbase.com'


def get_random_string_of_given_length(uid=False, letters=True, digits=True, special_chars=False, length=64):
    """
    Can be used to generate random string of any length.
    ie. name, description, secret keys, etc.
    default config is for random secret keys and access keys.
    """
    if uid:
        return str(uuid.uuid4())

    characters = ""
    if letters:
        characters += string.ascii_letters
    if digits:
        characters += string.digits
    if special_chars:
        characters += string.punctuation

    return ''.join(random.choice(characters) for _ in range(length))


class SecurityTest(SecurityBase):

    def setUp(self):
        SecurityBase.setUp(self)

        resp = self.capellaAPI.org_ops_apis.create_project(self.tenant_id, 'Secondary project')
        self.log.info(resp.json())
        self.secondary_project_id = resp.json()['id']

        # self.cluster_id = self.cluster.id
        # self.invalid_id = "00000000-0000-0000-0000-000000000000"
        self.test_api_keys = []
        self.users = []
        self.db_creds = []

        # load_sample_bucket
        self.bucket_name = 'travel-sample'
        resp = self.capellaAPI.load_sample_bucket(tenant_id=self.tenant_id, project_id=self.project_id,
                                                  cluster_id=self.cluster_id, bucket_name=self.bucket_name)
        self.assertEqual(resp.status_code, 201)

        self.sleep(30, "Wait after creating bucket")
        resp = self.capellaAPI.cluster_ops_apis.list_buckets(organizationId=self.tenant_id, projectId=self.project_id,
                                                             clusterId=self.cluster_id)

        beer_sample = filter(lambda x: x['name'] == self.bucket_name, resp.json()['data'])
        if len(beer_sample) < 0:
            retry = 5
            while retry >= 0:
                resp = self.capellaAPI.cluster_ops_apis.list_buckets(organizationId=self.tenant_id, projectId=self.project_id,
                                                             clusterId=self.cluster_id)

                beer_sample = filter(lambda x: x['name'] == self.bucket_name, resp.json()['data'])

                if len(beer_sample) > 0:
                    break
                retry -= 1
                self.sleep(10, "No buckets details returned. Retrying....")

            if len(beer_sample) == 0:
                self.fail("Failed to get bucket details")

        self.bucket_id = beer_sample[0]['id']

        self.api_keys_list = self.rbac_roles_generator_wrapper()

    def tearDown(self):
        failures = []
        self.reset_api_keys()

        for user in self.users:

            resp = self.make_call_to_given_method(method=self.capellaAPI.org_ops_apis.delete_user,
                                                  organizationId=user['tenant'],
                                                  userId=user["user"])

            if resp.status_code != 204:
                failures.append("Error while deleting User {}"
                                .format(user['user']))

        for api_key in self.test_api_keys:
            resp = self.make_call_to_given_method(method=self.capellaAPI.org_ops_apis.delete_api_key,
                                                  organizationId=api_key['tenant'],
                                                  accessKey=api_key["access_key"])

            if resp.status_code != 204:
                failures.append("Error while deleting api key {}"
                                .format(api_key['access_key']))

        for db_cred in self.db_creds:
            resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.delete_database_user,
                                                  organizationId=db_cred['tenant'],
                                                  projectId=db_cred['project'],
                                                  clusterId=db_cred['cluster'],
                                                  userId=db_cred['db_id'])

            if resp.status_code != 204:
                failures.append("Error while deleting db credential key {}"
                                .format(db_cred['db_id']))

        resp = self.make_call_to_given_method(method=self.capellaAPI.org_ops_apis.delete_project,
                                              organizationId=self.tenant_id,
                                              projectId=self.secondary_project_id)
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
        self.capellaAPI.org_ops_apis.bearer_token = self.bearer_token_ini

        self.capellaAPI.cluster_ops_apis.ACCESS = self.access_key_ini
        self.capellaAPI.cluster_ops_apis.bearer_token = self.bearer_token_ini

    def set_access_keys(self, accessKey, token):
        self.capellaAPI.org_ops_apis.ACCESS = accessKey
        self.capellaAPI.org_ops_apis.bearer_token = token

        self.capellaAPI.cluster_ops_apis.ACCESS = accessKey
        self.capellaAPI.cluster_ops_apis.bearer_token = token

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

    def append_to_db_creds(self, tenant, project, cluster, db_id):
        self.db_creds.append({
            "tenant": tenant,
            'project': project,
            'cluster': cluster,
            'db_id': db_id
        })

    def make_call_to_given_method(self, method, **kwargs):
        """
        to avoid unnecessary rate limit issues during test.
        """
        resp = method(**kwargs)

        if resp.status_code == 429:
            self.log.info("Rate limit exceeded, sleep for 1 min.")
            time.sleep(60)
            resp = method(**kwargs)

        return resp

    def test_create_database_credentials(self):
        self.log.info("Create database credentials")

        body_list = [
            {
                "name": "ReadWriteOnSpecificCollections",
                "access": [
                    {
                        "privileges": [
                            "data_reader",
                            "data_writer"
                        ],
                        "resources": {
                            "buckets": [
                                {
                                    "name": "travel-sample",
                                    "scopes": [
                                        {
                                            "name": "inventory",
                                            "collections": [
                                                "airport",
                                                "airline"
                                            ]
                                        }
                                    ]
                                }
                            ]
                        }
                    }
                ]
            },
            {
                "name": "ReadWriteOnAllCollectionsInAScopeAndBucket",
                "access": [
                    {
                        "privileges": [
                            "data_reader",
                            "data_writer"
                        ],
                        "resources": {
                            "buckets": [
                                {
                                    "name": "travel-sample",
                                    "scopes": [
                                        {
                                            "name": "inventory"
                                        }
                                    ]
                                }
                            ]
                        }
                    }
                ]
            },
            {
                "name": "ReadAccessForAllBuckets",
                "access": [
                    {
                        "privileges": [
                            "data_reader"
                        ]
                    }
                ]
            },
            {
                "name": "SeparateAccessForDifferentScopes",
                "access": [
                    {
                        "privileges": [
                            "data_reader"
                        ],
                        "resources": {
                            "buckets": [
                                {
                                    "name": "travel-sample",
                                    "scopes": [
                                        {
                                            "name": "inventory"
                                        }
                                    ]
                                }
                            ]
                        }
                    },
                    {
                        "privileges": [
                            "data_writer"
                        ],
                        "resources": {
                            "buckets": [
                                {
                                    "name": "travel-sample",
                                    "scopes": [
                                        {
                                            "name": "sales"
                                        }
                                    ]
                                }
                            ]
                        }
                    }
                ]
            },
            {
                "name": "WriteAccessForAllBuckets",
                "access": [
                    {
                        "privileges": [
                            "data_writer"
                        ]
                    }
                ]
            },
            {
                "name": "MultipleLevelOfAccess",
                "access": [
                    {
                        "privileges": [
                            "data_reader"
                        ],
                        "resources": {
                            "buckets": [
                                {
                                    "name": "travel-sample",
                                    "scopes": [
                                        {
                                            "name": "inventory",
                                            "collections": [
                                                "airport",
                                                "airline"
                                            ]
                                        }
                                    ]
                                }
                            ]
                        }
                    },
                    {
                        "privileges": [
                            "data_writer"
                        ],
                        "resources": {
                            "buckets": [
                                {
                                    "name": "travel-sample",
                                    "scopes": [
                                        {
                                            "name": "inventory",
                                            "collections": [
                                                "sales"
                                            ]
                                        }
                                    ]
                                }
                            ]
                        }
                    }
                ]
            }
        ]

        for body in body_list:

            resp = self.capellaAPI.cluster_ops_apis.create_database_user(organizationId=self.tenant_id,
                                                                         projectId=self.project_id,
                                                                         clusterId=self.cluster_id,
                                                                         name=get_random_string_of_given_length(
                                                                             length=10),
                                                                         access=body['access']
                                                                         )
            # self.assertEqual(resp.status_code, 201)        # AV-59843
            # self.assertEqual(set(resp.json().keys()), {"id", "password"})
            if resp.status_code == 201:
                self.append_to_db_creds(self.tenant_id, self.project_id, self.cluster_id, resp.json()['id'])

        resp = self.capellaAPI.cluster_ops_apis.create_database_user(organizationId=self.invalid_id,
                                                                     projectId=self.project_id,
                                                                     clusterId=self.cluster_id,
                                                                     name=get_random_string_of_given_length(
                                                                         length=10),
                                                                     access=body_list[0]['access']
                                                                     )
        self.assertEqual(resp.status_code, 403)   #bug https://couchbasecloud.atlassian.net/browse/AV-59990

        resp = self.capellaAPI.cluster_ops_apis.create_database_user(organizationId=self.tenant_id,
                                                                     projectId=get_random_string_of_given_length(
                                                                         uid=True),
                                                                     clusterId=self.cluster_id,
                                                                     name=get_random_string_of_given_length(
                                                                         length=10),
                                                                     access=body_list[0]['access']
                                                                     )
        self.assertEqual(resp.status_code, 404)   #Bug https://couchbasecloud.atlassian.net/browse/AV-59990

        resp = self.capellaAPI.cluster_ops_apis.create_database_user(organizationId=self.tenant_id,
                                                                     projectId=self.project_id,
                                                                     clusterId=get_random_string_of_given_length(
                                                                         uid=True),
                                                                     name=get_random_string_of_given_length(
                                                                         length=10),
                                                                     access=body_list[0]['access']
                                                                     )
        self.assertEqual(resp.status_code, 404)

        name_combinations = ['', None, get_random_string_of_given_length(length=300),
                             get_random_string_of_given_length(length=10)]

        privileges_combinations = ['', None, get_random_string_of_given_length(length=300), [''], [None],
                                   [get_random_string_of_given_length(length=300)], ['data_reader']]

        resource_combinations = [{'extra': []}, {}]

        bucket_combinations = [[], [''],
                               [{"name": self.bucket_name,
                                 "scopes": [
                                     {
                                         "name": "inventor",
                                         "collections": [
                                             "airline"
                                         ]
                                     }]
                                 }]
                               ]
        req_body = {}

        for name in name_combinations:

            for privileges in privileges_combinations:

                for resource in resource_combinations:

                    for bucket in bucket_combinations:
                        req_body['name'] = name
                        req_body['privileges'] = privileges
                        req_body['resources'] = resource
                        req_body['resources']['buckets'] = bucket

                        resp = self.make_call_to_given_method(
                            method=self.capellaAPI.cluster_ops_apis.create_database_user,
                            organizationId=self.tenant_id,
                            projectId=self.project_id,
                            clusterId=self.cluster_id,
                            name=get_random_string_of_given_length(length=10),
                            access=req_body)

                        self.assertEqual(resp.status_code, 400)

        self.authentication_token_test_wrapper(method=self.capellaAPI.cluster_ops_apis.create_database_user,
                                               organizationId=self.tenant_id,
                                               projectId=self.project_id,
                                               clusterId=self.cluster_id,
                                               name=get_random_string_of_given_length(length=10),
                                               access=body_list[0]['access'])

        # RBAC
        for api_key in self.api_keys_list:
            self.log.info("Rbac test for organizationRole: {}, projectRole: {}, project: {}"
                          .format(api_key['organizationRole'],
                                  api_key['projectRole'],
                                  api_key['description']))
            self.set_access_keys(api_key['accessKey'], api_key['token'])

            resp = self.make_call_to_given_method(
                method=self.capellaAPI.cluster_ops_apis.create_database_user,
                organizationId=self.tenant_id,
                projectId=self.project_id,
                clusterId=self.cluster_id,
                name=get_random_string_of_given_length(length=10),
                access=body_list[0]['access'])

            if resp.status_code == 201:
                self.append_to_db_creds(self.tenant_id, self.project_id, self.cluster_id, resp.json()['id'])

            if api_key['organizationRole'] == 'organizationOwner':
                self.assertEqual(resp.status_code, 201)
            elif api_key['projectRole'] == 'projectOwner':
                if api_key['description'] == 'same project':
                    self.assertEqual(resp.status_code, 201) # bug https://couchbasecloud.atlassian.net/browse/AV-59991
                    pass
                else:
                    self.assertEqual(resp.status_code, 403)
            else:
                self.assertEqual(resp.status_code, 403)

            self.reset_api_keys()

        # Rate limit test
        # result = self.rate_limit_wrapper(method=self.capellaAPI.cluster_ops_apis.create_database_user,
        #                                  organizationId=self.tenant_id,
        #                                  projectId=self.project_id,
        #                                  clusterId=self.cluster_id,
        #                                  name=get_random_string_of_given_length(length=10),
        #                                  access=body_list[0]['access'])

        # for res in result['response list']:
        #     # to delete any db credential created accidentally. should not create more than one key,
        #     # as we are passing a single unique name.
        #     if res.status_code == 201:
        #         self.append_to_db_creds(self.tenant_id, self.project_id, self.cluster_id, res.json()['id'])

        # self.log.info("invite user rate limit response : {}".format(result))
        # self.assertTrue(result["pass"])

    def test_fetch_database_credentials(self):

        self.log.info("Fetch database credentials")
        access = [
            {
                "privileges": [
                    "data_reader",
                    "data_writer"
                ],
                "resources": {
                    "buckets": [
                        {
                            "name": "travel-sample",
                            "scopes": [
                                {
                                    "name": "inventory",
                                    "collections": [
                                        "airport",
                                        "airline"
                                    ]
                                }
                            ]
                        }
                    ]
                }
            }
        ]
        resp = self.capellaAPI.cluster_ops_apis.create_database_user(organizationId=self.tenant_id,
                                                                     projectId=self.project_id,
                                                                     clusterId=self.cluster_id,
                                                                     name=get_random_string_of_given_length(
                                                                         length=10),
                                                                     access=access)

        db_cred = resp.json()
        self.append_to_db_creds(self.tenant_id, self.project_id, self.cluster_id, db_cred['id'])

        resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.fetch_database_user_info,
                                              organizationId=self.tenant_id,
                                              projectId=self.project_id,
                                              clusterId=self.cluster_id,
                                              userId=db_cred['id'])
        self.assertEqual(resp.status_code, 200)
        resp = resp.json()
        self.assertTrue(len(resp['access']) > 0)

        resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.fetch_database_user_info,
                                              organizationId=self.invalid_id,
                                              projectId=self.project_id,
                                              clusterId=self.cluster_id,
                                              userId=db_cred['id'])
        self.assertEqual(resp.status_code, 403)       # bug https://couchbasecloud.atlassian.net/browse/AV-59990

        resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.fetch_database_user_info,
                                              organizationId=self.tenant_id,
                                              projectId=get_random_string_of_given_length(uid=True),
                                              clusterId=self.cluster_id,
                                              userId=db_cred['id'])
        self.assertEqual(resp.status_code, 404)            # bug https://couchbasecloud.atlassian.net/browse/AV-59990

        resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.fetch_database_user_info,
                                              organizationId=self.tenant_id,
                                              projectId=self.secondary_project_id,
                                              clusterId=self.cluster_id,
                                              userId=db_cred['id'])
        self.assertEqual(resp.status_code, 404)           # bug https://couchbasecloud.atlassian.net/browse/AV-59990

        resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.fetch_database_user_info,
                                              organizationId=self.tenant_id,
                                              projectId=self.project_id,
                                              clusterId=get_random_string_of_given_length(uid=True),
                                              userId=db_cred['id'])
        self.assertEqual(resp.status_code, 404)

        self.authentication_token_test_wrapper(method=self.capellaAPI.cluster_ops_apis.fetch_database_user_info,
                                               organizationId=self.tenant_id,
                                               projectId=self.project_id,
                                               clusterId=get_random_string_of_given_length(uid=True),
                                               userId=db_cred['id']
                                               )

        # RBAC
        for api_key in self.api_keys_list:

            self.log.info("Rbac test for organizationRole: {}, projectRole: {}, project: {}"
                          .format(api_key['organizationRole'],
                                  api_key['projectRole'],
                                  api_key['description']))

            self.set_access_keys(api_key['accessKey'], api_key['token'])

            resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.fetch_database_user_info,
                                                  organizationId=self.tenant_id,
                                                  projectId=self.project_id,
                                                  clusterId=self.cluster_id,
                                                  userId=db_cred['id']
                                                  )

            if api_key['organizationRole'] == 'organizationOwner':
                self.assertEqual(resp.status_code, 200)
            elif api_key['projectRole'] in ["projectOwner", "projectManager", "projectViewer"]:
                if api_key['description'] == 'same project':
                    self.assertEqual(resp.status_code, 200)
                else:
                    self.assertEqual(resp.status_code, 403) # bug https://couchbasecloud.atlassian.net/browse/AV-59997
            else:
                self.assertEqual(resp.status_code, 403) # bug https://couchbasecloud.atlassian.net/browse/AV-59997

            self.reset_api_keys()

        # Rate limit test
        # result = self.rate_limit_wrapper(method=self.capellaAPI.cluster_ops_apis.fetch_database_user_info,
        #                                  organizationId=self.tenant_id,
        #                                  projectId=self.project_id,
        #                                  clusterId=self.cluster_id,
        #                                  userId=db_cred['id'])

        # self.log.info("Fetch user rate limit response : {}".format(result))
        # # self.assertTrue(result["pass"])

    def test_delete_database_credentials(self):

        self.log.info("delete database credentials")
        access = [
            {
                "privileges": [
                    "data_reader",
                    "data_writer"
                ],
                "resources": {
                    "buckets": [
                        {
                            "name": "travel-sample",
                            "scopes": [
                                {
                                    "name": "inventory",
                                    "collections": [
                                        "airport",
                                        "airline"
                                    ]
                                }
                            ]
                        }
                    ]
                }
            }
        ]
        resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.create_database_user,
                                              organizationId=self.tenant_id,
                                              projectId=self.project_id,
                                              clusterId=self.cluster_id,
                                              name=get_random_string_of_given_length(length=10),
                                              access=access)

        db_cred = resp.json()

        resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.delete_database_user,
                                              organizationId=self.invalid_id,
                                              projectId=self.project_id,
                                              clusterId=self.cluster_id,
                                              userId=db_cred['id'])
        self.assertEqual(resp.status_code, 403) # Bug https://couchbasecloud.atlassian.net/browse/AV-59990

        resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.delete_database_user,
                                              organizationId=self.tenant_id,
                                              projectId=get_random_string_of_given_length(uid=True),
                                              clusterId=self.cluster_id,
                                              userId=db_cred['id'])
        self.assertEqual(resp.status_code, 404)   # Bug https://couchbasecloud.atlassian.net/browse/AV-59990

        resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.delete_database_user,
                                              organizationId=self.tenant_id,
                                              projectId=self.secondary_project_id,
                                              clusterId=self.cluster_id,
                                              userId=db_cred['id'])
        self.assertEqual(resp.status_code, 404)  # Bug https://couchbasecloud.atlassian.net/browse/AV-59990

        resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.delete_database_user,
                                              organizationId=self.tenant_id,
                                              projectId=self.project_id,
                                              clusterId=get_random_string_of_given_length(uid=True),
                                              userId=db_cred['id'])
        self.assertEqual(resp.status_code, 404)

        self.authentication_token_test_wrapper(method=self.capellaAPI.cluster_ops_apis.delete_database_user,
                                               organizationId=self.tenant_id,
                                               projectId=self.project_id,
                                               clusterId=get_random_string_of_given_length(uid=True),
                                               userId=db_cred['id'])

        resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.delete_database_user,
                                              organizationId=self.tenant_id,
                                              projectId=self.project_id,
                                              clusterId=self.cluster_id,
                                              userId=db_cred['id'])
        self.assertEqual(resp.status_code, 204)

        for api_key in self.api_keys_list:

            self.log.info("Rbac test for organizationRole: {}, projectRole: {}, project: {}"
                          .format(api_key['organizationRole'],
                                  api_key['projectRole'],
                                  api_key['description']))

            resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.create_database_user,
                                                  organizationId=self.tenant_id,
                                                  projectId=self.project_id,
                                                  clusterId=self.cluster_id,
                                                  name=get_random_string_of_given_length(length=10),
                                                  access=access)
            db_crd = resp.json()
            self.set_access_keys(api_key['accessKey'], api_key['token'])

            resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.delete_database_user,
                                                  organizationId=self.tenant_id,
                                                  projectId=self.project_id,
                                                  clusterId=self.cluster_id,
                                                  userId=db_crd['id'])

            if api_key['organizationRole'] == 'organizationOwner':
                self.assertEqual(resp.status_code, 204)
            elif api_key['projectRole'] == "projectOwner":
                if api_key['description'] == 'same project':
                    self.assertEqual(resp.status_code, 204) # Bug https://couchbasecloud.atlassian.net/browse/AV-60016
                else:
                    self.assertEqual(resp.status_code, 403)
            else:
                self.assertEqual(resp.status_code, 403)

            if resp.status_code != 204:
                self.append_to_db_creds(self.tenant_id, self.project_id, self.cluster_id, db_crd['id'])

            self.reset_api_keys()

        # Rate limit test
        # result = self.rate_limit_wrapper(method=self.capellaAPI.cluster_ops_apis.delete_database_user,
        #                                  organizationId=self.tenant_id,
        #                                  projectId=self.project_id,
        #                                  clusterId=self.cluster_id,
        #                                  userId=db_cred['id'])

        # self.log.info("Delete user rate limit response : {}".format(result))
        # # self.assertTrue(result["pass"]) https://couchbasecloud.atlassian.net/browse/AV-60017

    def test_list_database_credentials(self):

        resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.list_database_users,
                                              organizationId=self.tenant_id,
                                              projectId=self.project_id,
                                              clusterId=self.cluster_id)
        self.assertEqual(resp.status_code, 200)

        resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.list_database_users,
                                              organizationId=self.invalid_id,
                                              projectId=self.project_id,
                                              clusterId=self.cluster_id)
        self.assertEqual(resp.status_code, 403)       # bug https://couchbasecloud.atlassian.net/browse/AV-59990

        resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.list_database_users,
                                              organizationId=self.tenant_id,
                                              projectId=get_random_string_of_given_length(uid=True),
                                              clusterId=self.cluster_id)
        self.assertEqual(resp.status_code, 404)            # bug https://couchbasecloud.atlassian.net/browse/AV-59990

        resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.list_database_users,
                                              organizationId=self.tenant_id,
                                              projectId=self.secondary_project_id,
                                              clusterId=self.cluster_id)
        self.assertEqual(resp.status_code, 404)           # bug https://couchbasecloud.atlassian.net/browse/AV-59990

        resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.list_database_users,
                                              organizationId=self.tenant_id,
                                              projectId=self.project_id,
                                              clusterId=get_random_string_of_given_length(uid=True))
        self.assertEqual(resp.status_code, 404)

        self.authentication_token_test_wrapper(method=self.capellaAPI.cluster_ops_apis.list_database_users,
                                               organizationId=self.tenant_id,
                                               projectId=self.project_id,
                                               clusterId=get_random_string_of_given_length(uid=True))

        # RBAC
        for api_key in self.api_keys_list:

            self.log.info("Rbac test for organizationRole: {}, projectRole: {}, project: {}"
                          .format(api_key['organizationRole'],
                                  api_key['projectRole'],
                                  api_key['description']))

            self.set_access_keys(api_key['accessKey'], api_key['token'])

            resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.list_database_users,
                                                  organizationId=self.tenant_id,
                                                  projectId=self.project_id,
                                                  clusterId=self.cluster_id)

            if api_key['organizationRole'] == 'organizationOwner':
                self.assertEqual(resp.status_code, 200)
            elif api_key['projectRole'] in ["projectOwner", "projectManager", "projectViewer"]:
                if api_key['description'] == 'same project':
                    self.assertEqual(resp.status_code, 200)
                else:
                    self.assertEqual(resp.status_code, 403)  # bug https://couchbasecloud.atlassian.net/browse/AV-59997
            else:
                self.assertEqual(resp.status_code, 403)  # bug https://couchbasecloud.atlassian.net/browse/AV-59997

            self.reset_api_keys()

        # Rate limit test
        # result = self.rate_limit_wrapper(method=self.capellaAPI.cluster_ops_apis.list_database_users,
        #                                  organizationId=self.tenant_id,
        #                                  projectId=self.project_id,
        #                                  clusterId=self.cluster_id)

        # self.log.info("List user rate limit response : {}".format(result))
        # self.assertTrue(result["pass"])

    def test_update_database_credentials(self):

        self.log.info("Update database credentials")
        access = [
            {
                "privileges": [
                    "data_reader",
                    "data_writer"
                ],
                "resources": {
                    "buckets": [
                        {
                            "name": "travel-sample",
                            "scopes": [
                                {
                                    "name": "inventory",
                                    "collections": [
                                        "airport",
                                        "airline"
                                    ]
                                }
                            ]
                        }
                    ]
                }
            }
        ]
        resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.create_database_user,
                                              organizationId=self.tenant_id,
                                              projectId=self.project_id,
                                              clusterId=self.cluster_id,
                                              name=get_random_string_of_given_length(length=10),
                                              access=access)

        db_cred = resp.json()

        resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.update_database_user,
                                              organizationId=self.invalid_id,
                                              projectId=self.project_id,
                                              clusterId=self.cluster_id,
                                              userId=db_cred['id'],
                                              ifmatch=False,
                                              access=access)
        self.assertEqual(resp.status_code, 403) # Bug https://couchbasecloud.atlassian.net/browse/AV-59990

        resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.update_database_user,
                                              organizationId=self.tenant_id,
                                              projectId=get_random_string_of_given_length(uid=True),
                                              clusterId=self.cluster_id,
                                              userId=db_cred['id'],
                                              ifmatch=False,
                                              access=access)
        self.assertEqual(resp.status_code, 404)   # Bug https://couchbasecloud.atlassian.net/browse/AV-59990

        resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.update_database_user,
                                              organizationId=self.tenant_id,
                                              projectId=self.secondary_project_id,
                                              clusterId=self.cluster_id,
                                              userId=db_cred['id'],
                                              ifmatch=False,
                                              access=access)
        self.assertEqual(resp.status_code, 404)  # Bug https://couchbasecloud.atlassian.net/browse/AV-59990

        resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.update_database_user,
                                              organizationId=self.tenant_id,
                                              projectId=self.project_id,
                                              clusterId=get_random_string_of_given_length(uid=True),
                                              userId=db_cred['id'],
                                              ifmatch=False,
                                              access=access)
        self.assertEqual(resp.status_code, 404)

        self.authentication_token_test_wrapper(method=self.capellaAPI.cluster_ops_apis.update_database_user,
                                               organizationId=self.tenant_id,
                                               projectId=self.project_id,
                                               clusterId=self.cluster_id,
                                               userId=db_cred['id'],
                                               ifmatch=False,
                                               access=access)

        # RBAC
        for api_key in self.api_keys_list:

            self.log.info("Rbac test for organizationRole: {}, projectRole: {}, project: {}"
                          .format(api_key['organizationRole'],
                                  api_key['projectRole'],
                                  api_key['description']))

            self.set_access_keys(api_key['accessKey'], api_key['token'])

            resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.update_database_user,
                                                  organizationId=self.tenant_id,
                                                  projectId=self.project_id,
                                                  clusterId=self.cluster_id,
                                                  userId=db_cred['id'],
                                                  ifmatch=False,
                                                  access=access)

            if api_key['organizationRole'] == 'organizationOwner':
                self.assertEqual(resp.status_code, 204)
            elif api_key['projectRole'] == "projectOwner":
                if api_key['description'] == 'same project':
                    self.assertEqual(resp.status_code, 204) # Bug https://couchbasecloud.atlassian.net/browse/AV-60016
                else:
                    self.assertEqual(resp.status_code, 403)
            else:
                self.assertEqual(resp.status_code, 403)

        # Rate limit test
        # result = self.rate_limit_wrapper(method=self.capellaAPI.cluster_ops_apis.update_database_user,
        #                                  organizationId=self.tenant_id, projectId=self.project_id,
        #                                  clusterId=self.cluster_id, userId=db_cred['id'], ifmatch=False, access=access)

        # self.log.info("Update user rate limit response : {}".format(result))
        # self.assertTrue(result["pass"])  # bug https://couchbasecloud.atlassian.net/browse/AV-60017

    def rate_limit_wrapper(self, method=None, **kwargs):
        """
        make 100 request to the given endpoint using method and kwargs.
        using multiple threads just to ensure all the api call are completed in the window of
        1 minute.
        if it still exceeds 1 minute then we are passing the test.
        """

        self.log.info("Rate limit test for method {} \nkwargs :{}".format(method.__name__, kwargs))

        self.reset_access_keys_to_default()

        resp_list = [None] * 101

        def call_endpoint(idx):

            for i in range(5):
                resp = method(**kwargs)
                resp_list[idx * 5 + i] = resp

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

    def authentication_token_test_wrapper(self, method=None, **kwargs):
        """
        Contains test for empty ,invalid token authentication, accessing from a different ip (allowed cidr),
        expired token.
        """
        self.log.info("Authentication test for method {} \nkwargs :{}".format(method.__name__, kwargs))

        self.log.info("Passing empty bearer token")
        self.set_access_keys(self.capellaAPI.org_ops_apis.ACCESS, '')

        resp = self.make_call_to_given_method(method=method, **kwargs)
        self.assertEqual(resp.status_code, 401)
        self.reset_api_keys()

        self.log.info("Creating access key using invalid auth")
        self.set_access_keys(self.capellaAPI.org_ops_apis.ACCESS,
                             get_random_string_of_given_length(
                                 length=len(self.capellaAPI.org_ops_apis.bearer_token)))

        resp = self.make_call_to_given_method(method=method, **kwargs)
        self.assertEqual(resp.status_code, 401)
        self.reset_api_keys()

        self.log.info("Accessing using different cidr")

        resp = self.make_call_to_given_method(method=self.capellaAPI.org_ops_apis.create_api_key,
                                              organizationId=self.tenant_id, name="name", expiry=1,
                                              organizationRoles=["organizationOwner"],
                                              description="description", allowedCIDRs=['10.254.254.254/20'])

        content = resp.json()

        self.append_to_api_keys(content['id'], self.tenant_id)
        self.set_access_keys(content['id'], content['token'])

        resp = self.make_call_to_given_method(method=method, **kwargs)
        # self.assertEqual(resp.status_code, 403)       # getting 404 instead, what should be the status code for this?
        self.reset_api_keys()

        self.log.info("Token Expiry Test.")
        resp = self.make_call_to_given_method(method=self.capellaAPI.org_ops_apis.create_api_key,
                                              organizationId=self.tenant_id, name="name",
                                              organizationRoles=["organizationOwner"],
                                              description="description", expiry=0.001)

        content = resp.json()
        self.set_access_keys(content['id'], content['token'])

        self.log.info("Sleep for 90 seconds for api-key to get expired.")
        time.sleep(90)

        resp = self.make_call_to_given_method(method=method, **kwargs)
        self.assertEqual(resp.status_code, 401)
        self.reset_api_keys()

    def rbac_roles_generator_wrapper(self):
        """
        generates all the possible combinations of roles using org level and project roles, combining with project
        level access, access to a secondary project and no project access.
        """

        # RBAC
        org_roles = ["organizationOwner", "projectCreator", "organizationMember"]
        project_roles = ["projectOwner", "projectManager", "projectViewer",
                         "projectDataReaderWriter", "projectDataReader"]

        api_keys_list = []
        for user in org_roles:
            for role in project_roles:
                self.log.info("Rbac test for {}, {} role".format(user, role))
                self.log.info("Using Project level access")

                resp = self.make_call_to_given_method(method=self.capellaAPI.org_ops_apis.create_api_key,
                                                      organizationId=self.tenant_id, name="name", expiry=1,
                                                      organizationRoles=[user], description="description",
                                                      resources=[{"id": self.project_id,
                                                                  "roles": [role]}])

                content = resp.json()
                self.assertEqual(resp.status_code, 201)
                self.append_to_api_keys(content['id'], self.tenant_id)

                api_keys_list.append({
                    'accessKey': content["id"],
                    'token': content['token'],
                    'organizationRole': user,
                    'projectId': self.project_id,
                    'projectRole': role,
                    'description': 'same project'
                })

                resp = self.make_call_to_given_method(method=self.capellaAPI.org_ops_apis.create_api_key,
                                                      organizationId=self.tenant_id, name="name", expiry=1,
                                                      organizationRoles=[user], description="description",
                                                      resources=[{"id": self.secondary_project_id,
                                                                  "roles": [role]}])

                content = resp.json()
                self.assertEqual(resp.status_code, 201)
                self.append_to_api_keys(content['id'], self.tenant_id)

                api_keys_list.append({
                    'accessKey': content["id"],
                    'token': content['token'],
                    'organizationRole': user,
                    'projectId': self.secondary_project_id,
                    'projectRole': role,
                    'description': 'secondary project'
                })

                self.log.info("Using Organization level access")

                resp = self.make_call_to_given_method(method=self.capellaAPI.org_ops_apis.create_api_key,
                                                      organizationId=self.tenant_id, name="name", expiry=1,
                                                      organizationRoles=[user], description="description")

                content = resp.json()
                self.assertEqual(resp.status_code, 201)
                self.append_to_api_keys(content['id'], self.tenant_id)

                api_keys_list.append({
                    'accessKey': content["id"],
                    'token': content['token'],
                    'organizationRole': user,
                    'projectId': self.project_id,
                    'projectRole': '',
                    'description': 'no project'
                })

        return api_keys_list
