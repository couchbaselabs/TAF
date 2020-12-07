from cbas.cbas_base import CBASBaseTest
from TestInput import TestInputSingleton
import random, json, copy, os
from rbac_utils.Rbac_ready_functions import RbacUtils
from com.couchbase.client.java.json import JsonObject
from couchbase_helper.documentgenerator import DocumentGenerator
from remote.remote_util import RemoteMachineShellConnection, RemoteUtilHelper
from threading import Thread
from awsLib.s3_data_helper import perform_S3_operation, S3DataHelper
from couchbase_helper.tuq_helper import N1QLHelper
from cbas_utils.cbas_utils import Dataset

rbac_users_created = {}

class CBASExternalLinks(CBASBaseTest):

    def setUp(self):
        self.input = TestInputSingleton.input
        if "default_bucket" not in self.input.test_params:
            self.input.test_params.update({"default_bucket": False})
        super(CBASExternalLinks, self).setUp()

        self.aws_access_key, self.aws_secret_key, self.aws_session_token = self.get_aws_credentials()

        self.dataset_created = False
        self.link_created = False
        self.aws_bucket_created = False
        self.speed_limit_set = False

        self.service_endpoint = None
        self.aws_bucket_name = self.input.test_params.get("aws_bucket_name",
                                                          "cbas-regression-{0}".format(random.randint(1, 1000)))
        self.aws_region_list = perform_S3_operation(
            aws_access_key=self.aws_access_key, aws_secret_key=self.aws_secret_key,
            aws_session_token=self.aws_session_token, get_regions=True)
        self.region = random.choice(self.aws_region_list)

        self.rbac_util = RbacUtils(self.cluster.master)

        # Create analytics admin user on Analytics cluster
        self.analytics_username = "admin_analytics"
        self.rbac_util._create_user_and_grant_role(self.analytics_username, "analytics_admin")
        rbac_users_created[self.analytics_username] = "analytics_admin"

        self.invalid_value = "invalid"
        self.invalid_ip = "127.0.0.99"
        self.invalid_region = "us-west-6"
        self.log.info("================================================================")
        self.log.info("SETUP has finished")
        self.log.info("================================================================")

    def tearDown(self):
        self.log.info("================================================================")
        self.log.info("TEARDOWN has started")
        self.log.info("================================================================")
        if self.speed_limit_set:
            for server in self.cluster.servers:
                RemoteUtilHelper.clear_all_speed_restrictions(server)
        if self.aws_bucket_created:
            if not perform_S3_operation(
                    aws_access_key=self.aws_access_key, aws_secret_key=self.aws_secret_key,
                    aws_session_token=self.aws_session_token, create_bucket=False,
                    bucket_name=self.aws_bucket_name, delete_bucket=True):
                self.log.error("Error while deleting AWS S3 bucket.")

        if self.dataset_created:
            self.cbas_util.drop_dataset(cbas_dataset_name=self.dataset_params["cbas_dataset_name"],
                                        dataverse=self.dataset_params["dataverse"])
        if self.link_created:
            self.cbas_util.drop_link_on_cbas(link_name="{0}.{1}".format(
                self.link_info["dataverse"],
                self.link_info["name"]))

        if hasattr(self, "dataverse_map"):
            for dataverse, links in self.dataverse_map.iteritems():
                for link in links:
                    self.cbas_util.drop_link_on_cbas(link_name="{0}.{1}".format(
                        dataverse, link))
                self.cbas_util.drop_dataverse_on_cbas(dataverse)

        self.create_or_delete_users(self.rbac_util, rbac_users_created, delete=True)

        self.cluster_util.reset_cluster()
        super(CBASExternalLinks, self).tearDown()
        self.log.info("================================================================")
        self.log.info("Teardown has finished")
        self.log.info("================================================================")

    def get_aws_credentials(self, user='full_access'):
        with open("/etc/aws_config.json", "r") as fh:
            cred = json.loads(fh.read())
        return cred[user]["aws_access_key"], cred[user]["aws_secret_key"], cred[user]["aws_session_token"]

    def get_link_property_dict(self, access_key, secret_key, serviceEndpoint=None,
                               create_dataverse=False, dataverse_cardinality=1):
        """
        Creates a dict of all the properties required to create external link to
        AWS S3 bucket.
        """
        if create_dataverse:
            dataverse_name = Dataset.create_name_with_cardinality(dataverse_cardinality)
            if not self.cbas_util.validate_dataverse_in_metadata(
                dataverse_name) and not self.cbas_util.create_dataverse_on_cbas(
                    dataverse_name=Dataset.format_name_for_error(True,dataverse_name)):
                self.fail("Creation of Dataverse {0} failed".format(
                    dataverse_name))
        else:
            dataverse_name = "Default"
        self.link_info = dict()
        self.link_info["dataverse"] = Dataset.format_name_for_error(True,dataverse_name)
        self.link_info["name"] = "newAwsLink"
        self.link_info["type"] = "s3"
        self.link_info["region"] = self.region
        self.link_info["accessKeyId"] = access_key
        self.link_info["secretAccessKey"] = secret_key
        self.link_info["serviceEndpoint"] = serviceEndpoint

    def get_dataset_parameters(self, cbas_dataset_name=None, aws_bucket_name=None, link_name=None,
                               object_construction_def=None, dataverse="Default",
                               path_on_aws_bucket=None, file_format="json", redact_warning=None,
                               header=None, null_string=None, include=None, exclude=None,
                               validate_error_msg=False, username=None,
                               password=None, expected_error=None, expected_error_code=None,
                               links_dataverse=None):
        self.dataset_params = dict()
        if cbas_dataset_name:
            self.dataset_params["cbas_dataset_name"] = cbas_dataset_name
        else:
            self.dataset_params["cbas_dataset_name"] = self.cbas_dataset_name
        if aws_bucket_name:
            self.dataset_params["aws_bucket_name"] = aws_bucket_name
        else:
            self.dataset_params["aws_bucket_name"] = self.aws_bucket_name
        if link_name:
            self.dataset_params["link_name"] = link_name
        else:
            self.dataset_params["link_name"] = self.link_info["name"]
        self.dataset_params["object_construction_def"] = object_construction_def
        self.dataset_params["dataverse"] = dataverse
        self.dataset_params["path_on_aws_bucket"] = path_on_aws_bucket
        self.dataset_params["file_format"] = file_format
        self.dataset_params["redact_warning"] = redact_warning
        self.dataset_params["header"] = header
        self.dataset_params["null_string"] = null_string
        self.dataset_params["include"] = include
        self.dataset_params["exclude"] = exclude
        self.dataset_params["validate_error_msg"] = validate_error_msg
        if username:
            self.dataset_params["username"] = username
        else:
            self.dataset_params["username"] = self.analytics_username
        self.dataset_params["password"] = password
        self.dataset_params["expected_error"] = expected_error
        self.dataset_params["expected_error_code"] = expected_error_code
        if links_dataverse:
            self.dataset_params["links_dataverse"] = links_dataverse
        else:
            self.dataset_params["links_dataverse"] = self.link_info["dataverse"]

    def setup_for_dataset(self, create_dataverse=False, dataverse_cardinality=1):
        retry = 0
        while (not self.aws_bucket_created) and retry < 3:
            try:
                self.log.info("Creating AWS bucket - {0}".format(self.aws_bucket_name))
                if not perform_S3_operation(
                        aws_access_key=self.aws_access_key, aws_secret_key=self.aws_secret_key,
                        aws_session_token=self.aws_session_token, create_bucket=True,
                        bucket_name=self.aws_bucket_name, region=self.region):
                    self.fail("Creating S3 bucket - {0}. Failed.".format(self.aws_bucket_name))
                self.aws_bucket_created = True
            except Exception as err:
                self.aws_bucket_name = self.input.test_params.get("aws_bucket_name", "cbas-regression-{0}".format(
                    random.randint(1, 1000)))
                retry += 1
        self.get_link_property_dict(self.aws_access_key, self.aws_secret_key,
                                    create_dataverse=create_dataverse, 
                                    dataverse_cardinality=dataverse_cardinality)
        if not self.cbas_util.create_external_link_on_cbas(link_properties=self.link_info):
            self.fail("link creation failed")
        self.link_created = True
        self.get_dataset_parameters()
        
        self.shell = RemoteMachineShellConnection(self.cluster.master)
        self.n1ql_helper = N1QLHelper(shell=self.shell, buckets=self.bucket_util.buckets,
                                      item_flag=0, n1ql_port=8093, log=self.log,
                                      input=self.input, master=self.cluster.master, 
                                      use_rest=True)
        
        self.s3_data_helper = S3DataHelper(
            aws_access_key=self.aws_access_key, aws_secret_key=self.aws_secret_key,
            aws_session_token=self.aws_session_token, cluster=self.cluster,
            bucket_util=self.bucket_util, rest=self.rest, task=self.task, log=self.log, 
            n1ql_helper=self.n1ql_helper)

    def execute_cbas_query(self, cbas_query, result, timeout=120, analytics_timeout=120):
        status, metrics, errors, cbas_result, handle = self.cbas_util.execute_statement_on_cbas_util(cbas_query,
                                                                                                     timeout=timeout,
                                                                                                     analytics_timeout=analytics_timeout)
        result.extend([status, cbas_result, errors])

    def perform_delete_recreate_file_on_AWS_S3(self, bucket_name, filename, dest_path, delete_only=False):
        if not perform_S3_operation(
                aws_access_key=self.aws_access_key, aws_secret_key=self.aws_secret_key,
                aws_session_token=self.aws_session_token, bucket_name=bucket_name, delete_file=True,
                file_path=filename):
            self.fail("Error while deleting file from S3")
        if not delete_only:
            if not perform_S3_operation(
                    aws_access_key=self.aws_access_key, aws_secret_key=self.aws_secret_key,
                    aws_session_token=self.aws_session_token, bucket_name=bucket_name, upload_file=True,
                    src_path=dest_path, dest_path=filename):
                self.fail("Error while uploading file from S3")

    def get_n1ql_query(self, query_name):
        query_dict = {
            "count_all": "Select count(*) from `{0}`;",
            "count_from_csv": "select count(*) from {0} where filename like '%.csv';",
            "count_from_tsv": "select count(*) from {0} where filename like '%.tsv';",
            "count_from_json": "select count(*) from {0} where filename like '%.json';",
            "invalid_folder": "select count(*) from {0} where folder like 'invalid/invalid';",
            "valid_folder": "select count(*) from {0} where folder like '{1}%';"
        }
        return query_dict[query_name]

    def get_include_exclude_flag(self, include=True, key_name='0'):
        include_dict = {
            "1": ["file_[1234567890].json", "*.json"],
            "2": ["file_[1234567890].csv", "*.csv"],
            "3": ["file_[1234567890].tsv", "*.tsv"],
            "4": 'file_?.[!ct]s[!v]*',
            "5": 'file_?.[!t]sv',
            "6": 'file_?.[!c]sv'
        }
        exclude_dict = {
            "1": ["*.csv", "*.tsv"],
            "2": ["*.json", "*.tsv"],
            "3": ["*.json", "*.csv"],
            "4": 'file_?*.[ct]s[!o]',
            "5": 'file_?*.[t]s[!v]',
            "6": 'file_?*.[c]s[!v]'
        }
        if include:
            return include_dict.get(key_name, None)
        else:
            return exclude_dict.get(key_name, None)

    def test_create_external_link(self):
        # Test parameters
        no_of_dataverse = int(self.input.test_params.get("dataverse", 0))
        no_of_link = int(self.input.test_params.get("link", 1))

        # Check for link creation failure scenario
        if no_of_link == 1:
            
            if self.input.param("multipart_dataverse", False):
                self.get_link_property_dict(self.aws_access_key, self.aws_secret_key, None ,True, 2)
                invalid_dv = "invalid.invalid"
            else:
                self.get_link_property_dict(self.aws_access_key, self.aws_secret_key)
                invalid_dv = "invalid"

            # Create users with all RBAC roles.
            self.create_or_delete_users(self.rbac_util, rbac_users_created)

            testcases = [
                {
                    "description": "Create a link with a non-existent dataverse",
                    "dataverse": invalid_dv,
                    "expected_error": "Cannot find dataverse with name {0}".format(self.invalid_value)
                },
                {
                    "description": "Create a link with an invalid region name",
                    "region": self.invalid_region,
                    "validate_error_msg": False
                },
                {
                    "description": "Create a link with an invalid credentials",
                    "secretAccessKey": self.invalid_value,
                    "validate_error_msg": False
                },
                {
                    "description": "Create a link with an invalid service endpoint",
                    "serviceEndpoint": self.invalid_value,
                    "validate_error_msg": False
                },
                {
                    "description": "Create a link with a name that already exists in the dataverse",
                    "recreate_link": True,
                    "validate_error_msg": False,
                    "expected_error": "Link {0} already exists".format(
                        Dataset.format_name_for_error(True, self.link_info["dataverse"],
                                                      self.link_info["name"]))
                },
                {
                    "description": "Create a link with a name of form Local*",
                    "name": "Local123",
                    "expected_error": 'Links starting with \"Local\" are reserved by the system'
                }
            ]

            rbac_testcases = self.create_testcase_for_rbac_user("Create a link using {0} user",
                                                                rbac_users_created)
            for testcase in rbac_testcases:
                if testcase["validate_error_msg"]:
                    testcase["expected_error"] = "Unauthorized user"
            testcases = testcases + rbac_testcases

            failed_testcases = list()

            for testcase in testcases:
                try:
                    self.log.info(testcase["description"])

                    link_properties = copy.deepcopy(self.link_info)

                    # Copying link property values for the testcase, to link_property dict, to create a new link.
                    for key, value in testcase.iteritems():
                        if key in link_properties:
                            link_properties[key] = value

                    if not self.cbas_util.create_external_link_on_cbas(
                            link_properties=link_properties,
                            validate_error_msg=testcase.get("validate_error_msg", True),
                            expected_error=testcase.get("expected_error", None),
                            username=testcase.get("username", self.analytics_username)):
                        raise Exception("Error while creating link")

                    if testcase.get("recreate_link", False):
                        testcase["validate_error_msg"] = True
                        if not self.cbas_util.create_external_link_on_cbas(
                                link_properties=link_properties,
                                validate_error_msg=testcase.get("validate_error_msg", True),
                                expected_error=testcase.get("expected_error", None),
                                username=testcase.get("username", self.analytics_username)):
                            raise Exception("Error message is different than expected.")
                        testcase["validate_error_msg"] = False

                    if not testcase.get("validate_error_msg", True):
                        self.cbas_util.drop_link_on_cbas(
                            link_name="{0}.{1}".format(link_properties["dataverse"],
                                                       link_properties["name"]),
                            username=self.analytics_username)

                except Exception as err:
                    self.log.error(str(err))
                    failed_testcases.append(testcase["description"])
            if failed_testcases:
                self.fail("Following testcases failed - {0}".format(str(failed_testcases)))
        else:
            # Check for multiple link creation scenario
            self.dataverse_map = self.create_dataverse_link_map(self.cbas_util,
                                                                no_of_dataverse,
                                                                no_of_link)
            try:
                for dataverse, links in self.dataverse_map.iteritems():
                    for link in links:
                        self.dataverse_map[dataverse][link]["dataverse"] = dataverse
                        self.dataverse_map[dataverse][link]["name"] = link
                        self.dataverse_map[dataverse][link]["type"] = "s3"
                        self.dataverse_map[dataverse][link]["region"] = random.choice(self.aws_region_list)
                        self.dataverse_map[dataverse][link]["accessKeyId"] = self.aws_access_key
                        self.dataverse_map[dataverse][link]["serviceEndpoint"] = self.service_endpoint
                        self.dataverse_map[dataverse][link]["secretAccessKey"] = self.aws_secret_key
                        if self.cbas_util.create_external_link_on_cbas(
                                link_properties=self.dataverse_map[dataverse][link],
                                username=self.analytics_username):
                            response = self.cbas_util.get_link_info(link_name=link, dataverse=dataverse,
                                                                    link_type="s3")
                            if not (response[0]["accessKeyId"] == self.dataverse_map[dataverse][link]["accessKeyId"]):
                                self.fail("Access key ID does not match. Expected - {0}\nActual - {1}".format(
                                    self.dataverse_map[dataverse][link]["accessKeyId"],
                                    response[0]["accessKeyId"]))
                            if not (response[0]["region"] == self.dataverse_map[dataverse][link]["region"]):
                                self.fail("Region does not match. Expected - {0}\nActual - {1}".format(
                                    self.dataverse_map[dataverse][link]["region"],
                                    response[0]["region"]))
                            if not (response[0]["secretAccessKey"] == "<redacted sensitive entry>"):
                                self.fail("secretAccessKey does not match. Expected - {0}\nActual - {1}".format(
                                    "<redacted sensitive entry>", response[0]["secretAccessKey"]))
                            if not (response[0]["serviceEndpoint"] == self.dataverse_map[dataverse][link][
                                "serviceEndpoint"]):
                                self.fail("serviceEndpoint does not match. Expected - {0}\nActual - {1}".format(
                                    self.dataverse_map[dataverse][link]["serviceEndpoint"],
                                    response[0]["serviceEndpoint"]))
                        else:
                            self.fail("Link creation failed.")
            except Exception as err:
                self.fail("Exception Occured - " + str(err))

    def test_list_external_links(self):
        if self.input.param("multipart_dataverse", False):
            self.get_link_property_dict(self.aws_access_key, self.aws_secret_key, None, True, 2)
        else:
            self.get_link_property_dict(self.aws_access_key, self.aws_secret_key)
        
        if self.cbas_util.create_external_link_on_cbas(link_properties=self.link_info,
                                                       username=self.analytics_username):
            self.link_created = True
            self.create_or_delete_users(self.rbac_util, rbac_users_created)

            testcases = [
                {
                    "expected_hits": 1,
                    "description": "Parameters Passed - None",
                },
                {
                    "dataverse": self.link_info["dataverse"],
                    "expected_hits": 1,
                    "description": "Parameters Passed - Dataverse",
                },
                {
                    "dataverse": self.link_info["dataverse"],
                    "name": self.link_info["name"],
                    "expected_hits": 1,
                    "description": "Parameters Passed - Dataverse, Name",
                },
                {
                    "type": "s3",
                    "expected_hits": 1,
                    "description": "Parameters Passed - Type",
                },
                {
                    "name": self.link_info["name"],
                    "expected_hits": 0,
                    "description": "Parameters Passed - Name",
                    "validate_error_msg": True,
                    "expected_error": "You must specify 'dataverse' if 'name' is specified"
                },
                {
                    "dataverse": self.link_info["dataverse"],
                    "name": self.link_info["name"],
                    "type": "s3",
                    "expected_hits": 1,
                    "description": "Parameters Passed - Dataverse, Name and Type",
                }
            ]
            rbac_testcases = self.create_testcase_for_rbac_user("List external links using {0} user",
                                                                rbac_users_created)
            for testcase in rbac_testcases:
                testcase["dataverse"] = self.link_info["dataverse"]
                testcase["name"] = self.link_info["name"]
                testcase["type"] = "s3"
                if testcase["username"] in ["admin_analytics"]:
                    testcase["validate_error_msg"] = False
                if testcase["validate_error_msg"]:
                    testcase["expected_hits"] = 0
                    testcase["expected_error"] = "Unauthorized user"
                else:
                    testcase["expected_hits"] = 1

            testcases = testcases + rbac_testcases
            failed_testcases = list()

            for testcase in testcases:
                try:
                    self.log.info(testcase["description"])
                    response = self.cbas_util.get_link_info(link_name=testcase.get("name", None),
                                                            dataverse=testcase.get("dataverse", None),
                                                            link_type=testcase.get("type", None),
                                                            validate_error_msg=testcase.get("validate_error_msg",
                                                                                            False),
                                                            expected_error=testcase.get("expected_error", None),
                                                            username=testcase.get("username", self.analytics_username))
                    if testcase.get("validate_error_msg", False):
                        if not response:
                            raise Exception("Error message is different than expected.")
                    else:
                        if not (len(response) == testcase["expected_hits"]):
                            raise Exception(
                                "Expected links - {0} \t Actual links - {1}".format(testcase["expected_hits"],
                                                                                    len(response)))
                        if not (response[0]["dataverse"] == Dataset.format_name_for_error(
                            True,self.link_info["dataverse"])):
                            raise Exception("Expected - {0} \t Actual- {1}".format(self.link_info["dataverse"],
                                                                                   response[0]["dataverse"]))
                        if not (response[0]["name"] == self.link_info["name"]):
                            raise Exception("Expected - {0} \t Actual- {1}".format(self.link_info["name"],
                                                                                   response[0]["name"]))
                        if not (response[0]["type"] == self.link_info["type"]):
                            raise Exception("Expected - {0} \t Actual- {1}".format(self.link_info["type"],
                                                                                   response[0]["type"]))
                        if not (response[0]["accessKeyId"] == self.link_info["accessKeyId"]):
                            raise Exception("Access key ID does not match. Expected - {0}\nActual - {1}".format(
                                self.link_info["accessKeyId"], response[0]["accessKeyId"]))
                        if not (response[0]["region"] == self.link_info["region"]):
                            raise Exception("Region does not match. Expected - {0}\nActual - {1}".format(
                                self.link_info["region"], response[0]["region"]))
                        if not (response[0]["secretAccessKey"] == "<redacted sensitive entry>"):
                            raise Exception("secretAccessKey does not match. Expected - {0}\nActual - {1}".format(
                                "<redacted sensitive entry>", response[0]["secretAccessKey"]))
                        if not (response[0]["serviceEndpoint"] == self.link_info["serviceEndpoint"]):
                            raise Exception("serviceEndpoint does not match. Expected - {0}\nActual - {1}".format(
                                self.link_info["serviceEndpoint"], response[0]["serviceEndpoint"]))
                except Exception as err:
                    self.log.error(str(err))
                    failed_testcases.append(testcase["description"])
            if failed_testcases:
                self.fail("Following testcases failed - {0}".format(str(failed_testcases)))
        else:
            self.fail("Link creation failed")

    def test_alter_link_properties(self):
        if self.input.param("multipart_dataverse", False):
            self.setup_for_dataset(True, 2)
            invalid_dv = "invalid.invalid"
        else:
            self.setup_for_dataset()
            invalid_dv = "invalid"

        # Create users with all RBAC roles.
        self.create_or_delete_users(self.rbac_util, rbac_users_created)
        
        self.dataset_params["dataverse"] = self.link_info["dataverse"]

        aws_access_key_1, aws_secret_key_1, aws_session_token_1 = self.get_aws_credentials("full_access_2")
        region2 = random.choice(self.remove_and_return_new_list(self.aws_region_list, self.link_info["region"]))

        testcases = [
            {
                "description": "Changing dataverse to another existing dataverse",
                "validate_error_msg": True,
                "expected_error": "Link {0} does not exist".format(self.link_info["name"]),
                "new_dataverse": True
            },
            {
                "description": "Changing dataverse to a non-existing dataverse",
                "dataverse": invalid_dv,
                "validate_error_msg": True,
                "expected_error": "Cannot find dataverse with name {0}".format(self.invalid_value)
            },
            {
                "description": "Changing link type",
                "type": "couchbase",
                "validate_error_msg": True,
                "expected_error": "Link type for an existing link cannot be changed"
            },
            {
                "description": "Changing accessKeyId and secretAccessKey to another set of working credentials",
                "accessKeyId": aws_access_key_1,
                "secretAccessKey": aws_secret_key_1,
            },
            {
                "description": "Changing accessKeyId and secretAccessKey to another set of non-working credentials",
                "secretAccessKey": self.invalid_value,
            },
            {
                "description": "Changing region",
                "region": region2,
            },
            {
                "description": "Changing region to a non-existent region",
                "region": self.invalid_region
            },
            {
                "description": "Changing serviceEndpoint to a new valid serviceEndpoint",
                "serviceEndpoint": "ec2.{0}.amazonaws.com".format(region2)
            },
            {
                "description": "Changing serviceEndpoint to an invalid serviceEndpoint",
                "serviceEndpoint": self.invalid_value
            }
        ]
        rbac_testcases = self.create_testcase_for_rbac_user("Altering external link properties using {0} user",
                                                            rbac_users_created)
        for testcase in rbac_testcases:
            if testcase["validate_error_msg"]:
                testcase["expected_error"] = "Unauthorized user"
        testcases = testcases + rbac_testcases
        failed_testcases = list()

        if not self.cbas_util.create_dataset_on_external_resource(**self.dataset_params):
            self.fail("Failed to create dataset")
        self.dataset_created = True

        result = self.s3_data_helper.generate_data_for_s3_and_upload(
            aws_bucket_name=self.dataset_params["aws_bucket_name"],
            key=self.key,
            no_of_files=1,
            file_formats=["json"],
            no_of_folders=0,
            max_folder_depth=0,
            header=False,
            null_key="",
            operation="create",
            bucket=self.bucket_util.buckets[0],
            no_of_docs=10,
            randomize_header=False,
            large_file=False,
            missing_field=[False])
        if result:
            self.fail("Error while uploading files to S3")

        cbas_query = "Select count(*) from {0};".format(
            Dataset.format_name_for_error(True, self.dataset_params["dataverse"],
                                          self.dataset_params["cbas_dataset_name"]))
        status, metrics, errors, cbas_result, handle = self.cbas_util.execute_statement_on_cbas_util(
            cbas_query, timeout=120, analytics_timeout=120)

        for testcase in testcases:
            try:
                self.log.info(testcase["description"])

                link_properties = copy.deepcopy(self.link_info)

                # Copying link property values for the testcase, to link_property dict, to create a new link.
                for key, value in testcase.iteritems():
                    if key in link_properties:
                        link_properties[key] = value

                if testcase.get("new_dataverse", False):
                    link_properties["dataverse"] = "dataverse2"
                    if not self.cbas_util.create_dataverse_on_cbas(dataverse_name=link_properties["dataverse"],
                                                                   username=self.analytics_username):
                        raise Exception("Dataverse creation failed")

                response = self.cbas_util.update_external_link_properties(
                    link_properties, validate_error_msg=testcase.get("validate_error_msg", False),
                    expected_error=testcase.get("expected_error", None),
                    username=testcase.get("username", self.analytics_username))

                if testcase.get("validate_error_msg", False):
                    if not response:
                        raise Exception("Error message is different than expected.")
                else:
                    status, metrics, errors, cbas_result_2, handle = self.cbas_util.execute_statement_on_cbas_util(
                        cbas_query, timeout=120, analytics_timeout=120)
                    if status == "fatal":
                        if self.link_info["region"] != link_properties["region"]:
                            pass
                        elif errors and "External source error" in errors[0]["msg"]:
                            pass
                        else:
                            raise Exception(
                                "Following error is raised while reading data from AWS after altering the link\nERROR : {0}"
                                    .format(str(errors)))

                    elif cbas_result[0]["$1"] != cbas_result_2[0]["$1"]:
                        raise Exception("Data read from AWS before and after altering link do not match")

                    response2 = self.cbas_util.get_link_info(link_name=link_properties["name"],
                                                             dataverse=link_properties["dataverse"])[0]
                    if not (response2["accessKeyId"] == link_properties["accessKeyId"]):
                        raise Exception("Access key ID does not match. Expected - {0}\nActual - {1}".format(
                            link_properties["accessKeyId"], response2["accessKeyId"]))
                    if not (response2["region"] == link_properties["region"]):
                        raise Exception("Region does not match. Expected - {0}\nActual - {1}".format(
                            link_properties["region"], response2["region"]))
                    if not (response2["secretAccessKey"] == "<redacted sensitive entry>"):
                        raise Exception("secretAccessKey does not match. Expected - {0}\nActual - {1}".format(
                            "<redacted sensitive entry>", response2["secretAccessKey"]))
                    if not (response2["serviceEndpoint"] == link_properties["serviceEndpoint"]):
                        raise Exception("serviceEndpoint does not match. Expected - {0}\nActual - {1}".format(
                            link_properties["serviceEndpoint"], response2["serviceEndpoint"]))

                if testcase.get("new_dataverse", False):
                    if not self.cbas_util.drop_dataverse_on_cbas(dataverse_name=link_properties["dataverse"],
                                                                 username=self.analytics_username):
                        raise Exception("Dataverse creation failed")

                self.log.info("Test Passed")

            except Exception as err:
                self.log.error(str(err))
                failed_testcases.append(testcase["description"])

        if failed_testcases:
            self.fail("Following testcases failed - {0}".format(str(failed_testcases)))

    def test_connect_link(self):
        self.get_link_property_dict(self.aws_access_key, self.aws_secret_key)
        if self.cbas_util.create_external_link_on_cbas(link_properties=self.link_info):
            self.link_created = True
            if not self.cbas_util.connect_link(
                    link_name="{0}.{1}".format(self.link_info["dataverse"], self.link_info["name"]),
                    validate_error_msg=True,
                    expected_error="Link {0}.{1} cannot be used for data ingestion".format(self.link_info["dataverse"],
                                                                                           self.link_info["name"])):
                self.fail("Expected error message different from actual error message")
        else:
            self.fail("Link creation failed")

    def test_disconnect_link(self):
        self.get_link_property_dict(self.aws_access_key, self.aws_secret_key)
        if self.cbas_util.create_external_link_on_cbas(link_properties=self.link_info):
            self.link_created = True
            if not self.cbas_util.disconnect_link(
                    link_name="{0}.{1}".format(self.link_info["dataverse"], self.link_info["name"]),
                    validate_error_msg=True,
                    expected_error="Link {0}.{1} cannot be used for data ingestion".format(self.link_info["dataverse"],
                                                                                           self.link_info["name"])):
                self.fail("Expected error message different from actual error message")
        else:
            self.fail("Link creation failed")

    def test_create_dataset(self):

        self.setup_for_dataset()

        # Create users with all RBAC roles.
        self.create_or_delete_users(self.rbac_util, rbac_users_created)

        new_region = random.choice(self.remove_and_return_new_list(self.aws_region_list, self.link_info["region"]))
        new_aws_bucket = "new-cbas-regression-{0}".format(random.randint(1, 1000))

        def create_bucket_in_other_region(aws_bucket, region):
            if perform_S3_operation(aws_access_key=self.aws_access_key, aws_secret_key=self.aws_secret_key,
                                    aws_session_token=self.aws_session_token, create_bucket=True,
                                    bucket_name=aws_bucket, region=region):
                return True
            else:
                return False

        testcases = [
            {
                "description": "Create dataset with same name",
                "recreate_dataset": True,
                "validate_error_msg": True,
                "expected_error": "A dataset with name {0} already exists in dataverse {1}".format(
                    self.cbas_dataset_name,
                    self.link_info["dataverse"])
            },
            {
                "description": "Create dataset without type definition for CSV format",
                "file_format": "csv",
                "header": False,
                "validate_error_msg": True,
                "expected_error": "Inline type definition is required for external datasets with 'csv' format"
            },
            {
                "description": "Create dataset without type definition for TSV format",
                "file_format": "tsv",
                "header": False,
                "validate_error_msg": True,
                "expected_error": "Inline type definition is required for external datasets with 'tsv' format"
            },
            {
                "description": "Create dataset with type definition for JSON format",
                "object_construction_def": "id INT",
                "validate_error_msg": True,
                "expected_error": "Inline type definition is not allowed for external datasets with 'json' format"
            },
            {
                "description": "Create dataset with upsupported data type in type definition",
                "object_construction_def": "id FLOAT",
                "header": False,
                "file_format": "csv",
                "validate_error_msg": True,
                "expected_error": "Type float could not be found"
            },
            {
                "description": "Create dataset with invalid type in type definition",
                "object_construction_def": "id {0}".format(self.invalid_value),
                "file_format": "csv",
                "header": False,
                "validate_error_msg": True,
                "expected_error": "Cannot find datatype with name {0}".format(self.invalid_value)
            },
            {
                "description": "Create dataset on a non existent AWS bucket",
                "aws_bucket_name": self.invalid_value,
                "validate_error_msg": True,
                "expected_error": "External source error"
            },
            {
                "description": "Create dataset on AWS bucket in different region than that specified while creating link",
                "aws_bucket_name": new_aws_bucket,
                "new_bucket": create_bucket_in_other_region(new_aws_bucket, new_region),
                "validate_error_msg": True,
                "expected_error": "External source error"
            },
            {
                "description": "Create dataset in different dataverse than link",
                "dataverse": "dataverse2",
                "link_name": "{0}.{1}".format(self.link_info["dataverse"], self.link_info["name"]),
                "new_dataverse": True,
                "validate_error_msg": False
            },
            {
                "description": "Create dataset when link's dataverse is not specified but dataset's dataverse is specified",
                "dataverse": "dataverse2",
                "cbas_dataset_name": "{0}.{1}".format(self.link_info["dataverse"], self.cbas_dataset_name),
                "new_dataverse": True,
                "validate_error_msg": False
            },
            {
                "description": "Create dataset in non-existent dataverse",
                "dataverse": self.invalid_value,
                "validate_error_msg": True,
                "expected_error": "Cannot find dataverse with name {0}".format(self.invalid_value)
            },
            {
                "description": "Create dataset on non-existent link",
                "link_name": self.invalid_value,
                "validate_error_msg": True,
                "expected_error": "Link {0}.{1} does not exist".format(self.link_info["dataverse"], self.invalid_value)
            },
            {
                "description": "Create dataset on Local link",
                "link_name": "Local",
                "validate_error_msg": True,
                "expected_error": "Link Default.Local cannot be used for external datasets"
            },
            {
                "description": "Create dataset with empty string as aws path",
                "path_on_aws_bucket": "",
                "validate_error_msg": False
            },
            {
                "description": "Create dataset with missing Using clause",
                "validate_error_msg": False
            },
            {
                "description": "Create dataset with a path that is not present on AWS bucket",
                "path_on_aws_bucket": self.invalid_value,
                "validate_error_msg": False
            },
            {
                "description": "Create dataset with unsupported file format",
                "file_format": "txt",
                "validate_error_msg": True,
                "expected_error": "Invalid value for parameter \"format\""
            },
            {
                "description": "Create dataset with missing header flag for csv file format",
                "file_format": "csv",
                "object_construction_def": "name STRING",
                "header": None,
                "validate_error_msg": True,
                "expected_error": "Parameter(s) header must be specified"
            },
            {
                "description": "Create dataset with missing header flag for tsv file format",
                "file_format": "tsv",
                "object_construction_def": "name STRING",
                "header": None,
                "validate_error_msg": True,
                "expected_error": "Parameter(s) header must be specified"
            },
            {
                "description": "Create dataset with header flag for json file format",
                "file_format": "json",
                "header": False,
                "validate_error_msg": True,
                "expected_error": "Invalid parameter \"header\""
            },
            {
                "description": "Create dataset with invalid header flag for tsv file format",
                "file_format": "tsv",
                "object_construction_def": "name STRING",
                "header": "False1",
                "validate_error_msg": True,
                "expected_error": "Invalid value for parameter \"header\""
            },
            {
                "description": "Create dataset with invalid null flag",
                "file_format": "tsv",
                "object_construction_def": "name STRING",
                "header": False,
                "null_string": "N"
            },
            {
                "description": "Create dataset with valid null flag",
                "file_format": "tsv",
                "object_construction_def": "name STRING",
                "header": False,
                "null_string": "\N",
                "validate_error_msg": False
            },
            {
                "description": "Create dataset with null flag for json file format",
                "file_format": "json",
                "null_string": "\N",
                "validate_error_msg": True,
                "expected_error": "Invalid parameter \"null\""
            },
            {
                "description": "Create dataset with invalid redact warning flag",
                "file_format": "json",
                "redact_warning": "False1",
                "validate_error_msg": True,
                "expected_error": "Invalid value for parameter \"redact-warnings\""
            },
            {
                "description": "Create dataset with both include and exclude flag set",
                "file_format": "json",
                "include": "*.json",
                "exclude": "*.csv",
                "validate_error_msg": True,
                "expected_error": "The parameters \"include\" and \"exclude\" cannot be provided at the same time"
            },
            {
                "description": "Create dataset with include flag set with invalid pattern",
                "file_format": "json",
                "include": "^{*.json}",
                "validate_error_msg": False
            },
            {
                "description": "Create dataset with exclude flag set with invalid pattern",
                "file_format": "json",
                "exclude": "^{*.json}",
                "validate_error_msg": False
            },
            {
                "description": "Create dataset with include flag set with valid pattern",
                "file_format": "json",
                "include": "*.json",
                "validate_error_msg": False
            },
            {
                "description": "Create dataset with exclude flag set with valid pattern",
                "file_format": "json",
                "exclude": "*.csv",
                "validate_error_msg": False
            },
        ]

        rbac_testcases = self.create_testcase_for_rbac_user("Creating dataset on external link using {0} user",
                                                            rbac_users_created)
        for tc in rbac_testcases:
            if tc["validate_error_msg"]:
                tc["expected_error"] = "User must have permission"

        testcases = testcases + rbac_testcases

        failed_testcases = list()

        for testcase in testcases:
            try:
                self.log.info(testcase["description"])

                dataset_param = copy.deepcopy(self.dataset_params)
                for param in testcase:
                    if param in dataset_param:
                        dataset_param[param] = testcase[param]

                if testcase.get("new_dataverse", False):
                    if not self.cbas_util.create_dataverse_on_cbas(dataset_param["dataverse"]):
                        raise Exception("Error while creating new dataverse")

                if testcase.get("recreate_dataset", False):
                    dataset_param["validate_error_msg"] = False

                if not self.cbas_util.create_dataset_on_external_resource(**dataset_param):
                    raise Exception("Error while creating dataset")

                if testcase.get("recreate_dataset", False):
                    dataset_param["validate_error_msg"] = True
                    if not self.cbas_util.create_dataset_on_external_resource(**dataset_param):
                        raise Exception("Error while creating dataset")
                    dataset_param["validate_error_msg"] = False

                if testcase.get("new_dataverse", False):
                    if not self.cbas_util.drop_dataverse_on_cbas(dataset_param["dataverse"]):
                        raise Exception("Error while deleting new dataverse")
                    dataset_param["dataverse"] = self.dataset_params["dataverse"]

                if testcase.get("new_bucket", False):
                    if not perform_S3_operation(
                            aws_access_key=self.aws_access_key, aws_secret_key=self.aws_secret_key,
                            aws_session_token=self.aws_session_token, create_bucket=False,
                            bucket_name=dataset_param["aws_bucket_name"],
                            delete_bucket=True):
                        raise Exception("Error while deleting bucket")

                if not dataset_param["validate_error_msg"]:
                    self.cbas_util.drop_dataset(dataset_param["cbas_dataset_name"],
                                                dataset_param["dataverse"])
            except Exception:
                failed_testcases.append(testcase["description"])
        if failed_testcases:
            self.fail("Following testcases failed - {0}".format(str(failed_testcases)))

    def test_query_dataset(self):
        """
        dataset_params
        mix_data_file bool
        no_of_files int
        file_format_for_upload ["json","csv","tsv]
        no_of_folders int
        max_folder_depth int
        header_s3_file bool
        null_s3_file str
        no_of_docs int
        randomize_header bool
        large_file bool
        missing_field bool
        select_aws_path bool
        invalid_aws_path
        """
        self.setup_for_dataset()

        # read dataset params from test config
        for param in self.dataset_params:
            if param in self.input.test_params:
                if param == "include":
                    self.dataset_params[param] = self.get_include_exclude_flag(True,
                                                                               self.input.test_params.get(param))
                elif param == "exclude":
                    self.dataset_params[param] = self.get_include_exclude_flag(False,
                                                                               self.input.test_params.get(param))
                else:
                    self.dataset_params[param] = self.convert_string_to_bool(self.input.test_params.get(param))

        # generate and upload data to AWS S3
        if self.convert_string_to_bool(self.input.test_params.get("mix_data_file", False)):
            if not self.s3_data_helper.generate_mix_data_file(
                    self.dataset_params["aws_bucket_name"], file_format=self.dataset_params["file_format"],
                    upload_to_s3=True):
                self.fail("Error while uploading files to S3")
        elif self.input.test_params.get("file_extension", None):
            if not self.s3_data_helper.generate_file_with_record_of_size_and_upload(
                    self.dataset_params["aws_bucket_name"], "sample",
                    record_size=1024, file_format=self.dataset_params["file_format"],
                    upload_to_s3=True, file_extension=self.input.test_params.get("file_extension")):
                self.fail("Error while uploading files to S3")
        else:
            missing_field = json.loads(self.input.test_params.get("missing_field", "[\"False\"]"))
            for field in missing_field:
                missing_field.append(self.convert_string_to_bool(field))
                missing_field.remove(field)
            result = self.s3_data_helper.generate_data_for_s3_and_upload(
                aws_bucket_name=self.dataset_params["aws_bucket_name"],
                key=self.key,
                no_of_files=int(self.input.test_params.get("no_of_files", 5)),
                file_formats=json.loads(self.input.test_params.get("file_format_for_upload", "[\"json\"]")),
                no_of_folders=int(self.input.test_params.get("no_of_folders", 0)),
                max_folder_depth=int(self.input.test_params.get("max_folder_depth", 0)),
                header=self.convert_string_to_bool(self.input.test_params.get("header_s3_file", False)),
                null_key=self.input.test_params.get("null_s3_file", ""),
                operation="create",
                bucket=self.bucket_util.buckets[0],
                no_of_docs=int(self.input.test_params.get("no_of_docs", 100)),
                randomize_header=self.convert_string_to_bool(self.input.test_params.get("randomize_header", False)),
                large_file=self.convert_string_to_bool(self.input.test_params.get("large_file", False)),
                missing_field=missing_field)
            if result:
                self.fail("Error while uploading files to S3")

        # run query on dataset and on bucket that was used to generate AWS data.
        n1ql_query = self.get_n1ql_query(self.input.test_params.get("n1ql_query", "count_all"))
        cbas_query = self.input.test_params.get("cbas_query", "Select count(*) from `{0}`;")

        # path_on_aws_bucket is used to set Using condition in create dataset DDL, when path_on_aws_bucket is True,
        # we will set the USING condition with valid AWS path else if False we will set it with invalid AWS path.
        if self.convert_string_to_bool(self.input.test_params.get("select_aws_path", False)):
            if len(self.s3_data_helper.folders) > 1:
                folder_name = random.choice(self.s3_data_helper.folders)
                while folder_name == "":
                    folder_name = random.choice(self.s3_data_helper.folders)
                self.dataset_params["path_on_aws_bucket"] = folder_name
                n1ql_query = n1ql_query.format("{0}", folder_name)

        if self.convert_string_to_bool(self.input.test_params.get("invalid_aws_path", False)):
            self.dataset_params["path_on_aws_bucket"] = "invalid/invalid"

        # create dataset
        if not self.cbas_util.create_dataset_on_external_resource(**self.dataset_params):
            self.fail("Failed to create dataset")
        self.dataset_created = True

        n1ql_result = self.rest.query_tool(n1ql_query.format(self.bucket_util.buckets[0].name))["results"][0]["$1"]
        status, metrics, errors, cbas_result, handle = self.cbas_util.execute_statement_on_cbas_util(
            cbas_query.format(self.dataset_params["cbas_dataset_name"]), timeout=120, analytics_timeout=120)

        if self.convert_string_to_bool(self.input.test_params.get("header", False)) and \
                not self.convert_string_to_bool(self.input.test_params.get("header_s3_file", False)):
            n1ql_result -= int(self.input.test_params.get("no_of_files", 5))
        elif not self.convert_string_to_bool(self.input.test_params.get("header", False)) and \
                self.convert_string_to_bool(self.input.test_params.get("header_s3_file", False)):
            if not metrics['warningCount'] == int(self.input.test_params.get("no_of_files", 5)):
                self.fail("No warnings were raised for data mismatch while querying on csv or tsv file.")

        if self.convert_string_to_bool(self.input.test_params.get("validate_error_conditions", False)):
            if not errors[0]["msg"] == "Malformed input stream":
                self.fail("Expected data does not match actual data")
        else:
            if self.input.test_params.get("expected_count", None):
                n1ql_result = int(self.input.test_params.get("expected_count"))
            if not n1ql_result == cbas_result[0]["$1"]:
                self.fail("Expected data does not match actual data")

    def test_effect_of_rbac_role_change_on_external_link(self):
        new_username = "user1"
        original_user_role = "analytics_admin"
        new_user_role = "analytics_reader"
        self.rbac_util._create_user_and_grant_role(new_username, original_user_role)
        rbac_users_created[new_username] = None

        self.setup_for_dataset()

        if not self.cbas_util.create_dataset_on_external_resource(**self.dataset_params):
            self.fail("Dataset creation failed")
        self.dataset_created = True

        result = self.s3_data_helper.generate_data_for_s3_and_upload(
            aws_bucket_name=self.dataset_params["aws_bucket_name"], key=self.key,
            no_of_files=5, file_formats=["json"], no_of_folders=0, max_folder_depth=0,
            header=False, null_key="", operation="create", bucket=self.bucket_util.buckets[0],
            no_of_docs=100, randomize_header=False, large_file=False, missing_field=[False])
        if result:
            self.fail("Error while uploading files to S3")

        # run query on dataset and on bucket that was used to generate AWS data.
        n1ql_query = "Select count(*) from {0};".format(self.bucket_util.buckets[0].name)
        n1ql_result = self.rest.query_tool(n1ql_query)["results"][0]["$1"]
        if not self.cbas_util.validate_cbas_dataset_items_count(self.dataset_params["cbas_dataset_name"], n1ql_result):
            self.fail("Expected data does not match actual data")

        self.rbac_util._create_user_and_grant_role(new_username, new_user_role)

        if not self.cbas_util.validate_cbas_dataset_items_count(self.dataset_params["cbas_dataset_name"], n1ql_result):
            self.fail("Expected data does not match actual data")

    def test_querying_while_network_failure(self):
        self.setup_for_dataset()
        if not self.cbas_util.create_dataset_on_external_resource(**self.dataset_params):
            self.fail("Dataset creation failed")
        self.dataset_created = True

        result = self.s3_data_helper.generate_data_for_s3_and_upload(
            aws_bucket_name=self.dataset_params["aws_bucket_name"], key=self.key,
            no_of_files=5, file_formats=["json"], no_of_folders=0, max_folder_depth=0,
            header=False, null_key="", operation="create", bucket=self.bucket_util.buckets[0],
            no_of_docs=10000, randomize_header=False, large_file=False, missing_field=[False])
        if result:
            self.fail("Error while uploading files to S3")

        shell = RemoteMachineShellConnection(self.cbas_node)
        self.speed_limit_set = True

        cbas_query = "Select count(*) from `{0}`;".format(self.dataset_params["cbas_dataset_name"])
        result = list()
        threads = list()
        threads.append(Thread(
            target=self.execute_cbas_query,
            name="s3_query_thread",
            args=(cbas_query, result, 120, 120,)
        ))
        threads.append(Thread(
            target=shell.stop_network,
            name="firewall_thread",
            args=(30,)
        ))
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        if not result[0] == "fatal":
            self.fail("query executed successfully")

    def test_querying_with_more_than_1000_files_in_S3_bucket(self):
        self.region = self.input.test_params.get("aws_region", "us-west-1")
        self.setup_for_dataset()
        if not self.cbas_util.create_dataset_on_external_resource(**self.dataset_params):
            self.fail("Dataset creation failed")
        self.dataset_created = True

        result = self.s3_data_helper.generate_data_for_s3_and_upload(
            aws_bucket_name=self.dataset_params["aws_bucket_name"], key=self.key, no_of_files=1500,
            file_formats=["json"], no_of_folders=0, max_folder_depth=0,
            header=False, null_key="", operation="create",
            bucket=self.bucket_util.buckets[0], no_of_docs=100000,
            randomize_header=False, large_file=False, missing_field=[False])
        if result:
            self.fail("Error while uploading files to S3")

        n1ql_query = "Select count(*) from {0};".format(self.bucket_util.buckets[0].name)
        n1ql_result = self.rest.query_tool(n1ql_query)["results"][0]["$1"]
        if not self.cbas_util.validate_cbas_dataset_items_count(self.dataset_params["cbas_dataset_name"], n1ql_result,
                                                                timeout=3600, analytics_timeout=3600):
            self.fail("Expected data does not match actual data")

    def test_file_deletion_from_AWS_while_query_is_reading_file(self):
        self.setup_for_dataset()
        if not self.cbas_util.create_dataset_on_external_resource(**self.dataset_params):
            self.fail("Dataset creation failed")
        self.dataset_created = True

        file_dict = self.s3_data_helper.generate_file_of_specified_size_and_upload(
            bucket_name=self.dataset_params["aws_bucket_name"],
            no_of_files=int(self.input.test_params.get("no_of_files", 5)),
            file_size_in_KB=int(self.input.test_params.get("file_size", 100)),
            record_type=self.input.test_params.get("record_type", "json"),
            upload_to_s3=True, file_extension=None)

        if self.convert_string_to_bool(self.input.test_params.get("delete_last_file", False)):
            # selecting a middle file to delete, as list object from aws returns object name in sorted alpha numeric way.
            file_to_delete = file_dict.keys()[-1]
        else:
            file_to_delete = file_dict.keys()[0]

        if not self.convert_string_to_bool(self.input.test_params.get("recreate", False)) and \
                self.convert_string_to_bool(self.input.test_params.get("delete_last_file", False)):
            n1ql_result = 0
            for filename in file_dict:
                if filename != file_to_delete:
                    n1ql_result += file_dict[filename]
        else:
            n1ql_result = sum(file_dict.values())

        cbas_query = "Select count(*) from {0};".format(self.dataset_params["cbas_dataset_name"])

        dest_path = os.path.join(r"/tmp/", file_to_delete)
        if self.convert_string_to_bool(self.input.test_params.get("recreate", False)):
            # Downloading file to be deleted, so that it can be recreated again
            if not perform_S3_operation(
                    aws_access_key=self.aws_access_key, aws_secret_key=self.aws_secret_key,
                    aws_session_token=self.aws_session_token, bucket_name=self.dataset_params["aws_bucket_name"],
                    download_file=True, src_path=file_to_delete, dest_path=dest_path):
                self.fail("Error while downloading file from S3")
            delete_only = False
        else:
            delete_only = True

        for server in self.cluster.servers:
            RemoteUtilHelper.set_upload_download_speed(server=server, download=1000)
            self.speed_limit_set = True

        result = list()

        threads = list()
        thread1 = Thread(target=self.execute_cbas_query,
                         name="s3_query_thread",
                         args=(cbas_query, result, 300, 300,))
        thread1.start()
        threads.append(thread1)
        self.sleep(30)

        thread2 = Thread(target=self.perform_delete_recreate_file_on_AWS_S3,
                         name="s3_thread",
                         args=(self.dataset_params["aws_bucket_name"], file_to_delete, dest_path, delete_only,))
        thread2.start()
        threads.append(thread2)

        while thread2.is_alive():
            self.sleep(1, "Waiting for thread to terminate")

        for server in self.cluster.servers:
            RemoteUtilHelper.clear_all_speed_restrictions(server=server)
        self.speed_limit_set = False

        for thread in threads:
            thread.join()

        if os.path.exists(dest_path):
            os.remove(dest_path)

        if not result[0] == "success":
            self.fail("query execution failed")

        if not n1ql_result == result[1][0]["$1"]:
            self.fail("Expected data does not match actual data")

    def get_rebalance_server(self):
        node_ip = [i.ip for i in self.rest.node_statuses()]
        rebalanceServers = list()
        for server in self.servers:
            if not (server.ip in node_ip):
                rebalanceServers.append(server)
        return rebalanceServers

    def rebalance_cbas(self, rebalanceServers, node_services, rebalance_in=True,
                       swap_rebalance=True, failover=True, sleep_bofore_starting=0):
        self.sleep(sleep_bofore_starting, "Sleeping before starting rebalance operations")
        self.log.info("Fetch node to remove during rebalance")
        if rebalance_in:
            self.log.info("Rebalance in CBAS nodes")
            self.cluster_util.add_node(node=rebalanceServers[0], services=node_services,
                                       rebalance=False, wait_for_rebalance_completion=False)
            self.cluster_util.add_node(node=rebalanceServers[1], services=node_services,
                                       rebalance=True, wait_for_rebalance_completion=True)
        elif failover:
            self.log.info("fail-over the node")
            if not self.task.failover(self.cluster.servers, failover_nodes=[rebalanceServers[0]],
                                      graceful=False, wait_for_pending=300):
                self.fail("Error while node failover")
            self.log.info("Read input param to decide on add back or rebalance out")
            self.rebalance_out = self.input.param("rebalance_out", False)
            self.sleep(10, "Sleeping before removing or adding back the failed over node")
            if self.rebalance_out:
                self.log.info("Rebalance out the fail-over node")
                result = self.cluster_util.rebalance()
                self.assertTrue(result, "Rebalance operation failed")
            else:
                self.recovery_strategy = self.input.param("recovery_strategy", "full")
                self.rest.set_recovery_type('ns_1@' + rebalanceServers[0].ip, self.recovery_strategy)
                result = self.cluster_util.rebalance()
                self.assertTrue(result, "Rebalance operation failed")
        else:
            out_nodes = []
            nodes = self.rest.node_statuses()
            for node in nodes:
                if swap_rebalance:
                    if node.ip == rebalanceServers[0].ip:
                        out_nodes.append(node)
                else:
                    if node.ip == rebalanceServers[0].ip or node.ip == rebalanceServers[1].ip:
                        out_nodes.append(node)
            if swap_rebalance:
                self.log.info("Swap rebalance CBAS nodes")
                self.cluster_util.add_node(node=rebalanceServers[1], services=node_services, rebalance=False)
            self.cluster_util.remove_all_nodes_then_rebalance(out_nodes)

    def test_analytics_cluster_when_rebalancing_in_cbas_node(self):
        '''
        1. We have 2 clusters, local cluster, remote cluster and 4 nodes - 101, 102, 103, 104.
        2, Post initial setup - local cluster - 1 node with cbas, remote cluster - 1 node with KV and query running
        3. As part of test add an extra cbas nodes and rebalance

        Data mutation is happening on remote cluster while local cluster is rebalancing.
        '''

        node_services = ["cbas"]

        self.log.info("Setup CBAS")
        self.setup_for_dataset()
        if not self.cbas_util.create_dataset_on_external_resource(**self.dataset_params):
            self.fail("Dataset creation failed")
        self.dataset_created = True

        result = self.s3_data_helper.generate_data_for_s3_and_upload(
            aws_bucket_name=self.dataset_params["aws_bucket_name"], key=self.key,
            no_of_files=5,
            file_formats=["json"],
            no_of_folders=0,
            max_folder_depth=0,
            header=False,
            null_key="",
            operation="create", bucket=self.bucket_util.buckets[0],
            no_of_docs=self.num_items,
            randomize_header=False,
            large_file=False,
            missing_field=[False])

        if result:
            self.fail("Error while uploading files to S3")

        query = "Select count(*) from `{0}`;"
        n1ql_result = self.rest.query_tool(query.format(self.bucket_util.buckets[0].name))["results"][0]["$1"]

        rebalanceServers = self.get_rebalance_server()

        result = list()
        threads = list()
        threads.append(Thread(
            target=self.execute_cbas_query,
            name="s3_query_thread",
            args=(query.format(self.dataset_params["cbas_dataset_name"]), result, 120, 120,)
        ))
        threads.append(Thread(
            target=self.rebalance_cbas,
            name="rebalance_thread",
            args=(rebalanceServers, node_services, True, False, False,)
        ))
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        if not result[0] == "success":
            self.fail("query executed successfully")
        if not n1ql_result == result[1][0]["$1"]:
            self.fail("Number of items in dataset do not match number of items in bucket")

    def test_analytics_cluster_swap_rebalancing(self):
        '''
        1. We have 2 clusters, local cluster, remote cluster and 4 nodes - 101, 102, 103, 104.
        2, Post initial setup - local cluster - 1 node with cbas, remote cluster - 1 node with KV and query running
        3. As part of test add an extra cbas node that we will swap rebalance later - Adding 103 and rebalance
        4. select the node added in #3 for remove and 104 to add during swap

        Data mutation is happening on remote cluster while local cluster is rebalancing.
        '''
        node_services = ["cbas"]

        self.log.info("Setup CBAS")
        self.setup_for_dataset()
        if not self.cbas_util.create_dataset_on_external_resource(**self.dataset_params):
            self.fail("Dataset creation failed")
        self.dataset_created = True

        rebalanceServers = self.get_rebalance_server()
        self.log.info("Rebalance in local cluster, this node will be removed during swap")
        self.cluster_util.add_node(node=rebalanceServers[0], services=node_services)

        result = self.s3_data_helper.generate_data_for_s3_and_upload(
            aws_bucket_name=self.dataset_params["aws_bucket_name"], key=self.key,
            no_of_files=5,
            file_formats=["json"],
            no_of_folders=0,
            max_folder_depth=0,
            header=False,
            null_key="",
            operation="create", bucket=self.bucket_util.buckets[0],
            no_of_docs=self.num_items,
            randomize_header=False,
            large_file=False,
            missing_field=[False])

        if result:
            self.fail("Error while uploading files to S3")

        query = "Select count(*) from `{0}`;"
        n1ql_result = self.rest.query_tool(query.format(self.bucket_util.buckets[0].name))["results"][0]["$1"]

        result = list()
        threads = list()
        threads.append(Thread(
            target=self.execute_cbas_query,
            name="s3_query_thread",
            args=(query.format(self.dataset_params["cbas_dataset_name"]), result, 120, 120,)
        ))
        threads.append(Thread(
            target=self.rebalance_cbas,
            name="rebalance_thread",
            args=(rebalanceServers, node_services, False, True, False,)
        ))
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        if not result[0] == "success":
            self.fail("query executed successfully")

        if not n1ql_result == result[1][0]["$1"]:
            self.fail("Number of items in dataset do not match number of items in bucket")

    def test_analytics_cluster_when_rebalancing_out_cbas_node(self):
        '''
        1. We have 2 clusters, local cluster, remote cluster and 4 nodes - 101, 102, 103, 104.
        2, Post initial setup - local cluster - 1 node with cbas, remote cluster - 1 node with KV and query running
        3. As part of test add extra cbas nodes that we will rebalance out later - Adding 103, 104 and rebalance
        4. select the nodes added in #3 for remove

        Data mutation is happening on remote cluster while local cluster is rebalancing.
        '''

        node_services = ["cbas"]

        self.log.info("Setup CBAS")
        self.setup_for_dataset()
        if not self.cbas_util.create_dataset_on_external_resource(**self.dataset_params):
            self.fail("Dataset creation failed")
        self.dataset_created = True

        rebalanceServers = self.get_rebalance_server()

        self.log.info("Rebalance in CBAS nodes")
        self.cluster_util.add_node(node=rebalanceServers[0], services=node_services,
                                   rebalance=False, wait_for_rebalance_completion=False)
        self.cluster_util.add_node(node=rebalanceServers[1], services=node_services,
                                   rebalance=True, wait_for_rebalance_completion=True)

        result = self.s3_data_helper.generate_data_for_s3_and_upload(
            aws_bucket_name=self.dataset_params["aws_bucket_name"], key=self.key,
            no_of_files=5,
            file_formats=["json"],
            no_of_folders=0,
            max_folder_depth=0,
            header=False,
            null_key="",
            operation="create", bucket=self.bucket_util.buckets[0],
            no_of_docs=self.num_items,
            randomize_header=False,
            large_file=False,
            missing_field=[False])

        if result:
            self.fail("Error while uploading files to S3")

        query = "Select count(*) from `{0}`;"
        n1ql_result = self.rest.query_tool(query.format(self.bucket_util.buckets[0].name))["results"][0]["$1"]

        result = list()
        threads = list()
        threads.append(Thread(
            target=self.execute_cbas_query,
            name="s3_query_thread",
            args=(query.format(self.dataset_params["cbas_dataset_name"]), result, 120, 120,)
        ))
        threads.append(Thread(
            target=self.rebalance_cbas,
            name="rebalance_thread",
            args=(rebalanceServers, node_services, False, False, False,)
        ))
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        if not result[0] == "success":
            self.fail("query executed successfully")

        if not n1ql_result == result[1][0]["$1"]:
            self.fail("Number of items in dataset do not match number of items in bucket")

    def test_fail_over_cbas_node_followed_by_rebalance_out_or_add_back(self):
        """
        1. We have 2 clusters, local cluster, remote cluster and 4 nodes - 101, 102, 103, 104.
        2, Post initial setup - local cluster - 1 node with cbas, remote cluster - 1 node with KV and query running
        3. Add a node that will be failed over - CBAS
        4. Create CBAS buckets and dataset
        5. Fail over the cbas node based on graceful_failover parameter specified
        6. Rebalance out/add back based on rebalance_out parameter specified
        7. Perform doc operations
        8. run concurrent queries
        9. Verify document count on dataset post failover
        """

        node_services = ["cbas"]

        self.log.info("Setup CBAS")
        self.setup_for_dataset()

        if not self.cbas_util.create_dataset_on_external_resource(**self.dataset_params):
            self.fail("Dataset creation failed")
        self.dataset_created = True

        rebalanceServers = self.get_rebalance_server()

        self.log.info("Add an extra node to fail-over")
        self.cluster_util.add_node(node=rebalanceServers[0], services=node_services,
                                   rebalance=True, wait_for_rebalance_completion=True)

        for i in range(0, 2):
            filename = "big_record_file_{0}".format(str(i))
            if not self.s3_data_helper.generate_file_with_record_of_size_and_upload(
                    self.dataset_params["aws_bucket_name"], filename, record_size=29 * 1024 * 1024,
                    file_format="json", upload_to_s3=True):
                self.fail("Error while uploading files to S3")
        self.log.info("File upload successfull")

        query = "Select count(*) from `{0}`;"

        result = list()
        threads = list()
        threads.append(Thread(
            target=self.execute_cbas_query,
            name="s3_query_thread",
            args=(query.format(self.dataset_params["cbas_dataset_name"]), result, 120, 120,)
        ))
        threads.append(Thread(
            target=self.rebalance_cbas,
            name="rebalance_thread",
            args=(rebalanceServers, node_services, False, False, True, 5)
        ))
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        if not self.cbas_util.validate_error_in_response(str(result[0]), result[2],
                                                         "Analytics Service is temporarily unavailable"):
            self.fail("query executed successfully")

        if not self.cbas_util.validate_cbas_dataset_items_count(self.dataset_params["cbas_dataset_name"],
                                                                2, timeout=600, analytics_timeout=600):
            self.fail("Expected data does not match actual data")

    def test_when_a_single_record_size_is_greater_than_32MB(self):
        self.setup_for_dataset()

        file_format = self.input.test_params.get("file_format", "json")
        record_size = int(self.input.test_params.get("record_size", 32))
        filename = "big_record_file"

        self.dataset_params["file_format"] = file_format

        if file_format in ["csv", "tsv"]:
            self.dataset_params[
                "object_construction_def"] = "key1 string, key2 string, key3 string, key4 string, key5 string"
            self.dataset_params["header"] = False

        if not self.cbas_util.create_dataset_on_external_resource(**self.dataset_params):
            self.fail("Dataset creation failed")
        self.dataset_created = True
        if not self.s3_data_helper.generate_file_with_record_of_size_and_upload(
                self.dataset_params["aws_bucket_name"], filename, record_size=record_size * 1024 * 1024,
                file_format=file_format, upload_to_s3=True):
            self.fail("Error while uploading files to S3")
        self.log.info("File upload successfull")
        cbas_query = "Select count(*) from {0};".format(self.dataset_params["cbas_dataset_name"])
        status, metrics, errors, results, handle = self.cbas_util.execute_statement_on_cbas_util(cbas_query)

        if record_size >= 32:
            if not errors:
                self.fail("query executed successfully")
            elif not "Record is too large. Maximum record size is 32000000" in errors[0]['msg']:
                self.fail("Error message mismatch.\nExpected - {0}\nActual - {1}".format(
                    "Record is too large. Maximum record size is 32000000", errors[0]['msg']))
        else:
            if not (results[0]["$1"] == 1):
                self.fail("Expected data does not match actual data")

    def test_large_file(self):
        aws_bucket_name = "cbas-regression-1"
        region = "us-west-1"
        object_construction_def = "num1 int, num2 int"
        doc_counts = {
            "json": 155861000,
            "csv": 219619000,
            "tsv": 219619000,
        }

        self.get_link_property_dict(self.aws_access_key, self.aws_secret_key)
        self.link_info["region"] = region
        if not self.cbas_util.create_external_link_on_cbas(link_properties=self.link_info):
            self.fail("link creation failed")
        self.link_created = True

        file_format = self.input.test_params.get("file_format", "json")

        self.get_dataset_parameters()
        self.dataset_params["aws_bucket_name"] = aws_bucket_name
        self.dataset_params["file_format"] = file_format
        self.dataset_params["include"] = "*.{0}".format(file_format)

        if file_format in ["csv", "tsv"]:
            self.dataset_params["object_construction_def"] = object_construction_def
            self.dataset_params["header"] = False

        if not self.cbas_util.create_dataset_on_external_resource(**self.dataset_params):
            self.fail("Dataset creation failed")
        self.dataset_created = True

        if not self.cbas_util.validate_cbas_dataset_items_count(self.dataset_params["cbas_dataset_name"],
                                                                doc_counts[file_format], timeout=7200,
                                                                analytics_timeout=7200):
            self.fail("Expected data does not match actual data")
    
    def test_create_query_drop_dataset_with_3_part_dataset_name(self):
        self.setup_for_dataset(create_dataverse=True, dataverse_cardinality=2)

        # read dataset params from test config
        for param in self.dataset_params:
            if param in self.input.test_params:
                self.dataset_params[param] = self.convert_string_to_bool(self.input.test_params.get(param))
        
        self.dataset_params["dataverse"] = self.link_info["dataverse"]

        missing_field = [False]
        result = self.s3_data_helper.generate_data_for_s3_and_upload(
            aws_bucket_name=self.dataset_params["aws_bucket_name"],
            key=self.key, no_of_files=5,
            file_formats=json.loads(self.input.test_params.get("file_format_for_upload", "[\"json\"]")),
            no_of_folders=0, max_folder_depth=0, header=False, null_key="", operation="create",
            bucket=self.bucket_util.buckets[0],
            no_of_docs=int(self.input.test_params.get("no_of_docs", 100)),
            randomize_header=False, large_file=False, missing_field=missing_field)
        if result:
            self.fail("Error while uploading files to S3")

        # run query on dataset and on bucket that was used to generate AWS data.
        n1ql_query = self.get_n1ql_query(self.input.test_params.get("n1ql_query", "count_all"))

        # create dataset
        if not self.cbas_util.create_dataset_on_external_resource(**self.dataset_params):
            self.fail("Failed to create dataset")
        self.dataset_created = True

        n1ql_result = self.rest.query_tool(n1ql_query.format(self.bucket_util.buckets[0].name))["results"][0]["$1"]
        if not self.cbas_util.validate_cbas_dataset_items_count(
            ".".join([self.dataset_params["links_dataverse"], self.dataset_params["cbas_dataset_name"]]), 
            n1ql_result, timeout=300, analytics_timeout=300):
            self.fail("Expected data does not match actual data")
        
        if not self.cbas_util.drop_dataset(cbas_dataset_name=self.dataset_params["cbas_dataset_name"], 
                                           dataverse=self.dataset_params["links_dataverse"]):
            self.fail("Error while dropping dataset")
        