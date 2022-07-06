import random, json, copy, os

from rbac_utils.Rbac_ready_functions import RbacUtils

from remote.remote_util import RemoteMachineShellConnection, RemoteUtilHelper
from threading import Thread
from awsLib.s3_data_helper import perform_S3_operation, S3DataHelper
from couchbase_helper.tuq_helper import N1QLHelper
from cbas.cbas_base import CBASBaseTest
from CbasLib.CBASOperations import CBASHelper
from cbas_utils.cbas_utils import CBASRebalanceUtil

rbac_users_created = {}


class CBASExternalLinks(CBASBaseTest):

    def setUp(self):

        super(CBASExternalLinks, self).setUp()

        # Randomly choose name cardinality of links and datasets.
        if "link_cardinality" in self.input.test_params:
            self.link_cardinality = self.input.param("link_cardinality")
        else:
            self.link_cardinality = random.randrange(1, 4)

        if "dataset_cardinality" in self.input.test_params:
            self.dataset_cardinality = self.input.param("dataset_cardinality")
        else:
            self.dataset_cardinality = random.randrange(1, 4)

        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.setUp.__name__)

        # Since all the test cases are being run on 1 cluster only
        self.cluster = self.cb_clusters.values()[0]

        self.aws_access_key, self.aws_secret_key, self.aws_session_token = self.get_aws_credentials()

        self.aws_buckets = {}
        self.speed_limit_set = False

        self.aws_region_list = perform_S3_operation(
            aws_access_key=self.aws_access_key,
            aws_secret_key=self.aws_secret_key,
            aws_session_token=self.aws_session_token, get_regions=True)

        self.rbac_util = RbacUtils(self.cluster.master)

        # Create analytics admin user on Analytics cluster
        self.analytics_username = "admin_analytics"
        self.rbac_util._create_user_and_grant_role(
            self.analytics_username, "analytics_admin")
        rbac_users_created[self.analytics_username] = "analytics_admin"

        self.invalid_value = "invalid"
        self.invalid_ip = "127.0.0.99"
        self.invalid_region = "us-west-6"
        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.setUp.__name__)

    def tearDown(self):
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)

        if self.speed_limit_set:
            for server in self.cluster.servers:
                RemoteUtilHelper.clear_all_speed_restrictions(server)

        for aws_bucket in self.aws_buckets.keys():
            if aws_bucket != "cbas-regression-1" and not perform_S3_operation(
                    aws_access_key=self.aws_access_key,
                    aws_secret_key=self.aws_secret_key,
                    aws_session_token=self.aws_session_token,
                    create_bucket=False, bucket_name=aws_bucket,
                    delete_bucket=True):
                self.log.error("Error while deleting AWS S3 bucket.")

        self.create_or_delete_users(self.rbac_util, rbac_users_created, delete=True)

        super(CBASExternalLinks, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.tearDown.__name__)

    def get_aws_credentials(self, user='full_access'):
        with open("/etc/aws_config.json", "r") as fh:
            cred = json.loads(fh.read())
        return cred[user]["aws_access_key"], cred[user]["aws_secret_key"], cred[user]["aws_session_token"]

    def setup_for_test(self, create_links=True, create_aws_buckets=True,
                       create_dataset_objs=True, same_dv_for_link_and_dataset=True,
                       create_datasets=False, initialize_helper_objs=True,
                       rebalance_util=False):
        self.cbas_util.create_link_obj(
            self.cluster, "s3", link_cardinality=self.link_cardinality,
            accessKeyId=self.aws_access_key, secretAccessKey=self.aws_secret_key,
            regions=self.aws_region_list,
            no_of_objs=self.input.param("no_of_links", 1))

        if create_links:

            link_objs = self.cbas_util.list_all_link_objs(link_type="s3")

            for link_obj in link_objs:
                if not self.cbas_util.create_link(
                        self.cluster, link_obj.properties,
                        username=self.analytics_username):
                    self.fail("link creation failed")

        if create_aws_buckets:
            link_regions = [link.properties["region"] for link in link_objs]
            for _ in range(self.input.param("no_of_aws_bucket", 1)):
                retry = 0
                bucket_created = False
                aws_bucket_name = self.input.param(
                    "aws_bucket_name", "cbas-regression-{0}".format(
                        random.randint(1, 10000)))

                while (not bucket_created) and retry < 10:
                    try:
                        self.log.info("Creating AWS bucket - {0}".format(
                            aws_bucket_name))
                        region = random.choice(link_regions)
                        if not perform_S3_operation(
                                aws_access_key=self.aws_access_key,
                                aws_secret_key=self.aws_secret_key,
                                aws_session_token=self.aws_session_token,
                                create_bucket=True, bucket_name=aws_bucket_name,
                                region=region):
                            self.fail("Creating S3 bucket - {0}. Failed.".format(
                                aws_bucket_name))
                        bucket_created = True
                        self.aws_buckets[aws_bucket_name] = region
                        self.sleep(60, "Sleeping for 60 seconds to ensure that AWS bucket is created")
                    except Exception as err:
                        self.log.error(str(err))
                        if "InvalidLocationConstraint" in str(err):
                            link_regions.remove(region)
                            if not link_regions:
                                region = random.choice(self.aws_region_list)
                            else:
                                region = random.choice(link_regions)
                        aws_bucket_name = self.input.param(
                            "aws_bucket_name", "cbas-regression-{0}".format(
                                random.randint(1, 10000)))
                    finally:
                        retry += 1
                if not bucket_created:
                    self.fail("Creating S3 bucket - {0}. Failed.".format(
                        aws_bucket_name))

        if create_dataset_objs:
            self.cbas_util.create_external_dataset_obj(
                self.cluster, self.aws_buckets,
                same_dv_for_link_and_dataset=same_dv_for_link_and_dataset,
                dataset_cardinality=self.dataset_cardinality,
                object_construction_def=None, path_on_aws_bucket=None,
                file_format="json", redact_warning=None, header=None,
                null_string=None, include=None, exclude=None,
                name_length=30, fixed_length=False,
                no_of_objs=self.input.param("no_of_datasets", 1))

        if create_datasets:
            for dataset_obj in self.cbas_util.list_all_dataset_objs():
                if not self.cbas_util.create_dataset_on_external_resource(
                        self.cluster, dataset_obj.name,
                        link_name=dataset_obj.link_name,
                        dataverse_name=dataset_obj.dataverse_name,
                        **dataset_obj.dataset_properties):
                    self.fail("Dataset creation failed")

        if initialize_helper_objs:
            self.n1ql_helper = N1QLHelper(
                shell=RemoteMachineShellConnection(self.cluster.master),
                buckets=self.cluster.buckets, item_flag=0,
                n1ql_port=8093, log=self.log, input=self.input,
                server=self.cluster.master, use_rest=True)

            self.s3_data_helper = S3DataHelper(
                aws_access_key=self.aws_access_key,
                aws_secret_key=self.aws_secret_key,
                aws_session_token=self.aws_session_token, cluster=self.cluster,
                bucket_util=self.bucket_util, task=self.task, log=self.log,
                n1ql_helper=self.n1ql_helper)

        if rebalance_util:
            self.rebalance_util = CBASRebalanceUtil(
                self.cluster_util, self.bucket_util, self.task, True, self.cbas_util)
            self.cluster.exclude_nodes = list()
            self.cluster.exclude_nodes.extend(
                [self.cluster.master, self.cluster.cbas_cc_node])

    def execute_cbas_query(self, cbas_query, result, timeout=120, analytics_timeout=120):
        status, metrics, errors, cbas_result, handle = self.cbas_util.execute_statement_on_cbas_util(
            self.cluster, cbas_query, timeout=timeout,
            analytics_timeout=analytics_timeout)
        result.extend([status, cbas_result, errors])

    def perform_delete_recreate_file_on_AWS_S3(self, bucket_name, filename,
                                               dest_path, delete_only=False):
        if not perform_S3_operation(
                aws_access_key=self.aws_access_key, aws_secret_key=self.aws_secret_key,
                aws_session_token=self.aws_session_token, bucket_name=bucket_name,
                delete_file=True, file_path=filename):
            self.fail("Error while deleting file from S3")
        if not delete_only:
            if not perform_S3_operation(
                    aws_access_key=self.aws_access_key, aws_secret_key=self.aws_secret_key,
                    aws_session_token=self.aws_session_token,
                    bucket_name=bucket_name, upload_file=True, src_path=dest_path,
                    dest_path=filename):
                self.fail("Error while uploading file from S3")

    def get_n1ql_query(self, query_name):
        query_dict = {
            "count_all": "Select count(*) from `{0}`;",
            "count_from_csv": "select count(*) from {0} where filename like '%.csv';",
            "count_from_tsv": "select count(*) from {0} where filename like '%.tsv';",
            "count_from_json": "select count(*) from {0} where filename like '%.json';",
            "count_from_parquet": "select count(*) from {0} where filename like '%.parquet';",
            "invalid_folder": "select count(*) from {0} where folder like 'invalid/invalid';",
            "valid_folder": "select count(*) from {0} where folder like '{1}%';"
        }
        return query_dict[query_name]

    def get_include_exclude_flag(self, include=True, key_name=0):
        include_dict = {
            1: ["file_[1234567890].json", "*.json"],
            2: ["file_[1234567890].csv", "*.csv"],
            3: ["file_[1234567890].tsv", "*.tsv"],
            4: 'file_?.[!ct]s[!v]*',
            5: 'file_?.[!t]sv',
            6: 'file_?.[!c]sv',
            7: ["file_[1234567890].parquet", "*.parquet"],
        }
        exclude_dict = {
            1: ["*.csv", "*.tsv", "*.parquet"],
            2: ["*.json", "*.tsv", "*.parquet"],
            3: ["*.json", "*.csv", "*.parquet"],
            4: 'file_?*.[ct]s[!o]',
            5: 'file_?*.[t]s[!v]',
            6: 'file_?*.[c]s[!v]',
            7: ["*.json", "*.csv", "*.tsv"],
        }
        if include:
            return include_dict.get(key_name, None)
        else:
            return exclude_dict.get(key_name, None)

    def test_create_external_link(self):

        self.setup_for_test(
            create_links=False, create_aws_buckets=False,
            create_dataset_objs=False, same_dv_for_link_and_dataset=False,
            initialize_helper_objs=False)

        if self.link_cardinality - 1 > 1:
            invalid_dv = "invalid.invalid"
        else:
            invalid_dv = "invalid"

        self.create_or_delete_users(self.rbac_util, rbac_users_created)

        link = self.cbas_util.list_all_link_objs()[0]

        testcases = [
                {
                    "description": "Create a link with a non-existent dataverse",
                    "dataverse": invalid_dv,
                    "expected_error": "Cannot find analytics scope with name {0}".format(self.invalid_value),
                    "create_dataverse":False
                },
                {
                    "description": "Create a link with an invalid region name",
                    "region": self.invalid_region,
                    "validate_error_msg": True,
                    "expected_error": "Provided S3 region is not supported"
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
                        CBASHelper.unformat_name(link.full_name))
                },
                {
                    "description": "Create a link with a name of form Local*",
                    "name": "Local123",
                    "expected_error": "Links starting with 'Local' are reserved by the system"
                }
            ]

        rbac_testcases = self.create_testcase_for_rbac_user(
            "Create a link using {0} user", rbac_users_created,
            users_with_permission=[self.analytics_username])

        for testcase in rbac_testcases:
            if testcase["validate_error_msg"]:
                testcase["expected_error"] = "Access denied: user lacks necessary permission(s) to access resource"
        testcases = testcases + rbac_testcases

        failed_testcases = list()

        for testcase in testcases:
            try:
                self.log.info(testcase["description"])

                link_properties = copy.deepcopy(link.properties)

                # Copying link property values for the testcase,
                # to link_property dict, to create a new link.
                for key, value in testcase.iteritems():
                    if key in link_properties:
                        link_properties[key] = value

                if not self.cbas_util.create_link(
                    self.cluster, link_properties,
                    username=testcase.get("username", self.analytics_username),
                    validate_error_msg=testcase.get("validate_error_msg", True),
                    expected_error=testcase.get("expected_error", None),
                    create_dataverse=testcase.get("create_dataverse", True)):
                    raise Exception("Error while creating link")

                if testcase.get("recreate_link", False):
                    testcase["validate_error_msg"] = True
                    if not self.cbas_util.create_link(
                        self.cluster, link_properties,
                        username=testcase.get("username", self.analytics_username),
                        validate_error_msg=testcase.get("validate_error_msg", True),
                        expected_error=testcase.get("expected_error", None)):
                        raise Exception("Error message is different than expected.")
                    testcase["validate_error_msg"] = False

                if not testcase.get("validate_error_msg", True):
                    self.cbas_util.drop_link(
                        self.cluster, link.full_name,
                        username=testcase.get("username", self.analytics_username))

            except Exception as err:
                self.log.error(str(err))
                failed_testcases.append(testcase["description"])
        if failed_testcases:
            self.fail("Following testcases failed - {0}".format(str(failed_testcases)))

    def test_create_multiple_external_link(self):

        self.setup_for_test(
            create_links=False, create_aws_buckets=False,
            create_dataset_objs=False, same_dv_for_link_and_dataset=False,
            initialize_helper_objs=False)

        for link_obj in self.cbas_util.list_all_link_objs():
            if self.cbas_util.create_link(
                    self.cluster, link_obj.properties,
                    username=self.analytics_username):
                if not self.cbas_util.validate_get_link_info_response(
                        self.cluster, link_obj.properties,
                        username=self.analytics_username):
                    self.fail("Link info for {0} does not match the Metadata entry".format(
                        link_obj.full_name))
            else:
                self.fail("Link creation failed.")

    def test_list_external_links(self):
        self.setup_for_test(
            create_links=True, create_aws_buckets=False,
            create_dataset_objs=False, same_dv_for_link_and_dataset=False,
            initialize_helper_objs=False)
        link = self.cbas_util.list_all_link_objs()[0]

        self.create_or_delete_users(self.rbac_util, rbac_users_created)
        testcases = [
            {
                "expected_hits": 1,
                "description": "Parameters Passed - None",
            },
            {
                "dataverse": link.properties["dataverse"],
                "expected_hits": 1,
                "description": "Parameters Passed - Dataverse",
            },
            {
                "dataverse": link.properties["dataverse"],
                "name": link.properties["name"],
                "expected_hits": 1,
                "description": "Parameters Passed - Dataverse, Name",
            },
            {
                "type": "s3",
                "expected_hits": 1,
                "description": "Parameters Passed - Type",
            },
            {
                "name": link.properties["name"],
                "expected_hits": 0,
                "description": "Parameters Passed - Name",
                "validate_error_msg": True,
                "expected_error": "Cannot find analytics scope with name {0}".format(
                    CBASHelper.format_name(link.properties["name"]))
            },
            {
                "dataverse": link.properties["dataverse"],
                "name": link.properties["name"],
                "type": "s3",
                "expected_hits": 1,
                "description": "Parameters Passed - Dataverse, Name and Type",
            }
        ]
        rbac_testcases = self.create_testcase_for_rbac_user(
            "List external links using {0} user", rbac_users_created,
            users_with_permission=[self.analytics_username])

        for testcase in rbac_testcases:
            testcase["dataverse"] = link.properties["dataverse"]
            testcase["name"] = link.properties["name"]
            testcase["type"] = "s3"
            if testcase["username"] in ["admin_analytics"]:
                testcase["validate_error_msg"] = False
            if testcase["validate_error_msg"]:
                testcase["expected_hits"] = 0
                testcase["expected_error"] = "Access denied: user lacks necessary permission(s) to access resource"
            else:
                testcase["expected_hits"] = 1

        testcases = testcases + rbac_testcases
        failed_testcases = list()

        for testcase in testcases:
            try:
                self.log.info(testcase["description"])
                response = self.cbas_util.get_link_info(
                    self.cluster, link_name=testcase.get("name", None),
                    dataverse=testcase.get("dataverse", None),
                    link_type=testcase.get("type", None),
                    validate_error_msg=testcase.get(
                        "validate_error_msg", False),
                    expected_error=testcase.get("expected_error", None),
                    username=testcase.get(
                        "username", self.analytics_username))

                if testcase.get("validate_error_msg", False):
                    if not response:
                        raise Exception("Error message is different than expected.")
                else:
                    if not (len(response) == testcase["expected_hits"]):
                        raise Exception(
                            "Expected links - {0} \t Actual links - {1}".format(
                                testcase["expected_hits"], len(response)))
                    if not (response[0]["scope"] == CBASHelper.metadata_format(
                        link.properties["dataverse"])):
                        raise Exception("Expected - {0} \t Actual- {1}".format(
                            link.properties["dataverse"], response[0]["scope"]))
                    if not (response[0]["name"] == link.properties["name"]):
                        raise Exception("Expected - {0} \t Actual- {1}".format(
                            link.properties["name"], response[0]["name"]))
                    if not (response[0]["type"] == link.properties["type"]):
                        raise Exception("Expected - {0} \t Actual- {1}".format(
                            link.properties["type"], response[0]["type"]))
                    if not (response[0]["accessKeyId"] == link.properties["accessKeyId"]):
                        raise Exception("Access key ID does not match. Expected - {0}\nActual - {1}".format(
                            link.properties["accessKeyId"], response[0]["accessKeyId"]))
                    if not (response[0]["region"] == link.properties["region"]):
                        raise Exception("Region does not match. Expected - {0}\nActual - {1}".format(
                            link.properties["region"], response[0]["region"]))
                    if not (response[0]["secretAccessKey"] == "<redacted sensitive entry>"):
                        raise Exception("secretAccessKey does not match. Expected - {0}\nActual - {1}".format(
                            "<redacted sensitive entry>", response[0]["secretAccessKey"]))
                    if not (response[0]["serviceEndpoint"] == link.properties["serviceEndpoint"]):
                        raise Exception("serviceEndpoint does not match. Expected - {0}\nActual - {1}".format(
                            link.properties["serviceEndpoint"], response[0]["serviceEndpoint"]))
            except Exception as err:
                self.log.error(str(err))
                failed_testcases.append(testcase["description"])
        if failed_testcases:
            self.fail("Following testcases failed - {0}".format(str(failed_testcases)))

    def test_alter_link_properties(self):
        self.setup_for_test(
            create_links=True, create_aws_buckets=True,
            create_dataset_objs=True, same_dv_for_link_and_dataset=True,
            create_datasets=True, initialize_helper_objs=True)

        if self.link_cardinality - 1 > 1:
            invalid_dv = "invalid.invalid"
        else:
            invalid_dv = "invalid"

        self.create_or_delete_users(self.rbac_util, rbac_users_created)

        link = self.cbas_util.list_all_link_objs()[0]
        dataset = self.cbas_util.list_all_dataset_objs()[0]

        aws_access_key_1, aws_secret_key_1, aws_session_token_1 = self.get_aws_credentials("full_access_2")
        self.aws_region_list.remove(link.properties["region"])
        region2 = random.choice(self.aws_region_list)

        testcases = [
            {
                "description": "Changing dataverse to another existing dataverse",
                "validate_error_msg": True,
                "expected_error": "Link {0} does not exist",
                "new_dataverse": True
            },
            {
                "description": "Changing dataverse to a non-existing dataverse",
                "dataverse": invalid_dv,
                "validate_error_msg": True,
                "expected_error": "Cannot find analytics scope with name {0}".format(self.invalid_value)
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
                "region": self.invalid_region,
                "validate_error_msg": True,
                "expected_error": "Provided S3 region is not supported"
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
        rbac_testcases = self.create_testcase_for_rbac_user(
            "Altering external link properties using {0} user",
            rbac_users_created, users_with_permission=[self.analytics_username])

        for testcase in rbac_testcases:
            if testcase["validate_error_msg"]:
                testcase["expected_error"] = "Access denied: user lacks necessary permission(s) to access resource"
        testcases = testcases + rbac_testcases
        failed_testcases = list()

        result = self.s3_data_helper.generate_data_for_s3_and_upload(
            aws_bucket_name=dataset.dataset_properties["aws_bucket_name"],
            key=self.key, no_of_files=1, file_formats=["json"], no_of_folders=0,
            max_folder_depth=0, header=False, null_key="", operation="create",
            bucket=self.cluster.buckets[0], no_of_docs=10,
            randomize_header=False, large_file=False, missing_field=[False])
        if result:
            self.fail("Error while uploading files to S3")

        cbas_query = "Select count(*) from {0};".format(dataset.full_name)
        status, metrics, errors, cbas_result, handle = self.cbas_util.execute_statement_on_cbas_util(
            self.cluster, cbas_query, timeout=120, analytics_timeout=120)

        for testcase in testcases:
            try:
                self.log.info(testcase["description"])

                link_properties = copy.deepcopy(link.properties)

                # Copying link property values for the testcase, to link_property dict, to create a new link.
                for key, value in testcase.iteritems():
                    if key in link_properties:
                        link_properties[key] = value

                if testcase.get("new_dataverse", False):
                    link_properties["dataverse"] = "dataverse2"

                    if not self.cbas_util.create_dataverse(
                        self.cluster, dataverse_name=link_properties["dataverse"],
                        username=self.analytics_username):
                        raise Exception("Dataverse creation failed")

                    testcase["expected_error"] = testcase.get("expected_error", None).format(
                        link_properties["dataverse"] + "." + CBASHelper.unformat_name(link.name))

                response = self.cbas_util.update_external_link_properties(
                    self.cluster, link_properties,
                    validate_error_msg=testcase.get("validate_error_msg", False),
                    expected_error=testcase.get("expected_error", None),
                    username=testcase.get("username", self.analytics_username))

                if testcase.get("validate_error_msg", False):
                    if not response:
                        raise Exception("Error message is different than expected.")
                else:
                    status, metrics, errors, cbas_result_2, handle = self.cbas_util.execute_statement_on_cbas_util(
                        self.cluster, cbas_query, timeout=120,
                        analytics_timeout=120)
                    if status == "fatal":
                        if link.properties["region"] != link_properties["region"]:
                            pass
                        elif errors and "External source error" in errors[0]["msg"]:
                            pass
                        else:
                            raise Exception(
                                "Following error is raised while reading data from AWS after altering the link\nERROR : {0}"
                                    .format(str(errors)))

                    elif cbas_result[0]["$1"] != cbas_result_2[0]["$1"]:
                        raise Exception("Data read from AWS before and after altering link do not match")

                    response2 = self.cbas_util.get_link_info(
                        self.cluster, link_name=link_properties["name"],
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
                    if not self.cbas_util.drop_dataverse(
                        self.cluster,
                        dataverse_name=link_properties["dataverse"],
                        username=self.analytics_username):
                        raise Exception("Dataverse creation failed")

                self.log.info("Test Passed")

            except Exception as err:
                self.log.error(str(err))
                failed_testcases.append(testcase["description"])

        if failed_testcases:
            self.fail("Following testcases failed - {0}".format(str(failed_testcases)))

    def test_connect_link(self):
        self.setup_for_test(
            create_links=True, create_aws_buckets=False,
            create_dataset_objs=False, same_dv_for_link_and_dataset=False,
            initialize_helper_objs=False)
        link = self.cbas_util.list_all_link_objs()[0]

        if not self.cbas_util.connect_link(
            self.cluster, link.full_name, validate_error_msg=True,
            expected_error="Link {0}.{1} cannot be used for data ingestion".format(
                CBASHelper.unformat_name(link.dataverse_name),
                CBASHelper.unformat_name(link.name))):
            self.fail("Expected error message different from actual error message")

    def test_disconnect_link(self):
        self.setup_for_test(
            create_links=True, create_aws_buckets=False,
            create_dataset_objs=False, same_dv_for_link_and_dataset=False,
            initialize_helper_objs=False)
        link = self.cbas_util.list_all_link_objs()[0]

        if not self.cbas_util.disconnect_link(
            self.cluster, link.full_name, validate_error_msg=True,
            expected_error="Link {0}.{1} cannot be used for data ingestion".format(
                CBASHelper.unformat_name(link.dataverse_name),
                CBASHelper.unformat_name(link.name))):
            self.fail("Expected error message different from actual error message")

    def test_create_dataset(self):
        self.setup_for_test(
            create_links=True, create_aws_buckets=True,
            create_dataset_objs=True, same_dv_for_link_and_dataset=True,
            initialize_helper_objs=False)
        link = self.cbas_util.list_all_link_objs()[0]
        dataset = self.cbas_util.list_all_dataset_objs()[0]

        # Create users with all RBAC roles.
        self.create_or_delete_users(self.rbac_util, rbac_users_created)
        self.aws_region_list.remove(link.properties["region"])
        new_region = random.choice(self.aws_region_list)
        new_aws_bucket = "new-cbas-regression-{0}".format(
            random.randint(1, 10000))

        def create_bucket_in_other_region(aws_bucket, region):
            if perform_S3_operation(
                    aws_access_key=self.aws_access_key,
                    aws_secret_key=self.aws_secret_key,
                    aws_session_token=self.aws_session_token, create_bucket=True,
                    bucket_name=aws_bucket, region=region):
                self.aws_buckets[aws_bucket]=region
                return True
            else:
                return False
            self.sleep(60, "Sleeping for 60 seconds to ensure that AWS bucket is created")

        testcases = [
            {
                "description": "Create dataset with same name",
                "recreate_dataset": True,
                "validate_error_msg": True,
                "expected_error": "An analytics collection with name {0} already exists in analytics scope {1}".format(
                    CBASHelper.unformat_name(dataset.name),
                    CBASHelper.unformat_name(dataset.dataverse_name))
            },
            {
                "description": "Create dataset without type definition for CSV format",
                "file_format": "csv",
                "header": False,
                "validate_error_msg": True,
                "expected_error": "Inline type definition is required for external analytics collections with 'csv' format"
            },
            {
                "description": "Create dataset without type definition for TSV format",
                "file_format": "tsv",
                "header": False,
                "validate_error_msg": True,
                "expected_error": "Inline type definition is required for external analytics collections with 'tsv' format"
            },
            {
                "description": "Create dataset with type definition for JSON format",
                "object_construction_def": "id INT",
                "validate_error_msg": True,
                "expected_error": "Inline type definition is not allowed for external analytics collections with 'json' format"
            },
            {
                "description": "Create dataset with type definition for parquet format",
                "file_format": "parquet",
                "object_construction_def": "id INT",
                "validate_error_msg": True,
                "expected_error": "Inline type definition is not allowed for external analytics collections with 'parquet' format"
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
                "dataverse_name": "dataverse2",
                "link_name": link.full_name,
                "new_dataverse": True,
                "validate_error_msg": False
            },
            {
                "description": "Create dataset when link's dataverse is not specified but dataset's \
                dataverse is specified and link's dataverse does not defaults to Default dataverse",
                "dataverse_name": "dataverse2",
                "new_dataverse": True,
                "validate_error_msg": True,
                "link_name": link.name,
                "expected_error": "Link dataverse2.{0} does not exist".format(
                    CBASHelper.unformat_name(link.name))
            },
            {
                "description": "Create dataset when link's dataverse is not specified but dataset's \
                dataverse is specified and link's dataverse defaults to datasets dataverse",
                "validate_error_msg": False,
                "link_name": link.name
            },
            {
                "description": "Create dataset in non-existent dataverse",
                "dataverse_name": self.invalid_value,
                "create_dv":False,
                "validate_error_msg": True,
                "expected_error": "Cannot find analytics scope with name {0}".format(self.invalid_value)
            },
            {
                "description": "Create dataset on non-existent link",
                "link_name": self.invalid_value,
                "validate_error_msg": True,
                "expected_error": "Link {0}.{1} does not exist".format(
                    CBASHelper.unformat_name(dataset.dataverse_name),
                    self.invalid_value)
            },
            {
                "description": "Create dataset on Local link",
                "link_name": "Local",
                "validate_error_msg": True,
                "expected_error": "Link {0}.Local cannot be used for external analytics collections".format(
                    CBASHelper.unformat_name(link.dataverse_name))
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
                "expected_error": "Invalid value for parameter 'format'"
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
                "expected_error": "Invalid parameter 'header'"
            },
            {
                "description": "Create dataset with header flag for parquet file format",
                "file_format": "parquet",
                "header": False,
                "validate_error_msg": True,
                "expected_error": "Invalid parameter 'header'"
            },
            {
                "description": "Create dataset with invalid header flag for tsv file format",
                "file_format": "tsv",
                "object_construction_def": "name STRING",
                "header": "False1",
                "validate_error_msg": True,
                "expected_error": "Invalid value for parameter 'header'"
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
                "expected_error": "Invalid parameter 'null'"
            },
            {
                "description": "Create dataset with null flag for parquet file format",
                "file_format": "parquet",
                "null_string": "\N",
                "validate_error_msg": True,
                "expected_error": "Invalid parameter 'null'"
            },
            {
                "description": "Create dataset with invalid redact warning flag",
                "file_format": "json",
                "redact_warning": "False1",
                "validate_error_msg": True,
                "expected_error": "Invalid value for parameter 'redact-warnings'"
            },
            {
                "description": "Create dataset with valid redact warning flag",
                "file_format": "parquet",
                "redact_warning": "True"
            },
            {
                "description": "Create dataset with both include and exclude flag set",
                "file_format": "json",
                "include": "*.json",
                "exclude": "*.csv",
                "validate_error_msg": True,
                "expected_error": "The parameters 'include' and 'exclude' cannot be provided at the same time"
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
                "description": "Create dataset with include flag set with "
                               "valid pattern for parquet file format",
                "file_format": "parquet",
                "include": "*.parquet",
                "validate_error_msg": False
            },
            {
                "description": "Create dataset with exclude flag set with valid pattern",
                "file_format": "json",
                "exclude": "*.csv",
                "validate_error_msg": False
            },
            {
                "description": "Create dataset with exclude flag set with "
                               "valid pattern for parquet file format",
                "file_format": "parquet",
                "exclude": "*.json",
                "validate_error_msg": False
            }
        ]

        rbac_testcases = self.create_testcase_for_rbac_user(
            "Creating dataset on external link using {0} user",
            rbac_users_created, users_with_permission=[self.analytics_username])

        for tc in rbac_testcases:
            if tc["validate_error_msg"]:
                tc["expected_error"] = "User must have permission"
                tc["create_dv"] = False

        testcases = testcases + rbac_testcases

        failed_testcases = list()

        for testcase in testcases:
            try:
                self.log.info(testcase["description"])

                temp_dataset = copy.deepcopy(dataset)

                for param in testcase:
                    if hasattr(temp_dataset, param):
                        setattr(temp_dataset, param, testcase[param])
                    elif param in temp_dataset.dataset_properties:
                        temp_dataset.dataset_properties[param] = testcase[param]

                if testcase.get("new_dataverse", False):
                    if not self.cbas_util.create_dataverse(
                            self.cluster, temp_dataset.dataverse_name):
                        raise Exception("Error while creating new dataverse")

                if testcase.get("recreate_dataset", False):
                    testcase["validate_error_msg"] = False

                if not self.cbas_util.create_dataset_on_external_resource(
                        self.cluster, temp_dataset.name,
                        link_name=testcase.get("link_name", temp_dataset.link_name),
                        dataverse_name=temp_dataset.dataverse_name,
                        validate_error_msg=testcase.get(
                            "validate_error_msg", False),
                        username=testcase.get("username", self.analytics_username),
                        expected_error=testcase.get("expected_error", ""),
                        create_dv=testcase.get("create_dv", True),
                        **temp_dataset.dataset_properties):
                    raise Exception("Dataset creation failed")

                if testcase.get("recreate_dataset", False):
                    testcase["validate_error_msg"] = True
                    if not self.cbas_util.create_dataset_on_external_resource(
                            self.cluster, temp_dataset.name,
                            link_name=temp_dataset.link_name,
                            dataverse_name=temp_dataset.dataverse_name,
                            validate_error_msg=testcase.get(
                                "validate_error_msg", False),
                            username=testcase.get("username", self.analytics_username),
                            expected_error=testcase.get("expected_error", ""),
                            **temp_dataset.dataset_properties):
                        raise Exception("Dataset creation failed")
                    testcase["validate_error_msg"] = False

                if testcase.get("new_dataverse", False):
                    if not self.cbas_util.drop_dataverse(
                            self.cluster, temp_dataset.dataverse_name):
                        raise Exception("Error while deleting new dataverse")
                    temp_dataset.dataverse_name = dataset.dataverse_name

                if testcase.get("new_bucket", False):
                    if not perform_S3_operation(
                            aws_access_key=self.aws_access_key,
                            aws_secret_key=self.aws_secret_key,
                            aws_session_token=self.aws_session_token,
                            create_bucket=False,
                            bucket_name=temp_dataset.dataset_properties["aws_bucket_name"],
                            delete_bucket=True):
                        raise Exception("Error while deleting bucket")
                    self.sleep(60, "Sleeping for 60 seconds to ensure that AWS bucket is deleted")

                if not testcase.get("validate_error_msg",False):
                    self.cbas_util.drop_dataset(self.cluster,
                                                temp_dataset.full_name)
            except Exception as err:
                self.log.error(str(err))
                failed_testcases.append(testcase["description"])
        if failed_testcases:
            self.fail("Following testcases failed - {0}".format(str(failed_testcases)))

    def test_drop_dataset(self):

        self.setup_for_test(
            create_links=True, create_aws_buckets=True,
            create_dataset_objs=True, same_dv_for_link_and_dataset=False,
            create_datasets=True, initialize_helper_objs=False)
        dataset_obj = self.cbas_util.list_all_dataset_objs()[0]

        # Create users with all RBAC roles.
        self.create_or_delete_users(self.rbac_util, rbac_users_created)

        testcases = self.create_testcase_for_rbac_user(
            "Dropping dataset on external link using {0} user",
            rbac_users_created, users_with_permission=[self.analytics_username])

        for tc in testcases:
            if tc["validate_error_msg"]:
                tc["expected_error"] = "User must have permission"

        failed_testcases = list()

        recreate_dataset = False

        for testcase in testcases:
            try:
                self.log.info(testcase["description"])

                if recreate_dataset:
                    if not self.cbas_util.create_dataset_on_external_resource(
                            self.cluster, dataset_obj.name,
                            link_name=dataset_obj.link_name,
                            dataverse_name=dataset_obj.dataverse_name,
                            **dataset_obj.dataset_properties):
                        self.fail("Dataset creation failed")
                    recreate_dataset = False

                if not self.cbas_util.drop_dataset(
                        self.cluster, dataset_obj.full_name,
                        validate_error_msg=testcase.get(
                            "validate_error_msg", False),
                        username=testcase.get("username", self.analytics_username),
                        expected_error=testcase.get("expected_error", "")):
                    raise Exception("Error while dropping dataset")

                if not testcase.get("validate_error_msg",False):
                    recreate_dataset = True

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
        self.setup_for_test(
            create_links=True, create_aws_buckets=True,
            create_dataset_objs=True, same_dv_for_link_and_dataset=False,
            initialize_helper_objs=True)
        dataset_obj = self.cbas_util.list_all_dataset_objs()[0]

        # read params from test config
        for param in self.input.test_params:
            if param == "include":
                dataset_obj.dataset_properties[param] = self.get_include_exclude_flag(
                    True, self.input.param(param))
            elif param == "exclude":
                dataset_obj.dataset_properties[param] = self.get_include_exclude_flag(
                    False, self.input.param(param))
            elif hasattr(dataset_obj, param):
                setattr(dataset_obj, param, self.input.param(param))
            elif param in dataset_obj.dataset_properties:
                dataset_obj.dataset_properties[param] = self.input.param(param)

        # generate and upload data to AWS S3
        if self.input.param("mix_data_file", False):
            if not self.s3_data_helper.generate_mix_data_file(
                    dataset_obj.dataset_properties["aws_bucket_name"],
                    file_format=dataset_obj.dataset_properties["file_format"],
                    upload_to_s3=True):
                self.fail("Error while uploading files to S3")
        elif self.input.param("file_extension", None):
            if not self.s3_data_helper.generate_file_with_record_of_size_and_upload(
                    dataset_obj.dataset_properties["aws_bucket_name"],
                    "sample", record_size=1024,
                    file_format=dataset_obj.dataset_properties["file_format"],
                    upload_to_s3=True,
                    file_extension=self.input.param("file_extension")):
                self.fail("Error while uploading files to S3")
        else:
            missing_field = [self.input.param("missing_field", False)]
            result = self.s3_data_helper.generate_data_for_s3_and_upload(
                aws_bucket_name=dataset_obj.dataset_properties["aws_bucket_name"],
                key=self.key,
                no_of_files=self.input.param("no_of_files", 5),
                file_formats=json.loads(self.input.param(
                    "file_format_for_upload", "[\"json\"]")),
                no_of_folders=self.input.param("no_of_folders", 0),
                max_folder_depth=self.input.param("max_folder_depth", 0),
                header=self.input.param("header_s3_file", False),
                null_key=self.input.param("null_s3_file", ""),
                operation="create", bucket=self.cluster.buckets[0],
                no_of_docs=self.input.param("no_of_docs", 100),
                randomize_header=self.input.param("randomize_header", False),
                large_file=self.input.param("large_file", False),
                missing_field=missing_field)
            if result:
                self.fail("Error while uploading files to S3")

        # run query on dataset and on bucket that was used to generate AWS data.
        n1ql_query = self.get_n1ql_query(self.input.param("n1ql_query", "count_all"))
        cbas_query = self.input.param("cbas_query", "Select count(*) from {0};")

        # path_on_aws_bucket is used to set Using condition in create dataset DDL, when path_on_aws_bucket is True,
        # we will set the USING condition with valid AWS path else if False we will set it with invalid AWS path.
        if self.input.param("select_aws_path", False):
            if len(self.s3_data_helper.folders) > 1:
                folder_name = random.choice(self.s3_data_helper.folders)
                while folder_name == "":
                    folder_name = random.choice(self.s3_data_helper.folders)
                dataset_obj.dataset_properties["path_on_aws_bucket"] = folder_name
                n1ql_query = n1ql_query.format("{0}", folder_name)

        if self.input.param("invalid_aws_path", False):
            dataset_obj.dataset_properties["path_on_aws_bucket"] = "invalid/invalid"

        # create dataset
        if not self.cbas_util.create_dataset_on_external_resource(
            self.cluster, dataset_obj.name, link_name=dataset_obj.link_name,
            dataverse_name=dataset_obj.dataverse_name,
            **dataset_obj.dataset_properties):
            self.fail("Dataset creation failed")

        n1ql_result = self.cluster.rest.query_tool(n1ql_query.format(
            self.cluster.buckets[0].name))["results"][0]["$1"]
        status, metrics, errors, cbas_result, handle = self.cbas_util.execute_statement_on_cbas_util(
            self.cluster, cbas_query.format(dataset_obj.full_name),
            timeout=120, analytics_timeout=120)

        if self.input.param("header", False) and not self.input.param(
            "header_s3_file", False):
            n1ql_result -= self.input.param("no_of_files", 5)
        elif not self.input.param("header", False) and self.input.param(
            "header_s3_file", False):
            if not metrics['warningCount'] == self.input.param("no_of_files", 5):
                self.fail("No warnings were raised for data mismatch while querying on csv or tsv file.")

        if self.input.param("validate_error_conditions", False):
            if dataset_obj.dataset_properties["file_format"] == "parquet":
                if not "not a Parquet file" in errors[0]["msg"]:
                    self.fail(
                        "Expected error message does not match actual data")
            else:
                if not errors[0]["msg"] == "Malformed input stream":
                    self.fail("Expected error message does not match actual data")
        else:
            if self.input.param("expected_count", None) is not None:
                n1ql_result = int(self.input.param("expected_count"))
            if not n1ql_result == cbas_result[0]["$1"]:
                self.fail(
                    "Expected data does not match actual data. Expected count - {0} Actual count - {1}".format(
                        n1ql_result, cbas_result[0]["$1"]))

    def test_effect_of_rbac_role_change_on_external_link(self):
        new_username = "user1"
        original_user_role = "analytics_admin"
        new_user_role = "analytics_reader"
        self.rbac_util._create_user_and_grant_role(new_username, original_user_role)
        rbac_users_created[new_username] = None

        self.setup_for_test(
            create_links=True, create_aws_buckets=True,
            create_dataset_objs=True, same_dv_for_link_and_dataset=False,
            create_datasets=True, initialize_helper_objs=True)
        dataset_obj = self.cbas_util.list_all_dataset_objs()[0]

        result = self.s3_data_helper.generate_data_for_s3_and_upload(
            aws_bucket_name=dataset_obj.dataset_properties["aws_bucket_name"],
            key=self.key, no_of_files=5, file_formats=["json"], no_of_folders=0,
            max_folder_depth=0, header=False, null_key="", operation="create",
            bucket=self.cluster.buckets[0], no_of_docs=100,
            randomize_header=False, large_file=False, missing_field=[False])
        if result:
            self.fail("Error while uploading files to S3")

        # run query on dataset and on bucket that was used to generate AWS data.
        n1ql_query = "Select count(*) from {0};".format(self.cluster.buckets[0].name)
        n1ql_result = self.cluster.rest.query_tool(n1ql_query)["results"][0]["$1"]
        if not self.cbas_util.validate_cbas_dataset_items_count(
            self.cluster, dataset_obj.full_name, n1ql_result):
            self.fail("Expected data does not match actual data")

        self.rbac_util._create_user_and_grant_role(new_username, new_user_role)

        if not self.cbas_util.validate_cbas_dataset_items_count(
            self.cluster, dataset_obj.full_name, n1ql_result):
            self.fail("Expected data does not match actual data")

    def test_querying_while_network_failure(self):
        self.setup_for_test(
            create_links=True, create_aws_buckets=True,
            create_dataset_objs=True, same_dv_for_link_and_dataset=False,
            create_datasets=True, initialize_helper_objs=True)
        dataset_obj = self.cbas_util.list_all_dataset_objs()[0]

        result = self.s3_data_helper.generate_data_for_s3_and_upload(
            aws_bucket_name=dataset_obj.dataset_properties["aws_bucket_name"],
            key=self.key, no_of_files=5, file_formats=["json"], no_of_folders=0,
            max_folder_depth=0, header=False, null_key="", operation="create",
            bucket=self.cluster.buckets[0], no_of_docs=10000,
            randomize_header=False, large_file=False, missing_field=[False])
        if result:
            self.fail("Error while uploading files to S3")

        shell = RemoteMachineShellConnection(self.cluster.cbas_cc_node)

        cbas_query = "Select count(*) from {0};".format(dataset_obj.full_name)
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
        self.setup_for_test(
            create_links=True, create_aws_buckets=True,
            create_dataset_objs=True, same_dv_for_link_and_dataset=False,
            create_datasets=True, initialize_helper_objs=True)
        dataset_obj = self.cbas_util.list_all_dataset_objs()[0]

        result = self.s3_data_helper.generate_data_for_s3_and_upload(
            aws_bucket_name=dataset_obj.dataset_properties["aws_bucket_name"],
            key=self.key, no_of_files=self.input.param("no_of_files", 1000),
            file_formats=["json"], no_of_folders=0, max_folder_depth=0,
            header=False, null_key="", operation="create",
            bucket=self.cluster.buckets[0], no_of_docs=100000,
            randomize_header=False, large_file=False, missing_field=[False])
        if result:
            self.fail("Error while uploading files to S3")

        n1ql_query = "Select count(*) from {0};".format(
            self.cluster.buckets[0].name)
        n1ql_result = self.cluster.rest.query_tool(n1ql_query)["results"][0]["$1"]
        if not self.cbas_util.validate_cbas_dataset_items_count(
            self.cluster,dataset_obj.full_name, n1ql_result,
            timeout=3600, analytics_timeout=3600):
            self.fail("Expected data does not match actual data")

    def test_file_deletion_from_AWS_while_query_is_reading_file(self):
        self.setup_for_test(
            create_links=True, create_aws_buckets=True,
            create_dataset_objs=True, same_dv_for_link_and_dataset=False,
            create_datasets=True, initialize_helper_objs=True)
        dataset_obj = self.cbas_util.list_all_dataset_objs()[0]

        file_dict = self.s3_data_helper.generate_file_of_specified_size_and_upload(
            bucket_name=dataset_obj.dataset_properties["aws_bucket_name"],
            no_of_files=self.input.param("no_of_files", 5),
            file_size_in_KB=self.input.param("file_size", 100),
            record_type=self.input.param("record_type", "json"),
            upload_to_s3=True, file_extension=None)

        if self.input.param("delete_last_file", False):
            # selecting a middle file to delete, as list object from aws returns object name in sorted alpha numeric way.
            file_to_delete = file_dict.keys()[-1]
        else:
            file_to_delete = file_dict.keys()[0]

        if not self.input.param("recreate", False) and \
                self.input.param("delete_last_file", False):
            n1ql_result = 0
            for filename in file_dict:
                if filename != file_to_delete:
                    n1ql_result += file_dict[filename]
        else:
            n1ql_result = sum(file_dict.values())

        cbas_query = "Select count(*) from {0};".format(dataset_obj.full_name)

        dest_path = os.path.join(r"/tmp/", file_to_delete)
        if self.input.param("recreate", False):
            # Downloading file to be deleted, so that it can be recreated again
            if not perform_S3_operation(
                aws_access_key=self.aws_access_key,
                aws_secret_key=self.aws_secret_key,
                aws_session_token=self.aws_session_token,
                bucket_name=dataset_obj.dataset_properties["aws_bucket_name"],
                download_file=True, src_path=file_to_delete, dest_path=dest_path):
                self.fail("Error while downloading file from S3")
            delete_only = False
        else:
            delete_only = True

        for server in self.cluster.servers:
            RemoteUtilHelper.set_upload_download_speed(
                server=server, download=1000)
            self.speed_limit_set = True

        result = list()

        threads = list()
        thread1 = Thread(
            target=self.execute_cbas_query, name="s3_query_thread",
            args=(cbas_query, result, 300, 300,))
        thread1.start()
        threads.append(thread1)
        self.sleep(3)

        thread2 = Thread(
            target=self.perform_delete_recreate_file_on_AWS_S3, name="s3_thread",
            args=(dataset_obj.dataset_properties["aws_bucket_name"],
                  file_to_delete, dest_path, delete_only,))
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

    def test_analytics_cluster_when_rebalancing_in_cbas_node(self):

        self.setup_for_test(
            create_links=True, create_aws_buckets=True,
            create_dataset_objs=True, same_dv_for_link_and_dataset=False,
            create_datasets=True, initialize_helper_objs=True, rebalance_util=True)
        dataset_obj = self.cbas_util.list_all_dataset_objs()[0]

        result = self.s3_data_helper.generate_data_for_s3_and_upload(
            aws_bucket_name=dataset_obj.dataset_properties["aws_bucket_name"],
            key=self.key, no_of_files=5, file_formats=["json"], no_of_folders=0,
            max_folder_depth=0, header=False, null_key="", operation="create",
            bucket=self.cluster.buckets[0], no_of_docs=self.num_items,
            randomize_header=False, large_file=False, missing_field=[False])

        if result:
            self.fail("Error while uploading files to S3")

        query = "Select count(*) from `{0}`;"
        n1ql_result = self.cluster.rest.query_tool(query.format(
            self.cluster.buckets[0].name))["results"][0]["$1"]

        rebalance_task, self.available_servers = self.rebalance_util.rebalance(
            self.cluster, kv_nodes_in=0, kv_nodes_out=0, cbas_nodes_in=1,
            cbas_nodes_out=0, available_servers=self.available_servers,
            exclude_nodes=self.cluster.exclude_nodes)

        _ , cbas_query_task = self.rebalance_util.start_parallel_queries(
            self.cluster, run_kv_queries=False, run_cbas_queries=True,
            parallelism=3)

        if not self.rebalance_util.wait_for_rebalance_task_to_complete(
                rebalance_task, self.cluster):
            self.fail("Rebalancing in CBAS node failed.")

        self.rebalance_util.stop_parallel_queries(None, cbas_query_task)

        if not n1ql_result == cbas_query_task.result[0][0]["$1"]:
            self.fail("Number of items in dataset do not match number of items in bucket")

    def test_analytics_cluster_swap_rebalancing(self):
        self.setup_for_test(
            create_links=True, create_aws_buckets=True,
            create_dataset_objs=True, same_dv_for_link_and_dataset=False,
            create_datasets=True, initialize_helper_objs=True, rebalance_util=True)
        dataset_obj = self.cbas_util.list_all_dataset_objs()[0]

        result = self.s3_data_helper.generate_data_for_s3_and_upload(
            aws_bucket_name=dataset_obj.dataset_properties["aws_bucket_name"],
            key=self.key, no_of_files=5, file_formats=["json"], no_of_folders=0,
            max_folder_depth=0, header=False, null_key="", operation="create",
            bucket=self.cluster.buckets[0], no_of_docs=self.num_items,
            randomize_header=False, large_file=False, missing_field=[False])

        if result:
            self.fail("Error while uploading files to S3")

        query = "Select count(*) from `{0}`;"
        n1ql_result = self.cluster.rest.query_tool(query.format(
            self.cluster.buckets[0].name))["results"][0]["$1"]

        rebalance_task, self.available_servers = self.rebalance_util.rebalance(
            self.cluster, kv_nodes_in=0, kv_nodes_out=0, cbas_nodes_in=1,
            cbas_nodes_out=1, available_servers=self.available_servers,
            exclude_nodes=self.cluster.exclude_nodes)

        _ , cbas_query_task = self.rebalance_util.start_parallel_queries(
            self.cluster, run_kv_queries=False, run_cbas_queries=True,
            parallelism=3)

        if not self.rebalance_util.wait_for_rebalance_task_to_complete(
                rebalance_task, self.cluster):
            self.fail("Rebalancing in CBAS node failed.")

        self.rebalance_util.stop_parallel_queries(None, cbas_query_task)

        if not n1ql_result == cbas_query_task.result[0][0]["$1"]:
            self.fail("Number of items in dataset do not match number of items in bucket")

    def test_analytics_cluster_when_rebalancing_out_cbas_node(self):
        self.setup_for_test(
            create_links=True, create_aws_buckets=True,
            create_dataset_objs=True, same_dv_for_link_and_dataset=False,
            create_datasets=True, initialize_helper_objs=True, rebalance_util=True)
        dataset_obj = self.cbas_util.list_all_dataset_objs()[0]

        result = self.s3_data_helper.generate_data_for_s3_and_upload(
            aws_bucket_name=dataset_obj.dataset_properties["aws_bucket_name"],
            key=self.key, no_of_files=5, file_formats=["json"], no_of_folders=0,
            max_folder_depth=0, header=False, null_key="", operation="create",
            bucket=self.cluster.buckets[0], no_of_docs=self.num_items,
            randomize_header=False, large_file=False, missing_field=[False])

        if result:
            self.fail("Error while uploading files to S3")

        query = "Select count(*) from `{0}`;"
        n1ql_result = self.cluster.rest.query_tool(query.format(
            self.cluster.buckets[0].name))["results"][0]["$1"]

        rebalance_task, self.available_servers = self.rebalance_util.rebalance(
            self.cluster, kv_nodes_in=0, kv_nodes_out=0, cbas_nodes_in=0,
            cbas_nodes_out=1, available_servers=self.available_servers,
            exclude_nodes=self.cluster.exclude_nodes)

        _ , cbas_query_task = self.rebalance_util.start_parallel_queries(
            self.cluster, run_kv_queries=False, run_cbas_queries=True,
            parallelism=3)

        if not self.rebalance_util.wait_for_rebalance_task_to_complete(
                rebalance_task, self.cluster):
            self.fail("Rebalancing in CBAS node failed.")

        self.rebalance_util.stop_parallel_queries(None, cbas_query_task)

        if not n1ql_result == cbas_query_task.result[0][0]["$1"]:
            self.fail("Number of items in dataset do not match number of items in bucket")

    def test_fail_over_cbas_node_followed_by_rebalance_out_or_add_back(self):
        self.setup_for_test(
            create_links=True, create_aws_buckets=True,
            create_dataset_objs=True, same_dv_for_link_and_dataset=False,
            create_datasets=True, initialize_helper_objs=True, rebalance_util=True)
        dataset_obj = self.cbas_util.list_all_dataset_objs()[0]

        for i in range(0, 2):
            filename = "big_record_file_{0}".format(str(i))
            if not self.s3_data_helper.generate_file_with_record_of_size_and_upload(
                dataset_obj.dataset_properties["aws_bucket_name"], filename,
                record_size=29 * 1024 * 1024, file_format="json",
                upload_to_s3=True):
                self.fail("Error while uploading files to S3")
        self.log.info("File upload successfull")

        _ , cbas_query_task = self.rebalance_util.start_parallel_queries(
            self.cluster, run_kv_queries=False, run_cbas_queries=True,
            parallelism=3)

        self.available_servers, _, _ = self.rebalance_util.failover(
            self.cluster, failover_type="Hard",
            action=self.input.param("action", "FullRecovery"),
            cbas_nodes=1, timeout=7200,
            available_servers=self.available_servers,
            exclude_nodes=self.cluster.exclude_nodes)

        self.rebalance_util.stop_parallel_queries(None, cbas_query_task)

        if not self.cbas_util.validate_cbas_dataset_items_count(
            self.cluster, dataset_obj.full_name, 2,
            timeout=600, analytics_timeout=600):
            self.fail("Expected data does not match actual data")

    def test_when_a_single_record_size_is_greater_than_32MB(self):
        self.setup_for_test(
            create_links=True, create_aws_buckets=True,
            create_dataset_objs=True, same_dv_for_link_and_dataset=False,
            create_datasets=False, initialize_helper_objs=True)
        dataset = self.cbas_util.list_all_dataset_objs()[0]

        file_format = self.input.param("file_format", "json")
        if file_format == "parquet":
            file_extension = "json"
        else:
            file_extension = None
        record_size = int(self.input.param("record_size", 32))
        filename = "big_record_file"

        dataset.dataset_properties["file_format"] = file_format

        if file_format in ["csv", "tsv"]:
            dataset.dataset_properties[
                "object_construction_def"] = "key1 string, key2 string, key3 string, key4 string, key5 string"
            dataset.dataset_properties["header"] = False

        if not self.cbas_util.create_dataset_on_external_resource(
            self.cluster, dataset.name, link_name=dataset.link_name,
            dataverse_name=dataset.dataverse_name,
            **dataset.dataset_properties):
            self.fail("Dataset creation failed")

        if not self.s3_data_helper.generate_file_with_record_of_size_and_upload(
            dataset.dataset_properties["aws_bucket_name"], filename,
            record_size=record_size * 1024 * 1024, file_format=file_format,
            upload_to_s3=True, file_extension=file_extension):
            self.fail("Error while uploading files to S3")

        self.log.info("File upload successfull")
        cbas_query = "Select count(*) from {0};".format(dataset.full_name)
        status, metrics, errors, results, handle = self.cbas_util.execute_statement_on_cbas_util(
            self.cluster, cbas_query)

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
        self.aws_buckets = {"cbas-regression-1":"us-west-1"}
        object_construction_def = "key1 STRING, key2 STRING, key3 STRING, key4 STRING"
        doc_counts = {
            "json": 13950000,
            "csv": 33560000,
            "tsv": 33560000,
        }

        self.setup_for_test(
            create_links=False, create_aws_buckets=False,
            create_dataset_objs=True, same_dv_for_link_and_dataset=False,
            create_datasets=False, initialize_helper_objs=False)

        link_obj = self.cbas_util.list_all_link_objs(link_type="s3")[0]
        link_obj.properties["region"] = "us-west-1"
        if not self.cbas_util.create_link(
            self.cluster, link_obj.properties, username=self.analytics_username):
            self.fail("link creation failed")

        file_format = self.input.param("file_format", "json")

        dataset_obj = self.cbas_util.list_all_dataset_objs()[0]
        dataset_obj.dataset_properties["file_format"] = file_format
        dataset_obj.dataset_properties["include"] = "*.{0}".format(file_format)

        if file_format in ["csv", "tsv"]:
            dataset_obj.dataset_properties[
                "object_construction_def"] = object_construction_def
            dataset_obj.dataset_properties["header"] = False

        if not self.cbas_util.create_dataset_on_external_resource(
            self.cluster, dataset_obj.name, link_name=dataset_obj.link_name,
            dataverse_name=dataset_obj.dataverse_name,
            **dataset_obj.dataset_properties):
            self.fail("Dataset creation failed")

        if not self.cbas_util.validate_cbas_dataset_items_count(
            self.cluster, dataset_obj.full_name, doc_counts[file_format],
            timeout=7200, analytics_timeout=7200):
            self.fail("Expected data does not match actual data")

    def test_dataset_creation_flags_for_parquet_files(self):
        """
        Prerequisite -
        parquetTypes.parquet will be uploaded to AWS S3 bucket, and this
        file's data will be used for verifying query results.
        """
        self.setup_for_test(
            create_links=True, create_aws_buckets=True,
            create_dataset_objs=True, same_dv_for_link_and_dataset=True,
            initialize_helper_objs=True)

        dataset = self.cbas_util.list_all_dataset_objs()[0]
        dataset.dataset_properties["file_format"] = "parquet"

        folder_path = os.path.join(os.path.dirname(__file__),
                                   "test_requirements")
        filename = "parquetTypes.parquet"

        if not \
                self.s3_data_helper.upload_parquet_file_with_specialized_data_types(
                    dataset.dataset_properties["aws_bucket_name"],
                    os.path.join(folder_path, filename), filename
                ):
            self.fail("Error while uploading parquetTypes.parquet to AWS S3 "
                      "bucket")

        with open(os.path.join(folder_path, "parquet_query_results.json")) \
                as fp:
            query_results = json.load(fp)

        testcases = [
            {
                # This test will create dataset with default values for all
                # the flags.
                "description": "Create dataset without explicitly passing any flags",
                "validate_query_error_msg": True,
                "expected_error": "Parquet type 'required int32 decimal32_field"
                                  " (DECIMAL(5,4))' is not supported by "
                                  "default. To enable type conversion, recreate"
                                  " the external collection with the option "
                                  "'decimal-to-double' enabled",
                "parse_json_string": 0,
                "convert_decimal_to_double": 0,
                "timezone": ""
            },
            {
                "description": "Create dataset by setting parse_json_string "
                               "to True",
                "validate_query_error_msg": True,
                "expected_error": "Parquet type 'required int32 decimal32_field"
                                  " (DECIMAL(5,4))' is not supported by "
                                  "default. To enable type conversion, recreate"
                                  " the external collection with the option "
                                  "'decimal-to-double' enabled",
                "parse_json_string": 1,
                "convert_decimal_to_double": 0,
                "timezone": ""
            },
            {
                "description": "Create dataset by setting parse_json_string "
                               "to False",
                "validate_query_error_msg": True,
                "expected_error": "Parquet type 'required int32 decimal32_field"
                                  " (DECIMAL(5,4))' is not supported by "
                                  "default. To enable type conversion, recreate"
                                  " the external collection with the option "
                                  "'decimal-to-double' enabled",
                "parse_json_string": 2,
                "convert_decimal_to_double": 0,
                "timezone": ""
            },
            {
                "description": "Create dataset by setting convert_decimal_to_double "
                               "to True",
                "result": 1,
                "parse_json_string": 0,
                "convert_decimal_to_double": 1,
                "timezone": ""
            },
            {
                "description": "Create dataset by setting parse_json_string "
                               "to True and convert_decimal_to_double to True",
                "result": 1,
                "parse_json_string": 1,
                "convert_decimal_to_double": 1,
                "timezone": ""
            },
            {
                "description": "Create dataset by setting parse_json_string "
                               "to False and convert_decimal_to_double to "
                               "True",
                "result": 2,
                "parse_json_string": 2,
                "convert_decimal_to_double": 1,
                "timezone": ""
            },
            {
                "description": "Create dataset by setting convert_decimal_to_double "
                               "to False",
                "parse_json_string": 0,
                "convert_decimal_to_double": 2,
                "timezone": "",
                "validate_query_error_msg": True,
                "expected_error": "Parquet type 'required int32 decimal32_field"
                                  " (DECIMAL(5,4))' is not supported by "
                                  "default. To enable type conversion, recreate"
                                  " the external collection with the option "
                                  "'decimal-to-double' enabled"
            },
            {
                "description": "Create dataset by setting parse_json_string "
                               "to True and convert_decimal_to_double to "
                               "False",
                "parse_json_string": 1,
                "convert_decimal_to_double": 2,
                "timezone": "",
                "validate_query_error_msg": True,
                "expected_error": "Parquet type 'required int32 decimal32_field"
                                  " (DECIMAL(5,4))' is not supported by "
                                  "default. To enable type conversion, recreate"
                                  " the external collection with the option "
                                  "'decimal-to-double' enabled",
            },
            {
                "description": "Create dataset by setting parse_json_string "
                               "to False and convert_decimal_to_double to "
                               "False",
                "parse_json_string": 2,
                "convert_decimal_to_double": 2,
                "timezone": "",
                "validate_query_error_msg": True,
                "expected_error": "Parquet type 'required int32 decimal32_field"
                                  " (DECIMAL(5,4))' is not supported by "
                                  "default. To enable type conversion, recreate"
                                  " the external collection with the option "
                                  "'decimal-to-double' enabled",
            },
            {
                "description": "Create dataset by setting timezone to IST",
                "parse_json_string": 0,
                "convert_decimal_to_double": 0,
                "timezone": "IST",
                "validate_query_error_msg": True,
                "expected_error": "Parquet type 'required int32 decimal32_field"
                                  " (DECIMAL(5,4))' is not supported by "
                                  "default. To enable type conversion, recreate"
                                  " the external collection with the option "
                                  "'decimal-to-double' enabled",
            },
            {
                "description": "Create dataset by setting parse_json_string "
                               "to True and timezone to IST",
                "parse_json_string": 1,
                "convert_decimal_to_double": 0,
                "timezone": "IST",
                "validate_query_error_msg": True,
                "expected_error": "Parquet type 'required int32 decimal32_field"
                                  " (DECIMAL(5,4))' is not supported by "
                                  "default. To enable type conversion, recreate"
                                  " the external collection with the option "
                                  "'decimal-to-double' enabled",
            },
            {
                "description": "Create dataset by setting parse_json_string "
                               "to False and timezone to IST",
                "parse_json_string": 2,
                "convert_decimal_to_double": 0,
                "timezone": "IST",
                "validate_query_error_msg": True,
                "expected_error": "Parquet type 'required int32 decimal32_field"
                                  " (DECIMAL(5,4))' is not supported by "
                                  "default. To enable type conversion, recreate"
                                  " the external collection with the option "
                                  "'decimal-to-double' enabled"
            },
            {
                "description": "Create dataset by setting convert_decimal_to_double "
                               "to True and timezone to IST",
                "parse_json_string": 0,
                "convert_decimal_to_double": 1,
                "timezone": "IST",
                "result": 3
            },
            {
                "description": "Create dataset by setting parse_json_string "
                               "and convert_decimal_to_double to True and timezone to IST",
                "parse_json_string": 1,
                "convert_decimal_to_double": 1,
                "timezone": "IST",
                "result": 3
            },
            {
                "description": "Create dataset by setting parse_json_string "
                               "to False, convert_decimal_to_double to True "
                               "and timezone to IST",
                "parse_json_string": 2,
                "convert_decimal_to_double": 1,
                "timezone": "IST",
                "result": 4
            },
            {
                "description": "Create dataset by setting "
                               "convert_decimal_to_double to False and timezone to IST",
                "parse_json_string": 0,
                "convert_decimal_to_double": 2,
                "timezone": "IST",
                "validate_query_error_msg": True,
                "expected_error": "Parquet type 'required int32 decimal32_field"
                                  " (DECIMAL(5,4))' is not supported by "
                                  "default. To enable type conversion, recreate"
                                  " the external collection with the option "
                                  "'decimal-to-double' enabled"
            },
            {
                "description": "Create dataset by setting parse_json_string "
                               "to True and convert_decimal_to_double to "
                               "False and timezone to IST",
                "parse_json_string": 1,
                "convert_decimal_to_double": 2,
                "timezone": "IST",
                "validate_query_error_msg": True,
                "expected_error": "Parquet type 'required int32 decimal32_field"
                                  " (DECIMAL(5,4))' is not supported by "
                                  "default. To enable type conversion, recreate"
                                  " the external collection with the option "
                                  "'decimal-to-double' enabled"
            },
            {
                "description": "Create dataset by setting parse_json_string "
                               "to False and convert_decimal_to_double to "
                               "False and timezone to IST",
                "parse_json_string": 2,
                "convert_decimal_to_double": 2,
                "timezone": "IST",
                "validate_query_error_msg": True,
                "expected_error": "Parquet type 'required int32 decimal32_field"
                                  " (DECIMAL(5,4))' is not supported by "
                                  "default. To enable type conversion, recreate"
                                  " the external collection with the option "
                                  "'decimal-to-double' enabled"
            }
        ]

        failed_testcases = list()

        def compare_json(a, b):
            """ checks if dictionary a is fully contained in b """
            if not isinstance(a, dict):
                return a == b
            else:
                return all(compare_json(v, b.get(k)) for k, v in a.items())

        for testcase in testcases:
            try:
                self.log.info(testcase["description"])

                temp_dataset = copy.deepcopy(dataset)

                for param in testcase:
                    if hasattr(temp_dataset, param):
                        setattr(temp_dataset, param, testcase[param])
                    elif param in temp_dataset.dataset_properties:
                        temp_dataset.dataset_properties[param] = testcase[
                            param]

                if not self.cbas_util.create_dataset_on_external_resource(
                        self.cluster, temp_dataset.name,
                        link_name= temp_dataset.link_name,
                        dataverse_name=temp_dataset.dataverse_name,
                        create_dv=testcase.get("create_dv", True),
                        **temp_dataset.dataset_properties):
                    raise Exception("Dataset creation failed")

                status, metrics, errors, cbas_result, handle = self.cbas_util.execute_statement_on_cbas_util(
                    self.cluster,
                    "select * from {0}".format(temp_dataset.full_name),
                    timeout=120, analytics_timeout=120)
                if status == "success":
                    if not compare_json(
                            cbas_result[0][CBASHelper.unformat_name(
                                temp_dataset.name)],
                            query_results[str(testcase["result"])]):
                        raise Exception("Actual query result did not match "
                                        "the expected output")
                elif status == "fatal":
                    if testcase.get("validate_query_error_msg", False):
                        if testcase["expected_error"] != str(errors[0]["msg"]):
                            raise Exception("Expected Error msg : {0} Actual Error msg: {1}".format(
                                testcase["expected_error"], str(errors[0]["msg"])))
                    else:
                        raise Exception("Test failed due to {0}".format(str(errors[0]["msg"])))
            except Exception as err:
                self.log.error(str(err))
                failed_testcases.append(testcase["description"])
            finally:
                self.cbas_util.drop_dataset(
                    self.cluster, temp_dataset.full_name)

        if failed_testcases:
            self.fail("Following testcases failed - {0}".format(
                str(failed_testcases)))
