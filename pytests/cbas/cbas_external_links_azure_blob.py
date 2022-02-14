import random
import random, json, copy, os
from azureLib.azure_data_helper import  AzureDataHelper
from TestInput import TestInputSingleton
from cbas.cbas_base import CBASBaseTest
from rbac_utils.Rbac_ready_functions import RbacUtils
from com.azure.storage.blob import BlobServiceClientBuilder,BlobClient,BlobContainerClient
from cbas_utils.cbas_utils import CBASRebalanceUtil
from membase.api.rest_client import RestConnection, RestHelper
from remote.remote_util import RemoteMachineShellConnection, RemoteUtilHelper
from couchbase_helper.tuq_helper import N1QLHelper
from CbasLib.CBASOperations import CBASHelper
from BucketLib.bucket import Bucket
from threading import Thread

rbac_users_created = {}

class CBASExternalAzureLinks(CBASBaseTest):


    def setUp(self):

        self.input = TestInputSingleton.input
        if "services_init" not in self.input.test_params:
            self.input.test_params.update(
                {"services_init": "kv-n1ql-index:cbas"})
        if "nodes_init" not in self.input.test_params:
            self.input.test_params.update(
                {"nodes_init": "2"})

        if self.input.param('setup_infra', True):
            self.input.test_params.update(
                {"cluster_kv_infra": "default"})

        # Randomly choose name cardinality of links and datasets.
        if "link_cardinality" not in self.input.test_params:
            self.input.test_params.update(
                {"link_cardinality": random.randrange(1,4)})
        if "dataset_cardinality" not in self.input.test_params:
            self.input.test_params.update(
                {"dataset_cardinality": random.randrange(1,4)})

        super(CBASExternalAzureLinks, self).setUp()
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.setUp.__name__)

        self.accountName, self.accountKey, self.connection_string = self.get_azure_credentials()

        self.endpoint = "https://{0}.blob.core.windows.net".format(self.accountName)

        self.azure_containers = {}
        self.azure_containers_list = []
        # Since all the test cases are being run on 1 cluster only
        self.cluster = self.cb_clusters.values()[0]
        self.rbac_util = RbacUtils(self.cluster.master)
        self.rest = RestConnection(self.cluster.master)

        # Create analytics admin user on Analytics cluster
        self.analytics_username = "admin_analytics"
        self.rbac_util._create_user_and_grant_role(
            self.analytics_username, "analytics_admin")
        rbac_users_created[self.analytics_username] = "analytics_admin"
        self.invalid_value = "invalid"

    def tearDown(self):
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)

        #Delete azure container
        if self.azure_containers_list:
            self.azure_data_helper.delete_azure_blob_container()

        self.create_or_delete_users(self.rbac_util, rbac_users_created, delete=True)

        super(CBASExternalAzureLinks, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.tearDown.__name__)

    def get_azure_credentials(self):
        with open("/etc/azure_creds", "r") as fh:
            cred = json.loads(fh.read())
            return cred["accountName"], cred["accountKey"], cred["connection_string"]

    def setup_for_test(self, create_links=True, create_azure_containers=True,
                       create_dataset_objs=True, same_dv_for_link_and_dataset=True,
                       create_datasets=True, initialize_helper_objs=True,rebalance_util=False,link_perm=False):

        self.cbas_util.create_link_obj(
            self.cluster,"azureblob",dataverse=None,
            accessKeyId=self.accountName, secretAccessKey=self.accountKey,
            regions=None,serviceEndpoint=self.endpoint,
            link_cardinality=self.input.param("link_cardinality", 1),
            no_of_objs=self.input.param("no_of_links", 1),link_perm=link_perm)

        if create_links:
            self.sleep(30, "Sleeping for 30 seconds to ensure that cbas service is up and running")
            link_objs = self.cbas_util.list_all_link_objs(link_type="azureblob")
            for link_obj in link_objs:
                if not self.cbas_util.create_link(
                    self.cluster, link_obj.properties,
                    username=self.analytics_username):
                    self.fail("link creation failed")
        if initialize_helper_objs:
            self.n1ql_helper = N1QLHelper(
                shell=RemoteMachineShellConnection(self.cluster.master),
                buckets=self.cluster.buckets, item_flag=0,
                n1ql_port=8093, log=self.log, input=self.input,
                server=self.cluster.master, use_rest=True)

            self.azure_data_helper = AzureDataHelper(
                connection_string=self.connection_string,cluster=self.cluster,
                bucket_util=self.bucket_util, task=self.task, log=self.log,
                n1ql_helper=self.n1ql_helper)
        if create_azure_containers:
            link_regions = [link.properties["type"] for link in link_objs]
            for _ in range(self.input.param("no_of_azure_container", 1)):
                retry = 0
                container_created = False
                azure_container_name = self.input.param(
                    "azure_container_name", "cbasregression{0}".format(
                        random.randint(1, 10000)))

                while (not container_created) and retry < 2:
                        self.log.info("Creating Azure container - {0}".format(
                            azure_container_name))
                        region = random.choice(link_regions)
                        blob_container_client = self.azure_data_helper.azure_create_containers(azure_container_name)
                        if blob_container_client:
                            self.azure_containers[azure_container_name] = region
                            self.azure_containers_list.append(azure_container_name)
                            container_created = True

                        self.sleep(10, "Sleeping for 10 seconds to ensure that Azure container is created")
                        retry += 1
                if not container_created:
                    self.fail("Creating Azure container - {0}. Failed.".format(
                        azure_container_name))

        if create_dataset_objs:
            self.cbas_util.create_external_dataset_azure_obj(
                self.cluster, self.azure_containers,
                same_dv_for_link_and_dataset=same_dv_for_link_and_dataset,
                dataset_cardinality=self.input.param("dataset_cardinality", 1),
                object_construction_def=None,
                file_format="json", redact_warning=None, header=None,
                null_string=None, include=None, exclude=None,
                name_length=30, fixed_length=False,
                no_of_objs=self.input.param("no_of_datasets", 1))

        if create_datasets:
            for dataset_obj in self.cbas_util.list_all_dataset_objs():
                if not self.cbas_util.create_dataset_on_external_resource_azure(
                    self.cluster,dataset_obj.name,
                    link_name=dataset_obj.link_name,
                    dataverse_name=dataset_obj.dataverse_name,
                    **dataset_obj.dataset_properties):
                    self.fail("Dataset creation failed")
        if rebalance_util:
            self.rebalance_util = CBASRebalanceUtil(
                self.cluster_util, self.bucket_util, self.task, True, self.cbas_util)
            self.cluster.exclude_nodes = list()
            self.cluster.exclude_nodes.extend(
                [self.cluster.master, self.cluster.cbas_cc_node])

    def test_query_data_ananymous(self):
        self.setup_for_test(
            create_links=True, create_azure_containers=True,
            create_dataset_objs=True, create_datasets=False, same_dv_for_link_and_dataset=True,
            initialize_helper_objs=True,link_perm=True)
        #self.azure_data_helper.set_azure_container_permission(container_public_access=False)
        dataset_obj = self.cbas_util.list_all_dataset_objs()[0]


        self.azure_data_helper.generate_data_for_azure_and_upload(
            self.azure_containers_list[0], key=self.key, no_of_files=1, file_formats=["json"], no_of_folders=0,
            max_folder_depth=0, header=False, null_key="", operation="create",
            bucket=self.cluster.buckets[0], no_of_docs=10, batch_size=10, exp=0, durability="",
            mutation_num=0, randomize_header=False, large_file=False,
            missing_field=[False])

        # run query on dataset.
        n1ql_query = self.get_n1ql_query(self.input.param("n1ql_query", "count_all"))
        cbas_query = self.input.param("cbas_query", "Select count(*) from {0};")


        # create dataset
        result = self.cbas_util.create_dataset_on_external_resource_azure(
                self.cluster, dataset_obj.name, link_name=dataset_obj.link_name,
                dataverse_name=dataset_obj.dataverse_name,validate_error_msg=True,
                expected_error="External source error", expected_error_code=24086,
                **dataset_obj.dataset_properties)
        if result :

            self.azure_data_helper.set_azure_container_permission(container_public_access=True)
            self.sleep(60, "Sleeping for 60 seconds to ensure that Azure container permission is changed")

            if not self.cbas_util.create_dataset_on_external_resource_azure(
                    self.cluster, dataset_obj.name,
                    link_name=dataset_obj.link_name,
                    dataverse_name=dataset_obj.dataverse_name,
                    **dataset_obj.dataset_properties):
                self.fail("Dataset creation failed")
            n1ql_result = self.cluster.rest.query_tool(n1ql_query.format(
                self.cluster.buckets[0].name))["results"][0]["$1"]
            status, metrics, errors, cbas_result, handle = self.cbas_util.execute_statement_on_cbas_util(
                self.cluster, cbas_query.format(dataset_obj.full_name),
                timeout=120, analytics_timeout=120)
            if not n1ql_result == cbas_result[0]["$1"]:
                self.fail("Data mis-match")
        else:
            self.fail("Dataset creation failed")

    def test_create_external_link(self):
        self.setup_for_test(
            create_links=True, create_azure_containers=True,
                create_dataset_objs=True, same_dv_for_link_and_dataset=True,
                initialize_helper_objs=True,rebalance_util=False)

        if self.input.param("link_cardinality", 1) - 1 > 1:
            invalid_dv = "invalid.invalid"
        else:
            invalid_dv = "invalid"

        self.create_or_delete_users(self.rbac_util, rbac_users_created)

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
                    "description": "Create a link with an invalid credentials",
                    "username": self.invalid_value,
                    "validate_error_msg": True
                },
                {
                    "description": "Create a link with a name that already exists",
                    "recreate_link": True,
                    "validate_error_msg": True,
                    "expected_error": "Link {0} already exists".format(
                        CBASHelper.unformat_name(link.full_name))
                },
                {
                    "description": "Create a link with a name of form Local*",
                    "name": "Local123",
                    "expected_error": 'Invalid link name Local123. Links starting with \'Local\' are reserved by the system'
                }
            ]


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
                if testcase["description"]== "Create a link with an invalid credentials":
                    if not self.cbas_util.create_link(
                            self.cluster, link_properties,
                            username=testcase.get("username", "invalid"),
                            validate_error_msg=testcase.get("validate_error_msg", True),
                            expected_error=testcase.get("expected_error", "Unauthorized user"),
                            create_dataverse=testcase.get("create_dataverse", True)):
                        raise Exception("Error while creating link")
                else:
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
            create_links=False, create_azure_containers=False,
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
            create_links=True, create_azure_containers=False,
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
                "name": link.properties["name"],
                "expected_hits": 0,
                "description": "Parameters Passed - Name",
                "validate_error_msg": True,
                "expected_error": "Cannot find analytics scope with name {0}".format(
                    link.properties["name"])
            },
            {
                "dataverse": link.properties["dataverse"],
                "name": link.properties["name"],
                "type": "azureblob",
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
            testcase["type"] = "azureblob"
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
                    if not (response[0]["accountName"] == link.properties["accountName"]):
                        raise Exception("Account Name  does not match. Expected - {0}\nActual - {1}".format(
                            link.properties["accountName"], response[0]["accountName"]))
                    if not (response[0]["endpoint"] == link.properties["endpoint"]):
                        raise Exception("Endpoint does not match. Expected - {0}\nActual - {1}".format(
                            link.properties["endpoint"], response[0]["endpoint"]))
            except Exception as err:
                self.log.error(str(err))
                failed_testcases.append(testcase["description"])
        if failed_testcases:
            self.fail("Following testcases failed - {0}".format(str(failed_testcases)))

    def test_create_dataset(self):
        self.setup_for_test(
            create_links=True, create_azure_containers=True,
            create_dataset_objs=True, same_dv_for_link_and_dataset=True,
            initialize_helper_objs=True)
        link = self.cbas_util.list_all_link_objs()[0]
        dataset = self.cbas_util.list_all_dataset_objs()[0]

        testcases = [
            {
                "description": "Create dataset without type definition for CSV format",
                "file_format": "csv",
                "header": False,
                "dataverse_name": "dataversename1",
                "new_dataverse": True,
                "validate_error_msg": True,
                "expected_error": "Inline type definition is required for external analytics collections with 'csv' format"
            },
            {
                "description": "Create dataset without type definition for TSV format",
                "file_format": "tsv",
                "header": False,
                "dataverse_name": "dataversename2",
                "new_dataverse": True,
                "validate_error_msg": True,
                "expected_error": "Inline type definition is required for external analytics collections with 'tsv' format"
            },
            {
                "description": "Create dataset with type definition for JSON format",
                "object_construction_def": "id INT",
                "validate_error_msg": True,
                "dataverse_name": "dataversename3",
                "new_dataverse": True,
                "expected_error": "Inline type definition is not allowed for external analytics collections with 'json' format"
            },
            {
                "description": "Create dataset with upsupported data type in type definition",
                "object_construction_def": "id FLOAT",
                "header": False,
                "dataverse_name": "dataversename4",
                "new_dataverse": True,
                "file_format": "csv",
                "validate_error_msg": True,
                "expected_error": "Type float could not be found"
            },
            {
                "description": "Create dataset with invalid type in type definition",
                "object_construction_def": "id {0}".format(self.invalid_value),
                "file_format": "csv",
                "header": False,
                "dataverse_name": "dataversename5",
                "new_dataverse": True,
                "validate_error_msg": True,
                "expected_error": "Cannot find datatype with name {0}".format(self.invalid_value)
            },
            {
                "description": "Create dataset on a non existent Azure container",
                "azure_container_name": self.invalid_value,
                "validate_error_msg": True,
                "dataverse_name":"dataversename0",
                "new_dataverse":True,
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
                "create_dv": False,
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
                "expected_error": "Invalid value for parameter \'format\': txt (in line 1, at column 1)"
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
                "expected_error": "Invalid parameter \'header\' (in line 1, at column 1)"
            },
            {
                "description": "Create dataset with invalid header flag for tsv file format",
                "file_format": "tsv",
                "object_construction_def": "name STRING",
                "header": "False1",
                "validate_error_msg": True,
                "expected_error": "Invalid value for parameter \'header\': false1"
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
                "expected_error": "Invalid parameter \'null\' (in line 1, at column 1)"
            },
            {
                "description": "Create dataset with invalid redact warning flag",
                "file_format": "json",
                "redact_warning": "False1",
                "validate_error_msg": True,
                "expected_error": "Invalid value for parameter \'redact-warnings\': false1"
            },
            {
                "description": "Create dataset with both include and exclude flag set",
                "file_format": "json",
                "include": "*.json",
                "exclude": "*.csv",
                "validate_error_msg": True,
                "expected_error": "The parameters \'include\' and \'exclude\' cannot be provided at the same time"
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

                if not self.cbas_util.create_dataset_on_external_resource_azure(
                        self.cluster, temp_dataset.name,
                        link_name=testcase.get("link_name", temp_dataset.link_name),
                        dataverse_name=temp_dataset.dataverse_name,
                        validate_error_msg=testcase.get("validate_error_msg", False),
                        username=testcase.get("username", self.analytics_username),
                        expected_error=testcase.get("expected_error", ""),
                        create_dv=testcase.get("create_dv", True),
                        **temp_dataset.dataset_properties):
                    raise Exception("Dataset creation failed")

                if testcase.get("recreate_dataset", False):
                    testcase["validate_error_msg"] = True
                    if not self.cbas_util.create_dataset_on_external_resource_azure(
                            self.cluster, temp_dataset.name,
                            link_name=temp_dataset.link_name,
                            dataverse_name=temp_dataset.dataverse_name,
                            validate_error_msg=testcase.get("validate_error_msg", False),
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

                if not testcase.get("validate_error_msg", False):
                    self.cbas_util.drop_dataset(self.cluster,
                                                temp_dataset.full_name)
            except Exception as err:
                self.log.error(str(err))
                failed_testcases.append(testcase["description"])
        if failed_testcases:
            self.fail("Following testcases failed - {0}".format(str(failed_testcases)))
    def get_include_exclude_flag(self, include=True, key_name=0):
        include_dict = {
            1: ["file_[1234567890].json", "*.json"],
            2: ["file_[1234567890].csv", "*.csv"],
            3: ["file_[1234567890].tsv", "*.tsv"],
            4: 'file_?.[!ct]s[!v]*',
            5: 'file_?.[!t]sv',
            6: 'file_?.[!c]sv'
        }
        exclude_dict = {
            1: ["*.csv", "*.tsv"],
            2: ["*.json", "*.tsv"],
            3: ["*.json", "*.csv"],
            4: 'file_?*.[ct]s[!o]',
            5: 'file_?*.[t]s[!v]',
            6: 'file_?*.[c]s[!v]'
        }
        if include:
            return include_dict.get(key_name, None)
        else:
            return exclude_dict.get(key_name, None)

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
    def test_query_dataset(self):
        """
        dataset_params
        mix_data_file bool
        no_of_files int
        file_format_for_upload ["json","csv","tsv]
        no_of_folders int
        max_folder_depth int
        header_azure_file bool
        null_azure_file str
        no_of_docs int
        randomize_header bool
        large_file bool
        missing_field bool
        select_azure_path bool
        invalid_azure_path
        """
        self.setup_for_test(
            create_links=True, create_azure_containers=True,
            create_dataset_objs=True,create_datasets=False, same_dv_for_link_and_dataset=True,
            initialize_helper_objs=True)

        file_format = self.input.param("file_format", "json")

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


        # generate and upload data to Azure
        if self.input.param("mix_data_file", False):
            if not self.azure_data_helper.generate_mix_data_file(
                dataset_obj.dataset_properties["azure_container_name"],
                file_format=dataset_obj.dataset_properties["file_format"],
                upload_to_azure=True):
                self.fail("Error while uploading files to Azure")
        elif self.input.param("file_extension", None):
            if not self.azure_data_helper.generate_file_with_record_of_size_and_upload(
                dataset_obj.dataset_properties["azure_container_name"], "sample",
                record_size=1024, file_format=dataset_obj.dataset_properties["file_format"],
                upload_to_azure=True, file_extension=self.input.param("file_extension")):
                self.fail("Error while uploading files to Azure")
        else:
            missing_field = [self.input.param("missing_field", False)]

            fileformat = self.input.param("file_format_for_upload", 1)
            if fileformat == 0:
                fileformats = ["json"]
            elif fileformat == 1:
                fileformats = ["csv"]
            elif fileformat == 2:
                fileformats = ["tsv"]
            else:
                fileformats = ["json","csv","tsv"]
            result = self.azure_data_helper.generate_data_for_azure_and_upload(
                azure_container_name=dataset_obj.dataset_properties["azure_container_name"],
                key=self.key,
                no_of_files=self.input.param("no_of_files", 5),
                file_formats=fileformats,
                #file_formats=json.loads(self.input.param(
                #    "file_format_for_upload", "[\"json\"]")),
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
                self.fail("Error while uploading files to Azure")

        # run query on dataset and on bucket that was used to generate azure blob data.
        n1ql_query = self.get_n1ql_query(self.input.param("n1ql_query", "count_all"))
        cbas_query = self.input.param("cbas_query", "Select count(*) from {0};")

        dataset_obj.dataset_properties["file_format"] = file_format
        file_extension = self.input.param("file_extension", None)
        exclude = self.input.param("exclude", None)
        if not exclude:
            if file_extension:
                dataset_obj.dataset_properties["include"] = "*.{0}".format(file_extension)
            else:
                dataset_obj.dataset_properties["include"] = "*.{0}".format(file_format)
        if file_format in ["csv", "tsv"]:
            dataset_obj.dataset_properties[
                "object_construction_def"] = "key1 string, key2 string, key3 string, key4 string, key5 string"
            dataset_obj.dataset_properties["header"] = False

        # create dataset
        if not self.cbas_util.create_dataset_on_external_resource_azure(
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
            if not errors[0]["msg"] == "Malformed input stream":
                self.fail("Expected error message does not match actual data")
        else:
            if self.input.param("expected_count", None) is not None:
                n1ql_result = int(self.input.param("expected_count"))
            if not n1ql_result == cbas_result[0]["$1"]:
                self.fail(
                    "Expected data does not match actual data. Expected count - {0} Actual count - {1}".format(
                        n1ql_result, cbas_result[0]["$1"]))


    def test_query_dataset_new(self):
        """
        dataset_params
        mix_data_file bool
        no_of_files int
        file_format_for_upload ["json","csv","tsv]
        no_of_folders int
        max_folder_depth int
        header_azure_file bool
        null_azure_file str
        no_of_docs int
        randomize_header bool
        large_file bool
        missing_field bool
        select_aws_path bool
        invalid_aws_path
        """
        self.setup_for_test(
            create_links=True, create_azure_containers=True,
            create_dataset_objs=True,create_datasets=False, same_dv_for_link_and_dataset=True,
            initialize_helper_objs=True)
        dataset_obj = self.cbas_util.list_all_dataset_objs()[0]

        buckets = self.create_buckets(num_buckets=1)

        self.azure_data_helper.generate_data_for_azure_and_upload(
            self.azure_containers_list[0], key=self.key, no_of_files=1, file_formats=["json"], no_of_folders=0,
            max_folder_depth=0, header=False, null_key="", operation="create",
            bucket=self.cluster.buckets[0], no_of_docs=10, batch_size=10, exp=0, durability="",
            mutation_num=0, randomize_header=False, large_file=False,
            missing_field=[False])

        # run query on dataset.
        n1ql_query = self.get_n1ql_query(self.input.param("n1ql_query", "count_all"))
        cbas_query = self.input.param("cbas_query", "Select count(*) from {0};")

        # create dataset
        if not self.cbas_util.create_dataset_on_external_resource_azure(
            self.cluster, dataset_obj.name, link_name=dataset_obj.link_name,
            dataverse_name=dataset_obj.dataverse_name,
            **dataset_obj.dataset_properties):
            self.fail("Dataset creation failed")


        n1ql_result = self.cluster.rest.query_tool(n1ql_query.format(
            self.cluster.buckets[0].name))["results"][0]["$1"]
        status, metrics, errors, cbas_result, handle = self.cbas_util.execute_statement_on_cbas_util(
            self.cluster, cbas_query.format(dataset_obj.full_name),
            timeout=120, analytics_timeout=120)
        if not n1ql_result == cbas_result[0]["$1"]:
            self.fail("Data Mismatch")


    def execute_cbas_query(self, cbas_query, result, timeout=120, analytics_timeout=120):
        status, metrics, errors, cbas_result, handle = self.cbas_util.execute_statement_on_cbas_util(
            self.cluster, cbas_query, timeout=timeout,
            analytics_timeout=analytics_timeout)
        result.extend([status, cbas_result, errors])
    def create_buckets(self, num_buckets):

        self.info = self.rest.get_nodes_self()
        self.num_buckets = num_buckets
        # threshold_memory_vagrant = 100
        kv_memory = self.info.memoryQuota - 100

        # Creating buckets for data loading purpose
        self.bucket_expiry = self.input.param("bucket_expiry", 0)
        ramQuota = self.input.param("ramQuota", kv_memory)
        buckets = self.input.param("bucket_names",
                                   "default").split(';')
        self.bucket_type = self.bucket_type.split(';')
        self.compression_mode = self.compression_mode.split(';')
        self.bucket_eviction_policy = self.bucket_eviction_policy
        for i in range(self.num_buckets):
            bucket = Bucket(
                {Bucket.name: buckets[i],
                 Bucket.ramQuotaMB: ramQuota / self.num_buckets,
                 Bucket.maxTTL: self.bucket_expiry,
                 Bucket.replicaNumber: self.num_replicas,
                 Bucket.storageBackend: self.bucket_storage,
                 Bucket.evictionPolicy: self.bucket_eviction_policy,
                 Bucket.bucketType: self.bucket_type[i],
                 Bucket.flushEnabled: Bucket.FlushBucket.ENABLED,
                 Bucket.compressionMode: self.compression_mode[i]})
            self.bucket_util.create_bucket(self.cluster,bucket)
        return bucket

    def test_querying_while_network_failure(self):
        self.setup_for_test(
            create_links=True, create_azure_containers=True,
            create_dataset_objs=True, same_dv_for_link_and_dataset=False,
            create_datasets=True, initialize_helper_objs=True)
        dataset_obj = self.cbas_util.list_all_dataset_objs()[0]
        #buckets=self.create_buckets(num_buckets=1)
        self.azure_data_helper.generate_data_for_azure_and_upload(
            self.azure_containers_list[0],key=self.key, no_of_files=1, file_formats=["json"], no_of_folders=0,
            max_folder_depth=0,header=False, null_key="", operation="create",
            bucket=self.cluster.buckets[0],no_of_docs=10,batch_size=10, exp=0, durability="",
            mutation_num=0, randomize_header=False, large_file=False,
            missing_field=[False])


        shell = RemoteMachineShellConnection(self.cluster.cbas_cc_node)

        cbas_query = "Select count(*) from {0};".format(dataset_obj.full_name)
        result = list()
        threads = list()
        threads.append(Thread(
            target=self.execute_cbas_query,
            name="azure_query_thread",
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

    def test_drop_dataset(self):

        self.setup_for_test(
            create_links=True, create_azure_containers=True,
            create_dataset_objs=True, same_dv_for_link_and_dataset=False,
            create_datasets=True, initialize_helper_objs=True)
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
                    if not self.cbas_util.create_dataset_on_external_resource_azure(
                        self.cluster, dataset_obj.name,
                        link_name=dataset_obj.link_name,
                        dataverse_name=dataset_obj.dataverse_name,
                        **dataset_obj.dataset_properties):
                        self.fail("Dataset creation failed")
                    recreate_dataset = False

                if not self.cbas_util.drop_dataset(
                    self.cluster, dataset_obj.full_name,
                    validate_error_msg=testcase.get("validate_error_msg",False),
                    username=testcase.get("username", self.analytics_username),
                    expected_error=testcase.get("expected_error","")):
                    raise Exception("Error while dropping dataset")

                if not testcase.get("validate_error_msg",False):
                    recreate_dataset = True

            except Exception:
                failed_testcases.append(testcase["description"])

        if failed_testcases:
            self.fail("Following testcases failed - {0}".format(str(failed_testcases)))
    def test_alter_link_properties(self):
        self.setup_for_test(
            create_links=True, create_azure_containers=True,
            create_dataset_objs=True, same_dv_for_link_and_dataset=True,
            create_datasets=True, initialize_helper_objs=True)

        if self.input.param("link_cardinality", 1) - 1 > 1:
            invalid_dv = "invalid.invalid"
        else:
            invalid_dv = "invalid"

        self.create_or_delete_users(self.rbac_util, rbac_users_created)

        link = self.cbas_util.list_all_link_objs()[0]
        dataset = self.cbas_util.list_all_dataset_objs()[0]


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
            #{
            #    "description": "Changing accessKeyId and secretAccessKey to another set of working credentials",
            #    "accessKeyId": aws_access_key_1,
            #    "secretAccessKey": aws_secret_key_1,
            #},
            #{
            #    "description": "Changing accountName and accountKey to another set of non-working credentials",
            #    "accountKey": self.invalid_value,
            #    "accountName":self.invalid_value
            #}
        ]
        rbac_testcases = self.create_testcase_for_rbac_user(
            "Altering external link properties using {0} user",
            rbac_users_created, users_with_permission=[self.analytics_username])

        for testcase in rbac_testcases:
            if testcase["validate_error_msg"]:
                testcase["expected_error"] = "Access denied: user lacks necessary permission(s) to access resource"
        testcases = testcases + rbac_testcases
        failed_testcases = list()

        result = self.azure_data_helper.generate_data_for_azure_and_upload(
            azure_container_name=dataset.dataset_properties["azure_container_name"],
            key=self.key, no_of_files=1, file_formats=["json"], no_of_folders=0,
            max_folder_depth=0, header=False, null_key="", operation="create",
            bucket=self.cluster.buckets[0], no_of_docs=10,
            randomize_header=False, large_file=False, missing_field=[False])
        if result:
            self.fail("Error while uploading files to Azure")

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
                                "Following error is raised while reading data from azure after altering the link\nERROR : {0}"
                                    .format(str(errors)))

                    elif cbas_result[0]["$1"] != cbas_result_2[0]["$1"]:
                        raise Exception("Data read from AWS before and after altering link do not match")

                    response2 = self.cbas_util.get_link_info(
                        self.cluster, link_name=link_properties["name"],
                        dataverse=link_properties["dataverse"])[0]
                    if not (response2["accountName"] == link_properties["accountName"]):
                        raise Exception("Access key ID does not match. Expected - {0}\nActual - {1}".format(
                            link_properties["accountName"], response2["accountName"]))
                    if not (response2["accountKey"] == "<redacted sensitive entry>"):
                        raise Exception("secretAccessKey does not match. Expected - {0}\nActual - {1}".format(
                            "<redacted sensitive entry>", response2["accountKey"]))
                    if not (response2["endpoint"] == link_properties["endpoint"]):
                        raise Exception("serviceEndpoint does not match. Expected - {0}\nActual - {1}".format(
                            link_properties["endpoint"], response2["endpoint"]))

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

    def test_analytics_cluster_when_rebalancing_in_cbas_node(self):

        self.setup_for_test(
            create_links=True, create_azure_containers=True,
            create_dataset_objs=True, same_dv_for_link_and_dataset=False,
            create_datasets=True, initialize_helper_objs=True, rebalance_util=True)
        dataset_obj = self.cbas_util.list_all_dataset_objs()[0]

        #buckets = self.create_buckets(num_buckets=1)

        result = self.azure_data_helper.generate_data_for_azure_and_upload(
            self.azure_containers_list[0], key=self.key, no_of_files=3, file_formats=["json"], no_of_folders=0,
            max_folder_depth=0, header=False, null_key="", operation="create",
            bucket=self.cluster.buckets[0], no_of_docs=10000, batch_size=10, exp=0, durability="",
            mutation_num=0, randomize_header=False, large_file=False,
            missing_field=[False])

        if result:
            self.fail("Error while uploading files to Azure")

        query = "Select count(*) from `{0}`;"
        n1ql_result = self.cluster.rest.query_tool(query.format(
            self.cluster.buckets[0].name))["results"][0]["$1"]

        rebalance_task, self.available_servers = self.rebalance_util.rebalance(
            self.cluster, kv_nodes_in=0, kv_nodes_out=0, cbas_nodes_in=1,
            cbas_nodes_out=0, available_servers=self.available_servers,
            exclude_nodes=self.cluster.exclude_nodes)

        _, cbas_query_task = self.rebalance_util.start_parallel_queries(
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
            create_links=True, create_azure_containers=True,
            create_dataset_objs=True, same_dv_for_link_and_dataset=False,
            create_datasets=True, initialize_helper_objs=True, rebalance_util=True)
        dataset_obj = self.cbas_util.list_all_dataset_objs()[0]

        #buckets = self.create_buckets(num_buckets=1)

        result = self.azure_data_helper.generate_data_for_azure_and_upload(
            self.azure_containers_list[0], key=self.key, no_of_files=3, file_formats=["json"], no_of_folders=0,
            max_folder_depth=0, header=False, null_key="", operation="create",
            bucket=self.cluster.buckets[0], no_of_docs=10000, batch_size=10, exp=0, durability="",
            mutation_num=0, randomize_header=False, large_file=False,
            missing_field=[False])

        if result:
            self.fail("Error while uploading files to Azure")

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
            create_links=True, create_azure_containers=True,
            create_dataset_objs=True, same_dv_for_link_and_dataset=False,
            create_datasets=True, initialize_helper_objs=True, rebalance_util=True)
        dataset_obj = self.cbas_util.list_all_dataset_objs()[0]

        #buckets = self.create_buckets(num_buckets=1)

        result = self.azure_data_helper.generate_data_for_azure_and_upload(
            self.azure_containers_list[0], key=self.key, no_of_files=3, file_formats=["json"], no_of_folders=0,
            max_folder_depth=0, header=False, null_key="", operation="create",
            bucket=self.cluster.buckets[0], no_of_docs=10000, batch_size=10, exp=0, durability="",
            mutation_num=0, randomize_header=False, large_file=False,
            missing_field=[False])

        if result:
            self.fail("Error while uploading files to Azure")

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



    def test_large_file(self):
        object_construction_def = "key1 STRING, key2 STRING, key3 STRING, key4 STRING"
        doc_counts = {
            "json": 100000,
            "csv": 100000,
            "tsv": 100000,
        }

        self.setup_for_test(
            create_links=True, create_azure_containers=True,
            create_dataset_objs=True, same_dv_for_link_and_dataset=True,
            create_datasets=False, initialize_helper_objs=True)

        file_format = self.input.param("file_format", "json")

        result = self.azure_data_helper.generate_data_for_azure_and_upload(
            self.azure_containers_list[0], key=self.key, no_of_files=10, file_formats=[file_format], no_of_folders=0,
            max_folder_depth=0, header=False, null_key="", operation="create",
            bucket=self.cluster.buckets[0], no_of_docs=doc_counts[file_format], batch_size=10, exp=0, durability="",
            mutation_num=0, randomize_header=False, large_file=False,
            missing_field=[False])

        if result:
            self.fail("Error while uploading files to Azure")

        dataset_obj = self.cbas_util.list_all_dataset_objs()[0]
        dataset_obj.dataset_properties["file_format"] = file_format
        dataset_obj.dataset_properties["include"] = "*.{0}".format(file_format)

        if file_format in ["csv", "tsv"]:
            dataset_obj.dataset_properties[
                "object_construction_def"] = object_construction_def
            dataset_obj.dataset_properties["header"] = False

        if not self.cbas_util.create_dataset_on_external_resource_azure(
            self.cluster, dataset_obj.name, link_name=dataset_obj.link_name,
            dataverse_name=dataset_obj.dataverse_name,
            **dataset_obj.dataset_properties):
            self.fail("Dataset creation failed")

        if not self.cbas_util.validate_cbas_dataset_items_count(
            self.cluster, dataset_obj.full_name, doc_counts[file_format],
            timeout=7200, analytics_timeout=7200):
            self.fail("Expected data does not match actual data")

    def test_when_a_single_record_size_is_greater_than_32MB(self):
        self.setup_for_test(
            create_links=True, create_azure_containers=True,
            create_dataset_objs=True, same_dv_for_link_and_dataset=False,
            create_datasets=False, initialize_helper_objs=True)
        dataset = self.cbas_util.list_all_dataset_objs()[0]

        file_format = self.input.param("file_format", "json")
        record_size = int(self.input.param("record_size", 32))
        filename = "big_record_file"

        dataset.dataset_properties["file_format"] = file_format

        if file_format in ["csv", "tsv"]:
            dataset.dataset_properties[
                "object_construction_def"] = "key1 string, key2 string, key3 string, key4 string, key5 string"
            dataset.dataset_properties["header"] = False

        if not self.cbas_util.create_dataset_on_external_resource_azure(
            self.cluster, dataset.name, link_name=dataset.link_name,
            dataverse_name=dataset.dataverse_name,
            **dataset.dataset_properties):
            self.fail("Dataset creation failed")


        if not self.azure_data_helper.generate_file_with_record_of_size_and_upload(
            dataset.dataset_properties["azure_container_name"], filename,
            record_size=record_size * 1024 * 1024, file_format=file_format,
            upload_to_azure=True):
            self.fail("Error while uploading files to azure container")

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

    def test_querying_with_more_than_1000_files_in_azure_container(self):
        self.setup_for_test(
            create_links=True, create_azure_containers=True,
            create_dataset_objs=True, same_dv_for_link_and_dataset=False,
            create_datasets=True, initialize_helper_objs=True)
        dataset_obj = self.cbas_util.list_all_dataset_objs()[0]

        result = self.azure_data_helper.generate_data_for_azure_and_upload(
            self.azure_containers_list[0],
            key=self.key, no_of_files=self.input.param("no_of_files", 1000),
            file_formats=["json"], no_of_folders=0, max_folder_depth=0,
            header=False, null_key="", operation="create",
            bucket=self.cluster.buckets[0], no_of_docs=100000,
            randomize_header=False, large_file=False, missing_field=[False])
        if result:
            self.fail("Error while uploading files to Azure")

        n1ql_query = "Select count(*) from {0};".format(
            self.cluster.buckets[0].name)
        n1ql_result = self.cluster.rest.query_tool(n1ql_query)["results"][0]["$1"]
        if not self.cbas_util.validate_cbas_dataset_items_count(
            self.cluster,dataset_obj.full_name, n1ql_result,
            timeout=3600, analytics_timeout=3600):
            self.fail("Expected data does not match actual data")
