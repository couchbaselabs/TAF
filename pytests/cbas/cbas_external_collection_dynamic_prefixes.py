import json
import random

from awsLib.s3_data_helper import perform_S3_operation, S3DataHelper
from cbas.cbas_base_server import CBASBaseTest
from cbas_utils.cbas_utils import CBASRebalanceUtil
from couchbase_helper.tuq_helper import N1QLHelper
from rbac_utils.Rbac_ready_functions import RbacUtils
from remote.remote_util import RemoteUtilHelper
from shell_util.remote_connection import RemoteMachineShellConnection

rbac_users_created = {}


class CBASExternalCollectionsDynamicPrefixes(CBASBaseTest):

    def setUp(self):

        super(CBASExternalCollectionsDynamicPrefixes, self).setUp()

        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.setUp.__name__)

        # Since all the test cases are being run on 1 cluster only
        self.cluster = self.cb_clusters.values()[0]

        self.aws_access_key = self.input.param("aws_access_key")
        self.aws_secret_key = self.input.param("aws_secret_key")
        self.aws_session_token = self.input.param("aws_session_token", "")

        self.paths_on_external_container = self.input.param("paths_on_external_container")
        self.file_format = self.input.param("file_format")
        self.file_format_for_upload = json.loads(self.input.param(
            "file_format_for_upload", "[\"json\"]"))
        self.no_of_files = self.input.param("no_of_files", 10)
        self.no_of_docs = self.input.param("no_of_docs", 100)
        self.embed_filter_values = self.input.param("embed_filter_values", True)

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

        super(CBASExternalCollectionsDynamicPrefixes, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.tearDown.__name__)

    def setup_for_test(self, create_links=True, create_aws_buckets=True,
                       create_dataset_objs=True, same_dv_for_link_and_dataset=True,
                       create_datasets=False, initialize_helper_objs=True,
                       rebalance_util=False):

        # First creating aws buckets, links will be created in the same region as buckets.
        link_regions = []
        if create_aws_buckets:
            for _ in range(self.input.param("no_of_aws_bucket", 1)):
                retry = 0
                bucket_created = False
                aws_bucket_name = self.input.param(
                    "aws_bucket_name", "cbas-regression-{0}".format(
                        random.randint(1, 10000)))

                while (not bucket_created) and retry < 10:
                    region = random.choice(self.aws_region_list)
                    try:
                        self.log.info("Creating AWS bucket - {0} in region {1}".format(
                            aws_bucket_name, region))
                        if not perform_S3_operation(
                                aws_access_key=self.aws_access_key,
                                aws_secret_key=self.aws_secret_key,
                                aws_session_token=self.aws_session_token,
                                create_bucket=True, bucket_name=aws_bucket_name,
                                region=region):
                            self.fail("Creating S3 bucket - {0} in region {1}. Failed.".format(
                                aws_bucket_name, region))
                        bucket_created = True
                        self.aws_buckets[aws_bucket_name] = region
                        link_regions.append(region)
                        self.sleep(60, "Sleeping for 60 seconds to ensure that AWS bucket is created")
                    except Exception as err:
                        self.log.error(str(err))
                        if "InvalidLocationConstraint" in str(err):
                            self.aws_region_list.remove(region)
                        aws_bucket_name = self.input.param(
                            "aws_bucket_name", "cbas-regression-{0}".format(
                                random.randint(1, 10000)))
                    finally:
                        retry += 1
                if not bucket_created:
                    self.fail("Creating S3 bucket - {0} in region {1}. Failed.".format(
                        aws_bucket_name, region))

        if not link_regions:
            link_regions = self.aws_region_list

        self.cbas_util.create_external_link_obj(
            self.cluster, link_type="s3", link_cardinality=self.link_cardinality,
            accessKeyId=self.aws_access_key, secretAccessKey=self.aws_secret_key,
            regions=link_regions, no_of_objs=self.input.param("no_of_links", 1))

        if create_links:

            link_objs = self.cbas_util.list_all_link_objs(link_type="s3")

            for link_obj in link_objs:
                if not self.cbas_util.create_external_link(
                        self.cluster, link_obj.properties,
                        username=self.analytics_username):
                    self.fail("link creation failed")

        if create_dataset_objs:
            self.cbas_util.create_external_dataset_obj(
                self.cluster, self.aws_buckets,
                same_dv_for_link_and_dataset=same_dv_for_link_and_dataset,
                dataset_cardinality=self.dataset_cardinality,
                object_construction_def=None,
                paths_on_external_container=self.paths_on_external_container,
                file_format=self.file_format, redact_warning=None, header=None,
                null_string=None, include=None, exclude=None,
                name_length=30, fixed_length=False,
                no_of_objs=self.input.param("no_of_datasets", 1),
                embed_filter_values=self.embed_filter_values)

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

    def test_create_query_external_collection_with_dynamic_prefixes(self):
        object_construction_def = "filename STRING, folder STRING, mutated INT, \
        null_key STRING, missing_field INT"

        self.setup_for_test(
            create_links=True, create_aws_buckets=True,
            create_dataset_objs=True, same_dv_for_link_and_dataset=False,
            initialize_helper_objs=True)

        dataset_obj = self.cbas_util.list_all_dataset_objs()[0]

        if self.file_format in ["csv", "tsv"]:
            dataset_obj.dataset_properties["object_construction_def"] = object_construction_def
            dataset_obj.dataset_properties["header"] = False

        # Create users with all RBAC roles.
        self.create_or_delete_users(self.rbac_util, rbac_users_created)

        # Load data on s3 bucket
        result = self.s3_data_helper.generate_data_for_s3_and_upload(
            aws_bucket_name=dataset_obj.dataset_properties["external_container_name"],
            key=self.key, no_of_files=self.no_of_files, file_formats=self.file_format_for_upload,
            no_of_folders=self.no_of_files, max_folder_depth=0, header=False, null_key="",
            operation="create", bucket=self.cluster.buckets[0], no_of_docs=self.no_of_docs,
            randomize_header=False, large_file=False, missing_field=[False],
            common_folder_pattern=True,
            folder_pattern=dataset_obj.dataset_properties["path_on_external_container"])
        if result:
            self.fail("Error while uploading files to S3")

        # create external dataset
        if not self.cbas_util.create_dataset_on_external_resource(
                self.cluster, dataset_obj.name, link_name=dataset_obj.link_name,
                dataverse_name=dataset_obj.dataverse_name,
                **dataset_obj.dataset_properties):
            self.fail("Dataset creation failed")

        # run query on dataset
        cbas_query = self.input.param("cbas_query", "Select count(*) from {0};")
        status, metrics, errors, cbas_result, handle = self.cbas_util.execute_statement_on_cbas_util(
            self.cluster, cbas_query.format(dataset_obj.full_name),
            timeout=120, analytics_timeout=120)
