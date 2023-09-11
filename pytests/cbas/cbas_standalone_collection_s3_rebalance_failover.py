import random, json, copy, os
import time

from rbac_utils.Rbac_ready_functions import RbacUtils

from remote.remote_util import RemoteUtilHelper
from threading import Thread
from awsLib.s3_data_helper import perform_S3_operation
from cbas.cbas_base import CBASBaseTest
from CbasLib.CBASOperations import CBASHelper
from cbas_utils.cbas_utils import CBASRebalanceUtil, Dataset_Util

rbac_users_created = {}


class CBASStandaloneExternalLink(CBASBaseTest):
    def setUp(self):

        super(CBASStandaloneExternalLink, self).setUp()

        # Randomly choose name cardinality of links and datasets.
        if "link_cardinality" in self.input.test_params:
            self.link_cardinality = self.input.param("link_cardinality")
        else:
            self.link_cardinality = random.randrange(1, 4)

        if "dataset_cardinality" in self.input.test_params:
            self.dataset_cardinality = self.input.param("dataset_cardinality")
        else:
            self.dataset_cardinality = 1

        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.setUp.__name__)

        # Since all the test cases are being run on 1 cluster only
        self.cluster = self.cb_clusters.values()[0]

        self.aws_access_key = self.input.param("aws_access_key")
        self.aws_secret_key = self.input.param("aws_secret_key")
        self.aws_session_token = self.input.param("aws_session_token", "")

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
        self.rebalance_util = CBASRebalanceUtil(
            self.cluster_util, self.bucket_util, self.task,
            self.input.param("vbucket_check", True), self.cbas_util)

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

        super(CBASStandaloneExternalLink, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.tearDown.__name__)

    def get_aws_credentials(self, user='full_access'):
        with open("/etc/aws_config.json", "r") as fh:
            cred = json.loads(fh.read())
        return cred[user]["aws_access_key"], cred[user]["aws_secret_key"], cred[user]["aws_session_token"]

    def setup_for_test(self, create_links=True, create_aws_buckets=True, create_standalone_collection_objs=True,
                       same_dv_for_link_and_dataset=True, create_datasets=False, initialize_helper_objs=True,
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
                    try:
                        region = random.choice(self.aws_region_list)
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

        self.cbas_util.create_link_obj(
            self.cluster, "s3", link_cardinality=self.link_cardinality,
            accessKeyId=self.aws_access_key, secretAccessKey=self.aws_secret_key,
            regions=link_regions, no_of_objs=self.input.param("no_of_links", 1))

        if create_links:

            link_objs = self.cbas_util.list_all_link_objs(link_type="s3")

            for link_obj in link_objs:
                if not self.cbas_util.create_link(
                        self.cluster, link_obj.properties,
                        username=self.analytics_username):
                    self.fail("link creation failed")

        if create_standalone_collection_objs:
            self.collection_objs = self.cbas_util.create_standalone_obj(
                name_cardinality=self.dataset_cardinality,
                name_length=30, fixed_length=False,
                no_of_obj=self.input.param("no_of_standalone_collection", 1),
                storage_format="column")

            for collection_obj in self.collection_objs:
                if not self.cbas_util.create_standalone_collection(
                        self.cluster, collection_obj.name,
                        dataverse_name=collection_obj.dataverse_name,
                        storage_format=collection_obj.storage_format, with_clause=True):
                    self.fail("Standalone collection creation failed")

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

    def rebalance_cluster(self, cbas_nodes_in=0, cbas_nodes_out=0):
        rebalance_task, self.available_servers = self.rebalance_util.rebalance(
            self.cluster, 0, 0, cbas_nodes_in,
            cbas_nodes_out, self.available_servers, [])

        if not self.rebalance_util.wait_for_rebalance_task_to_complete(
                rebalance_task, self.cluster):
            self.fail("Rebalance failed")

    def create_external_link_and_standalone_collection(self):
        self.aws_buckets_name = "cbas-regression-1"
        self.aws_buckets = {"cbas-regression-1": "us-west-1"}
        self.doc_counts = {
            "json": 1395000,
            "csv": 33560000,
            "tsv": 33560000,
        }
        self.setup_for_test(
            create_links=False, create_aws_buckets=False,
            create_standalone_collection_objs=True, same_dv_for_link_and_dataset=True,
            create_datasets=True, initialize_helper_objs=False)

        link_obj = self.cbas_util.list_all_link_objs(link_type="s3")[0]
        link_obj.properties["region"] = "us-west-1"
        if not self.cbas_util.create_link(
                self.cluster, link_obj.properties, username=self.analytics_username):
            self.fail("link creation failed")

        file_format = self.input.param("file_format", "json")
        dataset_obj = self.cbas_util.list_all_dataset_objs()[0]

        return file_format, dataset_obj, link_obj

    def test_ingestion_from_external_link_s3(self):
        file_format, dataset_obj, link_obj = self.create_external_link_and_standalone_collection()
        self.log.info("Executing copy command to ingest data from external link")
        if not self.cbas_util.copy_from_external_resource_into_standalone_collection(
                self.cluster, dataset_obj.name, self.aws_buckets_name,
                "{0}.{1}".format(CBASHelper.format_name(
                    link_obj.properties["dataverse"]),
                    CBASHelper.format_name(link_obj.properties["name"])),
                dataset_obj.dataverse_name,
                files_to_include=self.input.param("file_to_include"),
                file_format=self.input.param("file_type", "json")):
            self.fail("Fail to launch copy command")
        self.log.info("Checking doc count in standalone collection")
        if self.cbas_util.is_analytics_running(self.cluster):
            if not self.cbas_util.validate_cbas_dataset_items_count(
                    self.cluster, dataset_obj.full_name, self.doc_counts[file_format],
                    timeout=7200, analytics_timeout=7200):
                self.fail("Expected data does not match actual data")

        self.log.info("Test finished")

    def test_ingestion_from_external_link_s3_rebalance(self):
        data_load_stage = self.input.param("data_load_stage", '')
        nodes_in = self.input.param("nodes_in", 0)
        nodes_out = self.input.param("nodes_out", 0)
        file_format, dataset_obj, link_obj = self.create_external_link_and_standalone_collection()
        self.log.info("Executing copy command to ingest data from external link")
        if data_load_stage == "during":
            self.log.info("Scenario: Rebalancing while data loading from s3")
            threads = list()
            threads.append(Thread(
                target=self.cbas_util.copy_from_external_resource_into_standalone_collection,
                args=(self.cluster, dataset_obj.name, self.aws_buckets_name,
                      "{0}.{1}".format(
                          CBASHelper.format_name(link_obj.properties["dataverse"]),
                          CBASHelper.format_name(link_obj.properties["name"])),
                      dataset_obj.dataverse_name,
                      self.input.param("file_to_include"),
                      self.input.param("file_type", "json"),)
            ))
            threads.append(Thread(
                target=self.rebalance_cluster,
                args=(nodes_in, nodes_out,)
            ))
            for thread in threads:
                thread.start()
                time.sleep(20)
            for thread in threads:
                thread.join()

            self.log.info("Checking doc count match with S3 doc count")
            if self.cbas_util.is_analytics_running(self.cluster):
                if not self.cbas_util.validate_cbas_dataset_items_count(
                        self.cluster, dataset_obj.full_name, self.doc_counts[file_format],
                        timeout=7200, analytics_timeout=7200):
                    self.fail("Expected data does not match actual data")

        elif data_load_stage == "before":
            self.log.info("Scenario: Rebalancing after data loading from s3")
            self.log.info("Executing copy command to ingest data from external link")
            if not self.cbas_util.copy_from_external_resource_into_standalone_collection(
                    self.cluster, dataset_obj.name, self.aws_buckets_name,
                    "{0}.{1}".format(CBASHelper.format_name(
                        link_obj.properties["dataverse"]),
                        CBASHelper.format_name(link_obj.properties["name"])),
                    dataset_obj.dataverse_name,
                    self.input.param("file_to_include"),
                    self.input.param("file_type", "json")):
                self.fail("Fail to launch copy command")
            if self.cbas_util.is_analytics_running(self.cluster):
                if not self.cbas_util.validate_cbas_dataset_items_count(
                        self.cluster, dataset_obj.full_name, self.doc_counts[file_format],
                        timeout=7200, analytics_timeout=7200):
                    self.fail("Expected data does not match actual data")

            self.log.info("Rebalancing cluster")
            self.rebalance_cluster(nodes_in, nodes_out)

            self.log.info("Checking doc count match with S3 doc count")
            if self.cbas_util.is_analytics_running(self.cluster):
                if not self.cbas_util.validate_cbas_dataset_items_count(
                        self.cluster, dataset_obj.full_name, self.doc_counts[file_format],
                        timeout=7200, analytics_timeout=7200):
                    self.fail("Expected data does not match actual data")

        elif data_load_stage == "after":
            self.log.info("Scenario: Rebalancing before data loading from s3")
            self.log.info("Rebalancing cluster")
            self.rebalance_cluster(nodes_in, nodes_out)
            if self.cbas_util.is_analytics_running(self.cluster):
                self.log.info("Executing copy command to ingest data from external link")
                if not self.cbas_util.copy_from_external_resource_into_standalone_collection(
                        self.cluster, dataset_obj.name,
                        self.aws_buckets_name, "{0}.{1}".format(
                            CBASHelper.format_name(
                                link_obj.properties["dataverse"]),
                            CBASHelper.format_name(
                                link_obj.properties["name"])),
                        dataset_obj.dataverse_name,
                        self.input.param("file_to_include"),
                        self.input.param("file_type", "json")):
                    self.fail("Fail to launch copy command")
                self.log.info("Checking doc count match with S3 doc count")
                if not self.cbas_util.validate_cbas_dataset_items_count(
                        self.cluster, dataset_obj.full_name, self.doc_counts[file_format],
                        timeout=7200, analytics_timeout=7200):
                    self.fail("Expected data does not match actual data")
