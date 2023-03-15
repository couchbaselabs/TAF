"""
This util can be used to create buckets for TPCH and load data into the buckets created.
"""
import os
import importlib
import copy
from couchbase_utils.cb_tools.cbimport import CbImport
from remote.remote_util import RemoteMachineShellConnection
from global_vars import logger
from bucket_collections.collections_base import CollectionBase
from java.lang import Exception as Java_base_exception
from awsLib.s3_data_helper import perform_S3_operation


class TPCHUtil(object):

    def __init__(self, basetest_obj):
        """
        :param server_task task object
        """
        self.log = logger.get("test")
        self.basetest_obj = basetest_obj

    """
    Method is used to create buckets for TPCH data load
    :param basetest_obj the basetestcase object
    :param cluster the cluster obj for the cluster on which the buckets have to be loaded.
    """
    def create_kv_buckets_for_tpch(self, cluster):
        try:
            bucket_spec_name = "analytics.tpch"
            self.basetest_obj.bucket_util.add_rbac_user(cluster.master)
            buckets_spec = self.basetest_obj.bucket_util.get_bucket_template_from_package(bucket_spec_name)

            self.basetest_obj.bucket_util.create_buckets_using_json_data(cluster, buckets_spec)
            self.basetest_obj.bucket_util.wait_for_collection_creation_to_complete(cluster)

            # Prints bucket stats before doc_ops
            self.basetest_obj.bucket_util.print_bucket_stats(cluster)

            # Init sdk_client_pool if not initialized before
            if self.basetest_obj.sdk_client_pool is None:
                self.basetest_obj.init_sdk_pool_object()

            self.log.info("Creating required SDK clients for client_pool")
            CollectionBase.create_sdk_clients(
                self.basetest_obj.task_manager.number_of_threads, cluster.master, cluster.buckets,
                self.basetest_obj.sdk_client_pool, self.basetest_obj.sdk_compression)

            self.basetest_obj.cluster_util.print_cluster_stats(cluster)
        except Java_base_exception as exception:
            self.basetest_obj.handle_setup_exception(exception)
        except Exception as exception:
            self.basetest_obj.handle_setup_exception(exception)

    """
    Method downloads TPCH data files from S3 bucket and loads data into KV buckets
    """
    def load_tpch_data_into_KV_buckets(self, cluster):
        # Load data into tpch buckets
        aws_access_key = self.basetest_obj.input.param("aws_access_key")
        aws_secret_key = self.basetest_obj.input.param("aws_secret_key")
        aws_session_token = self.basetest_obj.input.param("aws_session_token", "")

        data_folder_path = os.path.dirname(__file__)
        dest_folder_path = "/tmp"
        info = TPCHUtil.fetch_info_from_tpch_helper()
        data_info = info.data_file_info
        for file_info in data_info:
            data_info[file_info]["data_file_path_src"] = os.path.join(data_folder_path,
                                                                      data_info[file_info]["filename"])
            data_info[file_info]["data_file_path_dest"] = os.path.join(dest_folder_path,
                                                                       data_info[file_info]["filename"])
            self.log.info("Downloading TPCH data file {0} from S3 bucket".format(data_info[file_info]["filename"]))
            response = perform_S3_operation(
                aws_access_key=aws_access_key, aws_secret_key=aws_secret_key,
                aws_session_token=aws_session_token, bucket_name="tpch-data-files", download_file=True,
                src_path=data_info[file_info]["filename"], dest_path=data_info[file_info]["data_file_path_src"])
            if not response:
                raise Exception("Error while downloading TPCH data file {0} from S3 bucket".format(data_info[file_info]["filename"]))

        shell_conn = RemoteMachineShellConnection(cluster.master)
        cbimport = CbImport(shell_conn)
        num_threads = cluster.rest.get_nodes_self().cpuCount
        for bucket in cluster.buckets:
            result = shell_conn.copy_file_local_to_remote(data_info[bucket.name]["data_file_path_src"], dest_folder_path)
            if not result:
                self.fail("Error while copying file {0} to node {1}".format(
                    data_info[bucket.name]["data_file_path_src"], cluster.master.ip))
            cbimport.import_cvs_data_from_file_into_bucket(
                bucket=bucket.name, file_path=data_info[bucket.name]["data_file_path_dest"],
                key_row_value=data_info[bucket.name]["key_generator"], field_separator="|",
                infer_types=True, no_of_threads=num_threads)

    @staticmethod
    def fetch_info_from_tpch_helper():
        return importlib.import_module(
            'couchbase_utils.tpch_utils.tpch_helper')

    @staticmethod
    def get_tpch_queries():
        """
        Fetches the TPCH queries from file mentioned.
        """
        info = TPCHUtil.fetch_info_from_tpch_helper()
        return copy.deepcopy(info.queries)

    @staticmethod
    def get_kv_index_queries_tpch():
        """
        Fetches the queries to create secondary indexes on TPCH KV collections from file mentioned.
        """
        info = TPCHUtil.fetch_info_from_tpch_helper()
        return copy.deepcopy(info.kv_indexes)

    @staticmethod
    def get_cbas_index_queries_tpch():
        """
        Fetches the queries to create secondary indexes on TPCH cbas collections from file mentioned.
        """
        info = TPCHUtil.fetch_info_from_tpch_helper()
        return copy.deepcopy(info.cbas_indexes)

    @staticmethod
    def get_doc_count_in_tpch_buckets():
        info = TPCHUtil.fetch_info_from_tpch_helper()
        return copy.deepcopy(info.bucket_doc_count)

