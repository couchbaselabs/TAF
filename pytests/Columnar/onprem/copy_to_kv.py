"""
Created on 22-April-2024
"""
import json
import time

from Columnar.onprem.columnar_onprem_base import ColumnarOnPremBase
from couchbase_utils.security_utils.x509main import x509main
from queue import Queue
from membase.helper.cluster_helper import ClusterOperationHelper
from columnarbasetest import ColumnarBaseTest
from py_constants import CbServer
from couchbase_utils.cbas_utils.cbas_utils_on_prem import CBASRebalanceUtil
from shell_util.remote_connection import RemoteMachineShellConnection


class CopyToKV(ColumnarOnPremBase):
    def setUp(self):
        super(CopyToKV, self).setUp()

        self.columnar_spec_name = self.input.param(
            "columnar_spec_name", "regressions.copy_to_s3"
        )
        self.columnar_spec = self.columnar_cbas_utils.get_columnar_spec(
            self.columnar_spec_name)

        self.analytics_cluster = self.cb_clusters['C1']
        self.remote_cluster = self.cb_clusters['C2']

        self.sdk_client_per_user = self.input.param("sdk_client_per_user", 1)
        ColumnarBaseTest.init_sdk_pool_object(
            self.analytics_cluster, self.sdk_client_per_user,
            self.analytics_cluster.master.rest_username,
            self.analytics_cluster.master.rest_password
        )

        self.no_of_docs = self.input.param("no_of_docs", 10000)
        self.doc_size = self.input.param("doc_size", 1024)

    def tearDown(self):
        super(CopyToKV, self).tearDown()

    def build_cbas_columnar_infra(self):
        # Update remote link specs
        status, certificate, header = x509main(
            self.remote_cluster.master
        )._get_cluster_ca_cert()
        if status:
            certificate = json.loads(certificate)["cert"]["pem"]

        self.columnar_spec["remote_link"]["properties"] = [{
            "type": "couchbase",
            "hostname": self.remote_cluster.master.ip,
            "username": self.remote_cluster.master.rest_username,
            "password": self.remote_cluster.master.rest_password,
            "encryption": self.input.param("link_auth", "full"),
            "certificate": certificate}]

        # update columnar entities values
        self.columnar_spec["dataverse"]["no_of_dataverses"] = \
            self.input.param("no_of_dataverses", 1)
        self.columnar_spec["databases"]["no_of_databases"] = \
            self.input.param("no_of_databases", 1)
        self.columnar_spec["remote_link"]["no_of_remote_links"] = \
            self.input.param("no_of_remote_links", 1)
        self.columnar_spec["external_link"][
            "no_of_external_links"] = self.input.param(
            "no_of_external_links", 0)
        self.columnar_spec["external_link"]["properties"] = [{
            "type": "s3",
            "region": self.aws_region,
            "accessKeyId": self.aws_access_key,
            "secretAccessKey": self.aws_secret_key,
            "serviceEndpoint": None
        }]

        self.columnar_spec["remote_dataset"]["no_of_remote_datasets"] \
            = self.input.param("no_of_remote_datasets", 1)
        self.columnar_spec["standalone_dataset"]["num_of_standalone_coll"] \
            = self.input.param("num_of_standalone_coll", 1)

        self.columnar_spec["external_dataset"]["num_of_external_datasets"] \
            = self.input.param("num_of_external_coll", 0)
        file_format = self.input.param("file_format", "json")
        if self.input.param("num_of_external_coll", 0):
            external_dataset_properties = [{
                "external_container_name": self.s3_source_bucket,
                "path_on_external_container": None,
                "file_format": file_format,
                "include": [f"*.{file_format}"],
                "exclude": None,
                "region": self.aws_region,
                "object_construction_def": None,
                "redact_warning": None,
                "header": None,
                "null_string": None,
                "parse_json_string": 0,
                "convert_decimal_to_double": 0,
                "timezone": ""
            }]
            self.columnar_spec["external_dataset"][
                "external_dataset_properties"] = external_dataset_properties

        if not self.columnar_cbas_utils.create_cbas_infra_from_spec(
                cluster=self.analytics_cluster, cbas_spec=self.columnar_spec,
                bucket_util=self.bucket_util, wait_for_ingestion=False,
                remote_clusters=[self.remote_cluster]):
            self.fail("Error while creating analytics entities.")

    def kill_memcached(self, cluster, master=False, all_nodes=False):
        self.log.info('Kill memcached service in all cluster nodes')
        node = None
        if all_nodes:
            for node in cluster.servers:
                RemoteMachineShellConnection(node).kill_memcached()
            return
        if master:
            RemoteMachineShellConnection(cluster.master).kill_memcached()
        else:
            for i, key in enumerate(cluster.nodes_in_cluster):
                if cluster.nodes_in_cluster[i].ip != cluster.master.ip:
                    RemoteMachineShellConnection(node).kill_memcached()
                    break

        self.log.info('Validate document count')
        if not self.cbas_util.validate_docs_in_all_datasets(
                self.cluster, self.bucket_util, timeout=600):
            self.fail("Dataset doc count does not match the actual "
                      "doc count in associated KV collection")

    def test_copy_to_kv_onprem(self):
        self.log.info("Creating bucket, scope, collection on remote cluster")
        self.collectionSetUp(cluster=self.remote_cluster, load_data=False)
        self.log.info("Creating columnar entities")
        self.build_cbas_columnar_infra()

        datasets = self.columnar_cbas_utils.get_all_dataset_objs("standalone")
        remote_link = \
            self.columnar_cbas_utils.get_all_link_objs("couchbase")[0]
        jobs = Queue()
        results = []

        self.log.info("Loading data in standalone collections")
        for dataset in datasets:
            jobs.put(
                (self.columnar_cbas_utils.load_doc_to_standalone_collection,
                 {"cluster": self.analytics_cluster,
                  "collection_name": dataset.name,
                  "dataverse_name": dataset.dataverse_name,
                  "database_name": dataset.database_name,
                  "no_of_docs": self.no_of_docs,
                  "document_size": self.doc_size}))

        self.columnar_cbas_utils.run_jobs_in_parallel(
            jobs, results, self.sdk_client_per_user, async_run=False)
        remote_bucket = self.remote_cluster.buckets[0]
        scope = self.remote_cluster.buckets[0].scopes[CbServer.default_scope]
        collections = self.remote_cluster.buckets[0].scopes[
            CbServer.default_scope].collections
        for i, key in enumerate(collections):
            collection = "{}.{}.{}".format(
                self.columnar_cbas_utils.format_name(remote_bucket.name),
                scope.name,
                collections[key].name)
            jobs.put((self.columnar_cbas_utils.copy_to_kv,
                      {"cluster": self.cluster,
                       "collection_name": datasets[i].name,
                       "database_name": datasets[i].database_name,
                       "dataverse_name": datasets[i].dataverse_name,
                       "dest_bucket": collection,
                       "link_name": remote_link.full_name}))

        self.columnar_cbas_utils.run_jobs_in_parallel(jobs, results,
                                                      self.sdk_client_per_user)
        if not all(results):
            self.fail("Copy to kv statement failed")

        for i, key in enumerate(collections):
            remote_dataset = \
                self.columnar_cbas_utils.create_remote_dataset_obj(
                    self.cluster, remote_bucket.name,
                    scope.name, collections[key].name, remote_link,
                    capella_as_source=True)[0]
            if not self.columnar_cbas_utils.create_remote_dataset(
                    self.cluster, remote_dataset.name,
                    remote_dataset.full_kv_entity_name,
                    remote_dataset.link_name,
                    dataverse_name=remote_dataset.dataverse_name,
                    database_name=remote_dataset.database_name):
                self.log.error("Failed to create remote dataset on KV")
                results.append(False)

            columnar_count = \
                self.columnar_cbas_utils.get_num_items_in_cbas_dataset(
                    self.cluster, datasets[i].full_name)
            kv_count = self.columnar_cbas_utils.get_num_items_in_cbas_dataset(
                self.cluster, remote_dataset.full_name)
            if columnar_count != kv_count:
                self.log.error(
                    f"Doc count mismatch. KV::{collections[key]}"
                    f" != Columnar::{datasets[i].full_name}, "
                    f"Expected: {columnar_count}, Got: {kv_count}")
                results.append(columnar_count != kv_count)
            if not all(results):
                self.fail("Mismatch found in Copy To KV")

    def test_kill_stop_memcached_copy_to_kv(self):
        self.log.info("Creating bucket, scope, collection on remote cluster")
        self.collectionSetUp(cluster=self.remote_cluster, load_data=False)
        self.log.info("Creating columnar entities")
        self.build_cbas_columnar_infra()

        datasets = self.columnar_cbas_utils.get_all_dataset_objs("external")
        remote_link = self.columnar_cbas_utils.get_all_link_objs(
            "couchbase")[0]
        jobs = Queue()
        results = []

        remote_bucket = self.remote_cluster.buckets[0]
        scope = self.remote_cluster.buckets[0].scopes[CbServer.default_scope]
        collections = self.remote_cluster.buckets[0].scopes[
            CbServer.default_scope].collections
        expected_error = self.input.param("expected_error")
        expected_error_code = self.input.param("expected_error_code", None)
        for i, key in enumerate(collections):
            collection = "{}.{}.{}".format(
                self.columnar_cbas_utils.format_name(remote_bucket.name),
                scope.name,
                collections[key].name)
            jobs.put((self.columnar_cbas_utils.copy_to_kv,
                      {"cluster": self.cluster,
                       "collection_name": datasets[i].name,
                       "database_name": datasets[i].database_name,
                       "dataverse_name": datasets[i].dataverse_name,
                       "dest_bucket": collection,
                       "link_name": remote_link.full_name,
                       "validate_error_msg": True,
                       "expected_error": expected_error,
                       "expected_error_code": expected_error_code}))

        self.columnar_cbas_utils.run_jobs_in_parallel(
            jobs, results, self.sdk_client_per_user, async_run=True)
        time.sleep(20)
        if self.input.param("kill_memcached", False):
            for i in range(5):
                self.kill_memcached(self.remote_cluster,
                                    self.input.param("kill_master", False),
                                    self.input.param("kill_all_node", False))
        if self.input.param("stop_server", False):
            ClusterOperationHelper.stop_cluster(self.remote_cluster.servers)
        self.columnar_cbas_utils.run_jobs_in_parallel(
            jobs, results, self.sdk_client_per_user, async_run=False)
        if not all(results):
            self.fail("Failed to retrieve the error")

    def test_rebalance_in_remote_copy_to_kv(self):
        self.log.info("Creating bucket, scope, collection on remote cluster")
        self.collectionSetUp(cluster=self.remote_cluster, load_data=False)
        self.log.info("Creating columnar entities")
        self.build_cbas_columnar_infra()

        datasets = self.columnar_cbas_utils.get_all_dataset_objs("standalone")
        remote_link = self.columnar_cbas_utils.get_all_link_objs(
            "couchbase")[0]
        jobs = Queue()
        results = []

        self.log.info("Loading data in standalone collections")
        for dataset in datasets:
            jobs.put(
                (self.columnar_cbas_utils.load_doc_to_standalone_collection,
                 {"cluster": self.analytics_cluster,
                  "collection_name": dataset.name,
                  "dataverse_name": dataset.dataverse_name,
                  "database_name": dataset.database_name,
                  "no_of_docs": self.no_of_docs,
                  "document_size": self.doc_size}))

        self.columnar_cbas_utils.run_jobs_in_parallel(
            jobs, results, self.sdk_client_per_user, async_run=False)
        remote_bucket = self.remote_cluster.buckets[0]
        scope = self.remote_cluster.buckets[0].scopes[CbServer.default_scope]
        collections = self.remote_cluster.buckets[0].scopes[
            CbServer.default_scope].collections
        for i, key in enumerate(collections):
            collection = "{}.{}.{}".format(
                self.columnar_cbas_utils.format_name(remote_bucket.name),
                scope.name,
                collections[key].name)
            jobs.put((self.columnar_cbas_utils.copy_to_kv,
                      {"cluster": self.cluster,
                       "collection_name": datasets[i].name,
                       "database_name": datasets[i].database_name,
                       "dataverse_name": datasets[i].dataverse_name,
                       "dest_bucket": collection,
                       "link_name": remote_link.full_name}))

        rebalance_stage = self.input.param("rebalance_stage", "during")
        rebalance_util = CBASRebalanceUtil(self.cluster_util, self.bucket_util,
                                           self.task, vbucket_check=False)

        if rebalance_stage == "before":
            rebalance_task, self.available_servers = rebalance_util.rebalance(
                self.remote_cluster, kv_nodes_in=1,
                available_servers=self.available_servers)
            results.append(rebalance_util.wait_for_rebalance_task_to_complete(
                rebalance_task, self.remote_cluster))
            self.columnar_cbas_utils.run_jobs_in_parallel(
                jobs, results, self.sdk_client_per_user, async_run=False)

        if rebalance_stage == "after":
            self.columnar_cbas_utils.run_jobs_in_parallel(
                jobs, results, self.sdk_client_per_user, async_run=False)
            rebalance_task, self.available_servers = rebalance_util.rebalance(
                self.remote_cluster, kv_nodes_in=1,
                available_servers=self.available_servers)
            results.append(rebalance_util.wait_for_rebalance_task_to_complete(
                rebalance_task, self.remote_cluster))

        if rebalance_stage == "during":
            rebalance_task, self.available_servers = rebalance_util.rebalance(
                self.remote_cluster, kv_nodes_in=1,
                available_servers=self.available_servers)
            self.columnar_cbas_utils.run_jobs_in_parallel(
                jobs, results, self.sdk_client_per_user, async_run=False)
            results.append(rebalance_util.wait_for_rebalance_task_to_complete(
                rebalance_task, self.remote_cluster))

        if not all(results):
            self.fail("Copy to kv failed while remote is scaling")

    def test_rebalance_out_remote_copy_to_kv(self):
        self.log.info("Creating bucket, scope, collection on remote cluster")
        self.collectionSetUp(cluster=self.remote_cluster, load_data=False)
        self.log.info("Creating columnar entities")
        self.build_cbas_columnar_infra()
        nodes_to_keep = []
        if self.input.param("node_to_remove", "other"):
            nodes_to_keep.append(self.remote_cluster.master)
        else:
            for node in self.remote_cluster.servers:
                if node.ip != self.remote_cluster.master.ip:
                    nodes_to_keep.append(node)

        datasets = self.columnar_cbas_utils.get_all_dataset_objs("standalone")
        remote_link = self.columnar_cbas_utils.get_all_link_objs(
            "couchbase")[0]
        jobs = Queue()
        results = []

        self.log.info("Loading data in standalone collections")
        for dataset in datasets:
            jobs.put(
                (self.columnar_cbas_utils.load_doc_to_standalone_collection,
                 {"cluster": self.analytics_cluster,
                  "collection_name": dataset.name,
                  "dataverse_name": dataset.dataverse_name,
                  "database_name": dataset.database_name,
                  "no_of_docs": self.no_of_docs,
                  "document_size": self.doc_size}))

        self.columnar_cbas_utils.run_jobs_in_parallel(
            jobs, results, self.sdk_client_per_user, async_run=False)
        remote_bucket = self.remote_cluster.buckets[0]
        scope = self.remote_cluster.buckets[0].scopes[CbServer.default_scope]
        collections = self.remote_cluster.buckets[0].scopes[
            CbServer.default_scope].collections
        expected_error = self.input.param("expected_error")
        expected_error_code = self.input.param("expected_error_code", None)
        for i, key in enumerate(collections):
            collection = "{}.{}.{}".format(
                self.columnar_cbas_utils.format_name(remote_bucket.name),
                scope.name,
                collections[key].name)
            jobs.put((self.columnar_cbas_utils.copy_to_kv,
                      {"cluster": self.cluster,
                       "collection_name": datasets[i].name,
                       "database_name": datasets[i].database_name,
                       "dataverse_name": datasets[i].dataverse_name,
                       "dest_bucket": collection,
                       "link_name": remote_link.full_name,
                       "validate_error_msg": True,
                       "expected_error": expected_error,
                       "expected_error_code": expected_error_code}))

        rebalance_stage = self.input.param("rebalance_stage", "during")
        rebalance_util = CBASRebalanceUtil(self.cluster_util, self.bucket_util,
                                           self.task, vbucket_check=False)

        if rebalance_stage == "before":
            rebalance_task, self.available_servers = rebalance_util.rebalance(
                self.remote_cluster, kv_nodes_out=1,
                exclude_nodes=nodes_to_keep)
            results.append(rebalance_util.wait_for_rebalance_task_to_complete(
                rebalance_task, self.remote_cluster))
            self.columnar_cbas_utils.run_jobs_in_parallel(
                jobs, results, self.sdk_client_per_user, async_run=False)

        if rebalance_stage == "after":
            self.columnar_cbas_utils.run_jobs_in_parallel(
                jobs, results, self.sdk_client_per_user, async_run=False)
            rebalance_task, self.available_servers = rebalance_util.rebalance(
                self.remote_cluster, kv_nodes_out=1,
                exclude_nodes=nodes_to_keep)
            results.append(rebalance_util.wait_for_rebalance_task_to_complete(
                rebalance_task, self.remote_cluster))

        if rebalance_stage == "during":
            rebalance_task, self.available_servers = rebalance_util.rebalance(
                self.remote_cluster, kv_nodes_out=1,
                exclude_nodes=nodes_to_keep)
            self.columnar_cbas_utils.run_jobs_in_parallel(
                jobs, results, self.sdk_client_per_user, async_run=False)
            results.append(rebalance_util.wait_for_rebalance_task_to_complete(
                rebalance_task, self.remote_cluster))

        if not all(results):
            self.fail("Some operations failed")

    def test_failover_remote_copy_to_kv(self):
        self.log.info("Creating bucket, scope, collection on remote cluster")
        self.collectionSetUp(cluster=self.remote_cluster, load_data=False)
        self.log.info("Creating columnar entities")
        self.build_cbas_columnar_infra()
        nodes_to_keep = []
        if self.input.param("node_to_remove", "other"):
            nodes_to_keep.append(self.remote_cluster.master)
        else:
            for node in self.remote_cluster.servers:
                if node.ip != self.remote_cluster.master.ip:
                    nodes_to_keep.append(node)

        datasets = self.columnar_cbas_utils.get_all_dataset_objs("standalone")
        remote_link = self.columnar_cbas_utils.get_all_link_objs(
            "couchbase")[0]
        jobs = Queue()
        results = []

        self.log.info("Loading data in standalone collections")
        for dataset in datasets:
            jobs.put(
                (self.columnar_cbas_utils.load_doc_to_standalone_collection,
                 {"cluster": self.analytics_cluster,
                  "collection_name": dataset.name,
                  "dataverse_name": dataset.dataverse_name,
                  "database_name": dataset.database_name,
                  "no_of_docs": self.no_of_docs,
                  "document_size": self.doc_size}))

        self.columnar_cbas_utils.run_jobs_in_parallel(
            jobs, results, self.sdk_client_per_user, async_run=False)
        remote_bucket = self.remote_cluster.buckets[0]
        scope = self.remote_cluster.buckets[0].scopes[CbServer.default_scope]
        collections = self.remote_cluster.buckets[0].scopes[
            CbServer.default_scope].collections
        expected_error = self.input.param("expected_error")
        expected_error_code = self.input.param("expected_error_code", None)
        for i, key in enumerate(collections):
            collection = "{}.{}.{}".format(
                self.columnar_cbas_utils.format_name(remote_bucket.name),
                scope.name,
                collections[key].name)
            jobs.put((self.columnar_cbas_utils.copy_to_kv,
                      {"cluster": self.cluster,
                       "collection_name": datasets[i].name,
                       "database_name": datasets[i].database_name,
                       "dataverse_name": datasets[i].dataverse_name,
                       "dest_bucket": collection,
                       "link_name": remote_link.full_name,
                       "validate_error_msg": True,
                       "expected_error": expected_error,
                       "expected_error_code": expected_error_code}))

        rebalance_stage = self.input.param("rebalance_stage", "during")
        graceful = self.input.param("graceful", "during")
        rebalance_util = CBASRebalanceUtil(self.cluster_util, self.bucket_util,
                                           self.task, vbucket_check=False)

        if rebalance_stage == "before":
            available_servers, kv_failover_nodes, _ = rebalance_util.failover(
                self.remote_cluster, kv_nodes=1,
                failover_type=graceful, exclude_nodes=nodes_to_keep)
            self.columnar_cbas_utils.run_jobs_in_parallel(
                jobs, results, self.sdk_client_per_user, async_run=False)

        if rebalance_stage == "after":
            self.columnar_cbas_utils.run_jobs_in_parallel(
                jobs, results, self.sdk_client_per_user, async_run=False)
            available_servers, kv_failover_nodes, _ = rebalance_util.failover(
                self.remote_cluster, kv_nodes=1,
                failover_type=graceful, exclude_nodes=nodes_to_keep)

        if rebalance_stage == "during":
            jobs.put((rebalance_util.failover,
                      {"cluster": self.remote_cluster,
                       "kv_nodes": 1,
                       "failover_type": graceful,
                       "exclude_noes": nodes_to_keep}))
            self.columnar_cbas_utils.run_jobs_in_parallel(
                jobs, results, self.sdk_client_per_user, async_run=False)
