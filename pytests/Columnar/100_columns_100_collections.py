import math
import threading
import time

import random
from queue import Queue

from Columnar.columnar_base import ColumnarBaseTest
from Jython_tasks.sirius_task import WorkLoadTask
from capellaAPI.capella.columnar.CapellaAPI import CapellaAPI as ColumnarAPI

from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from cbas_utils.cbas_utils_columnar import ColumnarStats
from sirius_client_framework.multiple_database_config import CouchbaseLoader
from sirius_client_framework.operation_config import WorkloadOperationConfig
from sirius_client_framework.sirius_constants import SiriusCodes


class Columns100Collections100(ColumnarBaseTest):
    def setUp(self):
        super(Columns100Collections100, self).setUp()
        self.cluster = self.tenant.columnar_instances[0]
        self.columnarAPI = ColumnarAPI(self.pod.url_public, '', '', self.tenant.user,
                                       self.tenant.pwd, '')

        self.no_of_docs = self.input.param("no_of_docs", 1000)

        if not self.columnar_spec_name:
            self.columnar_spec_name = "regressions.copy_to_s3"

        self.columnar_spec = self.cbas_util.get_columnar_spec(
            self.columnar_spec_name)

        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.setUp.__name__)

    def tearDown(self):
        """
        Delete all the analytics link and columnar instance
        """
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)
        self.stop_process()
        if hasattr(self, "remote_cluster") and hasattr(self.remote_cluster, "buckets"):
            self.delete_all_buckets_from_capella_cluster(self.tenant, self.remote_cluster)
        if not self.cbas_util.delete_cbas_infra_created_from_spec(
                self.cluster, self.columnar_spec):
            self.fail("Error while deleting cbas entities")

        super(ColumnarBaseTest, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished", stage="Teardown")

    def remote_cluster_setup(self):
        for key in self.cb_clusters:
            self.remote_cluster = self.cb_clusters[key]
            break
        resp = self.capellaAPI.cluster_ops_apis.add_CIDR_to_allowed_CIDRs_list(self.tenant.id,
                                                                               self.tenant.project_id,
                                                                               self.remote_cluster.id, "0.0.0.0/0")
        if resp.status_code == 201 or resp.status_code == 422:
            self.log.info("Added allowed IP 0.0.0.0/0")
        else:
            self.fail("Failed to add allowed IP")
        remote_cluster_certificate_request = (
            self.capellaAPI.cluster_ops_apis.get_cluster_certificate(self.tenant.id, self.tenant.project_id,
                                                                     self.remote_cluster.id))
        if remote_cluster_certificate_request.status_code == 200:
            self.remote_cluster_certificate = (remote_cluster_certificate_request.json()["certificate"])
        else:
            self.fail("Failed to get cluster certificate")

        # creating bucket scope and collections for remote collection
        no_of_remote_buckets = self.input.param("no_of_remote_bucket", 1)
        self.create_bucket_scopes_collections_in_capella_cluster(self.tenant, self.remote_cluster, no_of_remote_buckets,
                                                                 bucket_ram_quota=200)

    def base_infra_setup(self):
        self.columnar_spec["database"]["no_of_databases"] = self.input.param("no_of_database", 1)
        self.columnar_spec["dataverse"]["no_of_dataverses"] = self.input.param("no_of_scopes", 1)
        self.columnar_spec["synonym"]["no_of_synonyms"] = self.input.param(
            "synonym", 0)
        self.columnar_spec["index"]["no_of_indexes"] = self.input.param(
            "index", 0)
        self.columnar_spec["remote_link"]["no_of_remote_links"] = self.input.param(
            "no_of_remote_links", 0)
        if self.columnar_spec["remote_link"]["no_of_remote_links"] != 0:
            self.remote_cluster_setup()
            remote_link_properties = list()
            remote_link_properties.append(
                {"type": "couchbase", "hostname": str(self.remote_cluster.srv),
                 "username": self.remote_cluster.username,
                 "password": self.remote_cluster.password,
                 "encryption": "full",
                 "certificate": self.remote_cluster_certificate}
            )
            self.columnar_spec["remote_link"]["properties"] = remote_link_properties
            self.columnar_spec["remote_dataset"]["num_of_remote_datasets"] = self.input.param("no_of_remote_coll", 1)

        self.columnar_spec["external_link"]["no_of_external_links"] = self.input.param(
            "no_of_external_links", 0)

        self.columnar_spec["external_link"]["properties"] = [{
            "type": "s3",
            "region": self.aws_region,
            "accessKeyId": self.aws_access_key,
            "secretAccessKey": self.aws_secret_key,
            "serviceEndpoint": None
        }]
        self.columnar_spec["external_dataset"]["num_of_external_datasets"] = self.input.param("no_of_external_coll", 0)
        if self.columnar_spec["external_dataset"]["num_of_external_datasets"]:
            external_dataset_properties = [{
                "external_container_name": self.s3_source_bucket,
                "path_on_external_container": None,
                "file_format": self.input.param("file_format", "json"),
                "include": ["*.{0}".format(self.input.param("file_format", "json"))],
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

        self.columnar_spec["standalone_dataset"][
            "num_of_standalone_coll"] = self.input.param(
            "no_of_standalone_coll", 0)
        self.columnar_spec["standalone_dataset"]["primary_key"] = [{"product_id": "string"}]

        if not hasattr(self, "remote_cluster"):
            remote_cluster = None
        else:
            remote_cluster = [self.remote_cluster]
        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            self.cluster, self.columnar_spec, self.bucket_util, False, remote_clusters=remote_cluster)
        if not result:
            self.fail(msg)

    def calculate_volume_per_source(self):
        doc_size = self.input.param("doc_size", 3072)
        total_volume = self.input.param("total_volume", 1000000000)
        total_doc = math.ceil(total_volume / doc_size)
        self.remote_source_doc = int(total_doc)
        self.log.info("Total doc in remote source: {}".format(self.remote_source_doc))
        self.remote_source_doc_per_collection = (self.remote_source_doc //
                                                 self.input.param("no_of_remote_coll", 1))
        self.log.info("Doc per remote collection: {}".format(
            self.remote_source_doc_per_collection))

    def load_doc_to_remote_collection(self, data_loading_job, start, end):
        if hasattr(self, "remote_cluster") and hasattr(self.remote_cluster, "buckets"):
            for bucket in self.remote_cluster.buckets:
                if bucket.name != "_default":
                    for scope in bucket.scopes:
                        if scope != "_system" and scope != "_mobile":
                            for collection in bucket.scopes[scope].collections:
                                data_loading_job.put((self.remote_collections_operations,
                                                      {"bucket": bucket.name, "scope": scope, "collection": collection,
                                                       "start": start, "end": end}))
        else:
            self.log.warn("No cluster/bucket found for remote collection")

    def remote_collections_operations(self, bucket, scope, collection, start, end, action="create"):
        database_information = CouchbaseLoader(username=self.remote_cluster.username,
                                               password=self.remote_cluster.password,
                                               connection_string="couchbases://" + self.remote_cluster.srv,
                                               bucket=bucket, scope=scope, collection=collection, sdk_batch_size=500)
        operation_config = WorkloadOperationConfig(start=int(start), end=int(end), template="product_large",
                                                   doc_size=int(self.doc_size))

        required_action = None
        if action == "create":
            required_action = SiriusCodes.DocOps.CREATE
        if action == "delete":
            required_action = SiriusCodes.DocOps.DELETE
        if action == "upsert":
            required_action = SiriusCodes.DocOps.UPDATE
        task_insert = WorkLoadTask(bucket=bucket, task_manager=self.task_manager, op_type=required_action,
                                   database_information=database_information, operation_config=operation_config,
                                   default_sirius_base_url=self.sirius_base_url)
        self.task_manager.add_new_task(task_insert)
        self.task_manager.get_task_result(task_insert)
        return

    def run_queries_on_datasets(self, query_jobs):
        datasets = self.cbas_util.get_all_dataset_objs()
        while self.run_queries:
            if query_jobs.qsize() < 5:
                dataset = random.choice(datasets)
                queries = [
                    "SET `compiler.external.field.pushdown` 'false'; "
                    "SELECT COUNT(*) from {}".format(dataset.full_name),
                    "SELECT subcategory, SUM(stock_quantity) AS total_stock_quantity FROM {} GROUP BY subcategory;"
                    .format(dataset.full_name),
                    "SELECT category, AVG(price) AS average_price FROM {} GROUP BY category;".format(dataset.full_name),
                    "SELECT brand, AVG(rating) AS average_rating FROM {} GROUP BY brand;".format(dataset.full_name),
                    "SELECT brand, SUM(views) AS total_views FROM {} GROUP BY brand;".format(dataset.full_name),
                    "SELECT category, AVG(discount) AS average_discount FROM {} GROUP BY category;".
                    format(dataset.full_name),
                    "SET `compiler.external.field.pushdown` 'false'; "
                    "SELECT COUNT(*) from {}".format(dataset.full_name),
                    "SELECT subcategory, SUM(stock_quantity) AS total_stock_quantity FROM {} GROUP BY subcategory;"
                    .format(dataset.full_name),
                    "SELECT category, AVG(price) AS average_price FROM {} GROUP BY category;".format(dataset.full_name),
                    "SELECT brand, AVG(rating) AS average_rating FROM {} GROUP BY brand;".format(dataset.full_name),
                    "SELECT brand, SUM(views) AS total_views FROM {} GROUP BY brand;".format(dataset.full_name),
                    "SELECT category, AVG(discount) AS average_discount FROM {} GROUP BY category;".
                    format(dataset.full_name)
                ]
                query = random.choice(queries).format(dataset.full_name)
                with threading.Lock():
                    query_jobs.put((self.cbas_util.execute_statement_on_cbas_util,
                                    {"cluster": self.cluster, "statement": query, "analytics_timeout": 1800,
                                     "timeout": 1800}))

    def average_cpu_stats(self):
        average_cpu_utilization_rate = 0
        max_cpu_utilization_rate = 0
        min_cpu_utilization_rate = 1e9
        count = 0
        while self.get_cpu_stats:
            columnar_stats = ColumnarStats()
            cpu_node_average = columnar_stats.cpu_utalization_rate(cluster=self.cluster)
            average_cpu_utilization_rate = ((average_cpu_utilization_rate * count) + cpu_node_average) / (count + 1)
            if cpu_node_average > max_cpu_utilization_rate:
                max_cpu_utilization_rate = cpu_node_average
            if cpu_node_average < min_cpu_utilization_rate:
                min_cpu_utilization_rate = cpu_node_average
            count = count + 1
            time.sleep(60)
        self.log.info("Average CPU utilization rate: {}".format(average_cpu_utilization_rate))
        self.log.info("Max CPU utilization rate: {}".format(max_cpu_utilization_rate))
        self.log.info("Min CPU utilization rate: {}".format(min_cpu_utilization_rate))
        return average_cpu_utilization_rate, max_cpu_utilization_rate, min_cpu_utilization_rate

    def create_doc_per_cycle(self, data_partition_value, no_of_docs_total):
        if data_partition_value == 1:
            return 0, no_of_docs_total // 4
        if data_partition_value == 2:
            return no_of_docs_total // 4, no_of_docs_total // 2
        if data_partition_value == 3:
            return no_of_docs_total // 2, no_of_docs_total * 3 // 4
        if data_partition_value == 4:
            return no_of_docs_total * 3 // 4, no_of_docs_total
        else:
            self.fail(
                "Currently only supports 25% percent per iteration, to achieve 100%, "
                "call 4 cycles with data_partition_value as 1,2,3 and 4")

    def scale_columnar_cluster(self, nodes, timeout=10000):
        start_time = time.time()
        status = None
        resp = self.columnarAPI.update_columnar_instance(self.tenant.id,
                                                         self.tenant.project_id,
                                                         self.cluster.instance_id,
                                                         self.cluster.name, '', nodes)
        if resp.status_code != 202:
            self.fail("Failed to scale cluster")
            # check for nodes in the cluster, add a sleep here until node api is present
        time.sleep(10)
        while (status == "scaling" or status is None) and time.time() < start_time + timeout:
            self.log.info("Instance is still scaling after: {} seconds".format(time.time() - start_time))
            try:
                resp = self.columnarAPI.get_specific_columnar_instance(self.tenant.id,
                                                                       self.tenant.project_id,
                                                                       self.cluster.instance_id)
                resp = resp.json()
                status = resp["data"]["state"]
                time.sleep(20)
            except Exception as e:
                self.log.error(str(e))
        current_nodes = 0
        while current_nodes != nodes and time.time() < start_time + timeout:
            rest = ClusterRestAPI(self.cluster.master)
            status, content = rest.cluster_details()
            if not status:
                self.log.error("Error while fetching pools/default using connection string")
            current_nodes = len(content["nodes"])
            time.sleep(20)
        resp = self.columnarAPI.get_specific_columnar_instance(self.tenant.id,
                                                               self.tenant.project_id, self.cluster.instance_id)
        resp = resp.json()
        status = resp["data"]["state"]
        if status != "healthy":
            return False
        self.columnar_utils.update_columnar_instance_obj(self.pod, self.tenant, self.cluster)
        return True

    def stop_process(self, query_pass=False):
        self.wait_for_job = [False]
        self.run_queries = False
        self.get_cpu_stats = False
        self.cpu_stat_job.join()
        self.query_job.join()
        if query_pass and not all(self.query_work_results):
            self.log.error("Queries Failed")
            self.fail("Queries Failed")

    def test_100_columns_100_collections(self):
        self.crud_jobs = Queue()
        self.perform_crud = None
        self.query_work_results = None
        self.cpu_stat_job = Queue()
        self.query_job = Queue()
        self.data_loading_job = Queue()
        self.sirius_base_url = self.input.param("sirius_url", "http://127.0.0.1:4000")
        start_time = time.time()
        self.base_infra_setup()
        self.calculate_volume_per_source()
        create_query_job = Queue()
        self.cpu_stat_job = Queue()
        results = []

        # calculate doc to load for each cycle
        for i in range(1, 5):
            self.remote_start, self.remote_end = (self.create_doc_per_cycle(i, self.remote_source_doc_per_collection))
            self.load_doc_to_remote_collection(self.data_loading_job, self.remote_start,
                                               self.remote_end)
            self.cbas_util.run_jobs_in_parallel(self.data_loading_job, results, thread_count=5,
                                                async_run=True)
            self.run_queries = True
            self.get_cpu_stats = True
            self.wait_for_job = [True]
            self.query_work_results = []

            create_query_job.put((self.run_queries_on_datasets, {"query_jobs": self.query_job}))
            self.cpu_stat_job.put((self.average_cpu_stats, {}))

            self.cbas_util.run_jobs_in_parallel(self.cpu_stat_job, results, 1, async_run=True)
            self.cbas_util.run_jobs_in_parallel(create_query_job, self.query_work_results, 2,
                                                async_run=True)
            while self.query_job.qsize() < 5:
                self.log.info("Waiting for query job to be created")
                time.sleep(10)

            if not self.scale_columnar_cluster(8):
                self.fail("Failed to scale up the instance")

            self.cbas_util.run_jobs_in_parallel(self.query_job, self.query_work_results, 2,
                                                async_run=True, wait_for_job=self.wait_for_job)
            self.cbas_util.run_jobs_in_parallel(self.cpu_stat_job, results, 1,
                                                async_run=True)

            # wait for data loading to complete
            self.data_loading_job.join()
            self.cbas_util.wait_for_data_ingestion_in_the_collections(self.cluster)

            if not self.scale_columnar_cluster(2 ** i if i != 3 else 2):
                self.fail("Failed to scale up the instance")
            self.stop_process(False)

        self.log.info("Time taken to run mini-volume: {} minutes".format((time.time() - start_time) / 60))
