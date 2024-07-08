import math
import random
import time
from queue import Queue

from cbas_utils.cbas_utils_columnar import ColumnarStats
from sirius_client_framework.multiple_database_config import CouchbaseLoader
from sirius_client_framework.operation_config import WorkloadOperationConfig
from sirius_client_framework.sirius_constants import SiriusCodes
from Jython_tasks.sirius_task import WorkLoadTask
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI


class MiniVolume:
    def __init__(self, base_object, sirius_url="http://127.0.0.1:4000"):
        self.crud_jobs = Queue()
        self.perform_crud = None
        self.query_work_results = None
        self.cpu_stat_job = Queue()
        self.base_object = base_object
        self.base_object.sirius_base_url = sirius_url
        self.base_object.query_job = Queue()
        self.base_object.data_loading_job = Queue()

    def calculate_volume_per_source(self, percentage_remote_volume=0.8, percentage_byok_volume=0.2):
        doc_size = self.base_object.input.param("doc_size", 1024)
        self.base_object.standalone_volume = 1e+10 * self.base_object.input.param("no_of_standalone_coll", 0)
        self.base_object.log.info("Volume in standalone collection {} Gb".format(self.base_object.input.param
                                                                                 ("no_of_standalone_coll", 0) * 10))
        total_volume = self.base_object.input.param("total_volume", 1000000000)
        volume_left = total_volume - self.base_object.standalone_volume
        total_doc = math.ceil(volume_left / doc_size)
        self.base_object.remote_source_doc = int(math.ceil(total_doc * percentage_remote_volume))
        self.base_object.log.info("Total doc in remote source: {}".format(self.base_object.remote_source_doc))
        self.base_object.byok_source_doc = int(math.ceil(total_doc * percentage_byok_volume))
        self.base_object.remote_source_doc_per_collection = (self.base_object.remote_source_doc //
                                                             self.base_object.input.param("no_of_remote_coll", 1))
        self.base_object.log.info("Doc per remote collection: {}".format(
            self.base_object.remote_source_doc_per_collection))

    def crud_on_remote_collections(self, start, end):
        """
        Updates document, insert docs and delete docs.
        Does not increase the total number of items in the collection
        """
        deleted_items = []
        while self.perform_crud:
            operation_start = random.randint(start, end)
            operation_end = operation_start + random.randint(1, 100)
            if operation_end > end:
                operation_end = end
            actions_supported = ["create", "upsert", "delete"]
            actions_weights = [0.1, 0.8, 0.1]
            action = random.choices(actions_supported, weights=actions_weights, k=1)[0]

            if action == "create":
                if len(deleted_items):
                    item = random.choice(deleted_items)
                    start = item["start"]
                    end = item["end"]
                else:
                    continue

            if action == "delete":
                action_range = {"start": operation_start, "end": operation_end}
                deleted_items.append(action_range)

            if hasattr(self.base_object, "remote_cluster") and hasattr(self.base_object.remote_cluster, "buckets"):
                for bucket in self.base_object.remote_cluster.buckets:
                    if bucket.name != "_default":
                        for scope in bucket.scopes:
                            if scope != "_system" and scope != "_mobile":
                                for collection in bucket.scopes[scope].collections:
                                    self.remote_collections_operations(bucket.name, scope, collection, operation_start,
                                                                       operation_end, action)
            time.sleep(30)

        for item in deleted_items:
            start = item["start"]
            end = item["end"]
            action = "create"
            if hasattr(self.base_object, "remote_cluster") and hasattr(self.base_object.remote_cluster, "buckets"):
                for bucket in self.base_object.remote_cluster.buckets:
                    if bucket.name != "_default":
                        for scope in bucket.scopes:
                            if scope != "_system" and scope != "_mobile":
                                for collection in bucket.scopes[scope].collections:
                                    self.remote_collections_operations(bucket.name, scope, collection, start, end,
                                                                       action)

    def load_doc_to_remote_collection(self, data_loading_job, start, end):
        if hasattr(self.base_object, "remote_cluster") and hasattr(self.base_object.remote_cluster, "buckets"):
            for bucket in self.base_object.remote_cluster.buckets:
                if bucket.name != "_default":
                    for scope in bucket.scopes:
                        if scope != "_system" and scope != "_mobile":
                            for collection in bucket.scopes[scope].collections:
                                data_loading_job.put((self.remote_collections_operations,
                                                      {"bucket": bucket.name, "scope": scope, "collection": collection,
                                                       "start": start, "end": end}))
        else:
            self.base_object.log.warn("No cluster/bucket found for remote collection")

    def remote_collections_operations(self, bucket, scope, collection, start, end, action="create"):
        database_information = CouchbaseLoader(username=self.base_object.remote_cluster.username,
                                               password=self.base_object.remote_cluster.password,
                                               connection_string="couchbases://" + self.base_object.remote_cluster.srv,
                                               bucket=bucket, scope=scope, collection=collection, sdk_batch_size=500)
        operation_config = WorkloadOperationConfig(start=int(start), end=int(end), template="product",
                                                   doc_size=int(self.base_object.doc_size))

        required_action = None
        if action == "create":
            required_action = SiriusCodes.DocOps.CREATE
        if action == "delete":
            required_action = SiriusCodes.DocOps.DELETE
        if action == "upsert":
            required_action = SiriusCodes.DocOps.UPDATE
        task_insert = WorkLoadTask(task_manager=self.base_object.task_manager, op_type=required_action,
                                   database_information=database_information, operation_config=operation_config,
                                   default_sirius_base_url=self.base_object.sirius_base_url)
        self.base_object.task_manager.add_new_task(task_insert)
        self.base_object.task_manager.get_task_result(task_insert)
        return

    def run_queries_on_datasets(self, query_jobs):
        datasets = self.base_object.cbas_util.get_all_dataset_objs()
        while self.base_object.run_queries:
            if query_jobs.qsize() < 5:
                dataset = random.choice(datasets)
                queries = [
                    "SELECT p.seller_name, p.seller_location, AVG(p.avg_rating) AS avg_rating FROM (SELECT seller_name,"
                    "seller_location, avg_rating FROM {} WHERE 'Passenger car medium' in product_category) p "
                    "GROUP BY p.seller_name, p.seller_location ORDER BY avg_rating DESC;".format(dataset.full_name),
                    "SELECT AVG(r.avg_rating) AS avg_rating, SUM(r.num_sold) AS num_sold, AVG(r.price) AS avg_price, "
                    "ARRAY_FLATTEN(ARRAY_AGG(r.product_category), 1) AS product_category, "
                    "AVG(r.product_rating.build_quality) AS avg_build_quality, SUM(r.quantity) AS total_quantity, "
                    "r.seller_verified, AVG(r.weight) AS avg_weight FROM {} AS r GROUP BY r.product_name, "
                    "r.seller_verified".format(dataset.full_name),
                    "SET `compiler.external.field.pushdown` 'false'; "
                    "SELECT COUNT(*) from {}".format(dataset.full_name)
                ]
                query = random.choice(queries).format(dataset.full_name)
                with threading.Lock():
                    query_jobs.put((self.base_object.cbas_util.execute_statement_on_cbas_util,
                                    {"cluster": self.base_object.cluster, "statement": query, "analytics_timeout": 1800,
                                     "timeout": 1800}))

    def load_doc_to_standalone_collection(self, data_loading_job):
        standalone_datasets = self.base_object.cbas_util.get_all_dataset_objs("standalone")
        # start data load on standalone collections
        file_format = self.base_object.input.param("file_format", "json")
        external_link = random.choice(self.base_object.cbas_util.get_all_link_objs(link_type="s3"))
        for standalone_coll in standalone_datasets:
            data_loading_job.put((self.base_object.cbas_util.copy_from_external_resource_into_standalone_collection,
                                  {
                                      "cluster": self.base_object.cluster, "collection_name": standalone_coll.name,
                                      "aws_bucket_name": self.base_object.s3_source_bucket,
                                      "external_link_name": external_link.name,
                                      "dataverse_name": standalone_coll.dataverse_name,
                                      "database_name": standalone_coll.database_name,
                                      "files_to_include": "*.{0}".format(file_format),
                                      "type_parsing_info": "",
                                      "path_on_aws_bucket": ""
                                  }))

    def average_cpu_stats(self):
        average_cpu_utilization_rate = 0
        max_cpu_utilization_rate = 0
        min_cpu_utilization_rate = 1e9
        count = 0
        while self.base_object.get_cpu_stats:
            columnar_stats = ColumnarStats()
            cpu_node_average = columnar_stats.cpu_utalization_rate(cluster=self.base_object.cluster)
            average_cpu_utilization_rate = ((average_cpu_utilization_rate * count) + cpu_node_average) / (count + 1)
            if cpu_node_average > max_cpu_utilization_rate:
                max_cpu_utilization_rate = cpu_node_average
            if cpu_node_average < min_cpu_utilization_rate:
                min_cpu_utilization_rate = cpu_node_average
            count = count + 1
            time.sleep(60)
        self.base_object.log.info("Average CPU utilization rate: {}".format(average_cpu_utilization_rate))
        self.base_object.log.info("Max CPU utilization rate: {}".format(max_cpu_utilization_rate))
        self.base_object.log.info("Min CPU utilization rate: {}".format(min_cpu_utilization_rate))
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
            self.base_object.fail(
                "Currently only supports 25% percent per iteration, to achieve 100%, "
                "call 4 cycles with data_partition_value as 1,2,3 and 4")

    def scale_columnar_cluster(self, nodes, timeout=10000):
        start_time = time.time()
        status = None
        resp = self.base_object.columnarAPI.update_columnar_instance(self.base_object.tenant.id,
                                                                     self.base_object.tenant.project_id,
                                                                     self.base_object.cluster.instance_id,
                                                                     self.base_object.cluster.name, '', nodes)
        if resp.status_code != 202:
            self.base_object.fail("Failed to scale cluster")
            # check for nodes in the cluster, add a sleep here until node api is present
        time.sleep(10)
        while (status == "scaling" or status is None) and time.time() < start_time + timeout:
            self.base_object.log.info("Instance is still scaling after: {} seconds".format(time.time() - start_time))
            try:
                resp = self.base_object.columnarAPI.get_specific_columnar_instance(self.base_object.tenant.id,
                                                                                   self.base_object.tenant.project_id,
                                                                                   self.base_object.cluster.instance_id)
                resp = resp.json()
                status = resp["data"]["state"]
                time.sleep(20)
            except Exception as e:
                self.base_object.log.error(str(e))
        current_nodes = 0
        while current_nodes != nodes and time.time() < start_time + timeout:
            rest = ClusterRestAPI(self.base_object.cluster.master)
            status, content = rest.cluster_details()
            if not status:
                self.base_object.log.error("Error while fetching pools/default using "
                                           "connection string")

            current_nodes = len(content["nodes"])
            time.sleep(20)
        resp = self.base_object.columnarAPI.get_specific_columnar_instance(self.base_object.tenant.id,
                                                                           self.base_object.tenant.project_id,
                                                                           self.base_object.cluster.instance_id)
        resp = resp.json()
        status = resp["data"]["state"]
        if status != "healthy":
            return False
        return True

    def start_crud_on_data_sources(self, remote_start, remote_end, byok_start=None, byok_end=None):
        self.crud_jobs = Queue()
        results = []
        self.perform_crud = True
        self.crud_jobs.put((self.crud_on_remote_collections,
                            {"start": remote_start, "end": remote_end}))
        self.base_object.cbas_util.run_jobs_in_parallel(self.crud_jobs, results, 2, async_run=True)

    def stop_crud_on_data_sources(self):
        self.perform_crud = False
        self.crud_jobs.join()

    def stop_process(self, query_pass=False):
        self.base_object.run_queries = False
        self.base_object.get_cpu_stats = False
        self.cpu_stat_job.join()
        self.base_object.query_job.join()
        if query_pass and not all(self.query_work_results):
            self.base_object.log.error("Queries Failed")
            self.base_object.fail("Queries Failed")

    def run_processes(self, data_partition_number, scale_nodes, stop_all_at_end=True, query_pass=False, timeout=3600):
        # start phase-1 of on/off mini and start doc loading
        create_query_job = Queue()
        self.cpu_stat_job = Queue()
        results = []

        if not self.scale_columnar_cluster(8):
            self.base_object.fail("Failed to scale up the instance")

        # calculate doc to load for each cycle
        self.base_object.remote_start, self.base_object.remote_end = (
            self.create_doc_per_cycle(data_partition_number,
                                      self.base_object.remote_source_doc_per_collection))

        # complete load on standalone collection using query
        if data_partition_number == 1:
            self.load_doc_to_standalone_collection(self.base_object.data_loading_job)
            self.base_object.log.info("Loading doc to standalone collection using query")
            self.base_object.cbas_util.run_jobs_in_parallel(self.base_object.data_loading_job, results, thread_count=5,
                                                            async_run=False)

        # complete load on remote collections
        self.load_doc_to_remote_collection(self.base_object.data_loading_job, self.base_object.remote_start,
                                           self.base_object.remote_end)
        self.base_object.cbas_util.run_jobs_in_parallel(self.base_object.data_loading_job, results, thread_count=5,
                                                        async_run=True)

        # get cpu stats and run query on datasets until doc loading is complete
        self.base_object.run_queries = True
        self.base_object.get_cpu_stats = True
        self.query_work_results = []

        # Create a new queue for the query job and cpu stats to run in async mode
        create_query_job.put((self.run_queries_on_datasets, {"query_jobs": self.base_object.query_job}))
        self.cpu_stat_job.put((self.average_cpu_stats, {}))

        self.base_object.cbas_util.run_jobs_in_parallel(self.cpu_stat_job, results, 1, async_run=True)
        self.base_object.cbas_util.run_jobs_in_parallel(create_query_job, self.query_work_results, 2, async_run=True)
        while self.base_object.query_job.qsize() < 10:
            self.base_object.log.info("Waiting for query job to be created")
            time.sleep(10)
        self.base_object.cbas_util.run_jobs_in_parallel(self.base_object.query_job, self.query_work_results, 2,
                                                        async_run=True)
        self.base_object.cbas_util.run_jobs_in_parallel(self.cpu_stat_job, results, 1,
                                                        async_run=True)

        # wait for data loading to complete
        self.base_object.data_loading_job.join()
        self.base_object.cbas_util.wait_for_data_ingestion_in_the_collections(self.base_object.cluster)

        if not self.scale_columnar_cluster(scale_nodes):
            self.base_object.fail("Failed to scale up the instance")

        if stop_all_at_end:
            self.stop_process(query_pass)
