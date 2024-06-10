import math
import random
import time
from queue import Queue

from cbas_utils.cbas_utils_columnar import ColumnarStats
from sirius_client_framework.multiple_database_config import CouchbaseLoader
from sirius_client_framework.operation_config import WorkloadOperationConfig
from sirius_client_framework.sirius_constants import SiriusCodes
from CbasLib.cbas_entity_columnar import Remote_Dataset, Standalone_Dataset, External_Dataset
from Jython_tasks.sirius_task import WorkLoadTask


class MiniVolume:
    def __init__(self, base_object, sirius_url="http://127.0.0.1:4000"):
        self.crud_jobs = Queue()
        self.perform_crud = None
        self.query_work_results = None
        self.cpu_stat_job = None
        self.base_object = base_object
        self.base_object.sirius_base_url = sirius_url
        self.base_object.query_job = Queue()
        self.base_object.data_loading_job = Queue()

    def calculate_volume_per_source(self, percentage_remote_volume=0.5, percentage_standalone_volume=0.2,
                                    percentage_byok_volume=0.2, percentage_s3_volume=0.1):
        doc_size = self.base_object.input.param("doc_size", 1024)
        total_doc = math.ceil(self.base_object.input.param("total_volume", 1000000000) / doc_size)
        self.base_object.remote_source_doc = total_doc // int(1 / percentage_remote_volume)
        self.base_object.standalone_source_doc = total_doc // int(1 / percentage_standalone_volume)
        self.base_object.byok_source_doc = total_doc // int(1 / percentage_byok_volume)
        self.base_object.s3_volume = total_doc // int(1 / percentage_s3_volume)
        self.base_object.remote_source_doc_per_collection = (self.base_object.remote_source_doc //
                                                             (self.base_object.input.param("no_of_remote_coll", 1) *
                                                              self.base_object.input.param("no_of_remote_bucket", 1)))
        self.base_object.standalone_doc_per_collection = (self.base_object.standalone_source_doc //
                                                          self.base_object.input.param("num_of_standalone_coll", 1))

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
            actions_supported = ["insert", "upsert", "delete"]
            actions_weights = [0.1, 0.8, 0.1]
            action = random.choices(actions_supported, weights=actions_weights, k=1)

            if action == "insert":
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
                    if bucket.name == "_default":
                        continue
                    for scope in bucket.scopes:
                        if scope != "_system" and scope != "_mobile":
                            continue
                        for collection in bucket.scopes[scope].collections:
                            self.remote_collections_operations(bucket.name, scope, collection, start, end,
                                                               action)

        for item in deleted_items:
            start = item["start"]
            end = item["end"]
            action = "insert"
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
                if bucket.name == "_default":
                    continue
                for scope in bucket.scopes:
                    if scope != "_system" and scope != "_mobile":
                        continue
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
                                               bucket=bucket, scope=scope, collection=collection, sdk_batch_size=1000)
        operation_config = WorkloadOperationConfig(start=int(start), end=int(end), template="hotel",
                                                   doc_size=int(self.base_object.doc_size))

        required_action = None
        if action == "create":
            required_action = SiriusCodes.DocOps.BULK_CREATE
        if action == "delete":
            required_action = SiriusCodes.DocOps.BULK_DELETE
        if action == "upsert":
            required_action = SiriusCodes.DocOps.BULK_UPDATE
        task_insert = WorkLoadTask(task_manager=self.base_object.task_manager, op_type=required_action,
                                   database_information=database_information, operation_config=operation_config,
                                   default_sirius_base_url=self.base_object.sirius_base_url)
        self.base_object.task_manager.add_new_task(task_insert)
        self.base_object.task_manager.get_task_result(task_insert)
        return

    def run_queries_on_datasets(self, query_jobs):
        self.base_object.cbas_util.get_all_dataset_objs()
        queries = ["SELECT p.seller_name, p.seller_location, AVG(p.avg_rating) AS avg_rating FROM (SELECT seller_name, "
                   "seller_location, avg_rating FROM {} WHERE 'Passenger car medium' in product_category) p "
                   "GROUP BY p.seller_name, p.seller_location ORDER BY avg_rating DESC;",
                   "SELECT s.seller_name, p.product_name, p.avg_rating, CASE WHEN p.avg_rating = s.max_rating THEN "
                   "'Highest' WHEN p.avg_rating = s.min_rating THEN 'Lowest' END AS rating_type FROM TestCollection p "
                   "JOIN ( SELECT seller_name, MAX(avg_rating) OVER (PARTITION BY seller_name) AS max_rating, "
                   "MIN(avg_rating) OVER (PARTITION BY seller_name) AS min_rating FROM TestCollection) s ON "
                   "p.seller_name = s.seller_name WHERE p.avg_rating IN (s.max_rating, s.min_rating) "
                   "ORDER BY s.seller_name, rating_type;",
                   "SELECT c.category, p.product_name, p.num_sold FROM TestCollection p JOIN ( SELECT product_category,"
                   "MAX(num_sold) AS max_sold FROM TestCollection GROUP BY product_category) c ON "
                   "p.product_category = c.product_category AND p.num_sold = c.max_sold GROUP BY c.category, "
                   "p.product_name, p.num_sold ORDER BY c.category, p.num_sold DESC;"
                   ]
        datasets = self.base_object.cbas_util.get_all_dataset_objs()
        while self.base_object.run_queries:
            if query_jobs.qsize() < 5:
                dataset = random.choice(datasets)
                query = random.choice(queries).format(dataset.full_name)
                query_jobs.put((self.base_object.cbas_util.execute_statement_on_cbas_util,
                                {"cluster": self.base_object.cluster, "statement": query, "analytics_timeout": 100000}))

    def load_doc_to_standalone_collection(self, data_loading_job, start, end):
        standalone_datasets = self.base_object.cbas_util.get_all_dataset_objs("standalone")
        # start data load on standalone collections
        for dataset in standalone_datasets:
            data_loading_job.put((self.base_object.cbas_util.doc_operations_standalone_collection_sirius,
                                  {"collection_name": dataset.name, "dataverse_name": dataset.dataverse_name,
                                   "database_name": dataset.database_name,
                                   "connection_string": "couchbases://" + self.base_object.cluster.srv,
                                   "start": start, "end": end, "doc_size": self.base_object.doc_size,
                                   "username": self.base_object.cluster.servers[0].rest_username,
                                   "password": self.base_object.cluster.servers[0].rest_password}))

    def average_cpu_stats(self):
        average_cpu_utilization_rate = 0
        max_cpu_utilization_rate = 0
        min_cpu_utilization_rate = 0
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
            time.sleep(20)
        self.base_object.log.info("Average CPU utilization rate: {}".format(average_cpu_utilization_rate))
        self.base_object.log.info("Max CPU utilization rate: {}".format(max_cpu_utilization_rate))
        self.base_object.log.info("Min CPU utilization rate: {}".format(min_cpu_utilization_rate))
        return average_cpu_utilization_rate, max_cpu_utilization_rate, min_cpu_utilization_rate

    def wait_for_data_ingestion_in_the_collections(self, remote_docs, standalone_docs, external_docs=7920000,
                                                   timeout=900):
        datasets = self.base_object.cbas_util.get_all_dataset_objs()
        start_time = time.time()
        while len(datasets) != 0 and time.time() < start_time + timeout:
            for dataset in datasets:
                doc_count = self.base_object.cbas_util.get_num_items_in_cbas_dataset(self.base_object.cluster,
                                                                                     dataset.full_name)
                if isinstance(dataset, Remote_Dataset):
                    if doc_count == remote_docs:
                        self.base_object.log.info("Loading docs complete in collection: {0}".format(dataset.full_name))
                        datasets.remove(dataset)
                if isinstance(dataset, Standalone_Dataset):
                    if not dataset.data_source:
                        if doc_count == standalone_docs:
                            self.base_object.log.info(
                                "Loading docs complete in collection: {0}".format(dataset.full_name))
                            datasets.remove(dataset)
                    if dataset.data_source == "s3":
                        datasets.remove(dataset)
                        continue
                if isinstance(dataset, External_Dataset):
                    datasets.remove(dataset)
        for dataset in datasets:
            doc_count = self.base_object.cbas_util.get_num_items_in_cbas_dataset(self.base_object.cluster,
                                                                                 dataset.full_name)
            if isinstance(dataset, Remote_Dataset):
                self.base_object.log.error("Timeout while loading doc to collection {}".format(dataset.full_name))
                self.base_object.log.error("Expected: {}, Actual: {}".format(remote_docs, doc_count))
            if isinstance(dataset, Standalone_Dataset):
                if not dataset.data_source:
                    self.base_object.log.error("Timeout while loading doc to collection {}".format(dataset.full_name))
                    self.base_object.log.error("Expected: {}, Actual: {}".format(standalone_docs, doc_count))
                if dataset.data_source == "s3":
                    self.base_object.log.error("Timeout while loading doc to collection {}".format(dataset.full_name))
                    self.base_object.log.error("Expected: {}, Actual: {}".format(external_docs, doc_count))

    def create_doc_per_cycle(self, data_partition_value, no_of_docs_total):
        if data_partition_value == 1:
            return 1, no_of_docs_total // 4
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

    def scale_columnar_cluster(self, nodes, timeout=900):
        start_time = time.time()
        status = None
        resp = self.base_object.columnarAPI.update_columnar_instance(self.base_object.tenant.id,
                                                                     self.base_object.tenant.project_id,
                                                                     self.base_object.cluster.instance_id,
                                                                     self.base_object.cluster.name, '', nodes)
        if resp.status_code != 202:
            self.base_object.fail("Failed to scale cluster")
            # check for nodes in the cluster
        while status != "healthy" and start_time + 900 > time.time():
            resp = self.base_object.columnarAPI.get_specific_columnar_instance(self.base_object.tenant.id,
                                                                               self.base_object.tenant.project_id,
                                                                               self.base_object.cluster.instance_id)
            resp = resp.json()
            status = resp["data"]["state"]
        if time.time() > start_time + timeout:
            self.base_object.log.error("Cluster state is {} after 15 minutes".format(status))

        start_time = time.time()
        while start_time + timeout > time.time():
            nodes_in_cluster = self.base_object.capellaAPI.get_nodes(self.base_object.tenant.id,
                                                                     self.base_object.tenant.project_id,
                                                                     self.base_object.cluster.cluster_id)
            if nodes_in_cluster.status_code == 200:
                if len(nodes_in_cluster.json()["data"]) == nodes:
                    return True
        return False

    def start_crud_on_data_sources(self, remote_start, remote_end, byok_start=None, byok_end=None):
        self.crud_jobs = Queue()
        results = []
        self.perform_crud = True
        self.crud_jobs.put((self.crud_on_remote_collections,
                            {"remote_start": remote_start, "remote_end": remote_end}))
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

    def run_processes(self, data_partition_number, scale_nodes, stop_all_at_end=True, query_pass=False, timeout=900):
        # start phase-1 of on/off mini and start doc loading
        create_query_job = Queue()
        self.cpu_stat_job = Queue()
        results = []

        self.base_object.log.info("Running stage 1")

        # calculate doc to load for each cycle
        self.base_object.standalone_start, self.base_object.standalone_end = (
            self.create_doc_per_cycle(data_partition_number, self.base_object.standalone_doc_per_collection))
        self.load_doc_to_remote_collection(self.base_object.data_loading_job, self.base_object.remote_start,
                                           self.base_object.remote_end)
        self.base_object.remote_start, self.base_object.remote_end = (
            self.create_doc_per_cycle(data_partition_number,
                                      self.base_object.remote_source_doc_per_collection))

        # complete load on standalone collection using query
        self.load_doc_to_standalone_collection(self.base_object.data_loading_job, self.base_object.standalone_start,
                                               self.base_object.standalone_end)
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
        self.base_object.cbas_util.run_jobs_in_parallel(create_query_job, self.query_work_results, 1, async_run=True)
        while self.base_object.query_job.qsize() < 5:
            self.base_object.log.info("Waiting for query job to be created")
            time.sleep(10)
        self.base_object.cbas_util.run_jobs_in_parallel(self.base_object.query_job, self.query_work_results, 2,
                                                        async_run=True)
        self.base_object.cbas_util.run_jobs_in_parallel(self.cpu_stat_job, results, 1,
                                                        async_run=True)

        # wait for data loading to complete
        self.base_object.data_loading_job.join()
        self.wait_for_data_ingestion_in_the_collections(self.base_object.remote_end - 1,
                                                        self.base_object.standalone_end - 1, timeout=timeout)

        if not self.scale_columnar_cluster(scale_nodes):
            self.base_object.fail("Failed to scale up the instance")

        if stop_all_at_end:
            self.stop_process(query_pass)
