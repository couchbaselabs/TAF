import math
import random
import threading
import time
from queue import Queue

from cbas_utils.cbas_utils_columnar import ColumnarStats
from sirius_client_framework.sirius_constants import SiriusCodes

class MiniVolume:
    def __init__(self, base_object):
        self.query_work_results = None
        self.base_object = base_object
        self.query_stop_event = threading.Event()
        self.crud_stop_event = threading.Event()
        self.base_object.stop_event = threading.Event()
        self.doc_size = self.base_object.input.param("doc_size", 1024)
        self.base_object.data_loading_job = Queue()

    def tearDown(self):
        pass

    def calculate_volume_per_source(self, percentage_remote_volume=0.8, percentage_byok_volume=0.2):
        total_volume = self.base_object.input.param("total_volume", 1_000_000_000)
        num_standalone_collections = self.base_object.input.param("num_standalone_collections", 0)
        self.base_object.standalone_volume = 1e10 * num_standalone_collections

        self.base_object.log.info(
            f"Volume in standalone collection: {num_standalone_collections * 10} GB"
        )

        volume_left = total_volume - self.base_object.standalone_volume
        total_docs = math.ceil(volume_left / self.doc_size)

        self.base_object.remote_source_doc = int(math.ceil(total_docs * percentage_remote_volume))
        self.base_object.byok_source_doc = int(math.ceil(total_docs * percentage_byok_volume))
        self.base_object.remote_source_doc_per_collection = (
                self.base_object.remote_source_doc //
                self.base_object.input.param("num_remote_collections", 1)
        )
        self.base_object.byok_source_doc_per_collection = (self.base_object.byok_source_doc //
                                                           self.base_object.input.param("num_kafka_collections", 1))

        self.base_object.log.info(f"Total docs in remote source: {self.base_object.remote_source_doc}")
        self.base_object.log.info(
            f"Docs per remote collection: {self.base_object.remote_source_doc_per_collection}"
        )

    def load_doc_to_remote_collection(self, data_loading_job, start, end):
        if not hasattr(self.base_object, "remote_cluster") or not hasattr(self.base_object.remote_cluster, "buckets"):
            self.base_object.log.warning("No cluster/bucket found for remote collection")
            return

        for bucket in self.base_object.remote_cluster.buckets:
            if bucket.name != "_default":
                for scope in bucket.scopes:
                    if scope not in {"_system", "_mobile"}:
                        for collection in bucket.scopes[scope].collections:
                            data_loading_job.put((
                                self.remote_collections_operations,
                                {
                                    "bucket": bucket.name,
                                    "scope": scope,
                                    "collection": collection,
                                    "start": start,
                                    "end": end
                                }
                            ))

    def load_doc_to_kafka_source(self, data_loading_job, start, end):
        self.base_object.log.info(f"Loading docs in mongoDb collection "
                      f"{self.base_object.mongo_db_name}.{self.base_object.mongo_coll_name}")
        if hasattr(self.base_object, "mongo_db_name"):
            data_loading_job.put((self.base_object.mongo_util.load_docs_in_mongo_collection,
                                  {"database": self.base_object.mongo_db_name, "collection": self.base_object.mongo_coll_name,
                                   "start": start, "end": end, "doc_template": SiriusCodes.Templates.PRODUCT,
                                   "doc_size": self.doc_size, "sdk_batch_size": 1000}))

    def stop_crud_on_data_sources(self):
        self.crud_stop_event.set()

    def crud_on_remote_collections(self, start, end):
        deleted_items = []

        while not self.crud_stop_event.is_set():
            operation_start = random.randint(start, end)
            operation_end = operation_start + random.randint(1, 100)
            operation_end = min(operation_end, end)

            action = random.choices(["create", "upsert", "delete"], weights=[0.1, 0.8, 0.1], k=1)[0]

            if action == "create" and deleted_items:
                item = random.choice(deleted_items)
                start, end = item["start"], item["end"]
            elif action == "delete":
                deleted_items.append({"start": operation_start, "end": operation_end})

            if hasattr(self.base_object, "remote_cluster") and hasattr(self.base_object.remote_cluster, "buckets"):
                for bucket in self.base_object.remote_cluster.buckets:
                    if bucket.name != "_default":
                        for scope in bucket.scopes:
                            if scope not in {"_system", "_mobile"}:
                                for collection in bucket.scopes[scope].collections:
                                    self.remote_collections_operations(
                                        bucket.name, scope, collection, operation_start, operation_end, action
                                    )

            time.sleep(30)

        for item in deleted_items:
            start, end = item["start"], item["end"]
            for bucket in self.base_object.remote_cluster.buckets:
                if bucket.name != "_default":
                    for scope in bucket.scopes:
                        if scope not in {"_system", "_mobile"}:
                            for collection in bucket.scopes[scope].collections:
                                self.remote_collections_operations(bucket.name, scope, collection, start, end, "create")


    def remote_collections_operations(self, bucket, scope, collection, start, end, action="create"):
        try:
            loader = self.base_object.couchbase_doc_loader
            task = None

            if action == "create":
                task = loader.load_docs_in_couchbase_collection(
                    bucket, scope, collection, start, end, SiriusCodes.Templates.PRODUCT, self.doc_size, sdk_batch_size=1000
                )
            elif action == "delete":
                task = loader.delete_docs_from_couchbase_collection(
                    bucket, scope, collection, start, self.doc_size, sdk_batch_size=1000
                )
            elif action == "upsert":
                task = loader.update_docs_in_couchbase_collection(
                    bucket, scope, collection, start, end, SiriusCodes.Templates.PRODUCT, self.doc_size, sdk_batch_size=1000
                )

            if task and not task.result:
                self.base_object.log.error(f"Failed to {action} docs in {bucket}.{scope}.{collection}")

        except Exception as e:
            self.base_object.log.error(f"Error during {action} operation: {e}")

    def run_queries_on_datasets(self, results):
        datasets = self.base_object.cbas_util.get_all_dataset_objs()
        while not self.query_stop_event.is_set():
            dataset = random.choice(datasets)
            queries = [
                f"SELECT p.seller_name, p.seller_location, AVG(p.avg_rating) AS avg_rating FROM "
                f"(SELECT seller_name, seller_location, avg_rating FROM {dataset.full_name} "
                f"WHERE 'Passenger car medium' in product_category) p GROUP BY p.seller_name, "
                f"p.seller_location ORDER BY avg_rating DESC;",
                f"SELECT AVG(r.avg_rating) AS avg_rating, SUM(r.num_sold) AS num_sold, AVG(r.price) AS avg_price, "
                f"ARRAY_FLATTEN(ARRAY_AGG(r.product_category), 1) AS product_category, "
                f"AVG(r.product_rating.build_quality) AS avg_build_quality, SUM(r.quantity) AS total_quantity, "
                f"r.seller_verified, AVG(r.weight) AS avg_weight FROM {dataset.full_name} AS r "
                f"GROUP BY r.product_name, r.seller_verified",
                f"SET `compiler.external.field.pushdown` 'false'; SELECT COUNT(*) FROM {dataset.full_name}"
            ]

            query = random.choice(queries)
            with threading.Lock():
                status, metrics, errors, result, _, _ = self.base_object.cbas_util.execute_statement_on_cbas_util(
                    self.base_object.columnar_cluster, query, timeout=1800, analytics_timeout=1800)
                results.append(status)

    def load_doc_to_standalone_collection(self, data_loading_job):
        try:
            standalone_datasets = self.base_object.cbas_util.get_all_dataset_objs("standalone")
            if not standalone_datasets:
                self.base_object.log.warning("No standalone datasets found for loading.")
                return

            external_links = self.base_object.cbas_util.get_all_link_objs(link_type="s3")
            if not external_links:
                self.base_object.log.error("No external S3 links available.")
                return

            external_link = random.choice(external_links)
            standalone_collections_kafka = list()
            standalone_collections = list()
            for standalone_collection in self.base_object.cbas_util.get_all_dataset_objs(
                    "standalone"):
                if not standalone_collection.data_source:
                    standalone_collections.append(standalone_collection)
                elif standalone_collection.data_source in [
                    "MONGODB", "MYSQLDB", "POSTGRESQL"]:
                    standalone_collections_kafka.append(standalone_collection)
            for standalone_coll in standalone_collections:
                job_params = {
                    "cluster": self.base_object.columnar_cluster,
                    "collection_name": standalone_coll.name,
                    "aws_bucket_name": self.base_object.s3_source_bucket,
                    "external_link_name": external_link.name,
                    "dataverse_name": standalone_coll.dataverse_name,
                    "database_name": standalone_coll.database_name,
                    "files_to_include": "*.json",
                    "type_parsing_info": "",
                    "path_on_aws_bucket": ""
                }
                data_loading_job.put(
                    (self.base_object.cbas_util.copy_from_external_resource_into_standalone_collection, job_params))
        except Exception as e:
            self.base_object.log.error(f"Error while loading docs to standalone collections: {e}")

    def average_cpu_stats(self):
        lock = threading.Lock()
        average_cpu_utilization_rate = 0
        max_cpu_utilization_rate = 0
        min_cpu_utilization_rate = float('inf')
        count = 0

        while True:
            with lock:
                if not self.base_object.get_cpu_stats:
                    break

            columnar_stats = ColumnarStats()
            try:
                cpu_node_average = columnar_stats.cpu_utalization_rate(cluster=self.base_object.columnar_cluster)
                time.sleep(60)
            except Exception as e:
                self.base_object.log.error(f"Error fetching CPU utilization rate: {e}")
                continue

            average_cpu_utilization_rate = ((average_cpu_utilization_rate * count) + cpu_node_average) / (count + 1)
            max_cpu_utilization_rate = max(max_cpu_utilization_rate, cpu_node_average)
            min_cpu_utilization_rate = min(min_cpu_utilization_rate, cpu_node_average)

            count += 1
            time.sleep(60)

        self.base_object.log.info(f"Average CPU utilization rate: {average_cpu_utilization_rate}")
        self.base_object.log.info(f"Max CPU utilization rate: {max_cpu_utilization_rate}")
        self.base_object.log.info(f"Min CPU utilization rate: {min_cpu_utilization_rate}")

        return average_cpu_utilization_rate, max_cpu_utilization_rate, min_cpu_utilization_rate

    def create_doc_per_cycle(self, data_partition_value, no_of_docs_total):
        if 1 <= data_partition_value <= 4:
            start = (data_partition_value - 1) * (no_of_docs_total // 4)
            end = data_partition_value * (no_of_docs_total // 4)
            return start, end
        else:
            self.base_object.fail(
                "Currently only supports 25% per iteration. To achieve 100%, "
                "call 4 cycles with data_partition_value as 1, 2, 3, and 4."
            )

    def scale_columnar_instance(self, nodes, timeout=10000):
        try:
            if not self.base_object.columnar_utils.scale_instance(
                    self.base_object.pod, self.base_object.tenant,
                    self.base_object.tenant.project_id, self.base_object.columnar_cluster, nodes
            ):
                self.base_object.log.error(
                    f"Scale API failed while scaling instance from "
                    f"{len(self.base_object.columnar_cluster.nodes_in_cluster)} to {nodes}"
                )
                return False

            if not self.base_object.columnar_utils.wait_for_instance_scaling_operation(
                    self.base_object.pod, self.base_object.tenant, self.base_object.tenant.project_id,
                    self.base_object.columnar_cluster
            ):
                self.base_object.log.error(f"Instance scaling failed even after {timeout} seconds.")
                return False

            return True
        except Exception as e:
            self.base_object.log.error(f"Error during instance scaling: {e}")
            return False

    def stop_process(self, query_pass=False):
        self.query_stop_event.set()
        self.crud_stop_event.set()
        self.base_object.get_cpu_stats = False

        if query_pass and not all(self.query_work_results):
            self.base_object.log.error("Queries failed during execution.")
            self.base_object.fail("Query execution failed.")

    def run_processes(self, data_partition_number, scale_nodes, stop_all_at_end=True, query_pass=False):
        results = []
        cpu_stat_result = []
        query_job_queue = Queue()
        cpu_stat_job = Queue()

        self.query_stop_event.clear()
        self.crud_stop_event.clear()

        # Calculate doc range for the current cycle
        self.base_object.remote_start, self.base_object.remote_end = self.create_doc_per_cycle(
            data_partition_number, self.base_object.remote_source_doc_per_collection
        )

        self.base_object.byok_start, self.base_object.byok_end = (
            self.create_doc_per_cycle(data_partition_number, self.base_object.byok_source_doc_per_collection))


        self.load_doc_to_kafka_source(self.base_object.data_loading_job,self.base_object.byok_start, self.base_object.byok_end)
        # Load documents into remote collections
        self.load_doc_to_remote_collection(self.base_object.data_loading_job, self.base_object.remote_start,
                                           self.base_object.remote_end)
        self.base_object.cbas_util.run_jobs_in_parallel(self.base_object.data_loading_job, results, thread_count=5,
                                                        async_run=True)

        # Scale up the columnar instance
        if not self.scale_columnar_instance(8):
            self.base_object.fail("Failed to scale up the instance.")
        self.base_object.cbas_util.wait_for_cbas_to_recover(self.base_object.columnar_cluster, timeout=3600)

        # Load standalone collections if in the first cycle
        if data_partition_number == 1:
            self.load_doc_to_standalone_collection(self.base_object.data_loading_job)
            self.base_object.log.info("Loading docs to standalone collections.")
            self.base_object.cbas_util.run_jobs_in_parallel(self.base_object.data_loading_job, results, thread_count=5,
                                                            async_run=False)

        # Start CPU stats collection and query execution
        self.base_object.get_cpu_stats = True
        self.query_work_results = []

        query_job_queue.put((self.run_queries_on_datasets,
                              {"results": self.query_work_results}))
        query_job_queue.put((self.run_queries_on_datasets,
                             {"results": self.query_work_results}))
        query_job_queue.put((self.run_queries_on_datasets,
                             {"results": self.query_work_results}))
        cpu_stat_job.put((self.average_cpu_stats, {}))

        self.base_object.cbas_util.run_jobs_in_parallel(query_job_queue, results, thread_count=3,
                                                        async_run=True)
        self.base_object.cbas_util.run_jobs_in_parallel(cpu_stat_job, cpu_stat_result, thread_count=1,
                                                        async_run=True)

        # Wait for data loading to complete
        self.base_object.data_loading_job.join()
        self.base_object.cbas_util.wait_for_data_ingestion_in_the_collections(self.base_object.columnar_cluster)

        # Scale instance to a random node count after processing
        if scale_nodes == 8:
            scale_nodes = random.choice([2, 4, 16])
        if not self.scale_columnar_instance(scale_nodes):
            self.base_object.fail("Failed to scale instance.")
        self.base_object.cbas_util.wait_for_cbas_to_recover(self.base_object.columnar_cluster, timeout=3600)

        if stop_all_at_end:
            self.stop_process(query_pass)