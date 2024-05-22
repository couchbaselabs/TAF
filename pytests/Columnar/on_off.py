import time
import json
import random
import math
from Queue import Queue

from Columnar.columnar_base import ColumnarBaseTest
from capellaAPI.capella.columnar.CapellaAPI import CapellaAPI as ColumnarAPI
from capellaAPI.capella.common.CapellaAPI import CommonCapellaAPI
from capellaAPI.capella.dedicated.CapellaAPI_v4 import CapellaAPI
from couchbase_utils.capella_utils.dedicated import CapellaUtils
from BucketLib.bucket import Bucket
from sirius_client_framework.multiple_database_config import CouchbaseLoader
from sirius_client_framework.operation_config import WorkloadOperationConfig
from Jython_tasks.sirius_task import WorkLoadTask
from Jython_tasks.task_manager import TaskManager
from sirius_client_framework.sirius_constants import SiriusCodes
from CbasLib.cbas_entity_columnar import Remote_Dataset, Standalone_Dataset, External_Dataset
from cbas_utils.cbas_utils_columnar import ColumnarStats

class OnOff(ColumnarBaseTest):
    def setUp(self):
        super(OnOff, self).setUp()
        self.cluster = self.tenant.columnar_instances[0]
        self.columnarAPI = ColumnarAPI(self.pod.url_public, '', '', self.tenant.user,
                                       self.tenant.pwd, '')
        self.doc_size = self.input.param("doc_size", 1000)

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
        if not self.cbas_util.delete_cbas_infra_created_from_spec(
                self.cluster, self.columnar_spec):
            self.fail("Error while deleting cbas entities")

        # super(ColumnarBaseTest, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished", stage="Teardown")

    def wait_for_off(self, timeout=900):
        status = None
        start_time = time.time()
        while (status == 'turning_off' or not status) and time.time() < start_time + timeout:
            resp = self.columnar_utils.get_instance_info(self.pod, self.tenant, self.tenant.project_id,
                                                         self.cluster.instance_id)
            status = resp["data"]["state"]
        if status == "turned_off":
            self.log.info("Instance off successful")
            return True
        else:
            self.log.error("Failed to turn off the instance")
            return False
    def wait_for_on(self, timeout=900):
        status = None
        start_time = time.time()
        while (status == 'turning_on' or not status) and time.time() < start_time + timeout:
            resp = self.columnar_utils.get_instance_info(self.pod, self.tenant, self.tenant.project_id,
                                                         self.cluster.instance_id)
            status = resp["data"]["state"]
        if status == "healthy":
            self.log.info("Instance on successful")
            return True
        else:
            self.log.error("Failed to turn on the instance")
            return False

    def test_manual_on_off(self):
        resp = self.columnarAPI.turn_off_instance(self.tenant.id, self.tenant.project_id, self.cluster.instance_id)
        if resp.status_code == 202:
            self.log.info("Started turning off instance")
        else:
            self.fail("API failed to turn off the instance with status code: {}".format(resp.status_code))
        if not self.wait_for_off():
            self.fail("Failed to turn off instance")

        # resume the instance
        resp = self.columnarAPI.turn_on_instance(self.tenant.id, self.tenant.project_id, self.cluster.instance_id)
        if resp.status_code == 202:
            self.log.info("Started turning on instance")
        else:
            self.fail("API Failed to turn on instance with status code : {}".format(resp.status_code))
        if not self.wait_for_on():
            self.fail("Failed to turn on the instance")

    def base_infra_setup(self):
        self.columnar_spec["dataverse"]["no_of_dataverses"] = self.input.param(
            "no_of_scopes", 1)
        self.columnar_spec["remote_link"]["no_of_remote_links"] = self.input.param(
            "no_of_remote_links", 0)

        self.aws_region = self.input.param("aws_region", "us-west-1")
        self.aws_access_key = self.input.param("aws_access_key")
        self.aws_secret_key = self.input.param("aws_secret_key")

        if self.input.param("no_of_remote_links", 1):
            remote_link_properties = list()
            remote_link_properties.append(
                {"type": "couchbase", "hostname": str(self.remote_cluster.srv),
                 "username": self.remote_cluster.username,
                 "password": self.remote_cluster.password,
                 "encryption": "full",
                 "certificate": self.remote_cluster_certificate})
            self.columnar_spec["remote_link"]["properties"] = remote_link_properties
            self.columnar_spec["remote_dataset"]["num_of_remote_datasets"] = self.input.param("num_of_remote_coll", 0)

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

        self.columnar_spec["standalone_dataset"][
            "num_of_standalone_coll"] = self.input.param(
            "num_of_standalone_coll", 0)
        self.columnar_spec["standalone_dataset"]["primary_key"] = [{"name": "string", "email": "string"}]

        self.columnar_spec["external_dataset"]["num_of_external_datasets"] = self.input.param("num_of_external_coll", 0)
        if self.input.param("num_of_external_coll", 0):
            external_dataset_properties = [{
                "external_container_name": self.input.param("s3_source_bucket", None),
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
        if not hasattr(self, "remote_cluster"):
            remote_cluster = None
        else:
            remote_cluster = [self.remote_cluster]
        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            self.cluster, self.columnar_spec, self.bucket_util, False, remote_clusters=remote_cluster)
        if not result:
            self.fail(msg)

    def remote_source_setup(self):
        no_of_remote_buckets = self.input.param("no_of_remote_bucket", 1)
        for key in self.cb_clusters:
            self.remote_cluster = self.cb_clusters[key]
            break
        resp = (self.capellaAPI.create_control_plane_api_key(self.tenant.id, 'init api keys')).json()
        self.capellaAPI.cluster_ops_apis.SECRET = resp['secretKey']
        self.capellaAPI.cluster_ops_apis.ACCESS = resp['id']
        self.capellaAPI.cluster_ops_apis.bearer_token = resp['token']
        self.capellaAPI.org_ops_apis.SECRET = resp['secretKey']
        self.capellaAPI.org_ops_apis.ACCESS = resp['id']
        self.capellaAPI.org_ops_apis.bearer_token = resp['token']
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

        # creating bucket scope and collection to pump data
        for i in range(no_of_remote_buckets):
            bucket_name = self.cbas_util.generate_name()
            resp = self.capellaAPI.cluster_ops_apis.create_bucket(self.tenant.id,
                                                                  self.tenant.project_id,
                                                                  self.remote_cluster.id,
                                                                  bucket_name, "couchbase", "couchstore", 300, "seqno",
                                                                  "majorityAndPersistActive", 1, True, 1000000)
            if resp.status_code == 201:
                self.bucket_id = resp.json()["id"]
                self.log.info("Bucket created successfully")
            else:
                self.fail("Error creating bucket in remote_cluster")

        buckets = json.loads(CapellaUtils.get_all_buckets(self.pod, self.tenant, self.remote_cluster)
                             .content)["buckets"]["data"]

        for bucket in buckets:
            bucket = bucket["data"]
            bucket_obj = Bucket({
                Bucket.name: bucket["name"],
                Bucket.ramQuotaMB: bucket["memoryAllocationInMb"],
                Bucket.replicaNumber: bucket["replicas"],
                Bucket.conflictResolutionType:
                    bucket["bucketConflictResolution"],
                Bucket.flushEnabled: bucket["flush"],
                Bucket.durabilityMinLevel: bucket["durabilityLevel"],
                Bucket.maxTTL: bucket["timeToLive"],
            })
            bucket_obj.uuid = bucket["id"]
            bucket_obj.stats.itemCount = bucket["stats"]["itemCount"]
            bucket_obj.stats.memUsed = bucket["stats"]["memoryUsedInMib"]
            self.remote_cluster.buckets.append(bucket_obj)

        resp = self.capellaAPI.cluster_ops_apis.get_cluster_certificate(self.tenant.id,
                                                                        self.tenant.project_id,
                                                                        self.remote_cluster.id)
        if resp.status_code == 200:
            self.remote_cluster_certificate = (resp.json())["certificate"]
        else:
            self.fail("Failed to get cluster certificate")

    def load_doc_to_standalone_collection(self, data_loading_job, start, end):
        standalone_datasets = self.cbas_util.get_all_dataset_objs("standalone")
        # start data load on standalone collections
        for dataset in standalone_datasets:
            data_loading_job.put((self.cbas_util.load_doc_to_standalone_collection_sirius,
                                  {"collection_name": dataset.name, "dataverse_name": dataset.dataverse_name,
                                   "database_name": dataset.database_name,
                                   "connection_string": "couchbases://" + self.cluster.srv,
                                   "start": start, "end": end, "doc_size": self.doc_size,
                                   "username": self.cluster.servers[0].rest_username,
                                   "password": self.cluster.servers[0].rest_password}))

    def load_doc_to_remote_collection(self, data_loading_job, start, end):
        for bucket in self.remote_cluster.buckets:
            if bucket.name != "_default":
                for scope in bucket.scopes:
                    if scope != "_system" and scope != "_mobile":
                        for collection in bucket.scopes[scope].collections:
                            data_loading_job.put((self.load_doc_to_remote_collections,
                                                    {"bucket": bucket.name, "scope": scope, "collection": collection,
                                                    "start": start, "end": end}))

    def load_doc_to_remote_collections(self, bucket, scope, collection, start, end):
        database_information = CouchbaseLoader(username= self.remote_cluster.username, password=self.remote_cluster.password,
                                               connection_string="couchbases://"  +self.remote_cluster.srv,
                                               bucket=bucket, scope=scope, collection=collection, sdk_batch_size=10)
        operation_config = WorkloadOperationConfig(start=int(start), end=int(end), template="hotel", doc_size=int(self.doc_size))
        task_insert = WorkLoadTask(task_manager=self.task_manager, op_type=SiriusCodes.DocOps.CREATE,
                                   database_information=database_information, operation_config=operation_config,
                                   default_sirius_base_url=self.sirius_base_url)
        self.task_manager.add_new_task(task_insert)
        self.task_manager.get_task_result(task_insert)
        return

    def calculate_volume_per_source(self):
        doc_size = self.input.param("doc_size", 1024)
        total_doc = math.ceil(self.input.param("total_volume", 1000000000) / doc_size)
        self.remote_source_doc = total_doc // 2
        self.standalone_source_doc = total_doc // 5
        self.byok_source_doc = total_doc // 5
        self.s3_volume = total_doc // 10
        self.remote_source_doc_per_collection = (self.remote_source_doc //
                                                 (self.input.param("no_of_remote_coll", 1) *
                                                  self.input.param("no_of_remote_bucket", 1)))
        self.standalone_doc_per_collection = self.standalone_source_doc // self.input.param("num_of_standalone_coll", 1)

    def run_queries_on_datasets(self, query_jobs):
        self.cbas_util.get_all_dataset_objs()
        queries = ["SELECT name, AVG(review.rating.rating_value) AS avg_rating FROM {} AS d "
                   "UNNEST d.reviews AS review GROUP BY d.name;",
                   "SELECT name, COUNT(review) AS review_count FROM {} AS d "
                   "UNNEST d.reviews AS review GROUP BY d.name;",
                   "SELECT name, AVG(review.rating.checkin) AS avg_checkin_rating FROM mjObARKxQxO7tt5ZOn AS d "
                   "UNNEST d.reviews AS review WHERE review.rating.checkin IS NOT MISSING GROUP BY d.name;"
                   ]
        datasets = self.cbas_util.get_all_dataset_objs()
        while self.run_queries:
            if query_jobs.qsize() < 5:
                dataset = random.choice(datasets)
                query = random.choice(queries).format(dataset.full_name)
                query_jobs.put((self.cbas_util.execute_statement_on_cbas_util,
                                {"cluster": self.cluster, "statement": query, "analytics_timeout": 100000}))

    def initiate_on_off(self):
        # start turning off the columnar instance
        resp = self.columnarAPI.turn_off_instance(self.tenant.id, self.tenant.project_id, self.cluster.instance_id)
        if resp.status_code == 202:
            self.log.info("Started turning off instance")
        else:
            self.fail("API failed to turn off the instance with status code: {}".format(resp.status_code))
        if not self.wait_for_off(timeout=1800):
            self.fail("Failed to turn off instance")

        # start turning on the columnar instance
        resp = self.columnarAPI.turn_on_instance(self.tenant.id, self.tenant.project_id, self.cluster.instance_id)
        if resp.status_code == 202:
            self.log.info("Started turning on instance")
        else:
            self.fail("API Failed to turn on instance with status code : {}".format(resp.status_code))
        if not self.wait_for_on():
            self.fail("Failed to turn on the instance")

    def scale_columnar_cluster(self, nodes):
        start_time = time.time()
        status = None
        resp = self.columnarAPI.update_columnar_instance(self.tenant.id, self.tenant.project_id, self.cluster.instance_id,
                                                  self.cluster.name, '', nodes)
        if resp.status_code != 202:
            self.fail("Failed to scale cluster")
        time.sleep(5)
        while status != "healthy" and start_time + 900 > time.time():
            resp = self.commonAPI.get_cluster_info_internal(self.cluster.cluster_id).json()
            status = resp["meta"]["status"]["state"]
            if status == "healthy":
                return True
        if time.time() > start_time + 900:
            self.log.error("Cluster state is {} after 15 minutes".format(status))
        return False

    def wait_for_data_ingestion_in_the_collections(self, remote_docs, standalone_docs, external_docs=7920000, timeout=900):
        datasets = self.cbas_util.get_all_dataset_objs()
        start_time = time.time()
        while len(datasets) != 0 and time.time() < start_time + timeout:
            for dataset in datasets:
                doc_count = self.cbas_util.get_num_items_in_cbas_dataset(self.cluster, dataset.full_name)
                if type(dataset) == Remote_Dataset:
                    if doc_count == remote_docs:
                        self.log.info("Loading docs complete in collection: {0}".format(dataset.full_name))
                        datasets.remove(dataset)
                if type(dataset) == Standalone_Dataset:
                    if not dataset.data_source:
                        if doc_count == standalone_docs:
                            self.log.info("Loading docs complete in collection: {0}".format(dataset.full_name))
                            datasets.remove(dataset)
                    if dataset.data_source == "s3":
                        continue
        for dataset in datasets:
            doc_count = self.cbas_util.get_num_items_in_cbas_dataset(self.cluster, dataset.full_name)
            if type(dataset) == Remote_Dataset:
                self.log.error("Timeout while loading doc to collection {}".format(dataset.full_name))
                self.log.error("Expected: {}, Actual: {}".format(remote_docs, doc_count))
            if type(dataset) == Standalone_Dataset:
                if not dataset.data_source:
                    self.log.error("Timeout while loading doc to collection {}".format(dataset.full_name))
                    self.log.error("Expected: {}, Actual: {}".format(standalone_docs, doc_count))
                if dataset.data_source == "s3":
                    self.log.error("Timeout while loading doc to collection {}".format(dataset.full_name))
                    self.log.error("Expected: {}, Actual: {}".format(external_docs, doc_count))

    def average_cpu_stats(self):
        average_cpu_utilization_rate = 0
        max_cpu_utilization_rate = 0
        min_cpu_utilization_rate = 0
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
            time.sleep(20)
        self.log.info("Average CPU utilization rate: {}".format(average_cpu_utilization_rate))
        self.log.info("Max CPU utilization rate: {}".format(max_cpu_utilization_rate))
        self.log.info("Min CPU utilization rate: {}".format(min_cpu_utilization_rate))
        return average_cpu_utilization_rate, max_cpu_utilization_rate, min_cpu_utilization_rate

    def doc_in_each_dataset(self):
        docs_per_dataset = {}
        datasets = self.cbas_util.get_all_dataset_objs()
        for dataset in datasets:
            no_of_docs = self.cbas_util.get_num_items_in_cbas_dataset(dataset.full_name)
            docs_per_dataset[dataset.full_name] = no_of_docs
        return docs_per_dataset

    def test_mini_volume_on_off(self):
        self.sirius_base_url = "http://127.0.0.1:4000"
        self.capellaAPI = CapellaAPI(self.pod.url_public, '', '', self.tenant.user,
                                     self.tenant.pwd, '')
        self.commonAPI = CommonCapellaAPI(self.pod.url_public, '', '', self.tenant.user,
                                          self.tenant.pwd, self.input.param("internal_support_token"))

        # setup all the infra
        self.remote_source_setup()
        self.base_infra_setup()
        self.query_job = Queue()
        self.data_loading_job = Queue()
        self.copy_to_kv_job = Queue()
        create_query_job = Queue()
        cpu_stat_job = Queue()
        results = []

        # calculate docs per collection based on total volume needed for test
        self.calculate_volume_per_source()

        # start phase-1 of on/off mini and start doc loading
        self.log.info("Running stage 1")
        self.load_doc_to_standalone_collection(self.data_loading_job, 1,  self.standalone_doc_per_collection//4)
        self.load_doc_to_remote_collection(self.data_loading_job, 1, self.remote_source_doc_per_collection//4)
        self.cbas_util.run_jobs_in_parallel(self.data_loading_job, results, thread_count=5, async_run=True)

        # get cpu stats and run query on datasets until doc loading is complete
        self.run_queries = True
        self.get_cpu_stats = True
        query_work_results = []

        # Create a new queue for the query job and cpu stats to run in async mode
        create_query_job.put((self.run_queries_on_datasets, {"query_jobs": self.query_job}))
        cpu_stat_job.put((self.average_cpu_stats, {}))

        self.cbas_util.run_jobs_in_parallel(cpu_stat_job, results, 1, async_run=True)
        self.cbas_util.run_jobs_in_parallel(create_query_job, query_work_results, 1, async_run=True)
        while self.query_job.qsize() < 5:
            self.log.info("Waiting for query job to be created")
            time.sleep(10)
        self.cbas_util.run_jobs_in_parallel(self.query_job, query_work_results, 3, async_run=True)

        # wait for data loading to complete
        self.data_loading_job.join()
        self.wait_for_data_ingestion_in_the_collections((self.remote_source_doc_per_collection//4) - 1,
                                                        (self.standalone_doc_per_collection//4) - 1)

        # stop generating new queries
        self.run_queries = False

        # initiating scaling
        if not self.scale_columnar_cluster(32):
            self.fail("Failed to scale up the instance")

        # stop polling for cpy stats and wait for all the queries to complete
        self.get_cpu_stats = False
        self.query_job.join()

        # data in datasets before on and off
        docs_in_collections_before = self.doc_in_each_dataset()
        # initiate on and off on the instance
        self.initiate_on_off()

        docs_in_collection_after = self.doc_in_each_dataset()
        if docs_in_collection_after != docs_in_collections_before:
            self.fail("Doc count mismatch after on/off")

        self.log.info("Starting stage 2")
        # load data in sources for stage 2
        self.load_doc_to_remote_collection(self.data_loading_job,  self.remote_source_doc_per_collection // 4,
                                           (self.remote_source_doc_per_collection * 2) // 4)

        self.load_doc_to_standalone_collection(self.data_loading_job, self.standalone_doc_per_collection // 4,
                                               (self.standalone_doc_per_collection * 2) // 4)
        self.cbas_util.run_jobs_in_parallel(self.data_loading_job, results, thread_count=5, async_run=True)

        self.run_queries = True
        self.get_cpu_stats = True

        # Create a new queue for the query job
        create_query_job.put((self.run_queries_on_datasets, {"query_jobs": self.query_job}))
        cpu_stat_job.put((self.average_cpu_stats,{}))

        self.cbas_util.run_jobs_in_parallel(cpu_stat_job, results, 1, async_run=True)
        self.cbas_util.run_jobs_in_parallel(create_query_job, query_work_results, 1, async_run=True)
        while self.query_job.qsize() < 5:
            self.log.info("Waiting for query job to be created")
            time.sleep(10)
        self.cbas_util.run_jobs_in_parallel(self.query_job, query_work_results, 3, async_run=True)

        # wait for data load to complete
        self.data_loading_job.join()
        self.wait_for_data_ingestion_in_the_collections(((self.remote_source_doc_per_collection * 2) // 4) - 1,
                                                        ((self.standalone_doc_per_collection * 2) // 4) - 1)

        # disconnecting all the links
        link_objects = self.cbas_util.get_all_link_objs()
        for link in link_objects:
            if not self.cbas_util.disconnect_link(self.cluster, link.full_name):
                self.fail("Failed to disconnect link")

        # load data in sources for stage 3 in remote collection and BYOK when on/off is going on
        self.load_doc_to_remote_collection(self.data_loading_job, self.remote_source_doc_per_collection // 2,
                                           (self.standalone_doc_per_collection * 3) // 4)
        self.cbas_util.run_jobs_in_parallel(self.data_loading_job, results, thread_count=5, async_run=True)

        # stop generating new queries, wait for queries to complete and stop polling cpu stats
        self.run_queries = False
        self.get_cpu_stats = False
        self.query_job.join()

        # get count all the dataset and initiate on/off
        docs_in_collections_before = self.doc_in_each_dataset()
        self.initiate_on_off()

        # validate doc count doesn't change after on/off
        docs_in_collection_after = self.doc_in_each_dataset()
        if docs_in_collection_after != docs_in_collections_before:
            self.fail("Document count mismatch after on/off")

        self.log.info("Starting stage 3")
        # start loading in standalone collection for stage 3
        for link in link_objects:
            if not self.cbas_util.connect_link(self.cluster, link.full_name):
                self.fail("Failed to connect link")
        self.load_doc_to_standalone_collection(self.data_loading_job, self.standalone_doc_per_collection // 2,
                                               (self.standalone_doc_per_collection * 3) // 4)
        if self.data_loading_job.qsize() == 1:
            self.cbas_util.run_jobs_in_parallel(self.data_loading_job, results, thread_count=5, async_run=True)

        self.run_queries = True
        self.get_cpu_stats = True

        # Create a new queue for the query job
        create_query_job.put((self.run_queries_on_datasets, {"query_jobs": self.query_job}))
        cpu_stat_job.put((self.average_cpu_stats,{}))

        self.cbas_util.run_jobs_in_parallel(cpu_stat_job, results, 1, async_run=True)
        self.cbas_util.run_jobs_in_parallel(create_query_job, query_work_results, 1, async_run=True)
        while self.query_job.qsize() < 5:
            self.log.info("Waiting for query job to be created")
            time.sleep(10)
        self.cbas_util.run_jobs_in_parallel(self.query_job, query_work_results, 3, async_run=True)

        # wait for data loading jobs to complete
        self.data_loading_job.join()
        self.wait_for_data_ingestion_in_the_collections(((self.standalone_doc_per_collection * 3) // 4) - 1,
                                                        ((self.standalone_doc_per_collection * 3) // 4) - 1)


        # stop generating new queries, wait for queries to complete and stop polling cpu stats
        self.run_queries = False
        self.get_cpu_stats = False
        self.query_job.join()

        # get count all the dataset and initiate on/off
        docs_in_collections_before = self.doc_in_each_dataset()
        self.initiate_on_off()

        # validate doc count doesn't change after on/off
        docs_in_collection_after = self.doc_in_each_dataset()
        if docs_in_collection_after != docs_in_collections_before:
            self.fail("Document count mismatch after on/off")

        self.log.info("Starting stage 4")
        self.load_doc_to_standalone_collection(self.data_loading_job, (self.standalone_doc_per_collection * 3) // 4,
                                               self.standalone_doc_per_collection)
        self.load_doc_to_remote_collection(self.data_loading_job,  (self.remote_source_doc_per_collection * 3) // 4,
                                           self.remote_source_doc_per_collection)
        self.cbas_util.run_jobs_in_parallel(self.data_loading_job, results, thread_count=5, async_run=True)

        # run query on datasets until doc loading is complete
        self.run_queries = True
        self.get_cpu_stats = True

        # Create a new queue for the query job
        create_query_job.put((self.run_queries_on_datasets, {"query_jobs": self.query_job}))
        cpu_stat_job.put((self.average_cpu_stats,{}))

        self.cbas_util.run_jobs_in_parallel(cpu_stat_job, results, 1, async_run=True)
        self.cbas_util.run_jobs_in_parallel(create_query_job, query_work_results, 1, async_run=True)
        while self.query_job.qsize() < 5:
            self.log.info("Waiting for query job to be created")
            time.sleep(10)
        self.cbas_util.run_jobs_in_parallel(self.query_job, query_work_results, 3, async_run=True)

        # wait for data loading to complete
        self.data_loading_job.join()
        self.wait_for_data_ingestion_in_the_collections(self.remote_source_doc_per_collection - 1,
                                                        self.standalone_doc_per_collection - 1)

        # stop generating new queries, wait for running queries to complete and stop polling for cpu stats
        self.run_queries = False
        self.get_cpu_stats = False
        self.query_job.join()
        # get doc count in datasets and initiating instance on and off
        docs_in_collections_before = self.doc_in_each_dataset()
        self.initiate_on_off()
        if not self.scale_columnar_cluster(2):
            self.fail("Failed to scale down the instance")
        docs_in_collection_after = self.doc_in_each_dataset()
        if docs_in_collection_after != docs_in_collections_before:
            self.fail("Doc count mismatch after on/off and scaling")

        if not all(query_work_results):
            self.fail("Some queries failed")