'''
Created on 30-August-2020

@author: umang.agrawal
'''

import json
import random
from Queue import Queue
from threading import Thread
import time

from BucketLib.BucketOperations import BucketHelper
from CbasLib.CBASOperations import CBASHelper
from CbasLib.cbas_entity import Dataverse, Synonym, CBAS_Index
from bucket_utils.bucket_ready_functions import DocLoaderUtils
from cbas.cbas_base import CBASBaseTest
from collections_helper.collections_spec_constants import MetaCrudParams
from security.rbac_base import RbacBase
from Jython_tasks.task import RunQueriesTask, CreateDatasetsTask, DropDatasetsTask


class CBASCapellaSanity(CBASBaseTest):

    def setUp(self):
        super(CBASCapellaSanity, self).setUp()

        # Since all the test cases are being run on 1 cluster only
        self.cluster = self.cb_clusters.values()[0]
        self.run_concurrent_query = self.input.param("run_query", False)
        self.parallel_load_percent = int(self.input.param(
            "parallel_load_percent", 0))

        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.setUp.__name__)

    def tearDown(self):
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)
        super(CBASCapellaSanity, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished", stage="Teardown")

    def setup_for_test(self, update_spec={}):
        wait_for_ingestion = (not self.parallel_load_percent)
        if self.cbas_spec_name:
            self.cbas_spec = self.cbas_util.get_cbas_spec(
                self.cbas_spec_name)
            if update_spec:
                self.cbas_util.update_cbas_spec(
                    self.cbas_spec, update_spec)
            cbas_infra_result = self.cbas_util.create_cbas_infra_from_spec(
                self.cluster, self.cbas_spec, self.bucket_util,
                wait_for_ingestion=wait_for_ingestion)
            if not cbas_infra_result[0]:
                self.fail(
                    "Error while creating infra from CBAS spec -- " +
                    cbas_infra_result[1])

    def test_create_drop_dataverse(self):
        """
        This testcase verifies dataverse/cbas_scope creation and deletion.
        Supported Test params -
        :testparam cbas_spec str, name of the cbas spec file.
        """
        self.log.info("Test started")
        update_spec = {
            "no_of_dataverses": self.input.param('no_of_dv', 1)
        }
        self.setup_for_test(update_spec)
        self.log.info(
            "Performing validation in Metadata.Dataverse after creating dataverses")
        jobs = Queue()
        results = list()

        def populate_job_queue():
            for dataverse in self.cbas_util.dataverses:
                if dataverse != "Default":
                    jobs.put((self.cbas_util.validate_dataverse_in_metadata,
                              {"cluster": self.cluster,
                               "dataverse_name":dataverse}))

        populate_job_queue()
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.cbas_spec.get("max_thread_count", 1),
            async_run=False)
        if not all(results):
            self.fail("Dataverse creation failed for few dataverses")

        results = []
        if not self.cbas_util.delete_cbas_infra_created_from_spec(
                self.cluster, self.cbas_spec,
                expected_dataverse_drop_fail=False,
                delete_dataverse_object=False):
            self.fail(
                "Error while dropping Dataverses created from CBAS spec")
        self.log.info("Performing validation in Metadata.Dataverse after dropping dataverses")

        populate_job_queue()
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.cbas_spec.get("max_thread_count", 1),
            async_run=False)
        if any(results):
            self.fail("Dropping Dataverse failed for few dataverses")

    def start_data_load_task(self, doc_load_spec="initial_load",
                             async_load=True, percentage_per_collection=0,
                             batch_size=10, mutation_num=1, doc_ttl=0):
        collection_load_spec = \
            self.bucket_util.get_crud_template_from_package(doc_load_spec)
        if doc_ttl:
            collection_load_spec["doc_ttl"] = doc_ttl
        if percentage_per_collection > 0:
            collection_load_spec["doc_crud"][
                MetaCrudParams.DocCrud.CREATE_PERCENTAGE_PER_COLLECTION] = \
                percentage_per_collection
        self.data_load_task = \
            self.bucket_util.run_scenario_from_spec(
                self.task, self.cluster, self.cluster.buckets,
                collection_load_spec, mutation_num=mutation_num,
                batch_size=batch_size, async_load=async_load)

    def start_query_task(self, sleep_time=5000):
        query = "SELECT SLEEP(COUNT(*), " + str(sleep_time) + ") FROM {0} " \
                                                             "WHERE " \
                                                             "MUTATED >= 0"
        self.query_task = RunQueriesTask(
            self.cluster, [query], self.task_manager, self.cbas_util, "cbas",
            run_infinitely=True, parallelism=3, is_prepared=False,
            record_results=False)
        self.task_manager.add_new_task(self.query_task)

    def wait_for_data_load_task(self, verify=True):
        if hasattr(self, "data_load_task") and self.data_load_task:
            self.task.jython_task_manager.get_task_result(self.data_load_task)
            if verify:
                # Validate data loaded in parallel
                DocLoaderUtils.validate_doc_loading_results(
                    self.data_load_task)
                self.bucket_util._wait_for_stats_all_buckets(
                    self.cluster, self.cluster.buckets)
                self.bucket_util.validate_docs_per_collections_all_buckets(
                    self.cluster)
            delattr(self, "data_load_task")

    def stop_query_task(self):
        if hasattr(self, "query_task") and self.query_task:
            if self.query_task.exception:
                self.task_manager.get_task_result(self.query_task)
            self.task_manager.stop_task(self.query_task)
            delattr(self, "query_task")

    def test_create_drop_datasets(self):
        """
        This testcase verifies dataset creation and deletion.
        Supported Test params -
        :testparam bucket_spec str, KV bucket spec to be used to load buckets,
        scopes and collections.
        :testparam cbas_spec str, cbas spec to be used to load CBAS infra
        :testparam no_of_dv int, no of dataverses to be created
        :testparam ds_per_dv, int, no of datasets to be created per dataverse.
        :testparam max_thread_count, int, max no of threads to be run in parallel
        """
        self.log.info("Test started")

        self.log.info("Loading data into buckets")
        self.start_data_load_task(
            async_load=False, percentage_per_collection=100,
            batch_size=10, mutation_num=0, doc_ttl=0)
        self.wait_for_data_load_task()

        update_spec = {
            "no_of_dataverses": self.input.param('no_of_dv', 1),
            "no_of_datasets_per_dataverse": self.input.param('ds_per_dv', 1),
            "no_of_synonyms": 0,
            "no_of_indexes": 0,
            "max_thread_count": self.input.param('no_of_threads', 1),
            "dataset": {"creation_methods": ["cbas_collection", "cbas_dataset"]}
        }
        # start parallel data loading
        if self.parallel_load_percent:
            self.start_data_load_task(
                percentage_per_collection=self.parallel_load_percent)
        # start parallel queries
        if self.run_concurrent_query:
            self.start_query_task()

        self.setup_for_test(update_spec)

        # wait for queries to complete
        self.stop_query_task()

        # wait for data load to complete
        self.wait_for_data_load_task()

        if not self.cbas_util.wait_for_ingestion_all_datasets(
            self.cluster, self.bucket_util):
            self.fail("Ingestion failed")

        self.log.info(
            "Performing validation in Metadata.Dataset after creating datasets"
        )
        jobs = Queue()
        results = list()

        def populate_job_queue(func_name):
            for dataset in self.cbas_util.list_all_dataset_objs():
                jobs.put((func_name, {"dataset":dataset}))

        def validate_metadata(dataset):
            if not dataset.enabled_from_KV and not dataset.kv_scope:
                if not self.cbas_util.validate_dataset_in_metadata(
                        self.cluster, dataset.name, dataset.dataverse_name,
                        BucketName=dataset.kv_bucket.name):
                    return False
            else:
                if not self.cbas_util.validate_dataset_in_metadata(
                        self.cluster, dataset.name, dataset.dataverse_name,
                        BucketName=dataset.kv_bucket.name,
                        ScopeName=dataset.kv_scope.name,
                        CollectionName=dataset.kv_collection.name):
                    return False
            return True

        def validate_doc_count(dataset):
            if not self.cbas_util.validate_cbas_dataset_items_count(
                    self.cluster, dataset.full_name, dataset.num_of_items):
                return False
            return True

        populate_job_queue(validate_metadata)
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.cbas_spec["max_thread_count"], async_run=False)
        if not all(results):
            self.fail("Metadata validation for Datasets failed")

        self.log.info("Validating item count")
        results = []
        populate_job_queue(validate_doc_count)
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.cbas_spec["max_thread_count"], async_run=False)

        if not all(results):
            self.fail("Item count validation for Datasets failed")
        self.log.info("Drop datasets")
        if not self.cbas_util.delete_cbas_infra_created_from_spec(
                self.cluster, self.cbas_spec, expected_dataset_drop_fail=False,
                delete_dataverse_object=False):
            self.fail(
                "Error while dropping CBAS entities created from CBAS spec")
        self.log.info(
            "Performing validation in Metadata.Dataverse after dropping datasets")
        results = []
        populate_job_queue(validate_metadata)
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.cbas_spec["max_thread_count"], async_run=False)
        if any(results):
            self.fail("Metadata validation for Datasets failed")
        self.log.info("Test finished")

    def test_enabling_disabling_analytics_collection_on_all_KV_collections(
            self):
        """
        This testcase verifies enabling of analytics from KV on all collections
        Supported Test params -
        :testparam bucket_spec str, KV bucket spec to be used to load buckets,
        scopes and collections.
        :testparam create_dataverse, boolean
        :testparam bucket_cardinality int, accepted values are between 1-3
        """
        self.log.info("Test started")
        jobs = Queue()
        results = list()
        self.cbas_util.create_dataset_obj(
            self.cluster, self.bucket_util, bucket_cardinality=self.input.param(
                'bucket_cardinality', 1), enabled_from_KV=True,
            for_all_kv_entities=True)
        dataset_objs = self.cbas_util.list_all_dataset_objs()

        def populate_job_queue(list_of_objs, func_name):
            for obj in list_of_objs:
                if func_name == self.cbas_util.validate_dataset_in_metadata:
                    jobs.put((func_name, {
                        "cluster":self.cluster, "dataset_name": obj.name,
                        "dataverse_name": obj.dataverse_name}))
                elif func_name == self.cbas_util.wait_for_ingestion_complete:
                    jobs.put((func_name, {
                        "cluster":self.cluster, "dataset_name": obj.full_name,
                        "num_items": obj.num_of_items}))
                elif func_name == self.cbas_util.validate_synonym_in_metadata:
                    jobs.put((func_name, {
                        "cluster":self.cluster,
                        "synonym_name": obj.name,
                        "synonym_dataverse_name": obj.dataverse_name,
                        "cbas_entity_name": obj.cbas_entity_name,
                        "cbas_entity_dataverse_name": obj.cbas_entity_dataverse}))
                elif func_name == self.cbas_util.enable_analytics_from_KV or func_name == self.cbas_util.disable_analytics_from_KV:
                    jobs.put((func_name, {
                        "cluster":self.cluster,
                        "kv_entity_name":obj.get_fully_qualified_kv_entity_name(
                            self.input.param('bucket_cardinality', 1))}))
                elif func_name == self.cbas_util.validate_dataverse_in_metadata:
                    jobs.put((func_name, {
                        "cluster":self.cluster,
                        "dataverse_name": obj.dataverse_name}))

        populate_job_queue(
            dataset_objs, self.cbas_util.enable_analytics_from_KV)
        if self.parallel_load_percent:
            self.start_data_load_task(
                async_load=True,
                percentage_per_collection=self.parallel_load_percent)
        if self.run_concurrent_query:
            self.start_query_task()
        self.cbas_util.run_jobs_in_parallel(jobs, results, 1)
        self.stop_query_task()
        self.wait_for_data_load_task()
        if not all(results):
            self.fail("Error while enabling analytics collection from KV")
        self.cbas_util.refresh_dataset_item_count(self.bucket_util)
        populate_job_queue(dataset_objs,
                           self.cbas_util.validate_dataset_in_metadata)
        self.cbas_util.run_jobs_in_parallel(jobs, results, 15)
        if not all(results):
            self.fail("Error while validating the datasets in Metadata")
        if not self.cbas_util.wait_for_ingestion_all_datasets(
            self.cluster, self.bucket_util):
            self.fail("Ingestion failed")
        populate_job_queue(self.cbas_util.list_all_synonym_objs(),
                           self.cbas_util.validate_synonym_in_metadata)
        self.cbas_util.run_jobs_in_parallel(jobs, results, 15)
        if not all(results):
            self.fail("Synonym was not created")
        if self.input.param('disable_from_kv', False):
            populate_job_queue(dataset_objs,
                               self.cbas_util.disable_analytics_from_KV)
            if self.parallel_load_percent:
                self.start_data_load_task(
                    percentage_per_collection=int(
                        self.parallel_load_percent / 2.5))
            self.cbas_util.run_jobs_in_parallel(jobs, results, 1)
            self.wait_for_data_load_task()
            if not all(results):
                self.fail("Error while disabling analytics collection from KV")
            populate_job_queue(dataset_objs,
                               self.cbas_util.validate_dataverse_in_metadata)
            self.cbas_util.run_jobs_in_parallel(jobs, results, 15)
            if not all(results):
                self.fail(
                    "Dataverse got dropped after disabling analytics from KV")
            results = []
            populate_job_queue(dataset_objs,
                               self.cbas_util.validate_dataset_in_metadata)
            self.cbas_util.run_jobs_in_parallel(jobs, results, 15)
            if any(results):
                self.fail("Dataset entry is still present in Metadata dataset")
            populate_job_queue(self.cbas_util.list_all_synonym_objs(),
                               self.cbas_util.validate_synonym_in_metadata)
            self.cbas_util.run_jobs_in_parallel(jobs, results, 15)
            if any(results):
                self.fail("Synonym was not deleted")
        self.log.info("Test finished")
