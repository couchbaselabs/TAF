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
from CbasLib.cbas_entity_on_prem import Dataverse, Synonym, CBAS_Index
from bucket_utils.bucket_ready_functions import DocLoaderUtils
from cbas.cbas_base import CBASBaseTest
from collections_helper.collections_spec_constants import MetaCrudParams
from security.rbac_base import RbacBase
from Jython_tasks.task import RunQueriesTask, CreateDatasetsTask, DropDatasetsTask
from cbas_utils.cbas_utils import CBASRebalanceUtil


class CBASDataverseAndScopes(CBASBaseTest):

    def setUp(self):
        super(CBASDataverseAndScopes, self).setUp()

        # Since all the test cases are being run on 1 cluster only
        self.cluster = self.cb_clusters.values()[0]

        if self.cbas_spec_name:
            self.cbas_spec = self.cbas_util.get_cbas_spec(
                self.cbas_spec_name)
            self.cbas_util.update_cbas_spec(
                self.cbas_spec,
                {"no_of_dataverses": int(self.input.param("no_of_dv", 1)),
                 "max_thread_count": int(self.input.param("ds_per_dv", 1))})
            if not self.cbas_util.create_dataverse_from_spec(
                self.cluster, self.cbas_spec):
                self.fail("Error while creating Dataverses from CBAS spec")
        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.setUp.__name__)

    def tearDown(self):
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)
        super(CBASDataverseAndScopes, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished", stage="Teardown")

    def test_create_drop_dataverse(self):
        """
        This testcase verifies dataverse/cbas_scope creation and deletion.
        Supported Test params -
        :testparam cbas_spec str, name of the cbas spec file.
        """
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

    def test_create_dataverse(self):
        """
        This testcase verifies dataverse creation.
        Supported Test params -
        :testparam default_bucket boolean, whether to load default KV bucket or not
        :testparam dv_name str, name of the dataverse.
        :testparam name_length int, max length of dataverse name
        :testparam fixed_length boolean, if true dataverse name length equals name_length,
        else dataverse name length <= name_length
        :testparam error str, error msg to validate.
        :testparam error_code int,
        """
        dataverse_name = self.input.param('dv_name', "")
        error_name = ""
        if self.input.param('cardinality', 0):
            for i in range(0, self.input.param('cardinality', 0)):
                name_length = self.input.param('name_{0}'.format(str(i + 1)),
                                               255)
                name = self.cbas_util.generate_name(
                    name_cardinality=1, max_length=name_length,
                    fixed_length=True)
                if not error_name and name_length > 255:
                    error_name = name
                dataverse_name += "{0}.".format(name)
        dataverse_name = dataverse_name.strip(".")
        dataverse_name = CBASHelper.format_name(dataverse_name)
        if not error_name:
            error_name = dataverse_name
        if self.input.param('error', None):
            error_msg = self.input.param('error', None).format(error_name)
        else:
            error_msg = None
        if not self.cbas_util.create_dataverse(
                cluster=self.cluster ,dataverse_name=dataverse_name,
                validate_error_msg=self.input.param('validate_error', True),
                expected_error=error_msg,
                expected_error_code=self.input.param('error_code', None)):
            self.fail(
                "Dataverse {0} creation passed when it should be failed".format(
                    dataverse_name))

    def test_create_analytics_scope(self):
        """
        This testcase verifies analytics scope creation.
        Supported Test params -
        :testparam default_bucket boolean, whether to load default KV bucket or not
        :testparam dv_name str, name of the dataverse.
        :testparam name_length int, max length of dataverse name
        :testparam fixed_length boolean, if true dataverse name length equals name_length,
        else dataverse name length <= name_length
        :testparam error str, error msg to validate.
        :testparam error_code int,
        """
        dataverse_name = self.input.param('dv_name', "")
        error_name = ""
        if self.input.param('cardinality', 0):
            for i in range(0, self.input.param('cardinality', 0)):
                name_length = self.input.param('name_{0}'.format(str(i + 1)),
                                               255)
                name = self.cbas_util.generate_name(
                    name_cardinality=1, max_length=name_length,
                    fixed_length=True)
                if not error_name and name_length > 255:
                    error_name = name
                dataverse_name += "{0}.".format(name)
        dataverse_name = dataverse_name.strip(".")
        dataverse_name = CBASHelper.format_name(dataverse_name)
        if not error_name:
            error_name = dataverse_name
        if self.input.param('error', None):
            error_msg = self.input.param('error', None).format(error_name)
        else:
            error_msg = None
        if not self.cbas_util.create_analytics_scope(
                cluster=self.cluster, cbas_scope_name=dataverse_name,
                validate_error_msg=self.input.param('validate_error', True),
                expected_error=error_msg,
                expected_error_code=self.input.param('error_code', None)):
            self.fail(
                "Dataverse {0} creation passed when it should be failed".format(
                    dataverse_name))

    def test_drop_dataverse(self):
        """
        This testcase verifies dropping of dataverse.
        Supported Test params -
        :testparam default_bucket boolean, whether to load default KV bucket or not
        :testparam cardinality int, accepted values are between 0-4
        :testparam name_length int, max length of dataverse name
        :testparam fixed_length boolean, if true dataverse name length equals name_length,
        else dataverse name length <= name_length
        :testparam error str, error msg to validate.
        :testparam validate_error boolean
        :testparam error_code int,
        """
        dataverse_name = CBASHelper.format_name(self.input.param('dv_name', ""))
        if self.input.param('error', None):
            error_msg = self.input.param('error', None).format(dataverse_name)
        else:
            error_msg = None
        if not self.cbas_util.drop_dataverse(
                cluster=self.cluster, dataverse_name=dataverse_name,
                validate_error_msg=True, expected_error=error_msg,
                expected_error_code=self.input.param('error_code', None)):
            self.fail(
                "Dropping of Dataverse {0} failed".format(dataverse_name))

    def test_drop_analytics_scope(self):
        """
        This testcase verifies dropping of analytics scope.
        Supported Test params -
        :testparam default_bucket boolean, whether to load default KV bucket or not
        :testparam cardinality int, accepted values are between 0-4
        :testparam name_length int, max length of analytics scope name
        :testparam fixed_length boolean, if true analytics scope name length equals name_length,
        else analytics scope name length <= name_length
        :testparam error str, error msg to validate.
        :testparam validate_error boolean
        :testparam error_code int,
        """
        dataverse_name = CBASHelper.format_name(self.input.param('dv_name', ""))
        if self.input.param('error', None):
            error_msg = self.input.param('error', None).format(dataverse_name)
        else:
            error_msg = None
        if not self.cbas_util.drop_analytics_scope(
                cluster=self.cluster, cbas_scope_name=dataverse_name,
                validate_error_msg=True, expected_error=error_msg,
                expected_error_code=self.input.param('error_code', None)):
            self.fail(
                "Dropping of Dataverse {0} failed".format(dataverse_name))

    def test_use_statement(self):
        dataverse_name = CBASHelper.format_name(
            self.cbas_util.generate_name(
                self.input.param('cardinality', 1)))
        if 0 < int(self.input.param('cardinality', 1)) < 3:
            if not self.cbas_util.create_dataverse(
                cluster=self.cluster, dataverse_name=dataverse_name):
                self.fail(
                    "Creation of Dataverse {0} failed".format(dataverse_name))
        if self.input.param('cardinality', 1) == 3:
            dataverse_name = "part1.part2.part3"
        if self.input.param('split_name', 0):
            dataverse_name = dataverse_name.split(".")[
                self.input.param('split_name', 0) - 1]
        cmd = "Use {0}".format(dataverse_name)
        self.log.debug("Executing cmd - \n{0}\n".format(cmd))
        status, metrics, errors, results, _ = \
            self.cbas_util.execute_statement_on_cbas_util(self.cluster, cmd)
        if status != "success":
            if not self.cbas_util.validate_error_in_response(
                    status, errors,
                    expected_error=self.input.param('error', None).format(
                        dataverse_name)):
                self.fail(
                    "Validating error message failed. Error message was different from expected error")


class CBASDatasetsAndCollections(CBASBaseTest):

    def setUp(self):

        super(CBASDatasetsAndCollections, self).setUp()

        self.iterations = int(self.input.param("iterations", 1))

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
        super(CBASDatasetsAndCollections, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.tearDown.__name__)

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
                    self.cluster, self.data_load_task)
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
        self.log.info(
            "Performing validation in Metadata.Dataset after creating datasets"
        )
        jobs = Queue()
        results = list()
        if not self.cbas_util.wait_for_ingestion_all_datasets(
            self.cluster, self.bucket_util):
            self.fail("Ingestion failed")

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

    def test_create_dataset(self):
        """
        This testcase verifies dataset creation.
        Supported Test params -
        :testparam bucket_spec str, KV bucket spec to be used to load buckets,
        scopes and collections.
        :testparam cardinality int, accepted values are between 1-3
        :testparam bucket_cardinality int, accepted values are between 1-3
        :testparam invalid_kv_collection, boolean
        :testparam invalid_kv_scope, boolean
        :testparam invalid_dataverse, boolean
        :testparam name_length int, max length of dataverse name
        :testparam no_dataset_name, boolean
        :testparam error str, error msg to validate.
        :testparam validate_error boolean
        :testparam cbas_collection boolean
        """
        self.log.info("Test started")
        exclude_collections = []
        if not self.input.param('consider_default_KV_collection', True):
            exclude_collections = ["_default"]
        self.cbas_util.create_dataset_obj(
            self.cluster, self.bucket_util,
            dataset_cardinality=self.input.param('cardinality', 1),
            bucket_cardinality=self.input.param('bucket_cardinality', 3),
            enabled_from_KV=False,
            name_length=self.input.param('name_length', 30),
            fixed_length=self.input.param('fixed_length', False),
            exclude_bucket=[], exclude_scope=[],
            exclude_collection=exclude_collections, no_of_objs=1)
        dataset = self.cbas_util.list_all_dataset_objs()[0]
        # Negative scenario
        if self.input.param('error', None):
            error_msg = self.input.param('error', None)
        else:
            error_msg = None
        if self.input.param('invalid_kv_collection', False):
            dataset.kv_collection.name = "invalid"
            error_msg = error_msg.format(
                dataset.get_fully_qualified_kv_entity_name(3))
        elif self.input.param('invalid_kv_scope', False):
            dataset.kv_scope.name = "invalid"
            error_msg = error_msg.format(
                dataset.get_fully_qualified_kv_entity_name(2))
        elif self.input.param('invalid_dataverse', False):
            dataset.name = "invalid." + dataset.name
            dataset.dataverse_name = ""
            error_msg = error_msg.format("invalid")
        elif self.input.param('no_dataset_name', False):
            dataset.name = ''
        elif self.input.param('remove_default_collection', False):
            error_msg = error_msg.format(
                CBASHelper.format_name(dataset.kv_bucket.name))
        # Negative scenario ends
        if not self.cbas_util.create_dataset(
            self.cluster, dataset.name, dataset.get_fully_qualified_kv_entity_name(
                self.input.param('bucket_cardinality', 3)),
            dataset.dataverse_name,
            validate_error_msg=self.input.param('validate_error', False),
            expected_error=error_msg,
            analytics_collection=self.input.param('cbas_collection', False)):
            self.fail("Dataset creation failed")
        if not self.input.param('validate_error', False):
            if self.input.param('no_dataset_name', False):
                dataset.name = dataset.kv_bucket.name
                dataset.full_name = dataset.get_fully_qualified_kv_entity_name(
                    1)
            if not self.cbas_util.validate_dataset_in_metadata(
                self.cluster, dataset.name, dataset.dataverse_name):
                self.fail("Dataset entry not present in Metadata.Dataset")
            if not self.cbas_util.validate_cbas_dataset_items_count(
                self.cluster, dataset.full_name, dataset.num_of_items):
                self.fail(
                    "Expected item count in dataset does not match actual item count")
        self.log.info("Test finished")

    def test_drop_non_existent_dataset(self):
        """
        This testcase verifies dataset deletion.
        Supported Test params -
        :testparam cbas_collection boolean
        """
        self.log.info("Test started")
        if not self.cbas_util.drop_dataset(
            self.cluster, "invalid", validate_error_msg=True,
            expected_error="Cannot find analytics collection with name invalid",
            analytics_collection=self.input.param('cbas_collection', False)):
            self.fail("Successfully deleted non-existent dataset")
        self.log.info("Test finished")

    def test_create_analytics_collection(self):
        """Only dataset_creation_method parameter will change for these testcase"""
        self.test_create_dataset()

    def test_drop_non_existent_analytics_collection(self):
        """Only dataset_creation_method and dataset_drop_method parameter will change for these testcase"""
        self.test_drop_non_existent_dataset()

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
        if self.input.param('create_dataverse', False):
            for bucket in self.cluster.buckets:
                for scope in self.bucket_util.get_active_scopes(bucket):
                    dataverse_name = bucket.name + "." + scope.name
                    dataverse_obj = self.cbas_util.get_dataverse_obj(
                        dataverse_name)
                    if not dataverse_obj:
                        dataverse_obj = Dataverse(dataverse_name)
                        self.cbas_util.dataverses[
                            dataverse_name] = dataverse_obj
                    jobs.put((self.cbas_util.create_dataverse,
                              {"cluster":self.cluster,
                               "dataverse_name":dataverse_name}))

            self.cbas_util.run_jobs_in_parallel(jobs, results, 15)
            if not results:
                self.fail("Error while creating dataverses")
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

    def test_enabling_analytics_collection_from_KV(self):
        """
        This testcase verifies enabling of analytics from KV.
        Supported Test params -
        :testparam bucket_spec str, KV bucket spec to be used to load buckets,
        scopes and collections.
        :testparam bucket_cardinality int, accepted values are between 1-3
        :testparam consider_default_KV_scope, boolean
        :testparam consider_default_KV_collection boolean
        :testparam create_dataverse, boolean
        :testparam invalid_kv_collection, boolean
        :testparam invalid_kv_scope, boolean
        :testparam invalid_kv_bucket, boolean
        :testparam precreate_dataset str, accepted values NonDefault and Default
        :testparam synonym_name str, accepted values None, Collection and Bucket
        :testparam compress_dataset boolean
        :testparam verify_synonym boolean
        :testparam error str, error msg to validate.
        :testparam validate_error boolean
        """
        self.log.info("Test started")
        exclude_collections = []
        if not self.input.param('consider_default_KV_collection', True):
            exclude_collections = ["_default"]
        bucket_cardinality = self.input.param('bucket_cardinality', 1)
        self.cbas_util.create_dataset_obj(
            self.cluster, self.bucket_util,
            bucket_cardinality=bucket_cardinality, enabled_from_KV=True,
            exclude_collection=exclude_collections, no_of_objs=1)
        dataset = self.cbas_util.list_all_dataset_objs()[0]
        if self.input.param('create_dataverse', False) and \
                not self.cbas_util.create_dataverse(
                    self.cluster, dataverse_name=dataset.dataverse_name):
            self.fail(
                "Failed to create dataverse {0}".format(dataset.dataverse_name))
        # Negative scenarios
        if self.input.param('error', ''):
            error_msg = self.input.param('error', '')
        else:
            error_msg = ''
        if self.input.param('invalid_kv_collection', False):
            dataset.kv_collection.name = "invalid"
            error_msg = error_msg.format(
                CBASHelper.unformat_name(
                    dataset.get_fully_qualified_kv_entity_name(
                        bucket_cardinality)))
        elif self.input.param('invalid_kv_scope', False):
            dataset.kv_scope.name = "invalid"
            error_msg = error_msg.format(
                CBASHelper.unformat_name(
                    dataset.get_fully_qualified_kv_entity_name(2)))
        elif self.input.param('invalid_kv_bucket', False):
            dataset.kv_bucket.name = "invalid"
            error_msg = error_msg.format("invalid")
        elif self.input.param('remove_default_collection', False):
            error_msg = error_msg.format(
                dataset.get_fully_qualified_kv_entity_name(3))
        # Creating synonym before enabling analytics from KV
        if self.input.param('synonym_name', None) == "Bucket":
            synonym_name = dataset.get_fully_qualified_kv_entity_name(1)
            error_msg = error_msg.format(synonym_name.replace('`', ''))
        elif self.input.param('synonym_name', None) == "Collection":
            synonym_name = dataset.get_fully_qualified_kv_entity_name(3)
        else:
            synonym_name = None

        if synonym_name:
            if self.input.param('synonym_on_other_dataset', False):
                cbas_entity_full_name = CBASHelper.format_name(dataset.dataverse_name, "other")
            else:
                cbas_entity_full_name = dataset.full_name
            if not self.cbas_util.create_analytics_synonym(
                self.cluster,  synonym_full_name=synonym_name,
                cbas_entity_full_name=cbas_entity_full_name):
                self.fail("Error while creating synonym {0} on dataset {1}".format(
                synonym_name, dataset.full_name))
        if self.input.param('precreate_dataset', None):
            if self.input.param('precreate_dataset', None) == "Default":
                if not self.cbas_util.create_dataset(
                    self.cluster, dataset.get_fully_qualified_kv_entity_name(1),
                    dataset.get_fully_qualified_kv_entity_name(bucket_cardinality),
                    dataverse_name="Default"):
                    self.fail("Error while creating dataset")
            else:
                if not self.cbas_util.create_dataset(
                    self.cluster, dataset.name,
                    dataset.get_fully_qualified_kv_entity_name(bucket_cardinality),
                    dataverse_name=dataset.dataverse_name):
                    self.fail("Error while creating dataset")
                error_msg = error_msg.format(dataset.name,
                                             dataset.dataverse_name)
        # Negative scenario ends
        if not self.cbas_util.enable_analytics_from_KV(
            self.cluster, dataset.get_fully_qualified_kv_entity_name(
                bucket_cardinality),
            compress_dataset=self.input.param('compress_dataset', False),
            validate_error_msg=self.input.param('validate_error', False),
            expected_error=error_msg):
            self.fail("Error while enabling analytics collection from KV")
        if not self.input.param('validate_error', False):
            if not self.cbas_util.validate_dataset_in_metadata(
                self.cluster, dataset_name=dataset.name,
                dataverse_name=dataset.dataverse_name):
                self.fail("Error while validating the datasets in Metadata")
            if not self.cbas_util.wait_for_ingestion_complete(
                    self.cluster, dataset_name=dataset.full_name,
                    num_items=dataset.num_of_items):
                self.fail("Data ingestion into the datasets did not complete")
        if self.input.param('verify_synonym', False):
            if not self.cbas_util.validate_synonym_in_metadata(
                self.cluster, synonym_name=dataset.kv_bucket.name,
                synonym_dataverse_name="Default", cbas_entity_name=dataset.name,
                cbas_entity_dataverse_name=dataset.dataverse_name):
                self.fail(
                    "Synonym {0} is not created under Dataverse {1}".format(
                        dataset.kv_bucket.name, "Default"))

    def test_disabling_analytics_collection_from_KV(self):
        """
        This testcase verifies disabling of analytics from KV.
        Supported Test params -
        :testparam bucket_spec str, KV bucket spec to be used to load buckets,
        scopes and collections.
        :testparam dataset_cardinality int, accepted values are 0 or 3
        :testparam bucket_cardinality int, accepted values are between 1-3
        :testparam consider_default_KV_scope, boolean
        :testparam consider_default_KV_collection boolean
        :testparam create_dataverse, boolean
        :testparam invalid_kv_collection, boolean
        :testparam create_dataset boolean
        :testparam create_synonym boolean
        :testparam dataverse_deleted boolean,
        :testparam synonym_deleted boolean
        :testparam dataset_creation_method str, method to be used to create dataset
        on a bucket/collection, accepted values are cbas_dataset, cbas_collection,
        enable_cbas_from_kv.
        :testparam error str, error msg to validate.
        :testparam validate_error boolean
        """
        self.log.info("Test started")
        exclude_collections = []
        if not self.input.param('consider_default_KV_collection', True):
            exclude_collections = ["_default"]
        bucket_cardinality = self.input.param('bucket_cardinality', 1)
        self.cbas_util.create_dataset_obj(
            self.cluster, self.bucket_util,
            bucket_cardinality=bucket_cardinality, enabled_from_KV=True,
            exclude_collection=exclude_collections, no_of_objs=1)
        dataset = self.cbas_util.list_all_dataset_objs()[0]
        if self.input.param('enable_analytics', True):
            if not self.cbas_util.enable_analytics_from_KV(
                self.cluster, dataset.get_fully_qualified_kv_entity_name(
                    bucket_cardinality),
                compress_dataset=self.input.param('compress_dataset', False)):
                self.fail("Error while enabling analytics collection from KV")
        else:
            if not self.cbas_util.create_dataset(
                self.cluster, dataset.name, dataset.full_kv_entity_name,
                dataverse_name=dataset.dataverse_name):
                self.fail("Error creating dataset {0}".format(dataset.name))
        # Negative scenarios
        if self.input.param('error', None):
            error_msg = self.input.param('error', None)
        else:
            error_msg = None
        if self.input.param('invalid_kv_collection', False):
            dataset.kv_collection.name = "invalid"
            error_msg = error_msg.format(
                "invalid", CBASHelper.unformat_name(dataset.dataverse_name))
        if self.input.param('invalid_kv_bucket', False):
            dataset.kv_bucket.name = "invalid"
            error_msg = error_msg.format(
                "_default", CBASHelper.unformat_name("invalid", "_default"))
        # Negative scenario ends
        if self.input.param('create_dataset', False):
            new_dataset_name = CBASHelper.format_name(
                self.cbas_util.generate_name())
            if not self.cbas_util.create_dataset(
                self.cluster, new_dataset_name, dataset.full_kv_entity_name,
                dataverse_name=dataset.dataverse_name):
                self.fail("Error creating dataset {0}".format(new_dataset_name))
        if self.input.param('create_synonym', False):
            new_synonym_name = self.cbas_util.generate_name()
            if not self.cbas_util.create_analytics_synonym(
                self.cluster, synonym_full_name=CBASHelper.format_name(
                    dataset.dataverse_name, new_synonym_name),
                cbas_entity_full_name=dataset.full_name):
                self.fail("Error creating synonym {0}".format(new_synonym_name))
        self.log.info("Disabling analytics from KV")
        if not self.cbas_util.disable_analytics_from_KV(
            self.cluster, dataset.get_fully_qualified_kv_entity_name(
                bucket_cardinality),
            validate_error_msg=self.input.param('validate_error', False),
            expected_error=error_msg):
            self.fail("Error while disabling analytics on KV collection")
        if not self.input.param('validate_error', False):
            self.log.info("Validate dataverse is not deleted")
            if not self.cbas_util.validate_dataverse_in_metadata(
                self.cluster, dataset.dataverse_name):
                self.fail(
                    "Dataverse {0} got deleted after disabling analytics from KV".format(
                        dataset.dataverse_name))
            self.log.info("Validate dataset is deleted")
            if self.cbas_util.validate_dataset_in_metadata(
                self.cluster, dataset.name, dataset.dataverse_name):
                self.fail(
                    "Dataset {0} is still present in Metadata.Dataset".format(
                        dataset.name))
            if self.input.param('create_dataset', False):
                if not self.cbas_util.validate_dataset_in_metadata(
                    self.cluster,  new_dataset_name, dataset.dataverse_name,
                    BucketName=dataset.kv_bucket.name):
                    self.fail(
                        "Explicitly created dataset got deleted after disabling analytics from KV")
            if self.input.param('create_synonym', False):
                if not self.cbas_util.validate_synonym_in_metadata(
                    self.cluster, synonym_name=new_synonym_name,
                    synonym_dataverse_name=dataset.dataverse_name,
                    cbas_entity_name=dataset.name,
                    cbas_entity_dataverse_name=dataset.dataverse_name):
                    self.fail(
                        "Explicitly created synonym got deleted after disabling analytics from KV")
        self.log.info("Test finished")

    def test_create_multiple_synonyms(self):
        self.log.info("Test started")
        update_spec = {
            "no_of_dataverses": self.input.param('no_of_dv', 2),
            "no_of_datasets_per_dataverse": self.input.param('ds_per_dv', 1),
            "no_of_synonyms": self.input.param('no_of_synonym', 10),
            "no_of_indexes": 0,
            "max_thread_count": self.input.param('no_of_threads', 1)}
        if self.parallel_load_percent:
            self.start_data_load_task(
                percentage_per_collection=self.parallel_load_percent)
        if self.run_concurrent_query:
            self.start_query_task()
        self.setup_for_test(update_spec)
        self.stop_query_task()
        self.wait_for_data_load_task()
        synonyms = self.cbas_util.list_all_synonym_objs()
        jobs = Queue()
        results = list()

        def populate_job_queue(func_name):
            for synonym in synonyms:
                if func_name == self.cbas_util.validate_synonym_in_metadata:
                    jobs.put((func_name, {
                        "cluster":self.cluster,
                        "synonym_name": synonym.name,
                        "synonym_dataverse_name": synonym.dataverse_name,
                        "cbas_entity_name": synonym.cbas_entity_name,
                        "cbas_entity_dataverse_name": synonym.cbas_entity_dataverse}))
                elif func_name == self.cbas_util.validate_synonym_doc_count:
                    jobs.put((func_name, {
                        "cluster":self.cluster,
                        "synonym_full_name": synonym.full_name,
                        "cbas_entity_full_name": synonym.cbas_entity_full_name}))

        populate_job_queue(self.cbas_util.validate_synonym_in_metadata)
        self.cbas_util.run_jobs_in_parallel(jobs, results, 15)
        if not all(results):
            self.fail("Error while validating synonym in Metadata")
        populate_job_queue(self.cbas_util.validate_synonym_doc_count)
        self.cbas_util.run_jobs_in_parallel(jobs, results, 15)
        if not all(results):
            self.fail("Error while validating synonym doc count for synonym")
        self.log.info("Test finished")

    def test_create_analytics_synonym(self):
        """
        This testcase verifies creation of analytics synonym.
        Supported Test params -
        :testparam bucket_spec str, KV bucket spec to be used to load buckets,
        scopes and collections.
        :testparam dangling_synonym boolean,
        :testparam invalid_dataverse boolean,
        :testparam synonym_name boolean,
        :testparam synonym_dataverse str, accepted values dataset, new, Default
        :testparam error str, error msg to validate.
        :testparam validate_error boolean
        :testparam synonym_on_synonym boolean
        :testparam different_syn_on_syn_dv boolean,
        :testparam same_syn_on_syn_name boolean
        :testparam action_on_dataset str, accepted values None, drop, recreate
        :testparam action_on_synonym str, accepted values None, drop, recreate
        :testparam validate_query_error boolean
        :testparam query_error str,
        """
        self.log.info("Test started")
        error_msg = self.input.param('error', '')
        synonym_objs = list()
        self.cbas_util.create_dataset_obj(
            self.cluster, self.bucket_util, dataset_cardinality=3,
            bucket_cardinality=3, enabled_from_KV=False, name_length=30,
            fixed_length=False, exclude_bucket=[], exclude_scope=[],
            exclude_collection=[], no_of_objs=1)
        ds_obj = self.cbas_util.list_all_dataset_objs()[0]
        if not self.cbas_util.create_dataset(
            self.cluster, ds_obj.name, ds_obj.full_kv_entity_name,
            dataverse_name=ds_obj.dataverse_name):
            self.fail("Error creating dataset {0}".format(ds_obj.name))
        if not self.cbas_util.wait_for_ingestion_complete(
            self.cluster, ds_obj.full_name, ds_obj.num_of_items):
            self.fail("Data ingestion into dataset failed.")
        if self.input.param('synonym_dataverse', "new") == "new":
            dv_name = CBASHelper.format_name(self.cbas_util.generate_name(2))
            if not self.cbas_util.create_dataverse(self.cluster, dv_name):
                self.fail("Error while creating dataverse")
        else:
            dv_name = ds_obj.dataverse_name

        if self.input.param('synonym_name', "new") == "new":
            synonym_name = CBASHelper.format_name(
                self.cbas_util.generate_name(1))
        else:
            synonym_name = ds_obj.name
        if self.input.param('dangling_synonym', False):
            ds_obj.name = "invalid"
        if self.input.param('invalid_dataverse', False):
            dv_name = "invalid"
        syn_obj = Synonym(synonym_name, ds_obj.name, ds_obj.dataverse_name,
                          dataverse_name=dv_name, synonym_on_synonym=False)
        synonym_objs.append(syn_obj)
        if self.input.param('synonym_on_synonym', False):
            if self.input.param('different_syn_on_syn_dv', False):
                dv_name = CBASHelper.format_name(
                    self.cbas_util.generate_name(2))
                if not self.cbas_util.create_dataverse(self.cluster, dv_name):
                    self.fail("Error while creating dataverse")
            if not self.input.param('same_syn_on_syn_name', False):
                synonym_name = CBASHelper.format_name(
                    self.cbas_util.generate_name(1))
            new_syn_obj = Synonym(
                synonym_name, syn_obj.name, syn_obj.dataverse_name,
                dataverse_name=dv_name, synonym_on_synonym=True)
            synonym_objs.append(new_syn_obj)
        for obj in synonym_objs:
            if self.input.param('same_syn_on_syn_name',
                                False) and obj.synonym_on_synonym:
                self.input.test_params.update({"validate_error": True})
                error_msg = error_msg.format(
                    CBASHelper.unformat_name(synonym_name))
            if not self.cbas_util.create_analytics_synonym(
                self.cluster, obj.full_name, obj.cbas_entity_full_name,
                validate_error_msg=self.input.param('validate_error', False),
                expected_error=error_msg):
                self.fail("Error while creating synonym")
        if self.input.param("action_on_dataset", None):
            self.log.info("Dropping Dataset")
            if not self.cbas_util.drop_dataset(self.cluster, ds_obj.full_name):
                self.fail("Error while dropping dataset")
            if self.input.param("action_on_dataset", None) == "recreate":
                self.log.info("Recreating dataset")
                if not self.cbas_util.create_dataset(
                    self.cluster, ds_obj.name, ds_obj.full_kv_entity_name,
                    dataverse_name=ds_obj.dataverse_name):
                    self.fail("Error creating dataset {0}".format(ds_obj.name))
                if not self.cbas_util.wait_for_ingestion_complete(
                    self.cluster, ds_obj.full_name, ds_obj.num_of_items):
                    self.fail("data ingestion into dataset failed.")
        if self.input.param("action_on_synonym", None):
            self.log.info("Dropping Synonym")
            if not self.cbas_util.drop_analytics_synonym(
                self.cluster, syn_obj.full_name):
                self.fail("Error while dropping synonym")
            if self.input.param("action_on_synonym", None) == "recreate":
                self.log.info("Recreating synonym")
                if not self.cbas_util.create_analytics_synonym(
                    self.cluster, syn_obj.full_name, syn_obj.cbas_entity_full_name):
                    self.fail("Error while recreating synonym")
            else:
                synonym_objs.remove(syn_obj)
        if not self.input.param('validate_error', False):
            for obj in synonym_objs:
                if not self.cbas_util.validate_synonym_in_metadata(
                        self.cluster, obj.name, obj.dataverse_name,
                        obj.cbas_entity_name, obj.cbas_entity_dataverse):
                    self.fail("Error while validating synonym in Metadata")
                if not self.cbas_util.validate_synonym_doc_count(
                        self.cluster, obj.full_name, obj.cbas_entity_full_name,
                        validate_error_msg=self.input.param('validate_query_error', False),
                        expected_error=self.input.param('query_error','').format(
                            CBASHelper.unformat_name(obj.name),
                            CBASHelper.unformat_name(obj.dataverse_name))):
                    self.fail(
                        "Error while validating synonym doc count for synonym")
        self.log.info("Test finished")

    def test_if_not_exists_flag_for_synonym(self):
        """
        This test case verifies that if multiple synonyms with same name on different datasets/synonyms
        are created with if_not_exists flag set as True, then the metadata entry for the synonym does
        not change with subsequent synonym creation, i.e. the metadata entry for synonym contains entry
        from the first time that the synonym was created.
        """
        self.log.info("Test started")
        self.log.info("Creating synonym")
        synonym_name = CBASHelper.format_name(
            self.cbas_util.generate_name(1))
        object_name_1 = CBASHelper.format_name(
            self.cbas_util.generate_name(1))
        object_name_2 = CBASHelper.format_name(
            self.cbas_util.generate_name(1))
        for obj_name in [object_name_1, object_name_2]:
            if not self.cbas_util.create_analytics_synonym(
                self.cluster, synonym_name, obj_name, if_not_exists=True):
                self.fail(
                    "Error while creating synonym {0}".format(synonym_name))
        if not self.cbas_util.validate_synonym_in_metadata(
            self.cluster, synonym_name, "Default", object_name_1, "Default"):
            self.fail(
                "Synonym metadata entry changed with subsequent synonym creation")
        self.log.info("Test finished")

    def test_dataset_and_synonym_name_resolution_precedence(self):
        """
        This testcase verifies which is resolved first dataset or synonym.
        Supported Test params -
        :testparam bucket_spec str, KV bucket spec to be used to load buckets,
        scopes and collections.
        :testparam dangling_synonym boolean,
        :testparam synonym_on_synonym boolean
        :testparam different_dataverse boolean,
        """
        self.log.info("Test started")
        synonym_objs = list()
        self.cbas_util.create_dataset_obj(
            self.cluster, self.bucket_util, dataset_cardinality=3,
            bucket_cardinality=3, enabled_from_KV=False, name_length=30,
            fixed_length=False, exclude_bucket=[], exclude_scope=[],
            exclude_collection=[], no_of_objs=2)
        dataset_objs = self.cbas_util.list_all_dataset_objs()
        for dataset in dataset_objs:
            if not self.cbas_util.create_dataset(
                    self.cluster, dataset.name, dataset.full_kv_entity_name,
                    dataverse_name=dataset.dataverse_name):
                self.fail("Error creating dataset {0}".format(dataset.name))
            if self.input.param('dangling_synonym', False):
                break

        if self.input.param('dangling_synonym', False):
            if not self.cbas_util.wait_for_ingestion_complete(
                    self.cluster, dataset_objs[0].full_name,
                    dataset_objs[0].num_of_items):
                self.fail("Data ingestion failed")
        else:
            if not self.cbas_util.wait_for_ingestion_all_datasets(
                    self.cluster, self.bucket_util):
                self.fail("Data ingestion failed")

        syn_obj = Synonym(
            dataset_objs[0].name, dataset_objs[1].name,
            dataset_objs[1].dataverse_name,
            dataverse_name=dataset_objs[0].dataverse_name,
            synonym_on_synonym=False)
        synonym_objs.append(syn_obj)
        if self.input.param('synonym_on_synonym', False):
            if self.input.param('different_syn_on_syn_dv', False):
                dv_name = CBASHelper.format_name(
                    self.cbas_util.generate_name(2))
                if not self.cbas_util.create_dataverse(self.cluster, dv_name):
                    self.fail("Error while creating dataverse")
            else:
                dv_name = dataset_objs[0].dataverse_name
            new_syn_obj = Synonym(
                self.cbas_util.generate_name(1), syn_obj.name,
                syn_obj.dataverse_name, dataverse_name=dv_name,
                synonym_on_synonym=True)
            synonym_objs.append(new_syn_obj)
        for obj in synonym_objs:
            if not self.cbas_util.create_analytics_synonym(
                self.cluster, obj.full_name, obj.cbas_entity_full_name):
                self.fail("Error while creating synonym")
        if not self.cbas_util.validate_synonym_doc_count(
            self.cluster, syn_obj.full_name, dataset_objs[0].full_name):
            self.fail("Querying synonym with same name as dataset, is returning docs from dataset on which\
                 synonym is created instead of the dataset with the same name.")
        self.log.info("Test finished")

    def test_drop_analytics_synonym(self):
        """
        This testcase verifies dropping of analytics synonym.
        Supported Test params -
        :testparam bucket_spec str, KV bucket spec to be used to load buckets,
        scopes and collections.
        :testparam invalid_synonym boolean,
        :testparam validate_query_error boolean
        :testparam query_error str,
        """
        self.log.info("Test started")
        self.cbas_util.create_dataset_obj(
            self.cluster, self.bucket_util, dataset_cardinality=3,
            bucket_cardinality=3, enabled_from_KV=False, name_length=30,
            fixed_length=False, exclude_bucket=[], exclude_scope=[],
            exclude_collection=[], no_of_objs=1)
        ds_obj = self.cbas_util.list_all_dataset_objs()[0]
        if not self.cbas_util.create_dataset(
            self.cluster, ds_obj.name, ds_obj.full_kv_entity_name,
            dataverse_name=ds_obj.dataverse_name):
            self.fail("Error creating dataset {0}".format(ds_obj.name))
        syn_obj = Synonym(
            self.cbas_util.generate_name(1), ds_obj.name, ds_obj.dataverse_name,
            dataverse_name=ds_obj.dataverse_name, synonym_on_synonym=False)
        if not self.cbas_util.create_analytics_synonym(
            self.cluster, syn_obj.full_name, syn_obj.cbas_entity_full_name):
            self.fail("Error while creating synonym")
        if self.input.param('invalid_synonym', False):
            syn_obj.name = "invalid"
        self.log.info("Dropping synonym")
        if not self.cbas_util.drop_analytics_synonym(
            self.cluster,
            CBASHelper.format_name(syn_obj.dataverse_name, syn_obj.name),
            validate_error_msg=self.input.param('validate_error', False),
            expected_error=self.input.param('error', '').format(syn_obj.name)):
            self.fail("Error while dropping Synonym")
        self.log.info("Validate Dataset item count after dropping synonym")
        if not self.input.param('validate_error', False) and not \
                self.cbas_util.validate_cbas_dataset_items_count(
                    self.cluster, ds_obj.full_name, ds_obj.num_of_items):
            self.fail("Doc count mismatch")
        self.log.info("Test finished")

    def bucket_flush_and_validate(self, bucket_obj):
        """
        - Flush the entire bucket
        - Validate scope/collections are intact post flush
        :param bucket_obj: Target bucket object to flush
        :return: None
        """
        self.log.info("Flushing bucket: %s" % bucket_obj.name)
        self.bucket_util.flush_bucket(self.cluster, bucket_obj)
        self.log.info("Validating scope/collections mapping and doc_count")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self.bucket_util.validate_docs_per_collections_all_buckets(self.cluster)
        # Print bucket stats
        self.bucket_util.print_bucket_stats(self.cluster)

    def test_datasets_created_on_KV_collections_after_flushing_KV_bucket(self):
        """
        This testcase verifies the effects of KV flushing on datasets.
        Supported Test params -
        :testparam bucket_spec str, KV bucket spec to be used to load buckets,
        scopes and collections.
        :testparam create_ds_on_different_bucket boolean, it set to true, create a
        dataset on collection belonging to a bucket that is not being flushed.
        """
        self.log.info("Test started")
        if not self.cbas_util.create_datasets_on_all_collections(
                self.cluster, self.bucket_util, cbas_name_cardinality=3,
                kv_name_cardinality=3, remote_datasets=False,
                storage_format=self.input.param("storage_format", None)):
            self.fail("Error while creating datasets")
        dataset_objs = self.cbas_util.list_all_dataset_objs()
        # Remove this check once MB-53038 is resolved
        if not self.cbas_util.wait_for_ingestion_all_datasets(
                self.cluster, self.bucket_util):
            self.fail("Ingestion failed")
        bucket = random.choice(self.cluster.buckets)
        self.bucket_flush_and_validate(bucket)
        for dataset_obj in dataset_objs:
            self.log.info("Validating item count")
            if not self.cbas_util.validate_cbas_dataset_items_count(
                self.cluster, dataset_obj.full_name,
                dataset_obj.kv_collection.num_items):
                self.fail("Data is still present in dataset, even when KV collection\
                on which the dataset was created was flushed.")
        self.log.info("Test finished")

    def test_dataset_for_data_addition_post_KV_flushing(self):
        """
        This testcase verifies the effects of adding new data post
        KV flushing on datasets.
        Supported Test params -
        :testparam bucket_spec str, KV bucket spec to be used to load buckets,
        scopes and collections.
        :testparam no_of_flushes int, no of times the bucket needs to be flushed.
        :testparam reload_data boolean, to reload data in KV bucket
        """
        self.log.info("Test started")
        self.cbas_util.create_dataset_obj(
            self.cluster, self.bucket_util, dataset_cardinality=3,
            bucket_cardinality=3, enabled_from_KV=False, name_length=30,
            fixed_length=False, exclude_bucket=[], exclude_scope=[],
            exclude_collection=[], no_of_objs=1)
        dataset_obj = self.cbas_util.list_all_dataset_objs()[0]
        if not self.cbas_util.create_dataset(
            self.cluster, dataset_obj.name, dataset_obj.full_kv_entity_name,
            dataverse_name=dataset_obj.dataverse_name):
            self.fail("Error creating dataset {0}".format(dataset_obj.name))
        for i in range(0, int(self.input.param('no_of_flushes', 1))):
            self.bucket_flush_and_validate(dataset_obj.kv_bucket)
            self.sleep(10, "Waiting for flush to complete")
            self.log.info(
                "Validating item count in dataset before adding new data in KV")
            if not self.cbas_util.validate_cbas_dataset_items_count(
                self.cluster, dataset_obj.full_name, dataset_obj.kv_collection.num_items):
                self.fail("Data is still present in dataset, even when KV collection\
                on which the dataset was created was flushed.")
            if self.input.param('reload_data', True):
                doc_loading_spec = self.bucket_util.get_crud_template_from_package("initial_load")
                self.load_data_into_buckets(self.cluster, doc_loading_spec)
                self.log.info(
                    "Validating item count in dataset after adding new data in KV")
                if not self.cbas_util.validate_cbas_dataset_items_count(
                    self.cluster, dataset_obj.full_name,
                    dataset_obj.kv_collection.num_items):
                    self.fail("Newly added data in KV collection did not get ingested in\
                    dataset after flushing")
            i += 1
        self.log.info("Test finished")

    def test_dataset_for_adding_new_docs_while_flushing(self):
        self.log.info("Test started")
        self.cbas_util.create_dataset_obj(
            self.cluster, self.bucket_util, dataset_cardinality=3,
            bucket_cardinality=3, enabled_from_KV=False, name_length=30,
            fixed_length=False, exclude_bucket=[], exclude_scope=[],
            exclude_collection=[], no_of_objs=1)
        dataset_obj = self.cbas_util.list_all_dataset_objs()[0]
        if not self.cbas_util.create_dataset(
            self.cluster, dataset_obj.name, dataset_obj.full_kv_entity_name,
            dataverse_name=dataset_obj.dataverse_name):
            self.fail("Error creating dataset {0}".format(dataset_obj.name))
        doc_loading_spec = \
            self.bucket_util.get_crud_template_from_package("initial_load")
        doc_loading_spec["doc_crud"]["create_percentage_per_collection"] = 50
        threads = list()
        thread1 = Thread(target=self.bucket_flush_and_validate,
                         name="flush_thread",
                         args=(dataset_obj.kv_bucket,))
        thread1.start()
        threads.append(thread1)
        self.sleep(5, "Waiting for KV flush to start")
        thread2 = Thread(target=self.load_data_into_buckets,
                         name="data_load_thread",
                         args=(self.cluster, doc_loading_spec, False, False,))
        thread2.start()
        threads.append(thread2)
        for thread in threads:
            thread.join()
        self.log.info("Validating item count in dataset")
        if not self.cbas_util.validate_cbas_dataset_items_count(
            self.cluster, dataset_obj.full_name, dataset_obj.kv_collection.num_items):
            self.fail(
                "Number of docs in dataset does not match docs in KV collection")
        self.log.info("Test finished")

    def test_dataset_when_KV_flushing_during_data_mutation(self):
        self.log.info("Test started")
        self.cbas_util.create_dataset_obj(
            self.cluster, self.bucket_util, dataset_cardinality=3,
            bucket_cardinality=3, enabled_from_KV=False, name_length=30,
            fixed_length=False, exclude_bucket=[], exclude_scope=[],
            exclude_collection=[], no_of_objs=1)
        dataset_obj = self.cbas_util.list_all_dataset_objs()[0]
        if not self.cbas_util.create_dataset(
            self.cluster, dataset_obj.name, dataset_obj.full_kv_entity_name,
            dataverse_name=dataset_obj.dataverse_name):
            self.fail("Error creating dataset {0}".format(dataset_obj.name))
        doc_loading_spec = \
            self.bucket_util.get_crud_template_from_package("initial_load")
        doc_loading_spec["doc_crud"]["create_percentage_per_collection"] = 50
        self.load_data_into_buckets(self.cluster, doc_loading_spec, True)
        self.bucket_flush_and_validate(dataset_obj.kv_bucket)
        self.log.info("Validating item count in dataset")
        if not self.cbas_util.validate_cbas_dataset_items_count(
            self.cluster, dataset_obj.full_name, dataset_obj.kv_collection.num_items):
            self.fail(
                "Number of docs in dataset does not match docs in KV collection")
        self.log.info("Test finished")

    def test_docs_deleted_in_dataset_once_MaxTTL_reached(self):
        """
        In Case both collectionTTL and BucketTTL are set, collectionTTL
        takes preference.
        In case collectionTTL/BucketTTL + docTTL, then min among them.
        """
        self.log.info("Test started")
        self.bucket_spec = self.input.param("bucket_spec", "analytics.multi_bucket")
        buckets_spec = self.bucket_util.get_bucket_template_from_package(
            self.bucket_spec)
        doc_loading_spec = None
        docTTL = int(self.input.param('docTTL', 0))
        collectionTTL = int(self.input.param('collectionTTL', 0))
        bucketTTL = int(self.input.param('bucketTTL', 0))
        ttl_dict = dict()
        selected_bucket = random.choice(buckets_spec["buckets"].keys())
        if bucketTTL:
            buckets_spec["buckets"][selected_bucket]["maxTTL"] = bucketTTL
            ttl_dict["bucketTTL"] = bucketTTL
        if collectionTTL:
            selected_scope = random.choice(
                buckets_spec["buckets"][selected_bucket]["scopes"].keys())
            selected_collection = random.choice(
                buckets_spec["buckets"][selected_bucket]["scopes"][
                    selected_scope]["collections"].keys())
            buckets_spec["buckets"][selected_bucket]["scopes"][selected_scope][
                "collections"][selected_collection]["maxTTL"] = collectionTTL
            selected_collection = CBASHelper.format_name(
                selected_bucket, selected_scope, selected_collection)
            ttl_dict["collectionTTL"] = collectionTTL
        if docTTL:
            doc_loading_spec = self.bucket_util.get_crud_template_from_package(
                "initial_load")
            doc_loading_spec[MetaCrudParams.DOC_TTL] = docTTL
            ttl_dict["docTTL"] = docTTL

        ttl_to_check = None
        if len(list(set(list(ttl_dict.values())))) == 1:
            if "docTTL" in ttl_dict:
                ttl_to_check = "docTTL"
            elif "bucketTTL" in ttl_dict:
                ttl_to_check = "bucketTTL"
            elif "collectionTTL" in ttl_dict:
                ttl_to_check = "collectionTTL"
        else:
            for ttl in ttl_dict:
                if not ttl_to_check:
                    ttl_to_check = ttl
                elif ttl_dict[ttl] < ttl_dict[ttl_to_check]:
                    ttl_to_check = ttl

        end_time = 0
        if collectionTTL > 0 and bucketTTL > 0:
            if docTTL > 0:
                if (collectionTTL > bucketTTL) and (collectionTTL <= docTTL):
                    if collectionTTL == docTTL:
                        end_time = time.time() + ttl_dict["docTTL"]
                        ttl_to_check = "docTTL"
                    else:
                        end_time = time.time() + ttl_dict["collectionTTL"]
                elif (docTTL > bucketTTL) and (docTTL < collectionTTL):
                    end_time = time.time() + ttl_dict["docTTL"]
                    ttl_to_check = "docTTL"
            else:
                if collectionTTL > bucketTTL:
                    end_time = time.time() + ttl_dict["collectionTTL"]

        if not end_time:
            end_time = time.time() + ttl_dict[ttl_to_check]

        self.collectionSetUp(self.cluster, True, buckets_spec, doc_loading_spec)
        #inserting docs parallel
        if self.parallel_load_percent:
            self.start_data_load_task(
                percentage_per_collection=self.parallel_load_percent,
                doc_ttl=docTTL)
        if self.run_concurrent_query:
            self.start_query_task()
        if not self.cbas_util.create_datasets_on_all_collections(
                self.cluster, self.bucket_util, cbas_name_cardinality=3,
                kv_name_cardinality=3, creation_methods=["cbas_collection", "cbas_dataset"],
                storage_format=self.input.param("storage_format", None)):
            self.fail("Dataset creation failed")
        self.stop_query_task()
        self.wait_for_data_load_task()
        self.bucket_util.print_bucket_stats(self.cluster)
        if not self.cbas_util.wait_for_ingestion_all_datasets(
            self.cluster, self.bucket_util):
            self.fail("Ingestion failed")
        self.bucket_util._expiry_pager(self.cluster)

        while time.time() < end_time + 15:
            self.sleep(5, "waiting for maxTTL to complete")

        if "magma" in self.bucket_spec:
            rebalance_util = CBASRebalanceUtil(
                self.cluster_util, self.bucket_util, self.task,
                self.input.param("vbucket_check", True), self.cbas_util)
            rebalance_util.data_validation_collection(
                self.cluster, skip_validations=False,
                doc_and_collection_ttl=True, async_compaction=False)

        self.log.info("Validating item count")
        datasets = self.cbas_util.list_all_dataset_objs()

        for dataset in datasets:
            mutated_items = dataset.kv_collection.num_items - (
                    100 * dataset.kv_collection.num_items) / (
                    100 + self.parallel_load_percent)
            if ttl_to_check == "docTTL":
                if not self.cbas_util.validate_cbas_dataset_items_count(
                        self.cluster, dataset.full_name, 0):
                    self.fail(
                        "Docs are still present in the dataset even after "
                        "DocTTl reached")
            elif ttl_to_check == "bucketTTL":
                if dataset.kv_bucket.name == selected_bucket:
                    if not self.cbas_util.validate_cbas_dataset_items_count(
                            self.cluster, dataset.full_name, 0):
                        self.fail(
                            "Docs are still present in the dataset even "
                            "after bucketTTl reached")
                else:
                    if not self.cbas_util.validate_cbas_dataset_items_count(
                            self.cluster, dataset.full_name,
                            dataset.num_of_items, mutated_items):
                        self.fail(
                            "Docs are deleted from datasets when it should "
                            "not have been deleted")
            elif ttl_to_check == "collectionTTL":
                if dataset.full_kv_entity_name == selected_collection:
                    if not self.cbas_util.validate_cbas_dataset_items_count(
                            self.cluster, dataset.full_name, 0):
                        self.fail(
                            "Docs are still present in the dataset even "
                            "after CollectionTTl reached")
                else:
                    if not self.cbas_util.validate_cbas_dataset_items_count(
                            self.cluster, dataset.full_name,
                            dataset.num_of_items, mutated_items):
                        self.fail(
                            "Docs are deleted from datasets when it should "
                            "not have been deleted")
        self.log.info("Test finished")

    def test_create_query_drop_on_multipart_name_secondary_index(self):
        """
        This testcase verifies secondary index creation, querying using
        index and dropping of index.
        Supported Test params -
        :testparam analytics_index boolean, whether to use create/drop index or
        create/drop analytics index statements to create index
        """
        self.log.info("Test started")
        statement = 'SELECT VALUE v FROM {0} v WHERE age > 2'
        if self.parallel_load_percent:
            self.start_data_load_task(
                percentage_per_collection=self.parallel_load_percent)
        if self.run_concurrent_query:
            self.start_query_task()
        if not self.cbas_util.create_datasets_on_all_collections(
                self.cluster, self.bucket_util, cbas_name_cardinality=3,
                kv_name_cardinality=1,
                storage_format=self.input.param("storage_format", None)):
            self.fail("Dataset creation failed")
        dataset_objs = self.cbas_util.list_all_dataset_objs()
        count = 0
        for dataset in dataset_objs:
            count += 1
            index = CBAS_Index(
                "idx_{0}".format(count), dataset.name, dataset.dataverse_name,
                indexed_fields=self.input.param('index_fields', None))
            if not self.cbas_util.create_cbas_index(
                self.cluster, index.name, index.indexed_fields,
                index.full_dataset_name,
                analytics_index=self.input.param('analytics_index', False)):
                self.fail("Failed to create index on dataset {0}".format(
                    dataset.name))
            dataset.indexes[index.name] = index
            if not self.cbas_util.verify_index_created(
                self.cluster, index.name, index.dataset_name, index.indexed_fields):
                self.fail("Index {0} on dataset {1} was not created.".format(
                    index.name, index.dataset_name))
            query = ""
            if self.input.param('verify_index_on_synonym', False):
                self.log.info("Creating synonym")
                synonym = Synonym(
                    self.cbas_util.generate_name(), dataset.name,
                    dataset.dataverse_name, dataset.dataverse_name)
                if not self.cbas_util.create_analytics_synonym(
                    self.cluster, synonym.full_name,
                    synonym.cbas_entity_full_name, if_not_exists=True):
                    self.fail("Error while creating synonym")
                query = statement.format(synonym.full_name)
            else:
                query = statement.format(dataset.full_name)
            if not self.cbas_util.verify_index_used(
                self.cluster, query, True, index.name):
                self.fail("Index was not used while querying the dataset")
            if not self.cbas_util.drop_cbas_index(
                self.cluster, index.name, index.full_dataset_name,
                analytics_index=self.input.param('analytics_index', False)):
                self.fail("Drop index query failed")
        self.stop_query_task()
        self.wait_for_data_load_task()
        self.log.info("Test finished")

    def test_create_secondary_index_on_synonym(self):
        self.log.info("Test started")
        self.cbas_util.create_dataset_obj(
            self.cluster, self.bucket_util, dataset_cardinality=3,
            bucket_cardinality=3, enabled_from_KV=False, no_of_objs=1)
        dataset = self.cbas_util.list_all_dataset_objs()[0]
        if not self.cbas_util.create_dataset(
            self.cluster, dataset.name, dataset.full_kv_entity_name,
            dataset.dataverse_name):
            self.fail("Failed to create dataset")
        self.log.info("Creating synonym")
        synonym = Synonym(
            self.cbas_util.generate_name(), dataset.name,
            dataset.dataverse_name, dataset.dataverse_name)
        if not self.cbas_util.create_analytics_synonym(
            self.cluster, synonym.full_name, synonym.cbas_entity_full_name,
            if_not_exists=True):
            self.fail("Error while creating synonym")
        index = CBAS_Index(
            self.cbas_util.generate_name(), synonym.name, synonym.dataverse_name,
            indexed_fields=self.input.param('index_fields', None))
        expected_error = "Cannot find analytics collection with name {0} in analytics scope {" \
                         "1}".format(synonym.name, synonym.dataverse_name)
        if not self.cbas_util.create_cbas_index(
            self.cluster, index.name, index.indexed_fields, index.full_dataset_name,
                analytics_index=self.input.param('analytics_index', False),
                validate_error_msg=True, expected_error=expected_error):
            self.fail("Index was successfully created on synonym")
        self.log.info("Test finished")

    def test_dataset_after_deleting_and_recreating_KV_entity(self):
        self.log.info("Test started")
        jobs = Queue()
        if not self.cbas_util.create_datasets_on_all_collections(
                self.cluster, self.bucket_util,
                cbas_name_cardinality=self.input.param('cardinality', False),
                kv_name_cardinality=self.input.param('bucket_cardinality', False),
                storage_format=self.input.param("storage_format", None)):
            self.fail("Dataset creation failed")
        dataset_objs = self.cbas_util.list_all_dataset_objs()

        def execute_function_in_parallel(func_name, num_items=None):
            results = list()

            count = 0
            for dataset in dataset_objs:
                if func_name == self.cbas_util.wait_for_ingestion_complete:
                    jobs.put((func_name, {
                        "cluster":self.cluster,
                        "dataset_name": dataset.full_name,
                        "num_items": dataset.num_of_items,
                        "timeout": 1200}))
                elif func_name == self.cbas_util.create_cbas_index:
                    index = CBAS_Index(
                        "idx_{0}".format(count), dataset.name,
                        dataset.dataverse_name,
                        indexed_fields=self.input.param('index_fields', None))
                    count += 1
                    dataset.indexes[index.name] = index
                    jobs.put((func_name, {
                        "cluster":self.cluster, "index_name": index.name,
                        "indexed_fields": index.indexed_fields,
                        "dataset_name": index.full_dataset_name,
                        "analytics_index": self.input.param('analytics_index', False)}))
                elif func_name == self.cbas_util.verify_index_used:
                    statement = 'SELECT VALUE v FROM {0} v WHERE age > 2'
                    for index in dataset.indexes.values():
                        jobs.put((func_name, {
                            "cluster":self.cluster,
                            "statement": statement.format(index.full_dataset_name),
                            "index_used": True, "index_name": index.name}))
                else:
                    if num_items is not None:
                        jobs.put((func_name, {
                            "cluster":self.cluster,
                            "dataset_name": dataset.full_name,
                            "expected_count": num_items}))
                    else:
                        jobs.put(
                            (func_name, {
                                "cluster":self.cluster,
                                "dataset_name": dataset.full_name,
                                "expected_count": dataset.num_of_items}))

            self.cbas_util.run_jobs_in_parallel(jobs, results, 15, async_run=False)
            if not all(results):
                if func_name == self.cbas_util.wait_for_ingestion_complete:
                    self.fail("Data ingestion into datasets failed.")
                elif func_name == self.cbas_util.create_cbas_index:
                    self.fail("Index creation on datasets failed")
                elif func_name == self.cbas_util.verify_index_used:
                    self.fail("Index was not used while executing query")
                else:
                    self.fail(
                        "Expected no. of items in dataset does not match the actual no. of items")

        execute_function_in_parallel(self.cbas_util.wait_for_ingestion_complete)
        execute_function_in_parallel(self.cbas_util.create_cbas_index)
        execute_function_in_parallel(self.cbas_util.verify_index_used)
        if not self.bucket_util.delete_bucket(
            self.cluster, dataset_objs[0].kv_bucket, wait_for_bucket_deletion=True):
            self.fail("Error while deleting bucket")
        self.cbas_util.wait_for_ingestion_all_datasets(
            self.cluster, self.bucket_util)
        self.collectionSetUp(self.cluster)
        self.cbas_util.wait_for_ingestion_all_datasets(
            self.cluster, self.bucket_util)
        execute_function_in_parallel(self.cbas_util.verify_index_used)
        self.log.info("Test finished")

    def test_KV_collection_deletion_does_not_effect_dataset_on_other_collections(
            self):
        self.log.info("Test started")
        statement = 'SELECT VALUE v FROM {0} v WHERE age > 2'
        if not self.cbas_util.create_datasets_on_all_collections(
                self.cluster, self.bucket_util,
                cbas_name_cardinality=self.input.param('cardinality', None),
                kv_name_cardinality=self.input.param('bucket_cardinality', None),
                storage_format=self.input.param("storage_format", None)):
            self.fail("Dataset creation failed")
        dataset_objs = self.cbas_util.list_all_dataset_objs()
        count = 0
        for dataset in dataset_objs:
            count += 1
            index = CBAS_Index(
                "idx_{0}".format(count), dataset.name, dataset.dataverse_name,
                indexed_fields=self.input.param('index_fields', None))
            if not self.cbas_util.create_cbas_index(
                self.cluster, index.name, index.indexed_fields, index.full_dataset_name,
                analytics_index=self.input.param('analytics_index', False)):
                self.fail("Failed to create index on dataset {0}".format(
                    dataset.name))
            dataset.indexes[index.name] = index
            if not self.cbas_util.verify_index_created(
                self.cluster, index.name, index.dataset_name, index.indexed_fields):
                self.fail("Index {0} on dataset {1} was not created.".format(
                    index.name, index.dataset_name))
            if not self.cbas_util.verify_index_used(
                self.cluster, statement.format(dataset.full_name), True, index.name):
                self.fail("Index was not used while querying the dataset")
        if not self.cbas_util.wait_for_ingestion_all_datasets(
            self.cluster, self.bucket_util, timeout=1200):
            self.fail("Failed to ingest data into datasets")
        selected_dataset = random.choice(dataset_objs)
        collections = self.bucket_util.get_active_collections(
            selected_dataset.kv_bucket, selected_dataset.kv_scope.name, True)
        collections.remove(selected_dataset.kv_collection.name)
        collection_to_delete = random.choice(collections)
        self.bucket_util.drop_collection(
            self.cluster.master, selected_dataset.kv_bucket,
            scope_name=selected_dataset.kv_scope.name,
            collection_name=collection_to_delete)
        collection_to_delete = CBASHelper.format_name(
            selected_dataset.get_fully_qualified_kv_entity_name(2),
            collection_to_delete)
        for dataset in dataset_objs:
            if dataset.full_kv_entity_name != collection_to_delete:
                if not self.cbas_util.validate_cbas_dataset_items_count(
                        self.cluster, dataset.full_name, dataset.num_of_items):
                    self.fail("KV collection deletion affected data in "
                              "datasets that were not creation on the "
                              "deleted KV collection")
                if not self.cbas_util.verify_index_used(
                    self.cluster, statement.format(dataset.full_name), True,
                    dataset.indexes.keys()[0]):
                    self.fail("Index was not used while querying the dataset")
            else:
                if not self.cbas_util.validate_cbas_dataset_items_count(
                        self.cluster, dataset.full_name, 0):
                    self.fail("Data is still present in the dataset even "
                              "after the KV collection on which it was "
                              "created was deleted.")
        self.log.info("Test finished")

    def test_analytics_select_rbac_role_for_collections(self):
        self.log.info("Test started")
        dataset_names = list()
        bucket_helper = BucketHelper(self.cluster.master)
        for bucket in self.cluster.buckets:
            # Create datasets on KV collections in every scope in the bucket.
            status, content = bucket_helper.list_collections(bucket.name)
            if not status:
                self.fail(
                    "Failed to fetch all the collections in bucket {0}".format(
                        bucket.name))
            json_parsed = json.loads(content)
            for scope in json_parsed["scopes"]:
                count = 0
                for collection in scope["collections"]:
                    dataset_name = "{0}-{1}-{2}".format(
                        bucket.name, scope["name"], collection["name"])
                    dataset_names.append((dataset_name, bucket.name,
                                          scope["name"], collection["name"]))
                    self.log.info("Creating dataset {0}".format(dataset_name))
                    if not self.cbas_util.create_dataset(
                        self.cluster, CBASHelper.format_name(dataset_name),
                        CBASHelper.format_name(bucket.name, scope["name"],
                                               collection["name"])):
                        self.fail("Error while creating dataset {0}".format(
                            dataset_name))
                    count += 1
                    if count == 2:
                        break
        rbac_username = "test_user"
        dataset_info = random.choice(dataset_names)
        user = [
            {'id': rbac_username, 'password': 'password', 'name': 'Some Name'}]
        _ = RbacBase().create_user_source(user, 'builtin', self.cluster.master)
        if self.input.param('collection_user', False):
            self.log.info(
                "Granting analytics_select user access to following KV entity - {0}.{1}.{2}".format(
                    dataset_info[1], dataset_info[2], dataset_info[3]))
            payload = "name=" + rbac_username + "&roles=analytics_select" + "[" + \
                      dataset_info[1] + ":" + dataset_info[
                          2] + ":" + dataset_info[3] + "]"
            response = self.cluster.rest.add_set_builtin_user(rbac_username, payload)
        if self.input.param('scope_user', False):
            self.log.info(
                "Granting analytics_select user access to following KV entity - {0}.{1}".format(
                    dataset_info[1], dataset_info[2]))
            payload = "name=" + rbac_username + "&roles=analytics_select" + "[" + \
                      dataset_info[1] + ":" + dataset_info[
                          2] + "]"
            response = self.cluster.rest.add_set_builtin_user(rbac_username, payload)
        if self.input.param('bucket_user', False):
            self.log.info(
                "Granting analytics_select user access to following KV entity - {0}".format(
                    dataset_info[1]))
            payload = "name=" + rbac_username + "&roles=analytics_select" + "[" + \
                      dataset_info[1] + "]"
            response = self.cluster.rest.add_set_builtin_user(rbac_username, payload)
        for dataset_name in dataset_names:
            cmd_get_num_items = "select count(*) from %s;" % CBASHelper.format_name(
                dataset_name[0])
            status, metrics, errors, results, _ = self.cbas_util.execute_statement_on_cbas_util(
                self.cluster, cmd_get_num_items, username=rbac_username, password="password")
            validate_error = False
            if self.input.param('bucket_user', False):
                selected_kv_entity = dataset_info[1]
                current_kv_entity = dataset_name[1]
            elif self.input.param('scope_user', False):
                selected_kv_entity = "{0}-{1}".format(
                    dataset_info[1], dataset_info[2])
                current_kv_entity = "{0}-{1}".format(
                    dataset_name[1], dataset_name[2])
            elif self.input.param('collection_user', False):
                selected_kv_entity = dataset_info[0]
                current_kv_entity = dataset_name[0]
            if selected_kv_entity == current_kv_entity:
                if status == "success":
                    self.log.info("Query passed with analytics select user")
                else:
                    self.fail("RBAC user is unable to query dataset {0}".format(
                        dataset_name[0]))
            else:
                validate_error = True
            if validate_error and not self.cbas_util.validate_error_in_response(
                    status, errors, "User must have permission"):
                self.fail("RBAC user is able to query dataset {0}".format(
                    dataset_name[0]))
        self.log.info("Test finished")

    def test_analytics_with_parallel_dataset_creation(self):
        self.log.info("test_analytics_with_parallel_dataset_creation started")
        # Create Datasets on all collections in parallel
        create_datasets_task = CreateDatasetsTask(
            self.cluster, bucket_util=self.bucket_util,
            cbas_name_cardinality=self.input.param('cardinality', None),
            cbas_util=self.cbas_util,
            kv_name_cardinality=self.input.param('bucket_cardinality', None),
            creation_methods=["cbas_collection", "cbas_dataset"])
        self.task_manager.add_new_task(create_datasets_task)
        if self.parallel_load_percent <= 0:
            self.parallel_load_percent = 100
        self.start_data_load_task(
            percentage_per_collection=self.parallel_load_percent)
        self.start_query_task()
        # Wait for create dataset task to finish
        dataset_creation_result = self.task_manager.get_task_result(
            create_datasets_task)
        self.wait_for_data_load_task()
        self.stop_query_task()
        if not dataset_creation_result:
            self.fail("Datasets creation failed")
        # Validate ingestion
        if not self.cbas_util.wait_for_ingestion_all_datasets(
            self.cluster, self.bucket_util):
            self.fail("Ingestion failed")
        self.log.info("test_analytics_with_parallel_dataset_creation completed")

    def test_analytics_with_killing_cbas_memcached(self):
        # Create Datasets on all collections in parallel
        self.log.info("test_analytics_with_killing_cbas_memcached ")
        create_datasets_task = CreateDatasetsTask(
            self.cluster, bucket_util=self.bucket_util,
            cbas_name_cardinality=self.input.param('cardinality', None),
            cbas_util=self.cbas_util,
            kv_name_cardinality=self.input.param('bucket_cardinality', None),
            creation_methods=["cbas_collection", "cbas_dataset"])
        self.task_manager.add_new_task(create_datasets_task)
        if self.parallel_load_percent <= 0:
            self.parallel_load_percent = 100
        self.start_data_load_task(
            percentage_per_collection=self.parallel_load_percent)
        cbas_kill_count = self.input.param("cbas_kill_count", 0)
        memcached_kill_count = self.input.param("memcached_kill_count", 0)
        if not cbas_kill_count:
            self.start_query_task()
        # Wait for create dataset task to finish
        dataset_creation_result = self.task_manager.get_task_result(
            create_datasets_task)
        if not dataset_creation_result:
            self.fail("Datasets creation failed")
        self.wait_for_data_load_task()
        kill_process_task = self.cbas_util.start_kill_processes_task(
            self.cluster, self.cluster_util, cbas_kill_count, memcached_kill_count)
        self.stop_query_task()
        if kill_process_task:
            self.task_manager.get_task_result(kill_process_task)
            self.cluster.cbas_nodes = [node for node in self.cluster.servers
                                       if "cbas" in node.services]
            self.cbas_util.wait_for_processes(self.cluster.cbas_nodes,
                                                 ["cbas"])
            self.cbas_util.wait_for_processes(self.cluster.kv_nodes,
                                                 ["memcached"])

        # Validate ingestion
        if not self.cbas_util.wait_for_ingestion_all_datasets(
            self.cluster, self.bucket_util):
            self.fail("Ingestion failed")
        self.log.info("test_analytics_with_killing_cbas_memcached completed ")

    def test_analytics_with_tampering_links(self):
        self.log.info("test_analytics_with_tampering_links started")
        create_datasets_task = CreateDatasetsTask(
            self.cluster, bucket_util=self.bucket_util,
            cbas_name_cardinality=self.input.param('cardinality', None),
            cbas_util=self.cbas_util,
            kv_name_cardinality=self.input.param('bucket_cardinality', None),
            creation_methods=["cbas_collection", "cbas_dataset"],
            ds_per_collection=self.input.param('ds_per_collection', 1))
        self.task_manager.add_new_task(create_datasets_task)
        dataset_creation_result = self.task_manager.get_task_result(
            create_datasets_task)
        if not dataset_creation_result:
            self.fail("Datasets creation failed")
        links = [dataverse + ".Local" for dataverse in
                 self.cbas_util.dataverses.keys()] * self.input.param(
                     "tamper_links_count", 0)
        connect_disconnect_task = self.cbas_util.start_connect_disconnect_links_task(
            self.cluster, links=links)
        if connect_disconnect_task:
            if connect_disconnect_task.exception:
                self.task_manager.get_task_result(connect_disconnect_task)
            self.task_manager.stop_task(connect_disconnect_task)
        if not self.cbas_util.wait_for_ingestion_all_datasets(
            self.cluster, self.bucket_util):
            self.fail("Ingestion failed")
        self.log.info("test_analytics_with_tampering_links completed")

    def test_create_drop_datasets_in_loop(self):
        self.log.info("test_create_drop_datasets started")
        for _ in range(self.iterations):
            self.log.info("ITERATION: " + str(_), "DEBUG")

            create_task = CreateDatasetsTask(
                self.cluster, self.bucket_util, self.cbas_util,
                cbas_name_cardinality=3, kv_name_cardinality=3)
            self.task_manager.add_new_task(create_task)
            self.task_manager.get_task_result(create_task)

            self.cbas_util.wait_for_ingestion_all_datasets(
                self.cluster, self.bucket_util)

            drop_task = DropDatasetsTask(
                self.cluster, self.cbas_util, kv_name_cardinality=3)
            self.task_manager.add_new_task(drop_task)
            self.task_manager.get_task_result(drop_task)

            self.log.info("test_create_drop_datasets completed")

    def test_multiple_datasets_on_collection(self):
        self.log.info("TEST_MULTIPLE_DATASETS STARTED")
        ds_per_collection = int(self.input.param("ds_per_collection", 5))
        if self.parallel_load_percent <= 0:
            self.parallel_load_percent = 100
        create_task = CreateDatasetsTask(
            self.cluster, self.bucket_util, self.cbas_util, 3, 3,
            ds_per_collection=ds_per_collection,
            ds_per_dv=int(self.input.param("ds_per_dv", 1)))
        self.start_data_load_task(
            percentage_per_collection=self.parallel_load_percent)
        self.task_manager.add_new_task(create_task)
        self.task_manager.get_task_result(create_task)
        self.wait_for_data_load_task()
        self.cbas_util.wait_for_ingestion_all_datasets(self.cluster, self.bucket_util)
        self.log.info("TEST_MULTIPLE_DATASETS COMPLETED")
