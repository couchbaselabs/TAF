'''
Created on 30-August-2020

@author: umang.agrawal
'''

from TestInput import TestInputSingleton
from cbas.cbas_base import CBASBaseTest
import random, json
from threading import Thread
from BucketLib.BucketOperations import BucketHelper
from security.rbac_base import RbacBase
from Queue import Queue
from CbasLib.cbas_entity import Dataverse,Synonym,CBAS_Index
from CbasLib.CBASOperations import CBASHelper
from cbas_utils.cbas_utils_v2 import CreateDatasetsOnAllCollectionsTask, RunSleepQueryOnDatasetsTask
from math import ceil
from collections_helper.collections_spec_constants import MetaCrudParams
from bucket_utils.bucket_ready_functions import DocLoaderUtils

class CBASDataverseAndScopes(CBASBaseTest):

    def setUp(self):
        
        self.input = TestInputSingleton.input
        if "default_bucket" not in self.input.test_params:
            self.input.test_params.update({"default_bucket": False})
        super(CBASDataverseAndScopes, self).setUp()
        
        if self.cbas_spec_name:
            self.cbas_spec = self.cbas_util_v2.get_cbas_spec(self.cbas_spec_name)
            self.cbas_util_v2.update_cbas_spec(
                self.cbas_spec,
                {"no_of_dataverses":self.input.param('no_of_dv', 1),
                 "max_thread_count":self.input.param('no_of_threads', 1)},
                "dataverse")
            if not self.cbas_util_v2.create_dataverse_from_spec(self.cbas_spec):
                self.fail("Error while creating Dataverses from CBAS spec")
        
        self.log_setup_status("CBASRebalance", "Finished", stage="setup")

    def tearDown(self):
        
        self.log_setup_status("CBASRebalance", "Started", stage="Teardown")
        
        super(CBASDataverseAndScopes, self).tearDown()
        
        self.log_setup_status("CBASRebalance", "Finished", stage="Teardown")
    
    def test_create_drop_dataverse(self):
        """
        This testcase verifies dataverse/cbas_scope creation and deletion.
        Supported Test params -
        :testparam cbas_spec str, name of the cbas spec file.
        """
        
        self.log.info("Performing validation in Metadata.Dataverse after creating dataverses")
        jobs = Queue()
        results = list()
        for dataverse in self.cbas_util_v2.dataverses:
            jobs.put(dataverse)
        self.cbas_util_v2.run_jobs_in_parallel(self.cbas_util_v2.validate_dataverse_in_metadata, jobs, results, 
                                               self.cbas_spec.get("max_thread_count",1), 
                                               async_run=False, consume_from_queue_func=None)
        if not all(results):
            self.fail("Dataverse creation failed for few dataverses")
        
        results = []
        if not self.cbas_util_v2.delete_cbas_infra_created_from_spec(
            continue_if_dataverse_drop_fail=False, delete_dataverse_object=False):
            self.fail("Error while dropping Dataverses created from CBAS spec")
        
        self.log.info("Performing validation in Metadata.Dataverse after dropping dataverses")
        for dataverse in self.cbas_util_v2.dataverses:
            if dataverse != "Default":
                jobs.put(dataverse)
        self.cbas_util_v2.run_jobs_in_parallel(self.cbas_util_v2.validate_dataverse_in_metadata, jobs, results, 
                                               self.cbas_spec.get("max_thread_count",1), 
                                               async_run=False, consume_from_queue_func=None)
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
            for i in range(0,self.input.param('cardinality', 0)):
                name_length = self.input.param('name_{0}'.format(str(i+1)), 255)
                name = self.cbas_util_v2.generate_name(
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
        
        if not self.cbas_util_v2.create_dataverse(
            dataverse_name=dataverse_name,
            validate_error_msg=self.input.param('validate_error', True),
            expected_error=error_msg,
            expected_error_code=self.input.param('error_code', None)):
            self.fail("Dataverse {0} creation passed when it should be failed".format(dataverse_name))
    
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
            for i in range(0,self.input.param('cardinality', 0)):
                name_length = self.input.param('name_{0}'.format(str(i+1)), 255)
                name = self.cbas_util_v2.generate_name(
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
        
        if not self.cbas_util_v2.create_analytics_scope(
            cbas_scope_name=dataverse_name,
            validate_error_msg=self.input.param('validate_error', True),
            expected_error=error_msg,
            expected_error_code=self.input.param('error_code', None)):
            self.fail("Dataverse {0} creation passed when it should be failed".format(dataverse_name))
    
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
        if not self.cbas_util_v2.drop_dataverse(
            dataverse_name=dataverse_name, validate_error_msg=True,
            expected_error=error_msg, expected_error_code=self.input.param('error_code', None)):
            self.fail("Dropping of Dataverse {0} failed".format(self.entity_name))
      
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
        if not self.cbas_util_v2.drop_analytics_scope(
            cbas_scope_name=dataverse_name, validate_error_msg=True,
            expected_error=error_msg, expected_error_code=self.input.param('error_code', None)):
            self.fail("Dropping of Dataverse {0} failed".format(self.entity_name))
    
    def test_use_statement(self):
        dataverse_name = CBASHelper.format_name(
            self.cbas_util_v2.generate_name(
                self.input.param('cardinality', 1)))
        if 0 < int(self.input.param('cardinality', 1)) < 3:
            if not self.cbas_util_v2.create_dataverse(
                dataverse_name=dataverse_name):
                self.fail("Creation of Dataverse {0} failed".format(dataverse_name))
        
        if self.input.param('cardinality', 1) == 3:
            dataverse_name = "part1.part2.part3"
        
        if self.input.param('split_name', 0):
            dataverse_name = dataverse_name.split(".")[self.input.param('split_name', 0) -1]
        cmd = "Use {0}".format(dataverse_name)
        self.log.debug("Executing cmd - \n{0}\n".format(cmd))
        status, metrics, errors, results, _ = \
            self.cbas_util.execute_statement_on_cbas_util(cmd)
        if status != "success":
            if not self.cbas_util_v2.validate_error_in_response(
                status, errors, 
                expected_error=self.input.param('error', None).format(dataverse_name)):
                self.fail("Validating error message failed. Error message was different from expected error")
            
class CBASDatasetsAndCollections(CBASBaseTest):

    def setUp(self):
        self.input = TestInputSingleton.input
        if self.input.param('setup_infra', True):
            if "bucket_spec" not in self.input.test_params:
                self.input.test_params.update({"bucket_spec": "analytics.default"})
        else:
            if "default_bucket" not in self.input.test_params:
                self.input.test_params.update({"default_bucket": False})
        
        super(CBASDatasetsAndCollections, self).setUp()
        if self.nodes_init > 2:
            init_nodes = list(filter(
                lambda node: node.ip != self.cluster.master.ip and node.ip != self.cbas_node.ip,
                self.cluster.servers))
            self.cluster_util.add_all_nodes_then_rebalance(init_nodes[:self.nodes_init - 2])
        self.parallel_load = self.input.param("parallel_load", False)
        self.parallel_load_percent = int(self.input.param("parallel_load_percent", 100))
        self.run_concurrent_query = self.input.param("run_query", False)
        self.log_setup_status("CBASRebalance", "Finished", stage="setup")

    def tearDown(self):
        
        self.log_setup_status("CBASRebalance", "Started", stage="Teardown")
        
        super(CBASDatasetsAndCollections, self).tearDown()
        
        self.log_setup_status("CBASRebalance", "Finished", stage="Teardown")
    
    def setup_for_test(self,update_spec={}, sub_spec_name=None):
        wait_for_ingestion = (not self.parallel_load)
        if self.cbas_spec_name:
            self.cbas_spec = self.cbas_util_v2.get_cbas_spec(self.cbas_spec_name)
            if update_spec:
                self.cbas_util_v2.update_cbas_spec(
                    self.cbas_spec, update_spec, sub_spec_name)
            cbas_infra_result = self.cbas_util_v2.create_cbas_infra_from_spec(
                self.cbas_spec, self.bucket_util,
                wait_for_ingestion=wait_for_ingestion)
            if not cbas_infra_result[0]:
                self.fail("Error while creating infra from CBAS spec -- " + cbas_infra_result[1])
    
    def start_data_load_task(self, doc_load_spec="initial_load",
                             async_load=True, percentage_per_collection=0,
                             batch_size=10, mutation_num=0):
        collection_load_spec = \
        self.bucket_util.get_crud_template_from_package(doc_load_spec)
        if percentage_per_collection > 0:
            collection_load_spec["doc_crud"][
                MetaCrudParams.DocCrud.CREATE_PERCENTAGE_PER_COLLECTION] =\
                 percentage_per_collection
        self.data_load_task = \
        self.bucket_util.run_scenario_from_spec(self.task,
                                                self.cluster,
                                                self.bucket_util.buckets,
                                                collection_load_spec,
                                                mutation_num=mutation_num,
                                                batch_size=batch_size,
                                                async_load=async_load)

    def start_query_task(self, datasets=[], sleep_time=5000):
        self.query_task = RunSleepQueryOnDatasetsTask(
            num_queries=self.num_concurrent_queries,
            cbas_util=self.cbas_util_v2, datasets=datasets,
            sleep_time=sleep_time)
        self.task_manager.add_new_task(self.query_task)
    
    def wait_for_data_load_task(self):
        if hasattr(self, "data_load_task") and self.data_load_task:
            self.task.jython_task_manager.get_task_result(self.data_load_task)
            # Validate data loaded in parallel
            DocLoaderUtils.validate_doc_loading_results(self.data_load_task)
            self.bucket_util._wait_for_stats_all_buckets()
            self.bucket_util.validate_docs_per_collections_all_buckets()
            self.data_load_task = None
    
    def wait_for_query_task(self):
        if hasattr(self, "query_task") and self.query_task:
            self.task_manager.get_task_result(self.query_task)
            if self.query_task.exception:
                raise self.query_task.exception
            self.query_task = None
    
    def caller(self, job):
        return job[0](**job[1])
            
    def wait_for_ingestion_all_datasets(self):
        self.cbas_util_v2.refresh_dataset_item_count(self.bucket_util)
        jobs = Queue()
        results = []
        datasets = self.cbas_util_v2.list_all_dataset_objs()
        if len(datasets) > 0:
            self.log.info("Waiting for data to be ingested into datasets")
            for dataset in datasets:
                jobs.put((self.cbas_util_v2.wait_for_ingestion_complete,
                          {"dataset_names":[dataset.full_name], "num_items":dataset.num_of_items}))
        self.cbas_util_v2.run_jobs_in_parallel(self.caller, jobs, results, 15, 
                                  async_run=False, consume_from_queue_func=None)
        if not all(results):
            self.fail("Ingestion did not complete")
    
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
            "no_of_dataverses":self.input.param('no_of_dv', 1),
            "no_of_datasets_per_dataverse":self.input.param('ds_per_dv', 1),
            "no_of_synonyms":0,
            "no_of_indexes":0,
            "max_thread_count":self.input.param('no_of_threads', 1),
            "creation_methods":["cbas_collection","cbas_dataset"]}
        # start parallel data loading
        if self.parallel_load:
            self.start_data_load_task(percentage_per_collection=self.parallel_load_percent)
        # start parallel queries
        if self.run_concurrent_query:
            self.start_query_task()
        
        self.setup_for_test(update_spec, "dataset")
        # wait for queries to complete
        self.wait_for_query_task()
        # wait for data load to complete
        self.wait_for_data_load_task()
        
        self.log.info("Performing validation in Metadata.Dataset after creating datasets")
        jobs = Queue()
        results = list()
        self.wait_for_ingestion_all_datasets()
        
        def populate_job_queue():
            for dataset in self.cbas_util_v2.list_all_dataset_objs():
                jobs.put(dataset)
        
        def validate_metadata(dataset):
            if not dataset.enabled_from_KV and not dataset.kv_scope:
                if not self.cbas_util_v2.validate_dataset_in_metadata(
                    dataset.name, dataset.dataverse_name, BucketName=dataset.kv_bucket.name):
                    return False
            else:
                if not self.cbas_util_v2.validate_dataset_in_metadata(
                    dataset.name, dataset.dataverse_name, BucketName=dataset.kv_bucket.name,
                    ScopeName=dataset.kv_scope.name, 
                    CollectionName=dataset.kv_collection.name):
                    return False
            return True
        
        def validate_doc_count(dataset):
            if not self.cbas_util_v2.validate_cbas_dataset_items_count(
                dataset.full_name, dataset.num_of_items):
                    return False
            return True
        
        populate_job_queue()
        self.cbas_util_v2.run_jobs_in_parallel(validate_metadata, jobs, results, 
                                               self.cbas_spec["max_thread_count"], 
                                               async_run=False, consume_from_queue_func=None)
        if not all(results):
            self.fail("Metadata validation for Datasets failed")
        
        self.log.info("Validating item count")
        results = []
        populate_job_queue()
        self.cbas_util_v2.run_jobs_in_parallel(validate_doc_count, jobs, results, 
                                               self.cbas_spec["max_thread_count"], 
                                               async_run=False, consume_from_queue_func=None)
        
        if not all(results):
            self.fail("Item count validation for Datasets failed")
        
        self.log.info("Drop datasets")
        if not self.cbas_util_v2.delete_cbas_infra_created_from_spec(
            continue_if_dataset_drop_fail=False, delete_dataverse_object=False):
            self.fail("Error while dropping CBAS entities created from CBAS spec")
        
        self.log.info("Performing validation in Metadata.Dataverse after dropping datasets")
        results = []
        populate_job_queue()
        self.cbas_util_v2.run_jobs_in_parallel(validate_metadata, jobs, results, 
                                               self.cbas_spec["max_thread_count"], 
                                               async_run=False, consume_from_queue_func=None)
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
        
        exclude_collections=[]
        if not self.input.param('consider_default_KV_collection', True):
            exclude_collections=["_default"]
        
        self.cbas_util_v2.create_dataset_obj(
            self.bucket_util,
            dataset_cardinality=self.input.param('cardinality', 1), 
            bucket_cardinality=self.input.param('bucket_cardinality', 3),
            enabled_from_KV=False, 
            name_length=self.input.param('name_length', 30), 
            fixed_length=self.input.param('fixed_length', False),
            exclude_bucket=[], exclude_scope=[], exclude_collection=exclude_collections, no_of_objs=1)
        dataset = self.cbas_util_v2.list_all_dataset_objs()[0]
        
        # Negative scenario 
        if self.input.param('error', None):
            error_msg = self.input.param('error', None)
        else:
            error_msg = None
        
        if self.input.param('invalid_kv_collection', False):
            dataset.kv_collection.name = "invalid"
            error_msg = error_msg.format(dataset.get_fully_qualified_kv_entity_name(3))
        elif self.input.param('invalid_kv_scope', False):
            dataset.kv_scope.name = "invalid"
            error_msg = error_msg.format(dataset.get_fully_qualified_kv_entity_name(2))
        elif self.input.param('invalid_dataverse', False):
            dataset.name  = "invalid." + dataset.name
            dataset.dataverse_name = ""
            error_msg = error_msg.format("invalid")
        elif self.input.param('no_dataset_name', False):
            dataset.name = ''
        elif self.input.param('remove_default_collection', False):
            error_msg = error_msg.format(
                CBASHelper.format_name(dataset.kv_bucket.name))
        # Negative scenario ends
        
        if not self.cbas_util_v2.create_dataset(
            dataset.name, dataset.get_fully_qualified_kv_entity_name(self.input.param('bucket_cardinality', 3)), 
            dataset.dataverse_name, 
            validate_error_msg=self.input.param('validate_error', False), 
            expected_error=error_msg, 
            analytics_collection=self.input.param('cbas_collection', False)):
            self.fail("Dataset creation failed")
        
        if not self.input.param('validate_error', False):
            if self.input.param('no_dataset_name', False):
                dataset.name = dataset.kv_bucket.name
                dataset.full_name = dataset.get_fully_qualified_kv_entity_name(1)
            if not self.cbas_util_v2.validate_dataset_in_metadata(dataset.name, dataset.dataverse_name):
                self.fail("Dataset entry not present in Metadata.Dataset")
            if not self.cbas_util_v2.validate_cbas_dataset_items_count(dataset.full_name, dataset.num_of_items):
                self.fail("Expected item count in dataset does not match actual item count")
        self.log.info("Test finished")
        
    def test_drop_non_existent_dataset(self):
        """
        This testcase verifies dataset deletion.
        Supported Test params -
        :testparam cbas_collection boolean
        """
        self.log.info("Test started")
        
        if not self.cbas_util_v2.drop_dataset(
            "invalid", validate_error_msg=True,
            expected_error="Cannot find dataset with name invalid",
            analytics_collection=self.input.param('cbas_collection', False)):
            self.fail("Successfully deleted non-existent dataset")
        self.log.info("Test finished")
        
    def test_create_analytics_collection(self):
        """Only dataset_creation_method parameter will change for these testcase"""
        self.test_create_dataset()
    
    def test_drop_non_existent_analytics_collection(self):
        """Only dataset_creation_method and dataset_drop_method parameter will change for these testcase"""
        self.test_drop_dataset()
        
    def test_enabling_disabling_analytics_collection_on_all_KV_collections(self):
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
            for bucket in self.bucket_util.buckets:
                for scope in self.bucket_util.get_active_scopes(bucket):
                    dataverse_name = bucket.name + "." + scope.name
                    dataverse_obj = self.cbas_util_v2.get_dataverse_obj(dataverse_name)
                    if not dataverse_obj:
                        dataverse_obj = Dataverse(dataverse_name)
                        self.cbas_util_v2.dataverses[dataverse_name] = dataverse_obj
                    jobs.put(dataverse_name)
            self.cbas_util_v2.run_jobs_in_parallel(self.cbas_util_v2.create_dataverse, jobs, results, 15)
            if not results:
                self.fail("Error while creating dataverses")
        
        self.cbas_util_v2.create_dataset_obj(self.bucket_util,bucket_cardinality=self.input.param('bucket_cardinality', 1),
                                             enabled_from_KV=True)
        
        dataset_objs = self.cbas_util_v2.list_all_dataset_objs()
        
        def consumer_func(job):
            return job[0](**job[1])
        
        def populate_job_queue(list_of_objs, func_name):
            for obj in list_of_objs:
                if func_name == self.cbas_util_v2.validate_dataset_in_metadata:
                    jobs.put((func_name,{"dataset_name":obj.name, "dataverse_name":obj.dataverse_name}))
                elif func_name == self.cbas_util_v2.wait_for_ingestion_complete:
                    jobs.put((func_name,{"dataset_names":[obj.full_name], "num_items":obj.num_of_items}))
                elif func_name == self.cbas_util_v2.validate_synonym_in_metadata:
                    jobs.put((func_name,{
                        "synonym_name":obj.name, "synonym_dataverse_name":obj.dataverse_name,
                        "cbas_entity_name":obj.cbas_entity_name, "cbas_entity_dataverse_name":obj.cbas_entity_dataverse}))
                elif func_name == self.cbas_util_v2.enable_analytics_from_KV or func_name == self.cbas_util_v2.disable_analytics_from_KV:
                    jobs.put(obj.get_fully_qualified_kv_entity_name(self.input.param('bucket_cardinality', 1)))
                elif func_name == self.cbas_util_v2.validate_dataverse_in_metadata:
                    jobs.put(obj.dataverse_name)
        
        
        populate_job_queue(dataset_objs, self.cbas_util_v2.enable_analytics_from_KV)
        if self.parallel_load:
            self.start_data_load_task(async_load=True, percentage_per_collection=self.parallel_load_percent)
        if self.run_concurrent_query:
            self.start_query_task()
        self.cbas_util_v2.run_jobs_in_parallel(self.cbas_util_v2.enable_analytics_from_KV, jobs, results, 1)
        self.wait_for_query_task()
        self.wait_for_data_load_task()
        
        if not all(results):
            self.fail("Error while enabling analytics collection from KV")
        
        self.cbas_util_v2.refresh_dataset_item_count(self.bucket_util)
        populate_job_queue(dataset_objs, self.cbas_util_v2.validate_dataset_in_metadata)
        self.cbas_util_v2.run_jobs_in_parallel(consumer_func, jobs, results, 15)
        
        if not all(results):
            self.fail("Error while validating the datasets in Metadata")
        
        self.wait_for_ingestion_all_datasets()

        
        populate_job_queue(self.cbas_util_v2.list_all_synonym_objs(), self.cbas_util_v2.validate_synonym_in_metadata)
        self.cbas_util_v2.run_jobs_in_parallel(consumer_func, jobs, results, 15)
        
        if not all(results):
            self.fail("Synonym was not created")
        
        if self.input.param('disable_from_kv', False):
            populate_job_queue(dataset_objs, self.cbas_util_v2.disable_analytics_from_KV)
            if self.parallel_load:
                self.start_data_load_task(percentage_per_collection=int(self.parallel_load_percent/2.5))
            self.cbas_util_v2.run_jobs_in_parallel(self.cbas_util_v2.disable_analytics_from_KV, jobs, results, 1)
            self.wait_for_data_load_task()
            if not all(results):
                self.fail("Error while disabling analytics collection from KV")
            
            populate_job_queue(dataset_objs, self.cbas_util_v2.validate_dataverse_in_metadata)
            self.cbas_util_v2.run_jobs_in_parallel(self.cbas_util_v2.validate_dataverse_in_metadata, jobs, results, 15)
            
            if not all(results):
                self.fail("Dataverse got dropped after disabling analytics from KV")
            
            results = []
            
            populate_job_queue(dataset_objs, self.cbas_util_v2.validate_dataset_in_metadata)
            self.cbas_util_v2.run_jobs_in_parallel(consumer_func, jobs, results, 15)
            
            if any(results):
                self.fail("Dataset entry is still present in Metadata dataset")
            
            populate_job_queue(self.cbas_util_v2.list_all_synonym_objs(), self.cbas_util_v2.validate_synonym_in_metadata)
            self.cbas_util_v2.run_jobs_in_parallel(consumer_func, jobs, results, 15)
            
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
        
        exclude_collections=[]
        if not self.input.param('consider_default_KV_collection', True):
            exclude_collections=["_default"]
        
        bucket_cardinality = self.input.param('bucket_cardinality', 1)
        
        self.cbas_util_v2.create_dataset_obj(self.bucket_util,bucket_cardinality=bucket_cardinality,
                                enabled_from_KV=True, exclude_collection=exclude_collections, 
                                no_of_objs=1)
        dataset = self.cbas_util_v2.list_all_dataset_objs()[0]
        
        if self.input.param('create_dataverse', False) and \
            not self.cbas_util_v2.create_dataverse(dataverse_name=dataset.dataverse_name):
            self.fail("Failed to create dataverse {0}".format(dataset.dataverse_name))
        
        # Negative scenarios
        if self.input.param('error', ''):
            error_msg = self.input.param('error', '')
        else:
            error_msg = ''
        
        if self.input.param('invalid_kv_collection', False):
            dataset.kv_collection.name = "invalid"
            error_msg = error_msg.format(
                CBASHelper.unformat_name(dataset.get_fully_qualified_kv_entity_name(bucket_cardinality)))
        elif self.input.param('invalid_kv_scope', False):
            dataset.kv_scope.name = "invalid"
            error_msg = error_msg.format(
                CBASHelper.unformat_name(dataset.get_fully_qualified_kv_entity_name(2)))
        elif self.input.param('invalid_kv_bucket', False):
            dataset.kv_bucket.name = "invalid"
            error_msg = error_msg.format("invalid")
        elif self.input.param('remove_default_collection', False):
            bucket_cardinality = 1
            dataset.kv_collection.name = "_default"
            error_msg = error_msg.format(dataset.get_fully_qualified_kv_entity_name(3))
        
        # Creating synonym before enabling analytics from KV
        if self.input.param('synonym_name', None) == "Bucket":
            synonym_name = dataset.get_fully_qualified_kv_entity_name(1)
            error_msg = error_msg.format(synonym_name.replace('`',''))
        elif self.input.param('synonym_name', None) == "Collection":
            synonym_name = dataset.get_fully_qualified_kv_entity_name(3)
        else:
            synonym_name = None
        
        if synonym_name and not self.cbas_util_v2.create_analytics_synonym(
            synonym_full_name=synonym_name, cbas_entity_full_name=dataset.full_name):
            self.fail("Error while creating synonym {0} on dataset {1}".format(
                synonym_name, dataset.full_name))
        
        if self.input.param('precreate_dataset', None):
            if self.input.param('precreate_dataset', None) == "Default":
                if not self.cbas_util_v2.create_dataset(
                    dataset.get_fully_qualified_kv_entity_name(1), 
                    dataset.get_fully_qualified_kv_entity_name(bucket_cardinality), 
                    dataverse_name="Default"):
                    self.fail("Error while creating dataset")
            else:
                if not self.cbas_util_v2.create_dataset(
                    dataset.name, 
                    dataset.get_fully_qualified_kv_entity_name(bucket_cardinality), 
                    dataverse_name=dataset.dataverse_name):
                    self.fail("Error while creating dataset")
                error_msg = error_msg.format(dataset.name, dataset.dataverse_name)
        # Negative scenario ends
        
        if not self.cbas_util_v2.enable_analytics_from_KV(
            dataset.get_fully_qualified_kv_entity_name(bucket_cardinality),
            compress_dataset=self.input.param('compress_dataset', False),
            validate_error_msg=self.input.param('validate_error', False), 
            expected_error=error_msg):
            self.fail("Error while enabling analytics collection from KV")
        
        if not self.input.param('validate_error', False):
            if not self.cbas_util_v2.validate_dataset_in_metadata(dataset_name=dataset.name, dataverse_name=dataset.dataverse_name):
                self.fail("Error while validating the datasets in Metadata")
            
            if not self.cbas_util_v2.wait_for_ingestion_complete(dataset_names=[dataset.full_name], num_items=dataset.num_of_items):
                self.fail("Data ingestion into the datasets did not complete")
        
        if self.input.param('verify_synonym', False):
            if not self.cbas_util_v2.validate_synonym_in_metadata(
                synonym_name=dataset.kv_bucket.name, synonym_dataverse_name="Default",
                cbas_entity_name=dataset.name, cbas_entity_dataverse_name=dataset.dataverse_name):
                self.fail("Synonym {0} is not created under Dataverse {1}".format(
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
        
        exclude_collections=[]
        if not self.input.param('consider_default_KV_collection', True):
            exclude_collections=["_default"]
        
        bucket_cardinality = self.input.param('bucket_cardinality', 1)
        
        self.cbas_util_v2.create_dataset_obj(self.bucket_util,bucket_cardinality=bucket_cardinality,
                                enabled_from_KV=True, exclude_collection=exclude_collections, 
                                no_of_objs=1)
        dataset = self.cbas_util_v2.list_all_dataset_objs()[0]
        
        if self.input.param('enable_analytics', True):
            if not self.cbas_util_v2.enable_analytics_from_KV(
                dataset.get_fully_qualified_kv_entity_name(bucket_cardinality),
                compress_dataset=self.input.param('compress_dataset', False)):
                self.fail("Error while enabling analytics collection from KV")
        else:
            if not self.cbas_util_v2.create_dataset(
                dataset.name, dataset.full_kv_entity_name, dataverse_name=dataset.dataverse_name):
                self.fail("Error creating dataset {0}".format(dataset.name))
        
        # Negative scenarios
        if self.input.param('error', None):
            error_msg = self.input.param('error', None)
        else:
            error_msg = None
            
        if self.input.param('invalid_kv_collection', False):
            dataset.kv_collection.name = "invalid"
            error_msg = error_msg.format("invalid",
                CBASHelper.unformat_name(dataset.dataverse_name))
        if self.input.param('invalid_kv_bucket', False):
            dataset.kv_bucket.name = "invalid"
            error_msg = error_msg.format("_default",
                CBASHelper.unformat_name("invalid","_default"))
        # Negative scenario ends
        
        if self.input.param('create_dataset', False):
            new_dataset_name = self.cbas_util_v2.generate_name()
            if not self.cbas_util_v2.create_dataset(
                new_dataset_name, dataset.full_kv_entity_name, dataverse_name=dataset.dataverse_name):
                self.fail("Error creating dataset {0}".format(new_dataset_name))
        
        if self.input.param('create_synonym', False):
            new_synonym_name = self.cbas_util_v2.generate_name()
            if not self.cbas_util_v2.create_analytics_synonym(
                synonym_full_name=CBASHelper.format_name(dataset.dataverse_name,new_synonym_name),
                cbas_entity_full_name=dataset.full_name):
                self.fail("Error creating synonym {0}".format(new_synonym_name))
        
        self.log.info("Disabling analytics from KV")
        if not self.cbas_util_v2.disable_analytics_from_KV(
            dataset.get_fully_qualified_kv_entity_name(bucket_cardinality),
                                 validate_error_msg=self.input.param('validate_error', False), 
                                 expected_error=error_msg):
            self.fail("Error while disabling analytics on KV collection")
        
        if not self.input.param('validate_error', False):
            self.log.info("Validate dataverse is not deleted")
            if not self.cbas_util_v2.validate_dataverse_in_metadata(
                dataset.dataverse_name):
                self.fail("Dataverse {0} got deleted after disabling analytics from KV".format(
                    dataset.dataverse_name))
            
            self.log.info("Validate dataset is deleted")
            if self.cbas_util_v2.validate_dataset_in_metadata(dataset.name, dataset.dataverse_name):
                self.fail("Dataset {0} is still present in Metadata.Dataset".format(
                    dataset.name))
            
            if self.input.param('create_dataset', False):
                if not self.cbas_util_v2.validate_dataset_in_metadata(
                    new_dataset_name, dataset.dataverse_name, BucketName=dataset.kv_bucket.name):
                    self.fail("Explicitly created dataset got deleted after disabling analytics from KV")
            
            if self.input.param('create_synonym', False):
                if not self.cbas_util_v2.validate_synonym_in_metadata(
                    synonym_name=new_synonym_name, synonym_dataverse_name=dataset.dataverse_name,
                    cbas_entity_name=dataset.name, cbas_entity_dataverse_name=dataset.dataverse_name):
                    self.fail("Explicitly created synonym got deleted after disabling analytics from KV")
        
        self.log.info("Test finished")
        
    def test_create_multiple_synonyms(self):
        self.log.info("Test started")
        update_spec = {
            "no_of_dataverses":self.input.param('no_of_dv', 2),
            "no_of_datasets_per_dataverse":self.input.param('ds_per_dv', 1),
            "no_of_synonyms":self.input.param('no_of_synonym', 10),
            "no_of_indexes":0,
            "max_thread_count":self.input.param('no_of_threads', 1)}
        
        if self.parallel_load:
            self.start_data_load_task(percentage_per_collection=self.parallel_load_percent)
        if self.run_concurrent_query:
            self.start_query_task()
        self.setup_for_test(update_spec,"dataset")
        self.wait_for_query_task()
        self.wait_for_data_load_task()
        
        synonyms =  self.cbas_util_v2.list_all_synonym_objs()
        
        jobs = Queue()
        results = list()
        
        def consumer_func(job):
            return job[0](**job[1])
        
        def populate_job_queue(func_name):
            for synonym in synonyms:
                if func_name == self.cbas_util_v2.validate_synonym_in_metadata:
                    jobs.put((func_name,{
                        "synonym_name":synonym.name, "synonym_dataverse_name":synonym.dataverse_name,
                        "cbas_entity_name":synonym.cbas_entity_name, "cbas_entity_dataverse_name":synonym.cbas_entity_dataverse}))
                elif func_name == self.cbas_util_v2.validate_synonym_doc_count:
                    jobs.put((func_name,{
                        "synonym_full_name":synonym.full_name, "cbas_entity_full_name":synonym.cbas_entity_full_name}))
        
        populate_job_queue(self.cbas_util_v2.validate_synonym_in_metadata)
        self.cbas_util_v2.run_jobs_in_parallel(consumer_func, jobs, results, 15)
        if not all(results):
            self.fail("Error while validating synonym in Metadata")
        
        populate_job_queue(self.cbas_util_v2.validate_synonym_doc_count)
        self.cbas_util_v2.run_jobs_in_parallel(consumer_func, jobs, results, 15)
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
        
        self.cbas_util_v2.create_dataset_obj(
            self.bucket_util, dataset_cardinality=3, bucket_cardinality=3,
            enabled_from_KV=False, name_length=30, fixed_length=False,
            exclude_bucket=[], exclude_scope=[], exclude_collection=[], 
            no_of_objs=1)
        ds_obj = self.cbas_util_v2.list_all_dataset_objs()[0]
        
        if not self.cbas_util_v2.create_dataset(
            ds_obj.name, ds_obj.full_kv_entity_name, dataverse_name=ds_obj.dataverse_name):
            self.fail("Error creating dataset {0}".format(ds_obj.name))
        
        if not self.cbas_util_v2.wait_for_ingestion_complete([ds_obj.full_name], ds_obj.num_of_items):
            self.fail("Data ingestion into dataset failed.")
        
        if self.input.param('synonym_dataverse', "new") == "new":
            dv_name = CBASHelper.format_name(self.cbas_util_v2.generate_name(2))
            if not self.cbas_util_v2.create_dataverse(dv_name):
                self.fail("Error while creating dataverse")
        else:
            dv_name = ds_obj.dataverse_name
        
        if self.input.param('synonym_name', "new") == "new":
            synonym_name = CBASHelper.format_name(self.cbas_util_v2.generate_name(1))
        else:
            synonym_name = ds_obj.name
        
        if self.input.param('dangling_synonym', False):
            ds_obj.name = "invalid"
        if self.input.param('invalid_dataverse', False):
            dv_name = "invalid"
        
        syn_obj = Synonym(synonym_name, ds_obj.name, ds_obj.dataverse_name, dataverse_name=dv_name,
                          synonym_on_synonym=False)
        synonym_objs.append(syn_obj)
        
        if self.input.param('synonym_on_synonym', False):
            if self.input.param('different_syn_on_syn_dv', False):
                dv_name = CBASHelper.format_name(self.cbas_util_v2.generate_name(2))
                if not self.cbas_util_v2.create_dataverse(dv_name):
                    self.fail("Error while creating dataverse")
            
            if not self.input.param('same_syn_on_syn_name', False):
                synonym_name = CBASHelper.format_name(self.cbas_util_v2.generate_name(1))
            new_syn_obj = Synonym(
                synonym_name, syn_obj.name, syn_obj.dataverse_name, 
                dataverse_name=dv_name, synonym_on_synonym=True)
            synonym_objs.append(new_syn_obj)
        
        for obj in synonym_objs:
            if self.input.param('same_syn_on_syn_name', False) and obj.synonym_on_synonym:
                self.input.test_params.update({"validate_error": True})
                error_msg = error_msg.format(CBASHelper.unformat_name(synonym_name))
            if not self.cbas_util_v2.create_analytics_synonym(
                obj.full_name, obj.cbas_entity_full_name,
                validate_error_msg=self.input.param('validate_error', False), 
                expected_error=error_msg):
                self.fail("Error while creating synonym")
        
        if self.input.param("action_on_dataset", None):
            self.log.info("Dropping Dataset")
            if not self.cbas_util_v2.drop_dataset(ds_obj.full_name):
                self.fail("Error while dropping dataset")
            
            if self.input.param("action_on_dataset", None) == "recreate":
                self.log.info("Recreating dataset")
                if not self.cbas_util_v2.create_dataset(
                    ds_obj.name, ds_obj.full_kv_entity_name, dataverse_name=ds_obj.dataverse_name):
                    self.fail("Error creating dataset {0}".format(ds_obj.name))
                if not self.cbas_util_v2.wait_for_ingestion_complete([ds_obj.full_name],ds_obj.num_of_items):
                    self.fail("data ingestion into dataset failed.")
        
        if self.input.param("action_on_synonym", None):
            self.log.info("Dropping Synonym")
            if not self.cbas_util_v2.drop_analytics_synonym(syn_obj.full_name):
                self.fail("Error while dropping synonym")
            if self.input.param("action_on_synonym", None) == "recreate":
                self.log.info("Recreating synonym")
                if not self.cbas_util_v2.create_analytics_synonym(
                    syn_obj.full_name, syn_obj.cbas_entity_full_name):
                    self.fail("Error while recreating synonym")
            else:
                synonym_objs.remove(syn_obj)
        
        if not self.input.param('validate_error', False):
            for obj in synonym_objs:
                if not self.cbas_util_v2.validate_synonym_in_metadata(
                    obj.name, obj.dataverse_name, obj.cbas_entity_name, obj.cbas_entity_dataverse):
                    self.fail("Error while validating synonym in Metadata")
                
                if not self.cbas_util_v2.validate_synonym_doc_count(
                    obj.full_name, obj.cbas_entity_full_name,
                    validate_error_msg=self.input.param('validate_query_error', False),
                    expected_error=self.input.param('query_error', '').format(
                        CBASHelper.unformat_name(obj.name), obj.dataverse_name)):
                    self.fail("Error while validating synonym doc count for synonym")                    
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
        synonym_name = CBASHelper.format_name(self.cbas_util_v2.generate_name(1))
        object_name_1 = CBASHelper.format_name(self.cbas_util_v2.generate_name(1))
        object_name_2 = CBASHelper.format_name(self.cbas_util_v2.generate_name(1))
        
        for obj_name in [object_name_1, object_name_2]:
            if not self.cbas_util_v2.create_analytics_synonym(
                synonym_name, obj_name, if_not_exists=True):
                self.fail("Error while creating synonym {0}".format(synonym_name))
        
        if not self.cbas_util_v2.validate_synonym_in_metadata(
            synonym_name, "Default", object_name_1, "Default"):
            self.fail("Synonym metadata entry changed with subsequent synonym creation")
        
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
        
        self.cbas_util_v2.create_dataset_obj(
            self.bucket_util, dataset_cardinality=3, bucket_cardinality=3,
            enabled_from_KV=False, name_length=30, fixed_length=False,
            exclude_bucket=[], exclude_scope=[], exclude_collection=[], 
            no_of_objs=2)
        dataset_objs = self.cbas_util_v2.list_all_dataset_objs()
        
        for dataset in dataset_objs:
            if not self.cbas_util_v2.create_dataset(
                dataset.name, dataset.full_kv_entity_name, dataverse_name=dataset.dataverse_name):
                self.fail("Error creating dataset {0}".format(dataset.name))
            if self.input.param('dangling_synonym', False):
                break
        
        syn_obj = Synonym(dataset_objs[0].name, dataset_objs[1].name, dataset_objs[1].dataverse_name, 
                          dataverse_name=dataset_objs[0].dataverse_name, synonym_on_synonym=False)
        synonym_objs.append(syn_obj)
        
        if self.input.param('synonym_on_synonym', False):
            if self.input.param('different_syn_on_syn_dv', False):
                dv_name = CBASHelper.format_name(self.cbas_util_v2.generate_name(2))
                if not self.cbas_util_v2.create_dataverse(dv_name):
                    self.fail("Error while creating dataverse")
            else:
                dv_name = dataset_objs[0].dataverse_name
            new_syn_obj = Synonym(
                self.cbas_util_v2.generate_name(1), syn_obj.name, syn_obj.dataverse_name, 
                dataverse_name=dv_name, synonym_on_synonym=True)
            synonym_objs.append(new_syn_obj)
        
        for obj in synonym_objs:
            if not self.cbas_util_v2.create_analytics_synonym(
                obj.full_name, obj.cbas_entity_full_name):
                self.fail("Error while creating synonym")
        
        if not self.cbas_util_v2.validate_synonym_doc_count(
            syn_obj.full_name, dataset_objs[0].full_name):
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
        
        self.cbas_util_v2.create_dataset_obj(
            self.bucket_util, dataset_cardinality=3, bucket_cardinality=3,
            enabled_from_KV=False, name_length=30, fixed_length=False,
            exclude_bucket=[], exclude_scope=[], exclude_collection=[], 
            no_of_objs=1)
        ds_obj = self.cbas_util_v2.list_all_dataset_objs()[0]
        
        if not self.cbas_util_v2.create_dataset(
            ds_obj.name, ds_obj.full_kv_entity_name, dataverse_name=ds_obj.dataverse_name):
            self.fail("Error creating dataset {0}".format(ds_obj.name))
        
        syn_obj = Synonym(self.cbas_util_v2.generate_name(1), ds_obj.name, ds_obj.dataverse_name, 
                          dataverse_name=ds_obj.dataverse_name, synonym_on_synonym=False)
        
        if not self.cbas_util_v2.create_analytics_synonym(
            syn_obj.full_name, syn_obj.cbas_entity_full_name):
            self.fail("Error while creating synonym")
        
        if self.input.param('invalid_synonym', False):
            syn_obj.name = "invalid"
        
        self.log.info("Dropping synonym")
        if not self.cbas_util_v2.drop_analytics_synonym(
            CBASHelper.format_name(syn_obj.dataverse_name, syn_obj.name), 
            validate_error_msg=self.input.param('validate_error', False), 
            expected_error=self.input.param('error', '').format(syn_obj.name)):
            self.fail("Error while dropping Synonym")
        
        self.log.info("Validate Dataset item count after dropping synonym")
        if not self.input.param('validate_error', False) and not\
         self.cbas_util_v2.validate_cbas_dataset_items_count(
             ds_obj.full_name, ds_obj.num_of_items):
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
        self.bucket_util.flush_bucket(self.cluster.master, bucket_obj)

        self.log.info("Validating scope/collections mapping and doc_count")
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.validate_docs_per_collections_all_buckets()

        # Print bucket stats
        self.bucket_util.print_bucket_stats()
    
    def load_initial_data(self, doc_loading_spec=None, async_load=False,
                          validate_task=True):
        """
        Reload same data from initial_load spec template to validate
        post bucket flush collection stability
        :return: None
        """
        self.log.info("Loading same docs back into collections")
        
        if not doc_loading_spec:
            doc_loading_spec = \
                self.bucket_util.get_crud_template_from_package("initial_load")

        doc_loading_task = \
            self.bucket_util.run_scenario_from_spec(
                self.task,
                self.cluster,
                self.bucket_util.buckets,
                doc_loading_spec,
                mutation_num=0,
                batch_size=self.batch_size,
                async_load=async_load,
                validate_task=validate_task)
        
        if doc_loading_task.result is False:
            self.fail("Post flush doc_creates failed")

        # Print bucket stats
        self.bucket_util.print_bucket_stats()

        self.log.info("Validating scope/collections mapping and doc_count")
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.validate_docs_per_collections_all_buckets()
    
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
        
        if not self.cbas_util_v2.create_datasets_on_all_collections(
            self.bucket_util, cbas_name_cardinality=3, kv_name_cardinality=3, 
            remote_datasets=False):
            self.fail("Error while creating datasets")
            
        dataset_objs = self.cbas_util_v2.list_all_dataset_objs()
        
        bucket = random.choice(self.bucket_util.buckets)
        self.bucket_flush_and_validate(bucket)
        
        for dataset_obj in dataset_objs:
            self.log.info("Validating item count")
            if not self.cbas_util_v2.validate_cbas_dataset_items_count(
                dataset_obj.full_name, dataset_obj.kv_collection.num_items):
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
        
        self.cbas_util_v2.create_dataset_obj(
            self.bucket_util, dataset_cardinality=3, bucket_cardinality=3,
            enabled_from_KV=False, name_length=30, fixed_length=False,
            exclude_bucket=[], exclude_scope=[], exclude_collection=[], 
            no_of_objs=1)
        dataset_obj = self.cbas_util_v2.list_all_dataset_objs()[0]
        
        if not self.cbas_util_v2.create_dataset(
            dataset_obj.name, dataset_obj.full_kv_entity_name, dataverse_name=dataset_obj.dataverse_name):
            self.fail("Error creating dataset {0}".format(dataset_obj.name))
        
        for i in range(0,int(self.input.param('no_of_flushes', 1))):
            self.bucket_flush_and_validate(dataset_obj.kv_bucket)
            self.sleep(10, "Waiting for flush to complete")
            self.log.info("Validating item count in dataset before adding new data in KV")
            if not self.cbas_util_v2.validate_cbas_dataset_items_count(
                dataset_obj.full_name, dataset_obj.kv_collection.num_items):
                self.fail("Data is still present in dataset, even when KV collection\
                on which the dataset was created was flushed.")
            
            if self.input.param('reload_data', True):
                self.load_initial_data()
                self.log.info("Validating item count in dataset after adding new data in KV")
                if not self.cbas_util_v2.validate_cbas_dataset_items_count(
                    dataset_obj.full_name, dataset_obj.kv_collection.num_items):
                    self.fail("Newly added data in KV collection did not get ingested in\
                    dataset after flushing")
            i += 1
        
        self.log.info("Test finished")
    
    def test_dataset_for_adding_new_docs_while_flushing(self):
        self.log.info("Test started")
        
        self.cbas_util_v2.create_dataset_obj(
            self.bucket_util, dataset_cardinality=3, bucket_cardinality=3,
            enabled_from_KV=False, name_length=30, fixed_length=False,
            exclude_bucket=[], exclude_scope=[], exclude_collection=[], 
            no_of_objs=1)
        dataset_obj = self.cbas_util_v2.list_all_dataset_objs()[0]
        
        if not self.cbas_util_v2.create_dataset(
            dataset_obj.name, dataset_obj.full_kv_entity_name, dataverse_name=dataset_obj.dataverse_name):
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

        thread2 = Thread(target=self.load_initial_data,
                         name="data_load_thread",
                         args=(doc_loading_spec,False,False,))
        thread2.start()
        threads.append(thread2)

        for thread in threads:
            thread.join()
        
        self.log.info("Validating item count in dataset")
        if not self.cbas_util_v2.validate_cbas_dataset_items_count(
            dataset_obj.full_name, dataset_obj.kv_collection.num_items):
            self.fail("Number of docs in dataset does not match docs in KV collection")
        
        self.log.info("Test finished")
    
    def test_dataset_when_KV_flushing_during_data_mutation(self):
        self.log.info("Test started")
        
        self.cbas_util_v2.create_dataset_obj(
            self.bucket_util, dataset_cardinality=3, bucket_cardinality=3,
            enabled_from_KV=False, name_length=30, fixed_length=False,
            exclude_bucket=[], exclude_scope=[], exclude_collection=[], 
            no_of_objs=1)
        dataset_obj = self.cbas_util_v2.list_all_dataset_objs()[0]
        
        if not self.cbas_util_v2.create_dataset(
            dataset_obj.name, dataset_obj.full_kv_entity_name, dataverse_name=dataset_obj.dataverse_name):
            self.fail("Error creating dataset {0}".format(dataset_obj.name))
        
        doc_loading_spec = \
                self.bucket_util.get_crud_template_from_package("initial_load")
        doc_loading_spec["doc_crud"]["create_percentage_per_collection"] = 50
        
        self.load_initial_data(doc_loading_spec, True)
        self.bucket_flush_and_validate(dataset_obj.kv_bucket)
                
        self.log.info("Validating item count in dataset")
        if not self.cbas_util_v2.validate_cbas_dataset_items_count(
            dataset_obj.full_name, dataset_obj.kv_collection.num_items):
            self.fail("Number of docs in dataset does not match docs in KV collection")
        
        self.log.info("Test finished")
        
    def test_docs_deleted_in_dataset_once_MaxTTL_reached(self):
        self.log.info("Test started")
        buckets_spec = self.bucket_util.get_bucket_template_from_package(
            "analytics.multi_bucket")
        doc_loading_spec = None
        
        docTTL = int(self.input.param('docTTL', 0))
        collectionTTL = int(self.input.param('collectionTTL', 0))
        bucketTTL = int(self.input.param('bucketTTL', 0))
        
        selected_bucket = random.choice(buckets_spec["buckets"].keys())
        
        if bucketTTL:
            buckets_spec["buckets"][selected_bucket]["maxTTL"] = bucketTTL
        
        if collectionTTL:
            selected_scope = random.choice(buckets_spec["buckets"][selected_bucket]["scopes"].keys())
            selected_collection = random.choice(buckets_spec["buckets"][selected_bucket]["scopes"][
                selected_scope]["collections"].keys())
            buckets_spec["buckets"][selected_bucket]["scopes"][selected_scope][
                "collections"][selected_collection]["maxTTL"] = collectionTTL
            selected_collection = CBASHelper.format_name(selected_bucket,selected_scope,selected_collection)
        
        if docTTL:
            doc_loading_spec = self.bucket_util.get_crud_template_from_package("initial_load")
            doc_loading_spec["doc_ttl"] = docTTL
        
        self.collectionSetUp(self.cluster, self.bucket_util, self.cluster_util, True, buckets_spec, doc_loading_spec)
        self.bucket_util._expiry_pager()
        
        if not self.cbas_util_v2.create_datasets_on_all_collections(
            self.bucket_util, cbas_name_cardinality=3, kv_name_cardinality=3):
            self.fail("Dataset creation failed")
        
        datasets = self.cbas_util_v2.list_all_dataset_objs()
        for dataset in datasets:
            if not self.cbas_util_v2.wait_for_ingestion_complete([dataset.full_name], dataset.num_of_items):
                self.fail("Error while data ingestion into dataset")
        
        self.sleep(200, "waiting for maxTTL to complete")
        
        self.log.info("Validating item count")
        for dataset in datasets:
            if docTTL:
                if not self.cbas_util_v2.validate_cbas_dataset_items_count(dataset.full_name,0):
                    self.fail("Docs are still present in the dataset even after DocTTl reached")
            elif bucketTTL:
                if dataset.kv_bucket.name == selected_bucket:
                    if not self.cbas_util_v2.validate_cbas_dataset_items_count(dataset.full_name,0):
                        self.fail("Docs are still present in the dataset even after bucketTTl reached")
                else:
                    if not self.cbas_util_v2.validate_cbas_dataset_items_count(dataset.full_name,dataset.num_of_items):
                        self.fail("Docs are deleted from datasets when it should not have been deleted")
            else:
                if dataset.full_kv_entity_name == selected_collection:
                    if not self.cbas_util_v2.validate_cbas_dataset_items_count(dataset.full_name,0):
                        self.fail("Docs are still present in the dataset even after CollectionTTl reached")
                else:
                    if not self.cbas_util_v2.validate_cbas_dataset_items_count(dataset.full_name,dataset.num_of_items):
                        self.fail("Docs are deleted from datasets when it should not have been deleted")
        self.log.info("Test finished")
    
    def test_create_query_drop_on_multipart_name_secondary_index(self):
        """
        This testcase verifies secondary index creation, querying using index and 
        dropping of index.
        Supported Test params -
        :testparam analytics_index boolean, whether to use create/drop index or 
        create/drop analytics index statements to create index
        """
        self.log.info("Test started")
        
        statement = 'SELECT VALUE v FROM {0} v WHERE age > 2'
        if not self.cbas_util_v2.create_datasets_on_all_collections(
            self.bucket_util, cbas_name_cardinality=3, kv_name_cardinality=1):
            self.fail("Dataset creation failed")
        
        dataset_objs = self.cbas_util_v2.list_all_dataset_objs()
        count = 0
        
        if self.parallel_load:
            self.start_data_load_task(percentage_per_collection=self.parallel_load_percent)
        if self.run_concurrent_query:
            self.start_query_task()
        for dataset in dataset_objs:
            count += 1
            index = CBAS_Index(
                "idx_{0}".format(count), dataset.name, dataset.dataverse_name, 
                indexed_fields=self.input.param('index_fields', None))
        
            if not self.cbas_util_v2.create_cbas_index(
                index.name, index.indexed_fields, index.full_dataset_name, 
                analytics_index=self.input.param('analytics_index', False)):
                self.fail("Failed to create index on dataset {0}".format(dataset.name))
            
            dataset.indexes[index.name] = index            
            if not self.cbas_util_v2.verify_index_created(index.name, index.dataset_name, index.indexed_fields):
                self.fail("Index {0} on dataset {1} was not created.".format(index.name, index.dataset_name))
            
            if self.input.param('verify_index_on_synonym', False):
                self.log.info("Creating synonym")
                synonym = Synonym(
                    self.cbas_util_v2.generate_name(), dataset.name, dataset.dataverse_name, dataset.dataverse_name)
                
                if not self.cbas_util_v2.create_analytics_synonym(
                    synonym.full_name, synonym.cbas_entity_full_name, if_not_exists=True):
                    self.fail("Error while creating synonym")
                
                statement = statement.format(synonym.full_name)
            else:
                statement = statement.format(dataset.full_name)
            
            if not self.cbas_util_v2.verify_index_used(statement, True, index.name):
                self.fail("Index was not used while querying the dataset")
            
            if not self.cbas_util_v2.drop_cbas_index(
                index.name, index.full_dataset_name, 
                analytics_index=self.input.param('analytics_index', False)):
                self.fail("Drop index query failed")
        self.wait_for_query_task()
        self.wait_for_data_load_task()
        self.log.info("Test finished")
    
    def test_create_secondary_index_on_synonym(self):
        
        self.log.info("Test started")
        self.cbas_util_v2.create_dataset_obj(
            self.bucket_util, dataset_cardinality=3, bucket_cardinality=3, 
            enabled_from_KV=False, no_of_objs=1)
        dataset = self.cbas_util_v2.list_all_dataset_objs()[0]
        
        if not self.cbas_util_v2.create_dataset(dataset.name, dataset.full_kv_entity_name, dataset.dataverse_name):
            self.fail("Failed to create dataset")
        
        self.log.info("Creating synonym")
        synonym = Synonym(
            self.cbas_util_v2.generate_name(), dataset.name, dataset.dataverse_name, dataset.dataverse_name)
        
        if not self.cbas_util_v2.create_analytics_synonym(
            synonym.full_name, synonym.cbas_entity_full_name, if_not_exists=True):
            self.fail("Error while creating synonym")
        
        index = CBAS_Index(
            self.index_name, synonym.name, synonym.dataverse_name, 
            indexed_fields=self.input.param('index_fields', None))
        
        expected_error = "Cannot find dataset with name {0} in dataverse {1}".format(
            CBASHelper.unformat_name(synonym.name), synonym.dataverse_name)
        
        if not self.cbas_util_v2.create_cbas_index(
            index.name, index.indexed_fields, index.full_dataset_name, 
            analytics_index=self.input.param('analytics_index', False),
            validate_error_msg=True, 
            expected_error=expected_error):
            self.fail("Index was successfully created on synonym")
                    
        self.log.info("Test finished")
    
    def test_dataset_after_deleting_and_recreating_KV_entity(self):
        
        self.log.info("Test started")
        
        jobs = Queue()
        
        if not self.cbas_util_v2.create_datasets_on_all_collections(
            self.bucket_util, 
            cbas_name_cardinality=self.input.param('cardinality', False), 
            kv_name_cardinality=self.input.param('bucket_cardinality', False)):
            self.fail("Dataset creation failed")
        
        dataset_objs = self.cbas_util_v2.list_all_dataset_objs()
        
        def execute_function_in_parallel(func_name, num_items=None):
            results = list()
            def consumer_func(job):
                return job[0](**job[1])
            
            count = 0
            for dataset in dataset_objs:
                if func_name == self.cbas_util_v2.wait_for_ingestion_complete:
                    jobs.put((func_name, {"dataset_names":[dataset.full_name], "num_items":dataset.num_of_items}))
                elif func_name == self.cbas_util_v2.create_cbas_index: 
                    index = CBAS_Index("idx_{0}".format(count), dataset.name, dataset.dataverse_name, 
                                       indexed_fields=self.input.param('index_fields', None))
                    count += 1
                    dataset.indexes[index.name] = index
                    jobs.put((func_name, 
                              {"index_name":index.name, "indexed_fields": index.indexed_fields, 
                               "dataset_name":index.full_dataset_name, 
                               "analytics_index":self.input.param('analytics_index', False)}))
                elif func_name == self.cbas_util_v2.verify_index_used:
                    statement = 'SELECT VALUE v FROM {0} v WHERE age > 2'
                    for index in dataset.indexes.values():
                        jobs.put((func_name,
                                  {"statement":statement.format(index.full_dataset_name), 
                                   "index_used":True, "index_name":index.name}))
                else:
                    if num_items is not None:
                        jobs.put((func_name, {"dataset_name":dataset.full_name, "expected_count":num_items}))
                    else:
                        jobs.put((func_name, {"dataset_name":dataset.full_name, "expected_count":dataset.num_of_items}))
            
            self.cbas_util_v2.run_jobs_in_parallel(consumer_func, jobs, results, 15, async_run=False)
                
            if not all(results):
                if func_name == self.cbas_util_v2.wait_for_ingestion_complete:
                    self.fail("Data ingestion into datasets failed.")
                elif func_name == self.cbas_util_v2.create_cbas_index:
                    self.fail("Index creation on datasets failed")
                elif func_name == self.cbas_util_v2.verify_index_used:
                    self.fail("Index was not used while executing query")
                else:
                    self.fail("Expected no. of items in dataset does not match the actual no. of items")
        
        execute_function_in_parallel(self.cbas_util_v2.wait_for_ingestion_complete)
        execute_function_in_parallel(self.cbas_util_v2.create_cbas_index)
        execute_function_in_parallel(self.cbas_util_v2.verify_index_used)
        
        if not self.bucket_util.delete_bucket(
            self.cluster.master, dataset_objs[0].kv_bucket, 
            wait_for_bucket_deletion=True):
            self.fail("Error while deleting bucket")
        
        execute_function_in_parallel(self.cbas_util_v2.validate_cbas_dataset_items_count,0)
        
        self.collectionSetUp(self.cluster, self.bucket_util, self.cluster_util)
        
        execute_function_in_parallel(self.cbas_util_v2.validate_cbas_dataset_items_count)
        execute_function_in_parallel(self.cbas_util_v2.verify_index_used)
        
        self.log.info("Test finished")
        
    def test_KV_collection_deletion_does_not_effect_dataset_on_other_collections(self):
        self.log.info("Test started")
        
        statement = 'SELECT VALUE v FROM {0} v WHERE age > 2'
        
        if not self.cbas_util_v2.create_datasets_on_all_collections(
            self.bucket_util, 
            cbas_name_cardinality=self.input.param('cardinality', None), 
            kv_name_cardinality=self.input.param('bucket_cardinality', None)):
            self.fail("Dataset creation failed")
        
        dataset_objs = self.cbas_util_v2.list_all_dataset_objs()
        count = 0
        
        for dataset in dataset_objs:
            count += 1
            index = CBAS_Index(
                "idx_{0}".format(count), dataset.name, dataset.dataverse_name, 
                indexed_fields=self.input.param('index_fields', None))
        
            if not self.cbas_util_v2.create_cbas_index(
                index.name, index.indexed_fields, index.full_dataset_name, 
                analytics_index=self.input.param('analytics_index', False)):
                self.fail("Failed to create index on dataset {0}".format(dataset.name))
            
            dataset.indexes[index.name] = index
            
            if not self.cbas_util_v2.verify_index_created(index.name, index.dataset_name, index.indexed_fields):
                self.fail("Index {0} on dataset {1} was not created.".format(index.name, index.dataset_name))
            
            if not self.cbas_util_v2.verify_index_used(statement.format(dataset.full_name), True, index.name):
                self.fail("Index was not used while querying the dataset")
        
        selected_dataset = random.choice(dataset_objs)
        collections = self.bucket_util.get_active_collections(
            selected_dataset.kv_bucket, selected_dataset.kv_scope.name, True)
        collections.remove(selected_dataset.kv_collection.name) 
        collection_to_delete = random.choice(collections)
        self.bucket_util.drop_collection(self.cluster.master, selected_dataset.kv_bucket, 
                                         scope_name=selected_dataset.kv_scope.name, collection_name=collection_to_delete)
        collection_to_delete = CBASHelper.format_name(selected_dataset.get_fully_qualified_kv_entity_name(2), collection_to_delete)
        for dataset in dataset_objs:
            if dataset.full_kv_entity_name != collection_to_delete:
                if not self.cbas_util_v2.validate_cbas_dataset_items_count(
                    dataset.full_name, dataset.num_of_items):
                    self.fail("KV collection deletion affected data in datasets that were not creation on the deleted KV collection")
                
                if not self.cbas_util_v2.verify_index_used(statement.format(dataset.full_name), True, dataset.indexes.keys()[0]):
                    self.fail("Index was not used while querying the dataset")
            else:
                if not self.cbas_util_v2.validate_cbas_dataset_items_count(
                    dataset.full_name, 0):
                    self.fail("Data is still present in the dataset even after the KV collection on which it was created was deleted.")
        
        self.log.info("Test finished")
    
    def test_analytics_select_rbac_role_for_collections(self):
        
        self.log.info("Test started")
        
        dataset_names = list()
        
        bucket_helper = BucketHelper(self.cluster.master)
        
        for bucket in self.bucket_util.buckets:
            # Create datasets on KV collections in every scope in the bucket.
            status, content = bucket_helper.list_collections(bucket.name)
            if not status:
                self.fail("Failed to fetch all the collections in bucket {0}".format(bucket.name))
            json_parsed = json.loads(content)

            for scope in json_parsed["scopes"]:
                count = 0
                for collection in scope["collections"]:
                    dataset_name = "{0}-{1}-{2}".format(bucket.name,scope["name"],collection["name"])
                    dataset_names.append((dataset_name,bucket.name,scope["name"],collection["name"]))
                    self.log.info("Creating dataset {0}".format(dataset_name))
                    if not self.cbas_util_v2.create_dataset(
                        CBASHelper.format_name(dataset_name),
                        CBASHelper.format_name(bucket.name,scope["name"],collection["name"])):
                        self.fail("Error while creating dataset {0}".format(dataset_name))
                    count += 1
                    if count == 2:
                        break
        
        rbac_username = "test_user"
        dataset_info = random.choice(dataset_names)
        user = [{'id': rbac_username, 'password': 'password', 'name': 'Some Name'}]
        _ = RbacBase().create_user_source(user, 'builtin', self.cluster.master)
        
        if self.input.param('collection_user', False):
            self.log.info("Granting analytics_select user access to following KV entity - {0}.{1}.{2}".format(
                dataset_info[1], dataset_info[2], dataset_info[3]))
            payload = "name=" + rbac_username + "&roles=analytics_select" + "[" + dataset_info[1] + ":" + dataset_info[2] + ":" + dataset_info[3] + "]"
            response = self.rest.add_set_builtin_user(rbac_username, payload)
        if self.input.param('scope_user', False):
            self.log.info("Granting analytics_select user access to following KV entity - {0}.{1}".format(
                dataset_info[1], dataset_info[2]))
            payload = "name=" + rbac_username + "&roles=analytics_select" + "[" + dataset_info[1] + ":" + dataset_info[2] + "]"
            response = self.rest.add_set_builtin_user(rbac_username, payload)
        if self.input.param('bucket_user', False):
            self.log.info("Granting analytics_select user access to following KV entity - {0}".format(
                dataset_info[1]))
            payload = "name=" + rbac_username + "&roles=analytics_select" + "[" + dataset_info[1] + "]"
            response = self.rest.add_set_builtin_user(rbac_username, payload)
        
        for dataset_name in dataset_names:
            cmd_get_num_items = "select count(*) from %s;" % CBASHelper.format_name(dataset_name[0])
            status, metrics, errors, results, _ = self.cbas_util_v2.execute_statement_on_cbas_util(
                cmd_get_num_items, username=rbac_username, password="password")
            
            validate_error = False
            if self.input.param('bucket_user', False):
                selected_kv_entity = dataset_info[1]
                current_kv_entity = dataset_name[1]
            elif self.input.param('scope_user', False):
                selected_kv_entity = "{0}-{1}".format(dataset_info[1],dataset_info[2])
                current_kv_entity = "{0}-{1}".format(dataset_name[1],dataset_name[2] )
            elif self.input.param('collection_user', False):
                selected_kv_entity = dataset_info[0]
                current_kv_entity = dataset_name[0]
            
            if selected_kv_entity == current_kv_entity:
                if status == "success":
                    self.log.info("Query passed with analytics select user")
                else:
                    self.fail("RBAC user is unable to query dataset {0}".format(dataset_name[0]))
            else:
                validate_error = True
            
            if validate_error and not self.cbas_util_v2.validate_error_in_response(
                status, errors, "Unauthorized user"):
                self.fail("RBAC user is able to query dataset {0}".format(dataset_name[0]))
        self.log.info("Test finished")
    
    def test_analytics_with_parallel_dataset_creation(self):
        self.log.info("test_analytics_with_parallel_dataset_creation started")
        # Create Datasets on all collections in parallel
        create_datasets_task = CreateDatasetsOnAllCollectionsTask(
            bucket_util=self.bucket_util,
            cbas_name_cardinality=self.input.param('cardinality', None),
            cbas_util=self.cbas_util_v2,
            kv_name_cardinality=self.input.param('bucket_cardinality', None),
            creation_methods=["cbas_collection", "cbas_dataset"])
        self.task_manager.add_new_task(create_datasets_task)
        self.start_data_load_task(percentage_per_collection=self.parallel_load_percent)
        self.start_query_task()
        self.wait_for_query_task()
        self.wait_for_data_load_task()
        # Wait for create dataset task to finish
        dataset_creation_result = self.task_manager.get_task_result(create_datasets_task)
        if not dataset_creation_result:
            self.fail("Datasets creation failed")
        # Validate ingestion
        self.wait_for_ingestion_all_datasets()
        self.log.info("test_analytics_with_parallel_dataset_creation completed")
