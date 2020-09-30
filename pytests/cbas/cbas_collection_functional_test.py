'''
Created on 30-August-2020

@author: umang.agrawal
'''

from TestInput import TestInputSingleton
from cbas.cbas_base import CBASBaseTest
import random, json, copy, time
from threading import Thread
from cbas_utils.cbas_utils import Dataset
from __builtin__ import None


class CBASDataverseAndScopes(CBASBaseTest):

    def setUp(self):
        
        self.input = TestInputSingleton.input
        if "default_bucket" not in self.input.test_params:
            self.input.test_params.update({"default_bucket": False})
        super(CBASDataverseAndScopes, self).setUp()
        
        self.entity_name = Dataset.create_name_with_cardinality(
            name_cardinality=self.input.param('cardinality', 1), 
            no_of_char=self.input.param('name_length', 255), 
            fixed_length=self.input.param('fixed_length', False))
        
        if self.input.param('error', None):
            self.error_msg = self.input.param('error', None).format(self.entity_name)
        else:
            self.error_msg = None
        
        self.log.info("================================================================")
        self.log.info("SETUP has finished")
        self.log.info("================================================================")

    def tearDown(self):
        
        self.log.info("================================================================")
        self.log.info("TEARDOWN has started")
        self.log.info("================================================================")
        
        super(CBASDataverseAndScopes, self).tearDown()
        
        self.log.info("================================================================")
        self.log.info("Teardown has finished")
        self.log.info("================================================================")
    
    def test_create_dataverse(self):
        if not self.cbas_util.create_dataverse_on_cbas(dataverse_name=Dataset.format_name(self.entity_name),
                                                       validate_error_msg=self.input.param('validate_error', 
                                                                                           False),
                                                       expected_error=self.error_msg,
                                                       expected_error_code=self.input.param('error_code', 
                                                                                            None)):
            self.fail("Creation of Dataverse {0} failed".format(self.entity_name))
        
        self.log.info("Performing validation in Metadata.Dataverse")
        if not self.cbas_util.validate_dataverse_in_metadata(self.entity_name):
            self.fail("Validation in Metadata.Dataverse failed for {0}".format(self.entity_name))
    
    def test_create_analytics_scope(self):
        if not self.cbas_util.create_analytics_scope(scope_name=Dataset.format_name(self.entity_name),
                                                     validate_error_msg=self.input.param('validate_error', 
                                                                                         False),
                                                     expected_error=self.error_msg,
                                                     expected_error_code=self.input.param('error_code', 
                                                                                          None)):
            self.fail("Creation of Analytics Scope {0} failed".format(self.entity_name))
        
        self.log.info("Performing validation in Metadata.Dataverse")
        if not self.cbas_util.validate_dataverse_in_metadata(self.entity_name):
            self.fail("Validation in Metadata.Dataverse failed for {0}".format(self.entity_name))
    
    def test_drop_dataverse(self):
        if 0 < self.input.param('cardinality', 1) < 3:
            self.cbas_util.create_dataverse_on_cbas(dataverse_name=Dataset.format_name(self.entity_name))
        if not self.cbas_util.drop_dataverse_on_cbas(dataverse_name=Dataset.format_name(self.entity_name),
                                                     validate_error_msg=self.input.param('validate_error', 
                                                                                         False),
                                                     expected_error=self.error_msg,
                                                     expected_error_code=self.input.param('error_code', 
                                                                                          None)):
            self.fail("Dropping of Dataverse {0} failed".format(self.entity_name))
        
        self.log.info("Performing validation in Metadata.Dataverse")
        if self.cbas_util.validate_dataverse_in_metadata(self.entity_name):
            self.fail("Validation in Metadata.Dataverse failed for {0}".format(self.entity_name))
    
    
    def test_drop_analytics_scope(self):
        if 0 < self.input.param('cardinality', 1) < 3:
            self.cbas_util.create_analytics_scope(scope_name=Dataset.format_name(self.entity_name))
        if not self.cbas_util.drop_analytics_scope(scope_name=Dataset.format_name(self.entity_name),
                                                   validate_error_msg=self.input.param('validate_error', 
                                                                                       False),
                                                   expected_error=self.error_msg,
                                                   expected_error_code=self.input.param('error_code', 
                                                                                        None)):
            self.fail("Dropping of scope {0} failed".format(self.entity_name))
        
        self.log.info("Performing validation in Metadata.Dataverse")
        if self.cbas_util.validate_dataverse_in_metadata(self.entity_name):
            self.fail("Validation in Metadata.Dataverse failed for {0}".format(self.entity_name))

class CBASDatasetsAndCollections(CBASBaseTest):

    def setUp(self):
        
        self.input = TestInputSingleton.input
        if "bucket_spec" not in self.input.test_params:
            self.input.test_params.update({"bucket_spec": "single_bucket.default"})
        super(CBASDatasetsAndCollections, self).setUp()
        self.log.info("================================================================")
        self.log.info("SETUP has finished")
        self.log.info("================================================================")

    def tearDown(self):
        
        self.log.info("================================================================")
        self.log.info("TEARDOWN has started")
        self.log.info("================================================================")
        
        super(CBASDatasetsAndCollections, self).tearDown()
        
        self.log.info("================================================================")
        self.log.info("Teardown has finished")
        self.log.info("================================================================")
        
    def test_create_dataset(self):
        self.log.info("Test started")
        dataset_obj = Dataset(
            bucket_util=self.bucket_util,
            cbas_util=self.cbas_util,
            consider_default_KV_scope=True, 
            consider_default_KV_collection=True,
            dataset_name_cardinality=int(self.input.param('cardinality', 1)),
            bucket_cardinality=int(self.input.param('bucket_cardinality', 3)),
            random_dataset_name=True
            )
        
        # Negative scenario 
        if self.input.param('error', None):
            error_msg = self.input.param('error', None)
        else:
            error_msg = None
        
        if self.input.param('invalid_kv_collection', False):
            dataset_obj.kv_collection_obj.name = "invalid"
            error_msg = error_msg.format(
                dataset_obj.get_fully_quantified_kv_entity_name(
                    dataset_obj.bucket_cardinality))
        elif self.input.param('invalid_kv_scope', False):
            dataset_obj.kv_scope_obj.name = "invalid"
            error_msg = error_msg.format(
                dataset_obj.get_fully_quantified_kv_entity_name(2))
        elif self.input.param('invalid_dataverse', False):
            dataset_obj.dataverse  = "invalid"
            error_msg = error_msg.format("invalid")
        elif self.input.param('name_length', None):
            dataset_obj.dataverse, dataset_obj.name = dataset_obj.split_dataverse_dataset_name(
                dataset_obj.create_name_with_cardinality(
                    1, int(self.input.param('name_length', None)), True))
        elif self.input.param('no_dataset_name', False):
            dataset_obj.name = ''
        # Negative scenario ends
        
        dataset_obj.setup_dataset(
            dataset_creation_method=self.input.param('dataset_creation_method', "cbas_dataset"),
            validate_metadata=True, validate_doc_count=True, create_dataverse=True, 
            validate_error=self.input.param('validate_error', False), 
            error_msg=error_msg, username=None, password=None, timeout=120, analytics_timeout=120)
        self.log.info("Test finished")
        
    def test_drop_dataset(self):
        self.log.info("Test started")
        
        dataset_obj = Dataset(
            bucket_util=self.bucket_util,
            cbas_util=self.cbas_util,
            consider_default_KV_scope=True, 
            consider_default_KV_collection=True,
            dataset_name_cardinality=int(self.input.param('cardinality', 1)),
            bucket_cardinality=int(self.input.param('bucket_cardinality', 3)),
            random_dataset_name=True
            )
        
        if not dataset_obj.setup_dataset(
            dataset_creation_method=self.input.param('dataset_creation_method', "cbas_dataset")):
            self.fail("Error while creating dataset.")
        
        # Negative scenario   
        if self.input.param('invalid_dataset', False):
            dataset_obj.name = "invalid"
        
        if self.input.param('error', None):
            error_msg = self.input.param('error', None).format("invalid")
        else:
            error_msg = None
        # Negative scenario ends
        
        if not dataset_obj.teardown_dataset(
            dataset_drop_method = self.input.param('dataset_drop_method', "cbas_dataset"),
            validate_error=self.input.param('validate_error', False), 
            error_msg=error_msg):
            self.fail("Error while dropping dataset")
        
        self.log.info("Test finished")
        
    def test_create_analytics_collection(self):
        """Only dataset_creation_method parameter will change for these testcase"""
        self.test_create_dataset()
    
    def test_drop_analytics_collection(self):
        """Only dataset_creation_method and dataset_drop_method parameter will change for these testcase"""
        self.test_drop_dataset()
    
    def test_create_multiple_datasets(self):
        self.log.info("Test started")
        results = list()
        for i in range(int(self.input.param('no_of_datasets', 1))):
            dataset_obj = Dataset(
                bucket_util=self.bucket_util,
                cbas_util=self.cbas_util,
                consider_default_KV_scope=True, 
                consider_default_KV_collection=True,
                dataset_name_cardinality=random.randint(1,3),
                bucket_cardinality=random.choice([1,3]),
                random_dataset_name=True)
            results.append(dataset_obj.setup_dataset(
                dataset_creation_method=self.input.param('dataset_creation_method', "cbas_dataset")))
        
        if all(results):
            self.fail("All datasets were not created.")
        self.log.info("Test finished")
        
    def test_enabling_analytics_collection_from_KV(self):
        self.log.info("Test started")
        
        dataset_cardinality = int(self.input.param('dataset_cardinality', 0))
        if not dataset_cardinality:
            dataset_cardinality = int(self.input.param('bucket_cardinality', 1))
            
        dataset_obj = Dataset(
            bucket_util=self.bucket_util,
            cbas_util=self.cbas_util,
            consider_default_KV_scope=self.input.param('consider_default_KV_scope', True), 
            consider_default_KV_collection=self.input.param('consider_default_KV_collection', True),
            dataset_name_cardinality=dataset_cardinality,
            bucket_cardinality=int(self.input.param('bucket_cardinality', 1)),
            random_dataset_name=False
            )
        
        if self.input.param('create_dataverse', False) and \
            not self.cbas_util.create_dataverse_on_cbas(
                dataverse_name=dataset_obj.dataverse):
            self.fail("Failed to create dataverse {0}".format(dataset_obj.dataverse))
        
        # Negative scenarios
        if self.input.param('error', None):
            error_msg = self.input.param('error', None)
        else:
            error_msg = None
        
        if self.input.param('invalid_kv_collection', False):
            dataset_obj.kv_collection_obj.name = "invalid"
            error_msg = error_msg.format(
                dataset_obj.get_fully_quantified_kv_entity_name(
                    dataset_obj.bucket_cardinality).replace('`',''))
        elif self.input.param('invalid_kv_scope', False):
            dataset_obj.kv_scope_obj.name = "invalid"
        elif self.input.param('invalid_kv_bucket', False):
            dataset_obj.kv_bucket_obj = "invalid"
            error_msg = error_msg.format("invalid")
        
        # Creating dataverse before enabling analytics from KV
        precreate_dataset = self.input.param('precreate_dataset', None)
        if precreate_dataset:
            original_dataverse = dataset_obj.dataverse
            original_dataset = dataset_obj.name
            
            if precreate_dataset == "Default":
                dataset_obj.dataverse = "Default"
                dataset_obj.name = dataset_obj.kv_bucket_obj.name
            
            if not dataset_obj.setup_dataset(create_dataverse=True):
                self.fail("Error while creating dataset {0}".format(dataset_obj.full_dataset_name))
            
            dataset_obj.dataverse = original_dataverse
            dataset_obj.name = original_dataset
            
            error_msg = error_msg.format(dataset_obj.name, dataset_obj.dataverse)
        
        # Creating synonym before enabling analytics from KV
        if self.input.param('synonym_name', None) == "Bucket":
            synonym_name = dataset_obj.get_fully_quantified_kv_entity_name(1)
            error_msg = error_msg.format(synonym_name.replace('`',''))
        elif self.input.param('synonym_name', None) == "Collection":
            synonym_name = dataset_obj.get_fully_quantified_kv_entity_name(3)
            error_msg = error_msg.format(dataset_obj.split_dataverse_dataset_name(
                dataset_obj.full_dataset_name,True))
        else:
            synonym_name = None
        
        if synonym_name and not self.cbas_util.create_analytics_synonym(
            synonym_name=synonym_name,
            object_name=dataset_obj.full_dataset_name):
            self.fail("Error while creating synonym {0} on dataset {1}".format(
                synonym_name, dataset_obj.full_dataset_name))
        # Negative scenario ends
        
        if not dataset_obj.setup_dataset(
            dataset_creation_method=self.input.param('dataset_creation_method', "enable_cbas_from_kv"),
            validate_metadata=True, validate_doc_count=True, create_dataverse=False, 
            validate_error=self.input.param('validate_error', False),
            compress_dataset=self.input.param('compress_dataset', False),
            error_msg=error_msg, username=None, password=None, timeout=120, analytics_timeout=120):
            self.fail("Failed to enable analytics on {0}".format(dataset_obj.full_dataset_name))
        
        self.log.info("Validating created dataverse entry in Metadata")
        if not self.input.param('create_dataverse', False) and \
            not self.cbas_util.validate_dataverse_in_metadata(
                dataset_obj.dataverse):
            self.fail("Dataverse {0} was not created".format(dataset_obj.dataverse))
        
        self.log.info("Validating created Synonym entry in Metadata")
        synonym_validation = self.cbas_util.validate_synonym_in_metadata(
            synonym=dataset_obj.kv_bucket_obj.name,
            synonym_dataverse="Default",
            dataset_dataverse=dataset_obj.dataverse, dataset=dataset_obj.name)
         
        if not (self.input.param('verify_synonym', False) and synonym_validation):
            self.fail("Synonym {0} is not created under Dataverse {1}".format(
                dataset_obj.kv_bucket_obj.name, dataset_obj.dataverse))
        
        self.log.info("Test finished")
    
    def test_disabling_analytics_collection_from_KV(self):
        self.log.info("Test started")
        dataset_cardinality = int(self.input.param('dataset_cardinality', 0))
        if not dataset_cardinality:
            dataset_cardinality = int(self.input.param('bucket_cardinality', 1))
            
        dataset_obj = Dataset(
            bucket_util=self.bucket_util,
            cbas_util=self.cbas_util,
            consider_default_KV_scope=self.input.param('consider_default_KV_scope', True), 
            consider_default_KV_collection=self.input.param('consider_default_KV_collection', True),
            dataset_name_cardinality=dataset_cardinality,
            bucket_cardinality=int(self.input.param('bucket_cardinality', 1)),
            random_dataset_name=False
            )
        
        self.log.info("Enabling analytics from KV")
        if not dataset_obj.setup_dataset(
            dataset_creation_method=self.input.param('dataset_creation_method', "enable_cbas_from_kv"),
            validate_metadata=True, validate_doc_count=True, create_dataverse=False):
            self.fail("Failed to enable analytics on {0}".format(dataset_obj.full_dataset_name))
        
        # Negative scenarios
        if self.input.param('error', None):
            error_msg = self.input.param('error', None)
        else:
            error_msg = None
            
        if self.input.param('invalid_kv_collection', False):
            dataset_obj.kv_collection_obj.name = "invalid"
        # Negative scenario ends
        
        if self.input.param('create_dataset', False):
            new_dataset_name = dataset_obj.create_name_with_cardinality(1)
            new_dataset_full_name = dataset_obj.format_name(dataset_obj.dataverse, new_dataset_name)
            if not self.cbas_util.create_dataset_on_bucket(
                dataset_obj.get_fully_quantified_kv_entity_name(self.bucket_cardinality), 
                new_dataset_full_name):
                self.fail("Error creating dataset {0}".format(new_dataset_full_name))
        
        if self.input.param('create_synonym', False):
            new_synonym_name = dataset_obj.create_name_with_cardinality(1)
            if not self.cbas_util.create_analytics_synonym(
                synonym_name=new_synonym_name, 
                object_name=dataset_obj.full_dataset_name,
                synonym_dataverse="Default"):
                self.fail("Error creating synonym {0}".format(new_synonym_name))
        
        self.log.info("Disabling analytics from KV")
        if not dataset_obj.teardown_dataset(
            dataset_drop_method = self.input.param('dataset_creation_method', "enable_cbas_from_kv"),
            validate_error=self.input.param('validate_error', False), 
            error_msg=error_msg, validate_metadata=True):
            self.fail("Error while disabling analytics on KV collection")
        
        self.log.info("Validating whether the dataverse is deleted or not")
        if self.input.param('dataverse_deleted', False) and self.cbas_util.validate_dataverse_in_metadata(
            dataset_obj.dataverse):
            self.fail("Dataverse {0} is still present even after disabling analytics from KV".format(
                dataset_obj.dataverse))
        elif not self.input.param('dataverse_deleted', False) and not self.cbas_util.validate_dataverse_in_metadata(
            dataset_obj.dataverse):
            self.fail("Dataverse {0} got deleted after disabling analytics from KV".format(
                dataset_obj.dataverse))
        
        self.log.info("Validating whether the synonym is deleted or not")
        if self.input.param('synonym_deleted', False) and self.cbas_util.validate_synonym_in_metadata(
            synonym=dataset_obj.kv_bucket_obj.name,
            synonym_dataverse="Default",
            dataset_dataverse=dataset_obj.dataverse, dataset=dataset_obj.name):
            self.fail("Synonym {0} is still present even after disabling analytics from KV".format(
                dataset_obj.kv_bucket_obj.name))
        
        if self.input.param('create_dataset', False):
            if not self.cbas_util.validate_dataset_in_metadata(
                new_dataset_name, dataset_obj.dataverse, BucketName=dataset_obj.kv_bucket_obj.name):
                self.fail("Explicitly created dataset got deleted after disabling analytics from KV")
        
        if self.input.param('create_synonym', False):
            if not self.cbas_util.validate_synonym_in_metadata(
                synonym=new_synonym_name, synonym_dataverse="Default", 
                dataset_dataverse=dataset_obj.dataverse, dataset=dataset_obj.name):
                self.fail("Explicitly created synonym got deleted after disabling analytics from KV")
        
        self.log.info("Test finished")
    
    def test_create_analytics_synonym(self):
        self.log.info("Test started")
        self.log.info("Test finished")
    
    def test_drop_analytics_synonym(self):
        self.log.info("Test started")
        self.log.info("Test finished")
    
    def test_dataset_after_deleting_and_recreating_KV_collection(self):
        self.log.info("Test started")
        """self.input.test_params.update({"validate_dataset_creation": True})
        self.setup_dataset()
        self.bucket_util.drop_collection(self.cluster.master, 
                                         self.selected_bucket, 
                                         scope_name=self.kv_entity_name[1],
                                         collection_name=self.kv_entity_name[2])
        if self.input.param('recreate_collection', False):
            self.bucket_util.create_collection(self.cluster.master,
                                               self.selected_bucket,
                                               scope_name=self.kv_entity_name[1],
                                               collection_spec={"name":self.kv_entity_name[2]})"""
            
        self.log.info("Test finished")