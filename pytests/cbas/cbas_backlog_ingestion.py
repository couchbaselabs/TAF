# -*- coding: utf-8 -*-

"""
Created on 28-Mar-2018

@author: tanzeem
"""
import uuid
import json
import datetime
import time

import testconstants
from couchbase_helper.documentgenerator import DocumentGenerator
from couchbase_helper.tuq_generators import JsonGenerator
from membase.helper.cluster_helper import ClusterOperationHelper

from BucketLib.BucketOperations import BucketHelper
from cbas.cbas_base import CBASBaseTest
from sdk_client import SDKClient


class CBASBacklogIngestion(CBASBaseTest):

    def setUp(self):
        super(CBASBacklogIngestion, self).setUp()

    @staticmethod
    def generate_documents(start_at, end_at, role=None):
        age = range(70)
        first = ['james', 'sharon', 'dave', 'bill', 'mike', 'steve']
        if role is None:
            profession = ['teacher']
        else:
            profession = role
        template = '{{ "number": {0}, "first_name": "{1}" , "profession":"{2}", "mutated":0}}'
        documents = DocumentGenerator('test_docs', template, age, first, profession, start=start_at, end=end_at)
        return documents

    '''
    -i b/resources/4-nodes-template.ini -t cbas.cbas_backlog_ingestion.CBASBacklogIngestion.test_document_expiry_with_overlapping_filters_between_datasets,default_bucket=True,items=10000,cb_bucket_name=default,cbas_bucket_name=default_cbas,cbas_dataset_name=default_ds,where_field=profession,where_value=teacher,batch_size=10000
    -i b/resources/4-nodes-template.ini -t cbas.cbas_backlog_ingestion.CBASBacklogIngestion.test_document_expiry_with_overlapping_filters_between_datasets,default_bucket=True,items=10000,cb_bucket_name=default,cbas_bucket_name=default_cbas,cbas_dataset_name=default_ds,where_field=profession,where_value=teacher,batch_size=10000,secondary_index=True,index_fields=profession:string
    '''

    def test_document_expiry_with_overlapping_filters_between_datasets(self):
        """
        1. Create default bucket
        2. Load data with profession doctor and lawyer
        3. Load data with profession teacher
        4. Create dataset with no filters
        5. Create filter dataset that holds data with profession teacher
        6. Verify the dataset count
        7. Delete half the documents with profession teacher
        8. Verify the updated dataset count
        9. Expire data with profession teacher
        10. Verify the updated dataset count post expiry
        """
        self.log.info("Set expiry pager on default bucket")
        ClusterOperationHelper.flushctl_set(self.master, "exp_pager_stime", 1, bucket="default")

        self.log.info("Load data in the default bucket")
        num_items = self.input.param("items", 10000)
        batch_size = self.input.param("batch_size", 10000)
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create", 0, num_items, exp=0, batch_size=batch_size)

        self.log.info("Load data in the default bucket, with documents containing profession teacher")
        load_gen = CBASBacklogIngestion.generate_documents(num_items, num_items * 2, role=['teacher'])
        self._async_load_all_buckets(server=self.master, kv_gen=load_gen, op_type="create", exp=0, batch_size=batch_size)

        self.log.info("Create primary index")
        query = "CREATE PRIMARY INDEX ON {0} using gsi".format(self.buckets[0].name)
        self.rest.query_tool(query)

        self.log.info("Create a connection")
        cb_bucket_name = self.input.param("cb_bucket_name")
        self.cbas_util.createConn(cb_bucket_name)

        self.log.info("Create a CBAS bucket")
        cbas_bucket_name = self.input.param("cbas_bucket_name")
        self.cbas_util.create_bucket_on_cbas(cbas_bucket_name=cbas_bucket_name,
                                             cb_bucket_name=cb_bucket_name)

        self.log.info("Create a default data-set")
        cbas_dataset_name = self.input.param("cbas_dataset_name")
        self.cbas_util.create_dataset_on_bucket(cbas_bucket_name=cb_bucket_name,
                                                cbas_dataset_name=cbas_dataset_name)

        self.log.info("Read input params for field name and value")
        field = self.input.param("where_field", "")
        value = self.input.param("where_value", "")
        cbas_dataset_with_clause = cbas_dataset_name + "_" + value

        self.log.info("Create data-set with profession teacher")
        self.cbas_util.create_dataset_on_bucket(cbas_bucket_name=cb_bucket_name,
                                                cbas_dataset_name=cbas_dataset_with_clause,
                                                where_field=field, where_value=value)

        secondary_index = self.input.param("secondary_index", False)
        datasets = [cbas_dataset_name, cbas_dataset_with_clause]
        index_fields = self.input.param("index_fields", None)
        if secondary_index:
            self.log.info("Create secondary index")
            index_fields = ""
            for index_field in self.index_fields:
                index_fields += index_field + ","
                index_fields = index_fields[:-1]
            for dataset in datasets:
                create_idx_statement = "create index {0} on {1}({2});".format(
                    dataset + "_idx", dataset, index_fields)
                status, metrics, errors, results, _ = self.cbas_util.execute_statement_on_cbas_util(
                    create_idx_statement)

                self.assertTrue(status == "success", "Create Index query failed")
                self.assertTrue(self.cbas_util.verify_index_created(dataset + "_idx", self.index_fields,
                                                                    dataset)[0])

        self.log.info("Connect to CBAS bucket")
        self.cbas_util.connect_to_bucket(cbas_bucket_name=cbas_bucket_name,
                                         cb_bucket_password=self.cb_bucket_password)

        self.log.info("Wait for ingestion to complete on both data-sets")
        self.cbas_util.wait_for_ingestion_complete([cbas_dataset_name], num_items * 2)
        self.cbas_util.wait_for_ingestion_complete([cbas_dataset_with_clause], num_items)

        self.log.info("Validate count on data-set")
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(cbas_dataset_name, num_items * 2))
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(cbas_dataset_with_clause, num_items))

        self.log.info("Delete half of the teacher records")
        self.perform_doc_ops_in_all_cb_buckets(num_items // 2, "delete", num_items + (num_items // 2), num_items * 2)

        self.log.info("Wait for ingestion to complete")
        self.cbas_util.wait_for_ingestion_complete([cbas_dataset_name], num_items + (num_items // 2))
        self.cbas_util.wait_for_ingestion_complete([cbas_dataset_with_clause], num_items // 2)

        self.log.info("Validate count on data-set")
        self.assertTrue(
            self.cbas_util.validate_cbas_dataset_items_count(cbas_dataset_name, num_items + (num_items // 2)))
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(cbas_dataset_with_clause, num_items // 2))

        self.log.info("Update the documents with profession teacher to expire in next 1 seconds")
        self.perform_doc_ops_in_all_cb_buckets(num_items // 2, "update", num_items, num_items + (num_items // 2), exp=1)
        
        self.log.info("Wait for documents to expire")
        self.sleep(15, message="Waiting for documents to expire")
        
        self.log.info("Wait for ingestion to complete")
        self.cbas_util.wait_for_ingestion_complete([cbas_dataset_name], num_items)
        self.cbas_util.wait_for_ingestion_complete([cbas_dataset_with_clause], 0)

        self.log.info("Validate count on data-set")
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(cbas_dataset_name, num_items))
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(cbas_dataset_with_clause, 0))

    '''
    -i b/resources/4-nodes-template.ini -t cbas.cbas_backlog_ingestion.CBASBacklogIngestion.test_multiple_cbas_bucket_with_overlapping_filters_between_datasets,default_bucket=True,
    cb_bucket_name=default,cbas_bucket_name=default_cbas_,num_of_cbas_buckets=4,items=10000,cbas_dataset_name=default_ds_,where_field=profession,join_operator=or,batch_size=10000
    '''

    def test_multiple_cbas_bucket_with_overlapping_filters_between_datasets(self):
        """
        1. Create default bucket
        2. Load data in default bucket with professions picked from the predefined list
        3. Create CBAS bucket and a dataset in each CBAS bucket such that dataset between the cbas buckets have overlapping filter
        4. Verify dataset count
        """
        self.log.info("Load data in the default bucket")
        num_of_cbas_buckets = self.input.param("num_of_cbas_buckets", 4)
        batch_size = self.input.param("batch_size", 10000)
        cbas_bucket_name = self.input.param("cbas_bucket_name", "default_cbas_")
        professions = ['teacher', 'doctor', 'engineer', 'dentist', 'racer', 'dancer', 'singer', 'musician', 'pilot',
                       'finance']
        load_gen = CBASBacklogIngestion.generate_documents(0, self.num_items, role=professions[:num_of_cbas_buckets])
        self._async_load_all_buckets(server=self.master, kv_gen=load_gen, op_type="create", exp=0, batch_size=batch_size)

        self.log.info("Create primary index")
        query = "CREATE PRIMARY INDEX ON {0} using gsi".format(self.buckets[0].name)
        self.rest.query_tool(query)

        self.log.info("Create connection")
        self.cbas_util.createConn(self.cb_bucket_name)

        self.log.info("Create CBAS buckets")
        num_of_cbas_buckets = self.input.param("num_of_cbas_buckets", 2)
#         for index in range(num_of_cbas_buckets):
#             self.assertTrue(self.cbas_util.create_bucket_on_cbas(cbas_bucket_name=cbas_bucket_name + str(index),
#                                                                  cb_bucket_name=self.cb_bucket_name),
#                             "Failed to create cbas bucket " + self.cbas_bucket_name + str(index))

        self.log.info("Create data-sets")
        field = self.input.param("where_field", "")
        join_operator = self.input.param("join_operator", "or")
        for index in range(num_of_cbas_buckets):
            tmp = "\"" + professions[index] + "\" "
            join_values = professions[index + 1:index + 2]
            for join_value in join_values:
                tmp += join_operator + " `" + field + "`=\"" + join_value + "\""
            tmp = tmp[1:-1]
            self.assertTrue(self.cbas_util.create_dataset_on_bucket(self.cb_bucket_name,
                                                                    cbas_dataset_name=(self.cbas_dataset_name + str(
                                                                        index)),
                                                                    where_field=field, where_value=tmp))

        self.log.info("Connect to CBAS bucket")
        for index in range(num_of_cbas_buckets):
            self.cbas_util.connect_to_bucket(cbas_bucket_name=cbas_bucket_name + str(index),
                                             cb_bucket_password=self.cb_bucket_password)

        self.log.info("Wait for ingestion to completed and assert count")
        for index in range(num_of_cbas_buckets):
            n1ql_query = 'select count(*) from `{0}` where {1} = "{2}"'.format(self.cb_bucket_name, field,
                                                                               professions[index])
            tmp = " "
            join_values = professions[index + 1:index + 2]
            for join_value in join_values:
                tmp += join_operator + " `" + field + "`=\"" + join_value + "\""
            n1ql_query += tmp
            count_n1ql = self.rest.query_tool(n1ql_query)['results'][0]['$1']
            self.cbas_util.wait_for_ingestion_complete([self.cbas_dataset_name + str(index)], count_n1ql)
            _, _, _, results, _ = self.cbas_util.execute_statement_on_cbas_util(
                'select count(*) from `%s`' % (self.cbas_dataset_name + str(index)))
            count_ds = results[0]["$1"]
            self.assertEqual(count_ds, count_n1ql, msg="result count mismatch between N1QL and Analytics")

    def tearDown(self):
        super(CBASBacklogIngestion, self).tearDown()


class BucketOperations(CBASBaseTest):
    CBAS_BUCKET_CONNECT_ERROR_MSG = 'Connect link failed {\"Default.Local.default\" : \"Maximum number of active writable datasets (8) exceeded\"}'

    def setUp(self):
        super(BucketOperations, self).setUp()

    def fetch_test_case_arguments(self):
        self.cb_bucket_name = self.input.param("cb_bucket_name", "default")
        self.cbas_bucket_name = self.input.param("cbas_bucket_name", "default_cbas")
        self.dataset_prefix = self.input.param("dataset_prefix", "_ds_")
        self.num_of_dataset = self.input.param("num_of_dataset", 9)
        self.num_items = self.input.param("items", 10000)
        self.dataset_name = self.input.param("dataset_name", "ds")
        self.num_of_cb_buckets = self.input.param("num_of_cb_buckets", 8)
        self.num_of_cbas_buckets_per_cb_bucket = self.input.param("num_of_cbas_buckets_per_cb_bucket", 2)
        self.num_of_dataset_per_cbas = self.input.param("num_of_dataset_per_cbas", 8)
        self.default_bucket = self.input.param("default_bucket", False)
        self.cbas_bucket_prefix = self.input.param("cbas_bucket_prefix", "_cbas_bucket_")

    '''
    test_cbas_bucket_connect_with_more_than_eight_active_datasets,default_bucket=True,cb_bucket_name=default,cbas_bucket_name=default_cbas,dataset_prefix=_ds_,num_of_dataset=9,items=10   
    '''

    def test_cbas_bucket_connect_with_more_than_eight_active_datasets(self):
        """
        1. Create a cb bucket
        2. Create a cbas bucket
        3. Create 9 datasets
        4. Connect to cbas bucket must fail with error - Maximum number of active writable datasets (8) exceeded
        5. Delete 1 dataset, now the count must be 8
        6. Re-connect the cbas bucket and this time connection must succeed
        7. Verify count in dataset post connect
        """
        self.log.info("Fetch test case arguments")
        self.fetch_test_case_arguments()

        self.log.info("Load data in the default bucket")
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create", 0, self.num_items)

        self.log.info("Create reference to SDK client")
        client = SDKClient(scheme="couchbase", hosts=[self.master.ip], bucket=self.cb_bucket_name,
                           password=self.master.rest_password)

        self.log.info("Insert binary data into default bucket")
        keys = ["%s" % (uuid.uuid4()) for i in range(0, self.num_items)]
        client.insert_binary_document(keys)

        self.log.info("Insert Non-Json string data into default bucket")
        keys = ["%s" % (uuid.uuid4()) for i in range(0, self.num_items)]
        client.insert_string_document(keys)

        self.log.info("Create connection")
        self.cbas_util.createConn(self.cb_bucket_name)

        self.log.info("Create a CBAS bucket")
        self.assertTrue(self.cbas_util.create_bucket_on_cbas(cbas_bucket_name=self.cbas_bucket_name,
                                                             cb_bucket_name=self.cb_bucket_name),
                        msg="Failed to create CBAS bucket")

        self.log.info("Create datasets")
        for i in range(1, self.num_of_dataset + 1):
            self.assertTrue(self.cbas_util.create_dataset_on_bucket(cbas_bucket_name=self.cb_bucket_name,
                                                                    cbas_dataset_name=self.dataset_prefix + str(i)),
                            msg="Failed to create dataset {0}".format(self.dataset_prefix + str(i)))

        self.log.info("Verify connect to CBAS bucket must fail")
        self.assertTrue(self.cbas_util.connect_to_bucket(cbas_bucket_name=self.cbas_bucket_name,
                                                         cb_bucket_password=self.cb_bucket_password,
                                                         validate_error_msg=True,
                                                         expected_error=BucketOperations.CBAS_BUCKET_CONNECT_ERROR_MSG),
                        msg="Incorrect error msg while connecting to cbas bucket")

        self.log.info("Drop the last dataset created")
        self.assertTrue(self.cbas_util.drop_dataset(cbas_dataset_name=self.dataset_prefix + str(self.num_of_dataset)),
                        msg="Failed to drop dataset {0}".format(self.dataset_prefix + str(self.num_of_dataset)))

        self.log.info("Connect to CBAS bucket")
        self.assertTrue(self.cbas_util.connect_to_bucket(cbas_bucket_name=self.cbas_bucket_name),
                        msg="Failed to connect to cbas bucket")

        self.log.info("Wait for ingestion to complete and validate count")
        for i in range(1, self.num_of_dataset):
            self.cbas_util.wait_for_ingestion_complete([self.dataset_prefix + str(i)], self.num_items)
            self.assertTrue(
                self.cbas_util.validate_cbas_dataset_items_count(self.dataset_prefix + str(i), self.num_items))

    '''
    test_delete_cb_bucket_with_cbas_connected,default_bucket=True,cb_bucket_name=default,cbas_bucket_name=default_cbas,dataset_name=ds,items=10
    '''

    def test_delete_cb_bucket_with_cbas_connected(self):
        """
        1. Create a cb bucket
        2. Create a cbas bucket
        3. Create a dataset
        4. Connect to cbas bucket
        5. Verify count on dataset
        6. Delete the cb bucket
        7. Verify count on dataset must remain unchange
        8. Recreate the cb bucket
        9. Verify count on dataset must be 0
        9. Insert documents double the initial size
        9. Verify count on dataset post cb create
        """
        self.log.info("Fetch test case arguments")
        self.fetch_test_case_arguments()

        self.log.info("Load data in the default bucket")
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create", 0, self.num_items)

        self.log.info("Create connection")
        self.cbas_util.createConn(self.cb_bucket_name)

        self.log.info("Create a CBAS bucket")
        self.assertTrue(self.cbas_util.create_bucket_on_cbas(cbas_bucket_name=self.cbas_bucket_name,
                                                             cb_bucket_name=self.cb_bucket_name),
                        msg="Failed to create CBAS bucket")

        self.log.info("Create datasets")
        self.assertTrue(self.cbas_util.create_dataset_on_bucket(cbas_bucket_name=self.cb_bucket_name,
                                                                cbas_dataset_name=self.dataset_name),
                        msg="Failed to create dataset {0}".format(self.dataset_name))

        self.log.info("Connect to CBAS bucket")
        self.assertTrue(self.cbas_util.connect_to_bucket(cbas_bucket_name=self.cbas_bucket_name),
                        msg="Failed to connect to cbas bucket")

        self.log.info("Wait for ingestion to complete and verify count")
        self.cbas_util.wait_for_ingestion_complete([self.dataset_name], self.num_items)
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(self.dataset_name, self.num_items))

        self.log.info("Delete CB bucket")
        self.delete_bucket_or_assert(serverInfo=self.master)

        self.log.info("Verify count on dataset")
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(self.dataset_name, self.num_items))

        self.log.info("Recreate CB bucket")
        self.create_default_bucket()

        self.log.info("Wait for ingestion to complete and verify count")
        self.cbas_util.wait_for_ingestion_complete([self.dataset_name], self.num_items)
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(self.dataset_name, self.num_items))

        self.log.info("Load back data in the default bucket")
        self.log.info("Now that the UUID of this new bucet is different hence cbas will not ingest anything from this bucket anymore.")
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create", self.num_items, self.num_items * 2)

        self.log.info("Wait for ingestion to complete and verify count")
        self.cbas_util.wait_for_ingestion_complete([self.dataset_name], self.num_items)
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(self.dataset_name, self.num_items))

    '''
    test_create_multiple_cb_cbas_and_datasets,num_of_cb_buckets=8,num_of_dataset_per_cbas=8,default_bucket=False,cbas_bucket_prefix=_cbas_bucket_,dataset_prefix=_ds_,items=10
    '''
    def test_create_multiple_cb_cbas_and_datasets(self):

        self.log.info("Fetch test case arguments")
        self.fetch_test_case_arguments()
        
        self.log.info("Fetch and set memory quota")
        memory_for_kv = int(self.fetch_available_memory_for_kv_on_a_node())
        self.rest.set_service_memoryQuota(service='memoryQuota', memoryQuota=memory_for_kv)

        self.log.info("Update storageMaxActiveWritableDatasets count")
        active_data_set_count = self.num_of_cb_buckets * self.num_of_dataset_per_cbas
        update_config_map = {"storageMaxActiveWritableDatasets": active_data_set_count}
        status, _, _ = self.cbas_util.update_service_parameter_configuration_on_cbas(update_config_map)
        self.assertTrue(status, msg="Failed to update config")

        self.log.info("Restart the cbas node using the api")
        status, _, _ = self.cbas_util.restart_analytics_cluster_uri()
        self.assertTrue(status, msg="Failed to restart cbas")

        self.log.info("Wait for request to complete and cluster to be active: Using private ping() function")
        cluster_recover_start_time = time.time()
        while time.time() < cluster_recover_start_time + 180:
            try:
                status, metrics, _, cbas_result, _ = self.cbas_util.execute_statement_on_cbas_util("set `import-private-functions` `true`;ping()")
                if status == "success":
                    break
            except:
                self.sleep(3, message="Wait for service to up")

        self.log.info("Assert on storageMaxActiveWritableDatasets count")
        status, content, response = self.cbas_util.fetch_service_parameter_configuration_on_cbas()
        config_dict = json.loads((content.decode("utf-8")))
        active_dataset = config_dict['storageMaxActiveWritableDatasets']
        self.assertEqual(active_dataset, active_data_set_count, msg="Value in correct for active dataset count")

        self.log.info("Create {0} cb buckets".format(self.num_of_cb_buckets))
        self.create_multiple_buckets(server=self.master, replica=1, howmany=self.num_of_cb_buckets)
        self.sleep(30, message="Wait for buckets to be ready")

        self.log.info("Check if buckets are created")
        bucket_helper = BucketHelper(self.master)
        kv_buckets = bucket_helper.get_buckets()
        self.assertEqual(len(kv_buckets), self.num_of_cb_buckets, msg="CB bucket count mismatch")

        self.log.info("Load data in the default bucket")
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create", 0, self.num_items)

        self.log.info("Create connection to all buckets")
        for bucket in kv_buckets:
            self.cbas_util.createConn(bucket.name)
            break
            
        self.log.info("Create {0} datasets".format(self.num_of_cb_buckets * self.num_of_dataset_per_cbas))
        for kv_bucket in kv_buckets:
            for index in range(self.num_of_dataset_per_cbas):
                self.assertTrue(self.cbas_util.create_dataset_on_bucket(kv_bucket.name, kv_bucket.name.replace("-", "_") + self.dataset_prefix + str(index)),
                                msg="Failed to create dataset {0}".format(self.dataset_name))

        self.log.info("Connect Link")
        self.assertTrue(self.cbas_util.connect_link(), msg="Failed to connect to cbas bucket")

        self.log.info("Validate document count")
        for kv_bucket in kv_buckets:
            for index in range(self.num_of_dataset_per_cbas):
                self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(
                                kv_bucket.name.replace("-", "_") + self.dataset_prefix + str(index), self.num_items))
                        
    def tearDown(self):
        super(BucketOperations, self).tearDown()


class CBASDataOperations(CBASBaseTest):

    @staticmethod
    def generate_docs_bigdata(num_of_documents, start=0, document_size_in_mb=1):
        json_generator = JsonGenerator()
        return json_generator.generate_docs_bigdata(end=num_of_documents, start=start,
                                                    value_size=1024000 * document_size_in_mb)

    def setUp(self):
        super(CBASDataOperations, self).setUp()

    def fetch_test_case_arguments(self):
        self.cb_bucket_name = self.input.param("cb_bucket_name", "default")
        self.cbas_bucket_name = self.input.param("cbas_bucket_name", "default_cbas")
        self.dataset_name = self.input.param("dataset_name", "ds")
        self.default_bucket = self.input.param("default_bucket", False)
        self.analytics_memory = self.input.param("analytics_memory", 1200)
        self.document_size = self.input.param("document_size", 20)
        self.batch_size = self.input.param("batch_size", 5)
        self.secondary_index = self.input.param("secondary_index", False)
        self.num_of_documents = self.input.param("num_of_documents", 10)

    def cbas_dataset_setup(self):
        self.log.info("Create connection")
        self.cbas_util.createConn(self.cb_bucket_name)

        self.log.info("Create a CBAS bucket")
        self.assertTrue(self.cbas_util.create_bucket_on_cbas(cbas_bucket_name=self.cbas_bucket_name,
                                                             cb_bucket_name=self.cb_bucket_name),
                        msg="Failed to create CBAS bucket")

        self.log.info("Create datasets")
        self.assertTrue(self.cbas_util.create_dataset_on_bucket(cbas_bucket_name=self.cb_bucket_name,
                                                                cbas_dataset_name=self.dataset_name),
                        msg="Failed to create dataset {0}".format(self.dataset_name))

        if self.secondary_index:
            self.log.info("Create secondary index")
            index_fields = ""
            for index_field in self.index_fields:
                index_fields += index_field + ","
                index_fields = index_fields[:-1]
            create_idx_statement = "create index {0} on {1}({2});".format(self.dataset_name + "_idx", self.dataset_name,
                                                                          index_fields)
            status, metrics, errors, results, _ = self.cbas_util.execute_statement_on_cbas_util(create_idx_statement)
            self.assertTrue(status == "success", "Create secondary index query failed")
            self.assertTrue(self.cbas_util.verify_index_created(self.dataset_name + "_idx", self.index_fields,
                                                                self.dataset_name)[0])

        self.log.info("Connect to CBAS bucket")
        self.assertTrue(self.cbas_util.connect_to_bucket(cbas_bucket_name=self.cbas_bucket_name),
                        msg="Failed to connect to cbas bucket")

    '''
    cbas.cbas_backlog_ingestion.CBASDataOperations.test_cbas_ingestion_with_documents_containing_multilingual_data,default_bucket=True,cb_bucket_name=default,cbas_bucket_name=default_cbas,dataset_name=default_ds
    cbas.cbas_backlog_ingestion.CBASDataOperations.test_cbas_ingestion_with_documents_containing_multilingual_data,default_bucket=True,cb_bucket_name=default,cbas_bucket_name=default_cbas,dataset_name=default_ds,secondary_index=True,index_fields=content:string
    '''

    def test_cbas_ingestion_with_documents_containing_multilingual_data(self):
        """
        1. Create reference to SDK client
        2. Add multilingual json documents to default bucket
        3. Verify ingestion on dataset with and with out secondary index
        """
        multilingual_strings = [
            'De flesta sagorna här är från Hans Hörner svenska översättning',
            'Il était une fois une maman cochon qui avait trois petits cochons',
            '森林里住着一只小兔子，它叫“丑丑”。它的眼睛红红的，像一对红宝石',
            '外治オヒル回条フ聞定ッ加官言岸ムモヱツ求碁込ヌトホヒ舞高メ旅位',
            'ان عدة الشهور عند الله اثنا عشر شهرا في',
        ]

        self.log.info("Fetch test case arguments")
        self.fetch_test_case_arguments()

        self.log.info("Create reference to SDK client")
        client = SDKClient(scheme="couchbase", hosts=[self.master.ip], bucket=self.cb_bucket_name,
                           password=self.master.rest_password)

        self.log.info("Add multilingual documents to the default bucket")
        client.insert_custom_json_documents("custom-key-", multilingual_strings)

        self.log.info("Create connections, datasets and indexes")
        self.cbas_dataset_setup()

        self.log.info("Wait for ingestion to complete and verify count")
        self.cbas_util.wait_for_ingestion_complete([self.dataset_name], len(multilingual_strings))
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(self.dataset_name, len(multilingual_strings)))

    '''
    cbas.cbas_backlog_ingestion.CBASDataOperations.test_cbas_ingestion_with_large_document_size_and_changing_analytics_memory_quota,default_bucket=True,cb_bucket_name=default,cbas_bucket_name=default_cbas,dataset_name=default_ds,analytics_memory=1200,document_size=20,secondary_index=True,index_fields=name:string,batch_size=5
    cbas.cbas_backlog_ingestion.CBASDataOperations.test_cbas_ingestion_with_large_document_size_and_changing_analytics_memory_quota,default_bucket=True,cb_bucket_name=default,cbas_bucket_name=default_cbas,dataset_name=default_ds,analytics_memory=1200,document_size=20,batch_size=5
    '''
    def test_cbas_ingestion_with_large_document_size_and_changing_analytics_memory_quota(self):
        """
        1. Increase memory quota of Analytics to 1200 MB
        2. Add large size documents each of size 20 MB such that they use up analytics RAM memory
        3. Verify ingestion on dataset with and with out secondary index
        4. Decrease the memory quota to default - 1024
        5. Verify documents are not ingested from zero
        6. Verify the document count again
        """
        self.log.info("Fetch test case arguments")
        self.fetch_test_case_arguments()

        self.log.info("Increase analytics memory quota")
        self.rest.set_service_memoryQuota(service='cbasMemoryQuota', memoryQuota=self.analytics_memory)

        self.log.info("Insert MB documents into default buckets")
        start = 0
        end = self.batch_size
        total_documents = self.analytics_memory // self.document_size
        while end <= total_documents:
            load_gen = CBASDataOperations.generate_docs_bigdata(start=start, num_of_documents=end,
                                                                document_size_in_mb=self.document_size)
            tasks = self._async_load_all_buckets(server=self.master, kv_gen=load_gen, op_type="create", exp=0,
                                                 batch_size=self.batch_size)
            for task in tasks:
                self.log.info("-------------------------------------------------------- " + str(task.get_result()))
            start = end
            end += self.batch_size

        self.log.info("Create connections, datasets, indexes")
        self.cbas_dataset_setup()

        self.log.info("Wait for ingestion to complete and verify count")
        self.cbas_util.wait_for_ingestion_complete([self.dataset_name], total_documents)
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(self.dataset_name, total_documents))

        self.log.info("Decrease analytics memory quota to 1024 MB")
        self.rest.set_service_memoryQuota(service='cbasMemoryQuota', memoryQuota=testconstants.CBAS_QUOTA)

        self.log.info("Verify document count remains unchanged")
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(self.dataset_name, total_documents))

    '''
    cbas.cbas_backlog_ingestion.CBASDataOperations.test_ingestion_impact_for_documents_containing_xattr_meta_information,default_bucket=True,cb_bucket_name=default,cbas_bucket_name=default_cbas,dataset_name=default_ds,num_of_documents=5
    cbas.cbas_backlog_ingestion.CBASDataOperations.test_ingestion_impact_for_documents_containing_xattr_meta_information,default_bucket=True,cb_bucket_name=default,cbas_bucket_name=default_cbas,dataset_name=default_ds,secondary_index=True,index_fields=name:string,num_of_documents=5
    '''

    def test_ingestion_impact_for_documents_containing_xattr_meta_information(self):

        self.log.info("Fetch test case arguments")
        self.fetch_test_case_arguments()

        self.log.info("Create reference to SDK client")
        client = SDKClient(scheme="couchbase", hosts=[self.master.ip], bucket="default",
                           password=self.master.rest_password)

        self.log.info("Insert custom data into default bucket")
        documents = ['{"name":"value"}'] * self.num_of_documents
        document_id_prefix = "id-"
        client.insert_custom_json_documents(document_id_prefix, documents)

        self.log.info("Create connections, datasets, indexes")
        self.cbas_dataset_setup()

        self.log.info("Wait for ingestion to complete and verify count")
        self.cbas_util.wait_for_ingestion_complete([self.dataset_name], self.num_of_documents)
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(self.dataset_name, self.num_of_documents))

        self.log.info("Insert xattr attribute for all the documents and assert document count on dataset")
        for i in range(self.num_of_documents):
            client.insert_xattr_attribute(document_id=document_id_prefix + str(i), path="a", value="{'xattr-value': 1}",
                                          xattr=True, create_parents=True)
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(self.dataset_name, self.num_of_documents))

        self.log.info("Update xattr attribute and assert document count on dataset")
        for i in range(self.num_of_documents):
            client.update_xattr_attribute(document_id=document_id_prefix + str(i), path="a",
                                          value="{'xattr-value': 11}", xattr=True, create_parents=True)
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(self.dataset_name, self.num_of_documents))
        
        # TODO Uncomment the below lines once we figure out why removing xattr cause Couchbase OOM exception
        #self.log.info("Delete xattr attribute and assert document count on dataset")
        #for i in range(self.num_of_documents):
            #client.remove_xattr_attribute(document_id=document_id_prefix + str(i), path="a", xattr=True)
        #self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(self.dataset_name, self.num_of_documents))

    def tearDown(self):
        super(CBASDataOperations, self).tearDown()
