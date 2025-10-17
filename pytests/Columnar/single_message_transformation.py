"""
Created on 2025
@author: Anisha Sinha
"""
import json
import random
import string
import time

from deepdiff import DeepDiff
from confluent_kafka import Producer

from CbasLib.CBASOperations import CBASHelper
from CbasLib.cbas_entity_columnar import Standalone_Dataset
from Jython_tasks.java_loader_tasks import SiriusCouchbaseLoader
from Jython_tasks.sirius_task import CouchbaseUtil
from py_constants.cb_constants import DocLoading
from couchbase_helper.documentgenerator import doc_generator
from kafka_util.confluent_utils import ConfluentUtils
from kafka_util.kafka_connect_util import KafkaConnectUtil
from couchbase_utils.kafka_util.common_utils import KafkaClusterUtils
from TestInput import TestInputSingleton
runtype = TestInputSingleton.input.param("runtype", "default").lower()
if runtype == "columnar":
    from Columnar.columnar_base import ColumnarBaseTest
else:
    from Columnar.onprem.columnar_onprem_base import ColumnarOnPremBase as ColumnarBaseTest

class SingleMessageTransformationTest(ColumnarBaseTest):
    """
    Test class for single message transformation testing
    """
    def __init__(self, methodName: str = "runTest"):
        super().__init__(methodName)
        self.pod = None
        self.tenant = None
        self.no_of_docs = None

    def setUp(self):
        super(SingleMessageTransformationTest, self).setUp()

        if self._testMethodDoc:
            self.log.info("Starting Test: %s - %s"
                          % (self._testMethodName, self._testMethodDoc))
        else:
            self.log.info("Starting Test: %s" % self._testMethodName)
        if runtype == "columnar":
            self.columnar_cluster = self.tenant.columnar_instances[0]
            self.remote_cluster = None
            if len(self.tenant.clusters) > 0:
                self.remote_cluster = self.tenant.clusters[0]
        self.couchbase_doc_loader = CouchbaseUtil(
            task_manager=self.task_manager,
            hostname=self.remote_cluster.master.ip,
            username=self.remote_cluster.master.rest_username,
            password=self.remote_cluster.master.rest_password,
        )
        self.initial_doc_count = self.input.param("initial_doc_count", 100)
        self.kafka_test = self.input.param("kafka_test", False)
        self.doc_size = self.input.param("doc_size", 1024)
        if not self.columnar_spec_name:
            self.columnar_spec_name = "full_template"
        self.remote_collection_list = []
        self.remote_dataset_name = None

        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.setUp.__name__)
    def tearDown(self):
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)

        if not self.cbas_util.delete_cbas_infra_created_from_spec(
                self.columnar_cluster):
            self.fail("Error while deleting cbas entities")

        if runtype == "columnar":
            if hasattr(self, "remote_cluster") and self.remote_cluster:
                self.delete_all_buckets_from_capella_cluster(
                    self.tenant, self.remote_cluster)

        # Clean up Kafka resources if they were used
        if hasattr(self, "confluent_util") and hasattr(self, "confluent_cluster_obj"):
            self.log.info("Cleaning up Kafka resources.")
            try:
                # Delete any topics with the prefix
                if hasattr(self, "kafka_topic_prefix"):
                    self.confluent_util.kafka_cluster_util.delete_topic_by_topic_prefix(
                        self.kafka_topic_prefix)
                if hasattr(self, "smt_topic"):
                    self.confluent_util.kafka_cluster_util.delete_topic_by_topic_prefix(
                        self.smt_topic)
                
                # Clean up connectors if they exist
                if hasattr(self, "kafka_connect_hostname_cdc_confluent") and hasattr(self, "cdc_connector_name"):
                    self.confluent_util.cleanup_kafka_resources(
                        self.kafka_connect_hostname_cdc_confluent,
                        [self.cdc_connector_name], self.kafka_topic_prefix + "_cdc")
                
                if hasattr(self, "kafka_connect_hostname_non_cdc_confluent") and hasattr(self, "non_cdc_connector_name"):
                    self.confluent_util.cleanup_kafka_resources(
                        self.kafka_connect_hostname_non_cdc_confluent,
                        [self.non_cdc_connector_name], self.kafka_topic_prefix + "_non_cdc",
                        self.confluent_cluster_obj.cluster_access_key)
                
                # Delete API key
                if hasattr(self, "confluent_cluster_obj") and hasattr(self.confluent_cluster_obj, "cluster_access_key"):
                    try:
                        self.confluent_util.confluent_apis.delete_api_key(
                            self.confluent_cluster_obj.cluster_access_key)
                    except Exception as err:
                        self.log.error(f"Error deleting API key: {err}")
                        
            except Exception as e:
                self.log.error(f"Error during Kafka cleanup: {e}")

        super(SingleMessageTransformationTest, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished", stage="Teardown")

    def test_setup(self, load_docs=True):
        if runtype == "columnar":
            self.create_bucket_scopes_collections_in_capella_cluster(
                self.tenant, self.remote_cluster,
                self.input.param("num_buckets", 1))
        else:
            self.collectionSetUp(cluster=self.remote_cluster, load_data=False)
        if self.kafka_test:
            # Initialize variables for Kafka
            self.kafka_topic_prefix = f"smt_regression_{int(time.time())}"
            self.smt_topic = f"smt_test_{int(time.time())}"

            # Initializing Confluent util and Confluent cluster object.
            self.confluent_util = ConfluentUtils(
                cloud_access_key=self.input.param("confluent_cloud_access_key"),
                cloud_secret_key=self.input.param("confluent_cloud_secret_key"))
            self.confluent_cluster_obj = self.confluent_util.generate_confluent_kafka_object(
                kafka_cluster_id=self.input.param("confluent_cluster_id"),
                topic_prefix=self.kafka_topic_prefix)
            if not self.confluent_cluster_obj:
                self.fail("Unable to initialize Confluent Kafka cluster object")

            # Initializing KafkaConnect Util and kafka connect server hostnames
            self.kafka_connect_util = KafkaConnectUtil()
            kafka_connect_hostname = self.input.param('kafka_connect_hostname')
            self.kafka_connect_hostname_cdc_confluent = (
                f"{kafka_connect_hostname}:{KafkaConnectUtil.CONFLUENT_CDC_PORT}")
            self.kafka_connect_hostname_non_cdc_confluent = (
                f"{kafka_connect_hostname}:{KafkaConnectUtil.CONFLUENT_NON_CDC_PORT}")

            self.kafka_topics = {
                "confluent": {
                    "MONGODB": [],
                    "POSTGRESQL": [],
                    "MYSQLDB": []
                }
            }
            self.kafka_topics["confluent"]["POSTGRESQL"].append(
                {
                    "topic_name": "postgres2.public.employee_data",
                    "key_serialization_type": "json",
                    "value_serialization_type": "json",
                    "cdc_enabled": False,
                    "source_connector": "DEBEZIUM",
                    "num_items": 1000000
                }
            )

            self.serialization_type = self.input.param("serialization_type",
                                                       "JSON")
            confluent_kafka_cluster_details = [
                self.confluent_util.generate_confluent_kafka_cluster_detail(
                    brokers_url=self.confluent_cluster_obj.bootstrap_server,
                    auth_type="PLAIN", encryption_type="TLS",
                    api_key=self.confluent_cluster_obj.cluster_access_key,
                    api_secret=self.confluent_cluster_obj.cluster_secret_key)]
            self.columnar_spec = self.populate_columnar_infra_spec(
                columnar_spec=self.cbas_util.get_columnar_spec(
                    self.columnar_spec_name),
                remote_cluster=self.remote_cluster,
                confluent_kafka_cluster_details=confluent_kafka_cluster_details,
                kafka_topics=self.kafka_topics,
                external_dbs=["POSTGRESQL"])
            self.columnar_spec["kafka_dataset"]["primary_key"] = [
                {"id": "INT"}]
        else:
            self.columnar_spec = self.populate_columnar_infra_spec(
                columnar_spec=self.cbas_util.get_columnar_spec(
                    self.columnar_spec_name),
                remote_cluster=self.remote_cluster)

        self.log.info("Create test infra from spec.")
        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            cluster=self.columnar_cluster, cbas_spec=self.columnar_spec,
            bucket_util=self.bucket_util, wait_for_ingestion=False,
            remote_clusters=[self.remote_cluster])
        if not result:
            self.fail(msg)
        if load_docs:
            self.log.info("Load data to remote collection.")
            for bucket in self.remote_cluster.buckets:
                SiriusCouchbaseLoader.create_clients_in_pool(
                    self.remote_cluster.master, self.remote_cluster.master.rest_username,
                    self.remote_cluster.master.rest_password,
                    bucket.name, req_clients=1)

            self.log.info("Started Doc loading on remote cluster")
            self.load_remote_collections(self.remote_cluster, template="Product",
                                         create_start_index=0, create_end_index=self.initial_doc_count,
                                         wait_for_completion=True)
        remote_datasets = self.cbas_util.get_all_dataset_objs("remote")
        self.remote_dataset_name = remote_datasets[0].full_name

        self.cbas_util.refresh_remote_dataset_item_count(self.bucket_util)

        if load_docs:
            for dataset in remote_datasets:
                if not self.cbas_util.wait_for_ingestion_complete(
                        self.columnar_cluster, dataset.full_name,
                        self.initial_doc_count):
                    self.fail("Could not ingest data in remote cluster.")

        if self.kafka_test:
            status, _, errors, results, _, _ = self.cbas_util.execute_statement_on_cbas_util(
                self.columnar_cluster, f"select * from {self.remote_dataset_name}")

            if status != "success":
                self.fail("Could not get results from remote dataset.")

            # Upload results to Kafka topic
            connection_config = self.confluent_cluster_obj.generate_connection_config(
                self.confluent_cluster_obj.bootstrap_server,
                security_protocal="SASL_SSL", sasl_mechanism="PLAIN",
                sasl_username=self.confluent_cluster_obj.cluster_access_key,
                sasl_password=self.confluent_cluster_obj.cluster_secret_key)
            self.kafka_cluster_util = KafkaClusterUtils(connection_config)
            if not self.kafka_cluster_util.create_topic(self.smt_topic, partitions_count=4, replication_factor=-1):
                self.fail("Could not create topic on Kafka cluster.")
            producer = Producer(connection_config)
            remote_dataset_name = self.remote_dataset_name.split(".")[2]
            for result in results:
                producer.produce(self.smt_topic, value=json.dumps(result[remote_dataset_name]))
            dataset_name = self.cbas_util.generate_name()
            kafka_link = self.cbas_util.get_all_link_objs("kafka")[0]
            dataset_obj = Standalone_Dataset(
                name=dataset_name, data_source="POSTGRESQL",
                primary_key={"id": "STRING"},
                dataverse_name="default", database_name="default",
                link_name=kafka_link.name, storage_format="columnar",
                kafka_topic_name=self.smt_topic,
                num_of_items=1,
                cdc_enabled=False,
                key_serialization_type="json",
                value_serialization_type="json",
                cdc_source_connector="DEBEZIUM")
            if not self.cbas_util.create_standalone_collection_using_links(
                cluster=self.columnar_cluster, collection_name=dataset_obj.name, if_not_exists=False,
                primary_key=dataset_obj.primary_key,
                link_name=dataset_obj.link_name, storage_format=dataset_obj.storage_format,
                external_collection=dataset_obj.kafka_topic_name,
                cdc_enabled=dataset_obj.cdc_enabled,
                key_serialization_type=dataset_obj.key_serialization_type,
                value_serialization_type=dataset_obj.value_serialization_type,
                cdc_source=dataset_obj.data_source,
                cdc_source_connector=dataset_obj.cdc_source_connector
            ):
                self.fail("Could not create the collection from Kafka topic generated.")
            self.remote_dataset_name = dataset_obj.name

    def create_udf_and_remotedataset(self, udf_query, if_not_exists=False, udf_name=None, or_replace=False,
                                     wait_for_ingestion=True):
        if not udf_name:
            udf_name = self.cbas_util.generate_name()
        if not self.cbas_util.create_udf(
                cluster=self.columnar_cluster, name=udf_name, parameters=["item"],
                body=udf_query, transform_function=True, if_not_exists=if_not_exists, or_replace=or_replace):
            self.fail(f"Error while creating UDF with name {udf_name}")

        self.log.info("Create collection using the transform function.")
        remote_link = self.cbas_util.get_all_link_objs("couchbase")[0]
        remote_coll_obj = self.cbas_util.create_remote_dataset_obj(
            self.columnar_cluster,
            self.remote_cluster.buckets[0].name,
            "_default",
            "_default",
            remote_link,
            use_only_existing_db=True,
            use_only_existing_dv=True,
            database="Default",
            dataverse="Default",
            capella_as_source=True)[0]
        result = self.cbas_util.create_remote_dataset(
            self.columnar_cluster,
            remote_coll_obj.name,
            remote_coll_obj.full_kv_entity_name,
            remote_coll_obj.link_name,
            remote_coll_obj.dataverse_name,
            remote_coll_obj.database_name,
            transform_function=udf_name)
        if not result:
            self.fail("Failed to create remote collection {} from transform function {}".format(
                remote_coll_obj.name, udf_name))
        if wait_for_ingestion:
            if not self.cbas_util.wait_for_ingestion_complete(self.columnar_cluster,
                                                       remote_coll_obj.name,
                                                       self.initial_doc_count):
                self.fail("Did not ingest data into cluster.")
        return remote_coll_obj

    def validate_collection_created_from_udf(self, udf_query, remote_query, continue_test=False):
        # Validate result by running query on remote dataset and comparing with the transform collection result.
        diff = {}
        for i in range(3):
            status, _, errors, results, _, _ = self.cbas_util.execute_statement_on_cbas_util(
                self.columnar_cluster, udf_query)
            udf_results = results
            status, _, errors, results, _, _ = self.cbas_util.execute_statement_on_cbas_util(
                self.columnar_cluster, remote_query)
            diff = DeepDiff(results, udf_results, ignore_order=True)
            if diff:
                time.sleep(30)
            else:
                break
        if diff:
            self.log.info("There is data mismatch between expected and actual result.")
            self.log.info(f"Query on transformed dataset: f{udf_query}")
            self.log.info(f"Query on remote dataset without transform: f{remote_query}")
            self.log.info(f"Results on dataset with udf: f{udf_results}")
            self.log.info(f"Results on remote dataset without udf: f{results}")
            self.log.info(diff)
            if continue_test:
                return False, diff
            self.fail(
                f"Data mismatch between expected and actual result of transform function."
                f"{diff}")
        return True, None

    def drop_udf(self):
        udf_list = self.cbas_util.get_all_udfs_from_metadata(self.columnar_cluster)
        for udf in udf_list:
            return self.cbas_util.drop_udf(self.columnar_cluster, CBASHelper.format_name(udf[0]), None, None, udf[1])

    def test_smt_string_manipulation(self):
        """
        Test create transform function with string manipulation.
        """
        self.log.info("Running transform function with string manipulation test.")

        self.log.info("Start test setup.")
        self.test_setup()
        string_functions = ["concat", "replace", "ltrim", "rtrim", "substring", "regex_replace"]
        results = []

        for func in string_functions:
            self.log.info(f"Create UDF with {func} transform function.")

            # Prepare appropriate UDF body and remote query for each string function
            if func == "concat":
                udf_expr = "product_name || seller_name"
            elif func == "replace":
                udf_expr = "REPLACE(product_name, 'a', '@')"
            elif func == "ltrim":
                udf_expr = "LTRIM(product_name)"
            elif func == "rtrim":
                udf_expr = "RTRIM(product_name)"
            elif func == "substring":
                udf_expr = "SUBSTR(product_name, 0, 3)"  # first 3 chars
            elif func == "regex_replace":
                udf_expr = "REGEXP_REPLACE(product_name, '[aeiou]', '*')" # replace vowels with *
            else:
                continue  # Skip unsupported functions

            udf_create_query = f"""SELECT {udf_expr} AS result_field_{func} FROM [item] AS item LIMIT 1"""
            udf_coll_obj = self.create_udf_and_remotedataset(udf_create_query)

            udf_select_query = f"SELECT result_field_{func} FROM {udf_coll_obj.name}"
            remote_query = f"SELECT {udf_expr} AS result_field_{func} FROM {self.remote_dataset_name}"

            self.log.info(f"Validate collection created with the transform function: {func}")
            results.append(
                self.validate_collection_created_from_udf(udf_select_query, remote_query, continue_test=True))

        # Check results and fail if any test failed
        for i, result in enumerate(results):
            if not result[0]:
                self.fail(
                    f"For string function {string_functions[i]}: "
                    f"Data mismatch between expected and actual result of transform function. {result[1]}"
                )

        self.log.info("Test completed successfully.")

    def test_smt_filter_with_where_clause(self):
        """
        Test create transform function with where clause.
        """
        self.log.info("Running transform function with where clause test.")

        self.log.info("Start test setup.")
        self.test_setup()
        self.log.info("Create UDF with transform function.")
        udf_create_query = f"""SELECT product_name
                        FROM [item] as item
                        WHERE num_sold>10000
                        LIMIT 1 """
        udf_coll_obj = self.create_udf_and_remotedataset(udf_create_query, wait_for_ingestion=False)
        udf_select_query = f"select product_name from {udf_coll_obj.name}"
        remote_query = f"select product_name from {self.remote_dataset_name} where num_sold>10000"
        self.log.info("Validate collection created with the transform function.")
        self.validate_collection_created_from_udf(udf_select_query, remote_query)
        self.log.info("Test completed successfully.")

    def test_smt_select_multiple_fields(self):
        """
        Test create transform function with multiple fields.
        """
        self.log.info("Running transform function with select multiple fields.")

        self.log.info("Start test setup.")
        self.test_setup()
        self.log.info("Create UDF with transform function.")
        udf_create_query = f"""SELECT quantity, product_name
                                FROM [item] as item
                                LIMIT 1 """
        udf_coll_obj = self.create_udf_and_remotedataset(udf_create_query)
        udf_select_query = f"select quantity, product_name from {udf_coll_obj.name}"
        remote_query = f"select quantity, product_name from {self.remote_dataset_name}"
        self.log.info("Validate collection created with the transform function.")
        self.validate_collection_created_from_udf(udf_select_query, remote_query)
        self.log.info("Test completed successfully.")

    def test_smt_multiple_fields_excluded(self):
        """
        Test create transform function with multiple fields excluded.
        """
        self.log.info("Running transform function with multiple fields excluded.")

        self.log.info("Start test setup.")
        self.test_setup()
        self.log.info("Create UDF with transform function.")
        udf_create_query = f"""SELECT VALUE doc
                            FROM (SELECT i.*
                              EXCLUDE product_category,
                                      product_reviews,
                                      product_link
                              FROM [item] AS i
                              ) AS doc
                            LIMIT 1"""
        udf_coll_obj = self.create_udf_and_remotedataset(udf_create_query)
        for field in ["product_category", "product_reviews", "product_link"]:
            udf_select_query = f"select {field} from {udf_coll_obj.name}"
            status, _, errors, results, _, _ = self.cbas_util.execute_statement_on_cbas_util(
                self.columnar_cluster, udf_select_query)
            if results[0]:
                self.fail("Query result is not empty for selecting excluded field.")
        self.log.info("Test completed successfully.")

    def test_smt_create_udf_if_not_exists(self):
        """
        Test create udf if not exists.
        """
        self.log.info("Running transform function if not exists.")

        self.log.info("Start test setup.")
        self.test_setup()
        self.log.info("Create UDF with transform function.")
        udf_name = self.cbas_util.generate_name()
        udf_create_query = f"""SELECT quantity, product_name
                                        FROM [item] as item
                                        LIMIT 1 """
        self.create_udf_and_remotedataset(udf_create_query, udf_name=udf_name)

        if not self.cbas_util.create_udf(
                cluster=self.columnar_cluster, name=udf_name, parameters=["item"],
                body=udf_create_query, transform_function=True, if_not_exists=True):
            self.fail(f"Error while creating UDF with name {udf_name} which already exists using if not exists.")
        self.log.info("Test completed successfully.")

    def test_smt_replace_udf(self):
        """
        Test replace transform function.
        """
        self.log.info("Running transform function with Replace UDF.")

        self.log.info("Start test setup.")
        self.test_setup()
        self.log.info("Create UDF with transform function.")
        udf_create_query = f"""SELECT quantity, product_name
                                        FROM [item] as item
                                        LIMIT 1 """
        udf_name = self.cbas_util.generate_name()
        if not self.cbas_util.create_udf(
                cluster=self.columnar_cluster, name=udf_name, parameters=["item"],
                body=udf_create_query, transform_function=True):
            self.fail("Error while creating UDF.")
        self.log.info("Replace UDF with a new query.")
        udf_create_query = f"""SELECT product_name || seller_name as name_seller
                                FROM [item] as item
                                LIMIT 1 """
        udf_coll_obj = self.create_udf_and_remotedataset(udf_create_query, udf_name=udf_name, or_replace=True)
        udf_select_query = f"select name_seller from {udf_coll_obj.name}"
        remote_query = f"select product_name || seller_name as name_seller from {self.remote_dataset_name}"
        self.log.info("Validate collection created with the transform function.")
        self.validate_collection_created_from_udf(udf_select_query, remote_query)
        self.log.info("Test completed successfully.")

    def test_smt_nested_udf(self):
        """
        Test create nested transform function.
        """
        self.log.info("Running nested transform functions.")

        self.log.info("Start test setup.")
        self.test_setup()
        self.log.info("Create UDF with transform function.")
        udf_create_query = f"""SELECT VALUE doc
                                    FROM (SELECT i.*
                                      EXCLUDE product_category,
                                              product_reviews,
                                              product_link
                                      FROM [item] AS i
                                      ) AS doc
                                    LIMIT 1"""
        udf_name = self.cbas_util.generate_name()
        if not self.cbas_util.create_udf(
                cluster=self.columnar_cluster, name=udf_name, parameters=["item"],
                body=udf_create_query, transform_function=True):
            self.fail("Error while creating UDF.")
        nested_udf_query = f"""SELECT VALUE doc
                                    FROM (SELECT i.*
                                      EXCLUDE product_specs,
                                              product_features,
                                              product_image_links
                                      FROM {udf_name}(item) AS i
                                      ) AS doc
                                    LIMIT 1"""
        udf_coll_obj = self.create_udf_and_remotedataset(nested_udf_query)
        for field in ["product_category", "product_reviews", "product_link",
                      "product_specs", "product_features", "product_image_links"]:
            udf_select_query = f"select {field} from {udf_coll_obj.name}"
            status, _, errors, results, _, _ = self.cbas_util.execute_statement_on_cbas_util(
                self.columnar_cluster, udf_select_query)
            if results[0]:
                self.fail("Query result is not empty for selecting excluded field.")
        self.log.info("Test completed successfully.")

    def test_smt_delete_kv_items(self):
        """
        Test create transform function delete kv items.
        """
        self.log.info("Running transform function and delete items in kv.")
        self.log.info("Start test setup.")
        self.test_setup()
        self.log.info("Create UDF with transform function.")
        udf_create_query = f"""SELECT quantity, product_name
                                        FROM [item] as item
                                        LIMIT 1 """
        udf_coll_obj = self.create_udf_and_remotedataset(udf_create_query)
        self.log.info("Delete half the docs created from cluster.")
        for bucket in self.remote_cluster.buckets:
            SiriusCouchbaseLoader.create_clients_in_pool(
                self.remote_cluster.master, self.remote_cluster.master.rest_username,
                self.remote_cluster.master.rest_password,
                bucket.name, req_clients=1)

        delete_end_index = self.initial_doc_count // 2
        self.log.info(f"Deleting {delete_end_index} docs from remote cluster")
        # self.load_doc_to_remote_collections(self.remote_cluster, "Product",
        #                                     delete_start_index=0, delete_end_index=delete_end_index,
        #                                     create_percent=0, delete_percent=100)

        self.load_remote_collections(self.remote_cluster, template="Product",
                                     delete_start_index=0, delete_end_index=delete_end_index,
                                     wait_for_completion=True)

        udf_select_query = f"select quantity, product_name from {udf_coll_obj.name}"
        remote_query = f"select quantity, product_name from {self.remote_dataset_name}"
        self.log.info("Validate collection created with the transform function.")
        self.validate_collection_created_from_udf(udf_select_query, remote_query)
        self.log.info("Test completed successfully.")

    def test_smt_update_kv_items(self):
        """
        Test create transform function update kv items.
        """
        self.log.info("Running transform function and update items in kv.")
        self.log.info("Start test setup.")
        self.test_setup()
        self.log.info("Create UDF with transform function.")
        udf_create_query = f"""SELECT quantity, product_name
                                        FROM [item] as item
                                        LIMIT 1 """
        udf_coll_obj = self.create_udf_and_remotedataset(udf_create_query)
        self.log.info("Update half the docs created from cluster.")
        for bucket in self.remote_cluster.buckets:
            SiriusCouchbaseLoader.create_clients_in_pool(
                self.remote_cluster.master, self.remote_cluster.master.rest_username,
                self.remote_cluster.master.rest_password,
                bucket.name, req_clients=1)

        update_end_index = self.initial_doc_count // 2
        self.log.info(f"Updating {update_end_index} docs from remote cluster")
        # self.load_doc_to_remote_collections(self.remote_cluster, "Product",
        #                                     update_start_index=0, update_end_index=update_end_index)

        self.load_remote_collections(self.remote_cluster, template="Product",
                                     update_start_index=0, update_end_index=update_end_index,
                                     wait_for_completion=True)

        udf_select_query = f"select quantity, product_name from {udf_coll_obj.name}"
        remote_query = f"select quantity, product_name from {self.remote_dataset_name}"
        self.log.info("Validate collection created with the transform function.")
        self.validate_collection_created_from_udf(udf_select_query, remote_query)
        self.log.info("Test completed successfully.")

    def test_smt_rename_field(self):
        """
        Test create transform function with renaming fields.
        """
        self.log.info("Running transform function with renaming fields.")
        self.log.info("Start test setup.")
        self.test_setup()
        self.log.info("Create UDF with transform function.")
        udf_create_query = f"""SELECT VALUE doc FROM
                                (SELECT item.*, quantity as product_quantity, weight as product_weight
                                EXCLUDE quantity, weight
                                FROM [item] as item) AS doc
                                LIMIT 1"""
        udf_coll_obj = self.create_udf_and_remotedataset(udf_create_query)
        udf_select_query = f"select product_quantity, product_weight from {udf_coll_obj.name}"
        remote_query = f"select quantity as product_quantity, weight as product_weight from {self.remote_dataset_name}"
        self.log.info("Validate collection created with the transform function.")
        self.validate_collection_created_from_udf(udf_select_query, remote_query)
        self.log.info("Test completed successfully.")

    def test_smt_add_static_field(self):
        """
        Test create transform function with add static field.
        """
        self.log.info("Running transform function with add static field.")
        self.log.info("Start test setup.")
        self.test_setup()
        self.log.info("Create UDF with transform function.")
        udf_create_query = f"""SELECT quantity, product_name,1 as version
                                FROM [item] as item
                                LIMIT 1 """
        udf_coll_obj = self.create_udf_and_remotedataset(udf_create_query)
        self.log.info("Sleep for a second before validation.")
        time.sleep(1)
        udf_select_query = f"select quantity, product_name, version from {udf_coll_obj.name}"
        remote_query = f"select quantity, product_name,1 as version from {self.remote_dataset_name}"
        self.log.info("Validate collection created with the transform function.")
        self.validate_collection_created_from_udf(udf_select_query, remote_query)
        self.log.info("Test completed successfully.")

    def test_smt_drop_unused_udf(self):
        """
        Test smt drop unused udf.
        """
        self.log.info("Running transform function for drop unused udf.")
        self.log.info("Start test setup.")
        self.test_setup()
        self.log.info("Create UDF with transform function.")
        udf_create_query = f"""SELECT quantity, product_name
                                                FROM [item] as item
                                                LIMIT 1 """
        udf_name = self.cbas_util.generate_name()
        if not self.cbas_util.create_udf(
                cluster=self.columnar_cluster, name=udf_name, parameters=["item"],
                body=udf_create_query, transform_function=True):
            self.fail("Error while creating UDF.")

        if not self.drop_udf():
            self.fail("Unable to drop UDF.")
        self.log.info("Test completed successfully.")

    def test_smt_drop_udf_after_collection_is_deleted(self):
        """
        Test smt drop udf after collection using it is deleted.
        """
        self.log.info("Running transform function for drop udf after collection using it is deleted.")
        self.log.info("Start test setup.")
        self.test_setup()
        self.log.info("Create UDF with transform function.")
        udf_create_query = f"""SELECT quantity, product_name
                                                FROM [item] as item
                                                LIMIT 1 """
        udf_name = self.cbas_util.generate_name()
        udf_coll_obj = self.create_udf_and_remotedataset(udf_create_query, udf_name=udf_name)
        if not self.cbas_util.drop_dataset(self.columnar_cluster, udf_coll_obj):
            self.fail("Error while dropping dataset.")

        if not self.drop_udf():
            self.fail("Unable to drop UDF.")
        self.log.info("Test completed successfully.")

    def test_smt_type_conversions(self):
        """
        Test create transform function with type conversions.
        """
        self.log.info("Running transform function with type conversions.")
        self.log.info("Start test setup.")
        self.test_setup()
        transformations = [
            {
                "log_label": "number to string",
                "func": "TO_STRING(mutated)",
                "alias": "mutated",
                "fields": ["mutated", "product_name"]
            },
            {
                "log_label": "string to number",
                "func": "TO_NUMBER(weight)",
                "alias": "weight",
                "fields": ["weight", "product_name"]
            }
        ]

        results = []
        for transform in transformations:
            self.log.info(f"Transform function for {transform['log_label']}.")

            # Compose field selection string for SELECT clause
            select_fields = f"{transform['func']} AS {transform['alias']}, " + \
                            ", ".join([f for f in transform['fields'] if f != transform['alias']])

            udf_create_query = f"""SELECT VALUE doc FROM
                                    (SELECT {select_fields}
                                     FROM [item] AS item) AS doc
                                    LIMIT 1"""
            udf_coll_obj = self.create_udf_and_remotedataset(udf_create_query)

            # Build SELECT clause from alias + other fields
            udf_select_query = f"SELECT {', '.join(transform['fields'])} FROM {udf_coll_obj.name}"
            remote_query = f"""SELECT {select_fields} FROM {self.remote_dataset_name}"""

            self.log.info("Validate collection created with the transform function.")
            result = self.validate_collection_created_from_udf(
                udf_select_query, remote_query, continue_test=True
            )
            results.append((transform["log_label"], result))

        # Check results
        for label, result in results:
            if not result[0]:
                self.fail(
                    f"Data mismatch between expected and actual result of transform function for {label}. "
                    f"{result[1]}"
                )

        self.log.info("Test completed successfully.")

    def test_smt_arithmetic_functions(self):
        """
        Test create transform function with arithmetic functions.
        """
        self.log.info("Running transform function with arithmetic functions.")
        self.log.info("Start test setup.")
        self.test_setup()
        arithmetic_functions = ["AVG", "SUM", "MIN", "MAX"]
        results = []
        for func in arithmetic_functions:
            self.log.info(f"Transform function for {func}.")
            udf_create_query = f"""SELECT VALUE doc FROM
                                    (SELECT 
                                    {func}(r.ratings.performance) AS product_review_perf_{func}
                                    FROM [item] AS b
                                    UNNEST b.product_reviews AS r) AS doc
                                    LIMIT 1"""
            udf_coll_obj = self.create_udf_and_remotedataset(udf_create_query)
            udf_select_query = f"select product_review_perf_{func} from {udf_coll_obj.name}"
            remote_query = f"""SELECT 
                                {func}(r.ratings.performance) AS product_review_perf_{func}
                                from {self.remote_dataset_name} AS b
                                UNNEST b.product_reviews AS r
                                GROUP BY b;"""

            self.log.info("Validate collection created with the transform function.")
            results.append(self.validate_collection_created_from_udf(udf_select_query,
                                                                     remote_query,
                                                                     continue_test=True))
        for i, result in enumerate(results):
            if not result[0]:
                self.fail(
                    f"For arithmetic function {arithmetic_functions[i]}"
                    f"Data mismatch between expected and actual result of transform function."
                    f"{result[1]}")

        self.log.info("Test completed successfully.")

    def test_smt_array_functions(self):
        """
        Test create transform function with array functions.
        """
        self.log.info("Running transform function with array functions.")

        self.log.info("Start test setup.")
        self.test_setup()
        array_functions = ["ARRAY_COUNT", "ARRAY_SUM"]
        results = []

        for func in array_functions:
            self.log.info(f"Create UDF with {func} transform function.")
            if func == "ARRAY_COUNT":
                expr = "ARRAY_COUNT(item.product_reviews)"
            elif func == "ARRAY_SUM":
                expr = "ARRAY_SUM(item.product_reviews)"
            else:
                continue
            udf_create_query = f"""
                SELECT VALUE doc FROM
                                (SELECT {expr} AS result_field
                                FROM [item] as item
                                ) as doc
                                LIMIT 1"""
            remote_query = f"""
                SELECT {expr} AS result_field
                FROM {self.remote_dataset_name} AS item
                GROUP by item;"""

            udf_coll_obj = self.create_udf_and_remotedataset(udf_create_query)
            udf_select_query = f"SELECT result_field FROM {udf_coll_obj.name}"

            self.log.info(f"Validate collection created with the transform function: {func}")
            results.append(self.validate_collection_created_from_udf(
                udf_select_query, remote_query, continue_test=True
            ))

        # Final validation loop
        for i, result in enumerate(results):
            if not result[0]:
                self.fail(
                    f"For array function {array_functions[i]}: "
                    f"Data mismatch between expected and actual result of transform function. {result[1]}"
                )
        self.log.info("Test completed successfully.")

    def test_smt_filter_where_clause(self):
        """
        Test create transform function with filtering.
        """
        self.log.info("Running transform function with filtering.")
        self.log.info("Start test setup.")
        self.test_setup()
        filter_string = ["where num_sold<10000", "where avg_rating>2"]
        results = []
        for filter_s in filter_string:
            self.log.info(f"Transform function for filter string {filter_s}.")
            udf_create_query = f"""SELECT VALUE doc FROM
                                    (SELECT 
                                    b.*
                                    FROM [item] AS b
                                    {filter_s}) AS doc
                                    LIMIT 1"""
            udf_coll_obj = self.create_udf_and_remotedataset(udf_create_query, wait_for_ingestion=False)
            udf_select_query = f"select s.* from {udf_coll_obj.name} as s"
            remote_query = f"""SELECT 
                                b.*
                                from {self.remote_dataset_name} AS b
                                {filter_s};"""

            self.log.info("Validate collection created with the transform function.")
            results.append(self.validate_collection_created_from_udf(udf_select_query,
                                                                     remote_query,
                                                                     continue_test=True))
        for i, result in enumerate(results):
            if not result[0]:
                self.fail(
                    f"For filter {filter_string[i]}"
                    f"Data mismatch between expected and actual result of transform function."
                    f"{result[1]}")

        self.log.info("Test completed successfully.")

    def test_smt_flatten_dict(self):
        """
        Test create transform function to flatten dict.
        """
        self.log.info("Running transform function to flatten dict.")
        self.log.info("Start test setup.")
        self.test_setup()
        self.log.info("Create UDF with transform function.")
        udf_create_query = f"""SELECT VALUE doc FROM
                        (SELECT r.date, r.author,
                r.ratings.utility AS ratings_utility, r.ratings.performance AS ratings_performance,
                r.ratings.build_quality AS ratings_build_quality,
                r.ratings.pricing AS ratings_pricing,
                r.ratings.rating_value AS ratings_rating_value
                FROM [item] AS b
                UNNEST b.product_reviews AS r) AS doc LIMIT 1"""
        udf_coll_obj = self.create_udf_and_remotedataset(udf_create_query)
        udf_select_query = (f"select date, author, ratings_utility, ratings_performance, ratings_build_quality, "
                            f"ratings_pricing, ratings_rating_value from {udf_coll_obj.name}")
        remote_query = (f"select b.product_reviews[0].date AS date,"
                        f"b.product_reviews[0].author AS author, "
                        f"b.product_reviews[0].ratings.utility AS ratings_utility,"
                        f"b.product_reviews[0].ratings.performance AS ratings_performance,"
                        f"b.product_reviews[0].ratings.build_quality AS ratings_build_quality,"
                        f"b.product_reviews[0].ratings.pricing AS ratings_pricing,"
                        f"b.product_reviews[0].ratings.rating_value AS ratings_rating_value from "
                        f"{self.remote_dataset_name} AS b")
        self.log.info("Validate collection created with the transform function.")
        self.validate_collection_created_from_udf(udf_select_query, remote_query)
        self.log.info("Test completed successfully.")

    def test_smt_return_more_than_one_doc(self):
        """
        Test create transform function returning more than one doc.
        """
        self.log.info("Create UDF with transform function that returns more than one doc.")
        udf_create_query = f"""SELECT VALUE doc FROM (
                        SELECT _reviews.author, item.name
                        FROM [item] as item
                        UNNEST item.reviews AS _reviews) AS doc"""
        udf_name = self.cbas_util.generate_name()
        if self.cbas_util.create_udf(
                cluster=self.columnar_cluster, name=udf_name, parameters=["item"],
                body=udf_create_query, transform_function=True):
            self.fail("Error created UDF which returns more than 1 doc.")
        else:
            self.log.info("Could not create UDF returning more than 1 doc.")

        self.log.info("Test completed successfully.")

    def test_smt_include_5_of_1000_columns(self):
        """
        Test transform function that includes 5 keys out of 1000 keys.
        """
        self.test_setup(load_docs=False)

        # Generate a JSON document with 1000 keys
        doc_key = "doc_with_1000_keys"
        doc_value = self._generate_json_with_1000_keys()

        # Create document generator for a single document
        template = {"doc_data": doc_value}
        doc_gen = doc_generator(doc_key, 0, 1, doc_size=len(json.dumps(template)))

        # Load the document into the bucket
        self.log.info(f"Loading document with 1000 keys into bucket {self.remote_cluster.buckets[0].name}")

        for bucket in self.remote_cluster.buckets:
            SiriusCouchbaseLoader.create_clients_in_pool(
                self.remote_cluster.master, self.remote_cluster.master.rest_username,
                self.remote_cluster.master.rest_password,
                bucket.name, req_clients=1)

        # Use async load to create the document
        task = self.task.async_load_gen_docs(
            self.remote_cluster, self.remote_cluster.buckets[0], doc_gen,
            DocLoading.Bucket.DocOps.CREATE,
            load_using=self.load_docs_using,
            durability=self.durability_level,
            process_concurrency=1,
            suppress_error_table=False,
            print_ops_rate=False)

        # Wait for the task to complete
        self.task_manager.get_task_result(task)

        get_columns_query = f"SELECT OBJECT_NAMES(d) AS column_names FROM {self.remote_dataset_name} AS d LIMIT 1;"
        status, _, errors, results, _, _ = self.cbas_util.execute_statement_on_cbas_util(
            self.columnar_cluster, get_columns_query)

        columns = ','.join(results[0]['column_names'][0:5])

        udf_create_query = f"""SELECT VALUE doc FROM
                                (SELECT {columns}
                        FROM [item] as b) AS doc LIMIT 1"""
        udf_coll_obj = self.create_udf_and_remotedataset(udf_create_query, wait_for_ingestion=False)
        udf_select_query = f"select {columns} from {udf_coll_obj.name}"
        remote_query = f"select {columns} from {self.remote_dataset_name}"
        self.log.info("Validate collection created with the transform function.")
        self.validate_collection_created_from_udf(udf_select_query, remote_query)
        self.log.info("Test completed successfully.")

    def _generate_json_with_1000_keys(self):
        """
        Generate a JSON object with 1000 keys
        """
        doc_data = {}

        # Generate 1000 unique keys with different value types
        for i in range(1000):
            key_name = f"key_{i:04d}"

            # Generate different types of values
            value_type = i % 6

            if value_type == 0:
                # String value
                value = f"value_{i}_{''.join(random.choices(string.ascii_letters, k=10))}"
            elif value_type == 1:
                # Integer value
                value = random.randint(1, 1000000)
            elif value_type == 2:
                # Float value
                value = round(random.uniform(0.0, 1000.0), 2)
            elif value_type == 3:
                # Boolean value
                value = bool(i % 2)
            elif value_type == 4:
                # Array value
                value = [random.randint(1, 100) for _ in range(random.randint(1, 5))]
            else:
                # Nested object
                value = {
                    "nested_key_1": f"nested_value_{i}",
                    "nested_key_2": random.randint(1, 100),
                    "nested_key_3": [random.randint(1, 10) for _ in range(3)]
                }

            doc_data[key_name] = value

        return doc_data

    def test_smt_filter_with_where_clause_data_added_after(self):
        """
        Test create transform function with where clause where data is added after creating remote dataset.
        """
        self.log.info("Running transform function with where clause where data is added after creating remote dataset.")
        self.log.info("Start test setup.")
        self.test_setup(load_docs=False)
        self.log.info("Create UDF with transform function.")
        udf_create_query = f"""SELECT product_name
                        FROM [item] as item
                        WHERE num_sold>10000
                        LIMIT 1 """
        udf_coll_obj = self.create_udf_and_remotedataset(udf_create_query, wait_for_ingestion=False)
        
        # Load data to remote collection after creating the remote dataset
        self.log.info("Load data to remote collection after creating remote dataset.")
        for bucket in self.remote_cluster.buckets:
            SiriusCouchbaseLoader.create_clients_in_pool(
                self.remote_cluster.master, self.remote_cluster.master.rest_username,
                self.remote_cluster.master.rest_password,
                bucket.name, req_clients=1)

        self.load_remote_collections(self.remote_cluster, template="Product",
                                     create_start_index=0, create_end_index=self.initial_doc_count,
                                     wait_for_completion=True)
        
        udf_select_query = f"select product_name from {udf_coll_obj.name}"
        remote_query = f"select product_name from {self.remote_dataset_name} where num_sold>10000"
        self.log.info("Validate collection created with the transform function.")
        self.validate_collection_created_from_udf(udf_select_query, remote_query)
        self.log.info("Test completed successfully.")

    def test_smt_non_existing_field(self):
        """
        Test create transform function with non-existing field product_amount.
        """
        self.log.info("Running transform function with non-existing field product_amount.")
        self.log.info("Start test setup.")
        self.test_setup()
        self.log.info("Create UDF with transform function using non-existing field.")
        udf_create_query = f"""SELECT product_name, product_amount
                        FROM [item] as item
                        LIMIT 1 """
        udf_coll_obj = self.create_udf_and_remotedataset(udf_create_query, wait_for_ingestion=False)
        udf_select_query = f"select product_name, product_amount from {udf_coll_obj.name}"
        remote_query = f"select product_name, product_amount from {self.remote_dataset_name}"
        self.log.info("Validate collection created with the transform function.")
        self.validate_collection_created_from_udf(udf_select_query, remote_query)
        self.log.info("Test completed successfully.")

    def test_smt_special_char_field(self):
        """
        Test create transform function with field containing special characters.
        """
        self.log.info("Running transform function with field containing special characters.")
        self.log.info("Start test setup.")
        self.test_setup(load_docs=False)
        
        # Insert 10 specific rows with special character field
        self.log.info("Insert 10 rows with special character field.")
        for bucket in self.remote_cluster.buckets:
            SiriusCouchbaseLoader.create_clients_in_pool(
                self.remote_cluster.master, self.remote_cluster.master.rest_username,
                self.remote_cluster.master.rest_password,
                bucket.name, req_clients=1)

        # Create documents with special character field
        for i in range(10):
            doc_key = f"special_char_doc_{i}"
            doc_value = {
                "product_name": f"Product_{i}",
                "quantity": i * 100,
                "field_name!#%": f"special_value_{i}!#%",
                "num_sold": i * 1000,
                "weight": i * 2.5
            }
            
            # Create document generator for a single document
            template = {"doc_data": doc_value}
            doc_gen = doc_generator(doc_key, 0, 1, doc_size=len(json.dumps(template)))

            # Use async load to create the document
            task = self.task.async_load_gen_docs(
                self.remote_cluster, self.remote_cluster.buckets[0], doc_gen,
                DocLoading.Bucket.DocOps.CREATE,
                load_using=self.load_docs_using,
                durability=self.durability_level,
                process_concurrency=1,
                suppress_error_table=False,
                print_ops_rate=False)

            # Wait for the task to complete
            self.task_manager.get_task_result(task)
        
        self.log.info("Create UDF with transform function including special character field.")
        udf_create_query = f"""SELECT quantity, product_name, `field_name!#%`
                                FROM [item] as item
                                LIMIT 1 """
        udf_coll_obj = self.create_udf_and_remotedataset(udf_create_query, wait_for_ingestion=False)
        udf_select_query = f"select quantity, product_name, `field_name!#%` from {udf_coll_obj.name}"
        remote_query = f"select quantity, product_name, `field_name!#%` from {self.remote_dataset_name}"
        self.log.info("Validate collection created with the transform function.")
        self.validate_collection_created_from_udf(udf_select_query, remote_query)
        self.log.info("Test completed successfully.")

    def test_smt_drop_remote_collection(self):
        """
        Test create transform function with multiple fields and then drop remote collection and bucket.
        """
        self.log.info("Running transform function with select multiple fields and drop remote collection.")
        self.log.info("Start test setup.")
        self.test_setup()
        self.log.info("Create UDF with transform function.")
        udf_create_query = f"""SELECT quantity, product_name
                                FROM [item] as item
                                LIMIT 1 """
        udf_coll_obj = self.create_udf_and_remotedataset(udf_create_query)
        
        # Drop collection in remote cluster after creating the UDF and remote dataset
        self.log.info("Drop collection in remote cluster.")
        for bucket in self.remote_cluster.buckets:
            for scope_name, scope in bucket.scopes.items():
                if scope_name != "_system":
                    for collection_name, collection in scope.collections.items():
                        self.log.info(f"Dropping collection {bucket.name}.{scope_name}.{collection_name}")
                        # Drop the collection from the remote cluster
                        if not self.bucket_util.drop_collection(
                        node=self.columnar_cluster.master, bucket=bucket.name,
                        scope_name=scope_name,
                        collection_name=collection_name
                        ):
                            self.fail(f"Error while dropping collection {bucket.name}.{scope_name}.{collection_name}")
        
        # Validate the collection created with the transform function after dropping remote collection
        udf_select_query = f"select quantity, product_name from {udf_coll_obj.name}"
        remote_query = f"select quantity, product_name from {self.remote_dataset_name}"
        self.log.info("Validate collection created with the transform function after dropping remote collection.")
        self.validate_collection_created_from_udf(udf_select_query, remote_query)

        if runtype == "columnar":
            # Drop buckets from remote cluster
            if hasattr(self, "remote_cluster") and self.remote_cluster:
                self.delete_all_buckets_from_capella_cluster(self.tenant, self.remote_cluster)
        
        self.log.info("Test completed successfully.")

    def test_smt_drop_remote_dataset_confirm_dropped(self):
        """
        Test create UDF and remote dataset using UDF, then drop remote dataset and confirm it is dropped.
        """
        self.log.info("Running test to create UDF and remote dataset, then drop remote dataset and confirm it is dropped.")
        self.log.info("Start test setup.")
        self.test_setup()
        self.log.info("Create UDF with transform function.")
        udf_create_query = f"""SELECT quantity, product_name
                                FROM [item] as item
                                LIMIT 1 """
        udf_coll_obj = self.create_udf_and_remotedataset(udf_create_query)
        
        # Validate the collection created with the transform function
        udf_select_query = f"select quantity, product_name from {udf_coll_obj.name}"
        remote_query = f"select quantity, product_name from {self.remote_dataset_name}"
        self.log.info("Validate collection created with the transform function.")
        self.validate_collection_created_from_udf(udf_select_query, remote_query)
        
        # Drop the remote dataset
        self.log.info("Drop remote dataset.")
        if not self.cbas_util.drop_dataset(self.columnar_cluster, udf_coll_obj):
            self.fail(f"Error while dropping dataset {udf_coll_obj.name}")
        
        # Confirm that the remote dataset is dropped by trying to query it
        self.log.info("Confirm that the remote dataset is dropped.")
        try:
            status, _, errors, results, _, _ = self.cbas_util.execute_statement_on_cbas_util(
                self.columnar_cluster, f"SELECT * FROM {udf_coll_obj.name} LIMIT 1")
            if status == "success":
                self.fail(f"Dataset {udf_coll_obj.name} still exists after being dropped")
            else:
                self.log.info(f"Dataset {udf_coll_obj.name} successfully dropped - query failed as expected")
        except Exception as e:
            self.log.info(f"Dataset {udf_coll_obj.name} successfully dropped - exception occurred as expected: {e}")
        
        self.log.info("Test completed successfully.")

    def test_smt_udf_multiple_remote_datasets(self):
        """
        Test create a UDF and 2 remote datasets using that UDF, then validate both collections.
        """
        self.log.info("Running test to create UDF and 2 remote datasets using that UDF.")
        self.log.info("Start test setup.")
        self.test_setup()
        self.log.info("Create UDF with transform function.")
        udf_create_query = f"""SELECT quantity, product_name
                                FROM [item] as item
                                LIMIT 1 """
        udf_name = self.cbas_util.generate_name()
        if not self.cbas_util.create_udf(
                cluster=self.columnar_cluster, name=udf_name, parameters=["item"],
                body=udf_create_query, transform_function=True):
            self.fail(f"Error while creating UDF with name {udf_name}")
        
        # Create first remote dataset using the UDF
        self.log.info("Create first remote dataset using the UDF.")
        remote_link = self.cbas_util.get_all_link_objs("couchbase")[0]
        remote_coll_obj1 = self.cbas_util.create_remote_dataset_obj(
            self.columnar_cluster,
            self.remote_cluster.buckets[0].name,
            "_default",
            "_default",
            remote_link,
            use_only_existing_db=True,
            use_only_existing_dv=True,
            database="Default",
            dataverse="Default",
            capella_as_source=True)[0]
        result1 = self.cbas_util.create_remote_dataset(
            self.columnar_cluster,
            remote_coll_obj1.name,
            remote_coll_obj1.full_kv_entity_name,
            remote_coll_obj1.link_name,
            remote_coll_obj1.dataverse_name,
            remote_coll_obj1.database_name,
            transform_function=udf_name)
        if not result1:
            self.fail("Failed to create first remote collection {} from transform function {}".format(
                remote_coll_obj1.name, udf_name))
        
        # Create second remote dataset using the same UDF
        self.log.info("Create second remote dataset using the same UDF.")
        remote_coll_obj2 = self.cbas_util.create_remote_dataset_obj(
            self.columnar_cluster,
            self.remote_cluster.buckets[0].name,
            "_default",
            "_default",
            remote_link,
            use_only_existing_db=True,
            use_only_existing_dv=True,
            database="Default",
            dataverse="Default",
            capella_as_source=True)[0]
        result2 = self.cbas_util.create_remote_dataset(
            self.columnar_cluster,
            remote_coll_obj2.name,
            remote_coll_obj2.full_kv_entity_name,
            remote_coll_obj2.link_name,
            remote_coll_obj2.dataverse_name,
            remote_coll_obj2.database_name,
            transform_function=udf_name)
        if not result2:
            self.fail("Failed to create second remote collection {} from transform function {}".format(
                remote_coll_obj2.name, udf_name))
        
        # Wait for ingestion to complete for both datasets
        self.cbas_util.wait_for_ingestion_complete(self.columnar_cluster,
                                                   remote_coll_obj1.name,
                                                   self.initial_doc_count)
        self.cbas_util.wait_for_ingestion_complete(self.columnar_cluster,
                                                   remote_coll_obj2.name,
                                                   self.initial_doc_count)
        
        # Validate first collection
        self.log.info("Validate first collection created with the transform function.")
        udf_select_query1 = f"select quantity, product_name from {remote_coll_obj1.name}"
        remote_query = f"select quantity, product_name from {self.remote_dataset_name}"
        self.validate_collection_created_from_udf(udf_select_query1, remote_query)
        
        # Validate second collection
        self.log.info("Validate second collection created with the transform function.")
        udf_select_query2 = f"select quantity, product_name from {remote_coll_obj2.name}"
        self.validate_collection_created_from_udf(udf_select_query2, remote_query)
        
        self.log.info("Test completed successfully.")

    def test_smt_add_quantity_grade_field(self):
        """
        Test create transform function with add quantity_grade field based on quantity ranges.
        """
        self.log.info("Running transform function with add quantity_grade field.")
        self.log.info("Start test setup.")
        self.test_setup()
        self.log.info("Create UDF with transform function.")
        udf_create_query = f"""SELECT quantity, product_name,
                                CASE 
                                    WHEN quantity < 1000 THEN 'less'
                                    WHEN quantity >= 1000 AND quantity <= 2000 THEN 'medium'
                                    WHEN quantity > 2000 THEN 'large'
                                    ELSE 'unknown'
                                END AS quantity_grade
                                FROM [item] as item
                                LIMIT 1 """
        udf_coll_obj = self.create_udf_and_remotedataset(udf_create_query)
        self.log.info("Sleep for a second before validation.")
        time.sleep(1)
        udf_select_query = f"select quantity, product_name, quantity_grade from {udf_coll_obj.name}"
        remote_query = f"""SELECT quantity, product_name,
                                CASE 
                                    WHEN quantity < 1000 THEN 'less'
                                    WHEN quantity >= 1000 AND quantity <= 2000 THEN 'medium'
                                    WHEN quantity > 2000 THEN 'large'
                                    ELSE 'unknown'
                                END AS quantity_grade
                                FROM {self.remote_dataset_name}"""
        self.log.info("Validate collection created with the transform function.")
        self.validate_collection_created_from_udf(udf_select_query, remote_query)
        self.log.info("Test completed successfully.")

    def test_smt_drop_udf_while_in_use(self):
        """
        Test drop a UDF while it is being used in collection and check that it creates an error.
        """
        self.log.info("Running test to drop UDF while it is being used in collection.")
        self.log.info("Start test setup.")
        self.test_setup()
        self.log.info("Create UDF with transform function.")
        udf_create_query = f"""SELECT quantity, product_name
                                FROM [item] as item
                                LIMIT 1 """
        udf_name = self.cbas_util.generate_name()
        if not self.cbas_util.create_udf(
                cluster=self.columnar_cluster, name=udf_name, parameters=["item"],
                body=udf_create_query, transform_function=True):
            self.fail(f"Error while creating UDF with name {udf_name}")
        
        # Create remote dataset using the UDF
        self.log.info("Create remote dataset using the UDF.")
        remote_link = self.cbas_util.get_all_link_objs("couchbase")[0]
        remote_coll_obj = self.cbas_util.create_remote_dataset_obj(
            self.columnar_cluster,
            self.remote_cluster.buckets[0].name,
            "_default",
            "_default",
            remote_link,
            use_only_existing_db=True,
            use_only_existing_dv=True,
            database="Default",
            dataverse="Default",
            capella_as_source=True)[0]
        result = self.cbas_util.create_remote_dataset(
            self.columnar_cluster,
            remote_coll_obj.name,
            remote_coll_obj.full_kv_entity_name,
            remote_coll_obj.link_name,
            remote_coll_obj.dataverse_name,
            remote_coll_obj.database_name,
            transform_function=udf_name)
        if not result:
            self.fail("Failed to create remote collection {} from transform function {}".format(
                remote_coll_obj.name, udf_name))
        
        # Wait for ingestion to complete
        self.cbas_util.wait_for_ingestion_complete(self.columnar_cluster,
                                                   remote_coll_obj.name,
                                                   self.initial_doc_count)
        
        # Validate that the collection works correctly
        self.log.info("Validate that the collection works correctly.")
        udf_select_query = f"select quantity, product_name from {remote_coll_obj.name}"
        remote_query = f"select quantity, product_name from {self.remote_dataset_name}"
        self.validate_collection_created_from_udf(udf_select_query, remote_query)
        
        # Attempt to drop the UDF while it is being used
        self.log.info("Attempt to drop the UDF while it is being used.")
        try:
            drop_result = self.cbas_util.drop_udf(self.columnar_cluster, udf_name, None, None, None)
            if drop_result:
                self.fail("UDF was successfully dropped while being used in a collection - this should have failed")
            else:
                self.log.info("UDF drop failed as expected while being used in a collection")
        except Exception as e:
            self.log.info(f"UDF drop threw an exception as expected: {e}")
        
        self.log.info("Test completed successfully.")
