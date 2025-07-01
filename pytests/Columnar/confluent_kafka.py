import time

from Columnar.columnar_base import ColumnarBaseTest
from couchbase_utils.kafka_util.kafka_connect_util import KafkaConnectUtil
from couchbase_utils.kafka_util.confluent_utils import ConfluentUtils

from Jython_tasks.sirius_task import MongoUtil
from sirius_client_framework.sirius_constants import SiriusCodes
from TestInput import TestInputSingleton
runtype = TestInputSingleton.input.param("runtype", "default").lower()
if runtype == "columnar":
    from Columnar.columnar_base import ColumnarBaseTest
else:
    from Columnar.onprem.columnar_onprem_base import ColumnarOnPremBase as ColumnarBaseTest


class ConfluentKafka(ColumnarBaseTest):
    def setUp(self):
        super(ConfluentKafka, self).setUp()
        if runtype == "columnar":
            self.columnar_cluster = self.tenant.columnar_instances[0]
        else:
            self.columnar_cluster = self.analytics_cluster

        if not self.columnar_spec_name:
            self.columnar_spec_name = "full_template"

        # Initialize sirius doc loader utils
        self.mongo_util = MongoUtil(
            task_manager=self.task_manager,
            hostname=self.input.param("mongo_hostname"),
            username=self.input.param("mongo_username"),
            password=self.input.param("mongo_password")
        )
        self.mongo_collections = {}
        # Initial number of docs to be loaded in external DB collections,
        # remote collections and standalone collections.
        self.initial_doc_count = self.input.param("initial_doc_count", 1000)

        # Initialize variables for Kafka
        self.kafka_topic_prefix = f"confluent_regression_{int(time.time())}"

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

        self.serialization_type = self.input.param("serialization_type",
                                                   "JSON")
        self.schema_registry_url = self.input.param("schema_registry_url")
        self.schema_registry_api_key = self.input.param(
            "schema_registry_api_key")
        self.schema_registry_secret_key = self.input.param(
            "schema_registry_secret_key")

        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.setUp.__name__)

    def tearDown(self):
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)

        delete_confluent_dlq_topic = (
            self.confluent_util.kafka_cluster_util.delete_topic_by_topic_prefix(
                self.kafka_topic_prefix))

        if hasattr(self, "cdc_connector_name"):
            confluent_cleanup_for_cdc = self.confluent_util.cleanup_kafka_resources(
                self.kafka_connect_hostname_cdc_confluent,
                [self.cdc_connector_name], self.kafka_topic_prefix + "_cdc")
        else:
            confluent_cleanup_for_cdc = True

        if hasattr(self, "non_cdc_connector_name"):
            confluent_cleanup_for_non_cdc = (
                self.confluent_util.cleanup_kafka_resources(
                    self.kafka_connect_hostname_non_cdc_confluent,
                    [self.non_cdc_connector_name],
                    self.kafka_topic_prefix + "_non_cdc",
                    self.confluent_cluster_obj.cluster_access_key))
        else:
            try:
                self.confluent_util.confluent_apis.delete_api_key(
                    self.confluent_cluster_obj.cluster_access_key)
            except Exception as err:
                self.log.error(str(err))
            confluent_cleanup_for_non_cdc = True

        mongo_collections_deleted = True
        for mongo_coll, _ in self.mongo_collections.items():
            database, collection = mongo_coll.split(".")
            mongo_collections_deleted = mongo_collections_deleted and (
                self.mongo_util.delete_mongo_collection(database, collection))

        if not all([delete_confluent_dlq_topic, confluent_cleanup_for_cdc,
                    confluent_cleanup_for_non_cdc, mongo_collections_deleted]):
            self.fail("Unable to cleanup Confluent Kafka resources or delete "
                      "mongo collections")

        super(ConfluentKafka, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished", stage="Teardown")

    def setup_infra_for_mongo(self, cdc=True, non_cdc=True):
        """
        This method will create a mongo collection, load initial data into it
        and deploy connectors for streaming both cdc and non-cdc data from
        confluent and AWS MSK kafka
        :return:
        """
        self.log.info("Creating Collections on Mongo")
        mongo_db_name = f"TAF_confluent_regression__db_{int(time.time())}"
        for i in range(1, self.input.param("num_mongo_collections") + 1):
            mongo_coll_name = f"TAF_confluent_regression__coll_{i}"

            self.log.info(f"Creating collection {mongo_db_name}."
                          f"{mongo_coll_name}")
            if not self.mongo_util.create_mongo_collection(
                    mongo_db_name, mongo_coll_name):
                self.fail(
                    f"Error while creating mongo collection {mongo_db_name}."
                    f"{mongo_coll_name}")
            self.mongo_collections[f"{mongo_db_name}.{mongo_coll_name}"] = 0

            self.log.info(f"Loading docs in mongoDb collection "
                          f"{mongo_db_name}.{mongo_coll_name}")
            mongo_doc_loading_task = self.mongo_util.load_docs_in_mongo_collection(
                database=mongo_db_name, collection=mongo_coll_name,
                start=0, end=self.initial_doc_count,
                doc_template=SiriusCodes.Templates.PRODUCT,
                doc_size=self.doc_size, sdk_batch_size=1000,
                wait_for_task_complete=True)
            if not mongo_doc_loading_task.result:
                self.fail(f"Failed to load docs in mongoDb collection "
                          f"{mongo_db_name}.{mongo_coll_name}")
            else:
                self.mongo_collections[
                    f"{mongo_db_name}.{mongo_coll_name}"] = \
                mongo_doc_loading_task.success_count

        if cdc:
            self.log.info("Generating Connector config for Mongo CDC")
            self.cdc_connector_name = f"mongo_{self.kafka_topic_prefix}_cdc"
            cdc_connector_config = KafkaConnectUtil.generate_mongo_connector_config(
                mongo_connection_str=self.mongo_util.loader.connection_string,
                mongo_collections=list(self.mongo_collections.keys()),
                topic_prefix=self.kafka_topic_prefix+"_cdc",
                partitions=32, cdc_enabled=True,
                serialization_type=self.serialization_type,
                schema_registry_url=self.schema_registry_url,
                schema_registry_access_key=self.schema_registry_api_key,
                schema_registry_secret_access_key=self.schema_registry_secret_key)

            self.log.info(
                "Deploying CDC connectors to stream data from mongo to "
                "Confluent")
            if self.confluent_util.deploy_connector(
                    self.cdc_connector_name, cdc_connector_config,
                    self.kafka_connect_hostname_cdc_confluent):
                self.confluent_cluster_obj.connectors[
                    self.cdc_connector_name] = cdc_connector_config
            else:
                self.fail("Failed to deploy connector for confluent")

        if non_cdc:
            self.log.info("Generating Connector config for Mongo Non-CDC")
            self.non_cdc_connector_name = f"mongo_{self.kafka_topic_prefix}_non_cdc"
            non_cdc_connector_config = (
                KafkaConnectUtil.generate_mongo_connector_config(
                    mongo_connection_str=self.mongo_util.loader.connection_string,
                    mongo_collections=list(self.mongo_collections.keys()),
                    topic_prefix=self.kafka_topic_prefix + "_non_cdc",
                    partitions=32, cdc_enabled=False))

            self.log.info("Deploying Non-CDC connectors to stream data from mongo "
                          "to Confluent")
            if self.confluent_util.deploy_connector(
                    self.non_cdc_connector_name, non_cdc_connector_config,
                    self.kafka_connect_hostname_non_cdc_confluent):
                self.confluent_cluster_obj.connectors[
                    self.non_cdc_connector_name] = non_cdc_connector_config
            else:
                self.fail("Failed to deploy connector for confluent")

        self.kafka_topics["confluent"]["MONGODB"] = []
        for mongo_collection_name, num_items in self.mongo_collections.items():
            for kafka_type in ["confluent"]:
                if cdc:
                    self.kafka_topics[kafka_type]["MONGODB"].append(
                        {
                            "topic_name": f"{self.kafka_topic_prefix + '_cdc'}."
                                          f"{mongo_collection_name}",
                            "key_serialization_type": self.serialization_type,
                            "value_serialization_type": self.serialization_type,
                            "cdc_enabled": True,
                            "source_connector": "DEBEZIUM",
                            "num_items": num_items
                        }
                    )

                if non_cdc:
                    self.kafka_topics[kafka_type]["MONGODB"].append(
                        {
                            "topic_name": f"{self.kafka_topic_prefix + '_non_cdc'}."
                                          f"{mongo_collection_name}",
                            "key_serialization_type": "json",
                            "value_serialization_type": "json",
                            "cdc_enabled": False,
                            "source_connector": "DEBEZIUM",
                            "num_items": num_items
                        }
                    )

    def test_create_connect_query_disconnect_confluent_kafka_links(self):
        source_db = self.input.param("source_db", "MONGODB")
        if source_db == "MONGODB":
            self.setup_infra_for_mongo()
        elif source_db == "MYSQLDB":
            self.kafka_topics["confluent"]["MYSQLDB"].append(
                {
                    "topic_name": "mysql.db.employee_data",
                    "key_serialization_type": "json",
                    "value_serialization_type": "json",
                    "cdc_enabled": True,
                    "source_connector": "DEBEZIUM",
                    "num_items": 1000000
                }
            )
        elif source_db == "POSTGRESQL":
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

        confluent_kafka_cluster_details = [
            self.confluent_util.generate_confluent_kafka_cluster_detail(
                brokers_url=self.confluent_cluster_obj.bootstrap_server,
                auth_type="PLAIN", encryption_type="TLS",
                api_key=self.confluent_cluster_obj.cluster_access_key,
                api_secret=self.confluent_cluster_obj.cluster_secret_key)]

        schema_registry_details = []
        use_schema_registry = self.input.param("use_schema_registry", False)
        if use_schema_registry:
            schema_registry_details = [
                self.confluent_util.generate_confluent_schema_registry_detail(
                    self.schema_registry_url, self.schema_registry_api_key,
                    self.schema_registry_secret_key)]

        self.columnar_spec = self.populate_columnar_infra_spec(
            columnar_spec=self.cbas_util.get_columnar_spec(
                self.columnar_spec_name),
            confluent_kafka_cluster_details=confluent_kafka_cluster_details,
            confluent_kafka_schema_registry_details=schema_registry_details,
            external_dbs=[source_db], kafka_topics=self.kafka_topics)

        if source_db == "MONGODB":
            self.columnar_spec["kafka_dataset"]["primary_key"] = [
                {"_id": "string"}]
        else:
            self.columnar_spec["kafka_dataset"]["primary_key"] = [
                {"id": "INT"}]

        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            cluster=self.columnar_cluster, cbas_spec=self.columnar_spec,
            bucket_util=self.bucket_util, wait_for_ingestion=False)
        if not result:
            self.fail(msg)

        for collection in self.cbas_util.get_all_dataset_objs("standalone"):
            result = self.cbas_util.wait_for_ingestion_complete(
                self.columnar_cluster, collection.full_name,
                collection.num_of_items, 3600)
            if not result:
                self.fail("Data ingestion did not complete for kafka dataset")

        self.log.info("Data ingestion completed successfully")

        for link in self.cbas_util.get_all_link_objs("kafka"):
            if not self.cbas_util.disconnect_link(
                    self.columnar_cluster, link.full_name):
                self.fail(f"Unable to disconnect link {link.full_name}")

    def test_creating_multiple_collections_against_single_kafka_topic(self):
        self.setup_infra_for_mongo()
        confluent_kafka_cluster_details = [
            self.confluent_util.generate_confluent_kafka_cluster_detail(
                brokers_url=self.confluent_cluster_obj.bootstrap_server,
                auth_type="PLAIN", encryption_type="TLS",
                api_key=self.confluent_cluster_obj.cluster_access_key,
                api_secret=self.confluent_cluster_obj.cluster_secret_key)]

        self.columnar_spec = self.populate_columnar_infra_spec(
            columnar_spec=self.cbas_util.get_columnar_spec(
                self.columnar_spec_name),
            confluent_kafka_cluster_details=confluent_kafka_cluster_details,
            external_dbs=["MONGODB"],
            kafka_topics=self.kafka_topics)
        self.columnar_spec["kafka_dataset"]["primary_key"] = [
            {"_id": "string"}]

        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            cluster=self.columnar_cluster, cbas_spec=self.columnar_spec,
            bucket_util=self.bucket_util, wait_for_ingestion=False)
        if not result:
            self.fail(msg)

        for collection in self.cbas_util.get_all_dataset_objs("standalone"):
            result = self.cbas_util.wait_for_ingestion_complete(
                self.columnar_cluster, collection.full_name,
                collection.num_of_items, 3600)
            if not result:
                self.fail("Data ingestion did not complete for kafka dataset")

        self.log.info("Data ingestion completed successfully")

        for link in self.cbas_util.get_all_link_objs("kafka"):
            if not self.cbas_util.disconnect_link(
                    self.columnar_cluster, link.full_name):
                self.fail(f"Unable to disconnect link {link.full_name}")

    def test_crud_operations_against_confluent_kafka_links(self):
        self.setup_infra_for_mongo(non_cdc=False)

        confluent_kafka_cluster_details = [
            self.confluent_util.generate_confluent_kafka_cluster_detail(
                brokers_url=self.confluent_cluster_obj.bootstrap_server,
                auth_type="PLAIN", encryption_type="TLS",
                api_key=self.confluent_cluster_obj.cluster_access_key,
                api_secret=self.confluent_cluster_obj.cluster_secret_key)]

        schema_registry_details = []
        use_schema_registry = self.input.param("use_schema_registry", False)
        if use_schema_registry:
            schema_registry_details = [
                self.confluent_util.generate_confluent_schema_registry_detail(
                    self.schema_registry_url, self.schema_registry_api_key,
                    self.schema_registry_secret_key)]

        self.columnar_spec = self.populate_columnar_infra_spec(
            columnar_spec=self.cbas_util.get_columnar_spec(
                self.columnar_spec_name),
            confluent_kafka_cluster_details=confluent_kafka_cluster_details,
            confluent_kafka_schema_registry_details=schema_registry_details,
            external_dbs=["MONGODB"], kafka_topics=self.kafka_topics)

        self.columnar_spec["kafka_dataset"]["primary_key"] = [
            {"_id": "string"}]

        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            cluster=self.columnar_cluster, cbas_spec=self.columnar_spec,
            bucket_util=self.bucket_util, wait_for_ingestion=False)
        if not result:
            self.fail(msg)

        for collection in self.cbas_util.get_all_dataset_objs("standalone"):
            result = self.cbas_util.wait_for_ingestion_complete(
                self.columnar_cluster, collection.full_name,
                collection.num_of_items, 3600)
            if not result:
                self.fail("Data ingestion did not complete for kafka dataset")

        self.log.info("Data ingestion completed successfully")

        for mongo_collection_name, num_items in self.mongo_collections.items():
            database_name = mongo_collection_name.split(".")[0]
            collection_name = mongo_collection_name.split(".")[1]
            self.mongo_util.perform_crud_op_on_mongo_collection(
                database=database_name, collection=collection_name,
                start=0, end=num_items,
                percentage_create=50, percentage_delete=25,
                percentage_update=25, wait_for_task_complete=True)
            self.mongo_collections[
                mongo_collection_name] = self.mongo_util.get_collection_doc_count(
                database_name, collection_name)

        for collection in self.cbas_util.get_all_dataset_objs("standalone"):
            expected_doc_count = self.mongo_collections[
                ".".join(collection.kafka_topic_name.split(".")[1:])]
            result = self.cbas_util.wait_for_ingestion_complete(
                self.columnar_cluster, collection.full_name,
                expected_doc_count, 3600)
            if not result:
                self.fail("Data ingestion did not complete for kafka dataset")

        for link in self.cbas_util.get_all_link_objs("kafka"):
            if not self.cbas_util.disconnect_link(
                    self.columnar_cluster, link.full_name):
                self.fail(f"Unable to disconnect link {link.full_name}")