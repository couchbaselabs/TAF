import time

from Columnar.columnar_base import ColumnarBaseTest
from couchbase_utils.kafka_util.kafka_connect_util import KafkaConnectUtil
from couchbase_utils.kafka_util.msk_utils import MSKUtils

from Jython_tasks.sirius_task import MongoUtil


class AWSKafka(ColumnarBaseTest):
    def setUp(self):
        super(AWSKafka, self).setUp()
        self.columnar_cluster = self.tenant.columnar_instances[0]

        if not self.columnar_spec_name:
            self.columnar_spec_name = "full_template"

        # Initialize sirius doc loader utils
        self.mongo_util = MongoUtil(
            task_manager=self.task_manager,
            hostname=self.input.param("mongo_hostname"),
            username=self.input.param("mongo_username"),
            password=self.input.param("mongo_password")
        )

        # Initializing AWS_MSK util and AWS_MSK cluster object.
        # Initialize variables for Kafka
        self.kafka_topic_prefix = f"aws_msk_regression_{int(time.time())}"
        self.msk_util = MSKUtils(
            access_key=self.aws_access_key, secret_key=self.aws_secret_key,
            region=self.input.param("msk_region", "us-east-1"))
        self.msk_cluster_obj = self.msk_util.generate_msk_cluster_object(
            msk_cluster_name=self.input.param("msk_cluster_name"),
            topic_prefix=self.kafka_topic_prefix,
            sasl_username=self.input.param("msk_username"),
            sasl_password=self.input.param("msk_password"))
        if not self.msk_cluster_obj:
            self.fail("Unable to initialize AWS Kafka cluster object")

        # Initializing KafkaConnect Util and kafka connect server hostnames
        self.kafka_connect_util = KafkaConnectUtil()
        kafka_connect_hostname = self.input.param('kafka_connect_hostname')
        self.kafka_connect_hostname_cdc_msk = (
            f"{kafka_connect_hostname}:{KafkaConnectUtil.AWS_MSK_CDC_PORT}")
        self.kafka_connect_hostname_non_cdc_msk = (
            f"{kafka_connect_hostname}:{KafkaConnectUtil.AWS_MSK_NON_CDC_PORT}")

        self.kafka_topics = {
            "aws_kafka": {
                "MONGODB": [
                    {
                        "topic_name": "do-not-delete-mongo-cdc.Product_Template.10GB",
                        "key_serialization_type": "json",
                        "value_serialization_type": "json",
                        "cdc_enabled": True,
                        "source_connector": "DEBEZIUM",
                        "num_items": 10000000
                    },
                    {
                        "topic_name": "do-not-delete-mongo-non-cdc.Product_Template.10GB",
                        "key_serialization_type": "json",
                        "value_serialization_type": "json",
                        "cdc_enabled": False,
                        "source_connector": "DEBEZIUM",
                        "num_items": 10000000
                    },
                ],
                "POSTGRESQL": [],
                "MYSQLDB": []}}

        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.setUp.__name__)

    def tearDown(self):
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)

        delete_msk_dlq_topic = self.msk_util.kafka_cluster_util.delete_topic_by_topic_prefix(
            self.kafka_topic_prefix)

        """msk_cleanup_for_cdc = self.msk_util.cleanup_kafka_resources(
            self.kafka_connect_hostname_cdc_msk, [self.cdc_connector_name],
            self.kafka_topic_prefix + "_cdc"
        )

        msk_cleanup_for_non_cdc = self.msk_util.cleanup_kafka_resources(
            self.kafka_connect_hostname_non_cdc_msk,
            [self.non_cdc_connector_name],
            self.kafka_topic_prefix + "_non_cdc"
        )

        mongo_collections_deleted = True
        for mongo_coll, _ in self.mongo_collections.items():
            database, collection = mongo_coll.split(".")
            mongo_collections_deleted = mongo_collections_deleted and (
                self.mongo_util.delete_mongo_collection(database, collection))

        if not all([msk_cleanup_for_cdc, msk_cleanup_for_non_cdc,
                    delete_msk_dlq_topic, mongo_collections_deleted]):
            self.fail(f"Unable to either cleanup AWS Kafka resources or "
                      f"Confluent Kafka resources or delete mongo collections")"""

        super(AWSKafka, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished", stage="Teardown")

    def test_create_connect_disconnect_query_drop_aws_kafka_links(self):
        aws_kafka_cluster_details = [
            self.msk_util.generate_aws_kafka_cluster_detail(
                brokers_url=self.msk_cluster_obj.bootstrap_brokers[
                    "PublicSaslScram"],
                auth_type="SCRAM_SHA_512", encryption_type="TLS",
                username=self.msk_cluster_obj.sasl_username,
                password=self.msk_cluster_obj.sasl_password)]

        schema_registry_details = []

        use_schema_registry = self.input.param("use_schema_registry", False)

        if use_schema_registry:
            schema_registry_details = [self.msk_util.generate_aws_kafka_schema_registry_detail(
                self.aws_region, self.aws_access_key, self.aws_secret_key)]

        self.columnar_spec = self.populate_columnar_infra_spec(
            columnar_spec=self.cbas_util.get_columnar_spec(
                self.columnar_spec_name),
            aws_kafka_cluster_details=aws_kafka_cluster_details,
            aws_kafka_schema_registry_details=schema_registry_details,
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
