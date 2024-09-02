import random
import string
import time
from queue import Queue
from Columnar.columnar_base import ColumnarBaseTest
from capellaAPI.capella.dedicated.CapellaAPI_v4 import CapellaAPI
from couchbase_utils.kafka_util.common_utils import KafkaCluster
from couchbase_utils.kafka_util.kafka_connect_util import KafkaConnectUtil
from couchbase_utils.kafka_util.confluent_utils import ConfluentCloudAPIs, KafkaClusterConfigConstants, ConfluentUtils
from couchbase_utils.kafka_util.msk_utils import MSKUtils
from CbasLib.cbas_entity_columnar import KafkaClusterDetails


class AWSKafka(ColumnarBaseTest):
    def setUp(self):
        super(AWSKafka, self).setUp()
        self.cluster = self.tenant.columnar_instances[0]

        self.kafka_cluster_obj = KafkaClusterDetails()

        if not self.columnar_spec_name:
            self.columnar_spec_name = "sanity.S3_external_datasets"

        self.columnar_spec = self.cbas_util.get_columnar_spec(self.columnar_spec_name)

        self.columnar_spec["database"]["no_of_databases"] = self.input.param("no_of_databases", 1)
        self.columnar_spec["dataverse"]["no_of_dataverses"] = self.input.param(
            "no_of_dataverses", 1)

        self.connect_cluster_hostname = self.input.param("connect_cluster_hostname",
            "http://54.92.231.154:8084")
        self.topic_prefix = self.generate_random_entity_name(type="topic_prefix")

         # mongodb params
        self.mongo_username = self.input.param("mongo_user", "Administrator")
        self.mongo_password = self.input.param("mongo_password", "password")
        self.mongo_connection_string = self.input.param("mongo_connection_string",
            "mongodb://Administrator:password@mongo.cbqeoc.com:27017/"
            "?retryWrites=true&w=majority&replicaSet=rs0")
        self.mongo_database = self.input.param("mongo_database", "functional_testing")
        self.mongo_collection = self.input.param("mongo_collection", "functional_testing_static")
        self.mongo_collections = self.mongo_database + "." + self.mongo_collection
        self.doc_count = self.input.param("doc_count", "100000")

        self.topic_name = self.topic_prefix + "." + self.mongo_collections
        self.reuse_topic = self.input.param("reuse_topic", True)

        self.cloud_access_key = self.input.param("cloud_access_key")
        self.cloud_secret_key = self.input.param("cloud_secret_key")
        self.sasl_username = self.input.param("sasl_username")
        self.sasl_password = self.input.param("sasl_password")

        # making sure connector names are unique as test will fail incase connector already exists
        self.connector_name = self.generate_random_entity_name(type="connector")

        self.msk_utils = MSKUtils(self.connect_cluster_hostname, self.aws_access_key,
                                  self.aws_secret_key, self.aws_region)
        self.aws_kafka_obj = self.msk_utils.generate_msk_cluster_object("QE-Kafka-test-cluster", self.topic_prefix,
                                                                        self.sasl_username, self.sasl_password)

        if not self.reuse_topic:
            self.kafka_connect_utils = KafkaConnectUtil(self.connect_cluster_hostname)
            connector_config = self.kafka_connect_utils.generate_mongo_connector_config(
                self.mongo_connection_str, self.mongo_collections, self.topic_prefix)

            self.kafka_connect_utils.deploy_connector(self.connector_name, connector_config)
            self.aws_kafka_obj.connectors[self.connector_name] = connector_config
        if self.reuse_topic:
            self.topic_name = "aws_msk_mongo.functional_testing.functional_testing_static"

        self.kafka_link_name = self.input.param("kafka_link_name", "kafka_link")
        self.authentication_type = self.input.param("authentication_type", "SCRAM_SHA_512")
        self.collection_name = self.input.param("collection_name", "aws_kafka_collection")
        self.serialization_type = self.input.param("serialization_type", "JSON")
        self.use_schema_registry = self.input.param("use_schema_registry", False)

        self.doc_count_for_mongo_collections = {
            "functional_testing.functional_testing_static": 1000000
        }

        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.setUp.__name__)

    def tearDown(self):
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)

        self.msk_utils.cleanup_kafka_resources(self.aws_kafka_obj)

        super(AWSKafka, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished", stage="Teardown")

    def generate_random_entity_name(self, length=5, type="database"):
        """Generate random database name."""
        base_name = "TAF-" + type
        entity_id = ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(length))
        entity_name = base_name + "-" + entity_id
        return entity_name

    def populate_kafka_connector_details(self):
        kafka_connector_details = {
            "kafka-sink": "true",
            "keySerializationType": self.serialization_type,
            "valueSerializationType": self.serialization_type,
            "cdcEnabled":"true",
            "cdcDetails": {
                "cdcSource":"MONGODB",
                "cdcSourceConnector":"DEBEZIUM"
            }
        }
        return kafka_connector_details

    def test_create_connect_disconnect_query_drop_aws_kafka_links(self):
        aws_kafka_cluster_details = self.kafka_cluster_obj.generate_aws_kafka_cluster_detail(
            brokers_url="b-1-public.qekafkatestcluster.7b9vtv.c13.kafka.us-east-1.amazonaws.com:9196,"
                        "b-2-public.qekafkatestcluster.7b9vtv.c13.kafka.us-east-1.amazonaws.com:9196,"
                        "b-3-public.qekafkatestcluster.7b9vtv.c13.kafka.us-east-1.amazonaws.com:9196",
            auth_type=self.authentication_type,
            encryption_type="TLS", username=self.sasl_username, password=self.sasl_password
        )
        schema_registry_details = None
        if self.use_schema_registry:
            schema_registry_details = self.kafka_cluster_obj.generate_aws_kafka_schema_registry_detail(
                self.aws_region, self.aws_access_key, self.aws_secret_key
            )
        kafka_connector_details = self.populate_kafka_connector_details()
        self.cbas_util.create_kafka_link(self.cluster, self.kafka_link_name, aws_kafka_cluster_details,
                                         schema_registry_details)
        self.log.info(f"Created aws kafka link - {self.kafka_link_name}")

        self.cbas_util.create_standalone_collection_using_links(cluster=self.cluster,
            collection_name= self.collection_name, primary_key={"_id": "String"},
            link_name=self.kafka_link_name, external_collection=self.topic_name,
            kafka_connector_details=kafka_connector_details)
        self.log.info(f"Created dataset - {self.collection_name} on aws kafka link")

        self.cbas_util.connect_link(self.cluster, self.kafka_link_name)
        self.log.info(f"Successfully connected aws kafka link - {self.kafka_link_name}")

        result = self.cbas_util.wait_for_ingestion_complete(self.cluster, self.collection_name,
        self.doc_count_for_mongo_collections[self.mongo_collections], 3600)
        if not result:
            self.fail("Data ingestion did not complete for kafka dataset")
        self.log.info("Data ingestion completed successfully")

        self.cbas_util.disconnect_link(self.cluster, self.kafka_link_name)
        self.log.info(f"Successfully disconnected aws kafka link - {self.kafka_link_name}")
