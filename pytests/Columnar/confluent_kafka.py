import random
import string
import time
from queue import Queue
from Columnar.columnar_base import ColumnarBaseTest
from capellaAPI.capella.dedicated.CapellaAPI_v4 import CapellaAPI
from couchbase_utils.kafka_util.common_utils import KafkaCluster
from couchbase_utils.kafka_util.kafka_connect_util import KafkaConnectUtil
from couchbase_utils.kafka_util.confluent_utils import ConfluentCloudAPIs, KafkaClusterConfigConstants, ConfluentUtils
from CbasLib.cbas_entity_columnar import KafkaClusterDetails
from sirius_client_framework.multiple_database_config import MongoLoader
from sirius_client_framework.operation_config import WorkloadOperationConfig
from Jython_tasks.sirius_task import WorkLoadTask
from sirius_client_framework.sirius_constants import SiriusCodes
from capella_utils.columnar_final import ColumnarRBACUtil


class ConfluentKafka(ColumnarBaseTest):
    def setUp(self):
        super(ConfluentKafka, self).setUp()
        self.cluster = self.tenant.columnar_instances[0]

        self.kafka_cluster_obj = KafkaClusterDetails()
        self.columnar_rbac_util = ColumnarRBACUtil(self.log)

        if not self.columnar_spec_name:
            self.columnar_spec_name = "sanity.S3_external_datasets"

        self.columnar_spec = self.cbas_util.get_columnar_spec(self.columnar_spec_name)

        self.columnar_spec["database"]["no_of_databases"] = self.input.param("no_of_databases", 1)
        self.columnar_spec["dataverse"]["no_of_dataverses"] = self.input.param(
            "no_of_dataverses", 1)

        self.connect_cluster_hostname = self.input.param("connect_cluster_hostname",
            "http://54.92.231.154:8083")
        self.topic_prefix = self.input.param("topic_prefix", "test")

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

        # making sure connector names are unique as test will fail incase connector already exists
        self.connector_name = self.generate_random_entity_name(type="connector")

        self.confluent_apis =  ConfluentCloudAPIs(self.cloud_access_key, self.cloud_secret_key)
        env_details = self.confluent_apis.list_all_environments()[0]
        kafka_cluster_details = self.confluent_apis.list_all_kafka_clusters(env_details["id"])[0]

        # create cloud API keys
        user = self.confluent_apis.list_all_users()[0]
        response = self.confluent_apis.list_all_api_keys()[0]

        self.confluent_utils = ConfluentUtils(self.cloud_access_key, self.cloud_secret_key,
                                              self.connect_cluster_hostname)
        self.kafka_obj = self.confluent_utils.generate_confluent_kafka_object(kafka_cluster_details["id"],
                                                                              self.topic_prefix)

        if not self.reuse_topic:
            self.kafka_connect_utils = KafkaConnectUtil(self.connect_cluster_hostname)
            connector_config = self.kafka_connect_utils.generate_mongo_connector_config(
                self.mongo_connection_string, [self.mongo_collections], self.topic_prefix
            )
            self.kafka_connect_utils.deploy_connector(self.connector_name, connector_config)
            self.kafka_obj.connectors[self.connector_name] = connector_config
        if self.reuse_topic:
            self.topic_name = "confluent_mongo.functional_testing.functional_testing_static"

        self.kafka_link_name = self.input.param("kafka_link_name", "kafka_link")
        self.authentication_type = self.input.param("authentication_type", "PLAIN")
        self.collection_name = self.input.param("collection_name", "confluent_kafka_collection")
        self.serialization_type = self.input.param("serialization_type", "JSON")
        self.use_schema_registry = self.input.param("use_schema_registry", False)
        if self.use_schema_registry:
            self.schema_registry_api_key = self.input.param("schema_registry_api_key")
            self.schema_registry_secret_key = self.input.param("schema_registry_secret_key")

        self.doc_count_for_mongo_collections = {
            "functional_testing.functional_testing_static": 1000000
        }

        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.setUp.__name__)

    def tearDown(self):
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)

        self.confluent_utils.cleanup_kafka_resources(self.kafka_obj)

        super(ConfluentKafka, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished", stage="Teardown")

    def perform_crud_op_on_mongo_collection(self, op_type, start, end):
        database_information = MongoLoader(
            username=self.mongo_username, password=self.mongo_password,
            connection_string=self.mongo_connection_string,
            collection=self.mongo_collection, database=self.mongo_database,
        )
        operation_config = WorkloadOperationConfig(
            start=start, end=end,
            template=SiriusCodes.Templates.PERSON,
            doc_size=self.doc_size,
        )
        task = WorkLoadTask(
            task_manager=self.task_manager, op_type=op_type,
            database_information=database_information,
            operation_config=operation_config,
        )
        self.task_manager.add_new_task(task)
        self.task_manager.get_task_result(task)
        return task

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

    def generate_random_password(self, length=12):
        """Generate a random password."""
        password_characters = string.ascii_letters + string.digits
        password = ''.join(random.choice(password_characters) for i in range(length))
        password += "!123"
        return password

    def generate_random_entity_name(self, length=5, type="database"):
        """Generate random database name."""
        base_name = "TAF-" + type
        entity_id = ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(length))
        entity_name = base_name + "-" + entity_id
        return entity_name

    def create_rbac_user(self, privileges=[], resources=[],
                         resource_type="instance"):
        if len(resources) == 0:
            resources.append("")

        username = self.generate_random_entity_name(type="user")
        password = self.generate_random_password()
        resource_priv_map = []
        for res in resources:
            res_priv_obj = {
                "name": res,
                "type": resource_type,
                "privileges": privileges
            }
            resource_priv_map.append(res_priv_obj)

        privileges_payload = self.columnar_rbac_util.create_privileges_payload(resource_priv_map)

        user1 = self.columnar_rbac_util.create_api_keys(self.pod, self.tenant,
                                                        self.tenant.project_id, self.cluster,
                                                        username=username,
                                                        password=password,
                                                        privileges_payload=privileges_payload)

        return user1

    def test_kafka_rbac(self):
        rbac_user = self.create_rbac_user(["link_create", "link_create_collection"], [],
                                          resource_type="instance")
        #Test kafka link and colleciton creation with rbac
        kafka_cluster_details = self.kafka_cluster_obj.generate_confluent_kafka_cluster_detail(
            brokers_url="pkc-p11xm.us-east-1.aws.confluent.cloud:9092", auth_type=self.authentication_type,
            encryption_type="TLS", api_key=self.kafka_obj.cluster_access_key,
            api_secret=self.kafka_obj.cluster_secret_key
        )
        kafka_connector_details = self.populate_kafka_connector_details()

        schema_registry_details = None
        if self.use_schema_registry:
            schema_registry_details = self.kafka_cluster_obj.generate_confluent_schema_registry_detail(
                schema_registry_url="https://psrc-6kq702.us-east-1.aws.confluent.cloud",
                api_key=self.schema_registry_api_key, api_secret=self.schema_registry_secret_key
            )

        self.cbas_util.create_kafka_link(self.cluster, self.kafka_link_name, kafka_cluster_details,
                                         schema_registry_details, username=rbac_user.username,
                                         password=rbac_user.password)

        self.cbas_util.create_standalone_collection_using_links(cluster=self.cluster,
            collection_name= self.collection_name, primary_key={"_id": "String"},
            link_name=self.kafka_link_name, external_collection=self.topic_name,
            kafka_connector_details= kafka_connector_details,
            username=rbac_user.username,
            password=rbac_user.password)

        self.cbas_util.connect_link(self.cluster, self.kafka_link_name)

        result = self.cbas_util.wait_for_ingestion_complete(self.cluster, self.collection_name,
        self.doc_count_for_mongo_collections[self.mongo_collections], 3600)
        if not result:
            self.fail("Data ingestion did not complete for kafka dataset")

        self.cbas_util.disconnect_link(self.cluster, self.kafka_link_name)

    def test_create_connect_disconnect_query_drop_confluent_kafka_links(self):
        kafka_cluster_details = self.kafka_cluster_obj.generate_confluent_kafka_cluster_detail(
            brokers_url="pkc-p11xm.us-east-1.aws.confluent.cloud:9092", auth_type=self.authentication_type,
            encryption_type="TLS", api_key=self.kafka_obj.cluster_access_key,
            api_secret=self.kafka_obj.cluster_secret_key
        )
        kafka_connector_details = self.populate_kafka_connector_details()

        self.cbas_util.create_kafka_link(self.cluster, self.kafka_link_name, kafka_cluster_details)
        self.log.info(f"Created confluent kafka link - {self.kafka_link_name}")

        self.cbas_util.create_standalone_collection_using_links(cluster=self.cluster,
            collection_name= self.collection_name, primary_key={"_id": "String"},
            link_name=self.kafka_link_name, external_collection=self.topic_name,
            kafka_connector_details= kafka_connector_details)
        self.log.info(f"Created dataset - {self.collection_name} on confluent kafka link")

        self.cbas_util.connect_link(self.cluster, self.kafka_link_name)
        self.log.info(f"Successfully connected confluent kafka link - {self.kafka_link_name}")

        result = self.cbas_util.wait_for_ingestion_complete(self.cluster, self.collection_name,
        self.doc_count_for_mongo_collections[self.mongo_collections], 3600)
        if not result:
            self.fail("Data ingestion did not complete for kafka dataset")
        self.log.info("Data ingestion completed successfully")

        self.cbas_util.disconnect_link(self.cluster, self.kafka_link_name)
        self.log.info(f"Successfully disconnected confluent kafka link - {self.kafka_link_name}")

    def test_crud_operations_against_confluent_kafka_links(self):
        kafka_cluster_details = self.kafka_cluster_obj.generate_confluent_kafka_cluster_detail(
            brokers_url="pkc-p11xm.us-east-1.aws.confluent.cloud:9092", auth_type=self.authentication_type,
            encryption_type="TLS", api_key=self.kafka_obj.cluster_access_key,
            api_secret=self.kafka_obj.cluster_secret_key
        )
        kafka_connector_details = self.populate_kafka_connector_details()

        self.cbas_util.create_kafka_link(self.cluster, self.kafka_link_name, kafka_cluster_details)
        self.log.info(f"Created confluent kafka link - {self.kafka_link_name}")

        self.cbas_util.create_standalone_collection_using_links(cluster=self.cluster,
            collection_name= self.collection_name, primary_key={"_id": "String"},
            link_name=self.kafka_link_name, external_collection=self.topic_name,
            kafka_connector_details= kafka_connector_details)
        self.log.info(f"Created dataset - {self.collection_name} on confluent kafka link")

        self.cbas_util.connect_link(self.cluster, self.kafka_link_name)
        self.log.info(f"Successfully connected confluent kafka link - {self.kafka_link_name}")

        self.perform_crud_op_on_mongo_collection(SiriusCodes.DocOps.BULK_CREATE, 0, self.doc_count)
        result = self.cbas_util.wait_for_ingestion_complete(self.cluster, self.collection_name,
        self.doc_count, 3600)
        if not result:
            self.fail("Data ingestion did not complete for kafka dataset")

        self.perform_crud_op_on_mongo_collection(SiriusCodes.DocOps.BULK_DELETE, 0, self.doc_count)
        result = self.cbas_util.wait_for_ingestion_complete(self.cluster, self.collection_name,
        0, 3600)
        if not result:
            self.fail("Data ingestion did not complete for kafka dataset")
        self.log.info("Data ingestion completed successfully")

        self.cbas_util.disconnect_link(self.cluster, self.kafka_link_name)
        self.log.info(f"Successfully disconnected confluent kafka link - {self.kafka_link_name}")

    def test_creating_multiple_collections_against_single_kafka_topic(self):
        kafka_cluster_details = self.kafka_cluster_obj.generate_confluent_kafka_cluster_detail(
            brokers_url="pkc-p11xm.us-east-1.aws.confluent.cloud:9092", auth_type=self.authentication_type,
            encryption_type="TLS", api_key=self.kafka_obj.cluster_access_key,
            api_secret=self.kafka_obj.cluster_secret_key
        )
        kafka_connector_details = self.populate_kafka_connector_details()

        self.cbas_util.create_kafka_link(self.cluster, self.kafka_link_name, kafka_cluster_details)
        self.log.info(f"Created confluent kafka link - {self.kafka_link_name}")

        self.cbas_util.create_standalone_collection_using_links(cluster=self.cluster,
            collection_name= self.collection_name, primary_key={"_id": "String"},
            link_name=self.kafka_link_name, external_collection=self.topic_name,
            kafka_connector_details= kafka_connector_details)
        self.log.info(f"Created dataset - {self.collection_name} on confluent kafka link")

        second_col = "kafka_collection2"
        self.cbas_util.create_standalone_collection_using_links(cluster=self.cluster,
            collection_name= second_col, primary_key={"_id": "String"},
            link_name=self.kafka_link_name, external_collection=self.topic_name,
            kafka_connector_details= kafka_connector_details)
        self.log.info(f"Created dataset - {self.collection_name} on confluent kafka link")

        self.cbas_util.connect_link(self.cluster, self.kafka_link_name)
        self.log.info(f"Successfully connected confluent kafka link - {self.kafka_link_name}")

        result = self.cbas_util.wait_for_ingestion_all_datasets(self.cluster, 3600)
        if not result:
            self.fail("Data ingestion did not complete for all datasets")
        self.log.info("Data ingestion completed successfully")
        
        self.cbas_util.disconnect_link(self.cluster, self.kafka_link_name)
        self.log.info(f"Successfully disconnected confluent kafka link - {self.kafka_link_name}")
