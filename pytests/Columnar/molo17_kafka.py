import time
from random import randint

from CbasLib.cbas_entity_columnar import KafkaClusterDetails
from Columnar.columnar_base import ColumnarBaseTest
from Jython_tasks.sirius_task import WorkLoadTask
from awsLib.S3 import DynamoDB
from kafka_util.molo17_utils import Molo17APIs
from couchbase_utils.kafka_util.confluent_utils import ConfluentUtils
from sirius_client_framework.multiple_database_config import DynamoDBLoader
from sirius_client_framework.operation_config import WorkloadOperationConfig
from sirius_client_framework.sirius_constants import SiriusCodes
import Columnar.templates.molo17.multiple_key as multi_key_template
import Columnar.templates.molo17.single_key as single_key_template


class Molo17(ColumnarBaseTest):
    def __init__(self, methodName: str = "runTest"):
        super().__init__(methodName)
        self.tenant = None

    def setUp(self):
        super(Molo17, self).setUp()
        self.columnar_cluster = self.tenant.columnar_instances[0]

        self.kafka_cluster_obj = KafkaClusterDetails()

        # glueSync url
        self.glueSync_url = self.input.param("glueSync_url", "http://34.231.138.129:1717")
        self.glueSync_username = self.input.param("glueSync_username", "admin")
        self.glueSync_password = self.input.param("glueSync_password", "Couchbase@123")

        # dynamo util,  glueSync util, confluent util
        self.dynamo_lib = DynamoDB(self.aws_access_key, self.aws_secret_key, self.aws_region, self.aws_session_token)
        self.glueSync_lib = Molo17APIs(self.glueSync_url, self.glueSync_username, self.glueSync_password)
        self.confluent_util = ConfluentUtils(
            cloud_access_key=self.input.param("confluent_cloud_access_key"),
            cloud_secret_key=self.input.param("confluent_cloud_secret_key"))
        self.initial_doc_count = self.input.param("initial_doc_count", 100)
        if not self.columnar_spec_name:
            self.columnar_spec_name = "full_template"


    def tearDown(self):
        if hasattr(self, "dynamo_table"):
            self.dynamo_lib.delete_dynamo_table(self.dynamo_table)
        if hasattr(self, "glueSync_pipeline_id"):
            self.glueSync_lib.delete_pipelines(self.glueSync_pipeline_id)
        if hasattr(self, "kafka_topic_prefix"):
            kafka_topics = [self.kafka_topic_prefix, self.kafka_topic_prefix + ".dead_letter_queue"]
            self.confluent_util.kafka_cluster_util.delete_topics(kafka_topics)
        if hasattr(self, "confluent_cluster_obj"):
            self.confluent_util.cleanup_kafka_resources(None, [], self.kafka_topic_prefix, cluster_access_key=self.confluent_cluster_obj.cluster_access_key)
        super(Molo17, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage="Teardown")

    def load_doc_to_dynamo_table(self, table_name, start, end, template="product", action="create",
                                 sirius_url="http://127.0.0.1:4000", field_to_change=["product_link", "product_category"]):
        database_information = DynamoDBLoader(self.aws_access_key, self.aws_secret_key, self.aws_region, table_name)
        op_type = SiriusCodes.DocOps.CREATE
        if action == "delete":
            op_type = SiriusCodes.DocOps.DELETE
        if action == "upsert":
            op_type = SiriusCodes.DocOps.UPDATE
            operation_config = WorkloadOperationConfig(start=int(start), end=int(end), template=template,
                                                       doc_size=self.doc_size, fields_to_change=field_to_change)
        else:
            operation_config = WorkloadOperationConfig(start=int(start), end=int(end), template=template,
                                                    doc_size=self.doc_size)

        task = WorkLoadTask(task_manager=self.task, op_type=op_type,
                            database_information=database_information, operation_config=operation_config,
                            default_sirius_base_url=sirius_url, )
        self.task_manager.add_new_task(task)
        result = self.task_manager.get_task_result(task)
        return result

    def create_glueSync_connector(self, create_pipelines=True, sync_type="cdc", cdc=True):
        if create_pipelines:
            self.glueSync_pipeline_id = self.glueSync_lib.create_pipeline(self.kafka_topic_prefix, "")
            available_agents = self.glueSync_lib.get_unassigned_agents()
            if len(available_agents) < 2:
                self.fail("Not enough agents available to deploy connector")
            dynamo_agent = [agents for agents in available_agents if agents['agentTag'] == "dynamodb"][0]
            kafka_agent = [agents for agents in available_agents if agents['agentTag'] == "kafka"][0]

            if not self.glueSync_lib.assign_agent_to_pipeline(self.glueSync_pipeline_id, dynamo_agent['id']): self.fail("Failed to assign dynamo agent")
            if not self.glueSync_lib.assign_agent_to_pipeline(self.glueSync_pipeline_id, kafka_agent['id']): self.fail("Failed to assign kafka agent")

            self.glueSync_lib.configure_agent_credentials(self.glueSync_pipeline_id, dynamo_agent['id'], host=self.aws_region,
                                                          username=self.aws_access_key, password=self.aws_secret_key)
            kafka_server = self.confluent_cluster_obj.bootstrap_server.split(':')
            self.glueSync_lib.configure_agent_credentials(self.glueSync_pipeline_id, kafka_agent['id'],
                                                          host=kafka_server[0], port=kafka_server[1],
                                                          username=self.confluent_cluster_obj.cluster_access_key,
                                                          password=self.confluent_cluster_obj.cluster_secret_key,
                                                          trust_server_certificate=False, custom_key="saslMechanism",
                                                          custom_key_value="PLAIN", custom_value="securityProtocol",
                                                          custom_value_value="SASL_SSL")

            entity_name = self.kafka_topic_prefix
            entity_source = single_key_template.entity_source
            entity_target = single_key_template.entity_target
            if self.input.param("key_type", "single") == "multiple":
                entity_source = multi_key_template.entity_source
                entity_target = multi_key_template.entity_target

            entity_source['entityName'] = entity_name
            entity_source['agentId'] = dynamo_agent['id']
            entity_source['table']['name'] = self.dynamo_table
            entity_source['table']['schema'] = self.aws_region

            entity_target['entityName'] = entity_name
            entity_target['agentId'] = kafka_agent['id']
            entity_target['entityObject']['collection'] = self.kafka_topic_prefix

            self.glueSync_lib.upsert_entities_of_pipeline_agent(self.glueSync_pipeline_id, entity_name, entity_source,
                                                                entity_target)
            self.glueSync_lib.complete_pipeline_configuration(self.glueSync_pipeline_id, self.kafka_topic_prefix, "")
            entities = self.glueSync_lib.get_all_entities(self.glueSync_pipeline_id)
            if len(entities) > 1:
                self.fail("More than one entity currently configured")
            self.entity_id = entities[0]['entity']['entityId']
        if sync_type == 'one-time-snapshot':
            self.glueSync_lib.sync_one_time_snapshot(self.glueSync_pipeline_id, self.entity_id)
        elif sync_type == "redo":
            self.glueSync_lib.sync_redo(self.glueSync_pipeline_id, self.entity_id, cdc=cdc)
        else:
            self.glueSync_lib.sync_start(self.glueSync_pipeline_id, self.entity_id)


    def create_dynamo_table_load(self):
        # create dynamo table, enable streams and load data
        self.dynamo_table = self.dynamo_lib.create_table("product_name", 'S')
        self.log.info("Sleeping for 30 seconds for dynamo table to exist")
        time.sleep(30)
        self.dynamo_lib.enable_dynamodb_streams(self.dynamo_table)
        self.load_doc_to_dynamo_table(self.dynamo_table, 0, self.initial_doc_count)
        self.initial_doc_count = self.dynamo_lib.get_item_count(self.dynamo_table)

    def create_kafka_objects(self, cdc=True):
        self.kafka_topic_prefix = f"glueSync_{int(time.time())}"
        self.confluent_cluster_obj = self.confluent_util.generate_confluent_kafka_object(
            kafka_cluster_id=self.input.param("confluent_cluster_id"),
            topic_prefix=self.kafka_topic_prefix)
        self.confluent_kafka_cluster_details = [
            self.confluent_util.generate_confluent_kafka_cluster_detail(
                brokers_url=self.confluent_cluster_obj.bootstrap_server,
                auth_type="PLAIN", encryption_type="TLS",
                api_key=self.confluent_cluster_obj.cluster_access_key,
                api_secret=self.confluent_cluster_obj.cluster_secret_key)]
        if not self.confluent_util.kafka_cluster_util.create_topic(self.kafka_topic_prefix,
                                                                   {"cleanup.policy": "compact"},
                                                                   partitions_count=8, replication_factor=3):
            self.fail("Failed to create topic in Kafka")
        self.kafka_topics = {
            "confluent": {
                "DYNAMODB": [
                    {
                        "topic_name": self.kafka_topic_prefix,
                        "key_serialization_type": "json",
                        "value_serialization_type": "json",
                        "cdc_enabled": cdc,
                        "source_connector": "GLUESYNC",
                        "num_items": self.initial_doc_count
                    },
                ],
                "POSTGRESQL": [],
                "MYSQLDB": [],
                "MONGODB": [],
            },
        }


    def test_glueSync_initial_snapshot(self):
        """
        Steps:
            1. Create a dynamo table
            2. Load data to dynamo table
            3. Create a kafka empty topic
            4. Create glueSync configurations
            5. Start sync with initial snapshot
            6. Verify the result using number of items and queries
            7. Load more doc to dynamo table
            8. Re-Sync the connector
            9. Verify the result using number of items and queries
        """
        # create and prepare dynamo table for connector
        self.create_dynamo_table_load()

        # configure kafka objects and create topics
        self.create_kafka_objects(cdc=False)

        # create glueSync connector and start stream
        self.create_glueSync_connector(sync_type="one-time-snapshot", cdc=False)

        # create columnar kafka links and datasets on dynamo
        self.columnar_spec = self.populate_columnar_infra_spec(
            columnar_spec=self.cbas_util.get_columnar_spec(
                self.columnar_spec_name),
            confluent_kafka_cluster_details=self.confluent_kafka_cluster_details,
            external_dbs=["DYNAMODB"],
            kafka_topics=self.kafka_topics)

        self.columnar_spec["kafka_dataset"]["primary_key"] = [
            {"product_name": "string"}]

        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            cluster=self.columnar_cluster, cbas_spec=self.columnar_spec,
            bucket_util=self.bucket_util, wait_for_ingestion=True)

        if not result:
            self.fail(msg)

        # validate results in the columnar datasets
        datasets = self.cbas_util.get_all_dataset_objs('standalone')
        for dataset in datasets:
            self.cbas_util.wait_for_ingestion_complete(self.columnar_cluster, dataset.full_name, self.initial_doc_count)
            item_count = self.cbas_util.get_num_items_in_cbas_dataset(self.columnar_cluster, dataset.full_name)
            if item_count != self.initial_doc_count:
                self.fail("Data count mismatch in Dynamo and Kafka datasets")

    def test_glueSync_cdc(self):
        # create and prepare dynamo table for connector
        self.create_dynamo_table_load()

        # configure kafka objects and create topics
        self.create_kafka_objects()

        # create glueSync connector and start stream
        self.create_glueSync_connector()

        # create columnar kafka links and datasets on dynamo
        self.columnar_spec = self.populate_columnar_infra_spec(
            columnar_spec=self.cbas_util.get_columnar_spec(
                self.columnar_spec_name),
            confluent_kafka_cluster_details=self.confluent_kafka_cluster_details,
            external_dbs=["DYNAMODB"],
            kafka_topics=self.kafka_topics)

        self.columnar_spec["kafka_dataset"]["primary_key"] = [
            {"product_name": "string"}]

        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            cluster=self.columnar_cluster, cbas_spec=self.columnar_spec,
            bucket_util=self.bucket_util, wait_for_ingestion=True)

        if not result:
            self.fail(msg)

        # validate results in the columnar datasets initial count
        datasets = self.cbas_util.get_all_dataset_objs('standalone')
        for dataset in datasets:
            self.cbas_util.wait_for_ingestion_complete(self.columnar_cluster, dataset.full_name, self.initial_doc_count)
            item_count = self.cbas_util.get_num_items_in_cbas_dataset(self.columnar_cluster, dataset.full_name)
            if item_count != self.initial_doc_count:
                self.fail("Data count mismatch in Dynamo and Kafka datasets")

        # create crud on the dynamo tables
        self.load_doc_to_dynamo_table(self.dynamo_table, 0, self.initial_doc_count * 2)
        self.load_doc_to_dynamo_table(self.dynamo_table, 0, self.initial_doc_count * 2, action="upsert")
        delete_start = randint(0, self.initial_doc_count//4)
        delete_end = randint(self.initial_doc_count//4 + 1, self.initial_doc_count//2)
        self.load_doc_to_dynamo_table(self.dynamo_table, delete_start, delete_end , action="delete")

        item_count = self.dynamo_lib.get_item_count(self.dynamo_table)
        for dataset in datasets:
            if not self.cbas_util.wait_for_ingestion_complete(self.columnar_cluster, dataset.full_name, item_count):
                self.fail("Failed to ingest data from DynamoDB")
        self.log.info("Data count matched in Dynamo and Columnar")
