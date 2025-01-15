import time
from datetime import datetime
import pytz
from datetime import timedelta

from Columnar.columnar_base import ColumnarBaseTest
from capellaAPI.capella.columnar.CapellaAPI import CapellaAPI as ColumnarAPI
from Columnar.mini_volume_code_template import MiniVolume

# External Database loader related imports
from sirius_client_framework.sirius_constants import SiriusCodes
from couchbase_utils.kafka_util.confluent_utils import ConfluentUtils
from couchbase_utils.kafka_util.kafka_connect_util import KafkaConnectUtil
from Jython_tasks.sirius_task import MongoUtil, CouchbaseUtil


def convert_pacific_to_utc(pacific_time):
    utc_dt = pacific_time.astimezone(pytz.utc)
    utc_year = utc_dt.year
    utc_month = utc_dt.month
    utc_day = utc_dt.day
    utc_hour = utc_dt.hour
    utc_minute = utc_dt.minute
    utc_second = utc_dt.second
    input_datetime_utc = datetime(utc_year, utc_month, utc_day, utc_hour, utc_minute, utc_second)

    # Format the datetime to ISO 8601 with 'Z' for UTC
    formatted_datetime = input_datetime_utc.strftime('%Y-%m-%dT%H:%M:%SZ')
    return formatted_datetime


class OnOff(ColumnarBaseTest):
    def setUp(self):
        super(OnOff, self).setUp()
        self.columnar_cluster = self.tenant.columnar_instances[0]

        self.columnarAPI = ColumnarAPI(self.pod.url_public, '', '', self.tenant.user,
                                       self.tenant.pwd, '')

        self.no_of_docs = self.input.param("no_of_docs", 1000)
        
        if len(self.tenant.clusters) > 0:
            self.remote_cluster = self.tenant.clusters[0]
            self.couchbase_doc_loader = CouchbaseUtil(
                task_manager=self.task_manager,
                hostname=self.remote_cluster.master.ip,
                username=self.remote_cluster.master.rest_username,
                password=self.remote_cluster.master.rest_password,
            )

        self.mongo_util = MongoUtil(
            task_manager=self.task_manager,
            hostname=self.input.param("mongo_hostname"),
            username=self.input.param("mongo_username"),
            password=self.input.param("mongo_password")
        )

        # Initialize variables for Kafka
        self.kafka_topic_prefix = f"on_off_{int(time.time())}"

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
                "MYSQLDB": []
            }
        }

        if not self.columnar_spec_name:
            self.columnar_spec_name = "full_template"

        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.setUp.__name__)

    def tearDown(self):
        """
        Delete all the analytics link and columnar instance
        """
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)

        delete_confluent_dlq_topic = True
        if hasattr(self, "kafka_topic_prefix"):
            delete_confluent_dlq_topic = self.confluent_util.kafka_cluster_util.delete_topic_by_topic_prefix(
                self.kafka_topic_prefix)

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
            confluent_cleanup_for_non_cdc = True

        mongo_collections_deleted = True
        if hasattr(self, "mongo_collections"):
            for mongo_coll, _ in self.mongo_collections.items():
                database, collection = mongo_coll.split(".")
                mongo_collections_deleted = mongo_collections_deleted and (
                    self.mongo_util.delete_mongo_collection(database, collection))

        if not all([confluent_cleanup_for_non_cdc,
                    confluent_cleanup_for_cdc, delete_confluent_dlq_topic,
                    mongo_collections_deleted]):
            self.fail(f"Unable to either cleanup AWS Kafka resources or "
                      f"Confluent Kafka resources or delete mongo collections")

        if hasattr(self, "mini_volume"):
            self.mini_volume.stop_crud_on_data_sources()
            self.mini_volume.stop_process()
        if hasattr(self, "remote_cluster"):
            self.delete_all_buckets_from_capella_cluster(
                self.tenant, self.remote_cluster)
        
        if not self.cbas_util.delete_cbas_infra_created_from_spec(
                self.columnar_cluster, self.columnar_spec):
            self.fail("Error while deleting cbas entities")

        # super(ColumnarBaseTest, self).tearDown()
        self.log_setup_status(
            self.__class__.__name__, "Finished", stage="Teardown")

    def setup_infra_for_mongo(self):
        """
        This method will create a mongo collection, load initial data into it
        and deploy connectors for streaming both cdc and non-cdc data from
        confluent and AWS MSK kafka
        :return:
        """
        self.log.info("Creating Collections on Mongo")
        self.mongo_collections = {}
        self.mongo_db_name = f"TAF_on_off_mongo_db_{int(time.time())}"
        for i in range(1, self.input.param("num_mongo_collections") + 1):
            self.mongo_coll_name = f"TAF_on_off_mongo_coll_{i}"

            self.log.info(f"Creating collection {self.mongo_db_name}."
                          f"{self.mongo_coll_name}")
            if not self.mongo_util.create_mongo_collection(
                    self.mongo_db_name, self.mongo_coll_name):
                self.fail(
                    f"Error while creating mongo collection {self.mongo_db_name}."
                    f"{self.mongo_coll_name}")
            self.mongo_collections[f"{self.mongo_db_name}.{self.mongo_coll_name}"] = 0

        self.log.info(f"Loading docs in mongoDb collection "
                      f"{self.mongo_db_name}.{self.mongo_coll_name}")
        mongo_doc_loading_task = self.mongo_util.load_docs_in_mongo_collection(
            database=self.mongo_db_name, collection=self.mongo_coll_name,
            start=0, end=self.no_of_docs,
            doc_template=SiriusCodes.Templates.PRODUCT,
            doc_size=self.doc_size, sdk_batch_size=1000,
            wait_for_task_complete=True)
        if not mongo_doc_loading_task.result:
            self.fail(f"Failed to load docs in mongoDb collection "
                      f"{self.mongo_db_name}.{self.mongo_coll_name}")
        else:
            self.mongo_collections[
                f"{self.mongo_db_name}.{self.mongo_coll_name}"] = \
                mongo_doc_loading_task.success_count



        self.log.info("Generating Connector config for Mongo CDC")
        self.cdc_connector_name = f"mongo_{self.kafka_topic_prefix}_cdc"
        cdc_connector_config = KafkaConnectUtil.generate_mongo_connector_config(
            mongo_connection_str=self.mongo_util.loader.connection_string,
            mongo_collections=list(self.mongo_collections.keys()),
            topic_prefix=self.kafka_topic_prefix+"_cdc",
            partitions=32, cdc_enabled=True)

        self.log.info("Generating Connector config for Mongo Non-CDC")
        self.non_cdc_connector_name = f"mongo_{self.kafka_topic_prefix}_non_cdc"
        non_cdc_connector_config = (
            KafkaConnectUtil.generate_mongo_connector_config(
                mongo_connection_str=self.mongo_util.loader.connection_string,
                mongo_collections=list(self.mongo_collections.keys()),
                topic_prefix=self.kafka_topic_prefix + "_non_cdc",
                partitions=32, cdc_enabled=False))

        self.log.info("Deploying CDC connectors to stream data from mongo to "
                      "Confluent")
        if self.confluent_util.deploy_connector(
                self.cdc_connector_name, cdc_connector_config,
                self.kafka_connect_hostname_cdc_confluent):
            self.confluent_cluster_obj.connectors[
                self.cdc_connector_name] = cdc_connector_config
        else:
            self.fail("Failed to deploy connector for confluent")

        self.log.info("Deploying Non-CDC connectors to stream data from mongo "
                      "to Confluent")
        if self.confluent_util.deploy_connector(
                self.non_cdc_connector_name, non_cdc_connector_config,
                self.kafka_connect_hostname_non_cdc_confluent):
            self.confluent_cluster_obj.connectors[
                self.non_cdc_connector_name] = non_cdc_connector_config
        else:
            self.fail("Failed to deploy connector for confluent")

        for mongo_collection_name, num_items in self.mongo_collections.items():
            for kafka_type in ["confluent"]:
                self.kafka_topics[kafka_type]["MONGODB"].extend(
                    [
                        {
                            "topic_name": f"{self.kafka_topic_prefix+'_cdc'}."
                                          f"{mongo_collection_name}",
                            "key_serialization_type": "json",
                            "value_serialization_type": "json",
                            "cdc_enabled": True,
                            "source_connector": "DEBEZIUM",
                            "num_items": num_items
                        },
                        {
                            "topic_name": f"{self.kafka_topic_prefix+'_non_cdc'}."
                                          f"{mongo_collection_name}",
                            "key_serialization_type": "json",
                            "value_serialization_type": "json",
                            "cdc_enabled": False,
                            "source_connector": "DEBEZIUM",
                            "num_items": num_items
                        }
                    ]
                )

    def load_data_to_source(self, remote_start, remote_end):
        self.log.info(f"Loading docs in mongoDb collection "
                      f"{self.mongo_db_name}.{self.mongo_coll_name}")
        if hasattr(self, "mongo_db_name"):
            mongo_doc_loading_task = self.mongo_util.load_docs_in_mongo_collection(
                database=self.mongo_db_name, collection=self.mongo_coll_name,
                start=0, end=self.no_of_docs,
                doc_template=SiriusCodes.Templates.PRODUCT,
                doc_size=self.doc_size, sdk_batch_size=1000,
                wait_for_task_complete=True)
            if not mongo_doc_loading_task.result:
                self.fail(f"Failed to load docs in mongoDb collection "
                          f"{self.mongo_db_name}.{self.mongo_coll_name}")
            else:
                self.mongo_collections[
                    f"{self.mongo_db_name}.{self.mongo_coll_name}"] = \
                    mongo_doc_loading_task.success_count

        if hasattr(self, "remote_cluster"):
            for remote_bucket in self.remote_cluster.buckets:
                for scope_name, scope in remote_bucket.scopes.items():
                    if scope_name != "_system" and scope != "_mobile":
                        for collection_name, collection in (
                                scope.collections.items()):
                            self.log.info(
                                f"Loading docs in {remote_bucket.name}."
                                f"{scope_name}.{collection_name}")
                            cb_doc_loading_task = self.couchbase_doc_loader.load_docs_in_couchbase_collection(
                                bucket=remote_bucket.name, scope=scope_name,
                                collection=collection_name, start=remote_start,
                                end=remote_end,
                                doc_template=SiriusCodes.Templates.PRODUCT,
                                doc_size=self.doc_size, sdk_batch_size=1000
                            )
                            if not cb_doc_loading_task.result:
                                self.fail(
                                    f"Failed to load docs in couchbase collection "
                                    f"{remote_bucket.name}.{scope_name}.{collection_name}")
                            else:
                                collection.num_items = cb_doc_loading_task.success_count

        standalone_collections_kafka = list()
        standalone_collections = list()
        for standalone_collection in self.cbas_util.get_all_dataset_objs(
                "standalone"):
            if not standalone_collection.data_source:
                standalone_collections.append(standalone_collection)
            elif standalone_collection.data_source in [
                "MONGODB", "MYSQLDB", "POSTGRESQL"]:
                standalone_collections_kafka.append(standalone_collection)
        for collection in standalone_collections:
            if not self.cbas_util.load_doc_to_standalone_collection(
                    self.columnar_cluster, collection.name,
                    collection.dataverse_name, collection.database_name,
                    self.no_of_docs, self.doc_size):
                self.fail(f"Failed to insert docs into standalone collection "
                          f"{collection.full_name}")

    def dataset_count(self, timeout=3600):
        items_in_datasets = {}
        datasets = self.cbas_util.get_all_dataset_objs()
        for dataset in datasets:
            items_in_datasets[dataset.full_name] = self.cbas_util.get_num_items_in_cbas_dataset(
                self.columnar_cluster, dataset.full_name, timeout=timeout,
                analytics_timeout=timeout)
        return items_in_datasets

    def generate_columnar_entities_dict(self):
        columnar_entities = dict()
        columnar_entities[
            "links"] = self.cbas_util.get_all_links_from_metadata(
            self.columnar_cluster)
        columnar_entities[
            "databases"] = self.cbas_util.get_all_databases_from_metadata(
            self.columnar_cluster)
        columnar_entities[
            "dataverses"] = self.cbas_util.get_all_dataverses_from_metadata(
            self.columnar_cluster)
        columnar_entities[
            "datasets"] = self.cbas_util.get_all_datasets_from_metadata(
            self.columnar_cluster)
        columnar_entities[
            "indexes"] = self.cbas_util.get_all_indexes_from_metadata(
            self.columnar_cluster)
        columnar_entities[
            "synonyms"] = self.cbas_util.get_all_synonyms_from_metadata(
            self.columnar_cluster)
        columnar_entities["udfs"] = [
            udf[0] for udf in self.cbas_util.get_all_udfs_from_metadata(
                self.columnar_cluster)]
        return columnar_entities

    def test_on_demand_on_off(self):
        # creating bucket scope and collections for remote collection
        self.create_bucket_scopes_collections_in_capella_cluster(
            self.tenant, self.remote_cluster,
            self.input.param("num_buckets", 1))

        self.setup_infra_for_mongo()

        aws_kafka_cluster_details = [
            self.msk_util.generate_aws_kafka_cluster_detail(
                brokers_url=self.msk_cluster_obj.bootstrap_brokers[
                    "PublicSaslScram"],
                auth_type="SCRAM_SHA_512", encryption_type="TLS",
                username=self.msk_cluster_obj.sasl_username,
                password=self.msk_cluster_obj.sasl_password)]
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
            external_collection_file_formats=["json"],
            aws_kafka_cluster_details=aws_kafka_cluster_details,
            confluent_kafka_cluster_details=confluent_kafka_cluster_details,
            external_dbs=["MONGODB"],
            kafka_topics=self.kafka_topics)

        self.columnar_spec["standalone_dataset"]["primary_key"] = [
            {"name": "string", "email": "string"}]
        self.columnar_spec["index"]["indexed_fields"] = ["price:double"]

        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            cluster=self.columnar_cluster, cbas_spec=self.columnar_spec,
            bucket_util=self.bucket_util, wait_for_ingestion=False,
            remote_clusters=[self.remote_cluster])
        if not result:
            self.fail(msg)

        self.load_data_to_source(0, self.no_of_docs)

        self.cbas_util.refresh_remote_dataset_item_count(self.bucket_util)

        remote_datasets = self.cbas_util.get_all_dataset_objs("remote")
        for collection in remote_datasets:
            if not self.cbas_util.wait_for_ingestion_complete(
                    self.columnar_cluster, collection.full_name,
                    collection.num_of_items):
                self.fail(
                    f"FAILED: Initial ingestion into {collection.full_name}.")

        self.cbas_util.wait_for_data_ingestion_in_the_collections(self.columnar_cluster, 1800)
        cbas_entities_before_operation = self.generate_columnar_entities_dict()
        dataset_count = self.dataset_count()

        result = self.cbas_util.disconnect_links(
            self.columnar_cluster, self.columnar_spec)
        if not all(result):
            self.fail("Error while disconnecting link")

        result = self.cbas_util.connect_links(
            self.columnar_cluster, self.columnar_spec)
        if not all(result):
            self.fail("Error while connecting link")

        if not self.columnar_utils.turn_off_instance(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster, wait_to_turn_off=True):
            self.fail("Failed to Turn-Off the cluster")

        # resume the instance
        if not self.columnar_utils.turn_on_instance(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster, wait_to_turn_on=True):
            self.fail("Failed to Turn-On the cluster")

        self.cbas_util.wait_for_cbas_to_recover(self.columnar_cluster)
        for collection in remote_datasets:
            if not self.cbas_util.wait_for_ingestion_complete(
                    self.columnar_cluster, collection.full_name,
                    collection.num_of_items):
                self.fail(
                    f"FAILED: Initial ingestion into {collection.full_name}.")
        self.cbas_util.wait_for_data_ingestion_in_the_collections(self.columnar_cluster, 1800)

        dataset_count_after_restore = self.dataset_count()
        if dataset_count != dataset_count_after_restore:
            self.fail("Data mismatch after restore")

        status, error = self.cbas_util.perform_metadata_validation_for_all_entities(
            self.columnar_cluster, cbas_entities_before_operation)
        if not status:
            self.fail(error)

    def test_on_demand_off_after_scaling_and_scale_after_resume(self):
        # creating bucket scope and collections for remote collection
        self.create_bucket_scopes_collections_in_capella_cluster(
            self.tenant, self.remote_cluster,
            self.input.param("num_buckets", 1))

        self.setup_infra_for_mongo()

        aws_kafka_cluster_details = [
            self.msk_util.generate_aws_kafka_cluster_detail(
                brokers_url=self.msk_cluster_obj.bootstrap_brokers[
                    "PublicSaslScram"],
                auth_type="SCRAM_SHA_512", encryption_type="TLS",
                username=self.msk_cluster_obj.sasl_username,
                password=self.msk_cluster_obj.sasl_password)]
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
            external_collection_file_formats=["json"],
            aws_kafka_cluster_details=aws_kafka_cluster_details,
            confluent_kafka_cluster_details=confluent_kafka_cluster_details,
            external_dbs=["MONGODB"],
            kafka_topics=self.kafka_topics)

        self.columnar_spec["standalone_dataset"]["primary_key"] = [
            {"name": "string", "email": "string"}]
        self.columnar_spec["index"]["indexed_fields"] = ["price:double"]

        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            cluster=self.columnar_cluster, cbas_spec=self.columnar_spec,
            bucket_util=self.bucket_util, wait_for_ingestion=False,
            remote_clusters=[self.remote_cluster])
        if not result:
            self.fail(msg)

        self.load_data_to_source(0, self.no_of_docs)

        self.cbas_util.refresh_remote_dataset_item_count(self.bucket_util)

        remote_datasets = self.cbas_util.get_all_dataset_objs("remote")
        for collection in remote_datasets:
            if not self.cbas_util.wait_for_ingestion_complete(
                    self.columnar_cluster, collection.full_name,
                    collection.num_of_items):
                self.fail(
                    f"FAILED: Initial ingestion into {collection.full_name}.")

        self.cbas_util.wait_for_data_ingestion_in_the_collections(self.columnar_cluster, 1800)
        cbas_entities_before_operation = self.generate_columnar_entities_dict()
        dataset_count = self.dataset_count()

        result = self.cbas_util.disconnect_links(
            self.columnar_cluster, self.columnar_spec)
        if not all(result):
            self.fail("Error while disconnecting link")

        result = self.cbas_util.connect_links(
            self.columnar_cluster, self.columnar_spec)
        if not all(result):
            self.fail("Error while connecting link")

        if not self.columnar_utils.scale_instance(
                self.pod, self.tenant, self.tenant.project_id,
                self.columnar_cluster, 8):
            self.fail(
                "Scale API failed while scaling instance from {0} --> "
                "{1}".format(len(self.columnar_cluster.nodes_in_cluster), 8))

        if not self.columnar_utils.wait_for_instance_scaling_operation(
                self.pod, self.tenant, self.tenant.project_id,
                self.columnar_cluster):
            self.fail("Failed to scale OUT instance even after 3600 seconds")

        if not self.columnar_utils.turn_off_instance(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster, wait_to_turn_off=True):
            self.fail("Failed to Turn-Off the cluster")

        # resume instance
        if not self.columnar_utils.turn_on_instance(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster, wait_to_turn_on=True):
            self.fail("Failed to Turn-On the cluster")

        self.cbas_util.wait_for_cbas_to_recover(self.columnar_cluster)

        if not self.columnar_utils.scale_instance(
                self.pod, self.tenant, self.tenant.project_id,
                self.columnar_cluster, 2):
            self.fail(
                "Scale API failed while scaling instance from {0} --> "
                "{1}".format(len(self.columnar_cluster.nodes_in_cluster), 8))

        if not self.columnar_utils.wait_for_instance_scaling_operation(
                self.pod, self.tenant, self.tenant.project_id,
                self.columnar_cluster):
            self.fail("Failed to scale OUT instance even after 3600 seconds")

        for collection in remote_datasets:
            if not self.cbas_util.wait_for_ingestion_complete(
                    self.columnar_cluster, collection.full_name,
                    collection.num_of_items):
                self.fail(
                    f"FAILED: Initial ingestion into {collection.full_name}.")
        self.cbas_util.wait_for_data_ingestion_in_the_collections(self.columnar_cluster, 1800)

        dataset_count_after_restore = self.dataset_count()
        if dataset_count != dataset_count_after_restore:
            self.fail("Data mismatch after restore")

        status, error = self.cbas_util.perform_metadata_validation_for_all_entities(
            self.columnar_cluster, cbas_entities_before_operation)
        if not status:
            self.fail(error)

    def test_off_during_scale(self):
        if not self.columnar_utils.scale_instance(
                self.pod, self.tenant, self.tenant.project_id,
                self.columnar_cluster, 32):
            self.fail(
                "Scale API failed while scaling instance from {0} --> "
                "{1}".format(len(self.columnar_cluster.nodes_in_cluster), 32))
        # off during scale
        else:
            resp = self.columnarAPI.turn_off_instance(
                self.tenant.id, self.tenant.project_id,
                self.columnar_cluster.instance_id)
            if resp.status_code != 422:
                if resp["errorType"] != "EntityStateInvalid":
                    self.fail("Status code and errorType mismatch")

        if not self.columnar_utils.wait_for_instance_scaling_operation(
                self.pod, self.tenant, self.tenant.project_id,
                self.columnar_cluster):
            self.fail("Failed to scale instance even after 3600 seconds")

    def test_scale_during_resume(self):
        if not self.columnar_utils.turn_off_instance(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster, wait_to_turn_off=True):
            self.fail("Failed to Turn-Off the cluster")

        if not self.columnar_utils.turn_on_instance(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster, wait_to_turn_on=True):
            self.fail("Failed to Turn-On the cluster")

        if not self.columnar_utils.scale_instance(
                self.pod, self.tenant, self.tenant.project_id,
                self.columnar_cluster, 32):
            self.fail(
                "Scale API failed while scaling instance from {0} --> "
                "{1}".format(len(self.columnar_cluster.nodes_in_cluster), 4))

        if not self.columnar_utils.wait_for_instance_to_turn_on(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster, timeout=3600):
            self.fail("Failed to turn on the instance")

    def test_off_during_off(self):
        if not self.columnar_utils.turn_off_instance(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster, wait_to_turn_off=False):
            self.fail("Failed to Turn-Off the cluster")

        if not self.columnar_utils.turn_off_instance(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster, wait_to_turn_off=False):
            self.fail("Failed to Turn-Off the cluster")

        if not self.columnar_utils.wait_for_instance_to_turn_off(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster, timeout=3600):
            self.fail("Failed to turn on the instance")

        if not self.columnar_utils.turn_on_instance(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster, wait_to_turn_on=True):
            self.fail("Failed to Turn-On the cluster")

    def test_on_during_on(self):
        if not self.columnar_utils.turn_off_instance(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster, wait_to_turn_off=True):
            self.fail("Failed to Turn-Off the cluster")

        if not self.columnar_utils.turn_on_instance(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster, wait_to_turn_on=False):
            self.fail("Failed to Turn-On the cluster")

        if not self.columnar_utils.turn_on_instance(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster, wait_to_turn_on=False):
            self.fail("Failed to Turn-On the cluster")

        if not self.columnar_utils.wait_for_instance_to_turn_on(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster, timeout=3600):
            self.fail("Failed to turn on the instance")

    def test_on_during_off(self):
        if not self.columnar_utils.turn_off_instance(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster, wait_to_turn_off=False):
            self.fail("Failed to Turn-Off the cluster")

        resp = self.columnarAPI.turn_on_instance(
            self.tenant.id, self.tenant.project_id,
            self.columnar_cluster.instance_id)
        if resp.status_code == 500:
            if not self.columnar_utils.wait_for_instance_to_turn_off(
                    pod=self.pod, tenant=self.tenant,
                    project_id=self.tenant.project_id,
                    instance=self.columnar_cluster, timeout=3600):
                self.fail("Failed to turn on the instance")
            if not self.columnar_utils.turn_on_instance(
                    pod=self.pod, tenant=self.tenant,
                    project_id=self.tenant.project_id,
                    instance=self.columnar_cluster, wait_to_turn_on=True):
                self.fail("Failed to Turn-On the cluster")
            self.fail("Failed due to bug")

        self.log.info("Bug fixed")

        if not self.columnar_utils.wait_for_instance_to_turn_off(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster, timeout=3600):
            self.fail("Failed to turn on the instance")

        if not self.columnar_utils.turn_on_instance(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster, wait_to_turn_on=True):
            self.fail("Failed to Turn-On the cluster")

    def test_off_during_on(self):
        if not self.columnar_utils.turn_off_instance(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster, wait_to_turn_off=True):
            self.fail("Failed to Turn-Off the cluster")

        if not self.columnar_utils.turn_on_instance(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster, wait_to_turn_on=False):
            self.fail("Failed to Turn-On the cluster")

        resp = self.columnarAPI.turn_off_instance(
            self.tenant.id, self.tenant.project_id,
            self.columnar_cluster.instance_id)
        if resp.status_code == 500:
            if not self.columnar_utils.wait_for_instance_to_turn_on(
                    pod=self.pod, tenant=self.tenant,
                    project_id=self.tenant.project_id,
                    instance=self.columnar_cluster, timeout=3600):
                self.fail("Failed to turn on the instance")
            self.fail("Failed due to bug")

        if not self.columnar_utils.wait_for_instance_to_turn_on(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster, timeout=3600):
            self.fail("Failed to turn on the instance")

    def test_schedule_on_off(self):
        # creating bucket scope and collections for remote collection
        self.create_bucket_scopes_collections_in_capella_cluster(
            self.tenant, self.remote_cluster,
            self.input.param("num_buckets", 1))

        self.setup_infra_for_mongo()
        aws_kafka_cluster_details = [
            self.msk_util.generate_aws_kafka_cluster_detail(
                brokers_url=self.msk_cluster_obj.bootstrap_brokers[
                    "PublicSaslScram"],
                auth_type="SCRAM_SHA_512", encryption_type="TLS",
                username=self.msk_cluster_obj.sasl_username,
                password=self.msk_cluster_obj.sasl_password)]
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
            external_collection_file_formats=["json"],
            aws_kafka_cluster_details=aws_kafka_cluster_details,
            confluent_kafka_cluster_details=confluent_kafka_cluster_details,
            external_dbs=["MONGODB"],
            kafka_topics=self.kafka_topics)

        self.columnar_spec["standalone_dataset"]["primary_key"] = [
            {"name": "string", "email": "string"}]
        self.columnar_spec["index"]["indexed_fields"] = ["price:double"]

        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            cluster=self.columnar_cluster, cbas_spec=self.columnar_spec,
            bucket_util=self.bucket_util, wait_for_ingestion=False,
            remote_clusters=[self.remote_cluster])
        if not result:
            self.fail(msg)

        self.load_data_to_source(0, self.no_of_docs)

        self.cbas_util.refresh_remote_dataset_item_count(self.bucket_util)

        remote_datasets = self.cbas_util.get_all_dataset_objs("remote")
        for collection in remote_datasets:
            if not self.cbas_util.wait_for_ingestion_complete(
                    self.columnar_cluster, collection.full_name,
                    collection.num_of_items):
                self.fail(
                    f"FAILED: Initial ingestion into {collection.full_name}.")

        self.cbas_util.wait_for_data_ingestion_in_the_collections(self.columnar_cluster, 1800)
        cbas_entities_before_operation = self.generate_columnar_entities_dict()
        dataset_count = self.dataset_count()

        result = self.cbas_util.disconnect_links(
            self.columnar_cluster, self.columnar_spec)
        if not all(result):
            self.fail("Error while disconnecting link")

        result = self.cbas_util.connect_links(
            self.columnar_cluster, self.columnar_spec)
        if not all(result):
            self.fail("Error while connecting link")

        pacific = pytz.timezone('US/Pacific')
        now = datetime.now(pacific)

        # Calculate days until next Thursday (using modulo 7 to wrap around)
        days_until_friday = (4 - now.weekday()) % 7
        if days_until_friday == 0:
            days_until_friday += 7

        # Calculate the next Friday date
        next_friday = now + timedelta(days=days_until_friday)

        # Set the time to 16:30
        next_friday_1630 = next_friday.replace(hour=16, minute=30, second=0, microsecond=0)
        next_friday_1030 = next_friday.replace(hour=10, minute=30, second=0, microsecond=0)
        next_friday_1030_in_utc = convert_pacific_to_utc(next_friday_1030)
        next_friday_1630_in_utc = convert_pacific_to_utc(next_friday_1630)
        used_timezone = "US/Pacific"
        data = [
            {
                "day": "monday",
                "state": "on"
            },
            {
                "day": "tuesday",
                "state": "on"
            },
            {
                "day": "wednesday",
                "state": "on"
            },
            {
                "day": "thursday",
                "state": "on"
            },
            {
                "day": "friday",
                "state": "custom",
                "from": {
                    "hour": 10,
                    "minute": 30
                },
                "to": {
                    "hour": 16,
                    "minute": 30
                }
            },
            {
                "day": "saturday",
                "state": "off"
            },
            {
                "day": "sunday",
                "state": "off"
            }
        ]

        if not self.columnar_utils.create_schedule_on_off(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster, days=data,
                timezone=used_timezone):
            self.fail("Failed to Turn-Off the cluster")

        internal_support_token = self.capella.get("override_token")
        columnar_internal = ColumnarAPI(
            self.pod.url_public, '', '', self.tenant.user,
            self.tenant.pwd, internal_support_token)
        resp = columnar_internal.set_trigger_time_for_onoff(
            next_friday_1630_in_utc, [self.columnar_cluster.instance_id])
        if resp.status_code == 200:
            self.log.info("Applied sudo time to trigger off")
        else:
            self.fail("Failed to apply sudo time")

        if not self.columnar_utils.wait_for_instance_to_turn_off(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster, timeout=3600):
            self.fail("Failed to turn off the instance")

        resp = columnar_internal.set_trigger_time_for_onoff(
            next_friday_1030_in_utc, [self.columnar_cluster.instance_id])
        if resp.status_code == 200:
            self.log.info("Applied sudo time to trigger on")
        else:
            self.fail("Failed to apply sudo time")

        if not self.columnar_utils.wait_for_instance_to_turn_on(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster, timeout=3600):
            self.fail("Failed to turn off the instance")

        self.cbas_util.wait_for_cbas_to_recover(self.columnar_cluster)

        for collection in remote_datasets:
            if not self.cbas_util.wait_for_ingestion_complete(
                    self.columnar_cluster, collection.full_name,
                    collection.num_of_items):
                self.fail(
                    f"FAILED: Initial ingestion into {collection.full_name}.")
        self.cbas_util.wait_for_data_ingestion_in_the_collections(self.columnar_cluster, 1800)

        dataset_count_after_restore = self.dataset_count()
        if dataset_count != dataset_count_after_restore:
            self.fail("Data mismatch after restore")

        status, error = self.cbas_util.perform_metadata_validation_for_all_entities(
            self.columnar_cluster, cbas_entities_before_operation)
        if not status:
            self.fail(error)

    def test_mini_volume_on_off(self):
        # creating bucket scope and collections for remote collection
        self.create_bucket_scopes_collections_in_capella_cluster(
            self.tenant, self.remote_cluster,
            self.input.param("num_buckets", 1))

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
            remote_cluster=self.remote_cluster,
            external_collection_file_formats=["json"],
            confluent_kafka_cluster_details=confluent_kafka_cluster_details,
            external_dbs=["MONGODB"],
            kafka_topics=self.kafka_topics)

        self.columnar_spec["standalone_dataset"]["primary_key"] = [
            {"name": "string", "email": "string"}]
        self.columnar_spec["index"]["indexed_fields"] = ["price:double"]
        self.columnar_spec["kafka_dataset"]["primary_key"] = [
            {"_id": "string"}]

        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            cluster=self.columnar_cluster, cbas_spec=self.columnar_spec,
            bucket_util=self.bucket_util, wait_for_ingestion=False,
            remote_clusters=[self.remote_cluster])
        if not result:
            self.fail(msg)

        start_time = time.time()
        self.sirius_url = self.input.param("sirius_url", "http://127.0.0.1:4000")
        self.mini_volume = MiniVolume(self)
        self.mini_volume.calculate_volume_per_source()
        for i in range(1, 5):
            if i % 2 == 0:
                self.mini_volume.run_processes(i, 2 ** (i - 1), False, True)
            else:
                self.mini_volume.run_processes(i, 2 ** (i + 1), False)

            self.mini_volume.stop_process()
            docs_in_collections_before = self.dataset_count()
            self.cbas_util.disconnect_links(self.columnar_cluster, self.columnar_spec)
            self.cbas_util.connect_links(self.columnar_cluster, self.columnar_spec)
            # start turning off the columnar instance
            if not self.columnar_utils.turn_off_instance(
                    pod=self.pod, tenant=self.tenant,
                    project_id=self.tenant.project_id,
                    instance=self.columnar_cluster, wait_to_turn_off=True):
                self.fail("Failed to Turn-Off the cluster")

            # start turning on the columnar instance
            if not self.columnar_utils.turn_on_instance(
                    pod=self.pod, tenant=self.tenant,
                    project_id=self.tenant.project_id,
                    instance=self.columnar_cluster, wait_to_turn_on=False):
                self.fail("Failed to Turn-On the cluster")

            self.columnar_utils.update_columnar_instance_obj(
                self.pod, self.tenant, self.columnar_cluster)
            docs_in_collection_after = self.dataset_count()
            if docs_in_collection_after != docs_in_collections_before:
                self.fail("Doc count mismatch after on/off")
        self.log.info("Time taken to run mini-volume: {} minutes".format((time.time() - start_time)/60))
