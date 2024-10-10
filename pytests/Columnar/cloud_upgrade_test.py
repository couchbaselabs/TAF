"""
Created on 22-MAY-2024

@author: umang.agrawal

This test suite contains tests for columnar upgrades on cloud.
"""
import time

from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from Columnar.columnar_base import ColumnarBaseTest
from datetime import datetime, timedelta

# Kafka related imports
from couchbase_utils.kafka_util.confluent_utils import ConfluentUtils
from couchbase_utils.kafka_util.kafka_connect_util import KafkaConnectUtil
from couchbase_utils.kafka_util.msk_utils import MSKUtils

# External Database loader related imports
from Jython_tasks.sirius_task import MongoUtil, CouchbaseUtil
from sirius_client_framework.sirius_constants import SiriusCodes


class ColumnarCloudUpgrade(ColumnarBaseTest):

    def setUp(self):
        super(ColumnarCloudUpgrade, self).setUp()

        # Since all the test cases are being run on 1 cluster only
        self.columnar_cluster = self.tenant.columnar_instances[0]
        self.remote_cluster = self.tenant.clusters[0]

        # Initial number of docs to be loaded in external DB collections,
        # remote collections and standalone collections.
        self.initial_doc_count = self.input.param("initial_doc_count", 100)

        # Initialize sirius doc loader utils
        self.mongo_util = MongoUtil(
            task_manager=self.task_manager,
            hostname=self.input.param("mongo_hostname"),
            username=self.input.param("mongo_username"),
            password=self.input.param("mongo_password")
        )
        self.couchbase_doc_loader = CouchbaseUtil(
            task_manager=self.task_manager,
            hostname=self.remote_cluster.master.ip,
            username=self.remote_cluster.master.rest_username,
            password=self.remote_cluster.master.rest_password,
        )

        # Initialize variables for Kafka
        self.kafka_topic_prefix = f"e2e_upgrade_{int(time.time())}"

        # Initializing AWS_MSK util and AWS_MSK cluster object.
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
        self.kafka_connect_hostname_cdc_msk = (
            f"{kafka_connect_hostname}:{KafkaConnectUtil.AWS_MSK_CDC_PORT}")
        self.kafka_connect_hostname_non_cdc_msk = (
            f"{kafka_connect_hostname}:{KafkaConnectUtil.AWS_MSK_NON_CDC_PORT}")

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
            },
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
                "MYSQLDB": []
            }
        }

        self.doc_count_level_1_folder_1 = {
            "json": 1560000, "parquet": 1560000,
            "csv": 1560000, "tsv": 1560000, "avro": 1560000}

        # Columnar entities creation specifications
        if not self.columnar_spec_name:
            self.columnar_spec_name = "full_template"

        self.upgrade_version = self.input.param("upgrade_version")

        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.setUp.__name__)

    def tearDown(self):
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)
        delete_msk_dlq_topic = self.msk_util.kafka_cluster_util.delete_topic_by_topic_prefix(
                self.kafka_topic_prefix)

        if hasattr(self, "cdc_connector_name"):
            msk_cleanup_for_cdc = self.msk_util.cleanup_kafka_resources(
                self.kafka_connect_hostname_cdc_msk, [self.cdc_connector_name],
                self.kafka_topic_prefix + "_cdc"
                )
        else:
            msk_cleanup_for_cdc = True

        if hasattr(self, "non_cdc_connector_name"):
            msk_cleanup_for_non_cdc = self.msk_util.cleanup_kafka_resources(
                self.kafka_connect_hostname_non_cdc_msk, [self.non_cdc_connector_name],
                self.kafka_topic_prefix + "_non_cdc"
            )
        else:
            msk_cleanup_for_non_cdc = True

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
        for mongo_coll, _ in self.mongo_collections.items():
            database, collection = mongo_coll.split(".")
            mongo_collections_deleted = mongo_collections_deleted and (
                self.mongo_util.delete_mongo_collection(database, collection))

        if not all([msk_cleanup_for_cdc, msk_cleanup_for_non_cdc,
                    delete_msk_dlq_topic, confluent_cleanup_for_non_cdc,
                    confluent_cleanup_for_cdc, delete_confluent_dlq_topic,
                    mongo_collections_deleted]):
            self.fail(f"Unable to either cleanup AWS Kafka resources or "
                      f"Confluent Kafka resources or delete mongo collections")

        self.delete_all_buckets_from_capella_cluster(
            self.tenant, self.remote_cluster)

        super(ColumnarCloudUpgrade, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage="Teardown")

    def setup_infra_for_mongo(self):
        """
        This method will create a mongo collection, load initial data into it
        and deploy connectors for streaming both cdc and non-cdc data from
        confluent and AWS MSK kafka
        :return:
        """
        self.log.info("Creating Collections on Mongo")
        self.mongo_collections = {}
        mongo_db_name = f"TAF_upgrade_mongo_db_{int(time.time())}"
        for i in range(1, self.input.param("num_mongo_collections") + 1):
            mongo_coll_name = f"TAF_upgrade_mongo_coll_{i}"

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

        self.log.info("Deploying CDC connectors to stream data from mongo to "
                      "AWS Kafka")
        if self.msk_util.deploy_connector(
                self.cdc_connector_name, cdc_connector_config,
                self.kafka_connect_hostname_cdc_msk):
            self.msk_cluster_obj.connectors[self.cdc_connector_name] = cdc_connector_config
        else:
            self.fail("Failed to deploy connector for AWS Kafka")

        self.log.info("Deploying Non-CDC connectors to stream data from mongo "
                      "to AWS Kafka")
        if self.msk_util.deploy_connector(
                self.non_cdc_connector_name, non_cdc_connector_config,
                self.kafka_connect_hostname_non_cdc_msk):
            self.msk_cluster_obj.connectors[
                self.non_cdc_connector_name] = non_cdc_connector_config
        else:
            self.fail("Failed to deploy connector for AWS Kafka")

        for mongo_collection_name, num_items in self.mongo_collections.items():
            for kafka_type in ["confluent", "aws_kafka"]:
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

    def setup_infra_for_remote_cluster(self):
        """
        This method will create buckets, scopes and collections on remote
        couchbase cluster and will load initial data into it.
        :return:
        """
        self.log.info("Creating Buckets, Scopes and Collections on remote "
                      "cluster.")
        self.create_bucket_scopes_collections_in_capella_cluster(
            self.tenant, self.remote_cluster, num_buckets=self.num_buckets,
            bucket_ram_quota=self.bucket_size,
            num_scopes_per_bucket=self.input.param("num_scopes_per_bucket", 1),
            num_collections_per_scope=self.input.param(
                "num_collections_per_scope", 1))

        self.log.info("Loading data into remote couchbase collection")
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
                            collection=collection_name, start=0,
                            end=self.initial_doc_count,
                            doc_template=SiriusCodes.Templates.PRODUCT,
                            doc_size=self.doc_size, sdk_batch_size=1000
                        )
                        if not cb_doc_loading_task.result:
                            self.fail(
                                f"Failed to load docs in couchbase collection "
                                f"{remote_bucket.name}.{scope_name}.{collection_name}")
                        else:
                            collection.num_items = cb_doc_loading_task.success_count

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

    def validate_data_in_datasets(self):
        self.cbas_util.refresh_remote_dataset_item_count(self.bucket_util)
        self.log.info("Validating doc count in Analytics collections")
        for dataset in self.cbas_util.get_all_dataset_objs(
                "remote") + self.cbas_util.get_all_dataset_objs(
            "standalone"):
            if not self.cbas_util.wait_for_ingestion_complete(
                    cluster=self.columnar_cluster,
                    dataset_name=dataset.full_name,
                    num_items=dataset.num_of_items, timeout=3600):
                self.fail(
                    f"FAILED: Initial ingestion into {dataset.full_name}.")

        self.log.info("Verifying doc count in external collections")
        query = "select count(*) from {} where level_no=1 and folder_no=1"
        for dataset in self.cbas_util.get_all_dataset_objs("external"):
            self.log.debug(
                f"Executing query - {query.format(dataset.full_name)}")
            status, _, error, result, _, _ = \
                self.cbas_util.execute_statement_on_cbas_util(
                    self.columnar_cluster, query.format(dataset.full_name)
                )
            file_format = dataset.dataset_properties["file_format"]

            if status != "success":
                self.fail(f"Query execution failed with error - {error}")
            elif result[0]["$1"] != self.doc_count_level_1_folder_1[
                file_format]:
                self.fail(
                    "Doc count mismatch. Expected - {0}, Actual - {1}".format(
                        self.doc_count_level_1_folder_1[file_format],
                        result[0]["$1"]))

    def test_end_to_end_upgrade(self):
        """
        This test will perform following -
        1. Create all type of columnar entities. - Done
        2. Load data into all types of collections. - Done
        3. Validate ingestion into collections. - Done
        4. Turn off cluster. - Done
        5. Turn on Cluster. - Done
        6. Validate all entities and data is present. - Done
        7. On-Demand backup of the cluster. - Done
        8. Start parallel queries.
        9. Scale-up the cluster. - Done
        10. Perform Cluster upgrade. - Done
        11. Validate all entities and data is present. - Done
        12. Create more columnar entities. - Done
        13. Scale-down the cluster. - Done
        14. On-Demand backup of the cluster. - Done
        15. Restore the backup that was taken before cluster upgrade. - Done
        16. Validate all entities and data is present. - Done
        17. Turn off the cluster - Done
        18. Turn on the cluster - Done
        19. Validate all entities and data is present. - Done
        """
        self.setup_infra_for_mongo()

        self.setup_infra_for_remote_cluster()

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
            external_collection_file_formats=["json", "csv", "tsv", "avro",
                                              "parquet"],
            path_on_external_container="level_{level_no:int}_folder_{"
                                       "folder_no:int}",
            aws_kafka_cluster_details=aws_kafka_cluster_details,
            confluent_kafka_cluster_details=confluent_kafka_cluster_details,
            external_dbs=["MONGODB"],
            kafka_topics=self.kafka_topics)
        self.columnar_spec["kafka_dataset"]["primary_key"] = [
            {"_id": "string"}]
        self.columnar_spec["index"]["indexed_fields"] = ["price:double"]

        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            cluster=self.columnar_cluster, cbas_spec=self.columnar_spec,
            bucket_util=self.bucket_util, wait_for_ingestion=False,
            remote_clusters=[self.remote_cluster])

        if not result:
            self.fail(msg)

        standalone_collections_kafka = list()
        standalone_collections = list()
        for standalone_collection in self.cbas_util.get_all_dataset_objs(
                "standalone"):
            if not standalone_collection.data_source:
                standalone_collections.append(standalone_collection)
            elif standalone_collection.data_source in [
                "MONGODB", "MYSQLDB", "POSTGRESQL"]:
                standalone_collections_kafka.append(standalone_collection)

        self.log.info("Loading data into standalone collections")
        for standalone_collection in standalone_collections:
            if not self.cbas_util.load_doc_to_standalone_collection(
                    cluster=self.columnar_cluster,
                    collection_name=standalone_collection.name,
                    dataverse_name=standalone_collection.dataverse_name,
                    database_name=standalone_collection.database_name,
                    no_of_docs=self.initial_doc_count,
                    document_size=self.doc_size):
                self.fail(f"Failed to insert docs into standalone collection "
                          f"{standalone_collection.full_name}")
            standalone_collection.num_of_items = self.initial_doc_count

        self.validate_data_in_datasets()

        # This will be used to verify cbas entities post restore.
        cbas_entities_before_upgrade = self.generate_columnar_entities_dict()
        doc_count_for_datasets_before_upgrade = dict()
        for dataset in self.cbas_util.get_all_dataset_objs(
                "remote") + self.cbas_util.get_all_dataset_objs("standalone"):
            doc_count_for_datasets_before_upgrade[
                dataset.full_name] = dataset.num_of_items

        # Disconnecting all kafka and remote links so that the data is
        # persisted before backup.
        # https://jira.issues.couchbase.com/browse/AV-82641
        self.log.info("Disconnecting all kafka and remote links before "
                      "backup so that the data is persisted")
        for link in self.cbas_util.get_all_link_objs(
                "couchbase") + self.cbas_util.get_all_link_objs("kafka"):
            if not self.cbas_util.disconnect_link(
                    self.columnar_cluster, link.name):
                self.fail(f"Error while disconnecting link {link.name}")

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

        status, error = self.cbas_util.perform_metadata_validation_for_all_entities(
            self.columnar_cluster, cbas_entities_before_upgrade)
        if not status:
            self.fail(error)

        self.validate_data_in_datasets()

        self.log.info("Starting backup before upgrade")
        resp = self.columnar_utils.create_backup(
            pod=self.pod, tenant=self.tenant,
            project_id=self.tenant.project_id, instance=self.columnar_cluster)
        if resp is None:
            self.fail("Unable to schedule backup")
        else:
            backup_id_before_upgrade = resp["id"]

        if not self.columnar_utils.wait_for_backup_to_complete(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster,
                backup_id=backup_id_before_upgrade, timeout=7200):
            self.fail("Backup before upgrade failed.")

        # start parallel query here
        # start parallel doc loading

        self.log.info("Scaling-down columnar cluster before upgrade")
        if not self.columnar_utils.scale_instance(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster,
                nodes=self.input.param("num_nodes_in_columnar_instance") // 2):
            self.fail("Unable to initiate cluster scale operation before "
                      "upgrade")

        if not self.columnar_utils.wait_for_instance_scaling_operation(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster):
            self.fail("Scaling operation before upgrade failed")

        self.log.info("Scheduling columnar cluster upgrades")
        upgrade_start_time = datetime.utcnow() + timedelta(minutes=1)
        queue_time = upgrade_start_time + timedelta(minutes=2)
        upgrade_end_time = upgrade_start_time + timedelta(hours=2)
        resp = self.capellaAPI.schedule_cluster_upgrade(
            current_images=[self.columnar_image],
            new_image=self.upgrade_version,
            start_datetime=upgrade_start_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            end_datetime=upgrade_end_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            queue_datetime=queue_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            provider="hostedAWS",
            cluster_ids=[self.columnar_cluster.cluster_id])
        if resp.status_code == 202:
            ungrade_info = resp.json()
        else:
            self.fail(f"Failed to schedule columnar upgrade. Error - "
                      f"{resp.content}")

        self.log.info("Waiting for columnar cluster upgrades to finish")
        if not self.columnar_utils.wait_for_maintenance_job_to_complete(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster,
                maintenance_job_id=ungrade_info["id"], timeout=7200
        ):
            self.fail("Upgrade failed.")

        self.log.info("Validating server version post upgrade")
        rest = ClusterRestAPI(self.columnar_cluster.master)
        status, content = rest.cluster_details()
        if not status:
            self.fail(
                "Error while fetching pools/default using connection string")

        node_versions = set()
        for node_info in content["nodes"]:
            node_versions.add(node_info["version"])

        if len(node_versions) > 1:
            self.fail(f"Post upgrade cluster has nodes with different "
                      f"versions - {node_versions}")
        else:
            upgrade_version = self.upgrade_version.split("-")
            expected_version_post_upgrade = "-".join(
                upgrade_version[2:4] + [upgrade_version[1]])
            actual_version_post_upgrade = node_versions.pop()
            if actual_version_post_upgrade != expected_version_post_upgrade:
                self.fail(f"Incorrect Server version post upgrade. Expected "
                          f"version {expected_version_post_upgrade}, "
                          f"Actual version {actual_version_post_upgrade}")

        status, error = self.cbas_util.perform_metadata_validation_for_all_entities(
            self.columnar_cluster, cbas_entities_before_upgrade)
        if not status:
            self.fail(error)
        # stop data loading, update dataset doc count and validate data
        self.validate_data_in_datasets()

        self.log.info("Creating more columnar entities post upgrade")
        self.columnar_spec["database"]["no_of_databases"] = 1
        self.columnar_spec["dataverse"]["no_of_dataverses"] = 1

        self.columnar_spec["remote_link"]["no_of_remote_links"] = 1
        self.columnar_spec["external_link"]["no_of_external_links"] = 1
        self.columnar_spec["kafka_link"]["no_of_kafka_links"] = 2

        self.columnar_spec["remote_dataset"]["num_of_remote_datasets"] = 1
        self.columnar_spec["external_dataset"]["num_of_external_datasets"] = 5
        self.columnar_spec["standalone_dataset"]["num_of_standalone_coll"] = 1
        self.columnar_spec["kafka_dataset"]["num_of_ds_on_external_db"] = 8

        self.columnar_spec["synonym"]["no_of_synonyms"] = 1
        self.columnar_spec["index"]["no_of_indexes"] = 2

        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            cluster=self.columnar_cluster, cbas_spec=self.columnar_spec,
            bucket_util=self.bucket_util, wait_for_ingestion=False,
            remote_clusters=[self.remote_cluster])

        if not result:
            self.fail(msg)

        self.validate_data_in_datasets()

        self.log.info("Scaling-up columnar cluster after upgrade")
        if not self.columnar_utils.scale_instance(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster,
                nodes=self.input.param("num_nodes_in_columnar_instance")):
            self.fail("Unable to initiate cluster scale operation before "
                      "upgrade")

        if not self.columnar_utils.wait_for_instance_scaling_operation(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster):
            self.fail("Scaling operation before upgrade failed")

        # Disconnecting all kafka and remote links so that the data is
        # persisted before backup.
        self.sleep(60, "Waiting post scaling before disconnecting links.")
        self.log.info("Disconnecting all kafka and remote links before "
                      "backup so that the data is persisted")
        for link in self.cbas_util.get_all_link_objs(
                "couchbase") + self.cbas_util.get_all_link_objs("kafka"):
            if not self.cbas_util.disconnect_link(
                    self.columnar_cluster, link.name):
                self.fail(f"Error while disconnecting link {link.name}")

        self.log.info("Starting backup after upgrade")
        resp = self.columnar_utils.create_backup(
            pod=self.pod, tenant=self.tenant,
            project_id=self.tenant.project_id, instance=self.columnar_cluster)
        if resp is None:
            self.fail("Unable to schedule backup")
        else:
            backup_id_after_upgrade = resp["id"]

        if not self.columnar_utils.wait_for_backup_to_complete(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster,
                backup_id=backup_id_after_upgrade):
            self.fail("Backup before upgrade failed.")

        status, error = self.cbas_util.perform_metadata_validation_for_all_entities(
            self.columnar_cluster)
        if not status:
            self.fail(error)
        # perform ingestion validation

        self.log.info("Restoring backup taken before the upgrade")
        resp = self.columnar_utils.restore_backup(
            pod=self.pod, tenant=self.tenant,
            project_id=self.tenant.project_id, instance=self.columnar_cluster,
            backup_id=backup_id_before_upgrade
        )

        if resp is None:
            self.fail("Unable to start restore")
        else:
            restore_id = resp["id"]

        if not self.columnar_utils.wait_for_restore_to_complete(
                    pod=self.pod, tenant=self.tenant,
                    project_id=self.tenant.project_id,
                    instance=self.columnar_cluster,
                    restore_id=restore_id):
            self.fail("Unable to restore backup taken before the upgrade.")

        if not self.columnar_utils.allow_ip_on_instance(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster):
            self.fail("Unable to set Allowed IP post restoring backup")

        if not self.cbas_util.wait_for_cbas_to_recover(
                self.columnar_cluster, timeout=600):
            self.fail("Columnar cluster unable to recover after restoring "
                      "backup.")

        status, error = self.cbas_util.perform_metadata_validation_for_all_entities(
            self.columnar_cluster, cbas_entities_before_upgrade)
        if not status:
            self.fail(error)

        for dataset, expected_doc_count in (
                doc_count_for_datasets_before_upgrade.items()):
            if not self.cbas_util.wait_for_ingestion_complete(
                    self.columnar_cluster, dataset, expected_doc_count):
                self.fail("Doc count mismatch post restore.")

        self.log.info("Connecting all remote and kafka Links after restore")
        all_links = [link.name for link in self.cbas_util.get_all_link_objs(
            "kafka") + self.cbas_util.get_all_link_objs("couchbase")]
        for link in cbas_entities_before_upgrade["links"]:
            if link in all_links:
                if not self.cbas_util.connect_link(
                        self.columnar_cluster, link):
                    self.fail(f"Failed while connecting link {link}")

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

        status, error = self.cbas_util.perform_metadata_validation_for_all_entities(
            self.columnar_cluster, cbas_entities_before_upgrade)
        if not status:
            self.fail(error)

        for dataset, expected_doc_count in (
                doc_count_for_datasets_before_upgrade.items()):
            actual_doc_count = self.cbas_util.get_num_items_in_cbas_dataset(
                self.columnar_cluster, dataset)
            if actual_doc_count != expected_doc_count:
                self.fail(f"Doc count mismatch post restore. Expected - "
                          f"{expected_doc_count}, Actual - {actual_doc_count}")
