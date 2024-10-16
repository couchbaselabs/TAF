"""
Created on 3-May-2024

@author: Umang Agrawal

This utility is for making rest calls to Kafka Connect server in order to
deploy/undeploy connectors
"""
from capellaAPI.capella.lib.APIRequests import APIRequests
import logging
import time


class ConnectorConfigTemplate(object):

    class MongoConfigs:
        """
        topic.prefix - prefix that will be added to topic name.

        collection.include.list - a comma-separated list of collection
        names. Collection name should be in DB_name.Collection_name format.
        A topic with name of format topic_prefix.DB_name.Collection_name
        will be created for each mongo collection specified in the list.

        mongodb.connection.string - connection string for mongo DB from
        where the data is to be fetched
        """
        debezium = {
            "cdc": {
                "connector.class": "io.debezium.connector.mongodb.MongoDbConnector",
                "capture.mode": "change_streams_update_full",
                "mongodb.ssl.enabled": "false",
                "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter.schemas.enable": "false",
                "key.converter.schemas.enable": "false",
                "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                "offset.flush.interval.ms": "10000",
                "topic.creation.default.partitions": "32",
                "topic.creation.default.replication.factor": "-1",

                "topic.prefix": "",
                "collection.include.list": "",
                "mongodb.connection.string": "",
            },
            "non-cdc": {
                "connector.class": "io.debezium.connector.mongodb.MongoDbConnector",
                "capture.mode": "change_streams_update_full",
                "mongodb.ssl.enabled": "false",
                "value.converter": "org.apache.kafka.connect.storage.StringConverter",
                "value.converter.schemas.enable": "false",
                "key.converter": "org.apache.kafka.connect.storage.StringConverter",
                "key.converter.schemas.enable": "false",
                "offset.flush.interval.ms": "10000",
                "topic.creation.default.partitions": "32",
                "topic.creation.default.replication.factor": "-1",

                "transforms": "ExtractDocument,ExtractId,ConvertToJson",
                "transforms.ConvertToJson.type": "io.debezium.connector.mongodb.smt.BsonToJsonConverter",
                "transforms.ExtractId.type": "org.apache.kafka.connect.transforms.ExtractField$Key",
                "transforms.ExtractId.field": "id",
                "transforms.ExtractDocument.type": "org.apache.kafka.connect.transforms.ExtractField$Value",
                "transforms.ExtractDocument.field": "after",

                "mongodb.connection.string": "",
                "collection.include.list": "",
                "topic.prefix": "",
            }
        }


class KafkaConnectUtil(object):

    CONFLUENT_NON_CDC_PORT = 8082
    CONFLUENT_CDC_PORT = 8083
    AWS_MSK_CDC_PORT = 8084
    AWS_MSK_NON_CDC_PORT = 8085

    def __init__(self):
        self.connector_endpoint = "/connectors"
        self.connector_plugins = "/connector-plugins"

        self.log = logging.getLogger(__name__)

    @staticmethod
    def generate_mongo_connector_config(
            mongo_connection_str, mongo_collections, topic_prefix,
            src_connector="debezium", partitions=None, cdc_enabled=False,
            serialization_type="JSON", schema_registry_url=None,
            schema_registry_access_key=None,
            schema_registry_secret_access_key=None):
        """
        Method to generate mongo connector configurations
        :param mongo_connection_str <str> Connection string to connect to mongo
        :param mongo_collections <list> List of fully qualified mongo
        collection names i.e. DB_name.Collection_Name
        :param topic_prefix <str> prefix for the topics that will be created
        by the connector
        :param src_connector <str> Name of the source connector provider.
        Currently following are supported - debezium
        :param partitions <int> Number of partition per topic.
        :param cdc_enabled <bool> If True, then generate connector config
        to stream cdc data, otherwise generate config to stream non-cdc data.
        :param serialization_type <str> Supported values JSON, PROTOBUF, AVRO
        :param schema_registry_url <str> URL for the schema registry
        :param schema_registry_access_key <str> Access key for the schema
        registry
        :param schema_registry_secret_access_key <str> Secret Access key for
        the schema registry
        """
        if src_connector.lower() == "debezium":
            if cdc_enabled:
                config = ConnectorConfigTemplate.MongoConfigs.debezium["cdc"]
            else:
                config = ConnectorConfigTemplate.MongoConfigs.debezium[
                    "non-cdc"]
        config["topic.prefix"] = topic_prefix
        config["mongodb.connection.string"] = mongo_connection_str
        config["collection.include.list"] = ",".join(mongo_collections)
        config["tasks.max"] = len(mongo_collections)
        if partitions:
            config["topic.creation.default.partitions"] = partitions

        if serialization_type in ["AVRO", "PROTOBUF"]:
            if (not schema_registry_url or not schema_registry_access_key or
                    not schema_registry_secret_access_key):
                raise Exception(
                    f"Need schema registry's URL, access and secret access "
                    f"key for serialization type {serialization_type}")
            else:
                if serialization_type == "AVRO":
                    config["value.converter"] = "io.confluent.connect.avro.AvroConverter"
                    config["key.converter"] = "io.confluent.connect.avro.AvroConverter"
                elif serialization_type == "PROTOBUF":
                    config["value.converter"] = "io.confluent.connect.protobuf.ProtobufConverter"
                    config["key.converter"] = "io.confluent.connect.protobuf.ProtobufConverter"

        if schema_registry_url:
            config["key.converter.schema.registry.url"] = schema_registry_url
            config["value.converter.schema.registry.url"] = schema_registry_url
            config["key.converter.basic.auth.credentials.source"] = "USER_INFO"
            config[
                "value.converter.basic.auth.credentials.source"] = "USER_INFO"
            config["key.converter.schema.registry.basic.auth.user.info"] = (
                    schema_registry_access_key + ":" +
                    schema_registry_secret_access_key)
            config["value.converter.schema.registry.basic.auth.user.info"] = (
                    schema_registry_access_key + ":" +
                    schema_registry_secret_access_key)

        return config

    def format_connect_cluster_hostname(self, connect_cluster_hostname):
        return f"http://{connect_cluster_hostname}"

    def is_kafka_connect_running(self, connect_server_hostname):
        """
        Method verifies whether kafka connect cluster is running.
        :param connect_server_hostname: <str> Format ip:port
        """
        self.log.debug("Checking if Kafka Connect Cluster is running")
        api_request = APIRequests(
            self.format_connect_cluster_hostname(connect_server_hostname),
            "dummy", "dummy")
        response = api_request.api_get("")
        if response.status_code == 200:
            return True
        else:
            raise Exception(
                "Fetching Kafka Connect Cluster info failed. Response Code: {"
                "0}, Error: {1}".format(
                    response.status_code, response.content))

    def list_all_active_connectors(
            self, connect_server_hostname, get_connector_status=False,
            get_connector_info=False):
        """
        Method lists all the active connectors

        :param connect_server_hostname: <str> Format ip:port

        :param get_connector_status: <boolean> Retrieves additional state
        information for each of the connectors returned in the API call.

        :param get_connector_info: <boolean> Returns metadata of each of the
        connectors such as the configuration, task information, and type of connector

        returns <list/dict> If both flags are false, then it will return list
        of active connector names
        """
        self.log.debug("Fetching all active connectors deployed on Kafka "
                       "Connect Cluster")
        api_request = APIRequests(
            self.format_connect_cluster_hostname(connect_server_hostname),
            "dummy", "dummy")
        if get_connector_status and get_connector_info:
            query_str = "?expand=status&expand=info"
        elif get_connector_status:
            query_str = "?expand=status"
        elif get_connector_info:
            query_str = "?expand=info"
        else:
            query_str = ""
        response = api_request.api_get(
            self.connector_endpoint + query_str)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(
                "Listing active connectors failed. Response Code: {0}, "
                "Error: {1}".format(response.status_code, response.content))

    def create_connector(self, connect_server_hostname, connector_name,
                         connector_config):
        """
        Create a new connector, returning the current connector info if
        successful. Return 409 (Conflict) if rebalance is in process, or if
        the connector already exists.
        param connect_server_hostname <str> Format ip:port
        param connector_name <str> name of the connector
        param connector_config <dict> config for the connector
        """
        self.log.debug("Creating connector {0} with config {1}".format(
            connector_name, connector_config))
        api_request = APIRequests(
            self.format_connect_cluster_hostname(connect_server_hostname),
            "dummy", "dummy")
        config = {
            "name": connector_name,
            "config": connector_config
        }
        response = api_request.api_post(self.connector_endpoint, config)
        if response.status_code == 201:
            return response.json()
        else:
            raise Exception(
                "Creating connector failed. Response Code: {0}, Error: {"
                "1}".format(response.status_code, response.content))

    def get_connector_info(self, connect_server_hostname, connector_name):
        """
        Get information about the connector.
        param connector_name <str> name of the connector
        """
        self.log.debug("Fetching info for {} connector".format(connector_name))
        api_request = APIRequests(
            self.format_connect_cluster_hostname(connect_server_hostname),
            "dummy", "dummy")
        response = api_request.api_get(
            self.connector_endpoint + "/{0}".format(connector_name))
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(
                "Fetching info for connector {0} failed. Response Code: {1}, "
                "Error: {2}".format(connector_name, response.status_code,
                                    response.content))

    def get_connector_configuration(self, connect_server_hostname,
                                    connector_name):
        """
        Get the configuration for the connector.
        param connector_name <str> name of the connector
        """
        self.log.debug("Fetching configuration for {} connector".format(
            connector_name))
        api_request = APIRequests(
            self.format_connect_cluster_hostname(connect_server_hostname),
            "dummy", "dummy")
        response = api_request.api_get(
            self.connector_endpoint + "/{0}/config".format(connector_name))
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(
                "Fetching configuration for connector {0} failed. Response "
                "Code: {1}, Error: {2}".format(
                    connector_name, response.status_code, response.content))

    def update_connector_config(self, connect_server_hostname,
                                connector_name, connector_config):
        """
        Create a new connector using the given configuration, or update the
        configuration for an existing connector. Returns information about
        the connector after the change has been made. Return 409 (Conflict)
        if rebalance is in process.
        param connector_name <str> name of the connector
        param connector_config <dict> config for the connector
        """
        self.log.debug("Updating connector {0} with config {1}".format(
            connector_name, connector_config))
        api_request = APIRequests(
            self.format_connect_cluster_hostname(connect_server_hostname),
            "dummy", "dummy")
        response = api_request.api_put(
            self.connector_endpoint + "/{0}/config".format(connector_name),
            connector_config)
        if response.status_code == 201:
            return response.json()
        else:
            raise Exception(
                "Updating connector config for {0} failed. Response Code: {"
                "1}, Error: {2}".format(
                    connector_name, response.status_code, response.content))

    def get_connector_status(self, connect_server_hostname, connector_name):
        """
        Gets the current status of the connector, including:
        1. Whether it is running or restarting, or if it has failed or paused
        2. Which worker it is assigned to
        3. Error information if it has failed
        4. The state of all its tasks

        param connector_name <str> name of the connector
        """
        self.log.debug("Fetching status for {} connector".format(
            connector_name))
        api_request = APIRequests(
            self.format_connect_cluster_hostname(connect_server_hostname),
            "dummy", "dummy")
        response = api_request.api_get(
            self.connector_endpoint + "/{0}/status".format(connector_name))
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(
                "Fetching connector status for {0} failed. Response "
                "Code: {1}, Error: {2}".format(
                    connector_name, response.status_code, response.content))

    def list_all_connector_tasks(self, connect_server_hostname,
                                 connector_name):
        """
        Get a list of tasks currently running for the connector.
        param connector_name <str> name of the connector
        """
        self.log.debug("Fetching all tasks for {} connector".format(
            connector_name))
        api_request = APIRequests(
            self.format_connect_cluster_hostname(connect_server_hostname),
            "dummy", "dummy")
        response = api_request.api_get(
            self.connector_endpoint + "/{0}/tasks".format(connector_name))
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(
                "Listing connector tasks for {0} failed. Response "
                "Code: {1}, Error: {2}".format(
                    connector_name, response.status_code, response.content))

    def list_all_connector_topics(self, connect_server_hostname,
                                  connector_name):
        """
        The set of topic names the connector has been using since its creation
        or since the last time its set of active topics was reset.
        param connector_name <str> name of the connector
        """
        self.log.debug("Fetching all topics for {} connector".format(
            connector_name))
        api_request = APIRequests(
            self.format_connect_cluster_hostname(connect_server_hostname),
            "dummy", "dummy")
        response = api_request.api_get(
            self.connector_endpoint + "/{0}/topics".format(connector_name))
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(
                "Listing connector topics for {0} failed. Response "
                "Code: {1}, Error: {2}".format(
                    connector_name, response.status_code, response.content))

    def delete_connector(self, connect_server_hostname, connector_name):
        """
        Delete a connector, halting all tasks and deleting its configuration.
        Return 409 (Conflict) if rebalance is in process.
        param connector_name <str> name of the connector
        """
        self.log.debug("Deleting {} connector".format(
            connector_name))
        api_request = APIRequests(
            self.format_connect_cluster_hostname(connect_server_hostname),
            "dummy", "dummy")
        response = api_request.api_del(
            self.connector_endpoint + "/{0}/".format(connector_name))
        if response.status_code == 204:
            return True
        else:
            raise Exception(
                "Deleting connector {0} failed. Response "
                "Code: {1}, Error: {2}".format(
                    connector_name, response.status_code, response.content))

    def list_all_connector_plugins_installed(self, connect_server_hostname):
        api_request = APIRequests(
            self.format_connect_cluster_hostname(connect_server_hostname),
            "dummy", "dummy")
        response = api_request.api_get(self.connector_plugins)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(
                "Listing installed connector pluging failed. Response "
                "Code: {0}, Error: {1}".format(
                    response.status_code, response.content))

    # Do not use this method directly, instead use the ones available in
    # confluent or msk util.
    def deploy_connector(self, connect_server_hostname, connector_name,
                         connector_config):
        """
        Deploys a connector on Kafka connect cluster.
        """
        try:
            self.is_kafka_connect_running(connect_server_hostname)
            response = self.create_connector(
                connect_server_hostname, connector_name, connector_config)
            if not response:
                self.log.error("Unable to deploy connectors")
                return False
            time.sleep(10)
            connector_status = self.get_connector_status(
                connect_server_hostname, connector_name)
            while connector_status["connector"]["state"] != "RUNNING":
                if connector_status["connector"]["state"] == "FAILED":
                    raise Exception("Connector failed to deploy, current "
                                    "state is FAILED")
                self.log.info(
                    "Connector is in {0} state, waiting for it to be in "
                    "RUNNING state".format(
                        connector_status["connector"]["state"]))
                time.sleep(10)
                connector_status = self.get_connector_status(
                    connect_server_hostname, connector_name)
            return True
        except Exception as err:
            self.log.error(str(err))
            return False
