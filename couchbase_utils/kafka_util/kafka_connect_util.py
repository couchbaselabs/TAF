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
            "connector.class": "io.debezium.connector.mongodb.MongoDbConnector",
            "capture.mode": "change_streams_update_full",
            "mongodb.ssl.enabled": "false",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable": "false",
            "key.converter.schemas.enable": "false",
            "key.converter": "org.apache.kafka.connect.json.JsonConverter",
            "offset.flush.interval.ms": "10000",
            "topic.creation.default.partitions": "-1",
            "topic.creation.default.replication.factor": "-1",

            "topic.prefix": "",
            "collection.include.list": "",
            "mongodb.connection.string": "",
        }


class KafkaConnectUtil(object):

    def __init__(self, connect_server_hostname):
        self.api_request = APIRequests(connect_server_hostname, "dummy", "dummy")

        self.connector_endpoint = "/connectors"
        self.connector_plugins = "/connector-plugins"

        self.log = logging.getLogger(__name__)

    def generate_mongo_connector_config(
            self, mongo_connection_str, mongo_collections, topic_prefix,
            src_connector="debezium", partitions=None):
        """
        Method to generate mongo connector configurations
        :param mongo_connection_str <str> Connection string to connect to mongo
        :param mongo_collections <list> List of fully qualified mongo
        collection names i.e. DB_name.Collection_Name
        :param topic_prefix <str> prefix for the topics that will be created
        by the connector
        :param src_connector <str> Name of the source connector provider.
        Currently following are supported - debezium
        """
        if src_connector.lower() == "debezium":
            config = ConnectorConfigTemplate.MongoConfigs.debezium
        config["topic.prefix"] = topic_prefix
        config["mongodb.connection.string"] = mongo_connection_str
        config["collection.include.list"] = ",".join(mongo_collections)
        config["tasks.max"] = len(mongo_collections)
        if partitions:
            config["topic.creation.default.partitions"] = partitions
        return config

    def is_kafka_connect_running(self):
        """
        Method verifies whether kafka connect cluster is running.
        """
        self.log.debug("Checking if Kafka Connect Cluster is running")
        response = self.api_request.api_get("")
        if response.status_code == 200:
            return True
        else:
            raise Exception(
                "Fetching Kafka Connect Cluster info failed. Response Code: {"
                "0}, Error: {1}".format(
                    response.status_code, response.content))

    def list_all_active_connectors(self, get_connector_status=False,
                                   get_connector_info=False):
        """
        Method lists all the active connectors

        param get_connector_status <boolean> Retrieves additional state
        information for each of the connectors returned in the API call.

        param get_connector_info <boolean> Returns metadata of each of the
        connectors such as the configuration, task information, and type of connector

        returns <list/dict> If both flags are false, then it will return list
        of active connector names
        """
        self.log.debug("Fetching all active connectors deployed on Kafka "
                       "Connect Cluster")
        if get_connector_status and get_connector_info:
            query_str = "?expand=status&expand=info"
        elif get_connector_status:
            query_str = "?expand=status"
        elif get_connector_info:
            query_str = "?expand=info"
        response = self.api_request.api_get(
            self.connector_endpoint + query_str)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(
                "Listing active connectors failed. Response Code: {0}, "
                "Error: {1}".format(response.status_code, response.content))

    def create_connector(self, connector_name, connector_config):
        """
        Create a new connector, returning the current connector info if
        successful. Return 409 (Conflict) if rebalance is in process, or if
        the connector already exists.
        param connector_name <str> name of the connector
        param connector_config <dict> config for the connector
        """
        self.log.debug("Creating connector {0} with config {1}".format(
            connector_name, connector_config))
        config = {
            "name": connector_name,
            "config": connector_config
        }
        response = self.api_request.api_post(self.connector_endpoint, config)
        if response.status_code == 201:
            return response.json()
        else:
            raise Exception(
                "Creating connector failed. Response Code: {0}, Error: {"
                "1}".format(response.status_code, response.content))

    def get_connector_info(self, connector_name):
        """
        Get information about the connector.
        param connector_name <str> name of the connector
        """
        self.log.debug("Fetching info for {} connector".format(connector_name))
        response = self.api_request.api_get(
            self.connector_endpoint + "/{0}".format(connector_name))
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(
                "Fetching info for connector {0} failed. Response Code: {1}, "
                "Error: {2}".format(connector_name, response.status_code,
                                    response.content))

    def get_connector_configuration(self, connector_name):
        """
        Get the configuration for the connector.
        param connector_name <str> name of the connector
        """
        self.log.debug("Fetching configuration for {} connector".format(
            connector_name))
        response = self.api_request.api_get(
            self.connector_endpoint + "/{0}/config".format(connector_name))
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(
                "Fetching configuration for connector {0} failed. Response "
                "Code: {1}, Error: {2}".format(
                    connector_name, response.status_code, response.content))

    def update_connector_config(self, connector_name, connector_config):
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
        response = self.api_request.api_put(
            self.connector_endpoint + "/{0}/config".format(connector_name),
            connector_config)
        if response.status_code == 201:
            return response.json()
        else:
            raise Exception(
                "Updating connector config for {0} failed. Response Code: {"
                "1}, Error: {2}".format(
                    connector_name, response.status_code, response.content))

    def get_connector_status(self, connector_name):
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
        response = self.api_request.api_get(
            self.connector_endpoint + "/{0}/status".format(connector_name))
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(
                "Fetching connector status for {0} failed. Response "
                "Code: {1}, Error: {2}".format(
                    connector_name, response.status_code, response.content))

    def list_all_connector_tasks(self, connector_name):
        """
        Get a list of tasks currently running for the connector.
        param connector_name <str> name of the connector
        """
        self.log.debug("Fetching all tasks for {} connector".format(
            connector_name))
        response = self.api_request.api_get(
            self.connector_endpoint + "/{0}/tasks".format(connector_name))
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(
                "Listing connector tasks for {0} failed. Response "
                "Code: {1}, Error: {2}".format(
                    connector_name, response.status_code, response.content))

    def list_all_connector_topics(self, connector_name):
        """
        The set of topic names the connector has been using since its creation
        or since the last time its set of active topics was reset.
        param connector_name <str> name of the connector
        """
        self.log.debug("Fetching all topics for {} connector".format(
            connector_name))
        response = self.api_request.api_get(
            self.connector_endpoint + "/{0}/topics".format(connector_name))
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(
                "Listing connector topics for {0} failed. Response "
                "Code: {1}, Error: {2}".format(
                    connector_name, response.status_code, response.content))

    def delete_connector(self, connector_name):
        """
        Delete a connector, halting all tasks and deleting its configuration.
        Return 409 (Conflict) if rebalance is in process.
        param connector_name <str> name of the connector
        """
        self.log.debug("Deleting {} connector".format(
            connector_name))
        response = self.api_request.api_del(
            self.connector_endpoint + "/{0}/".format(connector_name))
        if response.status_code == 204:
            return True
        else:
            raise Exception(
                "Deleting connector {0} failed. Response "
                "Code: {1}, Error: {2}".format(
                    connector_name, response.status_code, response.content))

    def list_all_connector_plugins_installed(self):
        response = self.api_request.api_get(self.connector_plugins)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(
                "Listing installed connector pluging failed. Response "
                "Code: {0}, Error: {1}".format(
                    response.status_code, response.content))

    def deploy_connector(self, connector_name, connector_config):
        """
        Deploys a connector on Kafka connect cluster.
        """
        try:
            self.is_kafka_connect_running()
            response = self.create_connector(
                connector_name, connector_config)
            time.sleep(10)
            if not response:
                self.log.error("Unable to deploy connectors")
                return False
            connector_status = (
                self.get_connector_status(connector_name))
            while connector_status["connector"]["state"] != "RUNNING":
                if connector_status["connector"]["state"] == "FAILED":
                    raise Exception("Connector failed to deploy, current "
                                    "state is FAILED")
                self.log.info(
                    "Connector is in {0} state, waiting for it to be in "
                    "RUNNING state".format(
                        connector_status["connector"]["state"]))
                time.sleep(10)
                connector_status = self.get_connector_status(connector_name)
            return True
        except Exception as err:
            self.log.error(str(err))
            return False
