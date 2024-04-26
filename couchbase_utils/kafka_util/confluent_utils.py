# -*- coding: utf-8 -*-
# Generic/Built-in
"""
Created on 22-April-2024

@author: Umang Agrawal

This utility is for performing actions on Confluent Managed Kafka and
confluent connector server.
"""

import requests
import base64
from requests.auth import AuthBase
import logging
import time
from capellaAPI.capella.lib.CapellaExceptions import (
    CbcAPIError, MissingAccessKeyError, MissingSecretKeyError,
    GenericHTTPError)
from global_vars import logger

CONFLUENT_CLOUD_BASE_URL = "https://api.confluent.cloud"


class ConfluentKafkaCluster(object):
    """
    Object for storing confluent kafka information
    """

    def __init__(self):
        self.cloud_access_key = None
        self.cloud_secret_key = None

        # Environment properties
        self.environment_name = None
        self.environment_id = None

        # Kafka Cluster properties
        self.name = None
        self.id = None
        self.provider = None
        self.region = None
        self.cluster_type = None
        self.availability = None
        self.http_endpoint = None
        self.bootstrap_server = None

        # Cluster API keys
        self.cluster_access_key = None
        self.cluster_secret_key = None

        # Topic Details
        self.topic_prefix = None
        self.topics = list()
        self.internal_topics = ["connect-configs", "connect-offsets",
                                "connect-status"]
        # Dead letter queue topic name
        self.dlq_topic = None

        # Connector Info
        self.connectors = {}


class EnvironmentStreamGovernanceConfig(object):
    ESSENTIALS = "ESSENTIALS"
    ADVANCED = "ADVANCED"


class KafkaClusterConfigConstants(object):

    class Availability:
        MULTI_ZONE = "MULTI_ZONE"
        SINGLE_ZONE = "SINGLE_ZONE"

    class Provider:
        AWS = "AWS"
        GCP = "GCP"
        AZURE = "AZURE"

    class ClusterType:
        BASIC = "Basic"
        STANDARD = "Standard"
        DEDICATED = "Dedicated"
        ENTERPRISE = "Enterprise"


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


class APIAuth(AuthBase):
    # Extends requests AuthBase for API Authentication Handler.

    def __init__(self, access, secret):
        # Create an authentication handler for API calls
        # :param str access_key: access key for API calls
        # :param str secret_key: secret key for API calls

        self.ACCESS_KEY = access
        self.SECRET_KEY = secret

    def __call__(self, r):
        # Values for the header
        base64_encoded_auth = base64.b64encode("{0}:{1}".format(
            self.ACCESS_KEY, self.SECRET_KEY))
        api_request_headers = {
            'Authorization': 'Basic ' + base64_encoded_auth,
            'Content-Type': 'application/json'
        }
        # Add our key:values to the request header
        r.headers.update(api_request_headers)

        # Return the request back
        return r


class APIRequests(object):

    def __init__(self, access=None, secret=None):
        # handles http requests - GET , PUT, POST, DELETE
        self.SECRET = secret
        self.ACCESS = access

        self._log = logging.getLogger(__name__)

        # We will re-use the first session we setup to avoid
        # the overhead of creating new sessions for each request
        self.network_session = requests.Session()

    def api_get(self, url, params=None, headers=None):
        api_response = None
        self._log.info(url)

        try:
            api_response = self.network_session.get(
                url,
                auth=APIAuth(self.ACCESS, self.SECRET),
                params=params, verify=False, headers=headers)
            self._log.info(api_response.content)

        except requests.exceptions.HTTPError:
            raise GenericHTTPError(api_response.json())

        except MissingAccessKeyError:
            self._log.debug("Missing Access Key environment variable")
            print("Missing Access Key environment variable")

        except MissingSecretKeyError:
            self._log.debug("Missing Access Key environment variable")
            print("Missing Access Key environment variable")

        # Grab any other exception and send to our generic exception
        # handler
        except Exception as e:
            raise CbcAPIError(e)

        return api_response

    def api_post(self, url, request_body, headers=None):
        api_response = None

        self._log.info(url)
        self._log.debug("Request body: " + str(request_body))

        try:
            api_response = self.network_session.post(
                url, json=request_body,
                auth=APIAuth(self.ACCESS, self.SECRET),
                verify=False, headers=headers)
            self._log.debug(api_response.content)

        except requests.exceptions.HTTPError:
            raise GenericHTTPError(api_response.json())

        except MissingAccessKeyError:
            print("Missing Access Key environment variable")

        except MissingSecretKeyError:
            print("Missing Access Key environment variable")

        # Grab any other exception and send to our generic exception
        # handler
        except Exception as e:
            raise CbcAPIError(e)

        return api_response

    def api_put(self, url, request_body, headers=None):
        api_response = None

        self._log.info(url)
        self._log.debug("Request body: " + str(request_body))

        try:
            api_response = self.network_session.put(
                url, json=request_body,
                auth=APIAuth(self.ACCESS, self.SECRET),
                verify=False, headers=headers)
            self._log.debug(api_response.content)

        except requests.exceptions.HTTPError:
            raise GenericHTTPError(api_response.json())

        except MissingAccessKeyError:
            print("Missing Access Key environment variable")

        except MissingSecretKeyError:
            print("Missing Access Key environment variable")

        return api_response

    def api_patch(self, url, request_body, headers=None):
        api_response = None

        self._log.info(url)
        self._log.debug("Request body: " + str(request_body))

        try:
            api_response = self.network_session.patch(
                url, json=request_body,
                auth=APIAuth(self.ACCESS, self.SECRET),
                verify=False, headers=headers)
            self._log.debug(api_response.content)

        except requests.exceptions.HTTPError:
            raise GenericHTTPError(api_response.json())

        except MissingAccessKeyError:
            print("Missing Access Key environment variable")

        except MissingSecretKeyError:
            print("Missing Access Key environment variable")

        return api_response

    def api_del(self, url, request_body=None, params=None, headers=None):
        api_response = None

        self._log.info(url)
        self._log.debug("Request body: " + str(request_body))

        try:
            if request_body:
                api_response = self.network_session.delete(
                    url, json=request_body,
                    auth=APIAuth(self.ACCESS, self.SECRET),
                    verify=False, headers=headers)
            elif params:
                api_response = self.network_session.delete(
                    url, params=params,
                    auth=APIAuth(self.ACCESS, self.SECRET),
                    verify=False, headers=headers)
            else:
                api_response = self.network_session.delete(
                    url,
                    auth=APIAuth(self.ACCESS, self.SECRET),
                    verify=False, headers=headers)

            self._log.debug(api_response.content)

        except requests.exceptions.HTTPError:
            raise GenericHTTPError(api_response.json())

        except MissingAccessKeyError:
            print("Missing Access Key environment variable")

        except MissingSecretKeyError:
            print("Missing Access Key environment variable")

        # Grab any other exception and send to our generic exception
        # handler
        except Exception as e:
            raise CbcAPIError(e)

        return api_response


class ConfluentCloudAPIs(object):
    """
    This class contains collection of API to access confluent cloud.
    For more details on confluent APIs visit below page-
        https://docs.confluent.io/cloud/current/api.html
    """

    def __init__(self, access=None, secret=None):
        self.api_request = APIRequests(access, secret)

        # V2 Endpoints
        self.api_key_endpoint = CONFLUENT_CLOUD_BASE_URL + "/iam/v2/api-keys"
        self.user_endpoint = CONFLUENT_CLOUD_BASE_URL + "/iam/v2/users"
        self.environment_endpoint = (CONFLUENT_CLOUD_BASE_URL +
                                     "/org/v2/environments")
        self.kafka_cluster_endpoint = CONFLUENT_CLOUD_BASE_URL + "/cmk/v2/clusters"
        self.region_endpoint = CONFLUENT_CLOUD_BASE_URL + "/srcm/v2/regions"

    def parse_error(self, response):
        errors = response.json()["errors"]
        error_log = ""
        for error in errors:
            error_log += error["detail"]
        return error_log

    def set_authentication_keys(self, access, secret):
        self.api_request.ACCESS = access
        self.api_request.SECRET = secret

    # Methods for Confluent API Key
    def list_all_api_keys(self, page_token=None, owner_filter=None,
                          resource_filter=None):
        """
        Method lists all the API keys.
        """
        url = self.api_key_endpoint
        query_params = {"page_size": 100}
        if page_token:
            query_params["page_token"] = page_token
        if owner_filter:
            query_params["spec.owner"] = owner_filter
        if resource_filter:
            query_params["spec.resource"] = resource_filter
        response = self.api_request.api_get(url, params=query_params)
        api_key_list = list()
        if response.status_code == 200:
            parsed_response = response.json()
            api_key_list.extend(parsed_response["data"])
            while len(api_key_list) < parsed_response["metadata"][
                "total_size"]:
                page_token = parsed_response["metadata"]["next"].split("=")[1]
                api_key_list.extend(self.list_all_api_keys(
                    page_token, owner_filter, resource_filter))
            return api_key_list
        else:
            raise Exception(
                "Following errors occurred while listing api keys - "
                "{0}".format(self.parse_error(response)))

    def get_api_key_info(self, api_key_id):
        """
        Method gets info for a api key id
        """
        url = self.api_key_endpoint + "/{0}".format(api_key_id)
        response = self.api_request.api_get(url)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(
                "Following errors occurred while fetching api key {0} - "
                "{1}".format(api_key_id, self.parse_error(response)))

    def create_api_key(self, owner_id, name=None, description=None,
                       resource_id=None, environment_id=None):
        """
        Method to create a new api key.
        If resource is None then cloud level API key is created.
        """
        url = self.api_key_endpoint
        payload = {
            "spec": {
                "owner": {
                    "id": owner_id
                }
            }
        }
        if name:
            payload["spec"]["display_name"] = name
        if description:
            payload["spec"]["description"] = description
        if resource_id:
            payload["spec"]["resource"] = {
                    "id": resource_id,
                    "environment": environment_id
                }
        response = self.api_request.api_post(url, payload)
        if response.status_code == 202:
            return response.json()
        else:
            raise Exception(
                "Following errors occurred while creating api key - "
                "{0}".format(self.parse_error(response)))

    def delete_api_key(self, api_key_id):
        """
        Method to delete an API key.
        """
        url = self.api_key_endpoint + "/{0}".format(api_key_id)
        response = self.api_request.api_del(url)
        if response.status_code == 204:
            return True
        else:
            raise Exception(
                "Following errors occurred while deleting api key {0} - "
                "{1}".format(api_key_id, self.parse_error(response)))

    # Method for confluent Users
    def list_all_users(self, page_token=None):
        """
        Method to list all the users
        """
        url = self.user_endpoint
        query_params = {"page_size": 100}
        if page_token:
            query_params["page_token"] = page_token

        response = self.api_request.api_get(url, params=query_params)
        user_list = list()
        if response.status_code == 200:
            parsed_response = response.json()
            user_list.extend(parsed_response["data"])
            while len(user_list) < parsed_response["metadata"][
                "total_size"]:
                page_token = parsed_response["metadata"]["next"].split("=")[1]
                user_list.extend(self.list_all_api_keys(page_token))
            return user_list
        else:
            raise Exception("Following errors occurred while listing all "
                            "users - {0}".format(self.parse_error(response)))

    # Methods for Confluent Environment
    def list_all_environments(self, page_token=None):
        """
        Method to list all the environments.
        """
        url = self.environment_endpoint
        query_params = {"page_size": 100}
        if page_token:
            query_params["page_token"] = page_token

        response = self.api_request.api_get(url, params=query_params)
        env_list = list()
        if response.status_code == 200:
            parsed_response = response.json()
            env_list.extend(parsed_response["data"])
            while len(env_list) < parsed_response["metadata"][
                "total_size"]:
                page_token = parsed_response["metadata"]["next"].split("=")[1]
                env_list.extend(self.list_all_api_keys(page_token))
            return env_list
        else:
            raise Exception("Following errors occurred while listing all "
                            "environments - {0}".format(
                self.parse_error(response)))

    def get_environment_info(self, environment_id):
        """
        Method gets info for an environment id
        """
        url = self.environment_endpoint + "/{0}".format(environment_id)
        response = self.api_request.api_get(url)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(
                "Following errors occurred while fetching environment {0} - "
                "{1}".format(environment_id, self.parse_error(response)))

    def create_environment(self, env_name,
                           package=EnvironmentStreamGovernanceConfig.ESSENTIALS):
        """
        Method to create an environment
        """
        url = self.environment_endpoint
        payload = {
            "display_name": env_name,
            "stream_governance_config": {
                "package": package
            }}
        response = self.api_request.api_post(url, payload)
        if response.status_code == 201:
            return response.json()
        else:
            raise Exception("Following errors occurred while creating "
                            "environments - {0}".format(
                self.parse_error(response)))

    def delete_environment(self, environment_id):
        """
        Method to delete an environment
        """
        url = self.environment_endpoint + "/{0}".format(environment_id)
        response = self.api_request.api_del(url)
        if response.status_code == 204:
            return True
        else:
            raise Exception("Following errors occurred while deleting "
                            "environments - {0}".format(
                self.parse_error(response)))

    # Methods for Regions
    def list_all_regions(self, cloud_provider=None, page_token=None):
        """
        Method to list all the regions supported for cloud providers.
        param cloud_provider <str> Accepted values AWS, GCP, AZURE
        """
        url = self.region_endpoint
        query_params = {"page_size": 100}
        if page_token:
            query_params["page_token"] = page_token
        if cloud_provider:
            query_params["spec.cloud"] = cloud_provider

        response = self.api_request.api_get(url, params=query_params)
        region_list = list()
        if response.status_code == 200:
            parsed_response = response.json()
            region_list.extend(parsed_response["data"])
            while len(region_list) < parsed_response["metadata"][
                "total_size"]:
                page_token = parsed_response["metadata"]["next"].split("=")[1]
                region_list.extend(self.list_all_api_keys(page_token))
            return region_list
        else:
            raise Exception("Following errors occurred while listing all "
                            "environments - {0}".format(
                self.parse_error(response)))

    # Methods for Confluent cluster
    def list_all_kafka_clusters(self, environment_id, page_token=None):
        """
        Method to list all the kafka clusters in an environment
        """
        url = self.kafka_cluster_endpoint
        query_params = {"page_size": 100, "environment": environment_id}
        if page_token:
            query_params["page_token"] = page_token

        response = self.api_request.api_get(url, params=query_params)
        kafka_cluster_list = list()
        if response.status_code == 200:
            parsed_response = response.json()
            kafka_cluster_list.extend(parsed_response["data"])
            while len(kafka_cluster_list) < parsed_response["metadata"][
                "total_size"]:
                page_token = parsed_response["metadata"]["next"].split("=")[1]
                kafka_cluster_list.extend(self.list_all_api_keys(page_token))
            return kafka_cluster_list
        else:
            raise Exception("Following errors occurred while listing all "
                            "cluster - {0}".format(
                self.parse_error(response)))

    def get_kafka_cluster_info(self, cluster_id, environment_id):
        """
        Method to get info for a cluster in a given environment
        """
        url = self.kafka_cluster_endpoint + "/{0}".format(cluster_id)
        query_param = {"environment": environment_id}
        response = self.api_request.api_get(url, query_param)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(
                "Following errors occurred while fetching cluster {0} - "
                "{1}".format(cluster_id, self.parse_error(response)))

    def create_kafka_cluster(self, cluster_name, availability, provider,
                             region, cluster_type, environment_id):
        """
        Method to create kafka cluster
        """
        url = self.kafka_cluster_endpoint
        payload = {
            "spec": {
                "display_name": cluster_name,
                "availability": availability,
                "cloud": provider,
                "region": region,
                "config": {
                    "kind": cluster_type},
                "environment": {
                    "id": environment_id,
                    "environment": self.get_environment_info(
                        environment_id)["display_name"]}
            }
        }
        response = self.api_request.api_post(url, payload)
        if response.status_code == 202:
            return response.json()
        else:
            raise Exception(
                "Following errors occurred while creating kafka cluster {0} - "
                "{1}".format(cluster_name, self.parse_error(response)))

    def delete_kafka_cluster(self, cluster_id, environment_id):
        """
        Method to delete Kafka cluster
        """
        url = self.kafka_cluster_endpoint + "/{0}".format(cluster_id)
        query_param = {"environment": environment_id}
        response = self.api_request.api_del(url=url, params=query_param)
        if response.status_code == 204:
            return True
        else:
            raise Exception(
                "Following errors occurred while deleting cluster {0} - "
                "{1}".format(cluster_id, self.parse_error(response)))


class ConfluentKafkaAPIs(object):
    """
    This class contains collection of API to access confluent kafka cluster.
    For more details on confluent APIs visit below page-
        https://docs.confluent.io/cloud/current/api.html
    """

    def __init__(self, access=None, secret=None):
        self.api_request = APIRequests(access, secret)

        # V3 Endpoints
        self.topic_endpoint = "/kafka/v3/clusters/{0}/topics"
        self.partition_endpoint = ("/kafka/v3/clusters/{0}/topics/{"
                                   "1}/partitions")

    def set_authentication_keys(self, access, secret):
        self.api_request.ACCESS = access
        self.api_request.SECRET = secret

    # Method for topic partitions
    def list_all_partitions(
            self, cluster_http_endpoint, cluster_id, topic_name):
        """
        Method to list all the partitions for a topic
        """
        url = cluster_http_endpoint + self.partition_endpoint.format(
            cluster_id, topic_name)
        response = self.api_request.api_get(url)
        if response.status_code == 200:
            return response.json()["data"]
        else:
            raise Exception(
                "Following error occurred while listing all partitions for "
                "topic {0} - {1}".format(
                    topic_name, response.json()["message"]))

    def get_partition_info(self, cluster_http_endpoint, cluster_id,
                           topic_name, partition_id):
        """
        Method to get the partition info.
        """
        url = cluster_http_endpoint + self.partition_endpoint.format(
            cluster_id, topic_name) + "/{0}".format(partition_id)
        response = self.api_request.api_get(url)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(
                "Following error occurred while fetching partition {0} - "
                "{1}".format(partition_id, response.json()["message"]))

    # Method for topics
    def list_all_topics(self, cluster_http_endpoint, cluster_id):
        """
        Method to list all the topics for a kafka cluster
        """
        url = cluster_http_endpoint + self.topic_endpoint.format(
            cluster_id)
        response = self.api_request.api_get(url)
        if response.status_code == 200:
            return response.json()["data"]
        else:
            raise Exception(
                "Following error occurred while listing all topics - "
                "{0}".format(response.json()["message"]))

    def get_topic_info(self, cluster_http_endpoint, cluster_id,
                       topic_name):
        """
        Method to get info on a topic of a kafka cluster
        """
        url = (cluster_http_endpoint + self.topic_endpoint.format(
            cluster_id)
               + "/{0}".format(topic_name))
        response = self.api_request.api_get(url)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(
                "Following error occurred while fetching topic {0} - "
                "{1}".format(topic_name, response.json()["message"]))

    def create_topic(self, cluster_http_endpoint, cluster_id, topic_name,
                     configs, partitions_count=0, replication_factor=0,
                     validate_only=False):
        """
        Method to create topic in a Kafka cluster
        """
        url = cluster_http_endpoint + self.topic_endpoint.format(
            cluster_id)
        payload = {
            "topic_name": topic_name,
            "configs": configs
        }
        if partitions_count:
            payload["partitions_count"] = partitions_count
        if replication_factor:
            payload["replication_factor"] = replication_factor
        if validate_only:
            payload["validate_only"] = validate_only
        response = self.api_request.api_post(url, payload)
        if response.status_code in [200, 201]:
            return response.json()
        else:
            raise Exception(
                "Following error occurred while creating topic {0} - "
                "{1}".format(topic_name, response.json()["message"]))

    def update_partition_count_for_topic(
            self, cluster_http_endpoint, cluster_id, topic_name,
            partition_count):
        """
        Method to update the partition count of a topic in kafka cluster
        """
        url = (cluster_http_endpoint + self.topic_endpoint.format(
            cluster_id)
               + "/{0}".format(topic_name))
        payload = {"partitions_count": partition_count}
        response = self.api_request.api_patch(url, payload)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(
                "Following error occurred while updating topic partition "
                "count {0} - {1}".format(
                    topic_name, response.json()["message"]))

    def delete_topic(self, cluster_http_endpoint, cluster_id, topic_name):
        """
        Method to delete a topic in kafka cluster
        """
        url = (cluster_http_endpoint + self.topic_endpoint.format(
            cluster_id)
               + "/{0}".format(topic_name))
        response = self.api_request.api_del(url)
        if response.status_code == 204:
            return True
        else:
            raise Exception(
                "Following error occurred while deleting topic {0} - "
                "{1}".format(topic_name, response.json()["message"]))

class KafkaConnectUtil(object):

    def __init__(self, connect_server_hostname):
        self.host = connect_server_hostname
        self.api_request = APIRequests()

        self.connector_endpoint = self.host + "/connectors"
        self.connector_plugins = self.host + "/connector-plugins"

    def is_kafka_connect_running(self):
        """
        Method verifies whether kafka connect cluster is running.
        """
        response = self.api_request.api_get(self.host)
        if response.status_code == 200:
            return True
        else:
            raise Exception(
                "Fetching Kafka Connect Cluster info failed. Response Code: {"
                "0}, Error: {1}".format(response.status_code, response.content))

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
        if get_connector_status and get_connector_info:
            query_str = "?expand=status&expand=info"
        elif get_connector_status:
            query_str = "?expand=status"
        elif get_connector_info:
            query_str = "?expand=info"
        response = self.api_request.api_get(self.connector_endpoint + query_str)
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
        Get a list of tasks currently running for the connector.
        param connector_name <str> name of the connector
        """
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


class ConfluentUtils(object):

    """
    Must Read

    Assumptions -
    1. An environment is created in confluent cloud
    2. A kafka cluster is deployed in confluent cloud
    3. An API key for kafka cluster is already created. This is required for
       Kafka connect cluster.
    4. Always choose topic prefix to be the "GROUP" parameter that is
       present in the conf file. This will ensure following -
        1. No conflicting topic names in kafka
        2. Proper cleanup of all the topics.
        3. Proper cleanup of the connectors

    Steps to use -
    1. Call generate_confluent_kafka_object to create cluster obj.
    2. Call delete_topics, this will ensure that any topic with the prefix
       passed is deleted before the test starts. This is ensure that no old
       topic with same prefix is reused, as this can cause data mismatch.
    3. Generate connector config
    4. Deploy connector. This will automatically create topics based on the
       connector config.
    5. Add the connector name and connector config in kafka cluster object
    6. Call fetch_all_topics_for_a_cluster with populate_cluster_obj as True,
       to populate the kafka cluster object with the topics that were
       created by the connector.
    7. Run your test.
    8. Do not skip this step - Call cleanup_kafka_resources, to delete all the
       connectors that were deployed, to delete all the topics and api keys
       associated with the cluster.
    """

    def __init__(self, cloud_access_key, cloud_secret_key,
                 connect_cluster_hostname, topic_prefix):
        self.cloud_access_key = cloud_access_key
        self.cloud_secret_key = cloud_secret_key
        self.connect_cluster_hostname = connect_cluster_hostname
        self.topic_prefix = topic_prefix

        self.confluent_apis = ConfluentCloudAPIs(
            self.cloud_access_key, self.cloud_secret_key)

        # This will get initialized in generate_confluent_kafka_object
        self.kafka_cluster_apis = ConfluentKafkaAPIs()

        self.connect_cluster_apis = KafkaConnectUtil(
            self.connect_cluster_hostname)

        self.log = logger.get("test")

    def generate_mongo_connector_config(
            self, mongo_connection_str, mongo_collections,
            src_connector="debezium"):
        """
        Method to generate mongo connector configurations
        param mongo_connection_str <str> Connection string to connect to mongo
        param mongo_collections <list> List of fully qualified mongo
        collection names i.e. DB_name.Collection_Name
        param src_connector <str> Name of the source connector provider.
        Currently following are supported - debezium
        """
        if src_connector.lower() == "debezium":
            config = ConnectorConfigTemplate.MongoConfigs.debezium
        config["topic.prefix"] = self.topic_prefix
        config["mongodb.connection.string"] = mongo_connection_str
        config["collection.include.list"] = ",".join(mongo_collections)
        return config

    def generate_confluent_kafka_object(self, kafka_cluster_id):
        """
        Method to create and populate the kafka cluster object.
        This object can then be used in the test to interact with kafka
        cluster.
        """
        cluster_obj = ConfluentKafkaCluster()
        cluster_obj.cloud_access_key = self.cloud_access_key
        cluster_obj.cloud_secret_key = self.cloud_secret_key
        cluster_obj.id = kafka_cluster_id
        cluster_obj.topic_prefix = self.topic_prefix

        try:
            environments = self.confluent_apis.list_all_environments()
            for environment in environments:
                clusters = self.confluent_apis.list_all_kafka_clusters(
                    environment["id"])
                for cluster in clusters:
                    if cluster["id"] == kafka_cluster_id:
                        cluster_obj.environment_name = environment["display_name"]
                        cluster_obj.environment_id = environment["id"]
                        cluster_obj.name = cluster["spec"]["display_name"]
                        cluster_obj.provider = cluster["spec"]["cloud"]
                        cluster_obj.region = cluster["spec"]["region"]
                        cluster_obj.cluster_type = cluster["spec"]["config"][
                            "kind"]
                        cluster_obj.availability = cluster["spec"]["availability"]
                        cluster_obj.http_endpoint = cluster["spec"]["http_endpoint"]
                        cluster_obj.bootstrap_server = cluster["spec"][
                            "kafka_bootstrap_endpoint"].split("//")[1]
                        break
            user = self.confluent_apis.list_all_users()[0]
            response = self.confluent_apis.create_api_key(
                user["id"], "test_key_{0}".format(int(time.time())),
                resource_id=cluster_obj.id, environment_id=cluster_obj.environment_id)
            cluster_obj.cluster_access_key = response["id"]
            cluster_obj.cluster_secret_key = response["spec"]["secret"]
            self.kafka_cluster_apis.set_authentication_keys(
                cluster_obj.cluster_access_key, cluster_obj.cluster_secret_key)

            dlq_topic_name = self.topic_prefix + ".dead_letter_queue"
            self.log.info("Creating Dead Letter Queue topic {0}".format(dlq_topic_name))
            topic_config = [
                {
                    "name": "cleanup.policy",
                    "value": "compact"
                }
            ]
            self.kafka_cluster_apis.create_topic(
                cluster_obj.http_endpoint, cluster_obj.id, dlq_topic_name,
                topic_config, 6, 3)
            return cluster_obj
        except Exception as err:
            self.log.error(str(err))
            return None

    def fetch_all_topics_for_a_cluster(
            self, kafka_cluster_obj, prefix_filter=None,
            populate_cluster_obj=False):
        """
        Method fetches all the topics for a kafka cluster except the
        internal topics.
        param kafka_cluster_obj <obj> ConfluentKafkaCluster object.
        param prefix_filter <str> returns only the topics which have the
        prefix mentioned.
        """
        topic_names = []
        try:
            topics = self.kafka_cluster_apis.list_all_topics(
                kafka_cluster_obj.http_endpoint, kafka_cluster_obj.id)
            for topic in topics:
                if topic["is_internal"]:
                    pass
                elif prefix_filter and (prefix_filter in topic["topic_name"]):
                    topic_names.append(topic["topic_name"])
            if populate_cluster_obj:
                kafka_cluster_obj.topics.extend(topic_names)
            return topic_names
        except Exception as err:
            self.log.error(str(err))
            return topic_names

    def deploy_connector(self, connector_name, connector_config):
        """
        Deploys a connector on Kafka connect cluster.
        """
        try:
            self.connect_cluster_apis.is_kafka_connect_running()
            response = self.connect_cluster_apis.create_connector(
                connector_name, connector_config)
            if not response:
                self.log.error("Unable to deploy connectors")
            connector_status = (
                self.connect_cluster_apis.get_connector_status(connector_name))
            while connector_status["connector"]["state"] != "RUNNING":
                if connector_status["connector"]["state"] == "FAILED":
                    raise Exception("Connector failed to deploy, current "
                                    "state is FAILED")
                self.log.info("Connector is in {0} state, waiting for it to "
                              "be in RUNNING state".format(connector_status["connector"]["state"]))
                time.sleep(10)
                connector_status = self.connect_cluster_apis.get_connector_status(
                    connector_name)
            return True
        except Exception as err:
            self.log.error(str(err))
            return False

    def delete_topics(self, kafka_cluster_obj):
        """
        Deletes all topics which have a particular prefix.
        """
        topics = self.fetch_all_topics_for_a_cluster(
            kafka_cluster_obj, prefix_filter=kafka_cluster_obj.topic_prefix)
        failed_to_delete_topics = []
        for topic in topics:
            try:
                self.log.info("Deleting topic {0}".format(topic))
                self.kafka_cluster_apis.delete_topic(
                    kafka_cluster_obj.http_endpoint, kafka_cluster_obj.id, topic)
            except Exception as err:
                self.log.error(str(err))
                failed_to_delete_topics.append(topic)
        if failed_to_delete_topics:
            self.log.error("Following topics were not delete {0}. Delete "
                           "them manually in order to avoid test failure")
            return False
        return True

    def cleanup_kafka_resources(self, kafka_cluster_obj):
        self.log.info("Deleting all the deployed connectors")
        failed_connector_deletions = list()
        for connector in kafka_cluster_obj.connectors:
            try:
                self.connect_cluster_apis.delete_connector(connector)
            except Exception as err:
                self.log.error(str(err))
                failed_connector_deletions.append(connector)
        topic_delete_status = self.delete_topics(kafka_cluster_obj)
        try:
            key_deletion_status = self.confluent_apis.delete_api_key(
                kafka_cluster_obj.cluster_access_key)
        except Exception as err:
            self.log.error(str(err))

        if failed_connector_deletions or (not topic_delete_status) or (
                not key_deletion_status):
            self.log.error("Kafka resource cleanup failed.")
            return False
        return True





