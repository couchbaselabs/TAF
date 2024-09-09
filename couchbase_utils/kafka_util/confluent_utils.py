# -*- coding: utf-8 -*-
# Generic/Built-in
"""
Created on 22-April-2024

@author: Umang Agrawal

This utility is for performing actions on Confluent Managed Kafka and
confluent connector server.
"""


import time
import logging
import base64
from copy import deepcopy
from couchbase_utils.kafka_util.common_utils import KafkaCluster, KafkaClusterUtils
from capellaAPI.capella.lib.APIRequests import APIRequests
from couchbase_utils.kafka_util.kafka_connect_util import KafkaConnectUtil

CONFLUENT_CLOUD_BASE_URL = "https://api.confluent.cloud"


class ConfluentKafkaCluster(KafkaCluster):
    """
    Object for storing confluent kafka information
    """

    def __init__(self):
        super(ConfluentKafkaCluster, self).__init__()
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


class ConfluentCloudAPIs(object):
    """
    This class contains collection of API to access confluent cloud.
    For more details on confluent APIs visit below page-
        https://docs.confluent.io/cloud/current/api.html
    """

    def __init__(self, access=None, secret=None):
        self.api_request = APIRequests(CONFLUENT_CLOUD_BASE_URL)
        self.set_authentication_keys(access, secret)

        # V2 Endpoints
        self.api_key_endpoint = "/iam/v2/api-keys"
        self.user_endpoint = "/iam/v2/users"
        self.environment_endpoint = "/org/v2/environments"
        self.kafka_cluster_endpoint = "/cmk/v2/clusters"
        self.region_endpoint = "/srcm/v2/regions"

    def parse_error(self, response):
        errors = response.json()["errors"]
        error_log = ""
        for error in errors:
            error_log += error["detail"]
        return error_log

    def set_authentication_keys(self, access, secret):
        self.api_request.ACCESS = access
        self.api_request.SECRET = secret

    def generate_auth_header(self):
        base64_encoded_auth = base64.b64encode("{0}:{1}".format(
            self.api_request.ACCESS, self.api_request.SECRET).encode()).decode()
        api_request_headers = {
            'Authorization': 'Basic ' + base64_encoded_auth,
            'Content-Type': 'application/json'
        }

        # Return the header
        return api_request_headers

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
        response = self.api_request.api_get(
            url, params=query_params,
            headers=self.generate_auth_header())
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
        response = self.api_request.api_get(
            url, headers=self.generate_auth_header())
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
        response = self.api_request.api_post(
            url, payload, headers=self.generate_auth_header())
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
        response = self.api_request.api_del(
            url, headers=self.generate_auth_header())
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

        response = self.api_request.api_get(
            url, params=query_params, headers=self.generate_auth_header())
        user_list = list()
        if response.status_code == 200:
            parsed_response = response.json()
            user_list.extend(parsed_response["data"])
            if "total_size" in parsed_response["metadata"]:
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

        response = self.api_request.api_get(
            url, params=query_params, headers=self.generate_auth_header())
        env_list = list()
        if response.status_code == 200:
            parsed_response = response.json()
            env_list.extend(parsed_response["data"])
            if "total_size" in parsed_response["metadata"]:
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
        response = self.api_request.api_get(
            url, headers=self.generate_auth_header())
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
        response = self.api_request.api_post(
            url, payload, headers=self.generate_auth_header())
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
        response = self.api_request.api_del(
            url, headers=self.generate_auth_header())
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

        response = self.api_request.api_get(
            url, params=query_params, headers=self.generate_auth_header())
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

        response = self.api_request.api_get(
            url, params=query_params, headers=self.generate_auth_header())
        kafka_cluster_list = list()
        if response.status_code == 200:
            parsed_response = response.json()
            kafka_cluster_list.extend(parsed_response["data"])
            if "total_size" in parsed_response["metadata"]:
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
        response = self.api_request.api_get(
            url, query_param, headers=self.generate_auth_header())
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
        response = self.api_request.api_post(
            url, payload, headers=self.generate_auth_header())
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
        response = self.api_request.api_del(
            url, query_param, headers=self.generate_auth_header())
        if response.status_code == 204:
            return True
        else:
            raise Exception(
                "Following errors occurred while deleting cluster {0} - "
                "{1}".format(cluster_id, self.parse_error(response)))


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
    2. Call kafka_cluster_util.delete_topic_by_topic_prefix, this will ensure
       that any topic with the prefix passed is deleted before the test
       starts. This is ensure that no old topic with same prefix is reused,
       as this can cause data mismatch.
    3. Generate connector config
    4. Deploy connector. This will automatically create topics based on the
       connector config.
    5. Add the connector name and connector config in kafka cluster object
    6. Call kafka_cluster_util.update_topics_in_kafka_cluster_obj,
       to populate the kafka cluster object with the topics that were
       created by the connector.
    7. Run your test.
    8. Do not skip this step - Call cleanup_kafka_resources, to delete all the
       connectors that were deployed, to delete all the topics and api keys
       associated with the cluster.
    """

    # Auth Types
    CONFLUENT_AUTH_TYPES = ["PLAIN", "OAUTH", "SCRAM_SHA_256",
                            "SCRAM_SHA_512"]
    # Encryption Types
    ENCRYPTION_TYPES = ["PLAINTEXT", "TLS"]
    KAFKA_CLUSTER_DETAILS_TEMPLATE = {
        "vendor": None,
        "brokersUrl": None,  # Comma separated list of brokers
        "authenticationDetails": {
            "authenticationType": None,
            "encryptionType": None,
            "credentials": {}
        }
    }
    # Use this if authenticationType is PLAIN,SCRAM_SHA_256,SCRAM_SHA_512
    CONFLUENT_CREDENTIALS = {
        "apiKey": None,
        "apiSecret": None
    }
    # Use this if authenticationType is OAUTH
    CONFLUENT_CREDENTIALS_OAUTH = {
        "oauthTokenEndpointURL": None,
        "clientId": None,
        "clientSecret": None,
        "scope": None,  # optional
        "extension_logicalCluster": None,  # optional
        "extension_identityPoolId": None  # optional
    }
    CONFLUENT_SCHEMA_REGISTRY_DETAILS_TEMPLATE = {
        "connectionFields": {
            "schemaRegistryURL": None,
            "schemaRegistryCredentials": None  # Format "Api Key:Api Secret"
        }
    }

    def __init__(self, cloud_access_key, cloud_secret_key,
                 connect_cluster_hostname):
        self.cloud_access_key = cloud_access_key
        self.cloud_secret_key = cloud_secret_key

        self.connect_cluster_hostname = (
            f"http://{connect_cluster_hostname}:8083")
        self.connect_cluster_apis = KafkaConnectUtil(
            self.connect_cluster_hostname)

        self.confluent_apis = ConfluentCloudAPIs(
            self.cloud_access_key, self.cloud_secret_key)

        # This is instantiated in generate_confluent_kafka_object method
        self.kafka_cluster_util = None

        self.log = logging.getLogger(__name__)

    """
    Method generates kafka cluster details object, which can be used while
    creating kafka links.
    """
    def generate_confluent_kafka_cluster_detail(
            self, brokers_url, auth_type, encryption_type, api_key=None,
            api_secret=None, oauthTokenEndpointURL=None, clientId=None,
            clientSecret=None, scope=None, extension_logicalCluster=None,
            extension_identityPoolId=None):
        kafka_cluster_details = deepcopy(self.KAFKA_CLUSTER_DETAILS_TEMPLATE)
        kafka_cluster_details["vendor"] = "CONFLUENT"
        kafka_cluster_details["brokersUrl"] = brokers_url

        auth_type = auth_type.upper()
        if auth_type in self.CONFLUENT_AUTH_TYPES:
            kafka_cluster_details["authenticationDetails"][
                "authenticationType"] = auth_type
            if auth_type == "OAUTH":
                kafka_cluster_details["authenticationDetails"][
                    "credentials"] = deepcopy(self.CONFLUENT_CREDENTIALS_OAUTH)
                kafka_cluster_details["authenticationDetails"][
                    "credentials"][
                    "oauthTokenEndpointURL"] = oauthTokenEndpointURL
                kafka_cluster_details["authenticationDetails"][
                    "credentials"]["clientId"] = clientId
                kafka_cluster_details["authenticationDetails"][
                    "credentials"]["clientSecret"] = clientSecret
                if scope:
                    kafka_cluster_details["authenticationDetails"][
                        "credentials"]["scope"] = scope
                else:
                    del(kafka_cluster_details["authenticationDetails"][
                        "credentials"]["scope"])
                if extension_logicalCluster:
                    kafka_cluster_details["authenticationDetails"][
                        "credentials"][
                        "extension_logicalCluster"] = extension_logicalCluster
                else:
                    del(kafka_cluster_details["authenticationDetails"][
                        "credentials"]["extension_logicalCluster"])
                if extension_identityPoolId:
                    kafka_cluster_details["authenticationDetails"][
                        "credentials"][
                        "extension_identityPoolId"] = extension_identityPoolId
                else:
                    del(kafka_cluster_details["authenticationDetails"][
                        "credentials"]["extension_identityPoolId"])
            else:
                kafka_cluster_details["authenticationDetails"][
                    "credentials"] = deepcopy(self.CONFLUENT_CREDENTIALS)
                kafka_cluster_details["authenticationDetails"][
                    "credentials"]["apiKey"] = api_key
                kafka_cluster_details["authenticationDetails"][
                    "credentials"]["apiSecret"] = api_secret
        else:
            raise Exception(
                "Invalid Authentication type. Supported authentication types "
                "are {0}.".format(self.CONFLUENT_AUTH_TYPES))

        if encryption_type.upper() in self.ENCRYPTION_TYPES:
            kafka_cluster_details["authenticationDetails"][
                "encryptionType"] = encryption_type.upper()
        else:
            raise Exception(
                "Invalid Encryption type. Supported Encryption types "
                "are {0}.".format(self.ENCRYPTION_TYPES))
        return kafka_cluster_details

    """
    Method generates schema registry details object, which can be used while
    creating kafka links.
    """
    def generate_confluent_schema_registry_detail(
            self, schema_registry_url, api_key, api_secret):
        schema_registry_details = deepcopy(
            self.CONFLUENT_SCHEMA_REGISTRY_DETAILS_TEMPLATE)
        schema_registry_details["connectionFields"][
            "schemaRegistryURL"] = schema_registry_url
        schema_registry_details["connectionFields"][
            "schemaRegistryCredentials"] = f"{api_key}:{api_secret}"
        return schema_registry_details

    def generate_confluent_kafka_object(self, kafka_cluster_id, topic_prefix):
        """
        Method to create and populate the kafka cluster object.
        This object can then be used in the test to interact with kafka
        cluster.
        """
        cluster_obj = ConfluentKafkaCluster()
        cluster_obj.cloud_access_key = self.cloud_access_key
        cluster_obj.cloud_secret_key = self.cloud_secret_key
        cluster_obj.id = kafka_cluster_id
        cluster_obj.topic_prefix = topic_prefix

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
            connection_config = cluster_obj.generate_connection_config(
                cluster_obj.bootstrap_server,
                security_protocal="SASL_SSL", sasl_mechanism="PLAIN",
                sasl_username=cluster_obj.cluster_access_key,
                sasl_password=cluster_obj.cluster_secret_key)
            self.kafka_cluster_util = KafkaClusterUtils(connection_config)
            cluster_obj.dlq_topic = cluster_obj.topic_prefix + ".dead_letter_queue"
            self.log.info(
                "Creating Dead Letter Queue topic {0}".format(
                    cluster_obj.dlq_topic))
            self.kafka_cluster_util.create_topic(
                cluster_obj.dlq_topic, {"cleanup.policy": "compact"},
                partitions_count=8, replication_factor=3)
            return cluster_obj
        except Exception as err:
            self.log.error(str(err))
            return None

    def cleanup_kafka_resources(self, kafka_cluster_obj):
        self.log.info("Deleting all the deployed connectors")
        failed_connector_deletions = list()
        for connector in kafka_cluster_obj.connectors:
            try:
                self.connect_cluster_apis.delete_connector(connector)
            except Exception as err:
                self.log.error(str(err))
                failed_connector_deletions.append(connector)
        topic_delete_status = False
        self.log.info("Deleting all the topics")
        try:
            self.kafka_cluster_util.delete_topic_by_topic_prefix(
                kafka_cluster_obj.topic_prefix)
            topic_delete_status = True
        except Exception as err:
            self.log.error(str(err))
        finally:
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

    def deploy_connector(self, connector_name, connector_config):
        """
        This method uses KafkaConnectUtil's deploy_connector method for
        deploying connectors. After deploying the connector, it also checks
        whether the topics that were supposed to be created by the connector
        is actually created or not.
        :param connector_name:
        :param connector_config:
        :return:
        """
        result = self.connect_cluster_apis.deploy_connector(
            connector_name, connector_config)
        if not result:
            return result
        topic_prefix = connector_config["topic.prefix"]
        actual_topics_created = (
            self.kafka_cluster_util.list_all_topics_by_topic_prefix(
                topic_prefix))
        expected_topics = [f"{topic_prefix}.{collection_name}"
                           for collection_name in connector_config[
                               "collection.include.list"].split(",")]
        all_topics_created = True
        for topic in expected_topics:
            if topic not in actual_topics_created:
                all_topics_created = False
        return all_topics_created
