"""
Created on 3-May-2024

@author: Umang Agrawal

This utility is for interacting with AWS MSK service.
"""


from awsLib.S3 import MSK
from copy import deepcopy
from global_vars import logger
from couchbase_utils.kafka_util.common_utils import KafkaCluster, KafkaClusterUtils
from couchbase_utils.kafka_util.kafka_connect_util import KafkaConnectUtil


class MSkCluster(KafkaCluster):
    """
    Object for storing MSK cluster information
    """

    def __init__(self):
        super(MSkCluster, self).__init__()
        self.name = None
        self.region = None
        self.arn = None
        self.zookeeper_connect_str = {
            "PlainText": None,
            "TLs": None
        }
        self.bootstrap_brokers = {
            "PlainText": None,
            "TLs": None,
            "SaslScram": None,
            "SaslIam": None,
            "PublicTls": None,
            "PublicSaslScram": None,
            "PublicSaslIam": None,
            "VpcConnectivityTls": None,
            "VpcConnectivitySaslScram": None,
            "VpcConnectivitySaslIam": None,
        }
        self.sasl_username = None
        self.sasl_password = None


class MSKUtils(object):

    # Auth Types
    AWS_AUTH_TYPES = ["SCRAM_SHA_512", "IAM"]
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
    # Use this if authenticationType is SCRAM_SHA_512
    AWS_MSK_CREDENTIALS = {
        "username": None,
        "password": None
    }
    AWS_MSK_SCHEMA_REGISTRY_DETAILS_TEMPLATE = {
        "connectionFields": {
            "awsRegion": None,
            "accessKeyId": None,
            "secretAccessKey": None,
            "sessionToken": None,
            "registryName": None  # optional
        }
    }

    def __init__(self, connect_cluster_hostname,
                 access_key, secret_key, region, session_token=None):
        self.log = logger.get("test")

        self.region = region
        self.connect_cluster_hostname = (
            f"http://{connect_cluster_hostname}:8084")
        self.connect_cluster_apis = KafkaConnectUtil(
            self.connect_cluster_hostname)

        self.msk_lib = MSK(access_key, secret_key, region, session_token)

        # This is instantiated in generate_msk_cluster_object method
        self.kafka_cluster_util = None

    """
    Method generates kafka cluster details object, which can be used while
    creating kafka links.
    """
    def generate_aws_kafka_cluster_detail(
            self, brokers_url, auth_type, encryption_type, username=None,
            password=None):
        kafka_cluster_details = deepcopy(self.KAFKA_CLUSTER_DETAILS_TEMPLATE)
        kafka_cluster_details["vendor"] = "AWS_KAFKA"
        kafka_cluster_details["brokersUrl"] = brokers_url

        auth_type = auth_type.upper()
        if auth_type in self.AWS_AUTH_TYPES:
            kafka_cluster_details["authenticationDetails"][
                "authenticationType"] = auth_type
            if auth_type == "SCRAM_SHA_512":
                kafka_cluster_details["authenticationDetails"][
                    "credentials"] = deepcopy(self.AWS_MSK_CREDENTIALS)
                kafka_cluster_details["authenticationDetails"][
                    "credentials"]["username"] = username
                kafka_cluster_details["authenticationDetails"][
                    "credentials"]["password"] = password
        else:
            raise Exception(
                "Invalid Authentication type. Supported authentication types "
                "are {0}.".format(self.AWS_AUTH_TYPES))

        if encryption_type.upper() == "TLS":
            kafka_cluster_details["authenticationDetails"][
                "encryptionType"] = encryption_type.upper()
        else:
            raise Exception(
                "Invalid Encryption type. Supported Encryption types "
                "are {0}.".format("TLS"))
        return kafka_cluster_details

    """
    Method generates schema registry details object, which can be used while
    creating kafka links.
    """
    def generate_aws_kafka_schema_registry_detail(
            self, aws_region, access_key_id, secret_access_key,
            session_token=None, registry_name=None):
        schema_registry_details = deepcopy(
            self.AWS_MSK_SCHEMA_REGISTRY_DETAILS_TEMPLATE)
        schema_registry_details["connectionFields"][
            "awsRegion"] = aws_region
        schema_registry_details["connectionFields"][
            "accessKeyId"] = access_key_id
        schema_registry_details["connectionFields"][
            "secretAccessKey"] = secret_access_key
        if session_token:
            schema_registry_details["connectionFields"][
                "sessionToken"] = session_token
        else:
            del(schema_registry_details["connectionFields"]["sessionToken"])

        if registry_name:
            schema_registry_details["connectionFields"][
                "registryName"] = registry_name
        else:
            del(schema_registry_details["connectionFields"]["registryName"])

        return schema_registry_details

    def generate_msk_cluster_object(self, msk_cluster_name, topic_prefix,
                                    sasl_username, sasl_password):
        cluster_obj = MSkCluster()
        cluster_obj.name = msk_cluster_name
        cluster_obj.topic_prefix = topic_prefix
        cluster_obj.region = self.region

        try:
            clusters = self.msk_lib.list_all_msk_clusters()
            for cluster in clusters:
                if cluster["ClusterName"] == cluster_obj.name:
                    cluster_obj.arn = cluster["ClusterArn"]
                    cluster_obj.zookeeper_connect_str["PlainText"] = cluster[
                        "ZookeeperConnectString"]
                    cluster_obj.zookeeper_connect_str["TLs"] = cluster[
                        "ZookeeperConnectStringTls"]
                    break
            bootstrap_brokers_info = self.msk_lib.get_bootstrap_brokers(
                cluster_obj.arn)
            cluster_obj.bootstrap_brokers["PlainText"] = \
                bootstrap_brokers_info.get("BootstrapBrokerString", None)
            cluster_obj.bootstrap_brokers["TLs"] = \
                bootstrap_brokers_info.get("BootstrapBrokerStringTls", None)
            cluster_obj.bootstrap_brokers["SaslScram"] = \
                bootstrap_brokers_info.get("BootstrapBrokerStringSaslScram", None)
            cluster_obj.bootstrap_brokers["SaslIam"] = \
                bootstrap_brokers_info.get("BootstrapBrokerStringSaslIam", None)
            cluster_obj.bootstrap_brokers["PublicTls"] = \
                bootstrap_brokers_info.get("BootstrapBrokerStringPublicTls", None)
            cluster_obj.bootstrap_brokers["PublicSaslScram"] = \
                bootstrap_brokers_info.get(
                    "BootstrapBrokerStringPublicSaslScram", None)
            cluster_obj.bootstrap_brokers["PublicSaslIam"] = \
                bootstrap_brokers_info.get(
                    "BootstrapBrokerStringPublicSaslIam", None)
            cluster_obj.bootstrap_brokers["VpcConnectivityTls"] = \
                bootstrap_brokers_info.get(
                    "BootstrapBrokerStringVpcConnectivityTls", None)
            cluster_obj.bootstrap_brokers["VpcConnectivitySaslScram"] = \
                bootstrap_brokers_info.get(
                    "BootstrapBrokerStringVpcConnectivitySaslScram", None)
            cluster_obj.bootstrap_brokers["VpcConnectivitySaslIam"] = \
                bootstrap_brokers_info.get(
                    "BootstrapBrokerStringVpcConnectivitySaslIam", None)
            cluster_obj.sasl_username = sasl_username
            cluster_obj.sasl_password = sasl_password
            connection_config = cluster_obj.generate_connection_config(
                cluster_obj.bootstrap_brokers["PublicSaslScram"],
                security_protocal="SASL_SSL", sasl_mechanism="SCRAM-SHA-512",
                sasl_username=sasl_username, sasl_password=sasl_password)
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
            if failed_connector_deletions or (not topic_delete_status):
                self.log.error("Kafka resource cleanup failed.")
                return False
            return True
