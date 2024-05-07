"""
Created on 3-May-2024

@author: Umang Agrawal

This utility is for interacting with AWS MSK service.
"""


from kafka import KafkaAdminClient
from awsLib.S3 import MSK
from global_vars import logger
from couchbase_utils.kafka_util.common_utils import KafkaCluster
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


class MSKUtils(object):

    def __init__(self, connect_cluster_hostname, topic_prefix,
                 access_key, secret_key, region, session_token=None):
        self.log = logger.get("test")

        self.connect_cluster_hostname = connect_cluster_hostname

        self.topic_prefix = topic_prefix
        self.region = region

        self.connect_cluster_apis = KafkaConnectUtil(
            self.connect_cluster_hostname)

        self.msk_lib = MSK(access_key, secret_key, region, session_token)

    def generate_msk_cluster_object(self, msk_cluster_name):
        cluster_obj = MSkCluster()
        cluster_obj.name = msk_cluster_name
        cluster_obj.topic_prefix = self.topic_prefix
        cluster_obj.region = self.region

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
        return cluster_obj
