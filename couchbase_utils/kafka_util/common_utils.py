"""
Created on 3-May-2024

@author: Umang Agrawal

This utility is for making rest calls.
"""

import logging
from confluent_kafka.admin import AdminClient, NewTopic, NewPartitions
from confluent_kafka import KafkaException


class KafkaCluster(object):
    """
    KafkaCluster stores information regarding topics and connectors.
    """

    def __init__(self):
        self.connector_configs = dict()
        # Topic Details
        self.topic_prefix = None
        self.topics = list()
        self.internal_topics = ["connect-configs", "connect-offsets",
                                "connect-status"]
        # Dead letter queue topic name
        self.dlq_topic = None

        # Connector Info required to connect to Kafka cluster in order to
        # perform operations on topics and it's partitions.
        self.connectors = {}

    def generate_connection_config(
            self, bootstrap_servers, security_protocal, sasl_mechanism,
            sasl_username, sasl_password, **kwargs):
        """
        Generates config containing connection details for kafka cluster.
        """
        config = {
            'bootstrap.servers': bootstrap_servers,
            'security.protocol': security_protocal,
            'sasl.mechanisms': sasl_mechanism,
            'sasl.username': sasl_username,
            'sasl.password': sasl_password
        }
        if kwargs:
            config.update(kwargs)
        return config


class KafkaClusterUtils(object):
    """
    KafkaClusterUtils provides admin operations for Kafka brokers, topics,
    partitions, groups, and other resource types supported by the broker.
    Note - Currently only operations related to topics and partitions are
    supported by this utility.
    """

    def __init__(self, connect_config):
        self.client = AdminClient(connect_config)
        self.log = logging.getLogger(__name__)

    def create_topic(self, topic_name, configs={}, partitions_count=0,
                     replication_factor=0, validate_only=False,
                     operation_timeout=300, request_timeout=300):
        """
        Creates a new topic on kafka cluster
        :param topic_name Topic name
        :param configs Dict of topic configuration.
            See http://kafka.apache.org/documentation.html#topicconfigs
        :param partitions_count Number of partitions to create
        :param replication_factor Replication factor of partitions,
        :param validate_only If true, the request is only validated without
        creating the topic.
        :param operation_timeout The operation timeout in seconds, controlling
            how long the CreateTopics request will block on the broker
            waiting for the topic creation to propagate in the cluster.
            A value of 0 returns immediately.
        :param request_timeout The overall request timeout in seconds,
            including broker lookup, request transmission, operation time on
            broker, and response.
        """
        topic = NewTopic(topic_name, num_partitions=partitions_count,
                         replication_factor=replication_factor)
        if configs:
            topic.config = configs
        try:
            self.log.info(
                "Creating topic {0} with {1} partitions and {2} replication "
                "factor".format(
                    topic_name, partitions_count, replication_factor))
            response = self.client.create_topics(
                new_topics=[topic], operation_timeout=operation_timeout,
                request_timeout=request_timeout, validate_only=validate_only)
            response[topic_name].result()
            return True

        except KafkaException as err:
            raise Exception(err.args[0].str())

    def delete_topics(self, topics, operation_timeout=300, request_timeout=300):
        """
        Deletes topic/s on kafka cluster
        :param topics List of Topic names
        :param operation_timeout The operation timeout in seconds, controlling
            how long the CreateTopics request will block on the broker
            waiting for the topic creation to propagate in the cluster.
            A value of 0 returns immediately.
        :param request_timeout The overall request timeout in seconds,
            including broker lookup, request transmission, operation time on
            broker, and response.
        """
        try:
            self.log.info("Deleting topics {}".format(topics))
            response = self.client.delete_topics(
                topics, operation_timeout=operation_timeout,
                request_timeout=request_timeout)
            for topic in response:
                response[topic].result()

        except KafkaException as err:
            raise Exception(err.args[0].str())

    def list_all_topics(self, timeout=-1):
        """
        List all the topics in kafka cluster
        :param timeout The maximum response time before timing out, or -1 for
            infinite timeout.
        """
        try:
            self.log.debug("Listing all topics")
            response = self.client.list_topics(timeout=timeout)
            return response.topics.keys()
        except KafkaException as err:
            raise Exception(err.args[0].str())

    def get_topic_info(self, topic_name, timeout=-1):
        """
        Get info on a topic of a kafka cluster
        :param topic_name Name of the topic whose info has to be fetched.
        :param timeout The maximum response time before timing out, or -1 for
            infinite timeout.
        """
        try:
            self.log.info("Fetching info for topic {}".format(topic_name))
            response = self.client.list_topics(topic_name, timeout)
            response = response.__dict__
            for broker_index, broker in response["brokers"].iteritems():
                response["brokers"][broker_index] = broker.__dict__
            for topic_name, topic in response["topics"].iteritems():
                response["topics"][topic_name] = topic.__dict__
                for partition_idx, partition in response["topics"][
                    topic_name]["partitions"].iteritems():
                    response["topics"][topic_name]["partitions"][
                        partition_idx] = partition.__dict__
            return response
        except KafkaException as err:
            raise Exception(err.args[0].str())

    def list_all_partitions(self, topic_name, timeout=-1):
        """
        Lists all the partitions in a topic of a kafka cluster
        :param topic_name Name of the topic whose partitions have to be
            fetched.
        :param timeout The maximum response time before timing out, or -1 for
            infinite timeout.
        """
        self.log.info("Listing all partitions for topic {}".format(topic_name))
        response = self.get_topic_info(topic_name, timeout)
        return response["topics"][topic_name]["partitions"]

    def get_partition_info(self, topic_name, partition_id, timeout=-1):
        """
        Get info of a partition on a topic in a kafka cluster
        :param topic_name Name of the topic where partition is present.
        :param partition_id ID of the partition whose info has to be fetched.
        :param timeout The maximum response time before timing out, or -1 for
            infinite timeout.
        """
        self.log.info("Fetching info for partition {0} in topic {1}".format(
            partition_id, topic_name))
        response = self.list_all_partitions(topic_name, timeout)
        return response[partition_id]

    def update_partition_count_for_topic(
            self, topic_name, partition_count, operation_timeout=300,
            request_timeout=300, validate_only=False):
        self.log.info("Updating topic {0} partition count to {1}".format(
            topic_name, partition_count))
        partition = NewPartitions(topic_name, partition_count)
        try:
            response = self.client.create_partitions(
                [partition], operation_timeout=operation_timeout,
                request_timeout=request_timeout, validate_only=validate_only)
            response[topic_name].result()
            return True
        except KafkaException as err:
            raise Exception(err.args[0].str())

    def list_all_topics_by_topic_prefix(self, topic_prefix):
        topics = self.list_all_topics()
        filtered_topics = list()
        for topic in topics:
            if topic_prefix == topic.split(".")[0]:
                filtered_topics.append(topic)
        return filtered_topics

    def update_topics_in_kafka_cluster_obj(self, kafka_cluster_obj):
        topics = self.list_all_topics_by_topic_prefix(
            kafka_cluster_obj.topic_prefix)
        for topic in topics:
            if topic not in kafka_cluster_obj.topics:
                kafka_cluster_obj.topics.append(topic)

    def delete_topic_by_topic_prefix(self, topic_prefix):
        topics = self.list_all_topics_by_topic_prefix(topic_prefix)
        if topics:
            self.delete_topics(topics)


