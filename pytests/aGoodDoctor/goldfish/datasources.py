"""
Created on Nov 3, 2023

@author: ritesh.agarwal
"""
import json
import pprint
import random
import string
import time

import pymongo

from CbasLib.CBASOperations import CBASHelper
from Jython_tasks.java_loader_tasks import SiriusJavaMongoLoader
from .CbasUtil import execute_statement_on_cbas
from py_constants.cb_constants.CBServer import CbServer
from global_vars import logger
from kafka_util.kafka_connect_util import KafkaConnectUtil
from TestInput import TestInputSingleton
from couchbase.exceptions import AmbiguousTimeoutException
from confluent_kafka.admin import AdminClient

_input = TestInputSingleton.input

class MongoDB(object):
    def __init__(self, hostname, username, password, mongo_db=None, atlas=False):
        self.log = logger.get("infra")
        self.hostname = hostname
        self.username = username
        self.password = password
        self.port = 27017
        self.atlas = atlas
        self.connString = "mongodb"
        self.source_name = mongo_db or "Mongo_" + ''.join([random.choice(string.ascii_letters + string.digits) for _ in range(5)])
        self.name = self.source_name
        self.type = "onPrem"
        self.collections = list()
        self.primary_key = "_id"
        self.link_name = self.type + "_" + self.source_name
        self.links = [self.link_name]
        
        self.cbas_queries = list()
        self.cbas_collections = list()
        self.query_map = dict()

        if self.atlas:
            self.connString = self.connString + "+srv"
            self.type = "onCloud"
        self.connString = self.connString + "://" \
            + self.username + ":" + self.password \
            + "@" + self.hostname + ":27017/"
            
        self.client = pymongo.MongoClient(self.connString)

    def set_collections(self):
        for i in range(self.loadDefn.get("collections")):
            self.collections.append("volCollection" + str(i))
            mydb = self.client[self.name]
            mycoll = mydb[self.collections[i]]
            time.sleep(2)
        self.client.close()

    def drop(self):
        self.client = pymongo.MongoClient(self.connString)
        self.client.drop_database(self.name)
        self.client.close()

    def setup_kafka_connectors(self, prefix):
        connector_hostname = _input.kafka.get("connector") + ":8083"
        self.prefix = prefix
        connector_util = KafkaConnectUtil()
        config = connector_util.generate_mongo_connector_config(
            "mongodb://{0}:{1}@{2}:{3}/?retryWrites=true&w=majority&replicaSet=rs0".format(
                self.username, self.password, self.hostname, self.port),
            mongo_collections=[self.source_name + "." + coll_name for coll_name in self.collections],
            topic_prefix=prefix,
            partitions=32,
            cdc_enabled=True)
        self.kafka_topics = [prefix + "." + self.source_name + "." + coll_name for coll_name in self.collections]
        status = connector_util.deploy_connector(connector_hostname, self.source_name, config)
        if status:
            self.log.info("connectors are deployed properly!! Check for the topics")

    def create_link(self, cluster):
        while cluster.state != "ACTIVE":
            time.sleep(60)
            continue
        rest = CBASHelper(cluster.master)
        kafka_details = {
            "dataverse": "Default",
            "name": self.link_name,
            "type": "kafka",
            "kafkaClusterDetails": json.dumps({
                "vendor":"CONFLUENT",
                "brokersUrl": "{}:9092".format(_input.kafka.get("cluster_url")),
                "authenticationDetails":{
                    "authenticationType":"PLAIN",
                    "encryptionType":"TLS",
                    "credentials":{
                        "apiKey":_input.kafka.get("cluster_username"),
                        "apiSecret":_input.kafka.get("cluster_password")
                        }
                    }
                })
            }
        timeout = 120
        while timeout > 0:
            result, status, content, errors = rest.analytics_link_operations(method="POST", params=kafka_details)
            if not result:
                self.log.critical("Status: %s, content: %s, Errors: %s" % (status, content, errors))
                time.sleep(30)
                continue
            return
        raise Exception("Status: %s, content: %s, Errors: %s" % (status, content, errors))

    def create_cbas_collections(self, cluster, num_collections=None, skip_init=False):
        while cluster.state != "ACTIVE":
            time.sleep(60)
            continue
        client = cluster.SDKClients[0].cluster
        num_collections = num_collections or len(self.collections)
        i = 0
        new_collections = list()
        while i < num_collections:
            mongo_collection = self.collections[i%len(self.collections)]
            cbas_coll_name = self.link_name + "_volCollection_" + str(i)
            cbas_coll_name = cbas_coll_name + "_" + "".join([random.choice(string.ascii_lowercase) for _ in range(5)])
            new_collections.append(cbas_coll_name)
            i += 1
            statement = "CREATE COLLECTION `{}` PRIMARY KEY (`_id`: string) ON {}.{} AT {};".format(
                                    cbas_coll_name, self.source_name, mongo_collection, self.link_name)
            extra = {
                "kafka-sink": "true",
                "keySerializationType": "JSON",
                "valueSerializationType": "JSON",
                "cdcEnabled": "true",
                "cdcDetails": {"cdcSource": "MONGODB", "cdcSourceConnector": "DEBEZIUM"}
                }
            self.log.info("creating kafka collections on couchbase: %s" % cbas_coll_name)
            statement = 'CREATE COLLECTION `Default`.`Default`.`{0}` PRIMARY KEY (_id: string) \
                        ON `{1}.{2}.{3}` AT `{4}` WITH {5}'.format(
                                    cbas_coll_name, self.prefix, self.source_name, mongo_collection, self.link_name, json.dumps(extra))
            if not skip_init:
                execute_statement_on_cbas(client, statement)
        self.cbas_collections.extend(new_collections)
        return new_collections

class MongoDocLoading(object):
    def __init__(self, doc_loading_tm, key_prefix, key_size, doc_size, key_type, value_type,
                 process_concurrency, task_identifier, ops, suppress_error_table, track_failures, mutate):
        self.key_prefix = key_prefix
        self.key_size = key_size
        self.doc_size = doc_size
        self.key_type = key_type
        self.value_type = value_type
        self.process_concurrency = process_concurrency
        self.task_identifier = task_identifier
        self.ops = ops
        self.suppress_error_table = suppress_error_table
        self.track_failures = track_failures
        self.mutate = mutate
        self.doc_loading_tm = doc_loading_tm

    def _loader_dict(self, databases, overRidePattern=None, workers=10, cmd={}):
        workers = workers
        loader_map = dict()
        default_pattern = [100, 0, 0, 0, 0]
        for database in databases:
            per_coll_ops = self.ops//(len(database.collections))
            pattern = overRidePattern or database.loadDefn.get("pattern", default_pattern)
            for i, collection in enumerate(database.collections):
                workloads = database.loadDefn.get("collections_defn", [database.loadDefn])
                valType = workloads[i % len(workloads)]["valType"]
                loader = SiriusJavaMongoLoader(
                        server_ip=database.hostname, server_port=database.port,
                        username=database.username, password=database.password,
                        bucket_name=database.name, collection_name=collection,
                        is_atlas=database.atlas,
                        key_prefix=self.key_prefix, key_size=self.key_size, doc_size=self.doc_size,
                        key_type=database.key_type, value_type=valType,
                        create_percent=pattern[0], read_percent=pattern[1], update_percent=pattern[2],
                        delete_percent=pattern[3], expiry_percent=pattern[4],
                        create_start_index=database.create_start , create_end_index=database.create_end,
                        read_start_index=database.read_start, read_end_index=database.read_end,
                        update_start_index=database.update_start, update_end_index=database.update_end,
                        delete_start_index=database.delete_start, delete_end_index=database.delete_end,
                        expiry_start_index=database.expire_start, expiry_end_index=database.expire_end,
                        process_concurrency=self.process_concurrency, task_identifier="", ops=per_coll_ops,
                        suppress_error_table=False,
                        track_failures=True,
                        mutate=self.mutate,
                        )
                loader_map.update({database.name+collection: loader})
        return loader_map

    def perform_load(self, databases, wait_for_load=True, overRidePattern=None, workers=10):
        loader_map = self._loader_dict(databases, overRidePattern)
        tasks = list()
        for database in databases:
            for collection in database.collections:
                loader = loader_map[database.name+collection]
                result, json_response = loader.create_doc_load_task()
                if not result:
                    self.log.critical("Failed to create doc load task: %s" % json_response)
                self.doc_loading_tm.add_new_task(loader)
                tasks.append(loader)

        if wait_for_load:
            self.wait_for_doc_load_completion(tasks)
        else:
            return tasks

    def wait_for_doc_load_completion(self, tasks):
        for task in tasks:
            task.result = self.doc_loading_tm.get_task_result(task)
            if task.result is False:
                self.log.critical("Task failed: %s" % task.task_id)


class CouchbaseRemoteCluster(object):
    def __init__(self, remoteCluster, bucket_util):
        self.log = logger.get("test")
        self.remoteCluster = remoteCluster
        self.remoteClusterCA = remoteCluster.root_ca
        self.bucket_util = bucket_util
        self.name = self.type = "remote"
        self.type = self.dataset = "remote"
        self.collections = list()
        self.link_name = "remote_" + ''.join([random.choice(string.ascii_letters + string.digits) for _ in range(5)])
        self.links = [self.link_name]
        self.cbas_queries = list()
        self.cbas_collections = list()
        self.query_map = dict()

    def set_collections(self):
        pass

    def create_link(self, columnar):
        while columnar.state != "ACTIVE":
            time.sleep(60)
            continue
        rest = CBASHelper(columnar.master)
        params = {
            "dataverse": "Default",
            "name": self.link_name,
            "type": "couchbase",
            "hostname": str(self.remoteCluster.master.ip),
            "username": self.remoteCluster.username,
            "password": self.remoteCluster.password,
            "encryption": "full",
            "certificate": self.remoteClusterCA
        }
        pprint.pprint(params)
        result, status, content, errors = rest.analytics_link_operations(method="POST", params=params)
        if not result:
            raise Exception("Status: %s, content: %s, Errors: %s" % (status, content, errors))

    def create_cbas_collections(self, columnar, remote_collections=None, skip_init=False):
        while columnar.state != "ACTIVE":
            time.sleep(60)
            continue
        i = 0
        client = columnar.SDKClients[0].cluster
        # self.cbas_collections = list()
        new_collections = list()
        while i < remote_collections:
            for b in self.remoteCluster.buckets:
                for s in self.bucket_util.get_active_scopes(b, only_names=True):
                    if s == CbServer.system_scope:
                        continue
                    for _, c in enumerate(sorted(self.bucket_util.get_active_collections(b, s, only_names=True))):
                        if c == CbServer.default_collection:
                            continue
                        cbas_coll_name = self.link_name + "_volCollection_" + str(i)
                        cbas_coll_name = cbas_coll_name + "_" + "".join([random.choice(string.ascii_lowercase) for _ in range(5)])
                        self.log.info("creating remote collections on couchbase: %s" % cbas_coll_name)
                        statement = 'CREATE COLLECTION `{}` ON {}.{}.{} AT `{}`'.format(
                                                cbas_coll_name, b.name, s, c, self.link_name)
                        new_collections.append(cbas_coll_name)
                        if not skip_init:
                            execute_statement_on_cbas(client, statement)
                        i += 1
                        if remote_collections and i == remote_collections:
                            self.cbas_collections.extend(new_collections)
                            return new_collections


class s3(object):
    def __init__(self, username=None, password=None):
        self.accessKeyId = username
        self.secretAccessKey = password
        self.name = self.type = "s3"
        self.dataset = "external"
        self.collections = list()
        self.primary_key = None
        self.region = "us-west-2"
        self.type = "s3"
        self.link_name = "s3_" + ''.join([random.choice(string.ascii_letters + string.digits) for _ in range(5)])
        self.links = [self.link_name]
        self.cbas_queries = list()
        self.cbas_collections = list()
        self.query_map = dict()
        self.log = logger.get("test")

    def set_collections(self):
        for i in range(self.loadDefn.get("collections")):
            self.collections.append("volCollection" + str(i))

    def create_link(self, cluster):
        while cluster.state != "ACTIVE":
            time.sleep(60)
            continue
        self.log.info("creating link over s3")
        rest = CBASHelper(cluster.master)
        params = {'dataverse': 'Default', "name": self.link_name,
                  "type": "s3", 'accessKeyId': self.accessKeyId,
                  'secretAccessKey': self.secretAccessKey,
                  "region": self.region,
                  "crossRegion": True}
        result, status, content, errors = rest.analytics_link_operations(method="POST", params=params)
        if not result:
            raise Exception("Status: %s, content: %s, Errors: %s"
                            .format(status, content, errors))

    def create_cbas_collections(self, cluster, external_collections=None, skip_init=False):
        while cluster.state != "ACTIVE":
            time.sleep(60)
            continue
        client = cluster.SDKClients[0].cluster
        num_collections = external_collections or len(self.collections)
        new_collections = list()
        i = 0
        while i < num_collections:
            cbas_coll_name = self.link_name + "_external_volCollection_" + str(i)
            new_collections.append(cbas_coll_name)
            i += 1
            self.log.info("creating external collections: %s" % cbas_coll_name)
            statement = 'CREATE EXTERNAL COLLECTION `%s`  ON `%s` AT `%s`  PATH "%s" WITH {"format":"json"}' % (
                cbas_coll_name, "columnartest", self.link_name, "hotel")
            self.log.info(statement)
            if not skip_init:
                execute_statement_on_cbas(client, statement)
        self.cbas_collections.append(new_collections)
        self.copy_from_s3_into_standalone(cluster, external_collections)
        return new_collections

    def copy_from_s3_into_standalone(self, cluster, standalone_collections=1):
        while cluster.state != "ACTIVE":
            time.sleep(60)
            continue
        client = cluster.SDKClients[0].cluster
        num_collections = standalone_collections or len(self.collections)
        self.log.info("creating standalone collections - datasource is s3")
        i = 0
        while i < num_collections:
            cbas_coll_name = self.link_name + "_standalone_volCollection_" + str(i)
            self.cbas_collections.append(cbas_coll_name)
            i += 1

            self.log.info("Creating standalone collections: %s" % cbas_coll_name)
            statement = 'CREATE COLLECTION `{}`  PRIMARY KEY (_id: UUID) AUTOGENERATED'.format(cbas_coll_name)
            execute_statement_on_cbas(client, statement)

            statement = 'COPY into {0} FROM {1} AT {2} PATH "{3}" '.format(
                cbas_coll_name, "columnartest", self.link_name, "hotel") + 'WITH { "format": "json"};'
            self.log.info("COPYING into standalone collections: %s" % cbas_coll_name)
            self.log.info(statement)
            try:
                execute_statement_on_cbas(client, statement)
            except AmbiguousTimeoutException:
                pass


class KafkaClusterUtils():
    
    def __init__(self):
        self.log = logger.get("test")
        
        config = {
            'bootstrap.servers': "{}:9092".format(_input.kafka.get("cluster_url")),
            'security.protocol': "SASL_SSL",
            'sasl.mechanisms': "PLAIN",
            'sasl.username': _input.kafka.get("cluster_username"),
            'sasl.password': _input.kafka.get("cluster_password")
        }

        self.kafkaAdminClient = AdminClient(config);

    def deleteKafkaTopics(self, topics):
        deleteTopicsResult = self.kafkaAdminClient.delete_topics(topics)
        while not deleteTopicsResult.all().isDone():
            for topic in topics:
                print("still deleting: %s" % self.listKafkaTopics(topic))
            time.sleep(5)

    def listKafkaTopics(self, prefix=None):
        topics = self.kafkaAdminClient.list_topics().topics
        if prefix:
            return [topic for topic in topics if topic.find(prefix) != -1]
        self.log.info("Current kafka topics present: %s" % topics)
        return topics
