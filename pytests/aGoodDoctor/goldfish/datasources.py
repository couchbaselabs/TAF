'''
Created on Nov 3, 2023

@author: ritesh.agarwal
'''
from java.lang import String
from java.util import HashMap, Properties
import json
import pprint
import random
import string
import time

from CbasLib.CBASOperations import CBASHelper
from CbasUtil import execute_statement_on_cbas
from com.couchbase.client.core.error import AmbiguousTimeoutException
from com.couchbase.test.docgen import DRConstants
from com.couchbase.test.docgen import DocRange
from com.couchbase.test.docgen import WorkLoadSettings
from com.couchbase.test.sdk import Server
from com.couchbase.test.taskmanager import TaskManager
from com.mongo.loader import MongoSDKClient, WorkLoadGenerate, DocumentGenerator
from constants.cb_constants.CBServer import CbServer
from global_vars import logger
from kafka_util.kafka_connect_util import KafkaConnectUtil
from org.apache.kafka.clients.admin import AdminClientConfig, AdminClient
from TestInput import TestInputSingleton

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
            self.connString = self.connString + "+srv";
            self.type = "onCloud"
        self.connString = self.connString + "://" + self.username + ":" + self.password + "@" + self.hostname + ":27017/"

    def set_collections(self):
        master = Server(self.hostname, self.port,
                            self.username, self.password,
                            "")
        for i in range(self.loadDefn.get("collections")):
            self.collections.append("volCollection" + str(i))
            client = MongoSDKClient(master, self.name, "volCollection" + str(i), self.atlas)
            client.connectCluster()
            client.disconnectCluster()
            time.sleep(2)

    def drop(self):
        master = Server(self.hostname, self.port,
                            self.username, self.password,
                            "")
        client = MongoSDKClient(master, self.name, self.collections[0], self.atlas)
        client.connectCluster()
        client.dropDatabase()
        client.disconnectCluster()

    def setup_kafka_connectors(self, prefix):
        self.prefix = prefix
        connector_util = KafkaConnectUtil("http://{}:8083".format(_input.kafka.get("connector")))
        config = connector_util.generate_mongo_connector_config(
            "mongodb://{0}:{1}@{2}:{3}/?retryWrites=true&w=majority&replicaSet=rs0".format(
                self.username, self.password, self.hostname, self.port),
            mongo_collections=[self.source_name + "." + coll_name for coll_name in self.collections],
            topic_prefix=prefix,
            partitions=32)
        self.kafka_topics = [prefix + "." + self.source_name + "." + coll_name for coll_name in self.collections]
        status = connector_util.deploy_connector(self.source_name, config)
        if status:
            self.log.info("connectors are deployed properly!! Check for the topics")

    def create_link(self, cluster):
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

        result, status, content, errors = rest.analytics_link_operations(method="POST", params=kafka_details)
        if not result:
            raise Exception("Status: %s, content: %s, Errors: %s" % (status, content, errors))

    def create_cbas_collections(self, cluster, num_collections=None):
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
            execute_statement_on_cbas(client, statement)
        self.cbas_collections.extend(new_collections)
        return new_collections

    @staticmethod
    def perform_load(databases, wait_for_load=True, overRidePattern=None, tm=None, workers=10):
        loader_map = Loader._loader_dict(databases, overRidePattern)
        tasks = list()
        i = workers
        while i > 0:
            for database in databases:
                master = Server(database.hostname, database.port,
                                database.username, database.password,
                                "")
                for collection in database.collections:
                    client = MongoSDKClient(master, database.name, collection, database.atlas)
                    client.connectCluster()
                    time.sleep(1)
                    taskName = "Loader_%s_%s_%s" % (database.name, collection, time.time())
                    task = WorkLoadGenerate(taskName, loader_map[database.name+collection], client)
                    tasks.append(task)
                    tm.submit(task)
                    i -= 1

        if wait_for_load:
            tm.getAllTaskResult()
        else:
            return tasks


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

    def create_cbas_collections(self, columnar, remote_collections=None):
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
        self.region = "us-east-1"
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
        self.log.info("creating link over s3")
        rest = CBASHelper(cluster.master)
        params = {'dataverse': 'Default', "name": self.link_name, "type": "s3", 'accessKeyId': self.accessKeyId,
                  'secretAccessKey': self.secretAccessKey,
                  "region": self.region}
        result, status, content, errors = rest.analytics_link_operations(method="POST", params=params)
        if not result:
            raise Exception("Status: %s, content: %s, Errors: %s".format(status, content, errors))

    def create_cbas_collections(self, cluster, external_collections=None):
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
            execute_statement_on_cbas(client, statement)
        self.cbas_collections.append(new_collections)
        self.copy_from_s3_into_standalone(cluster, external_collections)
        return new_collections

    def copy_from_s3_into_standalone(self, cluster, standalone_collections=1):
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


class Loader():

    @staticmethod
    def _loader_dict(databases, overRidePattern=None, workers=10, cmd={}):
        workers = workers
        loader_map = dict()
        default_pattern = [100, 0, 0, 0, 0]
        for database in databases:
            pattern = overRidePattern or database.loadDefn.get("pattern", default_pattern)
            for i, collection in enumerate(database.collections):
                workloads = database.loadDefn.get("collections_defn", [database.loadDefn])
                valType = workloads[i % len(workloads)]["valType"]
                ws = WorkLoadSettings(cmd.get("keyPrefix", database.key),
                                      cmd.get("keySize", database.key_size),
                                      cmd.get("docSize", database.loadDefn.get("doc_size")),
                                      cmd.get("cr", pattern[0]),
                                      cmd.get("rd", pattern[1]),
                                      cmd.get("up", pattern[2]),
                                      cmd.get("dl", pattern[3]),
                                      cmd.get("ex", pattern[4]),
                                      cmd.get("workers", workers),
                                      cmd.get("ops", database.loadDefn.get("ops")),
                                      cmd.get("loadType", None),
                                      cmd.get("keyType", database.key_type),
                                      cmd.get("valueType", valType),
                                      cmd.get("validate", False),
                                      cmd.get("gtm", False),
                                      cmd.get("deleted", False),
                                      cmd.get("mutated", 0)
                                      )
                hm = HashMap()
                hm.putAll({DRConstants.create_s: database.create_start,
                           DRConstants.create_e: database.create_end,
                           DRConstants.update_s: database.update_start,
                           DRConstants.update_e: database.update_end,
                           DRConstants.expiry_s: database.expire_start,
                           DRConstants.expiry_e: database.expire_end,
                           DRConstants.delete_s: database.delete_start,
                           DRConstants.delete_e: database.delete_end,
                           DRConstants.read_s: database.read_start,
                           DRConstants.read_e: database.read_end})
                dr = DocRange(hm)
                ws.dr = dr
                dg = DocumentGenerator(ws, database.key_type, valType)
                loader_map.update({database.name+collection: dg})
        return loader_map


class KafkaClusterUtils():
    
    def __init__(self):
        username = _input.kafka.get("cluster_username");
        password = _input.kafka.get("cluster_password");
        
        jaasTemplate = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";";
        jaasCfg = String.format(jaasTemplate, username, password);

        props = Properties()
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "{}:9092".format(_input.kafka.get("cluster_url")));
        props.put("session.timeout.ms", "30000");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config", jaasCfg);

        self.kafkaAdminClient = AdminClient.create(props);

    def deleteKafkaTopics(self, topics):
        deleteTopicsResult = self.kafkaAdminClient.deleteTopics(topics)
        while not deleteTopicsResult.all().isDone():
            for topic in topics:
                print "still deleting: %s" % self.listKafkaTopics(topic) 
            time.sleep(5)

    def listKafkaTopics(self, prefix=None):
        topicsList = self.kafkaAdminClient.listTopics();
        topics = list()
        for topic in topicsList.listings().get(): 
            topics.append(topic.name())
        # self.log.info("Current kafta topics present: %s" % topics)
        if prefix:
            return [topic for topic in topics if topic.find(prefix) != -1]
        return topics
