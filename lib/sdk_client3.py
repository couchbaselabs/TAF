#!/usr/bin/env python
"""
Java based SDK client interface
Created on Mar 14, 2019

"""

import json as pyJson
import logging

from com.couchbase.client.core.env import \
    SeedNode, \
    TimeoutConfig, IoConfig
from com.couchbase.client.core.error import \
    CasMismatchException, \
    ConfigException, \
    CouchbaseException, \
    DocumentExistsException, \
    DocumentNotFoundException, \
    DurabilityAmbiguousException, \
    RequestCanceledException, \
    ServerOutOfMemoryException, \
    TemporaryFailureException, \
    TimeoutException
from com.couchbase.client.core.msg.kv import DurabilityLevel
from com.couchbase.client.core.retry import FailFastRetryStrategy
from com.couchbase.client.java import Cluster, ClusterOptions
from com.couchbase.client.java.codec import RawBinaryTranscoder
from com.couchbase.client.java.env import ClusterEnvironment
from com.couchbase.client.java.json import JsonObject
from com.couchbase.client.java.manager.collection import CollectionSpec
from com.couchbase.client.java.kv import \
    GetAllReplicasOptions, \
    GetOptions, \
    InsertOptions, \
    MutateInOptions, \
    PersistTo, \
    RemoveOptions, \
    ReplaceOptions, \
    ReplicateTo, \
    TouchOptions, \
    UpsertOptions
from java.time import Duration
from java.nio.charset import StandardCharsets
from java.time.temporal import ChronoUnit
from java.lang import System, String
from java.util import \
    Collections, \
    HashSet, \
    Optional
from java.util.logging import \
    ConsoleHandler, \
    Level, \
    Logger
from reactor.util.function import Tuples

import com.couchbase.test.doc_operations_sdk3.doc_ops as doc_op
import com.couchbase.test.doc_operations_sdk3.SubDocOperations as sub_doc_op

from Cb_constants import ClusterRun, CbServer
from couchbase_helper.durability_helper import DurabilityHelper


class SDKClient(object):
    sdk_connections = 0
    sdk_disconnections = 0
    doc_op = doc_op()
    sub_doc_op = sub_doc_op()
    """
    Java SDK Client Implementation for testrunner - master branch
    """

    def __init__(self, servers, bucket,
                 scope=CbServer.default_scope,
                 collection=CbServer.default_collection,
                 username="Administrator", password="password",
                 certpath=None, compression=True):
        # Used during Cluster.connect() call
        self.hosts = list()

        # Used while creating connection for Cluster_run
        self.servers = tuple()

        self.scope_name = scope
        self.collection_name = collection
        self.username = username
        self.password = password
        self.default_timeout = 0
        self.cluster = None
        self.bucket = bucket
        self.bucket_name = bucket
        self.bucketObj = None
        self.collection = None
        self.log = logging.getLogger("test")

        if hasattr(bucket, 'name'):
            self.bucket_name = bucket.name

        for server in servers:
            self.servers += (server.ip, int(server.port))
            if server.ip == "127.0.0.1":
                self.hosts.append("%s:%s" % (server.ip, server.port))
                self.scheme = "http"
            else:
                self.hosts.append(server.ip)
                self.scheme = "couchbase"

        self.__create_conn()
        SDKClient.sdk_connections += 1

    def __create_conn(self):
        try:
            self.log.debug("Creating cluster connection")
            System.setProperty("com.couchbase.forceIPv4", "false")
            logger = Logger.getLogger("com.couchbase.client")
            logger.setLevel(Level.SEVERE)
            for h in logger.getParent().getHandlers():
                if isinstance(h, ConsoleHandler):
                    h.setLevel(Level.SEVERE)
            cluster_env = \
                ClusterEnvironment \
                .builder() \
                .ioConfig(IoConfig.numKvConnections(25)) \
                .timeoutConfig(TimeoutConfig.builder()
                               .connectTimeout(Duration.ofSeconds(20))
                               .kvTimeout(Duration.ofSeconds(10)))

            cluster_options = \
                ClusterOptions \
                .clusterOptions(self.username, self.password) \
                .environment(cluster_env.build())
            i = 1
            while i <= 5:
                try:
                    # Code for cluster_run
                    if int(self.servers[0][1]) in xrange(ClusterRun.port,
                                                         ClusterRun.port+10):
                        master_seed = HashSet(Collections.singletonList(
                            SeedNode.create(
                                self.servers[0][0],
                                Optional.of(ClusterRun.memcached_port),
                                Optional.of(int(self.servers[0][1])))))
                        cluster_options = \
                            cluster_options.seedNodes(master_seed)
                    self.cluster = Cluster.connect(
                            ", ".join(self.hosts).replace(" ", ""),
                            cluster_options)
                    break
                except ConfigException as e:
                    self.log.error("Exception during cluster connection: %s"
                                   % e)
                    i += 1

            self.bucketObj = self.cluster.bucket(self.bucket_name)
            self.select_collection(self.scope_name, self.collection_name)
        except Exception as e:
            raise Exception("SDK Connection error: " + str(e))

    def close(self):
        self.log.debug("Closing down the cluster")
        if self.cluster:
            self.cluster.disconnect()
            self.cluster.environment().shutdown()
            self.log.debug("Cluster disconnected and env shutdown")
            SDKClient.sdk_disconnections += 1

    # Translate APIs for document operations
    def translate_to_json_object(self, value, doc_type="json"):
        if type(value) == JsonObject and doc_type == "json":
            return value
        json_obj = JsonObject.create()
        try:
            if doc_type.find("json") != -1:
                if type(value) != dict:
                    value = pyJson.loads(value)
                for field, val in value.items():
                    json_obj.put(field, val)
                return json_obj
            elif doc_type.find("binary") != -1:
                value = String(value)
                return value.getBytes(StandardCharsets.UTF_8)
            else:
                return value
        except Exception as e:
            pass

        return json_obj

    def __translate_upsert_multi_results(self, data):
        success = dict()
        fail = dict()
        if data is None:
            return success, fail
        for item in data:
            result = item['status']
            key = item['id']
            json_object = item["document"]
            if result:
                success[key] = dict()
                success[key]['value'] = json_object
                success[key]['cas'] = item['cas']
            else:
                fail[key] = dict()
                fail[key]['cas'] = item['cas']
                fail[key]['value'] = json_object
                fail[key]['error'] = item['error']
        return success, fail

    def __tranlate_delete_multi_results(self, data):
        success = dict()
        fail = dict()
        if data is None:
            return success, fail
        for result in data:
            key = result['id']
            if result['status']:
                success[key] = dict()
                success[key]['cas'] = result['cas']
            else:
                fail[key] = dict()
                fail[key]['cas'] = result['cas']
                fail[key]['error'] = result['error']
                fail[key]['value'] = dict()
        return success, fail

    def __translate_get_multi_results(self, data):
        success = dict()
        fail = dict()
        if data is None:
            return success, fail
        for result in data:
            key = result['id']
            if result['status']:
                success[key] = dict()
                success[key]['value'] = result['content']
                success[key]['cas'] = result['cas']
            else:
                fail[key] = dict()
                fail[key]['cas'] = result['cas']
                fail[key]['error'] = result['error']
                fail[key]['value'] = dict()
        return success, fail

    # Translate APIs for sub-document operations
    def __translate_upsert_multi_sub_doc_result(self, data):
        success = dict()
        fail = dict()
        if data is None:
            return success, fail
        for item in data:
            result = item['status']
            key = item['id']
            json_object = item["result"]
            if result:
                success[key] = dict()
                success[key]['value'] = json_object
                success[key]['cas'] = item['cas']
            else:
                fail[key] = dict()
                fail[key]['value'] = json_object
                fail[key]['error'] = item['error']
                fail[key]['cas'] = 0
        return success, fail

    # Document operations' getOptions APIs
    def getInsertOptions(self, exp=0, exp_unit="seconds",
                         persist_to=0, replicate_to=0,
                         timeout=5, time_unit="seconds",
                         durability=""):
        if durability:
            options = InsertOptions.insertOptions() \
                .timeout(self.getDuration(timeout, time_unit)) \
                .expiry(self.getDuration(exp, exp_unit)) \
                .durability(DurabilityHelper.getDurabilityLevel(durability))
        else:
            options = InsertOptions.insertOptions() \
                .timeout(self.getDuration(timeout, time_unit)) \
                .expiry(self.getDuration(exp, exp_unit)) \
                .durability(self.getPersistTo(persist_to),
                            self.getReplicateTo(replicate_to))
        return options

    def getReadOptions(self, timeout, time_unit="seconds"):
        return GetOptions.getOptions() \
            .timeout(self.getDuration(timeout, time_unit))

    def getUpsertOptions(self, exp=0, exp_unit="seconds",
                         persist_to=0, replicate_to=0,
                         timeout=5, time_unit="seconds",
                         durability=""):
        if durability:
            options = UpsertOptions.upsertOptions() \
                .timeout(self.getDuration(timeout, time_unit)) \
                .expiry(self.getDuration(exp, exp_unit)) \
                .durability(DurabilityHelper.getDurabilityLevel(durability))
        else:
            options = UpsertOptions.upsertOptions() \
                .timeout(self.getDuration(timeout, time_unit)) \
                .expiry(self.getDuration(exp, exp_unit)) \
                .durability(self.getPersistTo(persist_to),
                            self.getReplicateTo(replicate_to))
        return options

    def getRemoveOptions(self, persist_to=0, replicate_to=0,
                         timeout=5, time_unit="seconds",
                         durability="", cas=0):
        options = RemoveOptions.removeOptions() \
                  .timeout(self.getDuration(timeout, time_unit))

        if cas > 0:
            options = options.cas(cas)

        if durability:
            options = options \
                .durability(DurabilityHelper.getDurabilityLevel(durability))
        else:
            options = options \
                .durability(self.getPersistTo(persist_to),
                            self.getReplicateTo(replicate_to))

        return options

    def getReplaceOptions(self, persist_to=0, replicate_to=0,
                          timeout=5, time_unit="seconds",
                          durability="", cas=0):
        options = ReplaceOptions.replaceOptions() \
            .timeout(self.getDuration(timeout, time_unit))

        if cas > 0:
            options = options.cas(cas)

        if durability:
            options = options \
                .durability(DurabilityHelper.getDurabilityLevel(durability))
        else:
            options = options \
                .durability(self.getPersistTo(persist_to),
                            self.getReplicateTo(replicate_to))

        return options

    def getTouchOptions(self, timeout=5, time_unit="seconds"):
        return TouchOptions.touchOptions() \
            .timeout(self.getDuration(timeout, time_unit))

    def getMutateInOptions(self, exp=0, exp_unit="seconds",
                           persist_to=0, replicate_to=0, timeout=5,
                           time_unit="seconds", durability=""):
        if persist_to != 0 or replicate_to != 0:
            return MutateInOptions.mutateInOptions().durability(
                self.getPersistTo(persist_to), self.getReplicateTo(
                    replicate_to)).expiry(
                self.getDuration(exp, exp_unit)).timeout(
                self.getDuration(timeout, time_unit))
        else:
            return MutateInOptions.mutateInOptions()\
                .durability(DurabilityHelper.getDurabilityLevel(durability))\
                .expiry(self.getDuration(exp, exp_unit))\
                .timeout(self.getDuration(timeout, time_unit))

    def getPersistTo(self, persist_to):
        try:
            persist_list = [PersistTo.NONE, PersistTo.ONE, PersistTo.TWO,
                            PersistTo.THREE, PersistTo.FOUR]
            return persist_list[persist_to]
        except Exception as e:
            pass

        return PersistTo.ACTIVE

    def getReplicateTo(self, replicate_to):
        try:
            replicate_list = [ReplicateTo.NONE, ReplicateTo.ONE,
                              ReplicateTo.TWO, ReplicateTo.THREE]
            return replicate_list[replicate_to]
        except Exception:
            pass

        return ReplicateTo.NONE

    def getDuration(self, time, time_unit):
        time_unit = time_unit.lower()
        if time_unit == "milliseconds":
            temporal_unit = ChronoUnit.MILLIS
        elif time_unit == "minutes":
            temporal_unit = ChronoUnit.MINUTES
        elif time_unit == "hours":
            temporal_unit = ChronoUnit.HOURS
        elif time_unit == "days":
            temporal_unit = ChronoUnit.DAYS
        elif time_unit == "minutes":
            temporal_unit = ChronoUnit.MINUTES
        else:
            temporal_unit = ChronoUnit.SECONDS

        return Duration.of(time, temporal_unit)

    # Scope/Collection APIs
    def collection_manager(self):
        """
        :return collection_manager object:
        """
        return self.bucketObj.collections()

    @staticmethod
    def get_collection_spec(scope="_default", collection="_default"):
        """
        Returns collection_spec object for further usage in tests.

        :param scope: - Name of the scope
        :param collection: - Name of the collection
        :return CollectionSpec object:
        """
        return CollectionSpec.create(collection, scope)

    def select_collection(self, scope_name, collection_name):
        """
        Method to select collection. Can be called directly from test case.
        """
        if collection_name != CbServer.default_collection:
            self.collection = self.bucketObj \
                .scope(scope_name) \
                .collection(collection_name)
        else:
            self.collection = self.bucketObj.defaultCollection()

    def create_scope(self, scope):
        """
        Create a scope using the given name
        :param scope: Scope name to be created
        """
        self.collection_manager().createScope(scope)

    def drop_scope(self, scope):
        """
        Drop a scope using the given name
        :param scope: Scope name to be dropped
        """
        self.collection_manager().dropScope(scope)

    def create_collection(self, collection, scope=CbServer.default_scope):
        """
        API to creae collection under a particular scope
        :param collection: Collection name to be created
        :param scope: Scope under which the collection is
                      going to be created
                      default: Cb_Server's default scope name
        """
        collection_spec = SDKClient.get_collection_spec(scope,
                                                        collection)
        self.collection_manager().createCollection(collection_spec)

    def drop_collection(self, scope=CbServer.default_scope,
                        collection=CbServer.default_collection):
        """
        API to drop the collection (if exists)
        :param scope: Scope name for targeted collection
                      default: Cb_Server's default scope name
        :param collection: Targeted collection name to drop
                           default: Cb_Server's default collection name
        """
        collection_spec = SDKClient.get_collection_spec(scope,
                                                        collection)
        self.collection_manager().dropCollection(collection_spec)

    # Singular CRUD APIs
    def delete(self, key, persist_to=0, replicate_to=0,
               timeout=5, time_unit="seconds",
               durability="", cas=0, fail_fast=False):
        result = dict()
        result["cas"] = -1
        try:
            options = self.getRemoveOptions(persist_to=persist_to,
                                            replicate_to=replicate_to,
                                            timeout=timeout,
                                            time_unit=time_unit,
                                            durability=durability,
                                            cas=cas)
            if fail_fast:
                options = options.retryStrategy(FailFastRetryStrategy.INSTANCE)
            delete_result = self.collection.remove(key, options)
            result.update({"key": key, "value": None,
                           "error": None, "status": True,
                           "cas": delete_result.cas()})
        except DocumentNotFoundException as e:
            self.log.warning("Exception: Document id {0} not found - {1}"
                             .format(key, e))
            result.update({"key": key, "value": None,
                           "error": str(e), "status": False})
        except CasMismatchException as e:
            self.log.warning("Exception: Cas mismatch for doc {0} - {1}"
                             .format(key, e))
            result.update({"key": key, "value": None,
                           "error": str(e), "status": False})
        except TemporaryFailureException as e:
            self.log.warning("Exception: Retry for doc {0} - {1}"
                             .format(key, e))
            result.update({"key": key, "value": None,
                           "error": str(e), "status": False})
        except CouchbaseException as e:
            self.log.warning("Generic exception for doc {0} - {1}"
                            .format(key, e))
            result.update({"key": key, "value": None,
                           "error": str(e), "status": False})
        except (RequestCanceledException, TimeoutException) as ex:
            self.log.warning("Request cancelled/timed-out: " + str(ex))
            result.update({"key": key, "value": None,
                           "error": str(ex), "status": False})
        except Exception as e:
            self.log.error("Error during remove of {0} - {1}".format(key, e))
            result.update({"key": key, "value": None,
                           "error": str(e), "status": False})
        return result

    def insert(self, key, value, exp=0, exp_unit="seconds",
               persist_to=0, replicate_to=0,
               timeout=5, time_unit="seconds",
               doc_type="json",
               durability="", fail_fast=False):

        result = dict()
        result["cas"] = 0
        content = self.translate_to_json_object(value)
        try:
            options = self.getInsertOptions(exp=exp, exp_unit=exp_unit,
                                            persist_to=persist_to,
                                            replicate_to=replicate_to,
                                            timeout=timeout,
                                            time_unit=time_unit,
                                            durability=durability)
            if doc_type == "binary":
                options = options.transcoder(RawBinaryTranscoder.INSTANCE)
            if fail_fast:
                options = options.retryStrategy(FailFastRetryStrategy.INSTANCE)

            # Returns com.couchbase.client.java.kv.MutationResult object
            insert_result = self.collection.insert(key, content, options)
            result.update({"key": key, "value": content,
                           "error": None, "status": True,
                           "cas": insert_result.cas()})
        except DocumentExistsException as ex:
            self.log.warning("The document already exists! => " + str(ex))
            result.update({"key": key, "value": content,
                           "error": str(ex), "status": False})
        except (RequestCanceledException, TimeoutException) as ex:
            self.log.warning("Request cancelled/timed-out: " + str(ex))
            result.update({"key": key, "value": content,
                           "error": str(ex), "status": False})
        except DurabilityAmbiguousException as e:
            self.log.warning("D_Ambiguous for key %s" % key)
            result.update({"key": key, "value": None,
                           "error": str(e), "status": False})
        except ServerOutOfMemoryException as ex:
            self.log.warning("OOM exception: %s" % ex)
            result.update({"key": key, "value": content,
                           "error": str(ex), "status": False})
        except Exception as ex:
            self.log.error("Something else happened: " + str(ex))
            result.update({"key": key, "value": content,
                           "error": str(ex), "status": False})
        return result

    def replace(self, key, value, exp=0, exp_unit="seconds",
                persist_to=0, replicate_to=0,
                timeout=5, time_unit="seconds",
                durability="", cas=0, fail_fast=False):
        result = dict()
        result["cas"] = 0
        content = self.translate_to_json_object(value)
        try:
            options = self.getReplaceOptions(persist_to=persist_to,
                                             replicate_to=replicate_to,
                                             timeout=timeout,
                                             time_unit=time_unit,
                                             durability=durability,
                                             cas=cas)
            if fail_fast:
                options = options.retryStrategy(FailFastRetryStrategy.INSTANCE)

            # Returns com.couchbase.client.java.kv.MutationResult object
            replace_result = self.collection.replace(key, content, options)
            result.update({"key": key, "value": content,
                           "error": None, "status": True,
                           "cas": replace_result.cas()})
        except DocumentExistsException as ex:
            self.log.warning("The document already exists! => " + str(ex))
            result.update({"key": key, "value": content,
                           "error": str(ex), "status": False})
        except CasMismatchException as e:
            self.log.warning("Exception: Cas mismatch for doc {0} - {1}"
                             .format(key, e))
            result.update({"key": key, "value": None,
                           "error": str(e), "status": False})
        except DocumentNotFoundException as e:
            self.log.warning("Key '%s' not found!" % key)
            result.update({"key": key, "value": None,
                           "error": str(e), "status": False})
        except DurabilityAmbiguousException as e:
            self.log.warning("D_Ambiguous for key %s" % key)
            result.update({"key": key, "value": None,
                           "error": str(e), "status": False})
        except (RequestCanceledException, TimeoutException) as ex:
            self.log.warning("Request cancelled/timed-out: " + str(ex))
            result.update({"key": key, "value": None,
                           "error": str(ex), "status": False})
        except Exception as ex:
            self.log.error("Something else happened: " + str(ex))
            result.update({"key": key, "value": content,
                           "error": str(ex), "status": False})
        return result

    def touch(self, key, exp=0, exp_unit="seconds",
              persist_to=0, replicate_to=0,
              durability="", timeout=5, time_unit="seconds", fail_fast=False):
        result = {
            "key": key,
            "value": None,
            "cas": 0,
            "status": False,
            "error": None
        }
        touch_options = self.getTouchOptions(timeout, time_unit)
        if fail_fast:
            touch_options = touch_options.retryStrategy(
                FailFastRetryStrategy.INSTANCE)
        try:
            touch_result = self.collection.touch(
                key,
                self.getDuration(exp, exp_unit),
                touch_options)
            result.update({"status": True, "cas": touch_result.cas()})
        except DocumentNotFoundException as e:
            self.log.warning("Document key '%s' not found!" % key)
            result["error"] = str(e)
        except (RequestCanceledException, TimeoutException) as ex:
            self.log.warning("Request cancelled/timed-out: " + str(ex))
            result.update({"key": key, "value": None,
                           "error": str(ex), "status": False})
        except Exception as ex:
            self.log.error("Something else happened: " + str(ex))
            result.update({"key": key, "value": None,
                           "error": str(ex), "status": False})
        return result

    def read(self, key, timeout=5, time_unit="seconds", fail_fast=False):
        result = {
            "key": key,
            "value": None,
            "cas": 0,
            "status": False,
            "error": None
        }
        read_options = self.getReadOptions(timeout, time_unit)
        if fail_fast:
            read_options = read_options.retryStrategy(
                FailFastRetryStrategy.INSTANCE)
        try:
            get_result = self.collection.get(key, read_options)
            self.log.debug("Found document: cas=%s, content=%s"
                           % (str(get_result.cas()),
                              str(get_result.contentAsObject())))
            result["status"] = True
            result["value"] = str(get_result.contentAsObject())
            result["cas"] = get_result.cas()
        except DocumentNotFoundException as e:
            self.log.warning("Document key '%s' not found!" % key)
            result.update({"key": key, "value": None,
                           "error": str(e), "status": False})
        except (RequestCanceledException, TimeoutException) as ex:
            self.log.warning("Request cancelled/timed-out: " + str(ex))
            result.update({"key": key, "value": None,
                           "error": str(ex), "status": False})
        except Exception as ex:
            self.log.error("Something else happened: " + str(ex))
            result.update({"key": key, "value": None,
                           "error": str(ex), "status": False})
        return result

    def getFromAllReplica(self, key):
        result = []
        getResult = self.collection.getAllReplicas(key, GetAllReplicasOptions.getAllReplicasOptions())
        try:
            getResult = getResult.toArray()
            if getResult:
                for item in getResult:
                    result.append({"key": key,
                                   "value": item.contentAsObject(),
                                   "cas": item.cas(), "status": True})
        except:
            pass
        return result

    def upsert(self, key, value, exp=0, exp_unit="seconds",
               persist_to=0, replicate_to=0,
               timeout=5, time_unit="seconds",
               durability="", fail_fast=False):
        content = self.translate_to_json_object(value)
        result = dict()
        result["cas"] = 0
        try:
            options = self.getUpsertOptions(exp=exp, exp_unit=exp_unit,
                                            persist_to=persist_to,
                                            replicate_to=replicate_to,
                                            timeout=timeout,
                                            time_unit=time_unit,
                                            durability=durability)
            if fail_fast:
                options = options.retryStrategy(FailFastRetryStrategy.INSTANCE)

            upsertResult = self.collection.upsert(key, content, options)
            result.update({"key": key, "value": content,
                           "error": None, "status": True,
                           "cas": upsertResult.cas()})
        except DocumentExistsException as ex:
            self.log.warning("Upsert: Document already exists! => " + str(ex))
            result.update({"key": key, "value": content,
                           "error": str(ex), "status": False})
        except (RequestCanceledException, TimeoutException) as ex:
            self.log.warning("Request cancelled/timed-out: " + str(ex))
            result.update({"key": key, "value": None,
                           "error": str(ex), "status": False})
        except DurabilityAmbiguousException as ex:
            self.log.warning("Durability Ambiguous for key: %s" % key)
            result.update({"key": key, "value": content,
                           "error": str(ex), "status": False})
        except Exception as ex:
            self.log.error("Something else happened: " + str(ex))
            result.update({"key": key, "value": None,
                           "error": str(ex), "status": False})
        return result

    def crud(self, op_type, key, value=None, exp=0, replicate_to=0,
             persist_to=0, durability="", timeout=5, time_unit="seconds",
             create_path=True, xattr=False, cas=0, fail_fast=False):
        result = None
        if op_type == "update":
            result = self.upsert(
                key, value, exp=exp,
                persist_to=persist_to, replicate_to=replicate_to,
                durability=durability,
                timeout=timeout, time_unit=time_unit,
                fail_fast=fail_fast)
        elif op_type == "create":
            result = self.insert(
                key, value, exp=exp,
                persist_to=persist_to, replicate_to=replicate_to,
                durability=durability,
                timeout=timeout, time_unit=time_unit,
                fail_fast=fail_fast)
        elif op_type == "delete":
            result = self.delete(
                key,
                persist_to=persist_to, replicate_to=replicate_to,
                durability=durability,
                timeout=timeout, time_unit=time_unit,
                fail_fast=fail_fast)
        elif op_type == "replace":
            result = self.replace(
                key, value, exp=exp,
                persist_to=persist_to, replicate_to=replicate_to,
                durability=durability,
                timeout=timeout, time_unit=time_unit,
                cas=cas,
                fail_fast=fail_fast)
        elif op_type == "touch":
            result = self.touch(
                key, exp=exp,
                persist_to=persist_to, replicate_to=replicate_to,
                durability=durability,
                timeout=timeout, time_unit=time_unit,
                fail_fast=fail_fast)
        elif op_type == "read":
            result = self.read(
                key, timeout=timeout, time_unit=time_unit,
                fail_fast=fail_fast)
        elif op_type == "subdoc_insert":
            sub_key, value = value[0], value[1]
            mutate_in_specs = list()
            mutate_in_specs.append(SDKClient.sub_doc_op.getInsertMutateInSpec(
                sub_key, value, create_path, xattr))
            if not xattr:
                mutate_in_specs.append(SDKClient.sub_doc_op.getIncrMutateInSpec(
                    "mutated", 1))
            content = Tuples.of(key, mutate_in_specs)
            options = self.getMutateInOptions(exp, time_unit, persist_to, replicate_to,
                                              timeout, time_unit, durability)
            if cas > 0:
                options = options.cas(cas)

            result = SDKClient.sub_doc_op.bulkSubDocOperation(
                self.collection, [content], options)
            return self.__translate_upsert_multi_sub_doc_result(result)
        elif op_type == "subdoc_upsert":
            sub_key, value = value[0], value[1]
            mutate_in_specs = list()
            mutate_in_specs.append(SDKClient.sub_doc_op.getUpsertMutateInSpec(
                sub_key, value, create_path, xattr))
            if not xattr:
                mutate_in_specs.append(SDKClient.sub_doc_op.getIncrMutateInSpec(
                    "mutated", 1))
            content = Tuples.of(key, mutate_in_specs)
            options = self.getMutateInOptions(exp, time_unit, persist_to, replicate_to,
                                              timeout, time_unit, durability)
            if cas > 0:
                options = options.cas(cas)
            result = SDKClient.sub_doc_op.bulkSubDocOperation(
                self.collection, [content], options)
            return self.__translate_upsert_multi_sub_doc_result(result)
        elif op_type == "subdoc_delete":
            mutate_in_specs = list()
            mutate_in_specs.append(SDKClient.sub_doc_op.getRemoveMutateInSpec(
                value, xattr))
            if not xattr:
                mutate_in_specs.append(SDKClient.sub_doc_op.getIncrMutateInSpec(
                    "mutated", 1))
            content = Tuples.of(key, mutate_in_specs)
            options = self.getMutateInOptions(exp, time_unit, persist_to, replicate_to,
                                              timeout, time_unit, durability)
            if cas > 0:
                options = options.cas(cas)
            result = SDKClient.sub_doc_op.bulkSubDocOperation(
                self.collection, [content], options)
            result = self.__translate_upsert_multi_sub_doc_result(result)
        elif op_type == "subdoc_replace":
            sub_key, value = value[0], value[1]
            mutate_in_specs = list()
            mutate_in_specs.append(SDKClient.sub_doc_op.getReplaceMutateInSpec(
                sub_key, value, xattr))
            if not xattr:
                mutate_in_specs.append(
                    SDKClient.sub_doc_op.getIncrMutateInSpec("mutated", 1))
            content = Tuples.of(key, mutate_in_specs)
            options = self.getMutateInOptions(exp, time_unit, persist_to, replicate_to,
                                              timeout, time_unit, durability)
            if cas > 0:
                options = options.cas(cas)
            result = SDKClient.sub_doc_op.bulkSubDocOperation(
                self.collection, [content], options)
            result = self.__translate_upsert_multi_sub_doc_result(result)
        elif op_type == "subdoc_read":
            mutate_in_specs = list()
            mutate_in_specs.append(
                SDKClient.sub_doc_op.getLookUpInSpec(value, xattr))
            content = Tuples.of(key, mutate_in_specs)
            result = SDKClient.sub_doc_op.bulkGetSubDocOperation(
                self.collection, [content])
            result = self.__translate_get_multi_results(result)
        else:
            self.log.error("Unsupported operation %s" % op_type)
        return result

    # Bulk CRUD APIs
    def delete_multi(self, keys, persist_to=0,
                     replicate_to=0, timeout=5, time_unit="seconds",
                     durability=""):
        options = self.getRemoveOptions(persist_to=persist_to,
                                        replicate_to=replicate_to,
                                        timeout=timeout,
                                        time_unit=time_unit,
                                        durability=durability)
        result = SDKClient.doc_op.bulkDelete(
            self.collection, keys, options)
        return self.__tranlate_delete_multi_results(result)

    def touch_multi(self, keys, exp=0,
                    timeout=5, time_unit="seconds"):
        touch_options = self.getTouchOptions(timeout, time_unit)
        exp_duration = self.getDuration(exp, "seconds");
        result = SDKClient.doc_op.bulkTouch(
            self.collection, keys, exp,
            touch_options, exp_duration)
        return self.__tranlate_delete_multi_results(result)

    def setMulti(self, items, exp=0, exp_unit="seconds",
                 persist_to=0, replicate_to=0,
                 timeout=5, time_unit="seconds", retry=5,
                 doc_type="json", durability=""):

        options = self.getInsertOptions(exp=exp, exp_unit=exp_unit,
                                        persist_to=persist_to,
                                        replicate_to=replicate_to,
                                        timeout=timeout,
                                        time_unit=time_unit,
                                        durability=durability)
        if doc_type == "binary":
            options = options.transcoder(RawBinaryTranscoder.INSTANCE)
        result = SDKClient.doc_op.bulkInsert(
            self.collection, items, options)
        return self.__translate_upsert_multi_results(result)

    def upsertMulti(self, docs, exp=0, exp_unit="seconds",
                    persist_to=0, replicate_to=0,
                    timeout=5, time_unit="seconds", retry=5,
                    doc_type="json", durability=""):
        options = self.getUpsertOptions(exp=exp, exp_unit=exp_unit,
                                        persist_to=persist_to,
                                        replicate_to=replicate_to,
                                        timeout=timeout,
                                        time_unit=time_unit,
                                        durability=durability)
        if doc_type == "binary":
            options = options.transcoder(RawBinaryTranscoder.INSTANCE)
        result = SDKClient.doc_op.bulkUpsert(
            self.collection, docs, options)
        return self.__translate_upsert_multi_results(result)

    def replaceMulti(self, docs, exp=0, exp_unit="seconds",
                     persist_to=0, replicate_to=0,
                     timeout=5, time_unit="seconds",
                     doc_type="json", durability=""):
        options = self.getReplaceOptions(persist_to=persist_to,
                                         replicate_to=replicate_to,
                                         timeout=timeout,
                                         time_unit=time_unit,
                                         durability=durability)
        if doc_type == "binary":
            options = options.transcoder(RawBinaryTranscoder.INSTANCE)
        result = SDKClient.doc_op.bulkReplace(
            self.collection, docs, exp, exp_unit,
            options)
        return self.__translate_upsert_multi_results(result)

    def getMulti(self, keys, timeout=5, time_unit="seconds"):
        read_options = self.getReadOptions(timeout, time_unit)
        result = SDKClient.doc_op.bulkGet(self.collection, keys, read_options)
        return self.__translate_get_multi_results(result)

    # Bulk CRUDs for sub-doc APIs
    def sub_doc_insert_multi(self, keys, exp=0, exp_unit="seconds",
                             persist_to=0, replicate_to=0,
                             timeout=5, time_unit="seconds",
                             durability="",
                             create_path=False,
                             xattr=False,
                             cas=0):
        """

        :param keys: Documents to perform sub_doc operations on.
        Must be a dictionary with Keys and List of tuples for
        path and value.
        :param exp: Expiry of document
        :param exp_unit: Expiry time unit
        :param persist_to: Persist to parameter
        :param replicate_to: Replicate to parameter
        :param timeout: timeout for the operation
        :param time_unit: timeout time unit
        :param durability: Durability level parameter
        :param create_path: Boolean used to create sub_doc path if not exists
        :param xattr: Boolean. If 'True', perform xattr operation
        :param cas: CAS for the document to use
        :return:
        """
        mutate_in_specs = []
        for key, value in keys.items():
            mutate_in_spec = []
            for _tuple in value:
                _path = _tuple[0]
                _val = _tuple[1]
                _mutate_in_spec = SDKClient.sub_doc_op.getInsertMutateInSpec(
                    _path, _val, create_path, xattr)
                mutate_in_spec.append(_mutate_in_spec)
            if not xattr:
                _mutate_in_spec = SDKClient.sub_doc_op.getIncrMutateInSpec(
                    "mutated", 1)
                mutate_in_spec.append(_mutate_in_spec)
            content = Tuples.of(key, mutate_in_spec)
            mutate_in_specs.append(content)
        options = self.getMutateInOptions(exp, exp_unit, persist_to, replicate_to,
                                          timeout, time_unit, durability)
        if cas > 0:
            options = options.cas(cas)
        result = SDKClient.sub_doc_op.bulkSubDocOperation(
            self.collection, mutate_in_specs, options)
        return self.__translate_upsert_multi_sub_doc_result(result)

    def sub_doc_upsert_multi(self, keys, exp=0, exp_unit="seconds",
                             persist_to=0, replicate_to=0,
                             timeout=5, time_unit="seconds",
                             durability="",
                             create_path=False,
                             xattr=False,
                             cas=0):
        """
        :param keys: Documents to perform sub_doc operations on.
        Must be a dictionary with Keys and List of tuples for
        path and value.
        :param exp: Expiry of document
        :param exp_unit: Expiry time unit
        :param persist_to: Persist to parameter
        :param replicate_to: Replicate to parameter
        :param timeout: timeout for the operation
        :param time_unit: timeout time unit
        :param durability: Durability level parameter
        :param create_path: Boolean used to create sub_doc path if not exists
        :param xattr: Boolean. If 'True', perform xattr operation
        :param cas: CAS for the document to use
        :return:
        """
        mutate_in_specs = []
        for key, value in keys.items():
            mutate_in_spec = []
            for _tuple in value:
                _path = _tuple[0]
                _val = _tuple[1]
                _mutate_in_spec = SDKClient.sub_doc_op.getUpsertMutateInSpec(
                    _path, _val, create_path, xattr)
                mutate_in_spec.append(_mutate_in_spec)
            if not xattr:
                _mutate_in_spec = SDKClient.sub_doc_op.getIncrMutateInSpec(
                    "mutated", 1)
                mutate_in_spec.append(_mutate_in_spec)
            content = Tuples.of(key, mutate_in_spec)
            mutate_in_specs.append(content)
        options = self.getMutateInOptions(exp, exp_unit, persist_to, replicate_to,
                                          timeout, time_unit, durability)
        if cas > 0:
            options = options.cas(cas)
        result = SDKClient.sub_doc_op.bulkSubDocOperation(
            self.collection, mutate_in_specs, options)
        return self.__translate_upsert_multi_sub_doc_result(result)

    def sub_doc_read_multi(self, keys, timeout=5, time_unit="seconds",
                           xattr=False):
        """
        :param keys: Documents to perform sub_doc operations on.
                     Must be a dictionary with Keys and List of tuples for
                     path.
        :param timeout: timeout for the operation
        :param time_unit: timeout time unit
        :param xattr: Boolean. If 'True', perform xattr operation
        :param cas: CAS for the document to use
        :return:
        """
        mutate_in_specs = []
        keys_to_loop = keys.keys()
        keys_to_loop.sort()
        for key in keys_to_loop:
            value = keys[key]
            mutate_in_spec = []
            for _tuple in value:
                _path = _tuple[0]
                _mutate_in_spec = SDKClient.sub_doc_op.getLookUpInSpec(
                    _path,
                    xattr)
                mutate_in_spec.append(_mutate_in_spec)
            content = Tuples.of(key, mutate_in_spec)
            mutate_in_specs.append(content)
        result = SDKClient.sub_doc_op.bulkGetSubDocOperation(
            self.collection, mutate_in_specs)
            # timeout, time_unit)
        return self.__translate_get_multi_results(result)

    def sub_doc_remove_multi(self, keys, exp=0, exp_unit="seconds",
                             persist_to=0, replicate_to=0,
                             timeout=5, time_unit="seconds",
                             durability="",
                             xattr=False,
                             cas=0):
        """
        :param keys: Documents to perform sub_doc operations on.
        Must be a dictionary with Keys and List of tuples for
        path and value.
        :param exp: Expiry of document
        :param exp_unit: Expiry time unit
        :param persist_to: Persist to parameter
        :param replicate_to: Replicate to parameter
        :param timeout: timeout for the operation
        :param time_unit: timeout time unit
        :param durability: Durability level parameter
        :param xattr: Boolean. If 'True', perform xattr operation
        :param cas: CAS for the document to use
        :return:
        """
        mutate_in_specs = []
        for key, value in keys.items():
            mutate_in_spec = []
            for _tuple in value:
                _path = _tuple[0]
                _val = _tuple[1]
                _mutate_in_spec = SDKClient.sub_doc_op.getRemoveMutateInSpec(
                    _path, xattr)
                mutate_in_spec.append(_mutate_in_spec)
            if not xattr:
                _mutate_in_spec = SDKClient.sub_doc_op.getIncrMutateInSpec(
                    "mutated", 1)
                mutate_in_spec.append(_mutate_in_spec)
            content = Tuples.of(key, mutate_in_spec)
            mutate_in_specs.append(content)
        options = self.getMutateInOptions(exp, exp_unit, persist_to, replicate_to,
                                          timeout, time_unit, durability)
        if cas > 0:
            options = options.cas(cas)
        result = SDKClient.sub_doc_op.bulkSubDocOperation(
            self.collection, mutate_in_specs, options)
        return self.__translate_upsert_multi_sub_doc_result(result)

    def sub_doc_replace_multi(self, keys, exp=0, exp_unit="seconds",
                              persist_to=0, replicate_to=0,
                              timeout=5, time_unit="seconds",
                              durability="",
                              xattr=False,
                              cas=0):
        """

        :param keys: Documents to perform sub_doc operations on.
        Must be a dictionary with Keys and List of tuples for
        path and value.
        :param exp: Expiry of document
        :param exp_unit: Expiry time unit
        :param persist_to: Persist to parameter
        :param replicate_to: Replicate to parameter
        :param timeout: timeout for the operation
        :param time_unit: timeout time unit
        :param durability: Durability level parameter
        :param xattr: Boolean. If 'True', perform xattr operation
        :param cas: CAS for the document to use
        :return:
        """
        mutate_in_specs = []
        for key, value in keys.items():
            mutate_in_spec = []
            for _tuple in value:
                _path = _tuple[0]
                _val = _tuple[1]
                _mutate_in_spec = SDKClient.sub_doc_op.getReplaceMutateInSpec(
                    _path, _val, xattr)
                mutate_in_spec.append(_mutate_in_spec)
            if not xattr:
                _mutate_in_spec = SDKClient.sub_doc_op.getIncrMutateInSpec(
                    "mutated", 1)
                mutate_in_spec.append(_mutate_in_spec)
            content = Tuples.of(key, mutate_in_spec)
            mutate_in_specs.append(content)
        options = self.getMutateInOptions(exp, exp_unit, persist_to, replicate_to,
                                          timeout, time_unit, durability)
        if cas > 0:
            options = options.cas(cas)
        result = SDKClient.sub_doc_op.bulkSubDocOperation(
            self.collection, mutate_in_specs, options)
        return self.__translate_upsert_multi_sub_doc_result(result)
