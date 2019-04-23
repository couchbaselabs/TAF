#!/usr/bin/env python
"""
Java based SDK client interface
Created on Mar 14, 2019

"""

import json as pyJson
import logger
import time

from com.couchbase.client.java import Cluster
from com.couchbase.client.java.json import JsonObject
from com.couchbase.client.core.error import DocumentAlreadyExistsException, \
                                            CouchbaseException, \
                                            RequestTimeoutException
from com.couchbase.client.core.error import DocumentDoesNotExistException, \
                                            CASMismatchException
from com.couchbase.client.core.error import TemporaryFailureException
from com.couchbase.client.java.env import ClusterEnvironment
from com.couchbase.client.core.msg.kv import DurabilityLevel
from com.couchbase.client.java.kv import InsertOptions, UpsertOptions, \
                                         RemoveOptions, PersistTo, ReplicateTo
from com.couchbase.client.core.env import TimeoutConfig

from java.time import Duration
from java.lang import System
from java.util.logging import Logger, Level, ConsoleHandler
from reactor.util.function import Tuples

import com.couchbase.test.doc_operations_sdk3.doc_ops as doc_op
from java.time.temporal import ChronoUnit


log = logger.Logger.get_logger()


class SDKClient(object):
    """
    Java SDK Client Implementation for testrunner - master branch
    """

    def __init__(self, rest, bucket, info=None,  username="Administrator",
                 password="password",
                 quiet=True, certpath=None, transcoder=None, compression=True):
#         self.connection_string = \
#             self._createString(scheme=scheme, bucket=bucket, hosts=hosts,
#                                certpath=certpath, uhm_options=uhm_options,
#                                compression=compression)
        self.rest = rest
        self.hosts = []
        if rest.ip == "127.0.0.1":
            self.hosts.append("{0}:{1}".format(rest.ip, rest.port))
            self.scheme = "http"
        else:
            self.hosts.append(rest.ip)
            self.scheme = "couchbase"
        self.username = username
        self.password = password
        if hasattr(bucket, 'name'):
            self.bucket = bucket.name
        else:
            self.bucket = bucket
        self.quiet = quiet
        self.transcoder = transcoder
        self.default_timeout = 0
        self.cluster = None
        self._createConn()

    def _createConn(self):
        try:
            log.info("Creating cluster connection.")
            System.setProperty("com.couchbase.forceIPv4", "false")
            logger = Logger.getLogger("com.couchbase.client")
            logger.setLevel(Level.SEVERE)
            for h in logger.getParent().getHandlers():
                if isinstance(h, ConsoleHandler):
                    h.setLevel(Level.SEVERE)
            self.cluster = Cluster.connect(ClusterEnvironment
                                           .builder(", ".join(self.hosts).replace(" ", ""),
                                                    self.username, self.password)
                                           .timeoutConfig(TimeoutConfig.builder().kvTimeout(Duration.ofSeconds(10)))
                                           .build())
            self.bucketObj = self.cluster.bucket(self.bucket)
            self.collection = self.bucketObj.defaultCollection()
        except Exception as e:
            print("Exception: " + str(e))
#             self.cluster.disconnect()
            raise

    def close(self):
        log.info("Closing down the cluster.")
        if self.cluster:
            self.cluster.shutdown()
            self.cluster.environment().shutdown()
            log.info("Closed down Cluster Connection.")

    def delete(self, key, persist_to=0, replicate_to=0,
               timeout=5, time_unit="seconds",
               durability=""):

        result = dict()
        try:
            options = self.getRemoveOptions(persist_to=persist_to,
                                            replicate_to=replicate_to,
                                            timeout=timeout,
                                            time_unit=time_unit,
                                            durability=durability)

            deleteResult = self.collection.remove(key, options)
            result.update({"key": key, "value": None,
                           "error": None, "status": True})
        except DocumentDoesNotExistException as e:
            log.error("Exception: Document id {0} not found - {1}"
                      .format(key, e))
            result.update({"key": key, "value": None,
                           "error": str(e), "status": False})
        except CASMismatchException as e:
            log.error("Exception: Cas mismatch for doc {0} - {1}"
                      .format(key, e))
            result.update({"key": key, "value": None,
                           "error": str(e), "status": False})
        except TemporaryFailureException as e:
            log.warning("Exception: Retry for doc {0} - {1}"
                        .format(key, e))
            result.update({"key": key, "value": None,
                           "error": str(e), "status": False})
        except CouchbaseException as e:
            log.error("Generic exception for doc {0} - {1}"
                      .format(key, e))
            result.update({"key": key, "value": None,
                           "error": str(e), "status": False})
        except Exception as e:
            log.error("Error during remove of {0} - {1}".format(key, e))
            result.update({"key": key, "value": None,
                           "error": str(e), "status": False})
        return result

    def delete_multi(self, keys, quiet=True, persist_to=0, replicate_to=0):
        try:
            self.cb.remove_multi(keys, quiet, persist_to, replicate_to)
        except CouchbaseException:
            try:
                time.sleep(10)
                self.cb.remove_multi(keys, quiet, persist_to, replicate_to)
            except CouchbaseException:
                raise

    def insert(self, key, value, exp=0, exp_unit="seconds",
               persist_to=0, replicate_to=0,
               timeout=5, time_unit="seconds",
               durability=""):

        result = dict()
        content = self.__translate_to_json_object(value)
        try:
            options = self.getInsertOptions(exp=exp, exp_unit=exp_unit,
                                            persist_to=persist_to,
                                            replicate_to=replicate_to,
                                            timeout=timeout,
                                            time_unit=time_unit,
                                            durability=durability)

            insertResult = self.collection.insert(key, content, options)
            result.update({"key": key, "value": content,
                           "error": None, "status": True})
        except DocumentAlreadyExistsException as ex:
            log.error("The document already exists! => " + str(ex))
            result.update({"key": key, "value": content,
                           "error": str(ex), "status": False})
        except (CouchbaseException, Exception, RequestTimeoutException) as ex:
            log.error("Something else happened: " + str(ex))
            result.update({"key": key, "value": content,
                           "error": str(ex), "status": False})
        return result

    def read(self, key):
        result = dict()
        getResult = self.collection.get(key)
        print getResult
        if getResult.isPresent():
            log.info("Found document: cas=%s, content=%s"
                  % (str(getResult.get().cas()),
                     str(getResult.get().contentAsObject())))
            result.update({"key": key,
                           "value": str(getResult.get().contentAsObject()),
                           "error": None, "status": True})
        else:
            log.error("Document not found!")
            result.update({"key": key, "value": None,
                           "error": None, "status": False})
        return result

    def upsert(self, key, value, exp=0, exp_unit="seconds",
               persist_to=0, replicate_to=0,
               timeout=5, time_unit="seconds",
               durability=""):

        result = dict()
        content = self.__translate_to_json_object(value)
        try:
            options = self.getUpsertOptions(exp=exp, exp_unit=exp_unit,
                                            persist_to=persist_to,
                                            replicate_to=replicate_to,
                                            timeout=timeout,
                                            time_unit=time_unit,
                                            durability=durability)

            upsertResult = self.collection.upsert(key, content, options)
            result.update({"key": key, "value": content,
                           "error": None, "status": True})
        except DocumentAlreadyExistsException as ex:
            log.error("Upsert: The document already exists! => " + str(ex))
            result.update({"key": key, "value": content,
                           "error": str(ex), "status": False})
        except (CouchbaseException, Exception, RequestTimeoutException) as ex:
            log.error("Upsert: Something else happened: " + str(ex))
            result.update({"key": key, "value": content,
                           "error": str(ex), "status": False})
        return result

    def setMulti(self, keys, exp=0, exp_unit="seconds",
                 persist_to=0, replicate_to=0,
                 timeout=5, time_unit="seconds", retry=5,
                 doc_type="json", durability=""):

        docs = []
        for key, value in keys.items():
            content = self.__translate_to_json_object(value, doc_type)
            tuple = Tuples.of(key, content)
            docs.append(tuple)
        result = doc_op().bulkInsert(self.collection, docs, exp, exp_unit,
                                     persist_to, replicate_to, durability,
                                     timeout, time_unit)
        return self.__translate_upsert_multi_results(result)

    def upsertMulti(self, keys, exp=0, exp_unit="seconds",
                    persist_to=0, replicate_to=0,
                    timeout=5, time_unit="seconds", retry=5,
                    doc_type="json", durability=""):
        docs = []
        for key, value in keys.items():
            content = self.__translate_to_json_object(value, doc_type)
            tuple = Tuples.of(key, content)
            docs.append(tuple)
        result = doc_op().bulkInsert(self.collection, docs, exp, exp_unit,
                                     persist_to, replicate_to, durability,
                                     timeout, time_unit)
        return self.__translate_upsert_multi_results(result)

    def getMulti(self, keys):
        result = doc_op().bulkGet(self.collection, keys)
        return self.__translate_get_multi_results(result)

    def crud(self, op_type, key, value=None, exp=0, replicate_to=0,
             persist_to=0, durability="", timeout=5, time_unit="seconds"):
        result = None
        if op_type == "update":
            result = self.upsert(
                key, value, exp=exp,
                persist_to=persist_to, replicate_to=replicate_to,
                durability=durability,
                timeout=timeout, time_unit=time_unit)
        elif op_type == "create":
            result = self.insert(
                key, value, exp=exp,
                persist_to=persist_to, replicate_to=replicate_to,
                durability=durability,
                timeout=timeout, time_unit=time_unit)
        elif op_type == "delete":
            result = self.delete(
                key, exp=exp,
                persist_to=persist_to, replicate_to=replicate_to,
                durability=durability,
                timeout=timeout, time_unit=time_unit)
        return result

    def __translate_to_json_object(self, value, doc_type="json"):

        if type(value) == JsonObject:
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
                pass
        except Exception:
            pass

        return json_obj

    def __translate_upsert_multi_results(self, data):
        success = dict()
        fail = dict()
        if data is None:
            return success, fail
        for item in data:
            result = item['status']
            id = item['id']
            json_object = item["document"]
            if result:
                success[id] = [id, item['cas'], json_object]
            else:
                error = item['error']
                fail[id] = [error, id, json_object]
        return success, fail

    def __translate_get_multi_results(self, data):
        map = dict()
        if data is None:
            return map
        for result in data:
            id = result['id']
            cas = result['cas']
            status = result['status']
            content = result['content']
            error = result['error']
            if status:
                map[id] = [id, cas, content]
            else:
                map[id] = [id, cas, error]
        return map

    def getInsertOptions(self, exp=0, exp_unit="seconds",
                         persist_to=0, replicate_to=0,
                         timeout=5, time_unit="seconds",
                         durability=""):
        if durability:
            options = InsertOptions.insertOptions()\
                .timeout(self.getDuration(timeout, time_unit))\
                .expiry(self.getDuration(exp, exp_unit))\
                .withDurabilityLevel(self.getDurabilityLevel(durability))
        else:
            options = InsertOptions.insertOptions()\
                .timeout(self.getDuration(timeout, time_unit))\
                .expiry(self.getDuration(exp, exp_unit))\
                .withDurability(self.getPersistTo(persist_to),
                                self.getReplicateTo(replicate_to))
        return options

    def getUpsertOptions(self, exp=0, exp_unit="seconds",
                         persist_to=0, replicate_to=0,
                         timeout=5, time_unit="seconds",
                         durability=""):
        options = None
        if durability:
            options = UpsertOptions.upsertOptions()\
                .timeout(self.getDuration(timeout, time_unit))\
                .expiry(self.getDuration(exp, exp_unit))\
                .withDurabilityLevel(self.getDurabilityLevel(durability))
        else:
            options = UpsertOptions.upsertOptions()\
                .timeout(self.getDuration(timeout, time_unit))\
                .expiry(self.getDuration(exp, exp_unit))\
                .withDurability(self.getPersistTo(persist_to),
                                self.getReplicateTo(replicate_to))

        return options

    def getRemoveOptions(self, persist_to=0, replicate_to=0,
                         timeout=5, time_unit="seconds",
                         durability=""):
        options = None
        if durability:
            options = RemoveOptions.removeOptions()\
                .timeout(self.getDuration(timeout, time_unit))\
                .withDurabilityLevel(self.getDurabilityLevel(durability))
        else:
            options = RemoveOptions.removeOptions()\
                .timeout(self.getDuration(timeout, time_unit))\
                .withDurability(self.getPersistTo(persist_to),
                                self.getReplicateTo(replicate_to))

        return options

    def getPersistTo(self, persistTo):
        try:
            persistList = [PersistTo.NONE, PersistTo.ONE, PersistTo.TWO,
                           PersistTo.THREE, PersistTo.FOUR]
            return persistList[persistTo]
        except Exception as e:
            pass

        return PersistTo.ACTIVE

    def getReplicateTo(self, replicateTo):
        try:
            replicateList = [ReplicateTo.NONE, ReplicateTo.ONE,
                             ReplicateTo.TWO, ReplicateTo.THREE]
            return replicateList[replicateTo]
        except Exception:
            pass

        return ReplicateTo.NONE

    def getDurabilityLevel(self, durability_level):
        durability_level = durability_level.upper()
        if durability_level == "MAJORITY":
            return DurabilityLevel.MAJORITY

        if durability_level == "MAJORITY_AND_PERSIST_ON_MASTER":
            return DurabilityLevel.MAJORITY_AND_PERSIST_ON_MASTER

        if durability_level == "PERSIST_TO_MAJORITY":
            return DurabilityLevel.PERSIST_TO_MAJORITY

        return None

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
