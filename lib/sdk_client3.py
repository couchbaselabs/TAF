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
from com.couchbase.client.core.error import DocumentAlreadyExistsException, CouchbaseException
from com.couchbase.client.core.error import DocumentDoesNotExistException, CASMismatchException
from com.couchbase.client.core.error import TemporaryFailureException
from com.couchbase.client.java.env import ClusterEnvironment
from com.couchbase.client.core.msg.kv import DurabilityLevel
from com.couchbase.client.java.kv import InsertOptions, UpsertOptions,\
    RemoveOptions
from com.couchbase.client.core.env import TimeoutConfig

from java.time import Duration
from java.lang import System
from java.util.logging import Logger, Level, ConsoleHandler
from java.util.concurrent import TimeUnit
from reactor.util.function import Tuples

import com.couchbase.test.doc_operations_sdk3.doc_ops as doc_op


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
            self.cluster.environment().shutdown()
            self.cluster.shutdown()
            log.info("Closed down Cluster Connection.")

    def delete(self, key, timeout=120, timeoutunit=TimeUnit.SECONDS, ttl=0,
               ttlunit=TimeUnit.DAYS, durability=DurabilityLevel.MAJORITY,
               max_retry=3):
        retry_count = 0
        retry = True
        while retry_count <= max_retry and retry:
            try:
                options = RemoveOptions.removeOptions().timeout(self.getTime(timeout, timeoutunit)).expiry(self.getTime(ttl, ttlunit)).withDurabilityLevel(durability)
                deleteResult = self.collection.remove(key, options)
                print deleteResult
                retry = False
            except DocumentDoesNotExistException as e:
                log.error("Exception: Document id {0} not found - {1}"
                          .format(key, e))
                retry = False
                raise(e)
            except CASMismatchException as e:
                log.error("Exception: Cas mismatch for doc {0} - {1}"
                          .format(key, e))
                retry = False
                raise(e)
            except TemporaryFailureException as e:
                log.warning("Exception: Retry for doc {0} - {1}"
                            .format(key, e))
            except CouchbaseException as e:
                log.error("Generic exception for doc {0} - {1}"
                          .format(key, e))
                retry = False
            except Exception as e:
                print("Error during remove of {0} - {1}".format(key, e))
                retry = False

            retry_count += 1

    def delete_multi(self, keys, quiet=True, persist_to=0, replicate_to=0):
        try:
            self.cb.remove_multi(keys, quiet, persist_to, replicate_to)
        except CouchbaseException:
            try:
                time.sleep(10)
                self.cb.remove_multi(keys, quiet, persist_to, replicate_to)
            except CouchbaseException:
                raise

    def getTime(self, var, timeunit):
        time = {
            TimeUnit.SECONDS: Duration.ofSeconds(var),
            TimeUnit.MINUTES: Duration.ofMinutes(var),
            TimeUnit.HOURS: Duration.ofHours(var),
            TimeUnit.MILLISECONDS: Duration.ofMillis(var),
            TimeUnit.DAYS: Duration.ofDays(var)
        }

        return time.get(timeunit)

    def insert(self, key, value, timeout=120, timeoutunit=TimeUnit.SECONDS,
               ttl=0, ttlunit=TimeUnit.DAYS,
               durability=DurabilityLevel.MAJORITY):
        success = dict()
        fail = dict()

        content = self.__translate_to_json_object(value)
        try:
            options = InsertOptions.insertOptions().timeout(self.getTime(timeout, timeoutunit)).expiry(self.getTime(ttl, ttlunit)).withDurabilityLevel(durability)
            insertResult = self.collection.insert(key, content, options)
            success.update({"key": key, "value": content,
                            "error": None, "status": True})
            print insertResult
        except DocumentAlreadyExistsException as ex:
            print("The document already exists! => " + str(ex))
            fail.update({"key": key, "value": content,
                         "error": str(ex), "status": False})
        except (CouchbaseException, Exception) as ex:
            print("Something else happened: " + str(ex))
            fail.update({"key": key, "value": content,
                         "error": str(ex), "status": False})
        return success, fail

    def read(self, key):
        success = dict()
        fail = dict()
        getResult = self.collection.get(key)
        print getResult
        if getResult.isPresent():
            print("Found document: cas=%s, content=%s"
                  % (str(getResult.get().cas()),
                     str(getResult.get().contentAsObject())))
            success.update({"key": key,
                            "value": str(getResult.get().contentAsObject()),
                            "error": None, "status": True})
        else:
            print("Document not found!")

    def upsert(self, key, value, timeout=120, timeoutunit=TimeUnit.SECONDS,
               ttl=0, ttlunit=TimeUnit.DAYS,
               durability=DurabilityLevel.MAJORITY):
        success = dict()
        fail = dict()
        content = self.__translate_to_json_object(value)
        try:
            options = UpsertOptions.upsertOptions().timeout(self.getTime(timeout, timeoutunit)).expiry(self.getTime(ttl, ttlunit)).withDurabilityLevel(durability)
            upsertResult = self.collection.upsert(key, content, options)
            success.update({"key": key, "value": content,
                            "error": None, "status": True})
            print upsertResult
        except DocumentAlreadyExistsException as ex:
            print("Upsert: The document already exists! => " + str(ex))
            fail.update({"key": key, "value": content,
                         "error": str(ex), "status": False})
        except (CouchbaseException, Exception) as ex:
            print("Upsert: Something else happened: " + str(ex))
            fail.update({"key": key, "value": content,
                         "error": str(ex), "status": False})
        return success, fail

    def setMulti(self, keys, exp=0, exp_unit="seconds",
                    persist_to=0, replicate_to=0,
                    timeout=5, time_unit="seconds", retry=5,
                    doc_type="json", durability=""):

        docs = []
        for key, value in keys.items():
            content = self.__translate_to_json_object(value, doc_type)
            tuple = Tuples.of(key, content)
            docs.append(tuple)
        result = doc_op().bulkInsert(self.collection, docs, exp, exp_unit, persist_to, replicate_to, durability,
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
        result = doc_op().bulkInsert(self.collection, docs, exp, exp_unit, persist_to, replicate_to, durability, timeout,
                                     time_unit)
        return self.__translate_upsert_multi_results(result)

    def getMulti(self, keys):
        result = doc_op().bulkGet(self.collection, keys)
        return self.__translate_get_multi_results(result)
 
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


if __name__ == "__main__":
    client = SDKClient("default", hosts=["10.112.180.102"],
                       scheme="couchbase", password="password",
                       compression=True)
    """
    print client.insert("dh_persist_key", '{"test":"value1"}', ttl=1)
    client.read("dh_persist_key")
    print client.upsert("dh_persist_key", '{"test":"value2"}', ttl=1,
                        ttlunit=TimeUnit.MINUTES)
    client.read("dh_persist_key")
    client.delete("dh_persist_key")
    client.close()
    """

    from couchbase_helper.documentgenerator import \
        doc_generator, BatchedDocumentGenerator

    doc_create = doc_generator("ritesh", 0, 10000, doc_size=10,
                               doc_type="json", target_vbucket=None,
                               vbuckets=1024)
    batch_gen = BatchedDocumentGenerator(doc_create, 1000)
    print batch_gen
    key_value = batch_gen.next_batch()
    print key_value
    client.insert_multi(key_value, 100, TimeUnit.MINUTES, 1000)
    client.close()
