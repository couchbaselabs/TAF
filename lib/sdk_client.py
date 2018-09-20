#!/usr/bin/env python
"""
Java based SDK client interface

"""
import crc32
import time
from memcached.helper.old_kvstore import ClientKeyValueStore
from com.couchbase.client.java import CouchbaseCluster
from com.couchbase.client.java.document import *
from com.couchbase.client.java.document.json import *
from com.couchbase.client.java.query import N1qlQueryResult, N1qlQuery
from com.couchbase.client.core import CouchbaseException
from com.couchbase.client.java.transcoder import JsonTranscoder

from com.couchbase.client.deps.io.netty.buffer import Unpooled
from com.couchbase.client.deps.io.netty.util import CharsetUtil
from com.couchbase.client.java.document import BinaryDocument
from com.couchbase.client.java.document import StringDocument
from com.couchbase.client.java.document import JsonDocument
from com.couchbase.client.java.document.json import JsonObject
from com.couchbase.client.java.CouchbaseBucket import mutateIn

from mc_bin_client import MemcachedError
from java.util.concurrent import TimeUnit
from BucketLib.BucketOperations import BucketHelper
from com.couchbase.client.java.error.subdoc import DocumentNotJsonException
import Java_Connection
import logger
from java.util.logging import Logger, Level, ConsoleHandler
from com.couchbase.client.java.subdoc import SubdocOptionsBuilder
from java.lang import System

FMT_AUTO = "autoformat"

import json
log = logger.Logger.get_logger()

class SDKClient(object):
    """Java SDK Client Implementation for testrunner - master branch Implementation"""

    def __init__(self, bucket, hosts=["localhost"] , scheme="couchbase",
                 ssl_path=None, uhm_options=None, password=None,
                 quiet=True, certpath = None, transcoder = None, compression=True):
        
        self.connection_string = \
            self._createString(scheme = scheme, bucket = bucket, hosts = hosts,
                               certpath = certpath, uhm_options = uhm_options, compression=compression)
        self.hosts = hosts
        self.password = password
        self.bucket = bucket
        self.password = password
        self.quiet = quiet
        self.transcoder = transcoder
        self.default_timeout = 0
        self.cluster = None
        self._createConn()
        #couchbase.set_json_converters(json.dumps, json.loads)
        
    def _createString(self, scheme ="couchbase", bucket = None, hosts = ["localhost"], certpath = None,
                      uhm_options = "", ipv6=False, compression=True):
        connection_string = "{0}://{1}".format(scheme, ", ".join(hosts).replace(" ",""))
        # if bucket != None:
        #     connection_string = "{0}/{1}".format(connection_string, bucket)
        if uhm_options != None:
            connection_string = "{0}?{1}".format(connection_string, uhm_options)
        if ipv6 == True:
            if "?" in connection_string:
                connection_string = "{0},ipv6=allow".format(connection_string)
            else:
                connection_string = "{0}?ipv6=allow".format(connection_string)
        if compression == True:
            if "?" in connection_string:
                connection_string = "{0},compression=on".format(connection_string)
            else:
                connection_string = "{0}?compression=on".format(connection_string)
        else:
            if "?" in connection_string:
                connection_string = "{0},compression=off".format(connection_string)
            else:
                connection_string = "{0}?compression=off".format(connection_string)
        if scheme == "couchbases":
            if "?" in connection_string:
                connection_string = "{0},certpath={1}".format(connection_string, certpath)
            else:
                connection_string = "{0}?certpath={1}".format(connection_string, certpath)
        return connection_string
    
    def _createConn(self):
        try:
            System.setProperty("com.couchbase.forceIPv4", "false");
            logger = Logger.getLogger("com.couchbase.client");
            logger.setLevel(Level.SEVERE);
            for h in logger.getParent().getHandlers():
                if isinstance(h, ConsoleHandler) :
                    h.setLevel(Level.SEVERE);
#             self.cluster = CouchbaseCluster.create(Java_Connection.env, self.hosts)
            self.cluster = CouchbaseCluster.fromConnectionString(Java_Connection.env, self.connection_string);
            self.cluster.authenticate("Administrator", self.password)
            self.cb = self.cluster.openBucket(self.bucket)
        except CouchbaseException:
            self.cluster.disconnect()
            raise

    def reconnect(self):
        self.close()
        self._createConn()

    def close(self):
        log.info("Closing down the cluster.")
        if self.cb:
            self.cb.close()
            log.info("Closed down Bucket Conection.")
        if self.cluster:
            self.cluster.disconnect()
            log.info("Closed down Cluster Connection.")
    def counter_in(self, key, path, delta, create_parents=True, cas=0, ttl=0, persist_to=0, replicate_to=0):
        try:
            return self.cb.counter_in(key, path, delta, create_parents= create_parents, cas= cas, ttl= ttl, persist_to= persist_to, replicate_to= replicate_to)
        except CouchbaseException as e:
            raise

    def arrayappend_in(self, key, path, value, create_parents=True, cas=0, ttl=0, persist_to=0, replicate_to=0):
        try:
            return self.cb.arrayappend_in(key, path, value, create_parents=create_parents, cas=cas, ttl=ttl, persist_to=persist_to, replicate_to=replicate_to)
        except CouchbaseException as e:
            raise

    def arrayprepend_in(self, key, path, value, create_parents=True, cas=0, ttl=0, persist_to=0, replicate_to=0):
        try:
            return self.cb.arrayprepend_in(key, path, value, create_parents=create_parents, cas=cas, ttl=ttl, persist_to=persist_to, replicate_to=replicate_to)
        except CouchbaseException as e:
            raise

    def arrayaddunique_in(self, key, path, value, create_parents=True, cas=0, ttl=0, persist_to=0, replicate_to=0):
        try:
            return self.cb.addunique_in(key, path, value, create_parents=create_parents, cas=cas, ttl=ttl, persist_to=persist_to, replicate_to=replicate_to)
        except CouchbaseException as e:
            raise

    def arrayinsert_in(self, key, path, value, cas=0, ttl=0, persist_to=0, replicate_to=0):
        try:
            return self.cb.arrayinsert_in(key, path, value, cas=cas, ttl=ttl, persist_to=persist_to, replicate_to=replicate_to)
        except CouchbaseException as e:
            raise

    def remove_in(self, key, path,  cas=0, ttl=0):
        try:
            self.cb.remove_in(key, path, cas = cas, ttl = ttl)
        except CouchbaseException as e:
            raise

    def mutate_in(self, key, *specs, **kwargs):
        try:
            self.cb.mutate_in(key, *specs, **kwargs)
        except CouchbaseException as e:
            raise

    def lookup_in(self, key):
        try:
            self.cb.lookupIn(key)
        except CouchbaseException as e:
            raise

    def get_in(self, key, path):
        try:
            result = self.cb.get_in(key, path)
            return self.__translate_get(result)
        except CouchbaseException as e:
            raise

    def exists_in(self, key, path):
        try:
            self.cb.exists_in(key, path)
        except CouchbaseException as e:
            raise

    def replace_in(self, key, path, value, cas=0, ttl=0, persist_to=0, replicate_to=0):
        try:
            return self.cb.replace_in(key, path, value, cas=cas, ttl=ttl, persist_to=persist_to, replicate_to=replicate_to)
        except CouchbaseException as e:
            raise

    def insert_in(self, key, path, value, create_parents=True, cas=0, ttl=0, persist_to=0, replicate_to=0):
        try:
            return self.cb.insert_in(key, path, value, create_parents=create_parents, cas=cas, ttl=ttl, persist_to=persist_to, replicate_to=replicate_to)
        except CouchbaseException as e:
            raise

    def upsert_in(self, key, path, value, create_parents=True, cas=0, ttl=0, persist_to=0, replicate_to=0):
        try:
            return self.cb.upsert_in(key, path, value, create_parents=create_parents, cas=cas, ttl=ttl, persist_to=persist_to, replicate_to=replicate_to)
        except CouchbaseException as e:
            raise

    def append(self, key, value, cas=0, format=None, persist_to=0, replicate_to=0):
        try:
            self.cb.append(key, value, cas=cas, format=format, persist_to=persist_to, replicate_to=replicate_to)
        except CouchbaseException as e:
            try:
                time.sleep(10)
                self.cb.append(key, value, cas=cas, format=format, persist_to=persist_to, replicate_to=replicate_to)
            except CouchbaseException as e:
                raise

    def append_multi(self, keys, cas=0, format=None, persist_to=0, replicate_to=0):
        try:
            self.cb.append_multi(keys, cas=cas, format=format, persist_to=persist_to, replicate_to=replicate_to)
        except CouchbaseException as e:
            try:
                time.sleep(10)
                self.cb.append_multi(keys, cas=cas, format=format, persist_to=persist_to, replicate_to=replicate_to)
            except CouchbaseException as e:
                raise

    def prepend(self, key, value, cas=0, format=None, persist_to=0, replicate_to=0):
        try:
            self.cb.prepend(key, value, cas=cas, format=format, persist_to=persist_to, replicate_to=replicate_to)
        except CouchbaseException as e:
            try:
                self.cb.prepend(key, value, cas=cas, format=format, persist_to=persist_to, replicate_to=replicate_to)
            except CouchbaseException as e:
                raise

    def prepend_multi(self, keys, cas=0, format=None, persist_to=0, replicate_to=0):
        try:
            self.cb.prepend_multi(keys, cas=cas, format=format, persist_to=persist_to, replicate_to=replicate_to)
        except CouchbaseException as e:
            try:
                time.sleep(10)
                self.cb.prepend_multi(keys, cas=cas, format=format, persist_to=persist_to, replicate_to=replicate_to)
            except CouchbaseException as e:
                raise

    def replace(self, key, value, cas=0, ttl=0, format=None, persist_to=0, replicate_to=0):
        try:
           self.cb.replace( key, value, cas=cas, ttl=ttl, format=format,
                                    persist_to=persist_to, replicate_to=replicate_to)
        except CouchbaseException as e:
            try:
                time.sleep(10)
                self.cb.replace( key, value, cas=cas, ttl=ttl, format=format,
                                    persist_to=persist_to, replicate_to=replicate_to)
            except CouchbaseException as e:
                raise

    def replace_multi(self, keys, cas=0, ttl=0, format=None, persist_to=0, replicate_to=0):
        try:
            self.cb.replace_multi( keys, cas=cas, ttl=ttl, format=format, persist_to=persist_to, replicate_to=replicate_to)
        except CouchbaseException as e:
            try:
                time.sleep(10)
                self.cb.replace_multi( keys, cas=cas, ttl=ttl, format=format, persist_to=persist_to, replicate_to=replicate_to)
            except CouchbaseException as e:
                raise

    def cas(self, key, value, cas=0, ttl=0, format=None):
        return self.cb.replace(key, value, cas=cas,format=format)

    def delete(self,key, cas=0, quiet=True, persist_to=0, replicate_to=0):
        self.remove(key, cas=cas, quiet=quiet, persist_to=persist_to, replicate_to=replicate_to)

    def remove(self,key, cas=0, quiet=True, persist_to=0, replicate_to=0):
        try:
            return self.cb.remove(key)
        except CouchbaseException as e:
            try:
                time.sleep(10)
                return self.cb.remove(key)
            except CouchbaseException as e:
                raise

#     def delete(self, keys, quiet=True, persist_to=0, replicate_to=0):
#         return self.remove(self, keys, quiet=quiet, persist_to=persist_to, replicate_to=replicate_to)

    def remove_multi(self, keys, quiet=True, persist_to=0, replicate_to=0):
        try:
            self.cb.remove_multi(keys, quiet=quiet, persist_to=persist_to, replicate_to=replicate_to)
        except CouchbaseException as e:
            try:
                time.sleep(10)
                self.cb.remove_multi(keys, quiet=quiet, persist_to=persist_to, replicate_to=replicate_to)
            except CouchbaseException as e:
                raise

    def set(self, key, value, ttl=0, format=None, persist_to=0, replicate_to=0):
        doc = self.__translate_to_json_document(key, value, ttl)
        try:
            return self.cb.set(doc)
        except CouchbaseException as e:
            try:
                time.sleep(10)
                return self.cb.set(doc, persist_to, replicate_to, ttl, TimeUnit.SECONDS)
            except CouchbaseException as e:
                raise

    def upsert(self, key, value, ttl=0, persist_to=0, replicate_to=0):
        doc = self.__translate_to_json_document(key, value, ttl)
        try:
            return self.cb.upsert(doc)
        except CouchbaseException as e:
            try:
                time.sleep(10)
                return self.cb.upsert(doc)
            except CouchbaseException as e:
                raise

    def set_multi(self, keys, ttl=0, format=None, persist_to=0, replicate_to=0):
        import bulk_doc_operations.doc_ops as doc_op
        docs = []
        for key, value in keys.items():
            docs.append(self.__translate_to_json_document(key, value, ttl))
        try:
            doc_op().bulkSet(self.cb, docs)
        except:
            time.sleep(20)
            doc_op().bulkUpsert(self.cb, docs)
            log.info("Calling close inside SDK due to an exception during bulkSet.")
#             self.close()
            
    def upsert_multi(self, keys, ttl=0, persist_to=0, replicate_to=0):
        import bulk_doc_operations.doc_ops as doc_op
        docs = []
        for key, value in keys.items():
            docs.append(self.__translate_to_json_document(key, value, ttl))
        doc_op().bulkUpsert(self.cb, docs)
        
    def insert(self, key, value, ttl=0, format=None, persist_to=0, replicate_to=0):
        doc = self.__translate_to_json_document(key, value, ttl)
        try:
            self.cb.insert(doc)
        except CouchbaseException as e:
            try:
                time.sleep(10)
                self.cb.insert(doc)
            except CouchbaseException as e:
                raise

    def insert_multi(self, keys,  ttl=0, format=None, persist_to=0, replicate_to=0):
        import bulk_doc_operations.doc_ops as doc_op
        docs = []
        for key, value in keys.items():
            docs.append(self.__translate_to_json_document(key, value, ttl))
        doc_op().bulkSet(self.cb, docs)

    def touch(self, key, ttl=0):
        try:
            self.cb.getAndTouch(key, ttl)
        except CouchbaseException as e:
            try:
                time.sleep(10)
                self.cb.touch(key, ttl=ttl)
            except CouchbaseException as e:
                raise

    def touch_multi(self, keys, ttl=0):
        for key in keys:
            self.touch(key, ttl=ttl)

    def decr(self, key, delta=1, initial=None, ttl=0):
        self.counter(key, delta=-delta, initial=initial, ttl=ttl)

    def decr_multi(self, keys, delta=1, initial=None, ttl=0):
        self.counter_multi(keys, delta=-delta, initial=initial, ttl=ttl)

    def incr(self, key, delta=1, initial=None, ttl=0):
        self.counter(key, delta=delta, initial=initial, ttl=ttl)

    def incr_multi(self, keys, delta=1, initial=None, ttl=0):
        self.counter_multi(keys, delta=delta, initial=initial, ttl=ttl)

    def counter(self, key, delta=1, initial=None, ttl=0):
        try:
            self.cb.counter(key, delta=delta, initial=initial, ttl=ttl)
        except CouchbaseException as e:
            try:
                time.sleep(10)
                self.cb.counter(key, delta=delta, initial=initial, ttl=ttl)
            except CouchbaseException as e:
                raise

    def counter_multi(self, keys, delta=1, initial=None, ttl=0):
        try:
            self.cb.counter_multi(keys, delta=delta, initial=initial, ttl=ttl)
        except CouchbaseException as e:
            try:
                time.sleep(10)
                self.cb.counter_multi(keys, delta=delta, initial=initial, ttl=ttl)
            except CouchbaseException as e:
                raise

    def get(self, key, ttl=0, quiet=True, replica=False, no_format=False):
        try:
            rv = self.cb.get(key, ttl=ttl, quiet=quiet, replica=replica, no_format=no_format)
            return self.__translate_get(rv)
        except CouchbaseException as e:
            try:
                time.sleep(10)
                rv = self.cb.get(key, ttl=ttl, quiet=quiet, replica=replica, no_format=no_format)
                return self.__translate_get(rv)
            except CouchbaseException as e:
                raise

    def rget(self, key, replica_index=None, quiet=True):
        try:
            data  = self.rget(key, replica_index=replica_index, quiet=None)
            return self.__translate_get(data)
        except CouchbaseException as e:
            try:
                time.sleep(10)
                data  = self.rget(key, replica_index=replica_index, quiet=None)
                return self.__translate_get(data)
            except CouchbaseException as e:
                raise

    def get_multi(self, keys, ttl=0, quiet=True, replica=False, no_format=False):
        import bulk_doc_operations.doc_ops as doc_op
        try:
            data = doc_op().bulkGet(self.cb, keys)
#             data = self.cb.get_multi(keys, ttl=ttl, quiet=quiet, replica=replica, no_format=no_format)
            return self.__translate_get_multi(data)
        except CouchbaseException as e:
            try:
                time.sleep(10)
                data = self.cb.get_multi(keys, ttl=ttl, quiet=quiet, replica=replica, no_format=no_format)
                return self.__translate_get_multi(data)
            except CouchbaseException as e:
                raise

    def rget_multi(self, key, replica_index=None, quiet=True):
        try:
            data = self.cb.rget_multi(key, replica_index=None, quiet=quiet)
            return self.__translate_get_multi(data)
        except CouchbaseException as e:
            try:
                time.sleep(10)
                data = self.cb.rget_multi(key, replica_index=None, quiet=quiet)
                return self.__translate_get_multi(data)
            except CouchbaseException as e:
                raise

    def stats(self, keys=None):
        try:
            stat_map = self.cb.stats(keys = keys)
            return stat_map
        except CouchbaseException as e:
            try:
                time.sleep(10)
                return self.cb.stats(keys = keys)
            except CouchbaseException as e:
                raise

    def errors(self, clear_existing=True):
        try:
            rv = self.cb.errors(clear_existing = clear_existing)
            return rv
        except CouchbaseException as e:
            raise

    def observe(self, key, master_only=False):
        try:
            return self.cb.observe(key, master_only = master_only)
        except CouchbaseException as e:
            try:
                time.sleep(10)
                return self.cb.observe(key, master_only = master_only)
            except CouchbaseException as e:
                raise

    def observe_multi(self, keys, master_only=False):
        try:
            data = self.cb.observe_multi(keys, master_only = master_only)
            return self.__translate_observe_multi(data)
        except CouchbaseException as e:
            try:
                time.sleep(10)
                data = self.cb.observe_multi(keys, master_only = master_only)
                return self.__translate_observe_multi(data)
            except CouchbaseException as e:
                raise

    def endure(self, key, persist_to=-1, replicate_to=-1, cas=0, check_removed=False, timeout=5.0, interval=0.010):
        try:
            self.cb.endure(key, persist_to=persist_to, replicate_to=replicate_to,
                           cas=cas, check_removed=check_removed, timeout=timeout, interval=interval)
        except CouchbaseException as e:
            try:
                time.sleep(10)
                self.cb.endure(key, persist_to=persist_to, replicate_to=replicate_to,
                    cas=cas, check_removed=check_removed, timeout=timeout, interval=interval)
            except CouchbaseException as e:
                raise

    def endure_multi(self, keys, persist_to=-1, replicate_to=-1, cas=0, check_removed=False, timeout=5.0, interval=0.010):
        try:
            self.cb.endure(keys, persist_to=persist_to, replicate_to=replicate_to,
                           cas=cas, check_removed=check_removed, timeout=timeout, interval=interval)
        except CouchbaseException as e:
            try:
                time.sleep(10)
                self.cb.endure(keys, persist_to=persist_to, replicate_to=replicate_to,
                           cas=cas, check_removed=check_removed, timeout=timeout, interval=interval)
            except CouchbaseException as e:
                raise

    def lock(self, key, ttl=0):
        try:
            data = self.cb.getAndLock(key, ttl=ttl)
            return self.__translate_get(data)
        except CouchbaseException as e:
            try:
                time.sleep(10)
                data = self.cb.getAndLock(key, ttl=ttl)
                return self.__translate_get(data)
            except CouchbaseException as e:
                raise

    def lock_multi(self, keys, ttl=0):
        try:
            data = self.cb.lock_multi(keys, ttl = ttl)
            return self.__translate_get_multi(data)
        except CouchbaseException as e:
            try:
                time.sleep(10)
                data = self.cb.lock_multi(keys, ttl = ttl)
                return self.__translate_get_multi(data)
            except CouchbaseException as e:
                raise

    def unlock(self, key, ttl=0):
        try:
            return self.cb.unlock(key)
        except CouchbaseException as e:
            try:
                time.sleep(10)
                return self.cb.unlock(key)
            except CouchbaseException as e:
                raise

    def unlock_multi(self, keys):
        try:
            return self.cb.unlock_multi(keys)
        except CouchbaseException as e:
            try:
                time.sleep(10)
                return self.cb.unlock_multi(keys)
            except CouchbaseException as e:
                raise

    def n1ql_query(self, statement, prepared=False):
        try:
            return N1qlQuery(statement, prepared)
        except CouchbaseException as e:
            raise

    def n1ql_request(self, query):
        try:
            return N1qlQueryResult(query, self.cb)
        except CouchbaseException as e:
            raise
    
    def __translate_to_json_document(self, key, value, ttl=0):
        try:
            if type(value) != dict:
                value = json.loads(value)
            js = JsonObject.create()
            for field, val in value.items():
                js.put(field, val)
            doc = JsonDocument.create(key, ttl, js)
            return doc
        except DocumentNotJsonException as e:
            return StringDocument.create(key,str(value))
        except Exception as e:
            return StringDocument.create(key,str(value))
        except:
            pass
        return StringDocument.create(key,str(value))
    
    def __translate_get_multi(self, data):
        map = {}
        if data == None:
            return map
        for result in data:
            map[result.id()] = [result.id(), result.cas(), str(result.content())]
        return map

    def __translate_get(self, data):
        return data.id(), data.cas(), data.content()

    def __translate_delete(self, data):
        return data

    def __translate_observe(self, data):
        return data

    def __translate_observe_multi(self, data):
        map = {}
        if data == None:
            return map
        for key, result in data.items():
            map[key] = result.value
        return map

    def __translate_upsert_multi(self, data):
        map = {}
        if data == None:
            return map
        for key, result in data.items():
            map[key] = result
        return map

    def __translate_upsert_op(self, data):
        return data.rc, data.success, data.errstr, data.key
    
    def insert_binary_document(self, keys):
        for key in keys:
            binary_value = Unpooled.copiedBuffer('{value":"' + key + '"}', CharsetUtil.UTF_8)
            self.cb.upsert(BinaryDocument.create(key, binary_value))
    
    def insert_string_document(self, keys):
        for key in keys:
            self.cb.upsert(StringDocument.create(key, '{value":"' + key + '"}'))

    def insert_custom_json_documents(self, key_prefix, documents):
        for index, data in enumerate(documents):
            self.cb.insert(JsonDocument.create(key_prefix+str(index), JsonObject.create().put("content", data)))

    def insert_xattr_attribute(self, document_id, path, value, xattr=True, create_parents=True):
        mutateIn = self.cb.mutateIn(document_id)
        sub_doc = SubdocOptionsBuilder().createParents(create_parents).xattr(xattr)
        builder = mutateIn.insert(path, value, sub_doc)
        builder.execute()
    
    def update_xattr_attribute(self, document_id, path, value, xattr=True, create_parents=True):
        mutateIn = self.cb.mutateIn(document_id)
        sub_doc = SubdocOptionsBuilder().createParents(create_parents).xattr(xattr)
        builder = mutateIn.upsert(path, value, sub_doc)
        builder.execute()
        
    def remove_xattr_attribute(self, document_id, path, xattr=True):
        mutateIn = self.cb.mutateIn(document_id)
        sub_doc = SubdocOptionsBuilder().xattr(xattr)
        builder = mutateIn.remove(path, sub_doc)
        builder.execute()

    def insert_json_documents(self, key_prefix, documents):
        for index, data in enumerate(documents):
            self.cb.insert(JsonDocument.create(key_prefix+str(index), JsonObject.fromJson(data)))

class SDKSmartClient(object):
    def __init__(self, rest, bucket, info = None, compression=True):
        self.rest = rest
        self.server = info
        if hasattr(bucket, 'name'):
            self.bucket = bucket.name
        else:
            self.bucket = bucket

        if rest.ip == "127.0.0.1":
            self.host = "{0}:{1}".format(rest.ip,rest.port)
            self.scheme = "http"
        else:
            self.host = rest.ip
            self.scheme = "couchbase"
        self.client = SDKClient(self.bucket, hosts=[self.host], scheme=self.scheme, password=rest.password, 
                                compression=compression)

    def close(self):
        self.client.close()

    def reset(self,compression=True):
        self.client = SDKClient(self.bucket, hosts=[self.host], scheme=self.scheme, password=self.saslPassword,
                                compression=compression)

    def memcached(self):
        return self.client

    def set(self, key, exp, flags, value, format=FMT_AUTO):
        self.client.insert(key, value, ttl=exp, format=format)

    def append(self, key, value, format=FMT_AUTO):
        return self.client.set(key, value, format=format)

    def observe(self, key):
        return self.client.observe(key)

    def get(self, key):
        return self.client.get(key)

    def getr(self, key, replica_index=0):
        return self.client.rget(key,replica_index=replica_index)

    def setMulti(self, exp, flags, key_val_dic, pause = None, timeout = 5.0, parallel=None, format = FMT_AUTO):
#         try:
#             self.client.cb.timeout = timeout
        return self.client.set_multi(key_val_dic, ttl = exp)
#         finally:
#             self.client.cb.timeout = self.client.default_timeout

    def upsertMulti(self, exp, flags, key_val_dic, pause = None, timeout = 5.0, parallel=None, format = FMT_AUTO):
        return self.client.upsert_multi(key_val_dic, ttl = exp)
    
    def getMulti(self, keys_lst, pause = None, timeout_sec = 5.0, parallel=None):
        map = None
        try:
#             self.client.cb.timeout = timeout_sec
            map = self.client.get_multi(keys_lst)
        except Exception as e:
            return map
        return map
#         finally:
#             self.client.cb.timeout = self.client.default_timeout

    def getrMulti(self, keys_lst, replica_index= None, pause = None, timeout_sec = 5.0, parallel=None):
        try:
            self.client.cb.timeout = timeout_sec
            map = self.client.rget_multi(keys_lst, replica_index = replica_index)
            return map
        finally:
            self.client.cb.timeout = self.client.default_timeout

    def delete(self, key):
        return self.client.remove(key)

