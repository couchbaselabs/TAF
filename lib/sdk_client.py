#!/usr/bin/env python
"""
Java based SDK client interface

"""
import time
import json as pyJson

import logger
import connections.Java_Connection as Java_Connection
import com.couchbase.test.bulk_doc_operations.doc_ops as doc_op

from mimify import repl

from com.couchbase.client.deps.io.netty.buffer import Unpooled
from com.couchbase.client.deps.io.netty.util import CharsetUtil
from com.couchbase.client.java import CouchbaseCluster, ReplicaMode
from com.couchbase.client.java.CouchbaseBucket import mutateIn
from com.couchbase.client.java.document import *
from com.couchbase.client.java.document.json import *
from com.couchbase.client.java.error.subdoc import DocumentNotJsonException
from com.couchbase.client.java.subdoc import SubdocOptionsBuilder
from com.couchbase.client.java.query import N1qlQueryResult, N1qlQuery
from com.couchbase.test.sdk import BucketInterface

# Exceptions
from com.couchbase.client.core import CouchbaseException, BackpressureException
from com.couchbase.client.java.error import CASMismatchException, \
                                            DocumentDoesNotExistException, \
                                            TemporaryFailureException

from java.util.concurrent import TimeUnit
from java.util.logging import Logger, Level, ConsoleHandler
from java.lang import System

from mc_bin_client import MemcachedError

FMT_AUTO = "autoformat"

log = logger.Logger.get_logger()


class SDKClient(object):
    """
    Java SDK Client Implementation for testrunner - master branch
    """

    def __init__(self, bucket, hosts=["localhost"], scheme="couchbase",
                 ssl_path=None, uhm_options=None, password=None,
                 quiet=True, certpath=None, transcoder=None, compression=True):
        self.connection_string = \
            self._createString(scheme=scheme, bucket=bucket, hosts=hosts,
                               certpath=certpath, uhm_options=uhm_options,
                               compression=compression)
        self.hosts = hosts
        self.password = password
        self.bucket = bucket
        self.password = password
        self.quiet = quiet
        self.transcoder = transcoder
        self.default_timeout = 0
        self.cluster = None
        self._createConn()

    def _createString(self, scheme="couchbase", bucket=None,
                      hosts=["localhost"], certpath=None,
                      uhm_options="", ipv6=False, compression=True):
        connection_string = "{0}://{1}" \
                            .format(scheme, ", ".join(hosts).replace(" ", ""))
        # if bucket != None:
        #     connection_string = "{0}/{1}".format(connection_string, bucket)
        if uhm_options is not None:
            connection_string = "{0}?{1}" \
                                .format(connection_string, uhm_options)
        if ipv6 is True:
            if "?" in connection_string:
                connection_string = "{0},ipv6=allow" \
                                    .format(connection_string)
            else:
                connection_string = "{0}?ipv6=allow".format(connection_string)
        if compression is True:
            if "?" in connection_string:
                connection_string = "{0},compression=on" \
                                    .format(connection_string)
            else:
                connection_string = "{0}?compression=on" \
                                    .format(connection_string)
        else:
            if "?" in connection_string:
                connection_string = "{0},compression=off" \
                                    .format(connection_string)
            else:
                connection_string = "{0}?compression=off" \
                                    .format(connection_string)
        if scheme == "couchbases":
            if "?" in connection_string:
                connection_string = "{0},certpath={1}" \
                                    .format(connection_string, certpath)
            else:
                connection_string = "{0}?certpath={1}" \
                                    .format(connection_string, certpath)
        return connection_string

    def _createConn(self):
        try:
            System.setProperty("com.couchbase.forceIPv4", "false")
            logger = Logger.getLogger("com.couchbase.client")
            logger.setLevel(Level.SEVERE)
            for h in logger.getParent().getHandlers():
                if isinstance(h, ConsoleHandler):
                    h.setLevel(Level.SEVERE)
            # self.cluster = CouchbaseCluster.create(Java_Connection.env, self.hosts)
            self.cluster = CouchbaseCluster.fromConnectionString(
                Java_Connection.env, self.connection_string)
            self.cluster.authenticate("Administrator", self.password)
            self.cb = BucketInterface(self.cluster, self.bucket)
            self.cb.connect()
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

    def counter_in(self, key, path, delta, create_parents=True, cas=0,
                   ttl=None, persist_to=0, replicate_to=0):
        try:
            return self.cb.counter_in(key, path, delta, create_parents,
                                      cas, ttl, persist_to, replicate_to)
        except CouchbaseException:
            raise

    def arrayappend_in(self, key, path, value, create_parents=True, cas=0,
                       ttl=None, persist_to=0, replicate_to=0):
        try:
            return self.cb.arrayappend_in(key, path, value, create_parents,
                                          cas, ttl, persist_to, replicate_to)
        except CouchbaseException:
            raise

    def arrayprepend_in(self, key, path, value, create_parents=True, cas=0,
                        ttl=None, persist_to=0, replicate_to=0):
        try:
            return self.cb.arrayprepend_in(key, path, value, create_parents,
                                           cas, ttl, persist_to, replicate_to)
        except CouchbaseException:
            raise

    def arrayaddunique_in(self, key, path, value, create_parents=True, cas=0,
                          ttl=None, persist_to=0, replicate_to=0):
        try:
            return self.cb.addunique_in(key, path, value, create_parents, cas,
                                        ttl, persist_to, replicate_to)
        except CouchbaseException:
            raise

    def arrayinsert_in(self, key, path, value, cas=0, ttl=None,
                       persist_to=0, replicate_to=0):
        try:
            return self.cb.arrayinsert_in(key, path, value, cas, ttl,
                                          persist_to, replicate_to)
        except CouchbaseException:
            raise

    def remove_in(self, key, path,  cas=0, ttl=None):
        try:
            self.cb.remove_in(key, path, cas, ttl)
        except CouchbaseException:
            raise

    def mutate_in(self, key, *specs, **kwargs):
        try:
            self.cb.mutate_in(key, *specs, **kwargs)
        except CouchbaseException:
            raise

    def lookup_in(self, key):
        try:
            self.cb.lookupIn(key)
        except CouchbaseException:
            raise

    def get_in(self, key, path):
        try:
            result = self.cb.get_in(key, path)
            return self.__translate_get(result)
        except CouchbaseException:
            raise

    def exists_in(self, key, path):
        try:
            self.cb.exists_in(key, path)
        except CouchbaseException:
            raise

    def replace_in(self, key, path, value, cas=0, ttl=None,
                   persist_to=0, replicate_to=0):
        try:
            return self.cb.replace_in(key, path, value, cas, ttl,
                                      persist_to, replicate_to)
        except CouchbaseException:
            raise

    def insert_in(self, key, path, value, create_parents=True, cas=0, ttl=None,
                  persist_to=0, replicate_to=0):
        try:
            return self.cb.insert_in(key, path, value, create_parents,
                                     cas, ttl, persist_to, replicate_to)
        except CouchbaseException:
            raise

    def upsert_in(self, key, path, value, create_parents=True, cas=0, ttl=None,
                  persist_to=0, replicate_to=0):
        try:
            return self.cb.upsert_in(key, path, value, create_parents,
                                     cas, ttl, persist_to, replicate_to)
        except CouchbaseException:
            raise

    def append(self, key, value, cas=0, format=None,
               persist_to=0, replicate_to=0):
        try:
            self.cb.append(key, value, cas, format, persist_to, replicate_to)
        except CouchbaseException:
            try:
                time.sleep(10)
                self.cb.append(key, value, cas, format,
                               persist_to, replicate_to)
            except CouchbaseException:
                raise

    def append_multi(self, keys, cas=0, format=None,
                     persist_to=0, replicate_to=0):
        try:
            self.cb.append_multi(keys, cas, format,
                                 persist_to, replicate_to)
        except CouchbaseException:
            try:
                time.sleep(10)
                self.cb.append_multi(keys, cas, format,
                                     persist_to, replicate_to)
            except CouchbaseException:
                raise

    def prepend(self, key, value, cas=0, format=None,
                persist_to=0, replicate_to=0):
        try:
            self.cb.prepend(key, value, cas, format, persist_to, replicate_to)
        except CouchbaseException:
            try:
                self.cb.prepend(key, value, cas, format,
                                persist_to, replicate_to)
            except CouchbaseException:
                raise

    def prepend_multi(self, keys, cas=0, format=None,
                      persist_to=0, replicate_to=0):
        try:
            self.cb.prepend_multi(keys, cas, format, persist_to, replicate_to)
        except CouchbaseException:
            try:
                time.sleep(10)
                self.cb.prepend_multi(keys, cas, format,
                                      persist_to, replicate_to)
            except CouchbaseException:
                raise

    def replace(self, key, value, cas=0, ttl=None, format=None,
                persist_to=0, replicate_to=0):
        try:
            self.cb.replace(key, value, cas, ttl, format,
                            persist_to, replicate_to)
        except CouchbaseException:
            try:
                time.sleep(10)
                self.cb.replace(key, value, cas, ttl, format,
                                persist_to, replicate_to)
            except CouchbaseException:
                raise

    def replace_multi(self, keys, cas=0, ttl=None, format=None,
                      persist_to=0, replicate_to=0):
        try:
            self.cb.replace_multi(keys, cas, ttl, format,
                                  persist_to, replicate_to)
        except CouchbaseException:
            try:
                time.sleep(10)
                self.cb.replace_multi(keys, cas, ttl, format,
                                      persist_to, replicate_to)
            except CouchbaseException:
                raise

    def cas(self, key, value, cas=0, ttl=None, format=None):
        return self.cb.replace(key, value, cas, format)

    def delete(self, key, cas=0, quiet=True, persist_to=0, replicate_to=0,
               max_retry=3):
        self.remove(key, cas=cas, quiet=quiet,
                    persist_to=persist_to, replicate_to=replicate_to,
                    max_retry=max_retry)

    def remove(self, key, persist_to=None, replicate_to=None,
               timeout=None, timeunit=None, max_retry=3):
        retry_count = 0
        while retry_count <= max_retry:
            try:
                do_retry = self.generic_remove(
                    key, persistTo=persist_to, replicateTo=replicate_to,
                    timeout=timeout, timeunit=timeunit)
                if do_retry is True:
                    retry_count += 1
                    # Sleep for 5 sec before next retry
                    time.sleep(5)
                else:
                    break
            except Exception as e:
                log.error("Error during remove of {0} - {1}"
                          .format(key, e))

    def remove_multi(self, keys, quiet=True, persist_to=0, replicate_to=0):
        try:
            self.cb.remove_multi(keys, quiet, persist_to, replicate_to)
        except CouchbaseException:
            try:
                time.sleep(10)
                self.cb.remove_multi(keys, quiet, persist_to, replicate_to)
            except CouchbaseException:
                raise

    def set(self, key, value, ttl=None, format=None,
            persist_to=0, replicate_to=0):
        doc = self.__translate_to_json_document(key, value, ttl)
        try:
            return self.cb.insert(doc)
        except CouchbaseException:
            try:
                time.sleep(10)
                return self.cb.insertWithPersistToReplicateToAndTimeout(
                    doc, persist_to, replicate_to, ttl, TimeUnit.SECONDS)
            except CouchbaseException:
                raise

    def upsert(self, key, value, ttl=None, persist_to=0, replicate_to=0):
        doc = self.__translate_to_json_document(key, value, ttl)
        try:
            return self.cb.upsert(doc)
        except CouchbaseException:
            try:
                time.sleep(10)
                return self.cb.upsert(doc)
            except CouchbaseException:
                raise

    def set_multi(self, keys, ttl=None, format=None,
                  persist_to=0, replicate_to=0, timeOut=10,
                  timeUnit="seconds", retry=5, doc_type="json"):
        docs = []
        for key, value in keys.items():
            docs.append(self.__translate_to_json_document(key, value, ttl,
                                                          doc_type=doc_type))
        #success = {}
        #fail = {}
        result = doc_op().bulkSet(self.cb.getBucketObj(), docs,
                                  persist_to, replicate_to,
                                  timeOut, timeUnit)
        success, fail = self.__translate_upsert_multi(result)
        return success, fail
        # while retry > 0:
        #     result = doc_op().bulkSet(self.cb.getBucketObj(), docs,
        #                               persist_to, replicate_to,
        #                               timeOut, timeUnit)
        #     success, fail = self.__translate_upsert_multi(result)
        #     if fail:
        #         docs = [doc[3] for doc in fail.values()]
        #         retry -= 1
        #         errors = [doc[0] for doc in fail.values()]
        #         log.warning("Retrying {0} documents again. Error reasons: {1}."
        #                     "Retry count: {2}"
        #                     .format(docs.__len__(), errors, retry + 1))
        #         time.sleep(5)
        #     else:
        #         return success
        # if retry == 0:
        #     errors = [doc[0] for doc in fail.values()]
        #     errors = set(errors)
        #     log.error("Could not load all documents in this set."
        #               "Failure count={0}, reasons: {1}"
        #               .format(len(fail), errors))
        #     return fail

    def upsert_multi(self, keys, ttl=None, persist_to=0, replicate_to=0,
                     timeOut=10, timeUnit="seconds", retry=5, doc_type="json"):
        docs = []
        for key, value in keys.items():
            docs.append(self.__translate_to_json_document(key, value, ttl,
                                                          doc_type=doc_type))
        #success = {}
        #fail = {}
        result = doc_op().bulkUpsert(self.cb.getBucketObj(), docs,
                                     persist_to, replicate_to,
                                     timeOut, timeUnit)
        success, fail = self.__translate_upsert_multi(result)
        return success, fail
        # while retry > 0:
        #     result = doc_op().bulkUpsert(self.cb.getBucketObj(), docs,
        #                                  persist_to, replicate_to,
        #                                  timeOut, timeUnit)
        #     success, fail = self.__translate_upsert_multi(result)
        #     if fail:
        #         docs = [doc[3] for doc in fail.values()]
        #         result -= 1
        #         time.sleep(5)
        #     else:
        #         return success
        # if retry == 0:
        #     log.error("Could not load all documents in this set. Failed set={}"
        #               .format(fail.__str__()))
        #     return fail

    def insert(self, key, value, ttl=None, format=None,
               persist_to=0, replicate_to=0, doc_type="json"):
        doc = self.__translate_to_json_document(key, value, ttl,
                                                doc_type=doc_type)
        try:
            self.cb.insert(doc)
        except CouchbaseException:
            try:
                time.sleep(10)
                self.cb.insert(doc)
            except CouchbaseException:
                raise

    def insert_multi(self, keys,  ttl=None, format=None,
                     persist_to=0, replicate_to=0, retry=5,
                     doc_type="json"):
        import bulk_doc_operations.doc_ops as doc_op
        docs = []
        for key, value in keys.items():
            docs.append(self.__translate_to_json_document(key, value, ttl,
                                                          doc_type=doc_type))
        doc_op().bulkSet(self.cb.getBucketObj(), docs)

    def touch(self, key, ttl=None):
        try:
            self.cb.getAndTouch(key, ttl)
        except CouchbaseException:
            try:
                time.sleep(10)
                self.cb.touch(key, ttl)
            except CouchbaseException:
                raise

    def touch_multi(self, keys, ttl=None):
        for key in keys:
            self.touch(key, ttl=ttl)

    def decr(self, key, delta=1, initial=None, ttl=None):
        self.counter(key, delta=-delta, initial=initial, ttl=ttl)

    def decr_multi(self, keys, delta=1, initial=None, ttl=None):
        self.counter_multi(keys, delta=-delta, initial=initial, ttl=ttl)

    def incr(self, key, delta=1, initial=None, ttl=None):
        self.counter(key, delta=delta, initial=initial, ttl=ttl)

    def incr_multi(self, keys, delta=1, initial=None, ttl=None):
        self.counter_multi(keys, delta=delta, initial=initial, ttl=ttl)

    def generic_counter(self, key, delta=0, initial=None, ttl=None,
                        persistTo=None, replicateTo=None,
                        timeout=None, timeUnit=None):
        if initial is None:
            if ttl == persistTo == replicateTo == timeout == timeUnit is None:
                self.cb.counter(key, delta)
            elif ttl == persistTo == replicateTo is None and \
                    None not in (timeout, timeUnit):
                self.cb.counterWithTimeout(key, delta, timeout, timeUnit)
            elif ttl == replicateTo == timeout == timeUnit is None and \
                    persistTo is not None:
                self.cb.counterWithPersistTo(key, delta, persistTo)
            elif ttl == replicateTo is None and \
                    None not in (persistTo, timeout, timeUnit):
                self.cb.counterWithPersistToAndTimeout(key, delta, persistTo,
                                                       timeout, timeUnit)
            elif ttl == timeout == timeUnit is None and \
                    None not in (persistTo, replicateTo):
                self.cb.counterWithPersistToReplicateTo(key, delta,
                                                        persistTo, replicateTo)
            elif ttl == replicateTo is None and \
                    replicateTo is not None:
                self.cb.counterWithReplicateTo(key, delta, replicateTo)
            elif ttl == replicateTo == timeout == timeUnit is None and \
                    None not in (replicateTo, timeout, timeUnit):
                self.cb.counterWithReplicateToAndTimeout(key, delta,
                                                         replicateTo,
                                                         timeout, timeUnit)
            elif None not in (persistTo, replicateTo, timeout, timeUnit):
                self.cb.counterWithPersistToReplicateToAndTimeout(key, delta,
                                                                  persistTo,
                                                                  replicateTo,
                                                                  timeout,
                                                                  timeUnit)
        else:
            if ttl == persistTo == replicateTo == timeout == timeUnit is None:
                self.cb.counterWithInitial(key, delta, initial)
            elif persistTo == replicateTo == timeout == timeUnit is None and \
                    ttl is not None:
                self.cb.counterWithInitialExpiry(key, delta, initial, ttl)
            elif persistTo == replicateTo is None and \
                    None not in (ttl, timeout, timeUnit):
                self.cb.counterWithInitialExpiryAndTimeout(key, delta, initial,
                                                           ttl,
                                                           timeout, timeUnit)
            elif replicateTo == timeout == timeUnit is None and \
                    None not in (ttl, persistTo):
                self.cb.counterWithInitialExpiryPersistTo(key, delta, initial,
                                                          ttl, persistTo)
            elif replicateTo is None and \
                    None not in (ttl, persistTo, timeout, timeUnit):
                self.cb.counterWithInitialExpiryPersistToAndTimeout(key, delta,
                                                                    initial,
                                                                    ttl,
                                                                    persistTo,
                                                                    timeout,
                                                                    timeUnit)
            elif timeout == timeUnit is None and \
                    None not in (ttl, persistTo, replicateTo):
                self.cb.counterWithInitialExpiryPersistToReplicateTo(key,
                                                                     delta,
                                                                     initial,
                                                                     ttl,
                                                                     persistTo,
                                                                     replicateTo)
            elif None not in (ttl, persistTo, replicateTo, timeout, timeUnit):
                self.cb.counterWithInitialExpiryPersistToReplicateToAndTimeout(key,
                                                                               delta,
                                                                               initial,
                                                                               ttl,
                                                                               persistTo,
                                                                               replicateTo,
                                                                               timeout,
                                                                               timeUnit)
            elif persistTo == timeout == timeUnit is None and \
                    None not in (ttl, replicateTo):
                self.cb.counterWithInitialExpiryReplicateTo(key, delta,
                                                            initial, ttl,
                                                            replicateTo)
            elif persistTo is None and \
                    None not in (ttl, replicateTo, timeout, timeUnit):
                self.cb.counterWithInitialExpiryReplicateToAndTimeout(key,
                                                                      delta,
                                                                      initial,
                                                                      ttl,
                                                                      replicateTo,
                                                                      timeout,
                                                                      timeUnit)
            elif ttl == persistTo == replicateTo is None and \
                    None not in (timeout, timeUnit):
                self.cb.counterWithInitialAndTimeout(key, delta, initial,
                                                     timeout, timeUnit)
            elif ttl == replicateTo == timeout == timeUnit is None and \
                    persistTo is not None:
                self.cb.counterWithInitialPersistTo(key, delta, initial,
                                                    persistTo)
            elif ttl == replicateTo is None and \
                    None not in (persistTo, timeout, timeUnit):
                self.cb.counterWithInitialPersistToAndTimeout(key, delta,
                                                              initial,
                                                              persistTo,
                                                              timeout,
                                                              timeUnit)
            elif ttl == timeout == timeUnit is None and \
                    None not in (persistTo, replicateTo):
                self.cb.counterWithInitialPersistToReplicateTo(key, delta,
                                                               initial,
                                                               persistTo,
                                                               replicateTo)
            elif ttl is None and \
                    None not in (persistTo, replicateTo, timeout, timeUnit):
                self.cb.counterWithInitialPersistToReplicateToAndTimeout(key,
                                                                         delta,
                                                                         initial,
                                                                         persistTo,
                                                                         replicateTo,
                                                                         timeout,
                                                                         timeUnit)
            elif ttl == persistTo == timeout == timeUnit is None and \
                    replicateTo is not None:
                self.cb.counterWithInitialReplicateTo(key, delta, initial,
                                                      replicateTo)
            elif ttl == persistTo is None and \
                    None not in (replicateTo, timeout, timeUnit):
                self.cb.counterWithInitialReplicateToAndTimeout(
                    key, delta, initial, replicateTo, timeout, timeUnit)

    def generic_remove(self, key, persistTo=None, replicateTo=None,
                       timeout=None, timeunit=None):
        retry = False
        try:
            if timeout == timeunit is None:
                if persistTo == replicateTo is None:
                    self.cb.remove(key)
                elif persistTo is not None and replicateTo is None:
                    self.cb.removeWithPersistTo(key, persistTo)
                elif replicateTo is not None and persistTo is None:
                    self.cb.removeWithReplicateTo(key, replicateTo)
                elif None not in [replicateTo, persistTo]:
                    self.cb.removeWithPersistToReplicateTo(
                        key, persistTo, replicateTo)
            elif None not in [timeout, timeunit]:
                if persistTo == replicateTo is None:
                    self.cb.removeWithTimeout(key, timeout, timeunit)
                elif persistTo is not None and replicateTo is None:
                    self.cb.removeWithPersistToAndTimeout(key, persistTo,
                                                          timeout, timeunit)
                elif replicateTo is not None and persistTo is None:
                    self.cb.removeWithReplicateToAndTimeout(key, replicateTo,
                                                            timeout, timeunit)
                elif None not in [replicateTo, persistTo]:
                    self.cb.removeWithPersistToReplicateToAndTimeout(
                        key, persistTo, replicateTo, timeout, timeunit)
        except DocumentDoesNotExistException as e:
            log.error("Exception: Document id {0} not found - {1}"
                      .format(key, e))
            raise(e)
        except CASMismatchException as e:
            log.error("Exception: Cas mismatch for doc {0} - {1}"
                      .format(key, e))
            raise(e)
        except (BackpressureException, TemporaryFailureException) as e:
            log.warning("Exception: Retry for doc {0} - {1}"
                        .format(key, e))
            retry = True
        except CouchbaseException as e:
            log.error("Generic exception for doc {0} - {1}"
                      .format(key, e))
            raise(e)
        return retry

    def counter(self, key, delta=1, initial=None, ttl=None,
                persistTo=None, replicateTo=None, timeout=None, timeUnit=None):
        try:
            self.generic_counter(key, delta, initial, ttl,
                                 persistTo, replicateTo, timeout, timeUnit)
        except CouchbaseException:
            try:
                time.sleep(10)
                self.generic_counter(key, delta, initial, ttl,
                                     persistTo, replicateTo, timeout, timeUnit)
            except CouchbaseException:
                raise

    def counter_multi(self, keys, delta=1, initial=None, ttl=None):
        try:
            self.cb.counter_multi(keys, delta, initial, ttl)
        except CouchbaseException:
            try:
                time.sleep(10)
                self.cb.counter_multi(keys, delta, initial, ttl)
            except CouchbaseException:
                raise

    def get(self, key, ttl=None, quiet=True, replica=False, no_format=False):
        try:
            rv = self.cb.getBucketObj().get(key)
            return self.__translate_get(rv)
        except CouchbaseException:
            try:
                time.sleep(1)
                rv = self.cb.getBucketObj().get(key)
                return self.__translate_get(rv)
            except CouchbaseException as e:
                raise e

    def getfromReplica(self, key, ReplicaMode=None):
        try:
            data = self.cb.getFromReplica(key, ReplicaMode)
#             print data
            return self.__translate_get_replica(data)
        except CouchbaseException:
            try:
                time.sleep(10)
                data = self.cb.getfromReplica(key, ReplicaMode)
                print data
                return self.__translate_get_replica(data)
            except CouchbaseException:
                raise

    def get_multi(self, keys, ttl=None, quiet=True, replica=False,
                  no_format=False):
        try:
            data = doc_op().bulkGet(self.cb.getBucketObj(), keys)
            # data = self.cb.get_multi(keys, ttl, quiet, replica, no_format)
            return self.__translate_get_multi(data)
        except CouchbaseException:
            try:
                time.sleep(10)
                data = self.cb.get_multi(keys, ttl, quiet, replica, no_format)
                return self.__translate_get_multi(data)
            except CouchbaseException:
                raise

    def rget_multi(self, key, replica_index=None, quiet=True):
        try:
            data = self.cb.rget_multi(key, replica_index, quiet)
            return self.__translate_get_multi(data)
        except CouchbaseException:
            try:
                time.sleep(10)
                data = self.cb.rget_multi(key, replica_index, quiet)
                return self.__translate_get_multi(data)
            except CouchbaseException:
                raise

    def stats(self, keys=None):
        try:
            stat_map = self.cb.stats(keys)
            return stat_map
        except CouchbaseException:
            try:
                time.sleep(10)
                return self.cb.stats(keys)
            except CouchbaseException:
                raise

    def errors(self, clear_existing=True):
        try:
            rv = self.cb.errors(clear_existing)
            return rv
        except CouchbaseException:
            raise

    def observe(self, key, master_only=False):
        try:
            return self.cb.observe(key, master_only)
        except CouchbaseException:
            try:
                time.sleep(10)
                return self.cb.observe(key, master_only)
            except CouchbaseException:
                raise

    def observe_multi(self, keys, master_only=False):
        try:
            data = self.cb.observe_multi(keys, master_only)
            return self.__translate_observe_multi(data)
        except CouchbaseException:
            try:
                time.sleep(10)
                data = self.cb.observe_multi(keys, master_only)
                return self.__translate_observe_multi(data)
            except CouchbaseException:
                raise

    def endure(self, key, persist_to=-1, replicate_to=-1, cas=0,
               check_removed=False, timeout=5.0, interval=0.010):
        try:
            self.cb.endure(key, persist_to, replicate_to,
                           cas, check_removed, timeout, interval)
        except CouchbaseException:
            try:
                time.sleep(10)
                self.cb.endure(key, persist_to, replicate_to,
                               cas, check_removed, timeout, interval)
            except CouchbaseException:
                raise

    def endure_multi(self, keys, persist_to=-1, replicate_to=-1, cas=0,
                     check_removed=False, timeout=5.0, interval=0.010):
        try:
            self.cb.endure(keys, persist_to, replicate_to,
                           cas, check_removed, timeout, interval)
        except CouchbaseException:
            try:
                time.sleep(10)
                self.cb.endure(keys, persist_to, replicate_to,
                               cas, check_removed, timeout, interval)
            except CouchbaseException:
                raise

    def lock(self, key, ttl=None):
        try:
            data = self.cb.getAndLock(key, ttl)
            return self.__translate_get(data)
        except CouchbaseException:
            try:
                time.sleep(10)
                data = self.cb.getAndLock(key, ttl)
                return self.__translate_get(data)
            except CouchbaseException:
                raise

    def lock_multi(self, keys, ttl=None):
        try:
            data = self.cb.lock_multi(keys, ttl)
            return self.__translate_get_multi(data)
        except CouchbaseException:
            try:
                time.sleep(10)
                data = self.cb.lock_multi(keys, ttl)
                return self.__translate_get_multi(data)
            except CouchbaseException:
                raise

    def unlock(self, key, ttl=None):
        try:
            return self.cb.unlock(key)
        except CouchbaseException:
            try:
                time.sleep(10)
                return self.cb.unlock(key)
            except CouchbaseException:
                raise

    def unlock_multi(self, keys):
        try:
            return self.cb.unlock_multi(keys)
        except CouchbaseException:
            try:
                time.sleep(10)
                return self.cb.unlock_multi(keys)
            except CouchbaseException:
                raise

    def n1ql_query(self, statement, prepared=False):
        try:
            return N1qlQuery(statement, prepared)
        except CouchbaseException:
            raise

    def n1ql_request(self, query):
        try:
            return N1qlQueryResult(query, self.cb)
        except CouchbaseException:
            raise

    def __translate_to_json_document(self, key, value, ttl=0, doc_type="json"):
        try:
            if doc_type.find("json") != -1:
                js = JsonObject.create()
                value = pyJson.loads(value)
                for field, val in value.items():
                    js.put(field, val)
                doc = JsonDocument.create(key, ttl, js)
                return doc
            elif doc_type.find("binary") != -1:
                return BinaryDocument.create(key, Unpooled.copiedBuffer(value,
                                                                        CharsetUtil.UTF_8))
        except Exception:
            return JsonStringDocument.create(key, str(value))
        return JsonStringDocument.create(key, str(value))

    def __translate_upsert_multi(self, data):
        success = dict()
        fail = dict()
        if data is None:
            return success, fail
        for result in data:
            res = result['Status']
            if res:
                document = result['Document']
                success[document.id()] = [document.id(), document.cas(),
                                          document.content(), document]
            else:
                error = result['Error']
                document = result['Document']
                fail[document.id()] = [error, document.id(),
                                       document.content(), document]
        return success, fail

    def __translate_get_multi(self, data):
        map = dict()
        if data is None:
            return map
        for result in data:
            map[result.id()] = [result.id(), result.cas(),
                                str(result.content())]
        return map

    def __translate_get(self, data):
        if data:
            return data.id(), data.cas(), data.content()
        else:
            return None, None, None

    def __translate_get_replica(self, data):
        map = dict()
        if data is None:
            return map
        for result in data:
            map[result.id()] = [result.id(), result.cas(),
                                str(result.content())]
        return map

    def __translate_delete(self, data):
        return data

    def __translate_observe(self, data):
        return data

    def __translate_observe_multi(self, data):
        map = dict()
        if data is None:
            return map
        for key, result in data.items():
            map[key] = result.value
        return map

    def __translate_upsert_op(self, data):
        return data.rc, data.success, data.errstr, data.key

    def insert_binary_document(self, keys):
        for key in keys:
            binary_value = Unpooled.copiedBuffer('{value":"' + key + '"}',
                                                 CharsetUtil.UTF_8)
            self.cb.upsert(BinaryDocument.create(key, binary_value))

    def insert_string_document(self, keys):
        for key in keys:
            self.cb.upsert(StringDocument.create(key,
                                                 '{value":"' + key + '"}'))

    def insert_custom_json_documents(self, key_prefix, documents):
        for index, data in enumerate(documents):
            self.cb.insert(JsonDocument.create(key_prefix+str(index),
                                               JsonObject.create().put("content", data)))

    def insert_xattr_attribute(self, document_id, path, value, xattr=True,
                               create_parents=True):
        mutateIn = self.cb.mutateIn(document_id)
        sub_doc = SubdocOptionsBuilder().createParents(create_parents).xattr(xattr)
        builder = mutateIn.insert(path, value, sub_doc)
        builder.execute()

    def update_xattr_attribute(self, document_id, path, value, xattr=True,
                               create_parents=True):
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
            self.cb.insert(JsonDocument.create(key_prefix+str(index),
                                               JsonObject.fromJson(data)))


class SDKSmartClient(object):
    def __init__(self, rest, bucket, info=None, compression=True):
        self.rest = rest
        self.server = info
        if hasattr(bucket, 'name'):
            self.bucket = bucket.name
        else:
            self.bucket = bucket

        if rest.ip == "127.0.0.1":
            self.host = "{0}:{1}".format(rest.ip, rest.port)
            self.scheme = "http"
        else:
            self.host = rest.ip
            self.scheme = "couchbase"
        self.client = SDKClient(self.bucket, hosts=[self.host],
                                scheme=self.scheme, password=rest.password,
                                compression=compression)
        self.MemcachedError = MemcachedError

    def close(self):
        self.client.close()

    def reset(self, compression=True):
        self.client = SDKClient(self.bucket, hosts=[self.host],
                                scheme=self.scheme, password=self.saslPassword,
                                compression=compression)
    def reconnect(self):
        self.client.reconnect()

    def get_client(self):
        return self.client

    def set(self, key, exp, flags, value, format=FMT_AUTO):
        self.client.insert(key, value, ttl=exp, format=format)

    def append(self, key, value, format=FMT_AUTO):
        return self.client.set(key, value, format=format)

    def observe(self, key):
        return self.client.observe(key)

    def get(self, key):
        return self.client.get(key)

    def getfromReplica(self, key, ReplicaMode=ReplicaMode.ALL):
        return self.client.getfromReplica(key, ReplicaMode=ReplicaMode)

    def setMulti(self, key_val_dic, exp, exp_unit="seconds",
                 persist_to=0, replicate_to=0, timeout=5, time_unit="seconds", retry=5,
                 doc_type="json", durability=None):
        return self.client.set_multi(key_val_dic, ttl=exp,
                                     persist_to=persist_to,
                                     replicate_to=replicate_to,
                                     timeOut=timeout, timeUnit=time_unit,
                                     retry=retry, doc_type=doc_type)

    def upsertMulti(self, key_val_dic, exp, exp_unit="seconds",
                 persist_to=0, replicate_to=0, timeout=5, time_unit="seconds", retry=5,
                 doc_type="json", durability=None):
        return self.client.upsert_multi(key_val_dic, ttl=exp,
                                        persist_to=persist_to,
                                        replicate_to=replicate_to,
                                        timeOut=timeout, timeUnit=time_unit,
                                        retry=retry, doc_type=doc_type)

    def getMulti(self, keys_lst):
        map = None
        try:
            # self.client.cb.timeout = timeout_sec
            map = self.client.get_multi(keys_lst)
        except Exception:
            return map
        return map
#         finally:
#             self.client.cb.timeout = self.client.default_timeout

    def getrMulti(self, keys_lst, replica_index=None, pause=None,
                  timeout_sec=5.0, parallel=None):
        try:
            self.client.cb.timeout = timeout_sec
            map = self.client.rget_multi(keys_lst, replica_index=replica_index)
            return map
        finally:
            self.client.cb.timeout = self.client.default_timeout

    def delete(self, key, persist_to=None, replicate_to=None,
               timeout=None, timeunit=None, max_retry=3):
        return self.client.remove(key, persist_to=persist_to,
                                  replicate_to=replicate_to,
                                  timeout=timeout, timeunit=timeunit,
                                  max_retry=max_retry)
