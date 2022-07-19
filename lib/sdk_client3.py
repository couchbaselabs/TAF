#!/usr/bin/env python
"""
Java based SDK client interface
Created on Mar 14, 2019

"""

import json as pyJson
import subprocess
import os
import com.couchbase.client.core.deps.io.netty.handler.ssl.util.InsecureTrustManagerFactory \
    as InsecureTrustManagerFactory
import time

from _threading import Lock


from com.couchbase.client.core.env import \
    CompressionConfig, \
    SeedNode, \
    TimeoutConfig, IoConfig, \
    SecurityConfig
from com.couchbase.client.core.error import \
    CasMismatchException, \
    ConfigException, \
    CouchbaseException, \
    DocumentExistsException, \
    DocumentNotFoundException, \
    DurabilityAmbiguousException, \
    DurabilityImpossibleException, \
    FeatureNotAvailableException, \
    ReplicaNotConfiguredException, \
    RequestCanceledException, \
    ServerOutOfMemoryException, \
    TemporaryFailureException, \
    TimeoutException
from com.couchbase.client.core.service import ServiceType
from com.couchbase.client.java import Cluster, ClusterOptions
from com.couchbase.client.java.codec import RawBinaryTranscoder,\
    RawStringTranscoder
from com.couchbase.client.java.diagnostics import WaitUntilReadyOptions
from com.couchbase.client.java.env import ClusterEnvironment
from com.couchbase.client.java.json import JsonObject
from com.couchbase.client.java.manager.collection import CollectionSpec
from com.couchbase.client.java.kv import GetAllReplicasOptions
from java.time import Duration
from java.nio.charset import StandardCharsets
from java.lang import System, String
from java.util import \
    Collections, \
    HashSet, \
    Optional
from reactor.util.function import Tuples

import com.couchbase.test.doc_operations_sdk3.doc_ops as doc_op
import com.couchbase.test.doc_operations_sdk3.SubDocOperations as sub_doc_op
from com.couchbase.client.core.deps.io.netty.buffer import Unpooled
from com.couchbase.client.core.deps.io.netty.util import CharsetUtil

from Cb_constants import ClusterRun, CbServer, DocLoading
from constants.sdk_constants.java_client import SDKConstants
from global_vars import logger
from sdk_utils.java_sdk import SDKOptions
from sdk_exceptions import SDKException


class SDKClientPool(object):
    """
    Client pool manager for list of SDKClients per bucket which can be
    reused / shared across multiple tasks
    """
    def __init__(self):
        self.log = logger.get("infra")
        self.clients = dict()

    def shutdown(self):
        """
        Shutdown all active clients managed by this ClientPool Object
        :return None:
        """
        self.log.debug("Closing clients from SDKClientPool")
        for bucket_name, bucket_dict in self.clients.items():
            for client in bucket_dict["idle_clients"] \
                          + bucket_dict["busy_clients"]:
                client.close()
        self.clients = dict()

    def create_clients(self, bucket, servers,
                       req_clients=1,
                       username="Administrator", password="password",
                       compression_settings=None):
        """
        Create set of clients for the specified bucket and client settings.
        All created clients will be saved under the respective bucket key.

        :param bucket: Bucket object for which the clients will be created
        :param servers: List of servers for SDK to establish initial
                        connections with
        :param req_clients: Required number of clients to be created for the
                            given bucket and client settings
        :param username: User name using which to establish the connection
        :param password: Password for username authentication
        :param compression_settings: Same as expected by SDKClient class
        :return:
        """
        if bucket.name not in self.clients:
            self.clients[bucket.name] = dict()
            self.clients[bucket.name]["lock"] = Lock()
            self.clients[bucket.name]["idle_clients"] = list()
            self.clients[bucket.name]["busy_clients"] = list()

        for _ in range(req_clients):
            self.clients[bucket.name]["idle_clients"].append(SDKClient(
                servers, bucket,
                username=username, password=password,
                compression_settings=compression_settings))

    def get_client_for_bucket(self, bucket, scope=CbServer.default_scope,
                              collection=CbServer.default_collection):
        """
        API to get a client which can be used for SDK operations further
        by a callee.
        Note: Callee has to choose the scope/collection to work on
              later by itself.
        :param bucket: Bucket object for which the client has to selected
        :param scope: Scope name to select for client operation
        :param collection: Collection name to select for client operation
        :return client: Instance of SDKClient object
        """
        client = None
        col_name = scope + collection
        if bucket.name not in self.clients:
            return client
        while client is None:
            self.clients[bucket.name]["lock"].acquire()
            if col_name in self.clients[bucket.name]:
                # Increment tasks' reference counter using this client object
                client = self.clients[bucket.name][col_name]["client"]
                self.clients[bucket.name][col_name]["counter"] += 1
            elif self.clients[bucket.name]["idle_clients"]:
                client = self.clients[bucket.name]["idle_clients"].pop()
                client.select_collection(scope, collection)
                self.clients[bucket.name]["busy_clients"].append(client)
                # Create scope/collection reference using the client object
                self.clients[bucket.name][col_name] = dict()
                self.clients[bucket.name][col_name]["client"] = client
                self.clients[bucket.name][col_name]["counter"] = 1
            self.clients[bucket.name]["lock"].release()
        return client

    def release_client(self, client):
        """
        Release the acquired SDKClient object back into the pool
        :param client: Instance of SDKClient object
        :return None:
        """
        bucket = client.bucket
        if bucket.name not in self.clients:
            return
        col_name = client.scope_name + client.collection_name
        self.clients[bucket.name]["lock"].acquire()
        if self.clients[bucket.name][col_name]["counter"] == 1:
            self.clients[bucket.name].pop(col_name)
            self.clients[bucket.name]["busy_clients"].remove(client)
            self.clients[bucket.name]["idle_clients"].append(client)
        else:
            self.clients[bucket.name][col_name]["counter"] -= 1
        self.clients[bucket.name]["lock"].release()


class SDKClient(object):
    System.setProperty("com.couchbase.forceIPv4", "false")
    env = ClusterEnvironment \
        .builder() \
        .ioConfig(IoConfig.numKvConnections(25)) \
        .timeoutConfig(TimeoutConfig.builder()
                       .connectTimeout(Duration.ofSeconds(20))
                       .kvTimeout(Duration.ofSeconds(10)))
    cluster_env = env.build()
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
                 username=None, password=None,
                 compression_settings=None, cert_path=None):
        """
        :param servers: List of servers for SDK to establish initial
                        connections with
        :param bucket: Bucket object to which the SDK connection will happen
        :param scope:  Name of the scope to connect.
                       Default: '_default'
        :param collection: Name of the collection to connect.
                           Default: _default
        :param username: User name using which to establish the connection
        :param password: Password for username authentication
        :param compression_settings: Dict of compression settings. Format:
                                     {
                                      "enabled": Bool,
                                      "minRatio": Double int (None to default),
                                      "minSize": int (None to default)
                                     }
        :param cert_path: Path of certificate file to establish connection
        """
        # Used during Cluster.connect() call
        self.hosts = list()

        # Used while creating connection for Cluster_run
        self.servers = list()

        self.scope_name = scope
        self.collection_name = collection
        self.username = username or servers[0].rest_username
        self.password = password or servers[0].rest_password
        self.default_timeout = 0
        self.cluster = None
        self.bucket = bucket
        self.bucketObj = None
        self.collection = None
        self.compression = compression_settings
        self.cert_path = cert_path
        self.log = logger.get("test")
        for server in servers:
            self.servers.append((server.ip, int(server.port)))
            if server.ip == "127.0.0.1":
                if CbServer.use_https:
                    self.scheme = "https"
                else:
                    self.scheme = "http"
            else:
                self.hosts.append(server.ip)
                if CbServer.use_https:
                    self.scheme = "couchbases"
                else:
                    self.scheme = "couchbase"

        self.__create_conn()
        SDKClient.sdk_connections += 1

    def __create_conn(self):
        self.log.debug("Creating SDK connection for '%s'" % self.bucket)
        # Having 'None' will enable us to test without sending any
        # compression settings and explicitly setting to 'False' as well
        cluster_env = None
        if self.compression is not None:
            is_compression = self.compression.get("enabled", False)
            compression_config = CompressionConfig.enable(is_compression)
            if "minSize" in self.compression:
                compression_config = compression_config.minSize(
                    self.compression["minSize"])
            if "minRatio" in self.compression:
                compression_config = compression_config.minRatio(
                    self.compression["minRatio"])
            cluster_env = SDKClient.env.compressionConfig(compression_config)
        if CbServer.use_https:
            if cluster_env:
                cluster_env = cluster_env.\
                    securityConfig(SecurityConfig.enableTls(True).
                                   trustManagerFactory(InsecureTrustManagerFactory.INSTANCE))
            else:
                cluster_env = SDKClient.env.\
                    securityConfig(SecurityConfig.enableTls(True).
                                   trustManagerFactory(InsecureTrustManagerFactory.INSTANCE))
        if CbServer.use_https or (self.compression is not None):
            SDKClient.cluster_env = cluster_env.build()
        cluster_options = ClusterOptions \
            .clusterOptions(self.username, self.password) \
            .environment(SDKClient.cluster_env)
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
                    self.cluster = Cluster.connect(master_seed,
                                                   cluster_options)
                else:
                    connection_string = "{0}://{1}".format(self.scheme, ", ".
                                                           join(self.hosts).
                                                           replace(" ", ""))
                    self.cluster = Cluster.connect(
                        connection_string,
                        cluster_options)
                break
            except ConfigException as e:
                self.log.error("Exception during cluster connection: %s"
                               % e)
                i += 1
        if self.bucket is not None:
            self.bucketObj = self.cluster.bucket(self.bucket.name)
            wait_until_ready_options = \
                WaitUntilReadyOptions.waitUntilReadyOptions() \
                .serviceTypes(ServiceType.KV)
            # Temp work around until JCBC-1983 is fixed
            # adding delay here to avoid loading failures for collections
            time.sleep(10)
            # self.bucketObj.waitUntilReady(
            #     SDKOptions.get_duration(300, SDKConstants.TimeUnit.SECONDS),
            #     wait_until_ready_options)
            self.select_collection(self.scope_name,
                                   self.collection_name)

    def get_diagnostics_report(self):
        diagnostics_results = self.cluster.diagnostics()
        return diagnostics_results.toString()

    def get_memory_footprint(self):
        out = subprocess.Popen(
            ['ps', 'v', '-p', str(os.getpid())],
            stdout=subprocess.PIPE).communicate()[0].split(b'\n')
        vsz_index = out[0].split().index(b'RSS')
        mem = float(out[1].split()[vsz_index]) / 1024
        self.log.info("RAM FootPrint: {}".format(str(mem)))
        return mem

    def close(self):
        self.log.debug("Closing SDK for bucket '%s'" % self.bucket)
        if self.cluster:
            self.cluster.disconnect()
#             self.cluster.environment().shutdown()
            self.log.debug("Cluster disconnected and env shutdown")
            SDKClient.sdk_disconnections += 1

    # Translate APIs for document operations
    @staticmethod
    def translate_to_json_object(value, doc_type="json"):
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
        except Exception:
            pass

        return json_obj

    @staticmethod
    def populate_crud_failure_reason(failed_key, error):
        try:
            req_context = error.context().requestContext()
            failed_key['error'] += \
                " | " + str(req_context.lastDispatchedTo())
            failed_key['error'] += \
                " | reason:" \
                + str(req_context.request().cancellationReason())
            failed_key['error'] += \
                " | retryAttempts:" + str(req_context.retryAttempts())
            failed_key['error'] += \
                " | retryReasons:" + str(req_context.retryReasons())
        except Exception:
            pass

    @staticmethod
    def __translate_upsert_multi_results(data):
        success = dict()
        fail = dict()
        if data is None:
            return success, fail
        for result in data:
            key = result['id']
            json_object = result["document"]
            if result['status']:
                success[key] = dict()
                success[key]['value'] = json_object
                success[key]['cas'] = result['cas']
            else:
                fail[key] = dict()
                fail[key]['cas'] = result['cas']
                fail[key]['value'] = json_object
                fail[key]['error'] = str(result['error'].getClass().getName() +
                                         " | " + result['error'].getMessage())
                SDKClient.populate_crud_failure_reason(fail[key],
                                                       result['error'])
        return success, fail

    @staticmethod
    def __translate_delete_multi_results(data):
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
                fail[key]['value'] = dict()
                fail[key]['error'] = str(result['error'].getClass().getName() +
                                         " | " + result['error'].getMessage())
                SDKClient.populate_crud_failure_reason(fail[key],
                                                       result['error'])
        return success, fail

    @staticmethod
    def __translate_get_multi_results(data):
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
                fail[key]['value'] = dict()
                fail[key]['error'] = str(result['error'].getClass().getName() +
                                         " | " + result['error'].getMessage())
                SDKClient.populate_crud_failure_reason(fail[key],
                                                       result['error'])
        return success, fail

    @staticmethod
    def __translate_get_multi_sub_doc_result(data, path_val=None):
        success = dict()
        fail = dict()
        if data is None:
            return success, fail
        for result in data:
            key = result['id']
            if not result['status'] or \
                    (SDKException.LookUpPathNotFoundException in result['content']):
                fail[key] = dict()
                fail[key]['cas'] = result['cas']
                fail[key]['value'] = dict()
                if not result['status']:
                    fail[key]['error'] = str(result['error'].getClass().getName() +
                                             " | " + result['error'].getMessage())
                    SDKClient.populate_crud_failure_reason(fail[key],
                                                           result['error'])
                else:
                    fail[key]['error'] = SDKException.PathNotFoundException
                if path_val:
                    fail[key]['path_val'] = path_val[key]  # list of (path, val)
            elif result['status']:
                success[key] = dict()
                success[key]['value'] = result['content']
                success[key]['cas'] = result['cas']
        return success, fail

    # Translate APIs for sub-document operations
    @staticmethod
    def __translate_upsert_multi_sub_doc_result(data, path_val=None):
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
                fail[key]['error'] = str(item['error'].getClass().getName() +
                                         " | " + item['error'].getMessage())
                SDKClient.populate_crud_failure_reason(fail[key],
                                                       item['error'])
                fail[key]['cas'] = 0
                if path_val:
                    fail[key]['path_val'] = path_val[key]  # list of (path, val)
        return success, fail

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
        self.scope_name = scope_name
        self.collection_name = collection_name
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
        self.bucket.stats.increment_manifest_uid()

    def drop_scope(self, scope):
        """
        Drop a scope using the given name
        :param scope: Scope name to be dropped
        """
        self.collection_manager().dropScope(scope)
        self.bucket.stats.increment_manifest_uid()

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
        self.bucket.stats.increment_manifest_uid()

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
        self.bucket.stats.increment_manifest_uid()

    # Singular CRUD APIs
    def delete(self, key, persist_to=0, replicate_to=0,
               timeout=5, time_unit=SDKConstants.TimeUnit.SECONDS,
               durability="", cas=0, sdk_retry_strategy=None):
        result = dict()
        result["cas"] = -1
        try:
            options = SDKOptions.get_remove_options(
                persist_to=persist_to, replicate_to=replicate_to,
                timeout=timeout, time_unit=time_unit,
                durability=durability,
                cas=cas,
                sdk_retry_strategy=sdk_retry_strategy)
            delete_result = self.collection.remove(key, options)
            result.update({"key": key, "value": None,
                           "error": None, "status": True,
                           "cas": delete_result.cas()})
        except DocumentNotFoundException as e:
            self.log.debug("Exception: Document id {0} not found - {1}"
                           .format(key, e))
            result.update({"key": key, "value": None,
                           "error": str(e), "status": False})
        except CasMismatchException as e:
            self.log.debug("Exception: Cas mismatch for doc {0} - {1}"
                           .format(key, e))
            result.update({"key": key, "value": None,
                           "error": str(e), "status": False})
        except TemporaryFailureException as e:
            self.log.debug("Exception: Retry for doc {0} - {1}"
                           .format(key, e))
            result.update({"key": key, "value": None,
                           "error": str(e), "status": False})
        except CouchbaseException as e:
            self.log.debug("CB generic exception for doc {0} - {1}"
                           .format(key, e))
            result.update({"key": key, "value": None,
                           "error": str(e), "status": False})
        except (RequestCanceledException, TimeoutException) as ex:
            self.log.debug("Request cancelled/timed-out: " + str(ex))
            result.update({"key": key, "value": None,
                           "error": str(ex), "status": False})
        except Exception as e:
            self.log.error("Error during remove of {0} - {1}".format(key, e))
            result.update({"key": key, "value": None,
                           "error": str(e), "status": False})
        return result

    def insert(self, key, value,
               exp=0, exp_unit=SDKConstants.TimeUnit.SECONDS,
               persist_to=0, replicate_to=0,
               timeout=5, time_unit=SDKConstants.TimeUnit.SECONDS,
               doc_type="json",
               durability="", sdk_retry_strategy=None):

        result = dict()
        result["cas"] = 0
        content = self.translate_to_json_object(value)
        try:
            options = SDKOptions.get_insert_options(
                exp=exp, exp_unit=exp_unit,
                persist_to=persist_to, replicate_to=replicate_to,
                timeout=timeout, time_unit=time_unit,
                durability=durability,
                sdk_retry_strategy=sdk_retry_strategy)
            if doc_type == "binary":
                options = options.transcoder(RawBinaryTranscoder.INSTANCE)
            elif doc_type == "string":
                options = options.transcoder(RawStringTranscoder.INSTANCE)

            # Returns com.couchbase.client.java.kv.MutationResult object
            insert_result = self.collection.insert(key, content, options)
            result.update({"key": key, "value": content,
                           "error": None, "status": True,
                           "cas": insert_result.cas()})
        except DocumentExistsException as ex:
            self.log.debug("The document already exists! => " + str(ex))
            result.update({"key": key, "value": content,
                           "error": str(ex), "status": False})
        except DurabilityImpossibleException as ex:
            self.log.debug("Durability impossible for key: " + str(ex))
            result.update({"key": key, "value": content,
                           "error": str(ex), "status": False})
        except ReplicaNotConfiguredException as ex:
            self.log.debug("ReplicaNotConfigured for key: %s" % str(ex))
            result.update({"key": key, "value": content,
                           "error": str(ex), "status": False})
        except (RequestCanceledException, TimeoutException) as ex:
            self.log.debug("Request cancelled/timed-out: " + str(ex))
            result.update({"key": key, "value": content,
                           "error": str(ex), "status": False})
        except DurabilityAmbiguousException as e:
            self.log.debug("D_Ambiguous for key %s" % key)
            result.update({"key": key, "value": None,
                           "error": str(e), "status": False})
        except ServerOutOfMemoryException as ex:
            self.log.debug("OOM exception: %s" % ex)
            result.update({"key": key, "value": content,
                           "error": str(ex), "status": False})
        except FeatureNotAvailableException as ex:
            result.update({"key": key, "value": content,
                           "error": str(ex), "status": False})
        except Exception as ex:
            self.log.error("Something else happened: " + str(ex))
            result.update({"key": key, "value": content,
                           "error": str(ex), "status": False})
            if result["error"]:
                result["error"] = str(ex.getClass().getName() +
                                      " | " + ex.getMessage())
                SDKClient.populate_crud_failure_reason(result, ex)
        return result

    def replace(self, key, value,
                exp=0, exp_unit=SDKConstants.TimeUnit.SECONDS,
                persist_to=0, replicate_to=0,
                timeout=5, time_unit=SDKConstants.TimeUnit.SECONDS,
                durability="", cas=0, sdk_retry_strategy=None,
                preserve_expiry=None):
        result = dict()
        result["cas"] = 0
        content = self.translate_to_json_object(value)
        try:
            options = SDKOptions.get_replace_options(
                persist_to=persist_to, replicate_to=replicate_to,
                timeout=timeout, time_unit=time_unit,
                durability=durability,
                cas=cas,
                preserve_expiry=preserve_expiry,
                sdk_retry_strategy=sdk_retry_strategy)
            # Returns com.couchbase.client.java.kv.MutationResult object
            replace_result = self.collection.replace(key, content, options)
            result.update({"key": key, "value": content,
                           "error": None, "status": True,
                           "cas": replace_result.cas()})
        except DocumentExistsException as ex:
            self.log.debug("The document already exists! => " + str(ex))
            result.update({"key": key, "value": content,
                           "error": str(ex), "status": False})
        except CasMismatchException as e:
            self.log.debug("CAS mismatch for key %s - %s" % (key, e))
            result.update({"key": key, "value": None,
                           "error": str(e), "status": False})
        except DocumentNotFoundException as e:
            result.update({"key": key, "value": None,
                           "error": str(e), "status": False})
        except DurabilityAmbiguousException as e:
            self.log.debug("D_Ambiguous for key %s" % key)
            result.update({"key": key, "value": None,
                           "error": str(e), "status": False})
        except (RequestCanceledException, TimeoutException) as ex:
            self.log.debug("Request cancelled/timed-out: " + str(ex))
            result.update({"key": key, "value": None,
                           "error": str(ex), "status": False})
        except FeatureNotAvailableException as ex:
            result.update({"key": key, "value": content,
                           "error": str(ex), "status": False})
        except Exception as ex:
            self.log.error("Something else happened: " + str(ex))
            result.update({"key": key, "value": content,
                           "error": str(ex), "status": False})
        return result

    def touch(self, key, exp=0, exp_unit=SDKConstants.TimeUnit.SECONDS,
              persist_to=0, replicate_to=0,
              durability="",
              timeout=5, time_unit=SDKConstants.TimeUnit.SECONDS,
              sdk_retry_strategy=None):
        result = {
            "key": key,
            "value": None,
            "cas": 0,
            "status": False,
            "error": None
        }
        touch_options = SDKOptions.get_touch_options(
            timeout, time_unit,
            sdk_retry_strategy=sdk_retry_strategy)
        try:
            touch_result = self.collection.touch(
                key,
                SDKOptions.get_duration(exp, exp_unit),
                touch_options)
            result.update({"status": True, "cas": touch_result.cas()})
        except DocumentNotFoundException as e:
            self.log.debug("Document key '%s' not found!" % key)
            result["error"] = str(e)
        except (RequestCanceledException, TimeoutException) as ex:
            self.log.debug("Request cancelled/timed-out: " + str(ex))
            result.update({"key": key, "value": None,
                           "error": str(ex), "status": False})
        except FeatureNotAvailableException as ex:
            result.update({"key": key, "value": None,
                           "error": str(ex), "status": False})
        except Exception as ex:
            self.log.error("Something else happened: " + str(ex))
            result.update({"key": key, "value": None,
                           "error": str(ex), "status": False})
        return result

    def read(self, key, timeout=5, time_unit=SDKConstants.TimeUnit.SECONDS,
             sdk_retry_strategy=None):
        result = {
            "key": key,
            "value": None,
            "cas": 0,
            "status": False,
            "error": None
        }
        read_options = SDKOptions.get_read_options(
            timeout, time_unit,
            sdk_retry_strategy=sdk_retry_strategy)
        try:
            get_result = self.collection.get(key, read_options)
            self.log.debug("Found document: cas=%s, content=%s"
                           % (str(get_result.cas()),
                              str(get_result.contentAsObject())))
            result["status"] = True
            result["value"] = str(get_result.contentAsObject())
            result["cas"] = get_result.cas()
        except DocumentNotFoundException as e:
            result.update({"key": key, "value": None,
                           "error": str(e), "status": False})
        except (RequestCanceledException, TimeoutException) as ex:
            self.log.debug("Request cancelled/timed-out: " + str(ex))
            result.update({"key": key, "value": None,
                           "error": str(ex), "status": False})
        except CouchbaseException as e:
            result.update({"key": key, "value": None,
                           "error": str(e), "status": False})
        except Exception as ex:
            self.log.error("Something else happened: " + str(ex))
            result.update({"key": key, "value": None,
                           "error": str(ex), "status": False})
        return result

    def get_from_all_replicas(self, key):
        result = []
        get_result = self.collection.getAllReplicas(
            key, GetAllReplicasOptions.getAllReplicasOptions())
        try:
            get_result = get_result.toArray()
            if get_result:
                for item in get_result:
                    result.append({"key": key,
                                   "value": item.contentAsObject(),
                                   "cas": item.cas(), "status": True})
        except Exception:
            pass
        return result

    def upsert(self, key, value,
               exp=0, exp_unit=SDKConstants.TimeUnit.SECONDS,
               persist_to=0, replicate_to=0,
               timeout=5, time_unit=SDKConstants.TimeUnit.SECONDS,
               durability="", sdk_retry_strategy=None, preserve_expiry=None):
        content = self.translate_to_json_object(value)
        result = dict()
        result["cas"] = 0
        try:
            options = SDKOptions.get_upsert_options(
                exp=exp, exp_unit=exp_unit,
                persist_to=persist_to, replicate_to=replicate_to,
                timeout=timeout, time_unit=time_unit,
                durability=durability,
                preserve_expiry=preserve_expiry,
                sdk_retry_strategy=sdk_retry_strategy)
            upsert_result = self.collection.upsert(key, content, options)
            result.update({"key": key, "value": content,
                           "error": None, "status": True,
                           "cas": upsert_result.cas()})
        except DocumentExistsException as ex:
            self.log.debug("Upsert: Document already exists! => " + str(ex))
            result.update({"key": key, "value": content,
                           "error": str(ex), "status": False})
        except ReplicaNotConfiguredException as ex:
            self.log.debug("Upsert: ReplicaNotConfiguredException for %s: %s"
                           % (key, str(ex)))
            result.update({"key": key, "value": content,
                           "error": str(ex), "status": False})
        except (RequestCanceledException, TimeoutException) as ex:
            self.log.debug("Request cancelled/timed-out: " + str(ex))
            result.update({"key": key, "value": None,
                           "error": str(ex), "status": False})
        except DurabilityAmbiguousException as ex:
            self.log.debug("Durability Ambiguous for key: %s" % key)
            result.update({"key": key, "value": content,
                           "error": str(ex), "status": False})
        except FeatureNotAvailableException as ex:
            result.update({"key": key, "value": content,
                           "error": str(ex), "status": False})
        except CouchbaseException as ex:
            result.update({"key": key, "value": content,
                           "error": str(ex), "status": False})
        except Exception as ex:
            self.log.error("Something else happened: " + str(ex))
            result.update({"key": key, "value": None,
                           "error": str(ex), "status": False})
        return result

    def crud(self, op_type, key, value=None, exp=0, replicate_to=0,
             persist_to=0, durability="",
             timeout=5, time_unit=SDKConstants.TimeUnit.SECONDS,
             create_path=True, xattr=False, cas=0, sdk_retry_strategy=None,
             store_semantics=None, preserve_expiry=None, access_deleted=False,
             create_as_deleted=False):
        result = None
        if op_type == DocLoading.Bucket.DocOps.UPDATE:
            result = self.upsert(
                key, value, exp=exp,
                persist_to=persist_to, replicate_to=replicate_to,
                durability=durability,
                timeout=timeout, time_unit=time_unit,
                sdk_retry_strategy=sdk_retry_strategy,
                preserve_expiry=preserve_expiry)
        elif op_type == DocLoading.Bucket.DocOps.CREATE:
            result = self.insert(
                key, value, exp=exp,
                persist_to=persist_to, replicate_to=replicate_to,
                durability=durability,
                timeout=timeout, time_unit=time_unit,
                sdk_retry_strategy=sdk_retry_strategy)
        elif op_type == DocLoading.Bucket.DocOps.DELETE:
            result = self.delete(
                key,
                persist_to=persist_to, replicate_to=replicate_to,
                durability=durability,
                timeout=timeout, time_unit=time_unit,
                sdk_retry_strategy=sdk_retry_strategy)
        elif op_type == DocLoading.Bucket.DocOps.REPLACE:
            result = self.replace(
                key, value, exp=exp,
                persist_to=persist_to, replicate_to=replicate_to,
                durability=durability,
                timeout=timeout, time_unit=time_unit,
                cas=cas,
                sdk_retry_strategy=sdk_retry_strategy,
                preserve_expiry=preserve_expiry)
        elif op_type == DocLoading.Bucket.DocOps.TOUCH:
            result = self.touch(
                key, exp=exp,
                persist_to=persist_to, replicate_to=replicate_to,
                durability=durability,
                timeout=timeout, time_unit=time_unit,
                sdk_retry_strategy=sdk_retry_strategy)
        elif op_type == DocLoading.Bucket.DocOps.READ:
            result = self.read(
                key, timeout=timeout, time_unit=time_unit,
                sdk_retry_strategy=sdk_retry_strategy)
        elif op_type in [DocLoading.Bucket.SubDocOps.INSERT, "subdoc_insert"]:
            sub_key, value = value[0], value[1]
            path_val = dict()
            path_val[key] = [(sub_key, value)]
            mutate_in_specs = list()
            mutate_in_specs.append(SDKClient.sub_doc_op.getInsertMutateInSpec(
                sub_key, value, create_path, xattr))
            if not xattr:
                mutate_in_specs.append(
                    SDKClient.sub_doc_op.getIncrMutateInSpec("mutated", 1,
                                                             create_path))
            content = Tuples.of(key, mutate_in_specs)
            options = SDKOptions.get_mutate_in_options(
                exp, time_unit, persist_to, replicate_to,
                timeout, time_unit, durability,
                store_semantics=store_semantics,
                preserve_expiry=preserve_expiry,
                sdk_retry_strategy=sdk_retry_strategy,
                access_deleted=access_deleted,
                create_as_deleted=create_as_deleted)
            if cas > 0:
                options = options.cas(cas)

            result = SDKClient.sub_doc_op.bulkSubDocOperation(
                self.collection, [content], options)
            return self.__translate_upsert_multi_sub_doc_result(result, path_val)
        elif op_type in [DocLoading.Bucket.SubDocOps.UPSERT, "subdoc_upsert"]:
            sub_key, value = value[0], value[1]
            path_val = dict()
            path_val[key] = [(sub_key, value)]
            mutate_in_specs = list()
            mutate_in_specs.append(SDKClient.sub_doc_op.getUpsertMutateInSpec(
                sub_key, value, create_path, xattr))
            if not xattr:
                mutate_in_specs.append(
                    SDKClient.sub_doc_op.getIncrMutateInSpec("mutated", 1,
                                                             create_path))
            content = Tuples.of(key, mutate_in_specs)
            options = SDKOptions.get_mutate_in_options(
                exp, time_unit, persist_to, replicate_to,
                timeout, time_unit, durability,
                store_semantics=store_semantics,
                preserve_expiry=preserve_expiry,
                sdk_retry_strategy=sdk_retry_strategy,
                create_as_deleted=create_as_deleted)
            if cas > 0:
                options = options.cas(cas)
            result = SDKClient.sub_doc_op.bulkSubDocOperation(
                self.collection, [content], options)
            return self.__translate_upsert_multi_sub_doc_result(result, path_val)
        elif op_type in [DocLoading.Bucket.SubDocOps.REMOVE, "subdoc_delete"]:
            mutate_in_specs = list()
            path_val = dict()
            path_val[key] = [(value, '')]
            mutate_in_specs.append(SDKClient.sub_doc_op.getRemoveMutateInSpec(
                value, xattr))
            if not xattr:
                mutate_in_specs.append(
                    SDKClient.sub_doc_op.getIncrMutateInSpec("mutated", 1,
                                                             False))
            content = Tuples.of(key, mutate_in_specs)
            options = SDKOptions.get_mutate_in_options(
                exp, time_unit, persist_to, replicate_to,
                timeout, time_unit, durability,
                store_semantics=store_semantics,
                preserve_expiry=preserve_expiry,
                sdk_retry_strategy=sdk_retry_strategy,
                create_as_deleted=create_as_deleted)
            if cas > 0:
                options = options.cas(cas)
            result = SDKClient.sub_doc_op.bulkSubDocOperation(
                self.collection, [content], options)
            result = self.__translate_upsert_multi_sub_doc_result(result, path_val)
        elif op_type == "subdoc_replace":
            sub_key, value = value[0], value[1]
            path_val = dict()
            path_val[key] = [(sub_key, value)]
            mutate_in_specs = list()
            mutate_in_specs.append(SDKClient.sub_doc_op.getReplaceMutateInSpec(
                sub_key, value, xattr))
            if not xattr:
                mutate_in_specs.append(
                    SDKClient.sub_doc_op.getIncrMutateInSpec("mutated", 1,
                                                             False))
            content = Tuples.of(key, mutate_in_specs)
            options = SDKOptions.get_mutate_in_options(
                exp, time_unit, persist_to, replicate_to,
                timeout, time_unit, durability,
                store_semantics=store_semantics,
                preserve_expiry=preserve_expiry,
                sdk_retry_strategy=sdk_retry_strategy,
                create_as_deleted=create_as_deleted)
            if cas > 0:
                options = options.cas(cas)
            result = SDKClient.sub_doc_op.bulkSubDocOperation(
                self.collection, [content], options)
            result = self.__translate_upsert_multi_sub_doc_result(result, path_val)
        elif op_type in [DocLoading.Bucket.SubDocOps.LOOKUP, "subdoc_read"]:
            mutate_in_specs = list()
            path_val = dict()
            path_val[key] = [(value, '')]
            mutate_in_specs.append(
                SDKClient.sub_doc_op.getLookUpInSpec(value, xattr))
            content = Tuples.of(key, mutate_in_specs)
            lookup_in_options = SDKClient.sub_doc_op.getLookupInOptions(access_deleted)
            result = SDKClient.sub_doc_op.bulkGetSubDocOperation(
                self.collection, [content], lookup_in_options)
            result = self.__translate_get_multi_sub_doc_result(result, path_val)
        elif op_type == DocLoading.Bucket.SubDocOps.COUNTER:
            sub_key, step_value = value[0], value[1]
            mutate_in_specs = list()
            if not xattr:
                mutate_in_specs.append(
                    SDKClient.sub_doc_op.getIncrMutateInSpec(sub_key,
                                                             step_value,
                                                             create_path))
            content = Tuples.of(key, mutate_in_specs)
            options = SDKOptions.get_mutate_in_options(
                exp, time_unit, persist_to, replicate_to,
                timeout, time_unit, durability,
                store_semantics=store_semantics,
                preserve_expiry=preserve_expiry,
                sdk_retry_strategy=sdk_retry_strategy)
            if cas > 0:
                options = options.cas(cas)
            result = SDKClient.sub_doc_op.bulkSubDocOperation(
                self.collection, [content], options)
            result = self.__translate_upsert_multi_sub_doc_result(result)
        else:
            self.log.error("Unsupported operation %s" % op_type)
        return result

    # Bulk CRUD APIs
    def delete_multi(self, keys, persist_to=0, replicate_to=0,
                     timeout=5, time_unit=SDKConstants.TimeUnit.SECONDS,
                     durability="", sdk_retry_strategy=None):
        options = SDKOptions.get_remove_options(
            persist_to=persist_to, replicate_to=replicate_to,
            timeout=timeout, time_unit=time_unit,
            durability=durability,
            sdk_retry_strategy=sdk_retry_strategy)
        result = SDKClient.doc_op.bulkDelete(
            self.collection, keys, options)
        return self.__translate_delete_multi_results(result)

    def touch_multi(self, keys, exp=0,
                    timeout=5, time_unit=SDKConstants.TimeUnit.SECONDS,
                    sdk_retry_strategy=None):
        touch_options = SDKOptions.get_touch_options(
            timeout, time_unit, sdk_retry_strategy=sdk_retry_strategy)
        exp_duration = \
            SDKOptions.get_duration(exp, SDKConstants.TimeUnit.SECONDS)
        result = SDKClient.doc_op.bulkTouch(
            self.collection, keys, exp,
            touch_options, exp_duration)
        return self.__translate_delete_multi_results(result)

    def set_multi(self, items, exp=0, exp_unit=SDKConstants.TimeUnit.SECONDS,
                  persist_to=0, replicate_to=0,
                  timeout=5, time_unit=SDKConstants.TimeUnit.SECONDS,
                  doc_type="json", durability="", sdk_retry_strategy=None):
        options = SDKOptions.get_insert_options(
            exp=exp, exp_unit=exp_unit,
            persist_to=persist_to, replicate_to=replicate_to,
            timeout=timeout, time_unit=time_unit,
            durability=durability,
            sdk_retry_strategy=sdk_retry_strategy)
        if doc_type.lower() == "binary":
            options = options.transcoder(RawBinaryTranscoder.INSTANCE)
        elif doc_type.lower() == "string":
            options = options.transcoder(RawStringTranscoder.INSTANCE)
        result = SDKClient.doc_op.bulkInsert(
            self.collection, items, options)
        return self.__translate_upsert_multi_results(result)

    def upsert_multi(self, docs,
                     exp=0, exp_unit=SDKConstants.TimeUnit.SECONDS,
                     persist_to=0, replicate_to=0,
                     timeout=5, time_unit=SDKConstants.TimeUnit.SECONDS,
                     doc_type="json", durability="",
                     preserve_expiry=None, sdk_retry_strategy=None):
        options = SDKOptions.get_upsert_options(
            exp=exp, exp_unit=exp_unit,
            persist_to=persist_to, replicate_to=replicate_to,
            timeout=timeout, time_unit=time_unit,
            durability=durability,
            preserve_expiry=preserve_expiry,
            sdk_retry_strategy=sdk_retry_strategy)
        if doc_type.lower() == "binary":
            options = options.transcoder(RawBinaryTranscoder.INSTANCE)
        elif doc_type.lower() == "string":
            options = options.transcoder(RawStringTranscoder.INSTANCE)
        result = SDKClient.doc_op.bulkUpsert(
            self.collection, docs, options)
        return self.__translate_upsert_multi_results(result)

    def replace_multi(self, docs,
                      exp=0, exp_unit=SDKConstants.TimeUnit.SECONDS,
                      persist_to=0, replicate_to=0,
                      timeout=5, time_unit=SDKConstants.TimeUnit.SECONDS,
                      doc_type="json", durability="",
                      preserve_expiry=None, sdk_retry_strategy=None):
        options = SDKOptions.get_replace_options(
            exp=exp, exp_unit=exp_unit,
            persist_to=persist_to, replicate_to=replicate_to,
            timeout=timeout, time_unit=time_unit,
            durability=durability,
            preserve_expiry=preserve_expiry,
            sdk_retry_strategy=sdk_retry_strategy)
        if doc_type.lower() == "binary":
            options = options.transcoder(RawBinaryTranscoder.INSTANCE)
        elif doc_type.lower() == "string":
            options = options.transcoder(RawStringTranscoder.INSTANCE)
        result = SDKClient.doc_op.bulkReplace(
            self.collection, docs, options)
        return self.__translate_upsert_multi_results(result)

    def get_multi(self, keys,
                  timeout=5, time_unit=SDKConstants.TimeUnit.SECONDS,
                  sdk_retry_strategy=None):
        read_options = SDKOptions.get_read_options(
            timeout, time_unit,
            sdk_retry_strategy=sdk_retry_strategy)
        result = SDKClient.doc_op.bulkGet(self.collection, keys, read_options)
        return self.__translate_get_multi_results(result)

    # Bulk CRUDs for sub-doc APIs
    def sub_doc_insert_multi(self, keys,
                             exp=0, exp_unit=SDKConstants.TimeUnit.SECONDS,
                             persist_to=0, replicate_to=0,
                             timeout=5,
                             time_unit=SDKConstants.TimeUnit.SECONDS,
                             durability="",
                             create_path=False,
                             xattr=False,
                             cas=0,
                             store_semantics=None,
                             preserve_expiry=None,
                             sdk_retry_strategy=None,
                             access_deleted=False,
                             create_as_deleted=False):
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
        :param store_semantics: Extra action to take during mutate_in op
        :param preserve_expiry: Boolean to preserver ttl of the doc or not
        :param sdk_retry_strategy: Sets sdk_retry_strategy for doc ops
        :param access_deleted: Allows editing documents in Tombstones form
        :param create_as_deleted: Allows creating documents in Tombstone form
        :return:
        """
        mutate_in_specs = []
        path_val = dict()
        for item in keys:
            key = item.getT1()
            value = item.getT2()
            mutate_in_spec = []
            path_val[key] = value
            for _tuple in value:
                _path = _tuple[0]
                _val = _tuple[1]
                _mutate_in_spec = SDKClient.sub_doc_op.getInsertMutateInSpec(
                    _path, _val, create_path, xattr)
                mutate_in_spec.append(_mutate_in_spec)
            if not xattr:
                _mutate_in_spec = SDKClient.sub_doc_op.getIncrMutateInSpec(
                    "mutated", 1, True)
                mutate_in_spec.append(_mutate_in_spec)
            content = Tuples.of(key, mutate_in_spec)
            mutate_in_specs.append(content)
        options = SDKOptions.get_mutate_in_options(
            exp, exp_unit, persist_to, replicate_to,
            timeout, time_unit,
            durability,
            store_semantics=store_semantics,
            preserve_expiry=preserve_expiry,
            sdk_retry_strategy=sdk_retry_strategy,
            access_deleted=access_deleted,
            create_as_deleted=create_as_deleted)
        if cas > 0:
            options = options.cas(cas)
        result = SDKClient.sub_doc_op.bulkSubDocOperation(
            self.collection, mutate_in_specs, options)
        return self.__translate_upsert_multi_sub_doc_result(result, path_val)

    def sub_doc_upsert_multi(self, keys, exp=0,
                             exp_unit=SDKConstants.TimeUnit.SECONDS,
                             persist_to=0, replicate_to=0,
                             timeout=5,
                             time_unit=SDKConstants.TimeUnit.SECONDS,
                             durability="",
                             create_path=False,
                             xattr=False,
                             cas=0,
                             store_semantics=None,
                             preserve_expiry=None,
                             sdk_retry_strategy=None,
                             access_deleted=False,
                             create_as_deleted=False):
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
        :param store_semantics: Extra action to take during mutate_in op
        :param preserve_expiry: Boolean to preserver ttl of the doc or not
        :param sdk_retry_strategy: Sets sdk_retry_strategy for doc ops
        :return:
        """
        mutate_in_specs = []
        path_val = dict()
        for kv in keys:
            key = kv.getT1()
            value = kv.getT2()
            mutate_in_spec = []
            path_val[key] = value
            for _tuple in value:
                _path = _tuple[0]
                _val = _tuple[1]
                _mutate_in_spec = SDKClient.sub_doc_op.getUpsertMutateInSpec(
                    _path, _val, create_path, xattr)
                mutate_in_spec.append(_mutate_in_spec)
            if not xattr:
                _mutate_in_spec = SDKClient.sub_doc_op.getIncrMutateInSpec(
                    "mutated", 1, True)
                mutate_in_spec.append(_mutate_in_spec)
            content = Tuples.of(key, mutate_in_spec)
            mutate_in_specs.append(content)
        options = SDKOptions.get_mutate_in_options(
            exp, exp_unit, persist_to, replicate_to,
            timeout, time_unit,
            durability,
            store_semantics=store_semantics,
            preserve_expiry=preserve_expiry,
            sdk_retry_strategy=sdk_retry_strategy,
            access_deleted=access_deleted,
            create_as_deleted=create_as_deleted)
        if cas > 0:
            options = options.cas(cas)
        result = SDKClient.sub_doc_op.bulkSubDocOperation(
            self.collection, mutate_in_specs, options)
        return self.__translate_upsert_multi_sub_doc_result(result, path_val)

    def sub_doc_read_multi(self, keys, timeout=5,
                           time_unit=SDKConstants.TimeUnit.SECONDS,
                           xattr=False, access_deleted=False):
        """
        :param keys: List of tuples (key,value)
        :param timeout: timeout for the operation
        :param time_unit: timeout time unit
        :param xattr: Bool to enable xattr read
        :return:
        """
        mutate_in_specs = []
        path_val = dict()
        for kv in keys:
            key = kv.getT1()
            value = kv.getT2()
            path_val[key] = list()
            mutate_in_spec = []
            for _tuple in value:
                _path = _tuple[0]
                path_val[key].append((_path, ''))
                _mutate_in_spec = SDKClient.sub_doc_op.getLookUpInSpec(
                    _path,
                    xattr)
                mutate_in_spec.append(_mutate_in_spec)
            content = Tuples.of(key, mutate_in_spec)
            mutate_in_specs.append(content)
        lookup_in_options = SDKClient.sub_doc_op.getLookupInOptions(access_deleted)
        result = SDKClient.sub_doc_op.bulkGetSubDocOperation(
            # timeout, time_unit,
            self.collection, mutate_in_specs, lookup_in_options)
        return self.__translate_get_multi_sub_doc_result(result, path_val)

    def sub_doc_remove_multi(self, keys, exp=0,
                             exp_unit=SDKConstants.TimeUnit.SECONDS,
                             persist_to=0, replicate_to=0,
                             timeout=5,
                             time_unit=SDKConstants.TimeUnit.SECONDS,
                             durability="",
                             xattr=False,
                             cas=0,
                             store_semantics=None,
                             preserve_expiry=None,
                             sdk_retry_strategy=None,
                             access_deleted=False,
                             create_as_deleted=False):
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
        :param store_semantics: Value to be used in mutate_in_option
        :param preserve_expiry: Boolean to preserver ttl of the doc or not
        :param sdk_retry_strategy: Sets sdk_retry_strategy for doc ops
        :return:
        """
        mutate_in_specs = []
        path_val = dict()
        for kv in keys:
            mutate_in_spec = []
            key = kv.getT1()
            value = kv.getT2()
            path_val[key] = list()
            for _tuple in value:
                _path = _tuple[0]
                _val = _tuple[1]
                path_val[key].append((_path, ''))
                _mutate_in_spec = SDKClient.sub_doc_op.getRemoveMutateInSpec(
                    _path, xattr)
                mutate_in_spec.append(_mutate_in_spec)
            if not xattr:
                _mutate_in_spec = SDKClient.sub_doc_op.getIncrMutateInSpec(
                    "mutated", 1, False)
                mutate_in_spec.append(_mutate_in_spec)
            content = Tuples.of(key, mutate_in_spec)
            mutate_in_specs.append(content)
        options = SDKOptions.get_mutate_in_options(
            exp, exp_unit, persist_to, replicate_to,
            timeout, time_unit,
            durability,
            store_semantics=store_semantics,
            preserve_expiry=preserve_expiry,
            sdk_retry_strategy=sdk_retry_strategy,
            access_deleted=access_deleted,
            create_as_deleted=create_as_deleted)
        if cas > 0:
            options = options.cas(cas)
        result = SDKClient.sub_doc_op.bulkSubDocOperation(
            self.collection, mutate_in_specs, options)
        return self.__translate_upsert_multi_sub_doc_result(result, path_val)

    def sub_doc_replace_multi(self, keys, exp=0,
                              exp_unit=SDKConstants.TimeUnit.SECONDS,
                              persist_to=0, replicate_to=0,
                              timeout=5,
                              time_unit=SDKConstants.TimeUnit.SECONDS,
                              durability="",
                              xattr=False,
                              cas=0,
                              store_semantics=None,
                              preserve_expiry=None, sdk_retry_strategy=None,
                              access_deleted=False,
                              create_as_deleted=False):
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
        :param store_semantics: Extra action to take during mutate_in op
        :param preserve_expiry: Boolean to preserver ttl of the doc or not
        :param sdk_retry_strategy: Sets sdk_retry_strategy for doc ops
        :return:
        """
        mutate_in_specs = []
        path_val = dict()
        for kv in keys:
            mutate_in_spec = []
            key = kv.getT1()
            value = kv.getT2()
            path_val[key] = value
            for _tuple in value:
                _path = _tuple[0]
                _val = _tuple[1]
                _mutate_in_spec = SDKClient.sub_doc_op.getReplaceMutateInSpec(
                    _path, _val, xattr)
                mutate_in_spec.append(_mutate_in_spec)
            if not xattr:
                _mutate_in_spec = SDKClient.sub_doc_op.getIncrMutateInSpec(
                    "mutated", 1, False)
                mutate_in_spec.append(_mutate_in_spec)
            content = Tuples.of(key, mutate_in_spec)
            mutate_in_specs.append(content)
        options = SDKOptions.get_mutate_in_options(
            exp, exp_unit, persist_to, replicate_to,
            timeout, time_unit,
            durability,
            store_semantics=store_semantics,
            preserve_expiry=preserve_expiry,
            sdk_retry_strategy=sdk_retry_strategy,
            access_deleted=access_deleted,
            create_as_deleted=create_as_deleted)
        if cas > 0:
            options = options.cas(cas)
        result = SDKClient.sub_doc_op.bulkSubDocOperation(
            self.collection, mutate_in_specs, options)
        return self.__translate_upsert_multi_sub_doc_result(result, path_val)

    def insert_binary_document(self, keys, sdk_retry_strategy=None):
        options = \
            SDKOptions.get_insert_options(
                sdk_retry_strategy=sdk_retry_strategy)\
            .transcoder(RawBinaryTranscoder.INSTANCE)
        for key in keys:
            binary_value = Unpooled.copiedBuffer('{value":"' + key + '"}',
                                                 CharsetUtil.UTF_8)
            self.collection.upsert(key, binary_value, options)

    def insert_string_document(self, keys, sdk_retry_strategy=None):
        options = \
            SDKOptions.get_insert_options(
                sdk_retry_strategy=sdk_retry_strategy)\
            .transcoder(RawStringTranscoder.INSTANCE)
        for key in keys:
            self.collection.upsert(key, '{value":"' + key + '"}', options)

    def insert_custom_json_documents(self, key_prefix, documents):
        for index, data in enumerate(documents):
            self.collection.insert(key_prefix+str(index),
                                   JsonObject.create().put("content", data))

    def insert_xattr_attribute(self, document_id, path, value, xattr=True,
                               create_parents=True):
        self.crud("subdoc_insert",
                  document_id,
                  [path, value],
                  time_unit=SDKConstants.TimeUnit.SECONDS,
                  create_path=create_parents,
                  xattr=xattr)

    def update_xattr_attribute(self, document_id, path, value, xattr=True,
                               create_parents=True):
        self.crud("subdoc_upsert",
                  document_id,
                  [path, value],
                  time_unit=SDKConstants.TimeUnit.SECONDS,
                  create_path=create_parents,
                  xattr=xattr)

    def insert_json_documents(self, key_prefix, documents):
        for index, data in enumerate(documents):
            self.collection.insert(key_prefix+str(index),
                                   JsonObject.fromJson(data))
