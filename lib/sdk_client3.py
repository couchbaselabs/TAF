#!/usr/bin/env python
"""
Java based SDK client interface
Created on Mar 14, 2019

"""

import subprocess
import os
import time
from datetime import timedelta

from threading import Lock

from couchbase.diagnostics import ServiceType
from couchbase.exceptions import UnAmbiguousTimeoutException, \
    DocumentExistsException, DurabilityImpossibleException, \
    RequestCanceledException, TimeoutException, \
    DurabilitySyncWriteAmbiguousException, CouchbaseException, \
    CasMismatchException, DocumentNotFoundException
from couchbase.logic.options import Compression

from cb_constants import CbServer, DocLoading
from cb_constants.ClusterRun import ClusterRun
from constants.sdk_constants.java_client import SDKConstants
from global_vars import logger
from sdk_utils.sdk_options import SDKOptions
from sdk_exceptions import SDKException

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import (ClusterOptions, ClusterTimeoutOptions,
                               QueryOptions, WaitUntilReadyOptions,
                               GetAllReplicasOptions)
import couchbase.subdocument as SubDoc


class SDKClientPool(object):
    """
    Client pool manager for list of SDKClients per bucket which can be
    reused / shared across multiple tasks
    """
    def __init__(self):
        self.log = logger.get("infra")
        self.clients = dict()

    def create_cluster_clients(
            self, cluster, servers, req_clients=1, username="Administrator",
            password="password", compression_settings=None):
        """
        Create set of clients for the specified cluster.
        All created clients will be saved under the respective cluster key.
        :param cluster: cluster object for which the clients will be created
        :param servers: List of servers for SDK to establish initial
                        connections with
        :param req_clients: Required number of clients to be created for the
                            given bucket and client settings
        :param username: User name using which to establish the connection
        :param password: Password for username authentication
        :param compression_settings: Same as expected by SDKClient class
        :return:
        """
        if cluster.name not in self.clients:
            self.clients[cluster.name] = dict()
            self.clients[cluster.name]["lock"] = Lock()
            self.clients[cluster.name]["idle_clients"] = list()
            self.clients[cluster.name]["busy_clients"] = list()

        for _ in range(req_clients):
            self.clients[cluster.name]["idle_clients"].append(SDKClient(
                cluster, None, servers,
                username=username, password=password,
                compression_settings=compression_settings))

    def get_cluster_client(self, cluster):
        """
        Method to get a cluster client which can be used for SDK operations
        further by a callee.
        :param cluster: Cluster object for which the client has to selected
        :return client: Instance of SDKClient object
        """
        client = None
        if not self.clients:
            return client
        while client is None:
            self.clients[cluster.name]["lock"].acquire()
            if len(self.clients[cluster.name]["idle_clients"]) > 0:
                client = self.clients[cluster.name]["idle_clients"].pop()
                self.clients[cluster.name]["busy_clients"].append(client)
            self.clients[cluster.name]["lock"].release()
        return client

    def release_cluster_client(self, cluster, client):
        """
        Release the acquired SDKClient object back into the pool
        :param cluster: Cluster object for which the client has to released.
        :param client: Instance of SDKClient object
        :return None:
        """
        if cluster.name not in self.clients:
            return
        self.clients[cluster.name]["lock"].acquire()
        self.clients[cluster.name]["busy_clients"].remove(client)
        self.clients[cluster.name]["idle_clients"].append(client)
        self.clients[cluster.name]["lock"].release()

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

    def create_clients(self, cluster, bucket, servers=None,
                       req_clients=1,
                       username="Administrator", password="password",
                       compression_settings=None):
        """
        Create set of clients for the specified bucket and client settings.
        All created clients will be saved under the respective bucket key.

        :param cluster: Cluster object holding sdk_env variable
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
                cluster, bucket, servers=servers,
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


class TransactionConfig(object):
    def __init__(self, durability=None, timeout=None,
                 cleanup_window=None, transaction_keyspace=None):
        """
        None means leave it to default value during config creation
        """
        self.durability = durability
        self.timeout = timeout
        self.cleanup_window = cleanup_window
        self.transaction_keyspace = transaction_keyspace


class SDKClient(object):
    sdk_connections = 0
    sdk_disconnections = 0
    """
    Java SDK Client Implementation for testrunner - master branch
    """

    @staticmethod
    def get_transaction_options(transaction_config_obj):
        return None
        transaction_options = TransactionOptions.transactionOptions()
        if transaction_config_obj.timeout is not None:
            transaction_options.timeout(Duration.ofSeconds(
                transaction_config_obj.timeout))
        if transaction_config_obj.durability is not None:
            transaction_options = transaction_options.durabilityLevel(
                KVDurabilityLevel.decodeFromManagementApi(
                    transaction_config_obj.durability))
        # transaction_options.metadataCollection(tnx_keyspace)
        return transaction_options

    def __init__(self, framework_cb_cluster_obj, bucket, servers=None,
                 scope=CbServer.default_scope,
                 collection=CbServer.default_collection,
                 username=None, password=None,
                 compression_settings=None, cert_path=None,
                 transaction_config=None):
        """
        :param framework_cb_cluster_obj: Cluster object holding sdk_env var
        :param servers: List of servers for SDK to establish initial
                        connections with. If None, will use cluster.master
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
        # Following params will be passed to create_conn() and
        # no need of them post connection. So having these as local variables
        # Used during Cluster.connect() call
        hosts = list()
        # Cluster_run: Used for cluster connection
        server_list = list()

        if not servers:
            servers = [framework_cb_cluster_obj.master]

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
        self.transaction_conf = transaction_config
        if self.bucket is not None:
            if bucket.serverless is not None \
                    and bucket.serverless.nebula_endpoint:
                hosts = [bucket.serverless.nebula_endpoint.srv]
                self.log.info("For SDK, Nebula endpoint used for bucket is: %s"
                              % bucket.serverless.nebula_endpoint.ip)

        for server in servers:
            server_list.append((server.ip, int(server.port)))
            if CbServer.use_https:
                self.scheme = "couchbases"
            else:
                self.scheme = "couchbase"
            if not ClusterRun.is_enabled:
                if server.type == "columnar":
                    hosts.append(framework_cb_cluster_obj.srv)
                else:
                    hosts.append(server.ip)

        start_time = time.time()
        self.__create_conn(framework_cb_cluster_obj, servers, hosts)
        if bucket is not None:
            self.log.debug("SDK connection to bucket: {} took {}s".format(
                bucket.name, time.time()-start_time))
        SDKClient.sdk_connections += 1

    def __create_conn(self, cluster, servers, hosts):
        if self.bucket:
            self.log.debug("Creating SDK connection for '%s'" % self.bucket)
        # Having 'None' will enable us to test without sending any
        # compression settings and explicitly setting to 'False' as well
        auth = PasswordAuthenticator(self.username, self.password)
        timeout_opts = ClusterTimeoutOptions(
            kv_timeout=timedelta(seconds=10),
            dns_srv_timeout=timedelta(seconds=10))
        cluster_opts = {
            "authenticator": auth,
            "enable_tls": False,
            "timeout_options": timeout_opts,
            # "tls_verify": TLSVerifyMode.NO_VERIFY
        }

        if self.compression is not None:
            cluster_opts["compression"] = (
                Compression(self.compression.get("enabled", "NONE")))
            cluster_opts["compression_min_size"] = self.compression["minSize"]
            cluster_opts["compression_min_ratio"] = self.compression["minRatio"]

        if CbServer.use_https:
            cluster_opts["enable_tls"] = True

        if self.transaction_conf:
            cluster_opts["transaction_config"] = self.transaction_conf

        cluster_opts = ClusterOptions(**cluster_opts)
        i = 1
        while i <= 5:
            try:
                # Code for cluster_run
                if ClusterRun.is_enabled:
                    self.cluster = Cluster.connect(f"{self.scheme, hosts[0]}",
                                                   cluster_opts)
                else:
                    connection_string = "{0}://{1}".format(self.scheme, ", ".
                                                           join(hosts).
                                                           replace(" ", ""))
                    if self.scheme == "couchbases":
                        connection_string += "?ssl=no_verify"

                    self.cluster = Cluster.connect(connection_string,
                                                   cluster_opts)
                break
            except Exception as e:
                self.log.error("Exception during cluster connection: %s" % e)
                i += 1

        # Wait until cluster status ready
        wait_until_ready_options = \
            WaitUntilReadyOptions(service_types=[ServiceType.KeyValue])
        try:
            self.cluster.wait_until_ready(
                SDKOptions.get_duration(30, SDKConstants.TimeUnit.SECONDS),
                wait_until_ready_options)
        except UnAmbiguousTimeoutException as e:
            self.log.critical(e)

        # Select bucket / collections if required
        if self.bucket is not None:
            self.bucketObj = self.cluster.bucket(self.bucket.name)
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

    def sampling_scan(self, limit, seed, timeout=None):
        options = None
        if timeout is not None:
            options = SDKOptions.get_scan_options(timeout=60)
        data = SDKClient.doc_op.sampling_scan(self.collection, limit, seed,
                                              options)
        result = self.__translate_ranges_scan_results(data)
        self.log.debug("sampling scan result for term %s" % (str(result)))
        return result, self.bucket.name, self.collection_name, self.scope_name

    def prefix_range_scan(self, term, timeout=None):
        self.log.debug("prefix scan started!")
        options = None
        if timeout is not None:
            options = SDKOptions.get_scan_options(timeout=timeout)
        data = SDKClient.doc_op.prefix_scan_query(self.collection, term,
                                                  options)
        result = self.__translate_ranges_scan_results(data)
        self.log.debug("prefix scan result for term %s %s" % (str(result),
                                                              term))
        return result, self.bucket.name, self.collection_name, self.scope_name

    def range_scan(self, start_term, end_term, include_start=True,
                   include_end=True, timeout=None):
        options = None
        if timeout is not None:
            options = SDKOptions.get_scan_options(timeout=timeout)
        data = SDKClient.doc_op.range_scan_query(self.collection,
                                                 start_term, end_term,
                                                 include_start,
                                                 include_end, options)
        result = self.__translate_ranges_scan_results(data)
        return result, self.bucket.name, self.collection_name, self.scope_name

    def close(self):
        self.log.debug("Closing SDK for bucket '%s'" % self.bucket)
        if self.cluster:
            self.cluster.close()
            SDKClient.sdk_disconnections += 1

    # Translate APIs for document operations
    @staticmethod
    def __translate_ranges_scan_results(data):
        result = dict()
        result['time_taken'] = data['timeTaken']
        result['count'] = int(data['count'])
        result['exception'] = data['exception']
        result['status'] = data['status']
        return result

    @staticmethod
    def __translate_upsert_multi_results(data):
        success = dict()
        fail = dict()
        for key, mutation_result in data.results.items():
            success[key] = dict()
            success[key]["cas"] = mutation_result.cas
        for key, result_exception in data.exceptions.items():
            fail[key] = dict()
            fail[key]["cas"] = 0
            fail[key]['error'] = result_exception
        return success, fail

    @staticmethod
    def __translate_delete_multi_results(data):
        success = dict()
        fail = dict()
        for key, mutation_result in data.results.items():
            success[key] = dict()
            success[key]["cas"] = mutation_result.cas
        for key, result_exception in data.exceptions.items():
            fail[key] = dict()
            fail[key]["cas"] = 0
            fail[key]['error'] = result_exception
        return success, fail

    @staticmethod
    def __translate_get_multi_results(data):
        success = dict()
        fail = dict()
        for key, result in data.results.items():
            success[key] = dict()
            success[key]["cas"] = result.cas
            success[key]["value"] = result.content_as[dict]

        for key, result in data.exceptions.items():
            fail[key] = dict()
            fail[key]["cas"] = 0
            fail[key]["error"] = result
        return success, fail

    @staticmethod
    def __translate_get_multi_sub_doc_result(data, path_val=None):
        success = dict()
        fail = dict()
        if data is None:
            return success, fail
        for doc_key, result in data.items():
            if not result.success:
                fail[doc_key] = dict()
                fail[doc_key]['cas'] = result.cas
                fail[doc_key]['value'] = dict()
                for lookup_spec_result in data["application"].value:
                    if not lookup_spec_result["exists"]:
                        pass
                if not result['exists']:
                    fail[doc_key]['error'] = "SubDocKeyNotFound"
                else:
                    fail[doc_key]['error'] = SDKException.PathNotFoundException
                if path_val:
                    fail[doc_key]['path_val'] = path_val[doc_key]
            elif result.success:
                success[doc_key] = dict()
                success[doc_key]['cas'] = result.cas
                success[doc_key]['value'] = dict()
                for lookup_spec_result in result.value:
                    lookup_key = lookup_spec_result['path']
                    if lookup_spec_result["exists"]:
                        success[doc_key]['value'][lookup_key] \
                            = lookup_spec_result["value"]
                    else:
                        success[doc_key]['value'][lookup_key] = ""
        return success, fail

    # Translate APIs for sub-document operations
    @staticmethod
    def __translate_upsert_multi_sub_doc_result(result_dict, path_val=None):
        success = dict()
        fail = dict()
        if result_dict is None:
            return success, fail
        for doc_key, mutate_in_result in result_dict.items():
            if mutate_in_result.success:
                success[doc_key] = dict()
                success[doc_key]['cas'] = mutate_in_result.cas
                success[doc_key]['value'] = dict()
                for mutation_result in mutate_in_result.value:
                    success[doc_key]['value'][mutation_result['path']] = \
                        mutation_result["path"]
            else:
                fail[doc_key] = dict()
                fail[doc_key]['value'] = mutate_in_result["value"]
                fail[doc_key]['error'] = ""
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
        self.collection = self.bucketObj \
            .scope(scope_name) \
            .collection(collection_name)

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
        result = {"key": key, "value": None, "cas": 0,
                  "status": False, "error": None}
        try:
            options = SDKOptions.get_remove_options(
                persist_to=persist_to, replicate_to=replicate_to,
                timeout=timeout, time_unit=time_unit,
                durability=durability,
                cas=cas)
            delete_result = self.collection.remove(key, options)
            result.update({"status": delete_result.success,
                           "cas": delete_result.cas})
        except DocumentNotFoundException as e:
            self.log.debug("Exception: Document id {0} not found - {1}"
                           .format(key, e))
            result.update({"value": None, "error": str(e)})
        except CasMismatchException as e:
            self.log.debug("Exception: Cas mismatch for doc {0} - {1}"
                           .format(key, e))
            result.update({"error": str(e)})
        except (RequestCanceledException, TimeoutException) as e:
            self.log.debug("Request cancelled/timed-out: " + str(e))
            result.update({"error": str(e)})
        except CouchbaseException as e:
            self.log.debug("CB generic exception for doc {0} - {1}"
                           .format(key, e))
            result.update({"error": str(e)})
        except Exception as e:
            self.log.error("Error during remove of {0} - {1}".format(key, e))
            result.update({"error": str(e)})
        return result

    def insert(self, key, value,
               exp=0, exp_unit=SDKConstants.TimeUnit.SECONDS,
               persist_to=0, replicate_to=0,
               timeout=5, time_unit=SDKConstants.TimeUnit.SECONDS,
               doc_type="json",
               durability="", sdk_retry_strategy=None):
        result = {"key": key, "value": None, "cas": 0,
                  "status": False, "error": None}
        try:
            options = SDKOptions.get_insert_options(
                exp=exp, exp_unit=exp_unit,
                persist_to=persist_to, replicate_to=replicate_to,
                timeout=timeout, time_unit=time_unit,
                durability=durability)
            if doc_type == "binary":
                raise NotImplementedError()
            elif doc_type == "string":
                raise NotImplementedError()

            # Returns com.couchbase.client.java.kv.MutationResult object
            insert_result = self.collection.insert(key, value, options)
            result.update({"value": value,
                           "status": insert_result.success,
                           "cas": insert_result.cas})
        except DocumentExistsException as ex:
            self.log.debug("The document already exists! => " + str(ex))
            result.update({"value": value, "error": str(ex)})
        except DurabilityImpossibleException as ex:
            self.log.debug("Durability impossible for key: " + str(ex))
            result.update({"value": value, "error": str(ex)})
        except (RequestCanceledException, TimeoutException) as ex:
            self.log.debug("Request cancelled/timed-out: " + str(ex))
            result.update({"value": value, "error": str(ex)})
        except DurabilitySyncWriteAmbiguousException as e:
            self.log.debug("D_Ambiguous for key %s" % key)
            result.update({"error": str(e)})
        except CouchbaseException as e:
            self.log.debug("CB generic exception for doc {0} - {1}"
                           .format(key, e))
            result.update({"error": str(e)})
        except Exception as ex:
            self.log.error("Something else happened: " + str(ex))
            result.update({"value": value, "error": str(ex)})
            if result["error"]:
                result["error"] = str(ex.getClass().getName() +
                                      " | " + ex.getMessage())
        return result

    def replace(self, key, value,
                exp=0, exp_unit=SDKConstants.TimeUnit.SECONDS,
                persist_to=0, replicate_to=0,
                timeout=5, time_unit=SDKConstants.TimeUnit.SECONDS,
                durability="", cas=0, sdk_retry_strategy=None,
                preserve_expiry=None):
        result = {"key": key, "cas": 0,
                  "error": None, "status": False}
        try:
            options = SDKOptions.get_replace_options(
                persist_to=persist_to, replicate_to=replicate_to,
                timeout=timeout, time_unit=time_unit,
                durability=durability,
                cas=cas,
                preserve_expiry=preserve_expiry)
            # Returns com.couchbase.client.java.kv.MutationResult object
            replace_result = self.collection.replace(key, value, options)
            result.update({"value": value,
                           "status": replace_result.success,
                           "cas": replace_result.cas})
        except DocumentExistsException as e:
            self.log.debug("The document already exists! => " + str(e))
            result.update({"value": value, "error": str(e)})
        except CasMismatchException as e:
            self.log.debug("CAS mismatch for key %s - %s" % (key, e))
            result.update({"error": str(e)})
        except DocumentNotFoundException as e:
            result.update({"error": str(e)})
        except DurabilitySyncWriteAmbiguousException as e:
            self.log.debug("D_Ambiguous for key %s" % key)
            result.update({"error": str(e)})
        except (RequestCanceledException, TimeoutException) as e:
            self.log.debug("Request cancelled/timed-out: " + str(e))
            result.update({"error": str(e)})
        except Exception as e:
            self.log.error("Something else happened: " + str(e))
            result.update({"value": value, "error": str(e)})
        return result

    def touch(self, key, exp=0, exp_unit=SDKConstants.TimeUnit.SECONDS,
              timeout=5, time_unit=SDKConstants.TimeUnit.SECONDS,
              sdk_retry_strategy=None):
        result = {"key": key,
                  "value": None,
                  "cas": 0,
                  "status": False,
                  "error": None}
        touch_options = SDKOptions.get_touch_options(timeout, time_unit)
        try:
            touch_result = self.collection.touch(
                key, SDKOptions.get_duration(exp, exp_unit), touch_options)
            result.update({"status": touch_result.success,
                           "cas": touch_result.cas})
        except DocumentNotFoundException as e:
            self.log.debug("Document key '%s' not found!" % key)
            result["error"] = str(e)
        except (RequestCanceledException, TimeoutException) as ex:
            self.log.debug("Request cancelled/timed-out: " + str(ex))
            result.update({"error": str(ex)})
        except Exception as ex:
            self.log.error("Something else happened: " + str(ex))
            result.update({"error": str(ex)})
        return result

    def read(self, key, timeout=5, time_unit=SDKConstants.TimeUnit.SECONDS,
             sdk_retry_strategy=None, populate_value=True, with_expiry=None):
        result = {"key": key,
                  "value": None,
                  "cas": 0,
                  "status": False,
                  "error": None,
                  "ttl_present": None}
        read_options = SDKOptions.get_read_options(timeout, time_unit,
                                                   with_expiry=with_expiry)
        try:
            get_result = self.collection.get(key, read_options)
            result["status"] = get_result.success
            if populate_value:
                result["value"] = get_result.value
            result["cas"] = get_result.cas
            if with_expiry:
                result["ttl_present"] = get_result.expiryTime
        except (DocumentNotFoundException, RequestCanceledException,
                TimeoutException) as e:
            self.log.debug("Request cancelled/timed-out: " + str(e))
            result.update({"error": str(e)})
        except CouchbaseException as e:
            result.update({"error": str(e)})
        except Exception as ex:
            self.log.error("Something else happened: " + str(ex))
            result.update({"error": str(ex)})
        return result

    def get_from_all_replicas(self, key):
        result = list()
        get_results = self.collection.get_all_replicas(
            key, GetAllReplicasOptions(
                timeout=SDKOptions.get_duration(60, "seconds")))
        try:
            for get_result in get_results:
                result.append({"key": get_result.key,
                               "value": get_result.content_as[dict],
                               "cas": get_result.cas,
                               "status": get_result.success})
        except Exception:
            pass
        return result

    def upsert(self, key, value,
               exp=0, exp_unit=SDKConstants.TimeUnit.SECONDS,
               persist_to=0, replicate_to=0,
               timeout=5, time_unit=SDKConstants.TimeUnit.SECONDS,
               durability="", sdk_retry_strategy=None, preserve_expiry=None):
        result = dict()
        result["cas"] = 0
        try:
            options = SDKOptions.get_upsert_options(
                exp=exp, exp_unit=exp_unit,
                persist_to=persist_to, replicate_to=replicate_to,
                timeout=timeout, time_unit=time_unit,
                durability=durability,
                preserve_expiry=preserve_expiry)
            upsert_result = self.collection.upsert(key, value, options)
            result.update({"key": key, "value": value,
                           "error": None, "status": True,
                           "cas": upsert_result.cas})
        except DocumentExistsException as ex:
            self.log.debug("Upsert: Document already exists! => " + str(ex))
            result.update({"key": key, "value": value,
                           "error": str(ex), "status": False})
        except (RequestCanceledException, TimeoutException) as ex:
            self.log.debug("Request cancelled/timed-out: " + str(ex))
            result.update({"key": key, "value": None,
                           "error": str(ex), "status": False})
        except DurabilitySyncWriteAmbiguousException as ex:
            self.log.debug("Durability Ambiguous for key: %s" % key)
            result.update({"key": key, "value": value,
                           "error": str(ex), "status": False})
        except CouchbaseException as ex:
            result.update({"key": key, "value": value,
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
            result = self.touch(key, exp=exp,
                                timeout=timeout, time_unit=time_unit,
                                sdk_retry_strategy=sdk_retry_strategy)
        elif op_type == DocLoading.Bucket.DocOps.READ:
            result = self.read(key, timeout=timeout, time_unit=time_unit,
                               sdk_retry_strategy=sdk_retry_strategy)
        elif op_type in [DocLoading.Bucket.SubDocOps.INSERT, "subdoc_insert"]:
            sub_key, value = value[0], value[1]
            path_val = dict()
            path_val[key] = [(sub_key, value)]
            mutate_in_specs = list()
            mutate_in_specs.append(SubDoc.insert(
                sub_key, value,
                create_parents=create_path, xattr=xattr))
            if not xattr:
                mutate_in_specs.append(SubDoc.increment("mutated", 1))
            options = SDKOptions.get_mutate_in_options(
                cas, timeout, time_unit,
                persist_to, replicate_to, durability,
                store_semantics=store_semantics,
                preserve_expiry=preserve_expiry,
                access_deleted=access_deleted,
                create_as_deleted=create_as_deleted)

            result, _ = self.__translate_upsert_multi_sub_doc_result(
                {key: self.collection.mutate_in(key, mutate_in_specs,
                                                options)}, path_val)
        elif op_type in [DocLoading.Bucket.SubDocOps.UPSERT, "subdoc_upsert"]:
            sub_key, value = value[0], value[1]
            path_val = dict()
            path_val[key] = [(sub_key, value)]
            mutate_in_specs = list()
            mutate_in_specs.append(SubDoc.upsert(
                sub_key, value, create_parents=True, xattr=xattr))
            if not xattr:
                mutate_in_specs.append(SubDoc.increment("mutated", 1))
            options = SDKOptions.get_mutate_in_options(
                cas, timeout, time_unit,
                persist_to, replicate_to, durability,
                store_semantics=store_semantics,
                preserve_expiry=preserve_expiry,
                create_as_deleted=create_as_deleted)
            result, _ = self.__translate_upsert_multi_sub_doc_result(
                {key: self.collection.mutate_in(key, mutate_in_specs,
                                                options)}, path_val)
        elif op_type in [DocLoading.Bucket.SubDocOps.REMOVE, "subdoc_delete"]:
            mutate_in_specs = list()
            path_val = dict()
            path_val[key] = [(value, '')]
            mutate_in_specs.append(SubDoc.remove(value, xattr=xattr))
            if not xattr:
                mutate_in_specs.append(SubDoc.increment("mutated", 1))
            options = SDKOptions.get_mutate_in_options(
                cas, timeout, time_unit,
                persist_to, replicate_to, durability,
                store_semantics=store_semantics,
                preserve_expiry=preserve_expiry,
                create_as_deleted=create_as_deleted)
            result, _ = self.__translate_upsert_multi_sub_doc_result(
                {key: self.collection.mutate_in(key, mutate_in_specs,
                                                options)}, path_val)
        elif op_type == "subdoc_replace":
            sub_key, value = value[0], value[1]
            path_val = dict()
            path_val[key] = [(sub_key, value)]
            mutate_in_specs = list()
            mutate_in_specs.append(SubDoc.replace(sub_key, value, xattr=xattr))
            if not xattr:
                mutate_in_specs.append(SubDoc.increment("mutated", 1))
            options = SDKOptions.get_mutate_in_options(
                cas, timeout, time_unit,
                persist_to, replicate_to, durability,
                store_semantics=store_semantics,
                preserve_expiry=preserve_expiry,
                create_as_deleted=create_as_deleted)
            result, _ = self.__translate_upsert_multi_sub_doc_result(
                {key: self.collection.mutate_in(key, mutate_in_specs,
                                                options)}, path_val)
        elif op_type in [DocLoading.Bucket.SubDocOps.LOOKUP, "subdoc_read"]:
            mutate_in_specs = list()
            path_val = dict()
            path_val[key] = [(value, '')]
            mutate_in_specs.append(SubDoc.get(value, xattr=xattr))
            options = SDKOptions.get_look_up_in_options(
                timeout, time_unit)

            result, _ = self.__translate_get_multi_sub_doc_result(
                {key: self.collection.lookup_in(key, mutate_in_specs,
                                                options)}, path_val)
        elif op_type == DocLoading.Bucket.SubDocOps.COUNTER:
            sub_key, step_value = value[0], value[1]
            mutate_in_specs = list()
            if not xattr:
                mutate_in_specs.append(SubDoc.increment(
                    sub_key, step_value, create_parents=create_path))
            options = SDKOptions.get_mutate_in_options(
                cas, timeout, time_unit,
                persist_to, replicate_to, durability,
                store_semantics=store_semantics,
                preserve_expiry=preserve_expiry)
            result, _ = self.__translate_upsert_multi_sub_doc_result(
                {key: self.collection.mutate_in(key, mutate_in_specs,
                                                options)})
        else:
            self.log.error("Unsupported operation %s" % op_type)
        return result

    # Bulk CRUD APIs
    def delete_multi(self, keys, persist_to=0, replicate_to=0,
                     timeout=5, time_unit=SDKConstants.TimeUnit.SECONDS,
                     durability="", sdk_retry_strategy=None):
        options = SDKOptions.get_remove_multi_options(
            persist_to=persist_to, replicate_to=replicate_to,
            timeout=timeout, time_unit=time_unit,
            durability=durability)
        result = self.collection.remove_multi(keys, options)
        return self.__translate_delete_multi_results(result)

    def touch_multi(self, keys, exp=0,
                    timeout=5, time_unit=SDKConstants.TimeUnit.SECONDS,
                    sdk_retry_strategy=None):
        touch_options = SDKOptions.get_touch_multi_options(timeout, time_unit)
        exp_duration = SDKOptions.get_duration(
            exp, SDKConstants.TimeUnit.SECONDS)
        result = self.collection.touch_multi(keys, exp_duration, touch_options)
        return self.__translate_delete_multi_results(result)

    def set_multi(self, items, exp=0, exp_unit=SDKConstants.TimeUnit.SECONDS,
                  persist_to=0, replicate_to=0,
                  timeout=5, time_unit=SDKConstants.TimeUnit.SECONDS,
                  doc_type="json", durability="", sdk_retry_strategy=None):
        options = SDKOptions.get_insert_multi_options(
            exp=exp, exp_unit=exp_unit,
            persist_to=persist_to, replicate_to=replicate_to,
            timeout=timeout, time_unit=time_unit,
            durability=durability)
        if doc_type.lower() == "binary":
            raise NotImplementedError()
        elif doc_type.lower() == "string":
            raise NotImplementedError()
        result = self.collection.insert_multi(items, options)
        return self.__translate_upsert_multi_results(result)

    def upsert_multi(self, docs,
                     exp=0, exp_unit=SDKConstants.TimeUnit.SECONDS,
                     persist_to=0, replicate_to=0,
                     timeout=5, time_unit=SDKConstants.TimeUnit.SECONDS,
                     doc_type="json", durability="",
                     preserve_expiry=None, sdk_retry_strategy=None):
        options = SDKOptions.get_upsert_multi_options(
            exp=exp, exp_unit=exp_unit,
            persist_to=persist_to, replicate_to=replicate_to,
            timeout=timeout, time_unit=time_unit,
            durability=durability)
        if doc_type.lower() == "binary":
            raise NotImplementedError()
        elif doc_type.lower() == "string":
            raise NotImplementedError()
        result = self.collection.upsert_multi(docs, options)
        return self.__translate_upsert_multi_results(result)

    def replace_multi(self, docs,
                      exp=0, exp_unit=SDKConstants.TimeUnit.SECONDS,
                      persist_to=0, replicate_to=0,
                      timeout=5, time_unit=SDKConstants.TimeUnit.SECONDS,
                      doc_type="json", durability="",
                      preserve_expiry=None, sdk_retry_strategy=None):
        options = SDKOptions.get_replace_multi_options(
            exp=exp, exp_unit=exp_unit,
            persist_to=persist_to, replicate_to=replicate_to,
            timeout=timeout, time_unit=time_unit,
            durability=durability,
            preserve_expiry=preserve_expiry)
        if doc_type.lower() == "binary":
            raise NotImplementedError()
        elif doc_type.lower() == "string":
            raise NotImplementedError()
        result = self.collection.replace_multi(docs, options)
        return self.__translate_upsert_multi_results(result)

    def get_multi(self, keys,
                  timeout=5, time_unit=SDKConstants.TimeUnit.SECONDS,
                  sdk_retry_strategy=None):
        read_options = SDKOptions.get_read_multi_options(timeout, time_unit)
        result = self.collection.get_multi(keys, read_options)
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
        path_val = dict()
        look_up_multi_result = dict()
        options = SDKOptions.get_look_up_in_options(timeout=timeout,
                                                    time_unit=time_unit)
        for doc_key, sub_doc_tuples in keys:
            path_val[doc_key] = list()
            mutate_in_spec = list()
            for sd_key, sd_val in sub_doc_tuples:
                path_val[doc_key].append((sd_key, ''))
                mutate_in_spec.append(SubDoc.get(sd_key, xattr=xattr))
            look_up_multi_result.update(
                self.collection.mutate_in(doc_key, mutate_in_spec, options))
        return self.__translate_get_multi_sub_doc_result(
            look_up_multi_result, path_val)

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

    def run_query(self, query, timeout=None,
                  timeunit=SDKConstants.TimeUnit.SECONDS):
        if timeout is not None:
            options = QueryOptions.queryOptions()
            options = options.timeout(SDKOptions.get_duration(timeout,
                                                              timeunit))
            return self.cluster.query(query, options)
        return self.cluster.query(query)
