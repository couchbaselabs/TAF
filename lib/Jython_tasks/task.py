'''
Created on Sep 14, 2017

@author: riteshagarwal
'''
from BucketLib.BucketOperations import BucketHelper
from BucketLib.MemcachedOperations import MemcachedHelper
from Jython_tasks.shutdown import shutdown_and_await_termination
import copy

# from bucket_utils.bucket_ready_functions import Bucket
from couchbase_helper.document import DesignDocument
from couchbase_helper.documentgenerator import BatchedDocumentGenerator
from httplib import IncompleteRead
import json as Json
import logging
from membase.api.exception import N1QLQueryException, DropIndexException, CreateIndexException, \
    DesignDocCreationException, QueryViewException, ReadDocumentException, RebalanceFailedException, \
    GetBucketInfoFailed, CompactViewFailed, SetViewInfoNotFound, FailoverFailedException, \
    ServerUnavailableException, BucketCreationException
from membase.api.rest_client import RestConnection
from memcached.helper.data_helper import MemcachedClientHelper
import random
from sdk_client import SDKSmartClient as VBucketAwareMemcached
import socket
import string
import sys
from threading import Thread
import threading
import time
import math

from com.couchbase.client.java import *;
from com.couchbase.client.java.document import *;
from com.couchbase.client.java.document.json import *;
from com.couchbase.client.java.query import *;
from com.couchbase.client.java.transcoder import JsonTranscoder
from java.util.concurrent import Callable
from java.util.concurrent import Executors, TimeUnit
from Jython_tasks import task_manager

log = logging.getLogger(__name__)


class Task(Callable):
    def __init__(self, thread_name):
        self.thread_name = thread_name
        self.exception = None
        self.completed = False
        self.started = False
        self.start_time = 0

    def __str__(self):
        if self.exception:
            raise self.exception
        elif self.completed:
            log.info("Task %s completed on: %s" % (
            self.thread_name, str(time.strftime("%H:%M:%S", time.gmtime(self.end_time)))))
            return "%s task completed in %.2fs" % \
                   (self.thread_name, self.completed - self.started,)
        elif self.started:
            return "Thread %s at %s" % \
                   (self.thread_name, str(time.strftime("%H:%M:%S", time.gmtime(self.start_time))))
        else:
            return "[%s] not yet scheduled" % \
                   (self.thread_name)

    def start_task(self):
        self.started = True
        self.start_time = time.time()

    def set_exception(self, exception):
        self.exception = exception
        self.complete_task()
        raise BaseException(self.exception)

    def complete_task(self):
        self.completed = True
        self.end_time = time.time()

    def call(self):
        raise NotImplementedError


class DocloaderTask(Callable):
    def __init__(self, bucket, num_items, start_from, k, v, thread_name):
        self.bucket = bucket
        self.num_items = num_items
        self.start_from = start_from
        self.started = None
        self.completed = None
        self.loaded = 0
        self.thread_used = thread_name
        self.exception = None
        self.key = k
        self.value = v

    def __str__(self):
        if self.exception:
            return "[%s] %s download error %s in %.2fs" % \
                   (self.thread_used, self.num_items, self.exception,
                    self.completed - self.started,)  # , self.result)
        elif self.completed:
            print "Time: %s" % str(time.strftime("%H:%M:%S", time.gmtime(time.time())))
            return "[%s] %s items loaded in %.2fs" % \
                   (self.thread_used, self.loaded,
                    self.completed - self.started,)  # , self.result)
        elif self.started:
            return "[%s] %s started at %s" % \
                   (self.thread_used, self.num_items, self.started)
        else:
            return "[%s] %s not yet scheduled" % \
                   (self.thread_used, self.num_items)

    def call(self):
        self.started = time.time()
        try:
            var = str(Json.dumps(self.value))
            user = JsonTranscoder().stringToJsonObject(var);
            #             user = JsonObject.fromJson(str(self.value))
            for i in xrange(self.num_items):
                doc = JsonDocument.create(self.key + str(i + self.start_from), user);
                response = self.bucket.upsert(doc);
                self.loaded += 1
        except Exception, ex:
            self.exception = ex
        self.completed = time.time()
        return self


class docloadertask_executor():

    def load(self, k, v, docs=10000, server="localhost", bucket="default"):
        cluster = CouchbaseCluster.create(server);
        cluster.authenticate("Administrator", "password")
        bucket = cluster.openBucket(bucket);

        pool = Executors.newFixedThreadPool(5)
        docloaders = []
        num_executors = 5
        total_num_executors = 5
        num_docs = docs / total_num_executors
        for i in xrange(total_num_executors):
            docloaders.append(DocloaderTask(bucket, num_docs, i * num_docs, k, v))
        futures = pool.invokeAll(docloaders)
        for future in futures:
            print future.get(num_executors, TimeUnit.SECONDS)

        print "Executors completed!!"
        shutdown_and_await_termination(pool, 5)
        if bucket.close() and cluster.disconnect():
            pass


class rebalanceTask(Task):

    def __init__(self, servers, to_add=[], to_remove=[], do_stop=False, progress=30,
                 use_hostnames=False, services=None, check_vbucket_shuffling=True):
        super(rebalanceTask, self).__init__("Rebalance_task")
        self.servers = servers
        self.to_add = to_add
        self.to_remove = to_remove
        self.start_time = None
        self.services = services
        self.monitor_vbuckets_shuffling = False
        self.check_vbucket_shuffling = check_vbucket_shuffling
        try:
            self.rest = RestConnection(self.servers[0])
        except ServerUnavailableException, e:
            log.error(e)
            raise e
        self.retry_get_progress = 0
        self.use_hostnames = use_hostnames
        self.previous_progress = 0
        self.old_vbuckets = {}
        self.thread_used = "Rebalance_task"

    def __str__(self):
        if self.exception:
            return "[%s] %s download error %s in %.2fs" % \
                   (self.thread_name, self.num_items, self.exception,
                    self.completed - self.started,)  # , self.result)
        elif self.completed:
            print "Time: %s" % str(time.strftime("%H:%M:%S", time.gmtime(time.time())))
            return "[%s] %s items loaded in %.2fs" % \
                   (self.thread_name, self.loaded,
                    self.completed - self.started,)  # , self.result)
        elif self.started:
            return "[%s] %s started at %s" % \
                   (self.thread_name, self.num_items, self.started)
        else:
            return "[%s] %s not yet scheduled" % \
                   (self.thread_name, self.num_items)

    def call(self):
        self.start_task()
        try:
            if len(self.to_add) and len(self.to_add) == len(self.to_remove):
                node_version_check = self.rest.check_node_versions()
                non_swap_servers = set(self.servers) - set(self.to_remove) - set(self.to_add)
                if self.check_vbucket_shuffling:
                    self.old_vbuckets = BucketHelper(self.servers[0])._get_vbuckets(non_swap_servers, None)
                if self.old_vbuckets and self.check_vbucket_shuffling:
                    self.monitor_vbuckets_shuffling = True
                if self.monitor_vbuckets_shuffling and node_version_check and self.services:
                    for service_group in self.services:
                        if "kv" not in service_group:
                            self.monitor_vbuckets_shuffling = False
                if self.monitor_vbuckets_shuffling and node_version_check:
                    services_map = self.rest.get_nodes_services()
                    for remove_node in self.to_remove:
                        key = "{0}:{1}".format(remove_node.ip, remove_node.port)
                        services = services_map[key]
                        if "kv" not in services:
                            self.monitor_vbuckets_shuffling = False
                if self.monitor_vbuckets_shuffling:
                    log.info("This is swap rebalance and we will monitor vbuckets shuffling")
            self.add_nodes()
            self.start_rebalance()
            self.check()
            # self.task_manager.schedule(self)
        except Exception as e:
            self.exception = e
            self.result = False
            log.info(str(e))
            return self.result
        self.complete_task()
        self.result = True
        return self.result

    def add_nodes(self):
        master = self.servers[0]
        services_for_node = None
        node_index = 0
        for node in self.to_add:
            log.info("adding node {0}:{1} to cluster".format(node.ip, node.port))
            if self.services != None:
                services_for_node = [self.services[node_index]]
                node_index += 1
            if self.use_hostnames:
                self.rest.add_node(master.rest_username, master.rest_password,
                                   node.hostname, node.port, services=services_for_node)
            else:
                self.rest.add_node(master.rest_username, master.rest_password,
                                   node.ip, node.port, services=services_for_node)

    def start_rebalance(self):
        nodes = self.rest.node_statuses()

        # Determine whether its a cluster_run/not
        cluster_run = True

        firstIp = self.servers[0].ip
        if len(self.servers) == 1 and self.servers[0].port == '8091':
            cluster_run = False
        else:
            for node in self.servers:
                if node.ip != firstIp:
                    cluster_run = False
                    break
        ejectedNodes = []

        for server in self.to_remove:
            for node in nodes:
                if cluster_run:
                    if int(server.port) == int(node.port):
                        ejectedNodes.append(node.id)
                        log.info("removing node {0}:{1} to cluster".format(node.ip, node.port))
                else:
                    if self.use_hostnames:
                        if server.hostname == node.ip and int(server.port) == int(node.port):
                            ejectedNodes.append(node.id)
                            log.info("removing node {0}:{1} to cluster".format(node.ip, node.port))
                    elif server.ip == node.ip and int(server.port) == int(node.port):
                        ejectedNodes.append(node.id)
                        log.info("removing node {0}:{1} to cluster".format(node.ip, node.port))

        if self.rest.is_cluster_mixed():
            # workaround MB-8094
            log.warn("cluster is mixed. sleep for 15 seconds before rebalance")
            time.sleep(15)

        self.rest.rebalance(otpNodes=[node.id for node in nodes], ejectedNodes=ejectedNodes)
        self.start_time = time.time()

    def check(self):
        status = None
        progress = -100
        try:
            if self.monitor_vbuckets_shuffling:
                log.info("This is swap rebalance and we will monitor vbuckets shuffling")
                non_swap_servers = set(self.servers) - set(self.to_remove) - set(self.to_add)
                new_vbuckets = BucketHelper(self.servers[0])._get_vbuckets(non_swap_servers, None)
                for vb_type in ["active_vb", "replica_vb"]:
                    for srv in non_swap_servers:
                        if not (len(self.old_vbuckets[srv][vb_type]) + 1 >= len(new_vbuckets[srv][vb_type]) and \
                                len(self.old_vbuckets[srv][vb_type]) - 1 <= len(new_vbuckets[srv][vb_type])):
                            msg = "Vbuckets were suffled! Expected %s for %s" % (vb_type, srv.ip) + \
                                  " are %s. And now are %s" % (
                                      len(self.old_vbuckets[srv][vb_type]),
                                      len(new_vbuckets[srv][vb_type]))
                            log.error(msg)
                            log.error("Old vbuckets: %s, new vbuckets %s" % (self.old_vbuckets, new_vbuckets))
                            raise Exception(msg)
            (status, progress) = self.rest._rebalance_status_and_progress()
            log.info("Rebalance - status: %s, progress: %s", status, progress)
            # if ServerUnavailableException
            if progress == -100:
                self.retry_get_progress += 1
            if self.previous_progress != progress:
                self.previous_progress = progress
                self.retry_get_progress = 0
            else:
                self.retry_get_progress += 1
        except RebalanceFailedException as ex:
            self.result = False
            raise ex
        # catch and set all unexpected exceptions
        except Exception as e:
            self.result = False
            raise e
        retry_get_process_num = 25
        if self.rest.is_cluster_mixed():
            """ for mix cluster, rebalance takes longer """
            log.info("rebalance in mix cluster")
            retry_get_process_num = 40
        # we need to wait for status to be 'none' (i.e. rebalance actually finished and
        # not just 'running' and at 100%) before we declare ourselves done
        if progress != -1 and status != 'none':
            if self.retry_get_progress < retry_get_process_num:
                time.sleep(3)
                self.check()
            else:
                self.result = False
                self.rest.print_UI_logs()
                raise RebalanceFailedException("seems like rebalance hangs. please check logs!")
        else:
            success_cleaned = []
            for removed in self.to_remove:
                try:
                    rest = RestConnection(removed)
                except ServerUnavailableException, e:
                    log.error(e)
                    continue
                start = time.time()
                while time.time() - start < 30:
                    try:
                        if 'pools' in rest.get_pools_info() and \
                                (len(rest.get_pools_info()["pools"]) == 0):
                            success_cleaned.append(removed)
                            break
                        else:
                            time.sleep(0.1)
                    except (ServerUnavailableException, IncompleteRead), e:
                        log.error(e)
            result = True
            for node in set(self.to_remove) - set(success_cleaned):
                log.error("node {0}:{1} was not cleaned after removing from cluster" \
                          .format(node.ip, node.port))
                self.result = False

            log.info("rebalancing was completed with progress: {0}% in {1} sec".
                     format(progress, time.time() - self.start_time))
            self.result = True
            return


class GenericLoadingTask(Task):
    def __init__(self, cluster, bucket, client, batch_size=1, pause_secs=1, timeout_secs=60, compression=True,
                 throughput_concurrency=4):
        super(GenericLoadingTask, self).__init__("Loadgen_task")
        self.batch_size = batch_size
        self.pause = pause_secs
        self.timeout = timeout_secs
        self.cluster = cluster
        self.bucket = bucket
        self.client = client
        self.process_concurrency = throughput_concurrency
        self.random = random.Random()

    def call(self):
        self.start_task()
        log.info("Starting load generation thread")
        try:
            while self.has_next():
                self.next()
        except Exception as e:
            self.set_exception(Exception(e.message))
        log.info("Load generation thread completed")
        self.complete_task()

    def has_next(self):
        raise NotImplementedError

    def next(self):
        raise NotImplementedError

    def _unlocked_create(self, key, value, is_base64_value=False):
        try:
            value_json = Json.loads(value)
            if isinstance(value_json, dict):
                value_json['mutated'] = 0
            value = Json.dumps(value_json)
        except ValueError:
            self.random.seed(key)
            index = self.random.choice(range(len(value)))
            if not is_base64_value:
                value = value[0:index] + self.random.choice(string.ascii_uppercase) + value[index + 1:]
        except TypeError:
            value = Json.dumps(value)
        try:
            self.client.set(key, self.exp, self.flag, value)
        except Exception as error:
            self.set_exception(error)

    def _unlocked_read(self, key):
        try:
            o, c, d = self.client.get(key)
        except self.client.MemcachedError as error:
            self.set_exception(error)

    def _unlocked_replica_read(self, key):
        try:
            o, c, d = self.client.getr(key)
        except Exception as error:
            self.set_exception(error)

    def _unlocked_update(self, key):
        value = None
        try:
            o, c, value = self.client.get(key)
            if value is None:
                return
            value_json = Json.loads(value)
            value_json['mutated'] += 1
            value = Json.dumps(value_json)
        except self.client.MemcachedError as error:
            log.error("%s, key: %s update operation." % (error, key))
            self.set_exception(error)
            return
        except ValueError:
            if value is None:
                return
            self.random.seed(key)
            index = self.random.choice(range(len(value)))
            value = value[0:index] + self.random.choice(string.ascii_uppercase) + value[index + 1:]
        except BaseException as error:
            self.set_exception(error)

        try:
            self.client.upsert(key, self.exp, self.flag, value)
        except BaseException as error:
            self.set_exception(error)

    def _unlocked_delete(self, key):
        try:
            self.client.delete(key)
        except self.client.MemcachedError as error:
            log.error("%s, key: %s delete operation." % (error, key))
            self.set_exception(error)
        except BaseException as error:
            self.set_exception(error)

    def _unlocked_append(self, key, value):
        try:
            o, c, old_value = self.client.get(key)
            if value is None:
                return
            value_json = Json.loads(value)
            old_value_json = Json.loads(old_value)
            old_value_json.update(value_json)
            old_value = Json.dumps(old_value_json)
            value = Json.dumps(value_json)
        except self.client.MemcachedError as error:
            self.set_exception(error)
            return
        except ValueError:
            o, c, old_value = self.client.get(key)
            self.random.seed(key)
            index = self.random.choice(range(len(value)))
            value = value[0:index] + self.random.choice(string.ascii_uppercase) + value[index + 1:]
            old_value += value
        except BaseException as error:
            self.set_exception(error)

        try:
            self.client.append(key, value)
        except BaseException as error:
            self.set_exception(error)

    # start of batch methods
    def _create_batch_client(self, key_val, shared_client=None):
        """
        standalone method for creating key/values in batch (sans kvstore)

        arguments:
            key_val -- array of key/value dicts to load size = self.batch_size
            shared_client -- optional client to use for data loading
        """
        try:
            self._process_values_for_create(key_val)
            client = shared_client or self.client
            client.setMulti(self.exp, self.flag, key_val, self.pause, self.timeout, parallel=False)
            # log.info("Batch Operation: %s documents are INSERTED into bucket %s"%(len(key_val), self.bucket))
        except (self.client.MemcachedError, ServerUnavailableException, socket.error, EOFError, AttributeError,
                RuntimeError) as error:
            self.set_exception(error)

    def _create_batch(self, key_val):
        self._create_batch_client(key_val)

    def _update_batch(self, key_val):
        try:
            self._process_values_for_update(key_val)
            self.client.upsertMulti(self.exp, self.flag, key_val, self.pause, self.timeout, parallel=False)
            #log.info("Batch Operation: %s documents are UPSERTED into bucket %s" % (len(key_val), self.bucket))
        except (self.client.MemcachedError, ServerUnavailableException, socket.error, EOFError, AttributeError,
                RuntimeError) as error:
            self.set_exception(error)

    def _delete_batch(self, key_val):
        for key, value in key_val.items():
            try:
                self.client.delete(key)
            except self.client.MemcachedError as error:
                self.set_exception(error)
                return
            except (ServerUnavailableException, socket.error, EOFError, AttributeError) as error:
                self.set_exception(error)

    def _read_batch(self, key_val):
        try:
            result_map = self.client.getMulti(key_val.keys(), self.pause, self.timeout)
            # log.info("Batch Operation: %s documents are READ from bucket %s"%(len(key_val), self.bucket))
            return result_map
        except self.client.MemcachedError as error:
            self.set_exception(error)

    def _process_values_for_create(self, key_val):
        for key, value in key_val.items():
            try:
                value_json = Json.loads(value)
                value_json['mutated'] = 0
                value = Json.dumps(value_json)
            except ValueError:
                self.random.seed(key)
                index = self.random.choice(range(len(value)))
                value = value[0:index] + self.random.choice(string.ascii_uppercase) + value[index + 1:]
            except TypeError:
                value = Json.dumps(value)
            except Exception, e:
                print e
            finally:
                key_val[key] = value

    def _process_values_for_update(self, key_val):
        self._process_values_for_create(key_val)
        for key, value in key_val.items():
            try:
                value = key_val[key]  # new updated value, however it is not their in orginal code "LoadDocumentsTask"
                value_json = Json.loads(value)
                value_json['mutated'] += 1
                value = Json.dumps(value_json)
            except ValueError:
                self.random.seed(key)
                index = self.random.choice(range(len(value)))
                value = value[0:index] + self.random.choice(string.ascii_uppercase) + value[index + 1:]
            finally:
                key_val[key] = value


class LoadDocumentsTask(GenericLoadingTask):

    def __init__(self, cluster, bucket, client, generator, op_type, exp, flag=0,
                 proxy_client=None, batch_size=1, pause_secs=1, timeout_secs=30,
                 compression=True, throughput_concurrency=4):
        super(LoadDocumentsTask, self).__init__(cluster, bucket, client, batch_size=batch_size,
                                                pause_secs=pause_secs,
                                                timeout_secs=timeout_secs, compression=compression,
                                                throughput_concurrency=throughput_concurrency)

        self.generator = generator
        self.op_type = op_type
        self.exp = exp
        self.flag = flag

        if proxy_client:
            log.info("Changing client to proxy %s:%s..." % (proxy_client.host,
                                                            proxy_client.port))
            self.client = proxy_client

    def has_next(self):
        return self.generator.has_next()

    def next(self, override_generator=None):
        doc_gen = override_generator or self.generator
        key_value = doc_gen.next_batch()
        if self.op_type == 'create':
            self._create_batch(key_value)
        elif self.op_type == 'update':
            self._update_batch(key_value)
        elif self.op_type == 'delete':
            self._delete_batch(key_value)
        elif self.op_type == 'read':
            self._read_batch(key_value)
        else:
            self.set_exception(Exception("Bad operation type: %s" % self.op_type))

        # print "batchsize = {}".format(self.batch_size)
        # if self.batch_size == 1:
        #     print self.generator
        #     key, value = self.generator.next()
        #     print key
        #     print value
        #     if self.op_type == 'create':
        #         is_base64_value = (self.generator.__class__.__name__ == 'Base64Generator')
        #         print 'came here'
        #         self._unlocked_create(key, value, is_base64_value=is_base64_value)
        #     elif self.op_type == 'read':
        #         self._unlocked_read(key)
        #     elif self.op_type == 'read_replica':
        #         self._unlocked_replica_read(key)
        #     elif self.op_type == 'update':
        #         self._unlocked_update(key)
        #     elif self.op_type == 'delete':
        #         self._unlocked_delete(key)
        #     elif self.op_type == 'append':
        #         self._unlocked_append(key, value)
        #     else:
        #         self.set_exception(Exception("Bad operation type: %s" % self.op_type))
        # else:
        #     doc_gen = override_generator or self.generator
        #     key_value = doc_gen.next_batch()
        #     if self.op_type == 'create':
        #         self._create_batch(key_value)
        #     elif self.op_type == 'update':
        #         self._update_batch(key_value)
        #     elif self.op_type == 'delete':
        #         self._delete_batch(key_value)
        #     elif self.op_type == 'read':
        #         self._read_batch(key_value)
        #     else:
        #         self.set_exception(Exception("Bad operation type: %s" % self.op_type))


class LoadDocumentsGeneratorsTask(Task):
    def __init__(self, cluster, task_manager, bucket, client, generators, op_type, exp, flag=0,
                 only_store_hash=True, batch_size=1, pause_secs=1, timeout_secs=60, compression=True,
                 process_concurrency=4, print_ops_rate=True):
        super(LoadDocumentsGeneratorsTask, self).__init__("DocumentsLoadGenTask")
        self.cluster = cluster
        self.exp = exp
        self.flag = flag
        self.only_store_hash = only_store_hash
        self.pause_secs = pause_secs
        self.timeout_secs = timeout_secs
        self.compression = compression
        self.process_concurrency = process_concurrency
        self.client = client
        self.task_manager = task_manager
        self.batch_size = batch_size
        self.generators = generators
        self.input_generators = generators
        self.op_types = None
        self.buckets = None
        self.print_ops_rate = print_ops_rate
        if isinstance(op_type, list):
            self.op_types = op_type
        else:
            self.op_type = op_type
        if isinstance(bucket, list):
            self.buckets = bucket
        else:
            self.bucket = bucket

    def call(self):
        self.start_task()
        if self.op_types:
            if len(self.op_types) != len(self.generators):
                self.set_exception(Exception("not all generators have op_type!"))
                self.complete_task()
        if self.buckets:
            if len(self.op_types) != len(self.buckets):
                self.set_exception(Exception("not all generators have bucket specified!"))
                self.complete_task()
        iterator = 0
        tasks = []
        for generator in self.generators:
            if self.op_types:
                self.op_type = self.op_types[iterator]
            if self.buckets:
                self.bucket = self.buckets[iterator]
            tasks.extend(self.get_tasks(generator))
            iterator += 1
        if self.print_ops_rate:
            self.print_ops_rate_tasks = []
            if self.buckets:
                for bucket in self.buckets:
                    print_ops_rate_task = PrintOpsRate(self.cluster, bucket)
                    self.print_ops_rate_tasks.append(print_ops_rate_task)
                    self.task_manager.add_new_task(print_ops_rate_task)
            else:
                print_ops_rate_task = PrintOpsRate(self.cluster, self.bucket)
                self.print_ops_rate_tasks.append(print_ops_rate_task)
                self.task_manager.add_new_task(print_ops_rate_task)
        try:
            for task in tasks:
                self.task_manager.add_new_task(task)
            for task in tasks:
                self.task_manager.get_task_result(task)
        except Exception as e:
            self.set_exception(e)
        finally:
            self.client.close()
            if self.print_ops_rate and hasattr(self, "print_ops_rate_tasks"):
                for print_ops_rate_task in self.print_ops_rate_tasks:
                    print_ops_rate_task.end_task()
                    self.task_manager.get_task_result(print_ops_rate_task)
        self.complete_task()

    def get_tasks(self, generator):
        generators = []
        tasks = []
        gen_start = int(generator.start)
        gen_end = max(int(generator.end), 1)
        gen_range = max(int((generator.end - generator.start)/ self.process_concurrency), 1)
        for pos in range(gen_start, gen_end, gen_range):
            partition_gen = copy.deepcopy(generator)
            partition_gen.start = pos
            partition_gen.itr = pos
            partition_gen.end = pos + gen_range
            if partition_gen.end > generator.end:
                partition_gen.end = generator.end
            batch_gen = BatchedDocumentGenerator(
                partition_gen,
                self.batch_size)
            generators.append(batch_gen)
        for generator in generators:
            task = LoadDocumentsTask(self.cluster, self.bucket, self.client, generator, self.op_type,
                                     self.exp, self.flag, batch_size=self.batch_size,
                                     pause_secs=self.pause_secs, timeout_secs=self.timeout_secs,
                                     compression=self.compression, throughput_concurrency=self.process_concurrency)
            tasks.append(task)
        return tasks


class ValidateDocumentsTask(GenericLoadingTask):

    def __init__(self, cluster, bucket, client, generator, op_type, exp, flag=0,
                 proxy_client=None, batch_size=1, pause_secs=1, timeout_secs=30,
                 compression=True, throughput_concurrency=4):
        super(ValidateDocumentsTask, self).__init__(cluster, bucket, client, batch_size=batch_size,
                                                    pause_secs=pause_secs,
                                                    timeout_secs=timeout_secs, compression=compression,
                                                    throughput_concurrency=throughput_concurrency)

        self.generator = generator
        self.op_type = op_type
        self.exp = exp
        self.flag = flag

        if proxy_client:
            log.info("Changing client to proxy %s:%s..." % (proxy_client.host,
                                                            proxy_client.port))
            self.client = proxy_client

    def has_next(self):
        return self.generator.has_next()

    def next(self, override_generator=None):

        doc_gen = override_generator or self.generator
        key_value = doc_gen.next_batch()
        if self.op_type == 'create':
            self._process_values_for_create(key_value)
        elif self.op_type == 'update':
            self._process_values_for_update(key_value)
        elif self.op_type == 'delete':
            pass
        else:
            self.set_exception(Exception("Bad operation type: %s" % self.op_type))
        result_map = self._read_batch(key_value)
        missing_keys, wrong_values = self.validate_key_val(result_map, key_value)
        if self.op_type == 'delete':
            not_missing = []
            if missing_keys.__len__() == key_value.keys().__len__():
                for key in key_value.keys():
                    if key not in missing_keys:
                        not_missing.append(key)
                if not_missing:
                    self.set_exception(Exception("Keys were not deleted. "
                                                 "Keys not deleted: {}".format(','.join(not_missing))))
        else:
            if missing_keys:
                self.set_exception("Keys were missing. "
                                   "Keys missing: {}".format(','.join(missing_keys)))
            if wrong_values:
                self.set_exception("Wrong key value. "
                                   "Wrong key value: {}".format(','.join(wrong_values)))

    def validate_key_val(self, map, key_value):
        missing_keys = []
        wrong_values = []
        for key, value in key_value.items():
            if key in map:
                expected_val = Json.loads(value)
                actual_val = Json.loads(map[key][2])
                if expected_val == actual_val:
                    continue
                else:
                    wrong_value = "Key: {} Expected: {} Actual: {}".format(key, expected_val, actual_val)
                    wrong_values.append(wrong_value)
            else:
                missing_keys.append(key)
        return missing_keys, wrong_values


class DocumentsValidatorTask(Task):
    def __init__(self, cluster, task_manager, bucket, client, generators, op_type, exp, flag=0,
                 only_store_hash=True, batch_size=1, pause_secs=1, timeout_secs=60, compression=True,
                 process_concurrency=4):
        super(DocumentsValidatorTask, self).__init__("DocumentsLoadGenTask")
        self.cluster = cluster
        self.exp = exp
        self.flag = flag
        self.only_store_hash = only_store_hash
        self.pause_secs = pause_secs
        self.timeout_secs = timeout_secs
        self.compression = compression
        self.process_concurrency = process_concurrency
        self.client = client
        self.task_manager = task_manager
        self.batch_size = batch_size
        self.generators = generators
        self.input_generators = generators
        self.op_types = None
        self.buckets = None
        if isinstance(op_type, list):
            self.op_types = op_type
        else:
            self.op_type = op_type
        if isinstance(bucket, list):
            self.buckets = bucket
        else:
            self.bucket = bucket

    def call(self):
        self.start_task()
        if self.op_types:
            if len(self.op_types) != len(self.generators):
                self.set_exception(Exception("not all generators have op_type!"))
                self.complete_task()
        if self.buckets:
            if len(self.op_types) != len(self.buckets):
                self.set_exception(Exception("not all generators have bucket specified!"))
                self.complete_task()
        iterator = 0
        tasks = []
        for generator in self.generators:
            if self.op_types:
                self.op_type = self.op_types[iterator]
            if self.buckets:
                self.bucket = self.buckets[iterator]
            tasks.extend(self.get_tasks(generator))
            iterator += 1
        for task in tasks:
            self.task_manager.add_new_task(task)
        for task in tasks:
            self.task_manager.get_task_result(task)
        self.complete_task()

    def get_tasks(self, generator):
        generators = []
        tasks = []
        gen_start = int(generator.start)
        gen_end = max(int(generator.end), 1)
        gen_range = max(int((generator.end - generator.start) / self.process_concurrency), 1)
        for pos in range(gen_start, gen_end, gen_range):
            partition_gen = copy.deepcopy(generator)
            partition_gen.start = pos
            partition_gen.itr = pos
            partition_gen.end = pos + gen_range
            if partition_gen.end > generator.end:
                partition_gen.end = generator.end
            batch_gen = BatchedDocumentGenerator(
                partition_gen,
                self.batch_size)
            generators.append(batch_gen)
        for generator in generators:
            task = ValidateDocumentsTask(self.cluster, self.bucket, self.client, generator, self.op_type,
                                         self.exp, self.flag, batch_size=self.batch_size,
                                         pause_secs=self.pause_secs, timeout_secs=self.timeout_secs,
                                         compression=self.compression, throughput_concurrency=self.process_concurrency)
            tasks.append(task)
        return tasks


class StatsWaitTask(Task):
    EQUAL = '=='
    NOT_EQUAL = '!='
    LESS_THAN = '<'
    LESS_THAN_EQ = '<='
    GREATER_THAN = '>'
    GREATER_THAN_EQ = '>='

    def __init__(self, cluster, bucket, param, stat, comparison, value):
        super(StatsWaitTask, self).__init__("StatsWaitTask")
        self.cluster = cluster
        self.bucket = bucket
        self.param = param
        self.stat = stat
        self.comparison = comparison
        self.value = value
        self.conns = {}
        self.stop = False

    def call(self):
        self.start_task()
        stat_result = 0
        # import pydevd
        # pydevd.settrace(trace_only_current_thread=False)
        try:
            while not self.stop:
                self._get_stats_and_compare()
        finally:
            for server, conn in self.conns.items():
                conn.close()
        self.complete_task()

    def _get_stats_and_compare(self):
        stat_result = 0
        for server in self.cluster.nodes_in_cluster:
            try:
                client = self._get_connection(server)
                stats = client.stats(self.param)
                if not stats.has_key(self.stat):
                    self.set_exception(Exception("Stat {0} not found".format(self.stat)))
                    self.stop = True
                    return False
                if stats[self.stat].isdigit():
                    stat_result += long(stats[self.stat])
                else:
                    stat_result = stats[self.stat]
            except EOFError as ex:
                self.set_exception(ex)
                self.stop = True
                return False
        if not self._compare(self.comparison, str(stat_result), self.value):
            log.warn("Not Ready: %s %s %s %s expected on %s, %s bucket" % (self.stat, stat_result,
                                                                           self.comparison, self.value,
                                                                           self._stringify_servers(), self.bucket.name))
            time.sleep(5)
            return False
        else:
            self.stop = True
            return True

    def _stringify_servers(self):
        return ''.join([`server.ip + ":" + str(server.port)` for server in self.cluster.nodes_in_cluster])

    def _get_connection(self, server, admin_user='cbadminbucket', admin_pass='password'):
        if not self.conns.has_key(server):
            for i in xrange(3):
                try:
                    self.conns[server] = MemcachedClientHelper.direct_client(server, self.bucket, admin_user=admin_user,
                                                                             admin_pass=admin_pass)
                    return self.conns[server]
                except (EOFError, socket.error):
                    log.error("failed to create direct client, retry in 1 sec")
                    time.sleep(1)
            self.conns[server] = MemcachedClientHelper.direct_client(server, self.bucket, admin_user=admin_user,
                                                                     admin_pass=admin_pass)
        return self.conns[server]

    def _compare(self, cmp_type, a, b):
        if isinstance(b, (int, long)) and a.isdigit():
            a = long(a)
        elif isinstance(b, (int, long)) and not a.isdigit():
            return False
        if (cmp_type == StatsWaitTask.EQUAL and a == b) or \
                (cmp_type == StatsWaitTask.NOT_EQUAL and a != b) or \
                (cmp_type == StatsWaitTask.LESS_THAN_EQ and a <= b) or \
                (cmp_type == StatsWaitTask.GREATER_THAN_EQ and a >= b) or \
                (cmp_type == StatsWaitTask.LESS_THAN and a < b) or \
                (cmp_type == StatsWaitTask.GREATER_THAN and a > b):
            return True
        return False


class ViewCreateTask(Task):
    def __init__(self, server, design_doc_name, view, bucket="default", with_query=True,
                 check_replication=False, ddoc_options=None):
        super(ViewCreateTask, self).__init__("ViewCreateTask")
        self.server = server
        self.bucket = bucket
        self.view = view
        prefix = ""
        if self.view:
            prefix = ("", "dev_")[self.view.dev_view]
        if design_doc_name.find('/') != -1:
            design_doc_name = design_doc_name.replace('/', '%2f')
        self.design_doc_name = prefix + design_doc_name
        self.ddoc_rev_no = 0
        self.with_query = with_query
        self.check_replication = check_replication
        self.ddoc_options = ddoc_options
        self.rest = RestConnection(self.server)

    def call(self):
        self.start_task()
        try:
            # appending view to existing design doc
            content, meta = self.rest.get_ddoc(self.bucket, self.design_doc_name)
            ddoc = DesignDocument._init_from_json(self.design_doc_name, content)
            # if view is to be updated
            if self.view:
                if self.view.is_spatial:
                    ddoc.add_spatial_view(self.view)
                else:
                    ddoc.add_view(self.view)
            self.ddoc_rev_no = self._parse_revision(meta['rev'])
        except ReadDocumentException:
            # creating first view in design doc
            if self.view:
                if self.view.is_spatial:
                    ddoc = DesignDocument(self.design_doc_name, [], spatial_views=[self.view])
                else:
                    ddoc = DesignDocument(self.design_doc_name, [self.view])
            # create an empty design doc
            else:
                ddoc = DesignDocument(self.design_doc_name, [])
            if self.ddoc_options:
                ddoc.options = self.ddoc_options
        # catch and set all unexpected exceptions
        except Exception as e:
            self.set_exception(e)
            self.complete_task()
            return 0
        try:
            self.rest.create_design_document(self.bucket, ddoc)
            return_value = self.check()
            self.complete_task()
            return return_value
        except DesignDocCreationException as e:
            self.set_exception(e)
            self.complete_task()
            return 0

        # catch and set all unexpected exceptions
        except Exception as e:
            self.set_exception(e)
            self.complete_task()
            return 0

    def check(self):
        try:
            # only query if the DDoc has a view
            if self.view:
                if self.with_query:
                    query = {"stale": "ok"}
                    if self.view.is_spatial:
                        content = \
                            self.rest.query_view(self.design_doc_name, self.view.name,
                                                 self.bucket, query, type="spatial")
                    else:
                        content = \
                            self.rest.query_view(self.design_doc_name, self.view.name,
                                                 self.bucket, query)
                else:
                    _, json_parsed, _ = self.rest._get_design_doc(self.bucket, self.design_doc_name)
                    if self.view.is_spatial:
                        if self.view.name not in json_parsed["spatial"].keys():
                            self.set_exception(
                                Exception("design doc {0} doesn't contain spatial view {1}".format(
                                    self.design_doc_name, self.view.name)))
                            return 0
                    else:
                        if self.view.name not in json_parsed["views"].keys():
                            self.set_exception(Exception("design doc {0} doesn't contain view {1}".format(
                                self.design_doc_name, self.view.name)))
                            return 0
                log.info(
                    "view : {0} was created successfully in ddoc: {1}".format(self.view.name, self.design_doc_name))
            else:
                # if we have reached here, it means design doc was successfully updated
                log.info("Design Document : {0} was updated successfully".format(self.design_doc_name))

            if self._check_ddoc_revision():
                return self.ddoc_rev_no
            else:
                self.set_exception(Exception("failed to update design document"))
            if self.check_replication:
                self._check_ddoc_replication_on_nodes()

        except QueryViewException as e:
            if e.message.find('not_found') or e.message.find('view_undefined') > -1:
                self.check()
            else:
                self.set_exception(e)
                return 0
        # catch and set all unexpected exceptions
        except Exception as e:
            self.set_exception(e)
            return 0

    def _check_ddoc_revision(self):
        valid = False
        try:
            content, meta = self.rest.get_ddoc(self.bucket, self.design_doc_name)
            new_rev_id = self._parse_revision(meta['rev'])
            if new_rev_id != self.ddoc_rev_no:
                self.ddoc_rev_no = new_rev_id
                valid = True
        except ReadDocumentException:
            pass

        # catch and set all unexpected exceptions
        except Exception as e:
            self.set_exception(e)
            return False
        return valid

    def _parse_revision(self, rev_string):
        return int(rev_string.split('-')[0])

    def _check_ddoc_replication_on_nodes(self):

        nodes = self.rest.node_statuses()
        retry_count = 3

        # nothing to check if there is only 1 node
        if len(nodes) <= 1:
            return

        for node in nodes:
            server_info = {"ip": node.ip,
                           "port": node.port,
                           "username": self.rest.username,
                           "password": self.rest.password}

            for count in xrange(retry_count):
                try:
                    rest_node = RestConnection(server_info)
                    content, meta = rest_node.get_ddoc(self.bucket, self.design_doc_name)
                    new_rev_id = self._parse_revision(meta['rev'])
                    if new_rev_id == self.ddoc_rev_no:
                        break
                    else:
                        log.info("Design Doc {0} version is not updated on node {1}:{2}. Retrying.".format(
                            self.design_doc_name, node.ip, node.port))
                        time.sleep(2)
                except ReadDocumentException as e:
                    if (count < retry_count):
                        log.info(
                            "Design Doc {0} not yet available on node {1}:{2}. Retrying.".format(self.design_doc_name,
                                                                                                 node.ip, node.port))
                        time.sleep(2)
                    else:
                        log.error(
                            "Design Doc {0} failed to replicate on node {1}:{2}".format(self.design_doc_name, node.ip,
                                                                                        node.port))
                        self.set_exception(e)
                        break
                except Exception as e:
                    if (count < retry_count):
                        log.info("Unexpected Exception Caught. Retrying.")
                        time.sleep(2)
                    else:
                        self.set_exception(e)
                        break
            else:
                self.set_exception(Exception(
                    "Design Doc {0} version mismatch on node {1}:{2}".format(self.design_doc_name, node.ip, node.port)))


class ViewDeleteTask(Task):
    def __init__(self, server, design_doc_name, view, bucket="default"):
        Task.__init__(self, "delete_view_task")
        self.server = server
        self.bucket = bucket
        self.view = view
        prefix = ""
        if self.view:
            prefix = ("", "dev_")[self.view.dev_view]
        self.design_doc_name = prefix + design_doc_name

    def call(self):
        self.start_task()
        try:
            rest = RestConnection(self.server)
            if self.view:
                # remove view from existing design doc
                content, header = rest.get_ddoc(self.bucket, self.design_doc_name)
                ddoc = DesignDocument._init_from_json(self.design_doc_name, content)
                if self.view.is_spatial:
                    status = ddoc.delete_spatial(self.view)
                else:
                    status = ddoc.delete_view(self.view)
                if not status:
                    self.set_exception(Exception('View does not exist! %s' % (self.view.name)))
                    self.complete_task()
                    return False
                # update design doc
                rest.create_design_document(self.bucket, ddoc)
                return_value = self.check()
                self.complete_task()
                return return_value
            else:
                # delete the design doc
                rest.delete_view(self.bucket, self.design_doc_name)
                log.info("Design Doc : {0} was successfully deleted".format(self.design_doc_name))
                self.complete_task()
                return True

        except (ValueError, ReadDocumentException, DesignDocCreationException) as e:
            self.set_exception(e)
            self.complete_task()
            return False

        # catch and set all unexpected exceptions
        except Exception as e:
            self.set_exception(e)
            self.complete_task()
            return False

    def check(self):
        try:
            rest = RestConnection(self.server)
            # make sure view was deleted
            query = {"stale": "ok"}
            content = \
                rest.query_view(self.design_doc_name, self.view.name, self.bucket, query)
            return False
        except QueryViewException as e:
            log.info("view : {0} was successfully deleted in ddoc: {1}".format(self.view.name, self.design_doc_name))
            return True

        # catch and set all unexpected exceptions
        except Exception as e:
            self.set_exception(e)
            return False


class ViewQueryTask(Task):
    def __init__(self, server, design_doc_name, view_name,
                 query, expected_rows=None,
                 bucket="default", retry_time=2):
        Task.__init__(self, "query_view_task")
        self.server = server
        self.bucket = bucket
        self.view_name = view_name
        self.design_doc_name = design_doc_name
        self.query = query
        self.expected_rows = expected_rows
        self.retry_time = retry_time
        self.timeout = 900

    def call(self):
        self.start_task()
        try:
            rest = RestConnection(self.server)
            # make sure view can be queried
            content = \
                rest.query_view(self.design_doc_name, self.view_name, self.bucket, self.query, self.timeout)

            if self.expected_rows is None:
                # no verification
                self.complete_task()
                return content
            else:
                return_value = self.check()
                self.complete_task()
                return return_value

        except QueryViewException as e:
            # initial query failed, try again
            time.sleep(self.retry_time)
            self.execute()

        # catch and set all unexpected exceptions
        except Exception as e:
            self.set_exception(e)
            self.complete_task()
            return False

    def check(self):
        try:
            rest = RestConnection(self.server)
            # query and verify expected num of rows returned
            content = \
                rest.query_view(self.design_doc_name, self.view_name, self.bucket, self.query, self.timeout)

            log.info("Server: %s, Design Doc: %s, View: %s, (%d rows) expected, (%d rows) returned" % \
                     (self.server.ip, self.design_doc_name, self.view_name, self.expected_rows, len(content['rows'])))

            raised_error = content.get(u'error', '') or ''.join([str(item) for item in content.get(u'errors', [])])
            if raised_error:
                raise QueryViewException(self.view_name, raised_error)

            if len(content['rows']) == self.expected_rows:
                log.info("expected number of rows: '{0}' was found for view query".format(self.
                                                                                          expected_rows))
                return True
            else:
                if len(content['rows']) > self.expected_rows:
                    raise QueryViewException(self.view_name,
                                             "Server: {0}, Design Doc: {1}, actual returned rows: '{2}' are greater than expected {3}"
                                             .format(self.server.ip, self.design_doc_name, len(content['rows']),
                                                     self.expected_rows, ))
                if "stale" in self.query:
                    if self.query["stale"].lower() == "false":
                        return False

                # retry until expected results or task times out
                time.sleep(self.retry_time)
                self.check()
        except QueryViewException as e:
            # subsequent query failed! exit
            self.set_exception(e)
            return False

        # catch and set all unexpected exceptions
        except Exception as e:
            self.set_exception(e)
            return False


class N1QLQueryTask(Task):
    def __init__(self,
                 server, bucket,
                 query, n1ql_helper=None,
                 expected_result=None,
                 verify_results=True,
                 is_explain_query=False,
                 index_name=None,
                 retry_time=2,
                 scan_consistency=None,
                 scan_vector=None):
        super(N1QLQueryTask, self).__init__("query_n1ql_task")
        self.server = server
        self.bucket = bucket
        self.query = query
        self.expected_result = expected_result
        self.n1ql_helper = n1ql_helper
        self.timeout = 900
        self.verify_results = verify_results
        self.is_explain_query = is_explain_query
        self.index_name = index_name
        self.retry_time = 2
        self.retried = 0
        self.scan_consistency = scan_consistency
        self.scan_vector = scan_vector

    def call(self):
        self.start_task()
        try:
            # Query and get results
            log.info(" <<<<< START Executing Query {0} >>>>>>".format(self.query))
            if not self.is_explain_query:
                self.msg, self.isSuccess = self.n1ql_helper.run_query_and_verify_result(
                    query=self.query, server=self.server, expected_result=self.expected_result,
                    scan_consistency=self.scan_consistency, scan_vector=self.scan_vector,
                    verify_results=self.verify_results)
            else:
                self.actual_result = self.n1ql_helper.run_cbq_query(query=self.query, server=self.server,
                                                                    scan_consistency=self.scan_consistency,
                                                                    scan_vector=self.scan_vector)
                log.info(self.actual_result)
            log.info(" <<<<< Done Executing Query {0} >>>>>>".format(self.query))
            return_value = self.check()
            self.complete_task()
            return return_value
        except N1QLQueryException as e:
            # initial query failed, try again
            if self.retried < self.retry_time:
                self.retried += 1
                time.sleep(self.retry_time)
                self.call()

        # catch and set all unexpected exceptions
        except Exception as e:
            self.complete_task()
            self.set_exception(e)

    def check(self):
        try:
            # Verify correctness of result set
            if self.verify_results:
                if not self.is_explain_query:
                    if not self.isSuccess:
                        log.info(" Query {0} results leads to INCORRECT RESULT ".format(self.query))
                        raise N1QLQueryException(self.msg)
                else:
                    check = self.n1ql_helper.verify_index_with_explain(self.actual_result, self.index_name)
                    if not check:
                        actual_result = self.n1ql_helper.run_cbq_query(query="select * from system:indexes",
                                                                       server=self.server)
                        log.info(actual_result)
                        raise Exception(
                            " INDEX usage in Query {0} :: NOT FOUND {1} :: as observed in result {2}".format(
                                self.query, self.index_name, self.actual_result))
            log.info(" <<<<< Done VERIFYING Query {0} >>>>>>".format(self.query))
            return True
        except N1QLQueryException as e:
            # subsequent query failed! exit
            self.set_exception(e)
        # catch and set all unexpected exceptions
        except Exception as e:
            self.set_exception(e)


class CreateIndexTask(Task):
    def __init__(self,
                 server, bucket, index_name,
                 query, n1ql_helper=None,
                 retry_time=2, defer_build=False,
                 timeout=240):
        super(CreateIndexTask, self).__init__("create_index_task")
        Task.__init__(self, "create_index_task")
        self.server = server
        self.bucket = bucket
        self.defer_build = defer_build
        self.query = query
        self.index_name = index_name
        self.n1ql_helper = n1ql_helper
        self.retry_time = 2
        self.retried = 0
        self.timeout = timeout

    def call(self):
        self.start_task()
        try:
            # Query and get results
            self.n1ql_helper.run_cbq_query(query=self.query, server=self.server)
            return_value = self.check()
            self.complete_task()
            return return_value
        except CreateIndexException as e:
            # initial query failed, try again
            if self.retried < self.retry_time:
                self.retried += 1
                time.sleep(self.retry_time)
                self.call()
        # catch and set all unexpected exceptions
        except Exception as e:
            log.error(e)
            self.set_exception(e)

    def check(self):
        try:
            # Verify correctness of result set
            check = True
            if not self.defer_build:
                check = self.n1ql_helper.is_index_online_and_in_list(self.bucket, self.index_name, server=self.server,
                                                                     timeout=self.timeout)
            if not check:
                raise CreateIndexException("Index {0} not created as expected ".format(self.index_name))
            return True
        except CreateIndexException as e:
            # subsequent query failed! exit
            log.error(e)
            self.set_exception(e)
        # catch and set all unexpected exceptions
        except Exception as e:
            log.error(e)
            self.set_exception(e)


class BuildIndexTask(Task):
    def __init__(self,
                 server, bucket,
                 query, n1ql_helper=None,
                 retry_time=2):
        super(BuildIndexTask, self).__init__("build_index_task")
        self.server = server
        self.bucket = bucket
        self.query = query
        self.n1ql_helper = n1ql_helper
        self.retry_time = 2
        self.retried = 0

    def call(self):
        self.start_task()
        try:
            # Query and get results
            self.n1ql_helper.run_cbq_query(query=self.query, server=self.server)
            return_value = self.check()
            self.complete_task()
            return return_value
        except CreateIndexException as e:
            # initial query failed, try again
            if self.retried < self.retry_time:
                self.retried += 1
                time.sleep(self.retry_time)
                self.call()

        # catch and set all unexpected exceptions
        except Exception as e:
            self.set_exception(e)

    def check(self):
        try:
            # Verify correctness of result set
            return True
        except CreateIndexException as e:
            # subsequent query failed! exit
            self.set_exception(e)
        # catch and set all unexpected exceptions
        except Exception as e:
            self.set_exception(e)


class MonitorIndexTask(Task):
    def __init__(self,
                 server, bucket, index_name,
                 n1ql_helper=None,
                 retry_time=2,
                 timeout=240):
        super(MonitorIndexTask, self).__init__("build_index_task")
        self.server = server
        self.bucket = bucket
        self.index_name = index_name
        self.n1ql_helper = n1ql_helper
        self.retry_time = 2
        self.timeout = timeout

    def call(self):
        self.start_task()
        try:
            check = self.n1ql_helper.is_index_online_and_in_list(self.bucket, self.index_name,
                                                                 server=self.server, timeout=self.timeout)
            if not check:
                raise CreateIndexException("Index {0} not created as expected ".format(self.index_name))
            return_value = self.check()
            self.complete_task()
            return return_value
        except CreateIndexException as e:
            # initial query failed, try again
            self.set_exception(e)
        # catch and set all unexpected exceptions
        except Exception as e:
            self.set_exception(e)

    def check(self):
        try:
            return True
        except CreateIndexException as e:
            # subsequent query failed! exit
            self.set_exception(e)

        # catch and set all unexpected exceptions
        except Exception as e:
            self.set_exception(e)


class DropIndexTask(Task):
    def __init__(self,
                 server, bucket, index_name,
                 query, n1ql_helper=None,
                 retry_time=2):
        super(DropIndexTask, self).__init__("drop_index_task")
        self.server = server
        self.bucket = bucket
        self.query = query
        self.index_name = index_name
        self.n1ql_helper = n1ql_helper
        self.timeout = 900
        self.retry_time = 2
        self.retried = 0

    def call(self):
        self.start_task()
        try:
            # Query and get results
            check = self.n1ql_helper._is_index_in_list(self.bucket, self.index_name, server=self.server)
            if not check:
                raise DropIndexException("index {0} does not exist will not drop".format(self.index_name))
            self.n1ql_helper.run_cbq_query(query=self.query, server=self.server)
            return_value = self.check()
        except N1QLQueryException as e:
            # initial query failed, try again
            if self.retried < self.retry_time:
                self.retried += 1
                time.sleep(self.retry_time)
                self.call()
        # catch and set all unexpected exceptions
        except DropIndexException as e:
            self.setexception(e)

    def check(self):
        try:
            # Verify correctness of result set
            check = self.n1ql_helper._is_index_in_list(self.bucket, self.index_name, server=self.server)
            if check:
                raise Exception("Index {0} not dropped as expected ".format(self.index_name))
            return True
        except DropIndexException as e:
            # subsequent query failed! exit
            self.set_exception(e)
        # catch and set all unexpected exceptions
        except Exception as e:
            self.set_exception(e)


class PrintClusterStats(Task):
    def __init__(self, node, sleep=5):
        super(PrintClusterStats, self).__init__("print_cluster_stats")
        self.node = node
        self.sleep = sleep
        self.stop_task = False

    def call(self):
        self.start_task()
        rest = RestConnection(self.node)
        while not self.stop_task:
            try:
                cluster_stat = rest.get_cluster_stats()
                log.info("------- Cluster statistics -------")
                for cluster_node, node_stats in cluster_stat.items():
                    log.info("{0} => {1}".format(cluster_node, node_stats))
                log.info("--- End of cluster statistics ---")
                time.sleep(self.sleep)
            except Exception as e:
                log.error(e.message)
                time.sleep(self.sleep)
        self.complete_task()

    def end_task(self):
        self.stop_task = True


class PrintOpsRate(Task):
    def __init__(self, cluster, bucket, sleep=1):
        super(PrintOpsRate, self).__init__("print_ops_rate{}".format(bucket.name))
        self.cluster = cluster
        self.bucket = bucket
        self.bucket_helper = BucketHelper(self.cluster.master)
        self.sleep = sleep
        self.stop_task = False

    def call(self):
        self.start_task()
        while not self.stop_task:
            bucket_stats = self.bucket_helper.fetch_bucket_stats(self.bucket)
            if 'op' in bucket_stats and 'samples' in bucket_stats['op'] and 'ops' in bucket_stats['op']['samples']:
                ops = bucket_stats['op']['samples']['ops']
                log.info("Ops/sec for bucket {} : {}".format(self.bucket.name, ops[-1]))
                time.sleep(self.sleep)
        self.complete_task()

    def end_task(self):
        self.stop_task = True


class BucketCreateTask(Task):
    def __init__(self, server, bucket):
        super(BucketCreateTask, self).__init__("bucket_create_task")
        self.server = server
        self.bucket = bucket
        if self.bucket.priority is None or self.bucket.priority.lower() is 'low':
            self.bucket_priority = 3
        else:
            self.bucket_priority = 8
        self.retries = 0

    def call(self):
        try:
            rest = RestConnection(self.server)
        except ServerUnavailableException as error:
            self.set_exception(error)
            return False
        info = rest.get_nodes_self()

        if self.bucket.ramQuotaMB <= 0:
            self.size = info.memoryQuota * 2 / 3

        if int(info.port) in xrange(9091, 9991):
            try:
                self.port = info.port
                BucketHelper(self.server).create_bucket(self.bucket.__dict__)
                # return_value = self.check()
                self.complete_task()
                return True
            except Exception as e:
                log.info(str(e))
                self.set_exception(e)
        version = rest.get_nodes_self().version
        try:
            if float(version[:2]) >= 3.0 and self.bucket_priority is not None:
                self.bucket.threadsNumber = self.bucket_priority
            BucketHelper(self.server).create_bucket(self.bucket.__dict__)
            # return_value = self.check()
            self.complete_task()
            return True
        except BucketCreationException as e:
            log.info(str(e))
            self.set_exception(e)
        # catch and set all unexpected exceptions
        except Exception as e:
            log.info(str(e))
            self.set_exception(e)

    def check(self):
        try:
            # if self.bucket.bucketType == 'memcached' or int(self.server.port) in xrange(9091, 9991):
            #     return True
            if MemcachedHelper.wait_for_memcached(self.server, self.bucket.name):
                log.info(
                    "bucket '{0}' was created with per node RAM quota: {1}".format(self.bucket, self.bucket.ramQuotaMB))
                return True
            else:
                log.warn("vbucket map not ready after try {0}".format(self.retries))
                if self.retries >= 5:
                    return False
        except Exception as e:
            log.error("Unexpected error: %s" % str(e))
            log.warn("vbucket map not ready after try {0}".format(self.retries))
            if self.retries >= 5:
                self.set_exception(e)
        self.retries = self.retris + 1
        time.sleep(5)
        self.check()


class TestTask(Callable):
    def __init__(self, timeout):
        self.thread_used = "test_task"
        self.started = None
        self.completed = None
        self.exception = None
        self.timeout = timeout

    def __str__(self):
        if self.exception:
            return "[%s] %s download error %s in %.2fs" % \
                   (self.thread_used, self.num_items, self.exception,
                    self.completed - self.started,)  # , self.result)
        elif self.completed:
            print "Time: %s" % str(time.strftime("%H:%M:%S", time.gmtime(time.time())))
            return "[%s] %s items loaded in %.2fs" % \
                   (self.thread_used, self.loaded,
                    self.completed - self.started,)  # , self.result)
        elif self.started:
            return "[%s] %s started at %s" % \
                   (self.thread_used, self.num_items, self.started)
        else:
            return "[%s] %s not yet scheduled" % \
                   (self.thread_used, self.num_items)

    def call(self):
        self.started = time.time()
        while time.time() < self.started + self.timeout:
            print "Test Task"
            time.sleep(1)
        self.completed = time.time()
        return True
