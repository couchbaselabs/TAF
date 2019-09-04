"""
Created on Sep 14, 2017

@author: riteshagarwal
"""
import zlib
import copy
import json as Json
import json as pyJson
import os
import logging
import random
import socket
import string
import time
from httplib import IncompleteRead
from BucketLib.BucketOperations import BucketHelper
from BucketLib.MemcachedOperations import MemcachedHelper
from cb_tools.cbstats import Cbstats
from couchbase_helper.document import DesignDocument
from couchbase_helper.documentgenerator import BatchedDocumentGenerator, \
    doc_generator, SubdocDocumentGenerator
from membase.api.exception import \
    N1QLQueryException, DropIndexException, CreateIndexException, \
    DesignDocCreationException, QueryViewException, ReadDocumentException, \
    RebalanceFailedException, ServerUnavailableException, \
    BucketCreationException, AutoFailoverException, GetBucketInfoFailed,\
    CompactViewFailed, SetViewInfoNotFound
from membase.api.rest_client import RestConnection
from java.util.concurrent import Callable
from java.lang import Thread
from remote.remote_util import RemoteUtilHelper, RemoteMachineShellConnection
from reactor.util.function import Tuples
import com.couchbase.test.transactions.SimpleTransaction as Transaction
import com.couchbase.client.java.json.JsonObject as JsonObject
from com.couchbase.client.java.kv import ReplicaMode
from Jython_tasks.task_manager import TaskManager
from table_view import TableView, plot_graph
from time import sleep
from couchbase_helper.durability_helper import DurableExceptions


class Task(Callable):
    def __init__(self, thread_name):
        self.thread_name = thread_name
        self.exception = None
        self.completed = False
        self.started = False
        self.start_time = None
        self.end_time = None
        self.log = logging.getLogger("infra")
        self.test_log = logging.getLogger("test")
        self.result = False

    def __str__(self):
        if self.exception:
            raise self.exception
        elif self.completed:
            self.log.debug("Task %s completed on: %s"
                           % (self.thread_name,
                              str(time.strftime("%H:%M:%S",
                                                time.gmtime(self.end_time)))))
            return "%s task completed in %.2fs" % \
                   (self.thread_name, self.completed - self.started,)
        elif self.started:
            return "Thread %s at %s" % \
                   (self.thread_name,
                    str(time.strftime("%H:%M:%S",
                                      time.gmtime(self.start_time))))
        else:
            return "[%s] not yet scheduled" % self.thread_name

    def start_task(self):
        self.started = True
        self.start_time = time.time()
        self.log.debug("Thread %s is started:" % self.thread_name)

    def set_exception(self, exception):
        self.exception = exception
        self.complete_task()
        raise BaseException(self.exception)

    def complete_task(self):
        self.completed = True
        self.end_time = time.time()
        self.log.debug("Thread %s is completed:" % self.thread_name)

    def set_result(self, result):
        self.result = result

    def call(self):
        raise NotImplementedError

    @staticmethod
    def wait_until(value_getter, condition, timeout_secs=300):
        """
        Repeatedly calls value_getter returning the value when it
        satisfies condition. Calls to value getter back off exponentially.
        Useful if you simply want to synchronously wait for a condition to be
        satisfied.

        :param value_getter: no-arg function that gets a value
        :param condition: single-arg function that tests the value
        :param timeout_secs: number of seconds after which to timeout
                             default=300 seconds (5 mins.)
        :return: the value returned by value_getter
        :raises: Exception if the operation times out before
                 getting a value that satisfies condition
        """
        start_time = time.time()
        stop_time = start_time + timeout_secs
        interval = 0.01
        attempt = 0
        value = value_getter()
        while not condition(value):
            now = time.time()
            if timeout_secs < 0 or now < stop_time:
                time.sleep(2**attempt * interval)
                attempt += 1
                value = value_getter()
            else:
                raise Exception('Timeout after {0} seconds and {1} attempts'
                                .format(now - start_time, attempt))
        return value


class RebalanceTask(Task):

    def __init__(self, servers, to_add=[], to_remove=[], do_stop=False,
                 progress=30, use_hostnames=False, services=None,
                 check_vbucket_shuffling=True):
        super(RebalanceTask, self).__init__("Rebalance_task_IN=[{}]_OUT=[{}]_{}"
                                            .format(",".join([node.ip for node in to_add]),
                                                    ",".join([node.ip for node in to_remove]),
                                                    str(time.time())))
        self.servers = servers
        self.to_add = to_add
        self.to_remove = to_remove
        self.start_time = None
        self.services = services
        self.monitor_vbuckets_shuffling = False
        self.check_vbucket_shuffling = check_vbucket_shuffling
        self.result = False
        try:
            self.rest = RestConnection(self.servers[0])
        except ServerUnavailableException, e:
            self.test_log.error(e)
            raise e
        self.retry_get_progress = 0
        self.use_hostnames = use_hostnames
        self.previous_progress = 0
        self.old_vbuckets = {}
        self.thread_used = "Rebalance_task"

        cluster_stats = self.rest.get_cluster_stats()
        self.table = TableView(self.test_log.info)
        self.table.set_headers(["Nodes", "Services", "Status"])
        node_ips_to_remove = [node.ip for node in to_remove]
        for node, stat in cluster_stats.items():
            node_ip = node.split(':')[0]
            if node_ip in node_ips_to_remove:
                self.table.add_row([node_ip,
                                    cluster_stats[node]["services"],
                                    "--- OUT --->"])
            else:
                self.table.add_row([node_ip,
                                    stat["services"],
                                    "Cluster node"])

    def __str__(self):
        if self.exception:
            return "[%s] %s download error %s in %.2fs" % \
                   (self.thread_name, self.num_items, self.exception,
                    self.completed - self.started,)  # , self.result)
        elif self.completed:
            self.test_log.debug("Time: %s"
                                % str(time.strftime("%H:%M:%S",
                                                    time.gmtime(time.time()))))
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
                        key = "{0}:{1}".format(remove_node.ip,
                                               remove_node.port)
                        services = services_map[key]
                        if "kv" not in services:
                            self.monitor_vbuckets_shuffling = False
                if self.monitor_vbuckets_shuffling:
                    self.test_log.debug("This is swap rebalance and we will monitor vbuckets shuffling")
            self.add_nodes()
            self.start_rebalance()
            self.table.display("Rebalance Overview")
            self.check()
            # self.task_manager.schedule(self)
        except Exception as e:
            self.exception = e
            self.result = False
            self.test_log.error(str(e))
            return self.result
        self.complete_task()
        self.result = True
        return self.result

    def add_nodes(self):
        master = self.servers[0]
        services_for_node = None
        node_index = 0
        for node in self.to_add:
            self.table.add_row([node.ip, services_for_node, "<--- IN ---"])
            if self.services is not None:
                services_for_node = [self.services[node_index]]
                node_index += 1
            if self.use_hostnames:
                self.rest.add_node(master.rest_username, master.rest_password,
                                   node.hostname, node.port,
                                   services=services_for_node)
            else:
                self.rest.add_node(master.rest_username, master.rest_password,
                                   node.ip, node.port,
                                   services=services_for_node)

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

        remove_node_msg = "Removing node {0}:{1} from cluster"
        ejectedNodes = list()
        for server in self.to_remove:
            for node in nodes:
                if cluster_run:
                    if int(server.port) == int(node.port):
                        ejectedNodes.append(node.id)
                        self.test_log.debug(remove_node_msg.format(node.ip,
                                                                   node.port))
                else:
                    if self.use_hostnames:
                        if server.hostname == node.ip and int(server.port) == int(node.port):
                            ejectedNodes.append(node.id)
                            self.test_log.debug(remove_node_msg
                                                .format(node.ip, node.port))
                    elif server.ip == node.ip and int(server.port) == int(node.port):
                        ejectedNodes.append(node.id)
                        self.test_log.debug(remove_node_msg.format(node.ip,
                                                                   node.port))

        if self.rest.is_cluster_mixed():
            # workaround MB-8094
            self.test_log.warning("Mixed cluster. Sleep 15secs before rebalance")
            time.sleep(15)

        self.rest.rebalance(otpNodes=[node.id for node in nodes],
                            ejectedNodes=ejectedNodes)
        self.start_time = time.time()

    def check(self):
        status = None
        progress = -100
        try:
            if self.monitor_vbuckets_shuffling:
                self.test_log.debug("This is swap rebalance and we will monitor vbuckets shuffling")
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
                            self.test_log.error(msg)
                            self.test_log.error("Vbuckets - Old: %s, New: %s"
                                                % (self.old_vbuckets,
                                                   new_vbuckets))
                            raise Exception(msg)
            (status, progress) = self.rest._rebalance_status_and_progress()
            self.test_log.info("Rebalance - status: %s, progress: %s", status,
                               progress)
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
            self.test_log.debug("Rebalance in mix cluster")
            retry_get_process_num = 40
        # we need to wait for status to be 'none'
        # (i.e. rebalance actually finished and not just 'running' and at 100%)
        # before we declare ourselves done
        if progress != -1 and status != 'none':
            if self.retry_get_progress < retry_get_process_num:
                time.sleep(10)
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
                    self.test_log.error(e)
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
                        self.test_log.error(e)

            for node in set(self.to_remove) - set(success_cleaned):
                self.test_log.error(
                    "Node {0}:{1} was not cleaned after removing from cluster"
                    .format(node.ip, node.port))
                self.result = False

            self.test_log.info(
                "Rebalance completed with progress: {0}% in {1} sec"
                .format(progress, time.time() - self.start_time))
            self.result = True
            return


class GenericLoadingTask(Task):
    def __init__(self, cluster, bucket, client, batch_size=1, pause_secs=1,
                 timeout_secs=60, compression=True,
                 retries=5,transaction=False, commit=False):
        super(GenericLoadingTask, self).__init__("Loadgen_task_{}"
                                                 .format(time.time()))
        self.batch_size = batch_size
        self.pause = pause_secs
        self.timeout = timeout_secs
        self.cluster = cluster
        self.bucket = bucket
        self.client = client
        self.random = random.Random()
        self.retries = retries

    def call(self):
        self.start_task()
        self.log.debug("Starting GenericLoadingTask thread")
        try:
            while self.has_next():
                self.next()
        except Exception as e:
            self.test_log.error(e)
            self.set_exception(Exception(e.message))
            return
        self.log.debug("Load generation thread completed")
        self.complete_task()

    def has_next(self):
        raise NotImplementedError

    def next(self):
        raise NotImplementedError

    # start of batch methods
    def batch_create(self, key_val, shared_client=None, persist_to=0,
                     replicate_to=0, timeout=5, time_unit="seconds",
                     doc_type="json", durability="", skip_read_on_error=False):
        """
        standalone method for creating key/values in batch (sans kvstore)

        arguments:
            key_val -- array of key/value dicts to load size = self.batch_size
            shared_client -- optional client to use for data loading
        """
        success = dict()
        fail = dict()
        try:
            self._process_values_for_create(key_val)
            client = shared_client or self.client
            success, fail = client.setMulti(
                key_val, self.exp, exp_unit=self.exp_unit,
                persist_to=persist_to, replicate_to=replicate_to,
                timeout=timeout, time_unit=time_unit, retry=self.retries,
                doc_type=doc_type, durability=durability)
            if fail:
                failed_item_table = TableView(self.test_log.info)
                failed_item_table.set_headers(["Create doc_Id", "Exception"])
                try:
                    Thread.sleep(timeout)
                except Exception as e:
                    self.test_log.error(e)
                if not skip_read_on_error:
                    self.test_log.debug("Reading values {0} after failure"
                                        .format(fail.keys()))
                    read_map = self.batch_read(fail.keys())
                    for key, value in fail.items():
                        if key in read_map and read_map[key]["cas"] != 0:
                            success[key] = value
                            success[key].pop("error")
                            fail.pop(key)
                        else:
                            failed_item_table.add_row([key, value['error']])
                else:
                    for key, value in fail.items():
                        failed_item_table.add_row([key, value['error']])
                if fail:
                    failed_item_table.display("Failed items:")
            return success, fail
        except Exception as error:
            self.test_log.error(error)
        return success, fail

    def batch_update(self, key_val, shared_client=None, persist_to=0,
                     replicate_to=0, timeout=5, time_unit="seconds",
                     doc_type="json", durability="", skip_read_on_error=False):
        success = dict()
        fail = dict()
        try:
            self._process_values_for_update(key_val)
            client = self.client or shared_client
            retry_docs = key_val
            success, fail = client.upsertMulti(
                retry_docs, self.exp, exp_unit=self.exp_unit,
                persist_to=persist_to, replicate_to=replicate_to,
                timeout=timeout, time_unit=time_unit,
                retry=self.retries, doc_type=doc_type, durability=durability)
            if fail:
                failed_item_table = TableView(self.test_log.info)
                failed_item_table.set_headers(["Update doc_Id", "Exception"])
                try:
                    Thread.sleep(timeout)
                except Exception as e:
                    self.test_log.error(e)
                if not skip_read_on_error:
                    self.test_log.debug("Reading values {0} after failure"
                                        .format(fail.keys()))
                    read_map = self.batch_read(fail.keys())
                    for key, value in fail.items():
                        if key in read_map and read_map[key]["cas"] != 0:
                            success[key] = value
                            success[key].pop("error")
                            fail.pop(key)
                        else:
                            failed_item_table.add_row([key, value['error']])
                else:
                    for key, value in fail.items():
                        failed_item_table.add_row([key, value['error']])
                failed_item_table.display("Failed items after reads:")
            return success, fail
        except Exception as error:
            self.test_log.error(error)
        return success, fail

    def batch_delete(self, key_val, shared_client=None, persist_to=None,
                     replicate_to=None, timeout=None, timeunit=None,
                     durability=""):
        self.client = self.client or shared_client
        success, fail = self.client.delete_multi(key_val.keys(),
                                                 persist_to=persist_to,
                                                 replicate_to=replicate_to,
                                                 timeout=timeout,
                                                 time_unit=timeunit,
                                                 durability=durability)
        if fail:
            failed_item_view = TableView(self.test_log.info)
            failed_item_view.set_headers(["Delete doc_Id", "Exception"])
            for key, exception in fail.items():
                failed_item_view.add_row([key, exception])
            failed_item_view.display("Delete failed details")
        return success, fail

    def batch_read(self, keys, shared_client=None):
        success = dict()
        fail = dict()
        self.client = self.client or shared_client
        try:
            success, fail = self.client.getMulti(keys)
        except Exception as error:
            self.set_exception(error)
        return success, fail

    def batch_sub_doc_insert(self, key_value, persist_to=0,
                             replicate_to=0, timeout=5,
                             time_unit="seconds",
                             durability="",
                             create_path=True, xattr=False):
        success = dict()
        fail = dict()
        try:
            success, fail = self.client.sub_doc_insert_multi(
                key_value,
                exp=self.exp,
                exp_unit=self.exp_unit,
                persist_to=persist_to,
                replicate_to=replicate_to,
                timeout=timeout,
                time_unit=time_unit,
                durability=durability,
                create_path=create_path,
                xattr=xattr)
        except Exception as error:
            self.log.error(error)
            self.set_exception("Exception during sub_doc insert: {0}"
                               .format(error))
        return success, fail

    def batch_sub_doc_upsert(self, key_value, persist_to=0,
                             replicate_to=0, timeout=5,
                             time_unit="seconds",
                             durability="",
                             create_path=True, xattr=False):
        success = dict()
        fail = dict()
        try:
            success, fail = self.client.sub_doc_upsert_multi(
                key_value,
                exp=self.exp,
                exp_unit=self.exp_unit,
                persist_to=persist_to,
                replicate_to=replicate_to,
                timeout=timeout,
                time_unit=time_unit,
                durability=durability,
                create_path=create_path,
                xattr=xattr)
        except Exception as error:
            self.log.error(error)
            self.set_exception("Exception during sub_doc upsert: {0}"
                               .format(error))
        return success, fail

    def batch_sub_doc_replace(self, key_value, persist_to=0,
                              replicate_to=0, timeout=5,
                              time_unit="seconds",
                              durability="",
                              xattr=False):
        success = dict()
        fail = dict()
        try:
            success, fail = self.client.sub_doc_replace_multi(
                key_value,
                exp=self.exp,
                exp_unit=self.exp_unit,
                persist_to=persist_to,
                replicate_to=replicate_to,
                timeout=timeout,
                time_unit=time_unit,
                durability=durability,
                xattr=xattr)
        except Exception as error:
            self.log.error(error)
            self.set_exception("Exception during sub_doc upsert: {0}"
                               .format(error))
        return success, fail

    def batch_sub_doc_remove(self, key_value, persist_to=0,
                             replicate_to=0, timeout=5,
                             time_unit="seconds",
                             durability="",
                             xattr=False):
        success = dict()
        fail = dict()
        try:
            success, fail = self.client.sub_doc_remove_multi(
                key_value,
                exp=self.exp,
                exp_unit=self.exp_unit,
                persist_to=persist_to,
                replicate_to=replicate_to,
                timeout=timeout,
                time_unit=time_unit,
                durability=durability,
                xattr=xattr)
        except Exception as error:
            self.log.error(error)
            self.set_exception("Exception during sub_doc remove: {0}"
                               .format(error))
        return success, fail

    def batch_sub_doc_read(self, key_value, timeout=5, time_unit="seconds"):
        success = dict()
        fail = dict()
        try:
            success, fail = self.client.sub_doc_read_multi(
                key_value,
                timeout=timeout,
                time_unit=time_unit)
        except Exception as error:
            self.log.error(error)
            self.set_exception("Exception during sub_doc read: {0}"
                               .format(error))
        return success, fail

    def _process_values_for_create(self, key_val):
        for key, value in key_val.items():
            try:
                value_json = Json.loads(value)
                value_json['mutated'] = 0
                value = Json.dumps(value_json)
            except ValueError:
                value = Json.dumps(value)
            except TypeError:
                self.random.seed(key)
                index = self.random.choice(range(len(value)))
                value = value[0:index] + self.random.choice(string.ascii_uppercase) + value[index + 1:]
            except Exception, e:
                self.test_log.error(e)
            finally:
                key_val[key] = value

    def _process_values_for_update(self, key_val):
        self._process_values_for_create(key_val)
        for key, value in key_val.items():
            try:
                # new updated value, however it is not their in orginal "LoadDocumentsTask"
                value = key_val[key]
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

    def __init__(self, cluster, bucket, client, generator, op_type, exp,
                 exp_unit="seconds", flag=0,
                 persist_to=0, replicate_to=0, time_unit="seconds",
                 proxy_client=None, batch_size=1, pause_secs=1, timeout_secs=5,
                 compression=True, retries=5,
                 durability="", task_identifier="", skip_read_on_error=False):

        super(LoadDocumentsTask, self).__init__(
            cluster, bucket, client, batch_size=batch_size,
            pause_secs=pause_secs, timeout_secs=timeout_secs,
            compression=compression,
            retries=retries)
        self.thread_name = "LoadDocumentsTask-{}_{}_{}_{}_{}" \
            .format(task_identifier,
                    generator._doc_gen.start,
                    generator._doc_gen.end,
                    op_type,
                    durability)
        self.generator = generator
        self.op_type = op_type
        self.exp = exp
        self.exp_unit = exp_unit
        self.flag = flag
        self.persist_to = persist_to
        self.replicate_to = replicate_to
        self.time_unit = time_unit
        self.num_loaded = 0
        self.durability = durability
        self.fail = {}
        self.success = {}
        self.docs_loaded = 0
        self.skip_read_on_error = skip_read_on_error

        if proxy_client:
            self.log.debug("Changing client to proxy %s:%s..."
                           % (proxy_client.host, proxy_client.port))
            self.client = proxy_client

    def has_next(self):
        return self.generator.has_next()

    def next(self, override_generator=None):
        doc_gen = override_generator or self.generator
        key_value = doc_gen.next_batch()
        if self.op_type == 'create':
            success, fail = self.batch_create(
                key_value,
                persist_to=self.persist_to, replicate_to=self.replicate_to,
                timeout=self.timeout, time_unit=self.time_unit,
                doc_type=self.generator.doc_type, durability=self.durability,
                skip_read_on_error=self.skip_read_on_error)
            self.fail.update(fail)
            self.success.update(success)
        elif self.op_type == 'update':
            success, fail = self.batch_update(
                key_value,
                persist_to=self.persist_to,
                replicate_to=self.replicate_to,
                timeout=self.timeout,
                time_unit=self.time_unit,
                doc_type=self.generator.doc_type,
                durability=self.durability,
                skip_read_on_error=self.skip_read_on_error)
            self.fail.update(fail)
            self.success.update(success)
        elif self.op_type == 'delete':
            success, fail = self.batch_delete(key_value,
                                              persist_to=self.persist_to,
                                              replicate_to=self.replicate_to,
                                              timeout=self.timeout,
                                              timeunit=self.time_unit,
                                              durability=self.durability)
            self.fail.update(fail)
            self.success.update(success)
        elif self.op_type == 'read':
            success, fail = self.batch_read(key_value.keys())
            self.fail.update(fail)
            self.success.update(success)
        else:
            self.set_exception(Exception("Bad operation: %s" % self.op_type))
        self.docs_loaded += len(key_value)


class LoadSubDocumentsTask(GenericLoadingTask):
    def __init__(self, cluster, bucket, client, generator,
                 op_type, exp, create_paths=False,
                 xattr=False,
                 exp_unit="seconds", flag=0,
                 persist_to=0, replicate_to=0, time_unit="seconds",
                 batch_size=1, pause_secs=1, timeout_secs=5,
                 compression=True, retries=5,
                 durability="", task_identifier=""):
        super(LoadSubDocumentsTask, self).__init__(
            cluster, bucket, client, batch_size=batch_size,
            pause_secs=pause_secs, timeout_secs=timeout_secs,
            compression=compression,
            retries=retries)
        self.thread_name = "LoadSubDocsTask-{}_{}_{}_{}_{}" \
            .format(task_identifier,
                    generator._doc_gen.start,
                    generator._doc_gen.end,
                    op_type,
                    durability)
        self.generator = generator
        self.op_type = op_type
        self.exp = exp
        self.create_path = create_paths
        self.xattr = xattr
        self.exp_unit = exp_unit
        self.flag = flag
        self.persist_to = persist_to
        self.replicate_to = replicate_to
        self.time_unit = time_unit
        self.num_loaded = 0
        self.durability = durability
        self.fail = {}
        self.success = {}

    def has_next(self):
        return self.generator.has_next()

    def next(self, override_generator=None):
        doc_gen = override_generator or self.generator
        key_value = doc_gen.next_batch()
        if self.op_type == 'insert':
            success, fail = self.batch_sub_doc_insert(
                key_value,
                persist_to=self.persist_to,
                replicate_to=self.replicate_to,
                timeout=self.timeout,
                time_unit=self.time_unit,
                durability=self.durability,
                create_path=self.create_path,
                xattr=self.xattr)
            self.fail.update(fail)
            self.success.update(success)
        elif self.op_type == 'upsert':
            success, fail = self.batch_sub_doc_upsert(
                key_value,
                persist_to=self.persist_to,
                replicate_to=self.replicate_to,
                timeout=self.timeout,
                time_unit=self.time_unit,
                durability=self.durability,
                create_path=self.create_path,
                xattr=self.xattr)
            self.fail.update(fail)
            self.success.update(success)
        elif self.op_type == 'remove':
            success, fail = self.batch_sub_doc_remove(
                key_value,
                persist_to=self.persist_to,
                replicate_to=self.replicate_to,
                timeout=self.timeout,
                time_unit=self.time_unit,
                durability=self.durability,
                xattr=self.xattr)
            self.fail.update(fail)
            self.success.update(success)
        elif self.op_type == "replace":
            success, fail = self.batch_sub_doc_replace(
                key_value,
                persist_to=self.persist_to,
                replicate_to=self.replicate_to,
                timeout=self.timeout,
                time_unit=self.time_unit,
                durability=self.durability,
                xattr=self.xattr)
            self.fail.update(fail)
            self.success.update(success)
        elif self.op_type == 'read':
            success, fail = self.batch_sub_doc_read(key_value,
                                                    timeout=self.timeout,
                                                    time_unit=self.time_unit)
            self.fail.update(fail)
            self.success.update(success)
        else:
            self.set_exception(Exception("Bad operation type: %s"
                                         % self.op_type))


class Durability(Task):

    def __init__(self, cluster, task_manager, bucket, clients, generator,
                 op_type, exp, exp_unit="seconds", flag=0,
                 persist_to=0, replicate_to=0, time_unit="seconds",
                 only_store_hash=True, batch_size=1, pause_secs=1,
                 timeout_secs=5, compression=True, process_concurrency=8,
                 print_ops_rate=True, retries=5, durability="",
                 majority_value=0, check_persistence=False):

        super(Durability, self).__init__("DurabilityDocumentsMainTask{}"
                                         .format(time.time()))
        self.majority_value = majority_value
        self.fail = {}
        self.success = {}
        self.create_failed = {}
        self.update_failed = {}
        self.delete_failed = {}
        self.sdk_acked_curd_failed = {}
        self.sdk_exception_crud_succeed = {}
        self.sdk_acked_pers_failed = {}
        self.sdk_exception_pers_succeed = {}
        self.cluster = cluster
        self.exp = exp
        self.exp_unit = exp_unit
        self.durability = durability
        self.check_persistence = check_persistence
        self.flag = flag
        self.persit_to = persist_to
        self.replicate_to = replicate_to
        self.time_unit = time_unit
        self.only_store_hash = only_store_hash
        self.pause_secs = pause_secs
        self.timeout_secs = timeout_secs
        self.compression = compression
        self.process_concurrency = process_concurrency
        self.clients = clients
        self.task_manager = task_manager
        self.batch_size = batch_size
        self.generator = generator
        self.op_types = None
        self.buckets = None
        self.print_ops_rate = print_ops_rate
        self.retries = retries
        if isinstance(op_type, list):
            self.op_types = op_type
        else:
            self.op_type = op_type
        if isinstance(bucket, list):
            self.buckets = bucket
        else:
            self.bucket = bucket

    def call(self):
        generators = []
        self.tasks = []
        gen_start = int(self.generator.start)
        gen_end = max(int(self.generator.end), 1)
        gen_range = max(int((self.generator.end - self.generator.start) / self.process_concurrency), 1)
        for pos in range(gen_start, gen_end, gen_range):
            partition_gen = copy.deepcopy(self.generator)
            partition_gen.start = pos
            partition_gen.itr = pos
            partition_gen.end = pos + gen_range
            if partition_gen.end > self.generator.end:
                partition_gen.end = self.generator.end
            batch_gen = BatchedDocumentGenerator(
                partition_gen,
                self.batch_size)
            generators.append(batch_gen)
        i = 0
        for generator in generators:
            task = self.Loader(
                self.cluster, self.bucket, self.clients[i], generator,
                self.op_type, self.exp, self.exp_unit, self.flag,
                majority_value=self.majority_value,
                persist_to=self.persit_to, replicate_to=self.replicate_to,
                time_unit=self.time_unit, batch_size=self.batch_size,
                pause_secs=self.pause_secs, timeout_secs=self.timeout_secs,
                compression=self.compression,
                instance_num=i, durability=self.durability,
                check_persistence=self.check_persistence)
            self.tasks.append(task)
            i += 1
        try:
            for task in self.tasks:
                self.task_manager.add_new_task(task)
            for task in self.tasks:
                self.task_manager.get_task_result(task)
                if task.__class__ == self.Loader:
                    self.create_failed.update(task.create_failed)
                    self.update_failed.update(task.update_failed)
                    self.delete_failed.update(task.delete_failed)
                    self.sdk_acked_curd_failed.update(task.sdk_acked_curd_failed)
                    self.sdk_exception_crud_succeed.update(task.sdk_exception_crud_succeed)
                    self.sdk_acked_pers_failed.update(task.sdk_acked_pers_failed)
                    self.sdk_exception_pers_succeed.update(task.sdk_exception_pers_succeed)
        except Exception as e:
            self.set_exception(e)
        finally:
            self.log.debug("=== Tasks in DurabilityDocumentsMainTask pool ===")
            self.task_manager.print_tasks_in_pool()
            self.log.debug("=================================================")
            for task in self.tasks:
                self.task_manager.get_task_result(task)
                self.task_manager.stop_task(task)
            for client in self.clients:
                client.close()

    class Loader(GenericLoadingTask):
        '''
        1. Start inserting data into buckets
        2. Keep updating the write offset
        3. Start the reader thread
        4. Keep track of non durable documents
        '''

        def __init__(self, cluster, bucket, client, generator, op_type,
                     exp, exp_unit,
                     flag=0, majority_value=0, persist_to=0, replicate_to=0,
                     time_unit="seconds",
                     batch_size=1, pause_secs=1, timeout_secs=5,
                     compression=True, retries=5,
                     instance_num=0, durability="", check_persistence=False):
            super(Durability.Loader, self).__init__(
                cluster, bucket, client, batch_size=batch_size,
                pause_secs=pause_secs, timeout_secs=timeout_secs,
                compression=compression,
                retries=retries)
            self.thread_name = "DurabilityDocumentLoaderTask" + str(instance_num)
            self.generator = generator
            self.op_type = op_type
            self.exp = exp
            self.exp_unit = exp_unit
            self.flag = flag
            self.persist_to = persist_to
            self.replicate_to = replicate_to
            self.time_unit = time_unit
            self.instance = instance_num
            self.durability = durability
            self.check_persistence = check_persistence
            self.bucket = bucket
            self.tasks = []
            self.write_offset = self.generator._doc_gen.start
            self.majority_value = majority_value
            self.create_failed = {}
            self.update_failed = {}
            self.delete_failed = {}
            self.docs_to_be_updated = {}
            self.docs_to_be_deleted = {}
            self.sdk_acked_curd_failed = {}
            self.sdk_exception_crud_succeed = {}
            self.sdk_acked_pers_failed = {}
            self.sdk_exception_pers_succeed = {}
            self.test_log.debug("Docs start loading from {0}"
                                .format(generator._doc_gen.start))
            self.log.debug("Instance num {0}".format(self.instance))
            self.task_manager = TaskManager()

        def call(self):
            self.start_task()
            import threading
            if self.check_persistence:
                persistence = threading.Thread(target=self.Persistence)
                persistence.start()
            reader = threading.Thread(target=self.Reader)
            reader.start()

            self.log.debug("Starting load generation thread")
            try:
                while self.has_next():
                    self.next()
            except Exception as e:
                self.set_exception(Exception(e.message))
            self.log.debug("Load generation thread completed")

            self.log.debug("== Tasks in DurabilityDocumentLoaderTask pool ==")
            self.task_manager.print_tasks_in_pool()
            if self.check_persistence:
                persistence.join()
            reader.join()
            self.complete_task()

        def has_next(self):
            return self.generator.has_next()

        def next(self, override_generator=None):
            doc_gen = override_generator or self.generator
            key_value = doc_gen.next_batch()
            if self.op_type == 'create':
                _, f_docs = self.batch_create(
                    key_value, persist_to=self.persist_to,
                    replicate_to=self.replicate_to, timeout=self.timeout,
                    time_unit=self.time_unit,
                    doc_type=self.generator.doc_type,
                    durability=self.durability)

                if len(f_docs) > 0:
                    self.create_failed.update(f_docs)
            elif self.op_type == 'update':
                self.docs_to_be_updated.update(
                    self.batch_read(key_value.keys())[0])
                _, f_docs = self.batch_update(
                    key_value, persist_to=self.persist_to,
                    replicate_to=self.replicate_to, timeout=self.timeout,
                    durability=self.durability,
                    time_unit=self.time_unit,
                    doc_type=self.generator.doc_type)
                self.update_failed.update(f_docs)
            elif self.op_type == 'delete':
                self.docs_to_be_deleted.update(
                    self.batch_read(key_value.keys())[0])
                success, fail = self.batch_delete(
                    key_value,
                    persist_to=self.persist_to,
                    replicate_to=self.replicate_to,
                    timeout=self.timeout,
                    timeunit=self.time_unit,
                    durability=self.durability)
                self.delete_failed.update(fail)
            else:
                self.set_exception(Exception("Bad operation type: %s"
                                             % self.op_type))
            self.write_offset += len(key_value)

        def Persistence(self):
            partition_gen = copy.deepcopy(self.generator._doc_gen)
            partition_gen.start = self.generator._doc_gen.start
            partition_gen.itr = self.generator._doc_gen.start
            partition_gen.end = self.generator._doc_gen.end

            self.generator_persist = BatchedDocumentGenerator(
                partition_gen,
                self.batch_size)

            self.start = self.generator._doc_gen.start
            self.end = self.generator._doc_gen.end
            self.generator_persist._doc_gen.itr = self.generator_persist._doc_gen.start
            self.persistence_offset = self.generator_persist._doc_gen.start
            shells = {}

            for server in self.cluster.servers:
                shell = RemoteMachineShellConnection(server)
                shells.update({server.ip: Cbstats(shell)})

            while True:
                if self.persistence_offset < self.write_offset or self.persistence_offset == self.end:
                    self.test_log.debug("Persistence: ReadOffset=%s, WriteOffset=%s, Reader: FinalOffset=%s"
                                        % (self.persistence_offset,
                                           self.write_offset,
                                           self.end))
                    if self.generator_persist._doc_gen.has_next():
                        doc = self.generator_persist._doc_gen.next()
                        key, val = doc[0], doc[1]
                        vBucket = (((zlib.crc32(key)) >> 16) & 0x7fff) & (len(self.bucket.vbuckets)-1)
                        nodes = [self.bucket.vbuckets[vBucket].master]
                        if self.durability == "PERSIST_TO_MAJORITY":
                            nodes += self.bucket.vbuckets[vBucket].replica
                        count = 0
                        if self.op_type == 'create':
                            try:
                                for node in nodes:
                                    key_is_dirty = shells[node.split(":")[0]].vkey_stat(self.bucket.name, key, "key_is_dirty")
                                    if key_is_dirty in ["true", "True", "TRUE"]:
                                        self.test_log.error("Node: %s, Key: %s, key_is_dirty = %s"%(node.split(":")[0], key, key_is_dirty))
                                    else:
                                        self.test_log.info("Node: %s, Key: %s, key_is_dirty = %s"%(node.split(":")[0], key, key_is_dirty))
                                        count += 1
                            except:
                                pass
                            if key not in self.create_failed.keys():
                                '''
                                this condition make sure that document is
                                persisted in alleast 1 node
                                '''
                                if count == 0:
                                    # if count < self.majority_value:
                                    self.sdk_acked_pers_failed.update({key: val})
                                    self.test_log.error("Key isn't persisted although SDK reports Durable, Key = %s"%key)
                            elif count > 0:
                                self.test_log.error("SDK threw exception but document is present in the Server -> %s"%key)
                                self.sdk_exception_crud_succeed.update({key: val})
                            else:
                                self.test_log.error("Document is rolled back to nothing during create -> %s"%(key))

                        if self.op_type == 'update':
                            if key not in self.update_failed[self.instance].keys():
                                try:
                                    for node in nodes:
                                        key_is_dirty = shells[node.split(":")[0]].vkey_stat(self.bucket.name, key, "key_is_dirty")
                                        if key_is_dirty in ["true", "True", "TRUE"]:
                                            self.test_log.error("Node: %s, Key: %s, key_is_dirty = %s"%(node.split(":")[0], key, key_is_dirty))
                                        else:
                                            self.test_log.debug("Node: %s, Key: %s, key_is_dirty = %s"%(node.split(":")[0], key, key_is_dirty))
                                            count += 1
                                except:
                                    pass
                                if not count > 0:
                                # if count < self.majority_value:
                                    self.sdk_acked_pers_failed.update({key: val})
                                    self.test_log.error("Key isn't persisted although SDK reports Durable, Key = %s getfromReplica = %s"%key)
                        if self.op_type == 'delete':
                            for node in nodes:
                                try:
                                    key_is_dirty = shells[node.split(":")[0]].vkey_stat(self.bucket.name, key, "key_is_dirty")
                                    if key_is_dirty in ["true", "True", "TRUE"]:
                                        self.test_log.error("Node: %s, Key: %s, key_is_dirty = %s"%(node.split(":")[0], key, key_is_dirty))
                                    else:
                                        self.test_log.debug("Node: %s, Key: %s, key_is_dirty = %s"%(node.split(":")[0], key, key_is_dirty))
                                        count += 1
                                except Exception as e:
                                    pass
                            if key not in self.delete_failed[self.instance].keys():
                                if count == (self.bucket.replicaNumber+1):
                                # if count > (self.bucket.replicaNumber+1 - self.majority_value):
                                    self.sdk_acked_pers_failed.update({key: val})
                                    self.test_log.error("Key isn't Persisted-Delete although SDK reports Durable, Key = %s getfromReplica = %s"%key)
                            elif count >= self.majority_value:
                                self.test_log.error("Document is rolled back to original during delete -> %s"%(key))

                    if self.persistence_offset == self.end:
                        self.log.info("BREAKING PERSISTENCE!!")
                        break
                    self.persistence_offset = self.write_offset

            for key, cbstat in shells.items():
                cbstat.shellConn.disconnect()

        def Reader(self):
            partition_gen = copy.deepcopy(self.generator._doc_gen)
            partition_gen.start = self.generator._doc_gen.start
            partition_gen.itr = self.generator._doc_gen.start
            partition_gen.end = self.generator._doc_gen.end

            self.generator_reader = BatchedDocumentGenerator(
                                    partition_gen,
                                    self.batch_size)

            self.start = self.generator._doc_gen.start
            self.end = self.generator._doc_gen.end
            self.read_offset = self.generator._doc_gen.start
            while True:
                if self.read_offset < self.write_offset or self.read_offset == self.end:
                    self.test_log.debug(
                        "Reader: ReadOffset=%s, %sOffset=%s, Reader: FinalOffset=%s"
                        % (self.read_offset, self.op_type, self.write_offset, self.end))
                    if self.generator_reader._doc_gen.has_next():
                        doc = self.generator_reader._doc_gen.next()
                        key, val = doc[0], doc[1]
                        if self.op_type == 'create':
                            result = self.client.getFromReplica(key, ReplicaMode.ALL)
                            self.test_log.debug("Key = %s getfromReplica = %s" % (key, result))
                            if key not in self.create_failed.keys():
                                if len(result) == 0:
                                    # if len(result) < self.majority_value:
                                    self.sdk_acked_curd_failed.update({key: val})
                                    self.test_log.error(
                                        "Key isn't durable although SDK reports Durable, Key = %s getfromReplica = %s"
                                        % (key, result))
                            elif len(result) > 0:
                                if not (DurableExceptions.DurabilityAmbiguousException in self.create_failed[key]["error"] or
                                   DurableExceptions.RequestTimeoutException in self.create_failed[key]["error"]):
                                    self.test_log.error(
                                        "SDK threw exception but document is present in the Server -> %s:%s"
                                        % (key, result))
                                    self.sdk_exception_crud_succeed.update({key: val})
                            else:
                                self.test_log.debug(
                                    "Document is rolled back to nothing during create -> %s:%s"
                                    % (key, result))
                        if self.op_type == 'update':
                            result = self.client.getFromReplica(key, ReplicaMode.ALL)
                            if key not in self.update_failed.keys():
                                if len(result) == 0:
                                    # if len(result) < self.majority_value:
                                    self.sdk_acked_curd_failed.update({key: val})
                                    self.test_log.error(
                                        "Key isn't durable although SDK reports Durable, Key = %s getfromReplica = %s"
                                        % (key, result))
                                else:
                                    # elif len(result) >= self.majority_value:
                                    temp_count = 0
                                    for doc in result:
                                        if doc["value"] == self.docs_to_be_updated[key]["value"]:
                                            self.test_log.error(
                                                "Doc content is not updated yet on few nodes, Key = %s getfromReplica = %s"
                                                % (key, result))
                                        else:
                                            temp_count += 1
                                    '''
                                    This make sure that value has been updated 
                                    on at least 1 node
                                    '''
                                    if temp_count == 0:
                                        # if temp_count < self.majority_value:
                                        self.sdk_acked_curd_failed.update({key: val})
                            else:
                                for doc in result:
                                    if doc["value"] != self.docs_to_be_updated[key]["value"]:
                                        self.sdk_exception_crud_succeed.update({key: val})
                                        self.test_log.error(
                                            "Doc content is updated although SDK threw exception, Key = %s getfromReplica = %s"
                                            % (key, result))
                        if self.op_type == 'delete':
                            result = self.client.getFromReplica(key, ReplicaMode.ALL)
                            if key not in self.delete_failed.keys():
                                if len(result) > (self.bucket.replicaNumber):
                                    self.sdk_acked_curd_failed.update(result[0])
                                    self.test_log.error(
                                        "Key isn't durably deleted although SDK reports Durable, Key = %s getfromReplica = %s"
                                        % (key, result))
                            elif len(result) == self.bucket.replicaNumber + 1:
                                self.test_log.warn(
                                    "Document is rolled back to original during delete -> %s:%s"
                                    %(key, result))
                    if self.read_offset == self.end:
                        self.test_log.fatal("BREAKING!!")
                        break
                    self.read_offset += 1


class LoadDocumentsGeneratorsTask(Task):

    def __init__(self, cluster, task_manager, bucket, clients, generators,
                 op_type, exp, exp_unit="seconds", flag=0,
                 persist_to=0, replicate_to=0, time_unit="seconds",
                 only_store_hash=True, batch_size=1, pause_secs=1,
                 timeout_secs=5, compression=True, process_concurrency=8,
                 print_ops_rate=True, retries=5, durability="",
                 task_identifier="", skip_read_on_error=False):
        super(LoadDocumentsGeneratorsTask, self).__init__(
            "DocumentsLoadGenTask_{}_{}".format(task_identifier, time.time()))
        self.cluster = cluster
        self.exp = exp
        self.exp_unit = exp_unit
        self.flag = flag
        self.persit_to = persist_to
        self.replicate_to = replicate_to
        self.time_unit = time_unit
        self.only_store_hash = only_store_hash
        self.pause_secs = pause_secs
        self.timeout_secs = timeout_secs
        self.compression = compression
        self.process_concurrency = process_concurrency
        self.clients = clients
        self.task_manager = task_manager
        self.batch_size = batch_size
        self.generators = generators
        self.input_generators = generators
        self.op_types = None
        self.buckets = None
        self.print_ops_rate = print_ops_rate
        self.retries = retries
        self.durability = durability
        self.task_identifier = task_identifier
        self.skip_read_on_error = skip_read_on_error
        if isinstance(op_type, list):
            self.op_types = op_type
        else:
            self.op_type = op_type
        if isinstance(bucket, list):
            self.buckets = bucket
        else:
            self.bucket = bucket
        self.num_loaded = 0
        self.fail = {}
        self.success = {}

    def call(self):
        self.start_task()
        if self.op_types:
            if len(self.op_types) != len(self.generators):
                self.set_exception(
                    Exception("Not all generators have op_type!"))
                self.complete_task()
        if self.buckets:
            if len(self.op_types) != len(self.buckets):
                self.set_exception(
                    Exception("Not all generators have bucket specified!"))
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
            self.print_ops_rate_tasks = list()
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
                try:
                    self.task_manager.get_task_result(task)
                    self.log.info("Items loaded in task {} are {}"
                                  .format(task.thread_name, task.docs_loaded))
                    i = 0
                    while task.docs_loaded < task.generator._doc_gen.end - task.generator._doc_gen.start and i < 60:
                        self.log.error("Bug in java Futures task. Items loaded in task {} is {}".format(task.thread_name, task.docs_loaded))
                        sleep(1)
                        i += 1
                except Exception as e:
                    self.test_log.error(e)
                finally:
                    self.fail.update(task.fail)
                    self.success.update(task.success)
                    if task.fail.__len__() != 0:
                        target_log = self.test_log.error
                    else:
                        target_log = self.test_log.debug
                    target_log("Failed to load {} docs from {} to {}"
                               .format(task.fail.__len__(),
                                       task.generator._doc_gen.start,
                                       task.generator._doc_gen.end))
        except Exception as e:
            self.test_log.error(e)
            self.set_exception(e)
        finally:
            if self.print_ops_rate and hasattr(self, "print_ops_rate_tasks"):
                for print_ops_rate_task in self.print_ops_rate_tasks:
                    print_ops_rate_task.end_task()
                    self.task_manager.get_task_result(print_ops_rate_task)
            self.log.debug("========= Tasks in loadgen pool=======")
            self.task_manager.print_tasks_in_pool()
            self.log.debug("======================================")
            for task in tasks:
                self.task_manager.stop_task(task)
                self.log.info("Task '{0}' complete. Loaded {1} items"
                              .format(task.thread_name, task.docs_loaded))
            for client in self.clients:
                client.close()
        self.complete_task()
        return self.fail

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
        for i in range(0, len(generators)):
            task = LoadDocumentsTask(
                self.cluster, self.bucket, self.clients[i], generators[i],
                self.op_type, self.exp, self.exp_unit, self.flag,
                persist_to=self.persit_to, replicate_to=self.replicate_to,
                time_unit=self.time_unit, batch_size=self.batch_size,
                pause_secs=self.pause_secs, timeout_secs=self.timeout_secs,
                compression=self.compression,
                durability=self.durability,
                task_identifier=self.task_identifier,
                skip_read_on_error=self.skip_read_on_error)
            tasks.append(task)
        return tasks


class LoadSubDocumentsGeneratorsTask(Task):

    def __init__(self, cluster, task_manager, bucket, clients,
                 generators,
                 op_type, exp, create_paths=False,
                 xattr=False, exp_unit="seconds", flag=0,
                 persist_to=0, replicate_to=0, time_unit="seconds",
                 only_store_hash=True, batch_size=1, pause_secs=1,
                 timeout_secs=5, compression=True,
                 process_concurrency=8,
                 print_ops_rate=True, retries=5, durability="",
                 task_identifier=""):
        thread_name = "SubDocumentsLoadGenTask_{}_{}_{}_{}_{}" \
                      .format(task_identifier,
                              bucket.name,
                              op_type,
                              durability,
                              time.time())
        super(LoadSubDocumentsGeneratorsTask, self).__init__(thread_name)
        self.cluster = cluster
        self.exp = exp
        self.create_path = create_paths
        self.xattr = xattr
        self.exp_unit = exp_unit
        self.flag = flag
        self.persit_to = persist_to
        self.replicate_to = replicate_to
        self.time_unit = time_unit
        self.only_store_hash = only_store_hash
        self.pause_secs = pause_secs
        self.timeout_secs = timeout_secs
        self.compression = compression
        self.process_concurrency = process_concurrency
        self.clients = clients
        self.task_manager = task_manager
        self.batch_size = batch_size
        self.generators = generators
        self.input_generators = generators
        self.op_types = None
        self.buckets = None
        self.print_ops_rate = print_ops_rate
        self.retries = retries
        self.durability = durability
        if isinstance(op_type, list):
            self.op_types = op_type
        else:
            self.op_type = op_type
        if isinstance(bucket, list):
            self.buckets = bucket
        else:
            self.bucket = bucket
        self.num_loaded = 0
        self.fail = {}
        self.success = {}

    def call(self):
        self.start_task()
        if self.op_types:
            if len(self.op_types) != len(self.generators):
                self.set_exception(
                    Exception("Not all generators have op_type!"))
                self.complete_task()
        if self.buckets:
            if len(self.op_types) != len(self.buckets):
                self.set_exception(
                    Exception(
                        "Not all generators have bucket specified!"))
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
                    print_ops_rate_task = PrintOpsRate(self.cluster,
                                                       bucket)
                    self.print_ops_rate_tasks.append(
                        print_ops_rate_task)
                    self.task_manager.add_new_task(print_ops_rate_task)
            else:
                print_ops_rate_task = PrintOpsRate(self.cluster,
                                                   self.bucket)
                self.print_ops_rate_tasks.append(print_ops_rate_task)
                self.task_manager.add_new_task(print_ops_rate_task)
        try:
            for task in tasks:
                self.task_manager.add_new_task(task)
            for task in tasks:
                try:
                    self.task_manager.get_task_result(task)
                except Exception as e:
                    self.log.error(e)
                finally:
                    self.fail.update(task.fail)
                    self.success.update(task.success)
                    self.log.debug("Failed to load {} sub_docs from {} "
                                   "to {}"
                                   .format(task.fail.__len__(),
                                           task.generator._doc_gen.start,
                                           task.generator._doc_gen.end))
        except Exception as e:
            self.log.error(e)
            self.set_exception(e)
        finally:
            if self.print_ops_rate and hasattr(self,
                                               "print_ops_rate_tasks"):
                for print_ops_rate_task in self.print_ops_rate_tasks:
                    print_ops_rate_task.end_task()
                    self.task_manager.get_task_result(
                        print_ops_rate_task)
            self.log.debug("===========Tasks in loadgen pool=======")
            self.task_manager.print_tasks_in_pool()
            self.log.debug("=======================================")
            for task in tasks:
                self.task_manager.stop_task(task)
            for client in self.clients:
                client.close()
        self.complete_task()
        return self.fail

    def get_tasks(self, generator):
        generators = []
        tasks = []
        gen_start = int(generator.start)
        gen_end = max(int(generator.end), 1)
        gen_range = max(int((generator.end - generator.start) /
                            self.process_concurrency),
                        1)
        for pos in range(gen_start, gen_end, gen_range):
            if not isinstance(generator, SubdocDocumentGenerator):
                self.set_exception("Document generator needs to be of"
                                   " type SubdocDocumentGenerator")
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
        for i in range(0, len(generators)):
            task = LoadSubDocumentsTask(self.cluster, self.bucket,
                                        self.clients[i], generators[i],
                                        self.op_type, self.exp,
                                        create_paths=self.create_path,
                                        xattr=self.xattr,
                                        exp_unit=self.exp_unit,
                                        flag=self.flag,
                                        persist_to=self.persit_to,
                                        replicate_to=self.replicate_to,
                                        time_unit=self.time_unit,
                                        batch_size=self.batch_size,
                                        pause_secs=self.pause_secs,
                                        timeout_secs=self.timeout_secs,
                                        compression=self.compression,
                                        retries=self.retries,
                                        durability=self.durability)
            tasks.append(task)
        return tasks


class ContinuousDocUpdateTask(Task):
    def __init__(self, cluster, task_manager, bucket, client, generators,
                 op_type, exp, flag=0, persist_to=0, replicate_to=0,
                 durability="", time_unit="seconds",
                 only_store_hash=True, batch_size=1,
                 pause_secs=1, timeout_secs=5, compression=True,
                 process_concurrency=8, print_ops_rate=True, retries=5,
                 active_resident_threshold=99):
        super(ContinuousDocUpdateTask, self).__init__(
            "ContinuousDocUpdateTask {}".format(time.time()))
        self.cluster = cluster
        self.exp = exp
        self.flag = flag
        self.persist_to = persist_to
        self.replicate_to = replicate_to
        self.durability = durability
        self.time_unit = time_unit
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
        self.retries = retries
        self.active_resident_threshold = active_resident_threshold

        self.key = self.generators[0].name
        self.doc_start_num = self.generators[0].start
        self.doc_end_num = self.generators[0].end
        self.doc_type = self.generators[0].doc_type
        self.op_type = "update"

        if isinstance(bucket, list):
            self.buckets = bucket
        else:
            self.bucket = bucket

    def _start_doc_updates(self, bucket):
        while True:
            self.test_log.debug("Updating docs in {0}".format(bucket.name))
            task = LoadDocumentsGeneratorsTask(
                self.cluster, self.task_manager, bucket, self.client,
                self.generators, self.op_type, self.exp, flag=self.flag,
                persist_to=self.persist_to, replicate_to=self.replicate_to,
                durability=self.durability,
                only_store_hash=self.only_store_hash,
                batch_size=self.batch_size, pause_secs=self.pause_secs,
                timeout_secs=self.timeout_secs, compression=self.compression,
                retries=self.retries)
            self.task_manager.add_new_task(task)
            self.task_manager.get_task_result(task)

    def call(self):
        self.start_task()
        if self.buckets:
            for bucket in self.buckets:
                self._start_doc_updates(bucket)
        else:
            self._start_doc_updates(self.bucket)
        self.complete_task()


class LoadDocumentsForDgmTask(Task):
    def __init__(self, cluster, task_manager, bucket, clients, key, exp,
                 doc_index=0, batch_size=50,
                 persist_to=0, replicate_to=0,
                 durability="",
                 timeout_secs=5,
                 process_concurrency=4, print_ops_rate=True,
                 active_resident_threshold=99,
                 task_identifier=""):
        super(LoadDocumentsForDgmTask, self).__init__(
            "LoadDocumentsForDgmTask_{}_{}".format(bucket.name, time.time()))
        self.cluster = cluster
        self.exp = exp
        self.persist_to = persist_to
        self.replicate_to = replicate_to
        self.durability = durability
        self.timeout_secs = timeout_secs
        self.process_concurrency = process_concurrency
        self.clients = clients
        self.task_manager = task_manager
        self.batch_size = batch_size
        self.op_types = None
        self.buckets = None
        self.print_ops_rate = print_ops_rate
        self.active_resident_threshold = active_resident_threshold

        self.key = key
        self.task_identifier = task_identifier
        self.op_type = "create"
        self.rest_client = BucketHelper(self.cluster.master)
        self.doc_index = doc_index
        if isinstance(bucket, list):
            self.buckets = bucket
        else:
            self.buckets = [bucket]

    def _get_bucket_dgm(self, bucket):
        bucket_stat = self.rest_client.get_bucket_stats_for_node(
            bucket.name,
            self.cluster.master)
        return bucket_stat["vb_active_resident_items_ratio"]

    def _load_next_batch_of_docs(self, bucket):
        doc_gens = list()
        doc_tasks = list()
        self.test_log.debug("Doc load from index %d" % self.doc_index)
        for _ in self.clients:
            doc_gens.append(doc_generator(
                self.key, self.doc_index, self.doc_index+10000))
            self.doc_index += 10000

        # Start doc_loading tasks
        for index, generator in enumerate(doc_gens):
            batch_gen = BatchedDocumentGenerator(generator, self.batch_size)
            task = LoadDocumentsTask(
                self.cluster, bucket, self.clients[index], batch_gen,
                "create", self.exp,
                persist_to=self.persist_to,
                replicate_to=self.replicate_to,
                durability=self.durability,
                timeout_secs=self.timeout_secs,
                skip_read_on_error=True)
            self.task_manager.add_new_task(task)
            doc_tasks.append(task)

        # Wait for doc_loading tasks to complete
        for task in doc_tasks:
            self.task_manager.get_task_result(task)

    def _load_bucket_into_dgm(self, bucket):
        dgm_value = self._get_bucket_dgm(bucket)
        self.test_log.info("DGM doc loading for '%s' to atleast %s%%"
                           % (bucket.name, self.active_resident_threshold))
        while dgm_value > self.active_resident_threshold:
            self.test_log.debug("Active_resident_items_ratio for {0} is {1}"
                                .format(bucket.name, dgm_value))
            self._load_next_batch_of_docs(bucket)
            dgm_value = self._get_bucket_dgm(bucket)
        self.test_log.info("DGM %s%% achieved for '%s'"
                           % (dgm_value, bucket.name))

    def call(self):
        self.test_log.info("Starting DGM doc loading task")
        self.start_task()
        for bucket in self.buckets:
            self._load_bucket_into_dgm(bucket)
        self.complete_task()
        self.test_log.info("Done loading docs for DGM")


class ValidateDocumentsTask(GenericLoadingTask):

    def __init__(self, cluster, bucket, client, generator, op_type, exp,
                 flag=0, proxy_client=None, batch_size=1, pause_secs=1,
                 timeout_secs=30, compression=True):
        super(ValidateDocumentsTask, self).__init__(
            cluster, bucket, client, batch_size=batch_size,
            pause_secs=pause_secs, timeout_secs=timeout_secs,
            compression=compression)
        self.thread_name = "ValidateDocumentsTask-{}_{}_{}_{}".format(
            bucket.name, generator._doc_gen.start, generator._doc_gen.end,
            op_type)

        self.generator = generator
        self.op_type = op_type
        self.exp = exp
        self.flag = flag
        self.failed_item_table = TableView(self.test_log.info)
        self.failed_item_table.set_headers(["READ doc_Id", "Exception"])
        self.missing_keys = []
        self.wrong_values = []
        if proxy_client:
            self.log.debug("Changing client to proxy %s:%s..."
                           % (proxy_client.host, proxy_client.port))
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
            self.set_exception(Exception("Bad operation type: %s"
                                         % self.op_type))
        result_map, failed_reads = self.batch_read(key_value.keys())
        for key, value in failed_reads.items():
            self.failed_item_table.add_row([key, value['error']])
        missing_keys, wrong_values = self.validate_key_val(result_map,
                                                           key_value)
        if self.op_type == 'delete':
            not_missing = []
            if missing_keys.__len__() == key_value.keys().__len__():
                for key in key_value.keys():
                    if key not in missing_keys:
                        not_missing.append(key)
                if not_missing:
                    self.set_exception(Exception("Keys were not deleted. "
                                                 "Keys not deleted: {}"
                                                 .format(','.join(not_missing))))
        else:
            if missing_keys:
                self.missing_keys.extend(missing_keys)
            if wrong_values:
                self.wrong_values.extend(wrong_values)

    def validate_key_val(self, map, key_value):
        missing_keys = []
        wrong_values = []
        for key, value in key_value.items():
            if key in map:
                expected_val = Json.loads(value)
                actual_val = {}
                if map[key]['cas'] != 0:
                    actual_val = Json.loads(map[key][
                                                'value'].toString())
                elif map[key]['error'] is not None:
                    actual_val = map[key]['error'].toString()
                else:
                    missing_keys.append(key)
                    continue
                if expected_val == actual_val:
                    continue
                else:
                    wrong_value = "Key: {} Expected: {} Actual: {}" \
                        .format(key, expected_val, actual_val)
                    wrong_values.append(wrong_value)
            else:
                missing_keys.append(key)
        return missing_keys, wrong_values


class DocumentsValidatorTask(Task):
    def __init__(self, cluster, task_manager, bucket, client, generators,
                 op_type, exp, flag=0, only_store_hash=True, batch_size=1,
                 pause_secs=1, timeout_secs=60, compression=True,
                 process_concurrency=4):
        super(DocumentsValidatorTask, self).__init__(
            "DocumentsValidatorTask_{}".format(time.time()))
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
                self.set_exception(
                    Exception("Not all generators have op_type!"))
                self.complete_task()
        if self.buckets:
            if len(self.op_types) != len(self.buckets):
                self.set_exception(
                    Exception("Not all generators have bucket specified!"))
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
            if task.missing_keys:
                self.set_exception("{} keys were missing. Missing keys: "
                                   "{}".format(task.missing_keys.__len__(),
                                               task.missing_keys))
                task.failed_item_table.display("Failed items:")
            if task.wrong_values:
                self.set_exception("{} values were wrong. Wrong key-value: "
                                   "{}".format(task.wrong_values.__len__(),
                                               task.wrong_values))
        self.client.close()
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
            task = ValidateDocumentsTask(
                self.cluster, self.bucket, self.client, generator,
                self.op_type, self.exp, self.flag, batch_size=self.batch_size,
                pause_secs=self.pause_secs, timeout_secs=self.timeout_secs,
                compression=self.compression)
            tasks.append(task)
        return tasks


class StatsWaitTask(Task):
    EQUAL = '=='
    NOT_EQUAL = '!='
    LESS_THAN = '<'
    LESS_THAN_EQ = '<='
    GREATER_THAN = '>'
    GREATER_THAN_EQ = '>='

    def __init__(self, shell_conn_list, bucket, stat_cmd, stat, comparison,
                 value, timeout=300):
        super(StatsWaitTask, self).__init__("StatsWaitTask_{}_{}_{}"
                                            .format(
                                                bucket.name,
                                                stat,
                                                str(time.time())
                                                )
                                            )
        self.shellConnList = shell_conn_list
        self.bucket = bucket
        self.statCmd = stat_cmd
        self.stat = stat
        self.comparison = comparison
        self.value = value
        self.stop = False
        self.timeout = timeout
        self.cbstatObj = None

    def call(self):
        self.start_task()
        start_time = time.time()
        timeout = start_time + self.timeout
        self.cbstatObjList = list()
        for remote_conn in self.shellConnList:
            self.cbstatObjList.append(Cbstats(remote_conn))
        try:
            while not self.stop and time.time() < timeout:
                if self.statCmd in ["all", "dcp"]:
                    self._get_all_stats_and_compare()
                else:
                    raise Exception("Not supported. Implement the stat call")
        finally:
            pass
        if time.time() > timeout:
            for shell in self.shellConnList:
                shell.disconnect()
            self.set_exception("Could not verify stat {} within timeout {}"
                               .format(self.stat, self.timeout))

        self.complete_task()

    def _get_all_stats_and_compare(self):
        stat_result = 0
        val_dict = dict()
        try:
            for cb_stat_obj in self.cbstatObjList:
                tem_stat = cb_stat_obj.all_stats(self.bucket.name,
                                                 field_to_grep=self.stat,
                                                 stat_name=self.statCmd)
                val_dict[cb_stat_obj.shellConn.ip] = tem_stat
                if tem_stat and tem_stat != "None":
                    stat_result += int(tem_stat)
        except Exception as error:
            for shell in self.shellConnList:
                shell.disconnect()
            self.set_exception(error)
            self.stop = True
            return False
        if not self._compare(self.comparison, str(stat_result), self.value):
            self.test_log.warn("Not Ready: %s %s %s %s. Received: %s for bucket '%s'"
                               % (self.stat, stat_result, self.comparison,
                                  self.value, val_dict, self.bucket.name))
            time.sleep(5)
            return False
        else:
            self.test_log.debug("Ready: %s %s %s %s. Received: %s for bucket '%s'"
                                % (self.stat, stat_result, self.comparison,
                                   self.value, val_dict, self.bucket.name))
            self.stop = True
            return True

    def _compare(self, cmp_type, a, b):
        if isinstance(b, (int, long)) and a.isdigit():
            a = long(a)
        elif isinstance(b, (int, long)) and not a.isdigit():
            return False
        self.test_log.debug("Comparing %s %s %s" % (a, cmp_type, b))
        if (cmp_type == StatsWaitTask.EQUAL and a == b) or \
                (cmp_type == StatsWaitTask.NOT_EQUAL and a != b) or \
                (cmp_type == StatsWaitTask.LESS_THAN_EQ and a <= b) or \
                (cmp_type == StatsWaitTask.GREATER_THAN_EQ and a >= b) or \
                (cmp_type == StatsWaitTask.LESS_THAN and a < b) or \
                (cmp_type == StatsWaitTask.GREATER_THAN and a > b):
            return True
        return False


class ViewCreateTask(Task):
    def __init__(self, server, design_doc_name, view,
                 bucket="default", with_query=True,
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
            content, meta = self.rest.get_ddoc(self.bucket,
                                               self.design_doc_name)
            ddoc = DesignDocument._init_from_json(self.design_doc_name,
                                                  content)
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
                    ddoc = DesignDocument(self.design_doc_name, [],
                                          spatial_views=[self.view])
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
                        content = self.rest.query_view(
                            self.design_doc_name, self.view.name,
                            self.bucket, query, type="spatial")
                    else:
                        content = self.rest.query_view(
                            self.design_doc_name, self.view.name,
                            self.bucket, query)
                else:
                    _, json_parsed, _ = self.rest._get_design_doc(
                        self.bucket, self.design_doc_name)
                    if self.view.is_spatial:
                        if self.view.name not in json_parsed["spatial"].keys():
                            self.set_exception(
                                Exception("design doc {0} doesn't contain spatial view {1}"
                                          .format(self.design_doc_name,
                                                  self.view.name)))
                            return 0
                    else:
                        if self.view.name not in json_parsed["views"].keys():
                            self.set_exception(Exception("design doc {0} doesn't contain view {1}"
                                                         .format(self.design_doc_name, self.view.name)))
                            return 0
                self.test_log.debug("View: {0} was created successfully in ddoc: {1}"
                                    .format(self.view.name, self.design_doc_name))
            else:
                # If we are here, it means design doc was successfully updated
                self.test_log.debug("Design Doc: {0} was updated successfully"
                                    .format(self.design_doc_name))

            if self._check_ddoc_revision():
                return self.ddoc_rev_no
            else:
                self.set_exception(Exception("failed to update design doc"))
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
            content, meta = self.rest.get_ddoc(self.bucket,
                                               self.design_doc_name)
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
                    content, meta = rest_node.get_ddoc(self.bucket,
                                                       self.design_doc_name)
                    new_rev_id = self._parse_revision(meta['rev'])
                    if new_rev_id == self.ddoc_rev_no:
                        break
                    else:
                        self.test_log.debug("Design Doc {0} version is not updated on node {1}:{2}. Retrying."
                                            .format(self.design_doc_name,
                                                    node.ip, node.port))
                        time.sleep(2)
                except ReadDocumentException as e:
                    if count < retry_count:
                        self.test_log.debug("Design Doc {0} not yet available on node {1}:{2}. Retrying."
                                            .format(self.design_doc_name,
                                                    node.ip, node.port))
                        time.sleep(2)
                    else:
                        self.test_log.error("Design Doc {0} failed to replicate on node {1}:{2}"
                                            .format(self.design_doc_name, node.ip, node.port))
                        self.set_exception(e)
                        break
                except Exception as e:
                    if count < retry_count:
                        self.test_log.error("Retrying.. Unexpected Exception {0}"
                                            .format(e))
                        time.sleep(2)
                    else:
                        self.set_exception(e)
                        break
            else:
                self.set_exception(Exception(
                    "Design Doc {0} version mismatch on node {1}:{2}"
                        .format(self.design_doc_name, node.ip, node.port)))


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
                content, header = rest.get_ddoc(self.bucket,
                                                self.design_doc_name)
                ddoc = DesignDocument._init_from_json(self.design_doc_name,
                                                      content)
                if self.view.is_spatial:
                    status = ddoc.delete_spatial(self.view)
                else:
                    status = ddoc.delete_view(self.view)
                if not status:
                    self.set_exception(Exception('View does not exist! %s'
                                                 % (self.view.name)))
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
                self.test_log.debug("Design Doc : {0} was successfully deleted"
                                    .format(self.design_doc_name))
                self.complete_task()
                return True

        except (ValueError, ReadDocumentException,
                DesignDocCreationException) as e:
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
            content = rest.query_view(self.design_doc_name, self.view.name,
                                      self.bucket, query)
            return False
        except QueryViewException as e:
            self.test_log.debug("View: {0} was successfully deleted in ddoc: {1}"
                                .format(self.view.name, self.design_doc_name))
            return True

        # catch and set all unexpected exceptions
        except Exception as e:
            self.set_exception(e)
            return False


class ViewQueryTask(Task):
    def __init__(self, server, design_doc_name, view_name,
                 query, expected_rows=None,
                 bucket="default", retry_time=2):
        Task.__init__(self, "query_view_task_{}_{}".format(design_doc_name,
                                                           view_name))
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
        retries = 0
        while retries < self.retry_time:
            try:
                rest = RestConnection(self.server)
                # make sure view can be queried
                content = \
                    rest.query_view(self.design_doc_name, self.view_name,
                                    self.bucket, self.query, self.timeout)

                if self.expected_rows is None:
                    # no verification
                    self.result = True
                    self.complete_task()
                    return content
                else:
                    return_value = self.check()
                    self.result = return_value
                    self.complete_task()
                    return return_value

            except QueryViewException as e:
                # initial query failed, try again
                time.sleep(self.retry_time)
                retries += 1

            # catch and set all unexpected exceptions
            except Exception as e:
                self.set_exception(e)
                self.complete_task()
                self.result = True
                return False

    def check(self):
        try:
            rest = RestConnection(self.server)
            # query and verify expected num of rows returned
            content = \
                rest.query_view(self.design_doc_name, self.view_name,
                                self.bucket, self.query, self.timeout)

            self.test_log.debug("Server: %s, Design Doc: %s, View: %s, (%d rows) expected, (%d rows) returned"
                                % (self.server.ip, self.design_doc_name,
                                   self.view_name, self.expected_rows,
                                   len(content['rows'])))

            raised_error = content.get(u'error', '') or \
                           ''.join([str(item) for item in content.get(u'errors', [])])
            if raised_error:
                raise QueryViewException(self.view_name, raised_error)

            if len(content['rows']) == self.expected_rows:
                self.test_log.debug("Expected rows: '{0}' was found for view query"
                                    .format(self.expected_rows))
                return True
            else:
                if len(content['rows']) > self.expected_rows:
                    raise QueryViewException(self.view_name,
                                             "Server: {0}, Design Doc: {1}, actual returned rows: '{2}' are greater than expected {3}"
                                             .format(self.server.ip,
                                                     self.design_doc_name,
                                                     len(content['rows']),
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
    def __init__(self, server, bucket, query, n1ql_helper=None,
                 expected_result=None, verify_results=True,
                 is_explain_query=False, index_name=None, retry_time=2,
                 scan_consistency=None, scan_vector=None):
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
            self.test_log.debug(" <<<<< START Executing Query {0} >>>>>>"
                      .format(self.query))
            if not self.is_explain_query:
                self.msg, self.isSuccess = self.n1ql_helper.run_query_and_verify_result(
                    query=self.query, server=self.server,
                    expected_result=self.expected_result,
                    scan_consistency=self.scan_consistency,
                    scan_vector=self.scan_vector,
                    verify_results=self.verify_results)
            else:
                self.actual_result = self.n1ql_helper.run_cbq_query(
                    query=self.query, server=self.server,
                    scan_consistency=self.scan_consistency,
                    scan_vector=self.scan_vector)
                self.test_log.debug(self.actual_result)
            self.test_log.debug(" <<<<< Done Executing Query {0} >>>>>>"
                                .format(self.query))
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
                        self.test_log.debug("Query {0} results leads to INCORRECT RESULT"
                                            .format(self.query))
                        raise N1QLQueryException(self.msg)
                else:
                    check = self.n1ql_helper.verify_index_with_explain(self.actual_result, self.index_name)
                    if not check:
                        actual_result = self.n1ql_helper.run_cbq_query(
                            query="select * from system:indexes",
                            server=self.server)
                        self.test_log.debug(actual_result)
                        raise Exception(
                            " INDEX usage in Query {0} :: NOT FOUND {1} :: as observed in result {2}"
                                .format(self.query, self.index_name, self.actual_result))
            self.test_log.debug(" <<<<< Done VERIFYING Query {0} >>>>>>"
                                .format(self.query))
            return True
        except N1QLQueryException as e:
            # subsequent query failed! exit
            self.set_exception(e)
        # catch and set all unexpected exceptions
        except Exception as e:
            self.set_exception(e)


class CreateIndexTask(Task):
    def __init__(self, server, bucket, index_name, query, n1ql_helper=None,
                 retry_time=2, defer_build=False, timeout=240):
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
            self.test_log.error(e)
            self.set_exception(e)

    def check(self):
        try:
            # Verify correctness of result set
            check = True
            if not self.defer_build:
                check = self.n1ql_helper.is_index_online_and_in_list(
                    self.bucket, self.index_name, server=self.server,
                    timeout=self.timeout)
            if not check:
                raise CreateIndexException("Index {0} not created as expected"
                                           .format(self.index_name))
            return True
        except CreateIndexException as e:
            # subsequent query failed! exit
            self.test_log.error(e)
            self.set_exception(e)
        # catch and set all unexpected exceptions
        except Exception as e:
            self.test_log.error(e)
            self.set_exception(e)


class BuildIndexTask(Task):
    def __init__(self, server, bucket, query, n1ql_helper=None,
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
    def __init__(self, server, bucket, index_name, n1ql_helper=None,
                 retry_time=2, timeout=240):
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
            check = self.n1ql_helper.is_index_online_and_in_list(
                self.bucket, self.index_name, server=self.server,
                timeout=self.timeout)
            if not check:
                raise CreateIndexException("Index {0} not created as expected"
                                           .format(self.index_name))
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
    def __init__(self, server, bucket, index_name, query, n1ql_helper=None,
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
            check = self.n1ql_helper._is_index_in_list(
                self.bucket, self.index_name, server=self.server)
            if not check:
                raise DropIndexException("index {0} does not exist will not drop"
                                         .format(self.index_name))
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
            check = self.n1ql_helper._is_index_in_list(
                self.bucket, self.index_name, server=self.server)
            if check:
                raise Exception("Index {0} not dropped as expected"
                                .format(self.index_name))
            return True
        except DropIndexException as e:
            # subsequent query failed! exit
            self.set_exception(e)
        # catch and set all unexpected exceptions
        except Exception as e:
            self.set_exception(e)


class PrintOpsRate(Task):
    def __init__(self, cluster, bucket, sleep=1):
        super(PrintOpsRate, self).__init__("print_ops_rate_{}"
                                           .format(bucket.name))
        self.cluster = cluster
        self.bucket = bucket
        self.bucket_helper = BucketHelper(self.cluster.master)
        self.sleep = sleep
        self.stop_task = False

    def call(self):
        ops_rate_trend = list()
        t_ops_rate = list()
        self.start_task()
        while not self.stop_task:
            try:
                bucket_stats = self.bucket_helper.fetch_bucket_stats(self.bucket)
                if 'op' in bucket_stats and \
                        'samples' in bucket_stats['op'] and \
                        'ops' in bucket_stats['op']['samples']:
                    ops = bucket_stats['op']['samples']['ops'][-1]
                    self.test_log.debug("Ops rate for '%s': %f"
                                        % (self.bucket.name, ops))
                    if t_ops_rate and t_ops_rate[-1] > ops:
                        ops_rate_trend.append(t_ops_rate)
                        t_ops_rate = list()
                    t_ops_rate.append(ops)
                    time.sleep(self.sleep)
            except:
                #Case when cluster.master is rebalance out of the cluster
                self.bucket_helper = BucketHelper(self.cluster.master)
                time.sleep(20)
        if t_ops_rate:
            ops_rate_trend.append(t_ops_rate)
        plot_graph(self.test_log, self.bucket.name, ops_rate_trend)
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
            self.log.error("RestConnection failed for {0}: {1}"
                           .format(self.server.ip, error))
            self.result = False
            return
        info = rest.get_nodes_self()

        if self.bucket.ramQuotaMB <= 0:
            self.size = info.memoryQuota * 2 / 3

        if int(info.port) in xrange(9091, 9991):
            try:
                self.port = info.port
                BucketHelper(self.server).create_bucket(self.bucket.__dict__)
                # return_value = self.check()
                self.complete_task()
                self.result = True
                return
            except Exception as e:
                self.test_log.error(str(e))
                self.set_exception(e)
        version = rest.get_nodes_self().version
        try:
            if float(version[:2]) >= 3.0 and self.bucket_priority is not None:
                self.bucket.threadsNumber = self.bucket_priority
            BucketHelper(self.server).create_bucket(self.bucket.__dict__)
            # return_value = self.check()
            self.complete_task()
            self.result = True
            return
        except BucketCreationException as e:
            self.test_log.error(str(e))
            self.set_exception(e)
        # catch and set all unexpected exceptions
        except Exception as e:
            self.result = False
            self.test_log.error(str(e))
            self.set_exception(e)

    def check(self):
        try:
            # if self.bucket.bucketType == 'memcached' or \
            #        int(self.server.port) in xrange(9091, 9991):
            #     return True
            if MemcachedHelper.wait_for_memcached(self.server, self.bucket.name):
                self.test_log.debug("Bucket '{0}' created with per node RAM quota: {1}"
                                    .format(self.bucket, self.bucket.ramQuotaMB))
                return True
            else:
                self.test_log.error("Vbucket map not ready after try {0}"
                                    .format(self.retries))
                if self.retries >= 5:
                    return False
        except Exception as e:
            self.test_log.warn("Exception: {0}. vbucket map not ready after try {1}"
                               .format(e, self.retries))
            if self.retries >= 5:
                self.set_exception(e)
        self.retries = self.retris + 1
        time.sleep(5)
        self.check()


class MonitorActiveTask(Task):
    """
        Attempt to monitor active task that  is available in _active_tasks API.
        It allows to monitor indexer, bucket compaction.

        Execute function looks at _active_tasks API and tries to identifies
        task for monitoring and its pid by:
        task type('indexer' , 'bucket_compaction', 'view_compaction')
        and target value (for example "_design/ddoc" for indexing,
        bucket "default" for bucket compaction or
        "_design/dev_view" for view compaction).
        wait_task=True means that task should be found in the first attempt
        otherwise, we can assume that the task has been completed(reached 100%)

        Check function monitors task by pid that was identified in execute func
        and matches new progress result with the previous.
        task is failed if:
            progress is not changed  during num_iterations iteration
            new progress was gotten less then previous
        task is passed and completed if:
            progress reached wait_progress value
            task was not found by pid(believe that it's over)
    """

    def __init__(self, server, type, target_value, wait_progress=100,
                 num_iterations=100, wait_task=True):
        super(MonitorActiveTask, self).__init__("MonitorActiveTask")
        self.server = server
        self.type = type  # indexer or bucket_compaction
        self.target_key = ""

        if self.type == 'indexer':
            # no special actions
            pass
        elif self.type == "bucket_compaction":
            self.target_key = "original_target"
        elif self.type == "view_compaction":
            self.target_key = "designDocument"
        else:
            raise Exception("type %s is not defined!" % self.type)
        self.target_value = target_value
        self.wait_progress = wait_progress
        self.num_iterations = num_iterations
        self.wait_task = wait_task

        self.rest = RestConnection(self.server)
        self.current_progress = None
        self.current_iter = 0
        self.task = None

    def call(self):
        tasks = self.rest.active_tasks()
        for task in tasks:
            if task["type"] == self.type \
                    and ((self.target_key == "designDocument"
                          and task[self.target_key] == self.target_value)
                         or (self.target_key == "original_target"
                             and task[self.target_key]["type"] == self.target_value)
                         or (self.type == 'indexer')):
                self.current_progress = task["progress"]
                self.task = task
                self.test_log.debug("monitoring active task was found:" + str(task))
                self.test_log.debug("progress %s:%s - %s %%"
                                    % (self.type, self.target_value, task["progress"]))
                if self.current_progress >= self.wait_progress:
                    self.test_log.debug("Got expected progress: %s"
                                        % self.current_progress)
                    self.result = True
        if self.wait_task:
            # task is not performed
            self.test_log.error("Expected active task %s:%s was not found"
                                % (self.type, self.target_value))
            self.result = False
        else:
            # task was completed
            self.test_log.debug("task for monitoring %s:%s completed"
                                % (self.type, self.target_value))
            self.result = True

    def check(self):
        tasks = self.rest.active_tasks()
        self.test_log.info("tasks running on the server: %s" % tasks)
        if self.task in tasks and self.task is not None:
            for task in tasks:
                # if task still exists
                if task == self.task:
                    self.test_log.debug("Progress %s:%s - %s %%"
                                        % (self.type, self.target_value,
                                           task["progress"]))
                    # reached expected progress
                    if task["progress"] >= self.wait_progress:
                        self.test_log.info("Progress for task %s reached %s"
                                           % (self.task, self.wait_progress))
                        self.result = True
                        return
                    # progress value was changed
                    if task["progress"] > self.current_progress:
                        self.current_progress = task["progress"]
                        self.current_iter = 0
                        self.check()
                    # progress value was not changed
                    elif task["progress"] == self.current_progress:
                        if self.current_iter < self.num_iterations:
                            time.sleep(2)
                            self.current_iter += 1
                            self.check()
                        else:
                            self.test_log.error("Progress for active task was not changed during %s sec"
                                                % 2 * self.num_iterations)
                            self.result = False
                            return
                    else:
                        self.test_log.error("Progress for task %s:%s changed direction!"
                                            % (self.type, self.target_value))
                        self.result = False
                        return
        else:
            self.test_log.info("Task %s is not running on the server"
                               % (self.task))
            self.result = True


class MonitorDBFragmentationTask(Task):
    """
        Attempt to monitor fragmentation that is occurring for a given bucket.
        Note: If autocompaction is enabled and user attempts to monitor for
        fragmentation value higher than level at which auto_compaction
        kicks in a warning is sent and it is best user to use lower value
        as this can lead to infinite monitoring.
    """

    def __init__(self, server, fragmentation_value=10, bucket_name="default",
                 get_view_frag=False):
        Task.__init__(self, "monitor_frag_db_task")
        self.server = server
        self.bucket_name = bucket_name
        self.fragmentation_value = fragmentation_value
        self.get_view_frag = get_view_frag

    def check(self):
        # sanity check of fragmentation value
        if self.fragmentation_value < 0 or self.fragmentation_value > 100:
            err_msg = "Invalid value for fragmentation %d" \
                      % self.fragmentation_value
            self.set_exception(Exception(err_msg))

    def call(self):
        self.check()
        self.start_task()
        bucket_helper = BucketHelper(self.server)
        while True:
            try:
                stats = bucket_helper.fetch_bucket_stats(self.bucket_name)
                if self.get_view_frag:
                    new_frag_value = \
                        stats["op"]["samples"]["couch_views_fragmentation"][-1]
                    self.test_log.debug(
                        "Current amount of views fragmentation %d"
                        % new_frag_value)
                else:
                    new_frag_value = \
                        stats["op"]["samples"]["couch_docs_fragmentation"][-1]
                    self.test_log.debug(
                        "Current amount of docs fragmentation %d"
                        % new_frag_value)
                if new_frag_value >= self.fragmentation_value:
                    self.test_log.info("Fragmentation level: %d%%"
                                       % new_frag_value)
                    self.set_result(True)
                    break
            except Exception, ex:
                self.set_result(False)
                self.set_exception(ex)
            time.sleep(2)
        self.complete_task()


class AutoFailoverNodesFailureTask(Task):
    def __init__(self, task_manager, master, servers_to_fail, failure_type,
                 timeout, pause=0, expect_auto_failover=True, timeout_buffer=3,
                 check_for_failover=True, failure_timers=None,
                 disk_timeout=0, disk_location=None, disk_size=200):
        super(AutoFailoverNodesFailureTask, self) \
            .__init__("AutoFailoverNodesFailureTask")
        self.task_manager = task_manager
        self.master = master
        self.servers_to_fail = servers_to_fail
        self.num_servers_to_fail = self.servers_to_fail.__len__()
        self.itr = 0
        self.failure_type = failure_type
        self.timeout = timeout
        self.pause = pause
        self.expect_auto_failover = expect_auto_failover
        self.check_for_autofailover = check_for_failover
        self.start_time = 0
        self.timeout_buffer = timeout_buffer
        self.current_failure_node = self.servers_to_fail[0]
        self.max_time_to_wait_for_failover = self.timeout + \
                                             self.timeout_buffer + 60
        self.disk_timeout = disk_timeout
        self.disk_location = disk_location
        self.disk_size = disk_size
        if failure_timers is None:
            failure_timers = []
        self.failure_timers = failure_timers
        self.rebalance_in_progress = False

    def call(self):
        rest = RestConnection(self.master)
        if rest._rebalance_progress_status() == "running":
            self.rebalance_in_progress = True
        return_val = False
        while self.has_next() and not self.completed:
            self.next()
            if self.pause > 0 and self.pause > self.timeout:
                return_val = self.check()
        if self.pause == 0 or 0 < self.pause < self.timeout:
            return_val = self.check()
        self.complete_task()
        return return_val

    def check(self):
        if not self.check_for_autofailover:
            return True
        rest = RestConnection(self.master)
        max_timeout = self.timeout + self.timeout_buffer + self.disk_timeout
        if self.start_time == 0:
            message = "Did not inject failure in the system."
            rest.print_UI_logs(10)
            self.test_log.error(message)
            self.set_exception(AutoFailoverException(message))
            return False
        if self.rebalance_in_progress:
            status, stop_time = self._check_if_rebalance_in_progress(120)
            if not status:
                if stop_time == -1:
                    message = "Rebalance already completed before failover " \
                              "of node"
                    self.test_log.error(message)
                    self.set_exception(AutoFailoverException(message))
                    return False
                elif stop_time == -2:
                    message = "Rebalance failed but no failed autofailover " \
                              "message was printed in logs"
                    self.test_log.warning(message)
                else:
                    message = "Rebalance not failed even after 2 minutes " \
                              "after node failure."
                    self.test_log.error(message)
                    rest.print_UI_logs(10)
                    self.set_exception(AutoFailoverException(message))
                    return False
            else:
                self.start_time = stop_time
        autofailover_initiated, time_taken = \
            self._wait_for_autofailover_initiation(
                self.max_time_to_wait_for_failover)
        if self.expect_auto_failover:
            if autofailover_initiated:
                if time_taken < max_timeout + 1:
                    self.test_log.debug("Autofailover of node {0} successfully"
                                        " initiated in {1} sec"
                                        .format(self.current_failure_node.ip,
                                                time_taken))
                    rest.print_UI_logs(10)
                    return True
                else:
                    message = "Autofailover of node {0} was initiated after " \
                              "the timeout period. Expected  timeout: {1} " \
                              "Actual time taken: {2}".format(
                        self.current_failure_node.ip, self.timeout, time_taken)
                    self.test_log.error(message)
                    rest.print_UI_logs(10)
                    self.set_exception(AutoFailoverException(message))
                    return False
            else:
                message = "Autofailover of node {0} was not initiated after " \
                          "the expected timeout period of {1}".format(
                    self.current_failure_node.ip, self.timeout)
                rest.print_UI_logs(10)
                self.test_log.error(message)
                self.set_exception(AutoFailoverException(message))
                return False
        else:
            if autofailover_initiated:
                message = "Node {0} was autofailed over but no autofailover " \
                          "of the node was expected".format(
                    self.current_failure_node.ip)
                rest.print_UI_logs(10)
                self.test_log.error(message)
                self.set_exception(AutoFailoverException(message))
                return False
            else:
                self.test_log.error("Node not autofailed over as expected")
                rest.print_UI_logs(10)
                return False

    def has_next(self):
        return self.itr < self.num_servers_to_fail

    def next(self):
        if self.pause != 0:
            time.sleep(self.pause)
            if self.pause > self.timeout and self.itr != 0:
                rest = RestConnection(self.master)
                status = rest.reset_autofailover()
                self._rebalance()
                if not status:
                    self.set_exception(Exception("Reset of autofailover "
                                                 "count failed"))
                    return False
        self.current_failure_node = self.servers_to_fail[self.itr]
        self.test_log.debug("Before failure time: {}"
                            .format(time.ctime(time.time())))
        if self.failure_type == "enable_firewall":
            self._enable_firewall(self.current_failure_node)
        elif self.failure_type == "disable_firewall":
            self._disable_firewall(self.current_failure_node)
        elif self.failure_type == "restart_couchbase":
            self._restart_couchbase_server(self.current_failure_node)
        elif self.failure_type == "stop_couchbase":
            self._stop_couchbase_server(self.current_failure_node)
        elif self.failure_type == "start_couchbase":
            self._start_couchbase_server(self.current_failure_node)
        elif self.failure_type == "restart_network":
            self._stop_restart_network(self.current_failure_node,
                                       self.timeout + self.timeout_buffer + 30)
        elif self.failure_type == "restart_machine":
            self._restart_machine(self.current_failure_node)
        elif self.failure_type == "stop_memcached":
            self._stop_memcached(self.current_failure_node)
        elif self.failure_type == "start_memcached":
            self._start_memcached(self.current_failure_node)
        elif self.failure_type == "network_split":
            self._block_incoming_network_from_node(self.servers_to_fail[0],
                                                   self.servers_to_fail[
                                                       self.itr + 1])
            self.itr += 1
        elif self.failure_type == "disk_failure":
            self._fail_disk(self.current_failure_node)
        elif self.failure_type == "disk_full":
            self._disk_full_failure(self.current_failure_node)
        elif self.failure_type == "recover_disk_failure":
            self._recover_disk(self.current_failure_node)
        elif self.failure_type == "recover_disk_full_failure":
            self._recover_disk_full_failure(self.current_failure_node)
        self.test_log.debug("Start time = {}"
                            .format(time.ctime(self.start_time)))
        self.itr += 1

    def _enable_firewall(self, node):
        node_failure_timer = self.failure_timers[self.itr]
        self.task_manager.add_new_task(node_failure_timer)
        time.sleep(2)
        RemoteUtilHelper.enable_firewall(node)
        self.test_log.debug("Enabled firewall on {}".format(node))
        self.task_manager.get_task_result(node_failure_timer)
        self.start_time = node_failure_timer.start_time

    def _disable_firewall(self, node):
        shell = RemoteMachineShellConnection(node)
        shell.disable_firewall()
        shell.disconnect()

    def _restart_couchbase_server(self, node):
        node_failure_timer = self.failure_timers[self.itr]
        self.task_manager.add_new_task(node_failure_timer)
        time.sleep(2)
        shell = RemoteMachineShellConnection(node)
        shell.restart_couchbase()
        shell.disconnect()
        self.test_log.debug("{0} - Restarted couchbase server".format(node))
        self.task_manager.get_task_result(node_failure_timer)
        self.start_time = node_failure_timer.start_time

    def _stop_couchbase_server(self, node):
        node_failure_timer = self.failure_timers[self.itr]
        self.task_manager.add_new_task(node_failure_timer)
        time.sleep(1)
        shell = RemoteMachineShellConnection(node)
        shell.stop_couchbase()
        shell.disconnect()
        self.test_log.debug("{0} - Stopped couchbase server".format(node))
        self.task_manager.get_task_result(node_failure_timer)
        self.start_time = node_failure_timer.start_time

    def _start_couchbase_server(self, node):
        shell = RemoteMachineShellConnection(node)
        shell.start_couchbase()
        shell.disconnect()
        self.test_log.debug("{0} - Started couchbase server".format(node))

    def _stop_restart_network(self, node, stop_time):
        node_failure_timer = self.failure_timers[self.itr]
        self.task_manager.add_new_task(node_failure_timer)
        time.sleep(2)
        shell = RemoteMachineShellConnection(node)
        shell.stop_network(stop_time)
        shell.disconnect()
        self.test_log.debug("Stopped the network for {0} sec and restarted "
                            "the network on {1}".format(stop_time, node))
        self.task_manager.get_task_result(node_failure_timer)
        self.start_time = node_failure_timer.start_time

    def _restart_machine(self, node):
        node_failure_timer = self.failure_timers[self.itr]
        self.task_manager.add_new_task(node_failure_timer)
        time.sleep(2)
        shell = RemoteMachineShellConnection(node)
        command = "/sbin/reboot"
        shell.execute_command(command=command)
        shell.disconnect()
        self.task_manager.get_task_result(node_failure_timer)
        self.start_time = node_failure_timer.start_time

    def _stop_memcached(self, node):
        node_failure_timer = self.failure_timers[self.itr]
        self.task_manager.add_new_task(node_failure_timer)
        time.sleep(2)
        shell = RemoteMachineShellConnection(node)
        o, r = shell.stop_memcached()
        self.test_log.debug("Killed memcached. {0} {1}".format(o, r))
        shell.disconnect()
        self.task_manager.get_task_result(node_failure_timer)
        self.start_time = node_failure_timer.start_time

    def _start_memcached(self, node):
        shell = RemoteMachineShellConnection(node)
        o, r = shell.start_memcached()
        self.test_log.debug("Started back memcached. {0} {1}".format(o, r))
        shell.disconnect()

    def _block_incoming_network_from_node(self, node1, node2):
        shell = RemoteMachineShellConnection(node1)
        self.test_log.debug("Adding {0} into iptables rules on {1}"
                            .format(node1.ip, node2.ip))
        command = "iptables -A INPUT -s {0} -j DROP".format(node2.ip)
        shell.execute_command(command)
        shell.disconnect()
        self.start_time = time.time()

    def _fail_disk(self, node):
        shell = RemoteMachineShellConnection(node)
        output, error = shell.unmount_partition(self.disk_location)
        success = True
        if output:
            for line in output:
                if self.disk_location in line:
                    success = False
        if success:
            self.test_log.debug("Unmounted disk at location : {0} on {1}"
                                .format(self.disk_location, node.ip))
            self.start_time = time.time()
        else:
            exception_str = "Could not fail the disk at {0} on {1}" \
                            .format(self.disk_location, node.ip)
            self.test_log.error(exception_str)
            self.set_exception(Exception(exception_str))
        shell.disconnect()

    def _recover_disk(self, node):
        shell = RemoteMachineShellConnection(node)
        o, r = shell.mount_partition(self.disk_location)
        for line in o:
            if self.disk_location in line:
                self.test_log.debug("Mounted disk at location : {0} on {1}"
                                    .format(self.disk_location, node.ip))
                return
        self.set_exception(Exception("Failed mount disk at location {0} on {1}"
                                     .format(self.disk_location, node.ip)))
        shell.disconnect()
        raise Exception()

    def _disk_full_failure(self, node):
        shell = RemoteMachineShellConnection(node)
        output, error = shell.fill_disk_space(self.disk_location, self.disk_size)
        success = False
        if output:
            for line in output:
                if self.disk_location in line:
                    if "0 100% {0}".format(self.disk_location) in line:
                        success = True
        if success:
            self.test_log.debug("Filled up disk Space at {0} on {1}"
                                .format(self.disk_location, node.ip))
            self.start_time = time.time()
        else:
            self.test_log.debug("Could not fill the disk at {0} on {1}"
                                .format(self.disk_location, node.ip))
            self.set_exception(Exception("Failed to fill disk at {0} on {1}"
                                         .format(self.disk_location, node.ip)))
        shell.disconnect()

    def _recover_disk_full_failure(self, node):
        shell = RemoteMachineShellConnection(node)
        delete_file = "{0}/disk-quota.ext3".format(self.disk_location)
        output, error = shell.execute_command("rm -f {0}".format(delete_file))
        self.test_log.debug(output)
        if error:
            self.test_log.error(error)
        shell.disconnect()

    def _check_for_autofailover_initiation(self, failed_over_node):
        rest = RestConnection(self.master)
        ui_logs = rest.get_logs(10)
        ui_logs_text = [t["text"] for t in ui_logs]
        ui_logs_time = [t["serverTime"] for t in ui_logs]
        expected_log = "Starting failing over ['ns_1@{}']".format(
            failed_over_node.ip)
        if expected_log in ui_logs_text:
            failed_over_time = ui_logs_time[ui_logs_text.index(expected_log)]
            return True, failed_over_time
        return False, None

    def _wait_for_autofailover_initiation(self, timeout):
        autofailover_initated = False
        while time.time() < timeout + self.start_time:
            autofailover_initated, failed_over_time = \
                self._check_for_autofailover_initiation(
                    self.current_failure_node)
            if autofailover_initated:
                end_time = self._get_mktime_from_server_time(failed_over_time)
                time_taken = end_time - self.start_time
                return autofailover_initated, time_taken
        return autofailover_initated, -1

    def _get_mktime_from_server_time(self, server_time):
        time_format = "%Y-%m-%dT%H:%M:%S"
        server_time = server_time.split('.')[0]
        mk_time = time.mktime(time.strptime(server_time, time_format))
        return mk_time

    def _rebalance(self):
        rest = RestConnection(self.master)
        nodes = rest.node_statuses()
        rest.rebalance(otpNodes=[node.id for node in nodes])
        rebalance_progress = rest.monitorRebalance()
        if not rebalance_progress:
            self.set_result(False)
            self.set_exception(Exception("Failed to rebalance after failover"))

    def _check_if_rebalance_in_progress(self, timeout):
        rest = RestConnection(self.master)
        end_time = time.time() + timeout
        while time.time() < end_time:
            try:
                rebalance_status, progress = \
                    rest._rebalance_status_and_progress()
                if rebalance_status == "running":
                    continue
                elif rebalance_status is None and progress == 100:
                    return False, -1
            except RebalanceFailedException:
                ui_logs = rest.get_logs(10)
                ui_logs_text = [t["text"] for t in ui_logs]
                ui_logs_time = [t["serverTime"] for t in ui_logs]
                rebalace_failure_log = "Rebalance exited with reason"
                for ui_log in ui_logs_text:
                    if rebalace_failure_log in ui_log:
                        rebalance_failure_time = ui_logs_time[
                            ui_logs_text.index(ui_log)]
                        failover_log = "Could not automatically fail over " \
                                       "node ('ns_1@{}'). Rebalance is " \
                                       "running.".format(
                            self.current_failure_node.ip)
                        if failover_log in ui_logs_text:
                            return True, self._get_mktime_from_server_time(
                                rebalance_failure_time)
                        else:
                            return False, -2
        return False, -3


class NodeDownTimerTask(Task):
    def __init__(self, node, port=None, timeout=300):
        Task.__init__(self, "NodeDownTimerTask")
        self.test_log.debug("Initializing NodeDownTimerTask")
        self.node = node
        self.port = port
        self.timeout = timeout
        self.start_time = 0

    def call(self):
        self.test_log.debug("Starting execution of NodeDownTimerTask")
        end_task = time.time() + self.timeout
        while not self.completed and time.time() < end_task:
            if not self.port:
                try:
                    self.start_time = time.time()
                    response = os.system("ping -c 1 {} > /dev/null".format(
                        self.node))
                    if response != 0:
                        self.test_log.debug(
                            "Injected failure in {}. Caught due to ping"
                            .format(self.node))
                        self.complete_task()
                        self.set_result(True)
                        break
                except Exception as e:
                    self.test_log.warning("Unexpected exception: {}".format(e))
                    self.complete_task()
                    return True
                try:
                    self.start_time = time.time()
                    socket.socket().connect(("{}".format(self.node), 8091))
                    socket.socket().close()
                    socket.socket().connect(("{}".format(self.node), 11210))
                    socket.socket().close()
                except socket.error:
                    self.test_log.debug(
                        "Injected failure in {}. Caught due to ports"
                        .format(self.node))
                    self.complete_task()
                    return True
            else:
                try:
                    self.start_time = time.time()
                    socket.socket().connect(("{}".format(self.node),
                                             int(self.port)))
                    socket.socket().close()
                    socket.socket().connect(("{}".format(self.node), 11210))
                    socket.socket().close()
                except socket.error:
                    self.test_log.debug("Injected failure in {}"
                                        .format(self.node))
                    self.complete_task()
                    return True
        if time.time() >= end_task:
            self.complete_task()
            self.test_log.error("Could not inject failure in {}"
                                .format(self.node))
            return False


class Atomicity(Task):
    instances = 1
    num_items = 10000
    mutations = num_items
    start_from = 0
    op_type = "insert"
    persist_to = 1
    replicate_to = 1

    task_manager = []
    write_offset = []
    def __init__(self, cluster, task_manager, bucket, client, clients, generator, op_type, exp, flag=0,
                 persist_to=0, replicate_to=0, time_unit="seconds",
                 only_store_hash=True, batch_size=1, pause_secs=1, timeout_secs=5, compression=True,
                 process_concurrency=8, print_ops_rate=True, retries=5,update_count=1, transaction_timeout=5,
                 commit=True, durability=None, sync=True, num_threads=50):
        super(Atomicity, self).__init__("AtomicityDocumentsLoadGenTask")

        self.generators = generator
        self.cluster = cluster
        self.commit = commit
        Atomicity.create_threads = num_threads
        self.exp = exp
        self.flag = flag
        Atomicity.sync = sync
        self.persit_to = persist_to
        self.replicate_to = replicate_to
        self.time_unit = time_unit
        self.only_store_hash = only_store_hash
        self.pause_secs = pause_secs
        self.timeout_secs = timeout_secs
        Atomicity.transaction_timeout = transaction_timeout
        self.compression = compression
        self.process_concurrency = process_concurrency
        self.client = client
        Atomicity.update_keys = []
        Atomicity.delete_keys = []
        Atomicity.task_manager = task_manager
        self.batch_size = batch_size
        self.print_ops_rate = print_ops_rate
        self.retries = retries
        self.op_type = op_type
        self.bucket = bucket
        self.clients = clients
        Atomicity.clients = clients[0]
        Atomicity.generator = generator
        Atomicity.all_keys = []
        Atomicity.updatecount = update_count
        if durability == "MAJORITY":
            Atomicity.durability = 1
        elif durability == "MAJORITY_AND_PERSIST_ON_MASTER":
            Atomicity.durability = 2
        elif durability == "PERSIST_TO_MAJORITY":
            Atomicity.durability = 3
        elif durability == "ONLY_NONE":
            Atomicity.durability = 4
        else:
            Atomicity.durability = 0

    def call(self):
        tasks = []
        self.start_task()
        iterator = 0
        self.gen = []
        if self.op_type == "time_out":
            transaction_config = Transaction().createTransactionConfig(2, Atomicity.durability)
        else:
            self.test_log.info("transaction timeout is {}".format(Atomicity.transaction_timeout))
            transaction_config = Transaction().createTransactionConfig(Atomicity.transaction_timeout, Atomicity.durability)
        try:
            self.transaction = Transaction().createTansaction(self.client.cluster, transaction_config)
            self.test_log.info("transaction is {}".format(self.transaction))
        except Exception as e:
            self.set_exception(e)

        for generator in self.generators:
            tasks.extend(self.get_tasks(generator, 1))
            iterator += 1

        self.test_log.debug("going to add new task")
        for task in tasks:
            Atomicity.task_manager.add_new_task(task)

        for task in tasks:
            Atomicity.task_manager.get_task_result(task)

        time.sleep(60)
        self.transaction.close()

        tasks = []
        for generator in self.generators:
            tasks.extend(self.get_tasks(generator, 0))
            iterator += 1

        self.test_log.info("going to add verification task")
        for task in tasks:
            Atomicity.task_manager.add_new_task(task)
            Atomicity.task_manager.get_task_result(task)

        for con in self.clients:
            for client in con:
                client.close()

    def get_tasks(self, generator, load):
        generators = []
        tasks = []
        gen_start = int(generator.start)
        gen_end = max(int(generator.end), 1)

        if not load:
            self.process_concurrency = 1

        gen_range = max(int((generator.end - generator.start)/self.process_concurrency), 1)
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
        if load:
            for i in range(0, len(generators)):
                task = self.Loader(self.cluster, self.bucket[i], self.client, generators[i], self.op_type,
                                             self.exp, self.flag, persist_to=self.persit_to, replicate_to=self.replicate_to,
                                             time_unit=self.time_unit, batch_size=self.batch_size,
                                             pause_secs=self.pause_secs, timeout_secs=self.timeout_secs,
                                             compression=self.compression,
                                             instance_num = 1,transaction=self.transaction, commit=self.commit)
                tasks.append(task)
        else:
            for generator in generators:
                task = Atomicity.Validate(self.cluster, self.bucket, self.client, generator, self.op_type,
                                            self.exp, self.flag,  batch_size=self.batch_size,
                                            pause_secs=self.pause_secs, timeout_secs=self.timeout_secs,
                                            compression=self.compression)
                tasks.append(task)
        return tasks

    class Loader(GenericLoadingTask):
        '''
        1. Start inserting data into buckets
        2. Keep updating the write offset
        3. Start the reader thread
        4. Keep track of non durable documents
        '''
        def __init__(self, cluster, bucket, client, generator, op_type, exp,
                     flag=0, persist_to=0, replicate_to=0, time_unit="seconds",
                     batch_size=1, pause_secs=1, timeout_secs=5,
                     compression=True, throughput_concurrency=4, retries=5,
                     instance_num=0, transaction=None, commit=True):
            super(Atomicity.Loader, self).__init__(
                cluster, bucket, client, batch_size=batch_size,
                pause_secs=pause_secs,
                timeout_secs=timeout_secs, compression=compression,
                retries=retries, transaction=transaction, commit=commit)

            self.generator = generator
            self.op_type = []
            self.op_type.extend(op_type.split(';'))
            self.thread_name = "Atomicity_Loader_Task-{}_{}_{}" \
                               .format(generator._doc_gen.start,
                                       generator._doc_gen.end,
                                       op_type)
            self.commit = commit
            self.exp = exp
            self.flag = flag
            self.persist_to = persist_to
            self.replicate_to = replicate_to
            self.compression = compression
            self.pause_secs = pause_secs
            self.timeout_secs = timeout_secs
            self.time_unit = time_unit
            self.instance = instance_num
            self.transaction = transaction
            self.client = client
            self.bucket = bucket
            self.exp_unit = "seconds"

        def has_next(self):
            return self.generator.has_next()

        def call(self):
            self.start_task()
            self.test_log.info("Starting Atomicity load generation thread")
            exception = None
            first_batch = {}
            last_batch = {}
            self.all_keys = []
            self.update_keys = []
            self.delete_keys = []
            docs = []

            doc_gen = self.generator
            while self.has_next():
                self.key_value = doc_gen.next_batch()
                self._process_values_for_create(self.key_value)
                self.all_keys.extend(self.key_value.keys())

                for op_type in self.op_type:

                    if op_type == 'general_create':
                        for self.client in Atomicity.clients:
                            self.batch_create(
                                self.key_value, self.client,
                                persist_to=self.persist_to,
                                replicate_to=self.replicate_to,
                                timeout=self.timeout, time_unit=self.time_unit,
                                doc_type=self.generator.doc_type)

                for key, value in self.key_value.items():
                    content = self.__translate_to_json_object(value)
                    tuple = Tuples.of(key, content)
                    docs.append(tuple)

                last_batch = self.key_value

            Atomicity.all_keys.extend(self.all_keys)

            len_keys = len(self.all_keys)
            self.update_keys = random.sample(self.all_keys,
                                             random.randint(20, len_keys))
            self.delete_keys = random.sample(self.all_keys,
                                             random.randint(20, len_keys))

            for op_type in self.op_type:
                if op_type == "create":
                    if len(self.op_type) != 1:
                        commit = True
                    else:
                        commit = self.commit

                    docs = list(self.__chunks(docs, len(self.all_keys)/Atomicity.create_threads))
                    for doc in docs:
                        err = Transaction().RunTransaction(self.transaction, self.bucket, doc, [], [], commit, Atomicity.sync, Atomicity.updatecount )
                        if err:
                            exception = self.__retryDurabilityImpossibleException(err, doc, commit, op_type="create")
                    if not commit:
                        Atomicity.all_keys = []

                if op_type == "update":
                    list_docs = list(self.__chunks(self.update_keys, len(self.update_keys)/10))
                    for doc in list_docs:
                        err = Transaction().RunTransaction(self.transaction, self.bucket, [], doc, [], self.commit, Atomicity.sync, Atomicity.updatecount )
                        if err:
                            exception = self.__retryDurabilityImpossibleException(err, doc, self.commit, op_type="update")

                if op_type == "update_Rollback":
                    exception = Transaction().RunTransaction(self.transaction, self.bucket, [], self.update_keys, [], False, True, Atomicity.updatecount )

                if op_type == "delete":
                    self.test_log.info("delete keys count is {}".format(len(self.delete_keys)))
                    list_docs = list(self.__chunks(self.delete_keys, len(self.delete_keys)/10))
                    for doc in list_docs:
                        err = Transaction().RunTransaction(self.transaction, self.bucket, [], [], doc, self.commit, Atomicity.sync, Atomicity.updatecount )
                        if err:
                            exception = self.__retryDurabilityImpossibleException(err, doc, self.commit, op_type="delete")

                if op_type == "general_update":
                    for self.client in Atomicity.clients:
                        self.batch_update(last_batch, self.client, persist_to=self.persist_to, replicate_to=self.replicate_to,
                                  timeout=self.timeout, time_unit=self.time_unit, doc_type=self.generator.doc_type)
                    self.update_keys = last_batch.keys()
                    self.commit = True

                if op_type == "general_delete":
                    self.test_log.debug("performing delete for keys {}".format(last_batch.keys()))
                    for self.client in Atomicity.clients:
                        keys = self.batch_delete(last_batch, self.client, persist_to=self.persist_to,
                                              replicate_to=self.replicate_to,
                                              timeout=self.timeout,
                                              timeunit=self.time_unit,
                                              durability="")
                    self.delete_keys = last_batch.keys()
                    self.commit = True

                if op_type == "create_update":
                    exception = Transaction().RunTransaction(self.transaction, self.bucket, docs, self.update_keys, [], self.commit, True, Atomicity.updatecount)

                if op_type == "rebalance_update":
                    self.update_keys = self.all_keys
                    err = Transaction().RunTransaction(self.transaction, self.bucket, docs, self.all_keys, [], self.commit, True, Atomicity.updatecount)
                    if err:
                        exception = self.__retryDurabilityImpossibleException(err, docs, self.commit, op_type="create", update_keys=self.all_keys)

                if op_type == "rebalance_only_update":
                    self.update_keys = self.all_keys
                    err = Transaction().RunTransaction(self.transaction, self.bucket, [], self.all_keys, [], self.commit, Atomicity.sync, Atomicity.updatecount)
                    if err:
                        exception = self.__retryDurabilityImpossibleException(err, self.all_keys, self.commit, op_type="update")

                if op_type == "rebalance_delete":
                    self.delete_keys = self.all_keys
                    err = Transaction().RunTransaction(self.transaction, self.bucket, [], [],  self.all_keys, self.commit, Atomicity.sync, Atomicity.updatecount)
                    if err:
                        exception = self.__retryDurabilityImpossibleException(err, Atomicity.delete_keys, self.commit, op_type="delete")

                if op_type == "time_out":
                    err = Transaction().RunTransaction(self.transaction, self.bucket, docs, [], [], True, True, Atomicity.updatecount )
                    if "AttemptExpired" in str(err):
                        self.test_log.info("Transaction Expired as Expected")
                        for line in err:
                            self.test_log.info("{}".format(line))
                        self.test_log.debug("End of First Transaction that is getting timeout")
                    else:
                        exception = err

                if ("update" in op_type) and self.commit:
                    Atomicity.update_keys.extend(self.update_keys)

                if ("delete" in op_type) and self.commit:
                    Atomicity.delete_keys.extend(self.delete_keys)

                if exception:
                    self.set_exception(Exception(exception))
                    break

            self.test_log.info("Atomicity Load generation thread completed")

            self.complete_task()

        def __chunks(self, l, n):
            """Yield successive n-sized chunks from l."""
            for i in range(0, len(l), n):
                yield l[i:i + n]

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

        def __retryDurabilityImpossibleException(self, err, docs, commit, op_type="update", update_keys=[]):
            if "DurabilityImpossibleException" in str(err):
                self.test_log.info("DurabilityImpossibleException seen so retrying")
                n=5
                while n > 0:
                    n -= 1
                    time.sleep(30)
                    if op_type == "create":
                        err = Transaction().RunTransaction(self.transaction, self.bucket, docs, update_keys, [], commit, Atomicity.sync, Atomicity.updatecount)
                    if "update" in op_type:
                        err = Transaction().RunTransaction(self.transaction, self.bucket, [], docs, [], commit, Atomicity.sync, Atomicity.updatecount)
                    if "delete" in op_type:
                        err = Transaction().RunTransaction(self.transaction, self.bucket, [], [], docs, commit, Atomicity.sync, Atomicity.updatecount)
                    if "DurabilityImpossibleException" in str(err):
                        self.test_log.info("DurabilityImpossibleException seen so retrying")
                    else:
                        break
            return err

    class Validate(GenericLoadingTask):

        def __init__(self, cluster, bucket, client, generator, op_type, exp,
                     flag=0, proxy_client=None, batch_size=1, pause_secs=1,
                     timeout_secs=30, compression=True):
            super(Atomicity.Validate, self).__init__(cluster, bucket, client,
                                                     batch_size=batch_size,
                                                     pause_secs=pause_secs,
                                                     timeout_secs=timeout_secs,
                                                     compression=compression)

            self.generator = generator
            self.op_type = op_type
            self.exp = exp
            self.flag = flag
            self.updated_keys = Atomicity.update_keys
            self.delete_keys = Atomicity.delete_keys
            self.mutate_value = Atomicity.updatecount
            self.all_keys = {}
            for client in Atomicity.clients:
                self.all_keys[client] = []
                self.all_keys[client].extend(Atomicity.all_keys)
                self.test_log.info("the length of the keys {}"
                                   .format(len(self.all_keys[client])))

            if proxy_client:
                self.test_log.info("Changing client to proxy %s:%s..."
                                   % (proxy_client.host, proxy_client.port))
                self.client = proxy_client

        def has_next(self):
            return self.generator.has_next()

        def call(self):
            self.start_task()
            self.test_log.info("Starting Atomicity Verification thread")
            if len(Atomicity.all_keys) > 0 :
                doc_gen = self.generator
                while self.has_next():
                    key_value = doc_gen.next_batch()
                    self.process_values_for_verification(key_value)
                    for client in Atomicity.clients:
                        result_map = self.batch_read(key_value.keys(), client)
                        wrong_values = self.validate_key_val(result_map[0], key_value, client)

                        if wrong_values:
                            self.set_exception("Wrong key value. "
                                               "Wrong key value: {}"
                                               .format(','.join(wrong_values)))

                for key in self.delete_keys:
                    for client in Atomicity.clients:
                        if key in self.all_keys[client]:
                            self.all_keys[client].remove(key)

                for client in Atomicity.clients:
                    if self.all_keys[client] and "time_out" not in self.op_type:
                        self.set_exception("Keys were missing. "
                                           "Keys missing: {}"
                                           .format(','.join(self.all_keys[client])))

            self.test_log.info("Completed Atomicity Verification thread")
            self.complete_task()

        def validate_key_val(self, map, key_value, client):
            wrong_values = []
            for key, value in key_value.items():
                if key in map:
                    if self.op_type == "time_out":
                        expected_val = {}
                    else:
                        expected_val = Json.loads(value)
                    actual_val = {}
                    if map[key]['cas'] != 0:
                        actual_val = Json.loads(map[key]['value'].toString())
                    elif map[key]['error'] is not None:
                        actual_val = map[key]['error'].toString()
                    if expected_val == actual_val or map[key]['cas'] == 0:
                        self.all_keys[client].remove(key)
                    else:
                        wrong_values.append(key)
                        self.test_log.info("actual value for key {} is {}".format(key,actual_val))
                        self.test_log.info("expected value for key {} is {}".format(key,expected_val))
            return wrong_values

        def process_values_for_verification(self, key_val):
            self._process_values_for_create(key_val)
            for key, value in key_val.items():
                if key in self.updated_keys or self.op_type =="verify":
                    try:
                        value = key_val[key]  # new updated value, however it is not their in orginal code "LoadDocumentsTask"
                        value_json = Json.loads(value)
                        value_json['mutated'] += self.mutate_value
                        value = Json.dumps(value_json)
                    except ValueError:
                        self.random.seed(key)
                        index = self.random.choice(range(len(value)))
                        value = value[0:index] + self.random.choice(string.ascii_uppercase) + value[index + 1:]
                    finally:
                        key_val[key] = value


class MonitorViewFragmentationTask(Task):
    """
        Attempt to monitor fragmentation that is occurring for a given design_doc.
        execute stage is just for preliminary sanity checking of values and environment.
        Check function looks at index file accross all nodes and attempts to calculate
        total fragmentation occurring by the views within the design_doc.
        Note: If autocompaction is enabled and user attempts to monitor for fragmentation
        value higher than level at which auto_compaction kicks in a warning is sent and
        it is best user to use lower value as this can lead to infinite monitoring.
    """

    def __init__(self, server, design_doc_name, fragmentation_value=10, bucket="default"):

        Task.__init__(self, "monitor_frag_task")
        self.server = server
        self.bucket = bucket
        self.fragmentation_value = fragmentation_value
        self.design_doc_name = design_doc_name
        self.result = False

    def call(self):
        self.start_task()
        # sanity check of fragmentation value
        print "self.fragmentation_value: %s" % self.fragmentation_value
        if self.fragmentation_value < 0 or self.fragmentation_value > 100:
            err_msg = "Invalid value for fragmentation %d" % self.fragmentation_value
            self.set_exception(Exception(err_msg))

        try:
            auto_compact_percentage = self._get_current_auto_compaction_percentage()
            if auto_compact_percentage != "undefined" and auto_compact_percentage < self.fragmentation_value:
                self.log.warn("Auto compaction is set to %s. Therefore fragmentation_value %s may not be reached" % (auto_compact_percentage, self.fragmentation_value))

        except GetBucketInfoFailed as e:
            self.set_exception(e)
        except Exception as e:
            self.set_exception(e)
        self.check()
        self.complete_task()

    def _get_current_auto_compaction_percentage(self):
        """ check at bucket level and cluster level for compaction percentage """

        auto_compact_percentage = None
        rest = BucketHelper(self.server)

        content = rest.get_bucket_json(self.bucket)
        if content["autoCompactionSettings"] is False:
            # try to read cluster level compaction settings
            content = rest.cluster_status()

        auto_compact_percentage = content["autoCompactionSettings"]["viewFragmentationThreshold"]["percentage"]

        return auto_compact_percentage

    def check(self):

        rest = RestConnection(self.server)
        new_frag_value = 0
        timeout = 300
        while new_frag_value < self.fragmentation_value and timeout > 0:
            new_frag_value = MonitorViewFragmentationTask.calc_ddoc_fragmentation(
                rest, self.design_doc_name, bucket=self.bucket)
            self.log.info("%s: current amount of fragmentation = %d, \
            required: %d" % (self.design_doc_name,
                             new_frag_value, self.fragmentation_value))
            if new_frag_value > self.fragmentation_value:
                self.result = True
                break
            timeout -= 1
            sleep(1)

    @staticmethod
    def aggregate_ddoc_info(rest, design_doc_name, bucket="default",
                            with_rebalance=False):

        nodes = rest.node_statuses()
        info = []
        for node in nodes:
            server_info = {"ip": node.ip,
                           "port": node.port,
                           "username": rest.username,
                           "password": rest.password}
            rest = RestConnection(server_info)
            status = False
            try:
                status, content = rest.set_view_info(bucket, design_doc_name)
            except Exception as e:
                print(str(e))
                if "Error occured reading set_view _info" in str(e) and with_rebalance:
                    print("node {0} {1} is not ready yet?: {2}".format(
                                    node.id, node.port, e.message))
                else:
                    raise e
            if status:
                info.append(content)
        return info

    @staticmethod
    def calc_ddoc_fragmentation(rest, design_doc_name, bucket="default", with_rebalance=False):

        total_disk_size = 0
        total_data_size = 0
        total_fragmentation = 0
        nodes_ddoc_info = \
            MonitorViewFragmentationTask.aggregate_ddoc_info(rest,
                                                         design_doc_name,
                                                         bucket, with_rebalance)
        total_disk_size = sum([content['disk_size'] for content in nodes_ddoc_info])
        total_data_size = sum([content['data_size'] for content in nodes_ddoc_info])

        if total_disk_size > 0 and total_data_size > 0:
            total_fragmentation = \
                (total_disk_size - total_data_size) / float(total_disk_size) * 100

        return total_fragmentation


class ViewCompactionTask(Task):

    """
        Executes view compaction for a given design doc. This is technicially view compaction
        as represented by the api and also because the fragmentation is generated by the
        keys emitted by map/reduce functions within views.  Task will check that compaction
        history for design doc is incremented and if any work was really done.
    """

    def __init__(self, server, design_doc_name, bucket="default", with_rebalance=False):

        Task.__init__(self, "view_compaction_task")
        self.server = server
        self.bucket = bucket
        self.design_doc_name = design_doc_name
        self.ddoc_id = "_design%2f" + design_doc_name
        self.compaction_revision = 0
        self.precompacted_fragmentation = 0
        self.with_rebalance = with_rebalance
        self.rest = RestConnection(self.server)
        self.result = False

    def call(self):
        try:
            self.compaction_revision, self.precompacted_fragmentation = \
                self._get_compaction_details()
            self.log.info("{0}: stats compaction before triggering it: ({1},{2})".
                          format(self.design_doc_name,
                                 self.compaction_revision, self.precompacted_fragmentation))
            if self.precompacted_fragmentation == 0:
                self.log.info("%s: There is nothing to compact, fragmentation is 0" %
                              self.design_doc_name)
                self.set_result(False)
                return
            self.rest.ddoc_compaction(self.ddoc_id, self.bucket)
            self.check()
        except (CompactViewFailed, SetViewInfoNotFound) as ex:
            self.result = False
            self.set_exception(ex)
        # catch and set all unexpected exceptions
        except Exception as e:
            self.result = False
            self.set_exception(e)

    # verify compaction history incremented and some defraging occurred
    def check(self):

        try:
            _compaction_running = self._is_compacting()
            new_compaction_revision, fragmentation = self._get_compaction_details()
            self.log.info("{0}: stats compaction:revision and fragmentation: ({1},{2})".
                          format(self.design_doc_name,
                                 new_compaction_revision, fragmentation))

            if new_compaction_revision == self.compaction_revision and _compaction_running :
                # compaction ran successfully but compaction was not changed
                # perhaps we are still compacting
                self.log.info("design doc {0} is compacting".format(self.design_doc_name))
                self.check()
            elif new_compaction_revision > self.compaction_revision or\
                 self.precompacted_fragmentation > fragmentation:
                self.log.info("{1}: compactor was run, compaction revision was changed on {0}".format(new_compaction_revision,
                                                                                                      self.design_doc_name))
                frag_val_diff = fragmentation - self.precompacted_fragmentation
                self.log.info("%s: fragmentation went from %d to %d" % \
                              (self.design_doc_name,
                               self.precompacted_fragmentation, fragmentation))

                if frag_val_diff > 0:

                    # compaction ran successfully but datasize still same
                    # perhaps we are still compacting
                    if self._is_compacting():
                        self.check()
                    self.log.info("compaction was completed, but fragmentation value {0} is more than before compaction {1}".
                                  format(fragmentation, self.precompacted_fragmentation))
                    # probably we already compacted, but no work needed to be done
                    self.set_result(self.with_rebalance)
                else:
                    self.set_result(True)
            else:
                # Sometimes the compacting is not started immediately
                for i in xrange(17):
                    time.sleep(3)
                    if self._is_compacting():
                        self.check()
                    else:
                        new_compaction_revision, fragmentation = self._get_compaction_details()
                        self.log.info("{2}: stats compaction: ({0},{1})".
                          format(new_compaction_revision, fragmentation,
                                 self.design_doc_name))
                        # case of rebalance when with concurrent updates it's possible that
                        # compaction value has not changed significantly
                        if new_compaction_revision > self.compaction_revision and self.with_rebalance:
                            self.log.info("the compaction revision was increased")
                            self.set_result(True)
                            return
                        else:
                            continue
                # print details in case of failure
                self.log.info("design doc {0} is compacting:{1}".format(self.design_doc_name, self._is_compacting()))
                new_compaction_revision, fragmentation = self._get_compaction_details()
                self.log.error("stats compaction still: ({0},{1})".
                          format(new_compaction_revision, fragmentation))
                status, content = self.rest.set_view_info(self.bucket, self.design_doc_name)
                stats = content["stats"]
                self.log.warn("general compaction stats:{0}".format(stats))
                self.set_exception(Exception("Check system logs, looks like compaction failed to start"))

        except (SetViewInfoNotFound) as ex:
            self.result = False
            self.set_exception(ex)
        # catch and set all unexpected exceptions
        except Exception as e:
            self.result = False
            self.set_exception(e)

    def _get_compaction_details(self):
        status, content = self.rest.set_view_info(self.bucket, self.design_doc_name)
        curr_no_of_compactions = content["stats"]["compactions"]
        curr_ddoc_fragemtation = \
            MonitorViewFragmentationTask.calc_ddoc_fragmentation(self.rest, self.design_doc_name, self.bucket, self.with_rebalance)
        return (curr_no_of_compactions, curr_ddoc_fragemtation)

    def _is_compacting(self):
        status, content = self.rest.set_view_info(self.bucket, self.design_doc_name)
        return content["compact_running"] == True


class MonitorBucketCompaction(Task):
    """
    Monitors bucket compaction status from start to complete
    """
    def __init__(self, cluster, bucket, timeout=300):
        """
        :param cluster: Couchbase cluster object
        :param bucket: Bucket object
        :param timeout: Timeout value in seconds
        """
        super(MonitorBucketCompaction, self).__init__("CompactionTask_%s"
                                                      % bucket.name)
        self.bucket = bucket
        self.cluster = cluster
        self.status = "NOT_STARTED"
        self.progress = 0
        self.timeout = timeout
        self.rest = RestConnection(self.cluster.master)

    def call(self):
        self.start_task()
        start_time = time.time()
        stop_time = start_time + self.timeout

        # Wait for compaction to start
        while self.status != "RUNNING":
            now = time.time()
            if self.timeout > 0 and now > stop_time:
                self.set_exception("Compaction start timed out")
                break

            status, self.progress = \
                self.rest.check_compaction_status(self.bucket.name)
            if status is True:
                self.status = "RUNNING"
                self.test_log.info("Compaction started for %s"
                                   % self.bucket.name)
                self.test_log.debug("%s compaction done: %s%%"
                                    % (self.bucket.name, self.progress))

        start_time = time.time()
        stop_time = start_time + self.timeout
        # Wait for compaction to complete
        while self.status == "RUNNING" and self.status != "COMPLETED":
            now = time.time()
            if self.timeout > 0 and now > stop_time:
                self.set_exception("Compaction timed out to complete with "
                                   "%s seconds" % self.timeout)
                break

            status, self.progress = \
                self.rest.check_compaction_status(self.bucket.name)
            if status is False:
                self.progress = 100
                self.status = "COMPLETED"
                self.test_log.info("Compaction completed for %s"
                                   % self.bucket.name)
            self.test_log.debug("%s compaction done: %s%%"
                                % (self.bucket.name, self.progress))

        self.complete_task()
