import os
import time
import logging
import random
import socket
import string
import copy
import json
import re
import math
import crc32
import traceback
from httplib import IncompleteRead
from threading import Thread
# from memcacheConstants import ERR_NOT_FOUND,NotFoundError
from membase.api.rest_client import RestConnection, RestHelper
from membase.api.exception import BucketCreationException
from memcached.helper.data_helper import MemcachedClientHelper
from memcached.helper.kvstore import KVStore
from mc_bin_client import MemcachedError
from java.util.concurrent import Callable, TimeUnit, CancellationException, TimeoutException
# from couchbase_helper.stats_tools import StatsCommon
from membase.api.exception import N1QLQueryException, DropIndexException, CreateIndexException, DesignDocCreationException, QueryViewException, ReadDocumentException, RebalanceFailedException, \
                                    GetBucketInfoFailed, CompactViewFailed, SetViewInfoNotFound, FailoverFailedException, \
                                    ServerUnavailableException, BucketFlushFailed, CBRecoveryFailedException, BucketCompactionException, AutoFailoverException
from remote.remote_util import RemoteMachineShellConnection, RemoteUtilHelper
from couchbase_helper.documentgenerator import BatchedDocumentGenerator
from TestInput import TestInputServer, TestInputSingleton
from testconstants import MIN_KV_QUOTA, INDEX_QUOTA, FTS_QUOTA, COUCHBASE_FROM_4DOT6, THROUGHPUT_CONCURRENCY, ALLOW_HTP, CBAS_QUOTA, COUCHBASE_FROM_VERSION_4
from BucketLib.BucketOperations import BucketHelper
from BucketLib.MemcachedOperations import MemcachedHelper
import testconstants
import sys, traceback

# try:
#     CHECK_FLAG = False
#     if (testconstants.TESTRUNNER_CLIENT in os.environ.keys()) and os.environ[testconstants.TESTRUNNER_CLIENT] == testconstants.PYTHON_SDK:
#         from sdk_client import SDKSmartClient as VBucketAwareMemcached
#         from sdk_client import SDKBasedKVStoreAwareSmartClient as KVStoreAwareSmartClient
#     if (testconstants.TESTRUNNER_CLIENT in os.environ.keys()) and os.environ[testconstants.TESTRUNNER_CLIENT] == testconstants.JAVA_SDK:
#         from java_sdk_client import SDKSmartClient as VBucketAwareMemcached
#         from java_sdk_client import SDKBasedKVStoreAwareSmartClient as KVStoreAwareSmartClient
#     else:
#         CHECK_FLAG = True
#         from memcached.helper.data_helper import VBucketAwareMemcached,KVStoreAwareSmartClient
# except Exception as e:
#     CHECK_FLAG = True
#     from memcached.helper.data_helper import VBucketAwareMemcached,KVStoreAwareSmartClient
#from sdk_client import SDKSmartClient as VBucketAwareMemcached

from sdk_client import SDKSmartClient as VBucketAwareMemcached
CHECK_FLAG = False

PENDING = 'PENDING'
EXECUTING = 'EXECUTING'
CHECKING = 'CHECKING'
FINISHED = 'FINISHED'
CANCELLED = 'CANCELLED'

log = logging.getLogger(__name__)


class Task(Callable):
    def __init__(self, name, task_manager):
        self.name = name
        self.task_manager = task_manager
        self.state = EXECUTING
        self.result = None
        self.future = None
        log.info("*** TASK {0} Scheduled...".format(self.name))

    def call(self):
        log.debug("*** TASK  {0} in progress...".format(self.name))
        if self.state == PENDING:
            self.state = EXECUTING
            self.future = self.task_manager.schedule(self)
        if self.state == EXECUTING:
            self.execute()
        elif self.state == CHECKING:
            self.check()
        elif self.state != FINISHED:
            raise Exception("Bad State in {0}: {1}".format(self.name,
                                                           self.state))

    def execute(self):
        raise NotImplementedError

    def check(self):
        raise NotImplementedError

    def set_result(self, result):
        self.result = result

    def get_result(self, timeout=None):
        try:
            if timeout:
                self.future.get(timeout, TimeUnit.SECONDS)
            else:
                self.future.get()
            if self.state == FINISHED:
                log.info("*** TASK  {0} Finished...".format(self.name))
                return self.result
        except CancellationException, ex:
            self.state = CANCELLED
            self.result = False
        except TimeoutException, ex:
            log.error("Task Timed Out")

    def cancel(self, interrupt_if_running=True):
        check = self.future.cancel(interrupt_if_running)
        if check:
            self.state = CANCELLED
            raise CancellationException
        elif self.future.isDone():
            self.state = FINISHED

    def cancelled(self):
        return self.future.isCancelled()

    def set_unexpected_exception(self, e, suffix=""):
        log.error("Unexpected exception [{0}] caught".format(e) + suffix)
        log.error(''.join(traceback.format_stack()))

    @staticmethod
    def wait_until(value_getter, condition, timeout_secs=-1):
        """
        Repeatedly calls value_getter returning the value when it
        satisfies condition. Calls to value getter back off exponentially.
        Useful if you simply want to synchronously wait for a condition to be
        satisfied.

        :param value_getter: no-arg function that gets a value
        :param condition: single-arg function that tests the value
        :param timeout_secs: number of seconds after which to timeout; if negative
                             waits forever; default is to wait forever
        :return: the value returned by value_getter
        :raises: TimeoutError if the operation times out before
                 getting a value that satisfies condition
        """
        start_time = time.time()
        stop_time = start_time + max(timeout_secs, 0)
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
                raise TimeoutException('Timed out after {0} seconds and {1} attempts'
                                   .format(now - start_time, attempt))
        return value

class NodeInitializeTask(Task):
    def __init__(self, server, task_manager, disabled_consistent_view=None,
                 rebalanceIndexWaitingDisabled=None,
                 rebalanceIndexPausingDisabled=None,
                 maxParallelIndexers=None,
                 maxParallelReplicaIndexers=None,
                 port=None, quota_percent=None,
                 index_quota_percent=None,
                 services = None, gsi_type='forestdb'):
        Task.__init__(self, name="node_init_task", task_manager=task_manager)
        self.server = server
        self.port = port or server.port
        self.quota = 0
        self.index_quota = 0
        self.index_quota_percent = index_quota_percent
        self.quota_percent = quota_percent
        self.disable_consistent_view = disabled_consistent_view
        self.rebalanceIndexWaitingDisabled = rebalanceIndexWaitingDisabled
        self.rebalanceIndexPausingDisabled = rebalanceIndexPausingDisabled
        self.maxParallelIndexers = maxParallelIndexers
        self.maxParallelReplicaIndexers = maxParallelReplicaIndexers
        self.services = services
        self.gsi_type = gsi_type

    def execute(self):
        try:
            rest = RestConnection(self.server)
        except ServerUnavailableException as error:
                self.state = FINISHED
                self. set_unexpected_exception(error)
                return
        info = Task.wait_until(lambda: rest.get_nodes_self(),
                                 lambda x: x.memoryTotal > 0, 10)
        log.info("server: %s, nodes/self: %s", self.server, info.__dict__)

        username = self.server.rest_username
        password = self.server.rest_password

        if int(info.port) in range(9091,9991):
            self.state = FINISHED
            self.set_result(True)
            return

        self.quota = int(info.mcdMemoryReserved * 2/3)
        if self.index_quota_percent:
            self.index_quota = int((info.mcdMemoryReserved * 2/3) * \
                                      self.index_quota_percent / 100)
            rest.set_service_memoryQuota(service='indexMemoryQuota', username=username, password=password, memoryQuota=self.index_quota)
        if self.quota_percent:
            self.quota = int(info.mcdMemoryReserved * self.quota_percent / 100)

        """ Adjust KV RAM to correct value when there is INDEX
            and FTS services added to node from Watson  """
        index_quota = INDEX_QUOTA
        kv_quota = int(info.mcdMemoryReserved * 2/3)
        if self.index_quota_percent:
                index_quota = self.index_quota
        if not self.quota_percent:
            set_services = copy.deepcopy(self.services)
            if set_services is None:
                set_services = ["kv"]
#             info = rest.get_nodes_self()
#             cb_version = info.version[:5]
#             if cb_version in COUCHBASE_FROM_VERSION_4:
            if "index" in set_services:
                log.info("quota for index service will be %s MB" % (index_quota))
                kv_quota -= index_quota
                log.info("set index quota to node %s " % self.server.ip)
                rest.set_service_memoryQuota(service='indexMemoryQuota', memoryQuota=index_quota)
            if "fts" in set_services:
                log.info("quota for fts service will be %s MB" % (FTS_QUOTA))
                kv_quota -= FTS_QUOTA
                log.info("set both index and fts quota at node %s "% self.server.ip)
                rest.set_service_memoryQuota(service='ftsMemoryQuota', memoryQuota=FTS_QUOTA)
            if "cbas" in set_services:
                log.info("quota for cbas service will be %s MB" % (CBAS_QUOTA))
                kv_quota -= CBAS_QUOTA
                rest.set_service_memoryQuota(service = "cbasMemoryQuota", memoryQuota=CBAS_QUOTA)
            if kv_quota < MIN_KV_QUOTA:
                    raise Exception("KV RAM needs to be more than %s MB"
                            " at node  %s"  % (MIN_KV_QUOTA, self.server.ip))
            if kv_quota < int(self.quota):
                self.quota = kv_quota

        rest.init_cluster_memoryQuota(username, password, self.quota)
        rest.set_indexer_storage_mode(username, password, self.gsi_type)

        if self.services:
            status = rest.init_node_services(username= username, password = password,\
                                          port = self.port, hostname= self.server.ip,\
                                                              services= self.services)
            if not status:
                self.state = FINISHED
                self. set_unexpected_exception(Exception('unable to set services for server %s'\
                                                               % (self.server.ip)))
                return
        if self.disable_consistent_view is not None:
            rest.set_reb_cons_view(self.disable_consistent_view)
        if self.rebalanceIndexWaitingDisabled is not None:
            rest.set_reb_index_waiting(self.rebalanceIndexWaitingDisabled)
        if self.rebalanceIndexPausingDisabled is not None:
            rest.set_rebalance_index_pausing(self.rebalanceIndexPausingDisabled)
        if self.maxParallelIndexers is not None:
            rest.set_max_parallel_indexers(self.maxParallelIndexers)
        if self.maxParallelReplicaIndexers is not None:
            rest.set_max_parallel_replica_indexers(self.maxParallelReplicaIndexers)

        rest.init_cluster(username, password, self.port)
        self.server.port = self.port
        try:
            rest = RestConnection(self.server)
        except ServerUnavailableException as error:
                self.state = FINISHED
                self. set_unexpected_exception(error)
                return
        info = rest.get_nodes_self()

        if info is None:
            self.state = FINISHED
            self. set_unexpected_exception(Exception('unable to get information on a server %s, it is available?' % (self.server.ip)))
            return
        self.state = CHECKING
        self.call()

    def check(self):
        self.state = FINISHED
        self.set_result(self.quota)

class BucketCreateTask(Task):
    def __init__(self, server, bucket, task_manager):
        Task.__init__(self, "bucket_create_task", task_manager=task_manager)
        self.server = server
        self.bucket = bucket
        if self.bucket.priority is None or self.bucket.priority.lower() is 'low':
            self.bucket_priority = 3
        else:
            self.bucket_priority = 8

    def execute(self):
        try:
            rest = RestConnection(self.server)
        except ServerUnavailableException as error:
                self.state = FINISHED
                self. set_unexpected_exception(error)
                return
        info = rest.get_nodes_self()
        if int(info.port) in xrange(9091, 9991):
            self.port = info.port
            
        if self.bucket.ramQuotaMB <= 0:
            self.size = info.memoryQuota * 2 / 3

        if int(info.port) in xrange(9091, 9991):
            try:
                BucketHelper(self.server).create_bucket(self.bucket.__dict__)
                self.state = CHECKING
                self.call()
            except Exception as e:
                log.info(str(e))
                self.state = FINISHED
                #self. set_unexpected_exception(e)
            return
        version = rest.get_nodes_self().version
        try:
            if float(version[:2]) >= 3.0 and self.bucket_priority is not None:
                self.bucket.threadsNumber = self.bucket_priority
            BucketHelper(self.server).create_bucket(self.bucket.__dict__)
            self.state = CHECKING
            self.call()

        except BucketCreationException as e:
            self.state = FINISHED
            log.info(str(e))
            #self. set_unexpected_exception(e)
        # catch and set all unexpected exceptions
        except Exception as e:
            log.info(str(e))
            self.state = FINISHED
            #self.set_unexpected_exception(e)

    def check(self):
        try:
            if self.bucket.bucketType == 'memcached' or int(self.port) in xrange(9091, 9991):
                self.set_result(True)
                self.state = FINISHED
                return
            if MemcachedHelper.wait_for_memcached(self.server, self.bucket.name):
                log.info("bucket '{0}' was created with per node RAM quota: {1}".format(self.bucket, self.bucket.ramQuotaMB))
                self.set_result(True)
                self.state = FINISHED
                return
            else:
                log.warn("vbucket map not ready after try {0}".format(self.retries))
                if self.retries >= 5:
                    self.set_result(False)
                    self.state = FINISHED
                    return
        except Exception as e:
            log.error("Unexpected error: %s" % str(e))
            log.warn("vbucket map not ready after try {0}".format(self.retries))
            if self.retries >= 5:
                self.state = FINISHED
                self. set_unexpected_exception(e)
        self.retries = self.retries + 1
        self.call()

class RebalanceTask(Task):
    def __init__(self, servers, task_manager, to_add=[], to_remove=[], do_stop=False, progress=30,
                 use_hostnames=False, services = None, check_vbucket_shuffling=True):
        Task.__init__(self, "rebalance_task", task_manager=task_manager)
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
            self.state = FINISHED
            self. set_unexpected_exception(e)
        self.retry_get_progress = 0
        self.use_hostnames = use_hostnames
        self.previous_progress = 0
        self.old_vbuckets = {}

    def execute(self):
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
                        key = "{0}:{1}".format(remove_node.ip,remove_node.port)
                        services = services_map[key]
                        if "kv" not in services:
                            self.monitor_vbuckets_shuffling = False
                if self.monitor_vbuckets_shuffling:
                    log.info("This is swap rebalance and we will monitor vbuckets shuffling")
            self.add_nodes()
            self.start_rebalance()
            self.state = CHECKING
            self.call()
            #self.task_manager.schedule(self)
        except Exception as e:
            self.state = FINISHED
            log.info(str(e))
            #self. set_unexpected_exception(e)

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
                                   node.hostname, node.port, services = services_for_node)
            else:
                self.rest.add_node(master.rest_username, master.rest_password,
                                   node.ip, node.port, services = services_for_node)

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
                        if not(len(self.old_vbuckets[srv][vb_type]) + 1 >= len(new_vbuckets[srv][vb_type]) and\
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
            self.state = FINISHED
            self. set_unexpected_exception(ex)
            self.retry_get_progress += 1
        # catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            self.set_unexpected_exception(e, " in {0} sec".format(time.time() - self.start_time))
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
                self.call()
                #self.task_manager.schedule(self, 10)
            else:
                self.state = FINISHED
                #self.set_result(False)
                self.rest.print_UI_logs()
                self. set_unexpected_exception(RebalanceFailedException(\
                                "seems like rebalance hangs. please check logs!"))
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
                log.error("node {0}:{1} was not cleaned after removing from cluster"\
                                                              .format(node.ip, node.port))
                result = False

            log.info("rebalancing was completed with progress: {0}% in {1} sec".
                          format(progress, time.time() - self.start_time))
            self.state = FINISHED
            self.set_result(result)

class StatsWaitTask(Task):
    EQUAL = '=='
    NOT_EQUAL = '!='
    LESS_THAN = '<'
    LESS_THAN_EQ = '<='
    GREATER_THAN = '>'
    GREATER_THAN_EQ = '>='

    def __init__(self, servers, bucket, param, stat, comparison, value, task_manager):
        Task.__init__(self, "stats_wait_task", task_manager=task_manager)
        self.servers = servers
        self.bucket = bucket
        if isinstance(bucket, Bucket):
            self.bucket = bucket.name
        self.param = param
        self.stat = stat
        self.comparison = comparison
        self.value = value
        self.conns = {}

    def execute(self):
        self.state = CHECKING
        self.call()

    def check(self):
        stat_result = 0
        for server in self.servers:
            try:
                client = self._get_connection(server)
                stats = client.stats(self.param)
                if not stats.has_key(self.stat):
                    self.state = FINISHED
                    self. set_unexpected_exception(Exception("Stat {0} not found".format(self.stat)))
                    return
                if stats[self.stat].isdigit():
                    stat_result += long(stats[self.stat])
                else:
                    stat_result = stats[self.stat]
            except EOFError as ex:
                self.state = FINISHED
                self. set_unexpected_exception(ex)
                return
        if not self._compare(self.comparison, str(stat_result), self.value):
            log.warn("Not Ready: %s %s %s %s expected on %s, %s bucket" % (self.stat, stat_result,
                      self.comparison, self.value, self._stringify_servers(), self.bucket))
            self.task_manager.schedule(self, 5)
            return
        log.info("Saw %s %s %s %s expected on %s,%s bucket" % (self.stat, stat_result,
                      self.comparison, self.value, self._stringify_servers(), self.bucket))

        for server, conn in self.conns.items():
            conn.close()
        self.state = FINISHED
        self.set_result(True)

    def _stringify_servers(self):
        return ''.join([`server.ip + ":" + str(server.port)` for server in self.servers])

    def _get_connection(self, server, admin_user='cbadminbucket',admin_pass='password'):
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
        if (cmp_type == StatsWaitTask.EQUAL and a == b) or\
            (cmp_type == StatsWaitTask.NOT_EQUAL and a != b) or\
            (cmp_type == StatsWaitTask.LESS_THAN_EQ and a <= b) or\
            (cmp_type == StatsWaitTask.GREATER_THAN_EQ and a >= b) or\
            (cmp_type == StatsWaitTask.LESS_THAN and a < b) or\
            (cmp_type == StatsWaitTask.GREATER_THAN and a > b):
            return True
        return False

class GenericLoadingTask(Thread, Task):
    def __init__(self, server, task_manager, bucket, kv_store, batch_size=1, pause_secs=1, timeout_secs=60, compression=True):
        Thread.__init__(self)
        Task.__init__(self, "load_gen_task", task_manager=task_manager)
        self.kv_store = kv_store
        self.batch_size = batch_size
        self.pause = pause_secs
        self.timeout = timeout_secs
        self.server = server
        self.bucket = bucket
        if CHECK_FLAG:
            self.client = VBucketAwareMemcached(RestConnection(server), bucket, info=self.server)
        else:
            self.client = VBucketAwareMemcached(RestConnection(server), bucket, info=self.server, compression=compression)
        self.process_concurrency = THROUGHPUT_CONCURRENCY
        # task queue's for synchronization
        #process_manager = Manager()
        #self.wait_queue = process_manager.Queue()
        #self.shared_kvstore_queue = process_manager.Queue()

    def execute(self):
        self.start()
        self.state = EXECUTING

    def check(self):
#         self.client.close()
        pass

    def run(self):
        while self.has_next():
            self.next()
        self.state = FINISHED
        self.set_result(True)

    def has_next(self):
        raise NotImplementedError

    def next(self):
        raise NotImplementedError

    def _unlocked_create(self, partition, key, value, is_base64_value=False):
        try:
            value_json = json.loads(value)
            if isinstance(value_json, dict):
                value_json['mutated'] = 0
            value = json.dumps(value_json)
        except ValueError:
            index = random.choice(range(len(value)))
            if not is_base64_value:
                value = value[0:index] + random.choice(string.ascii_uppercase) + value[index + 1:]
        except TypeError:
            value = json.dumps(value)
        try:
            self.client.set(key, self.exp, self.flag, value)
            if self.only_store_hash:
                value = str(crc32.crc32_hash(value))
            partition.set(key, value, self.exp, self.flag)
        except Exception as error:
            self.state = FINISHED
            self. set_unexpected_exception(error)


    def _unlocked_read(self, partition, key):
        try:
            o, c, d = self.client.get(key)
        except MemcachedError as error:
            if error.status == ERR_NOT_FOUND and partition.get_valid(key) is None:
                pass
            else:
                self.state = FINISHED
                self. set_unexpected_exception(error)

    def _unlocked_replica_read(self, partition, key):
        try:
            o, c, d = self.client.getr(key)
        except Exception as error:
            self.state = FINISHED
            self. set_unexpected_exception(error)

    def _unlocked_update(self, partition, key):
        value = None
        try:
            o, c, value = self.client.get(key)
            if value is None:
                return

            value_json = json.loads(value)
            value_json['mutated'] += 1
            value = json.dumps(value_json)
        except MemcachedError as error:
            if error.status == ERR_NOT_FOUND and partition.get_valid(key) is None:
                # there is no such item, we do not know what value to set
                return
            else:
                self.state = FINISHED
                log.error("%s, key: %s update operation." % (error, key))
                self. set_unexpected_exception(error)
                return
        except ValueError:
            if value is None:
                return
            index = random.choice(range(len(value)))
            value = value[0:index] + random.choice(string.ascii_uppercase) + value[index + 1:]
        except BaseException as error:
            self.state = FINISHED
            self. set_unexpected_exception(error)

        try:
            self.client.upsert(key, self.exp, self.flag, value)
            if self.only_store_hash:
                value = str(crc32.crc32_hash(value))
            partition.set(key, value, self.exp, self.flag)
        except BaseException as error:
            self.state = FINISHED
            self. set_unexpected_exception(error)

    def _unlocked_delete(self, partition, key):
        try:
            self.client.delete(key)
            partition.delete(key)
        except MemcachedError as error:
            if error.status == ERR_NOT_FOUND and partition.get_valid(key) is None:
                pass
            else:
                self.state = FINISHED
                log.error("%s, key: %s delete operation." % (error, key))
                self. set_unexpected_exception(error)
        except BaseException as error:
            self.state = FINISHED
            self. set_unexpected_exception(error)

    def _unlocked_append(self, partition, key, value):
        try:
            o, c, old_value = self.client.get(key)
            if value is None:
                return
            value_json = json.loads(value)
            old_value_json = json.loads(old_value)
            old_value_json.update(value_json)
            old_value = json.dumps(old_value_json)
            value = json.dumps(value_json)
        except MemcachedError as error:
            if error.status == ERR_NOT_FOUND and partition.get_valid(key) is None:
                # there is no such item, we do not know what value to set
                return
            else:
                self.state = FINISHED
                self. set_unexpected_exception(error)
                return
        except ValueError:
            o, c, old_value = self.client.get(key)
            index = random.choice(range(len(value)))
            value = value[0:index] + random.choice(string.ascii_uppercase) + value[index + 1:]
            old_value += value
        except BaseException as error:
            self.state = FINISHED
            self. set_unexpected_exception(error)

        try:
            self.client.append(key, value)
            if self.only_store_hash:
                old_value = str(crc32.crc32_hash(old_value))
            partition.set(key, old_value)
        except BaseException as error:
            self.state = FINISHED
            self. set_unexpected_exception(error)


    # start of batch methods
    def _create_batch_client(self, key_val, shared_client = None):
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
            log.info("Batch Operation: %s documents are INSERTED into bucket %s"%(len(key_val), self.bucket))
        except (MemcachedError, ServerUnavailableException, socket.error, EOFError, AttributeError, RuntimeError) as error:
            self.state = FINISHED
            self. set_unexpected_exception(error)

    def _create_batch(self, partition_keys_dic, key_val):
            self._create_batch_client(key_val)
            self._populate_kvstore(partition_keys_dic, key_val)

    def _update_batch(self, partition_keys_dic, key_val):
        try:
            self._process_values_for_update(partition_keys_dic, key_val)
            self.client.upsertMulti(self.exp, self.flag, key_val, self.pause, self.timeout, parallel=False)
            log.info("Batch Operation: %s documents are UPSERTED into bucket %s"%(len(key_val), self.bucket))
            self._populate_kvstore(partition_keys_dic, key_val)
        except (MemcachedError, ServerUnavailableException, socket.error, EOFError, AttributeError, RuntimeError) as error:
            self.state = FINISHED
            self. set_unexpected_exception(error)


    def _delete_batch(self, partition_keys_dic, key_val):
        for partition, keys in partition_keys_dic.items():
            for key in keys:
                try:
                    self.client.delete(key)
                    partition.delete(key)
                except MemcachedError as error:
                    if error.status == ERR_NOT_FOUND and partition.get_valid(key) is None:
                        pass
                    else:
                        self.state = FINISHED
                        self. set_unexpected_exception(error)
                        return
                except (ServerUnavailableException, socket.error, EOFError, AttributeError) as error:
                    self.state = FINISHED
                    self. set_unexpected_exception(error)


    def _read_batch(self, partition_keys_dic, key_val):
        try:
            o, c, d = self.client.getMulti(key_val.keys(), self.pause, self.timeout)
            log.info("Batch Operation: %s documents are READ from bucket %s"%(len(key_val), self.bucket))
        except MemcachedError as error:
                self.state = FINISHED
                self. set_unexpected_exception(error)

    def _process_values_for_create(self, key_val):
        for key, value in key_val.items():
            try:
                value_json = json.loads(value)
                value_json['mutated'] = 0
                value = json.dumps(value_json)
            except ValueError:
                index = random.choice(range(len(value)))
                value = value[0:index] + random.choice(string.ascii_uppercase) + value[index + 1:]
            except TypeError:
                value = json.dumps(value)
            finally:
                key_val[key] = value

    def _process_values_for_update(self, partition_keys_dic, key_val):
        for partition, keys in partition_keys_dic.items():
            for key in keys:
                value = partition.get_valid(key)
                if value is None:
                    del key_val[key]
                    continue
                try:
                    value = key_val[key]  # new updated value, however it is not their in orginal code "LoadDocumentsTask"
                    value_json = json.loads(value)
                    value_json['mutated'] += 1
                    value = json.dumps(value_json)
                except ValueError:
                    index = random.choice(range(len(value)))
                    value = value[0:index] + random.choice(string.ascii_uppercase) + value[index + 1:]
                finally:
                    key_val[key] = value


    def _populate_kvstore(self, partition_keys_dic, key_val):
        for partition, keys in partition_keys_dic.items():
            self._populate_kvstore_partition(partition, keys, key_val)

    def _release_locks_on_kvstore(self):
        for part in self._partitions_keyvals_dic.keys:
            self.kv_store.release_lock(part)

    def _populate_kvstore_partition(self, partition, keys, key_val):
        for key in keys:
            if self.only_store_hash:
                key_val[key] = str(crc32.crc32_hash(key_val[key]))
            partition.set(key, key_val[key], self.exp, self.flag)

class LoadDocumentsTask(GenericLoadingTask):

    def __init__(self, server, task_manager, bucket, generator, kv_store, op_type, exp, flag=0,
                 only_store_hash=True, proxy_client=None, batch_size=1, pause_secs=1, timeout_secs=30,
                 compression=True):
        GenericLoadingTask.__init__(self, server, task_manager, bucket, kv_store, batch_size=batch_size,pause_secs=pause_secs,
                                    timeout_secs=timeout_secs, compression=compression)

        self.generator = generator
        self.op_type = op_type
        self.exp = exp
        self.flag = flag
        self.only_store_hash = only_store_hash

        if proxy_client:
            log.info("Changing client to proxy %s:%s..." % (proxy_client.host,
                                                              proxy_client.port))
            self.client = proxy_client

    def has_next(self):
        return self.generator.has_next()

    def next(self, override_generator = None):
#         log.info("BATCH SIZE for documents load: %s" % self.batch_size)
        if self.batch_size == 1:
            key, value = self.generator.next()
            partition = self.kv_store.acquire_partition(key)
            if self.op_type == 'create':
                is_base64_value = (self.generator.__class__.__name__ == 'Base64Generator')
                self._unlocked_create(partition, key, value, is_base64_value=is_base64_value)
            elif self.op_type == 'read':
                self._unlocked_read(partition, key)
            elif self.op_type == 'read_replica':
                self._unlocked_replica_read(partition, key)
            elif self.op_type == 'update':
                self._unlocked_update(partition, key)
            elif self.op_type == 'delete':
                self._unlocked_delete(partition, key)
            elif self.op_type == 'append':
                self._unlocked_append(partition, key, value)
            else:
                self.state = FINISHED
                self. set_unexpected_exception(Exception("Bad operation type: %s" % self.op_type))
            self.kv_store.release_partition(key)

        else:
            doc_gen = override_generator or self.generator
            key_value = doc_gen.next_batch()
            partition_keys_dic = self.kv_store.acquire_partitions(key_value.keys())
            if self.op_type == 'create':
                self._create_batch(partition_keys_dic, key_value)
            elif self.op_type == 'update':
                self._update_batch(partition_keys_dic, key_value)
            elif self.op_type == 'delete':
                self._delete_batch(partition_keys_dic, key_value)
            elif self.op_type == 'read':
                self._read_batch(partition_keys_dic, key_value)
            else:
                self.state = FINISHED
                self. set_unexpected_exception(Exception("Bad operation type: %s" % self.op_type))
            self.kv_store.release_partitions(partition_keys_dic.keys())

class LoadDocumentsGeneratorsTask(LoadDocumentsTask):
    def __init__(self, server, task_manager, bucket, generators, kv_store, op_type, exp, flag=0, only_store_hash=True,
                 batch_size=1,pause_secs=1, timeout_secs=60, compression=True):
        LoadDocumentsTask.__init__(self, server, task_manager, bucket, generators[0], kv_store, op_type, exp, flag=flag,
                    only_store_hash=only_store_hash, batch_size=batch_size, pause_secs=pause_secs, 
                    timeout_secs=timeout_secs, compression=compression)
#         log.info("BATCH SIZE for documents load: %s" % batch_size)
        if batch_size == 1:
            self.generators = generators
        else:
            self.generators = []
            for i in generators:
                self.generators.append(BatchedDocumentGenerator(i, batch_size))

        # only run high throughput for batch-create workloads
        # also check number of input generators isn't greater than
        # process_concurrency as too many generators become inefficient
        self.is_high_throughput_mode = False
        if ALLOW_HTP and not TestInputSingleton.input.param("disable_HTP", False):
            self.is_high_throughput_mode = self.op_type == "create" and \
                self.batch_size > 1 and \
                len(self.generators) < self.process_concurrency

        self.input_generators = generators

        self.op_types = None
        self.buckets = None
        if isinstance(op_type, list):
            self.op_types = op_type
        if isinstance(bucket, list):
            self.buckets = bucket
        
        self.compression = compression
        
    def run(self):
        if self.op_types:
            if len(self.op_types) != len(self.generators):
                self.state = FINISHED
                self. set_unexpected_exception(Exception("not all generators have op_type!"))
        if self.buckets:
            if len(self.op_types) != len(self.buckets):
                self.state = FINISHED
                self. set_unexpected_exception(Exception("not all generators have bucket specified!"))
        log.info("Staring pumping data through run_normal_throughput_mode")
        self.run_normal_throughput_mode()
        # check if running in high throughput mode or normal
        # if self.is_high_throughput_mode:
        #     self.run_high_throughput_mode()
        # else:
        #     self.run_normal_throughput_mode()

        self.state = FINISHED
        log.info("Now closing connections.")
        self.client.close()
        self.set_result(True)

    def run_normal_throughput_mode(self):
        iterator = 0
        for generator in self.generators:
            self.generator = generator
            if self.op_types:
                self.op_type = self.op_types[iterator]
            if self.buckets:
                self.bucket = self.buckets[iterator]
            while self.has_next():
                self.next()
            iterator += 1

    def run_high_throughput_mode(self):

        # high throughput mode requires partitioning the doc generators
        self.generators = []
        for gen in self.input_generators:
            gen_start = int(gen.start)
            gen_end = max(int(gen.end), 1)
            gen_range = max(int(gen.end/self.process_concurrency), 1)
            for pos in range(gen_start, gen_end, gen_range):
                partition_gen = copy.deepcopy(gen)
                partition_gen.start = pos
                partition_gen.itr = pos
                partition_gen.end = pos+gen_range
                if partition_gen.end > gen.end:
                    partition_gen.end = gen.end
                batch_gen = BatchedDocumentGenerator(
                        partition_gen,
                        self.batch_size)
                self.generators.append(batch_gen)

        iterator = 0
        all_processes = []
        for generator in self.generators:

            # only start processing when there resources available
            CONCURRENCY_LOCK.acquire()

            generator_process = Process(
                target=self.run_generator,
                args=(generator, iterator))
            generator_process.start()
            iterator += 1
            all_processes.append(generator_process)

            # add child process to wait queue
            self.wait_queue.put(iterator)

        # wait for all child processes to finish
        self.wait_queue.join()

        # merge kvstore partitions
        while self.shared_kvstore_queue.empty() is False:

            # get partitions created by child process
            rv =  self.shared_kvstore_queue.get()
            if rv["err"] is not None:
                raise Exception(rv["err"])

            # merge child partitions with parent
            generator_partitions = rv["partitions"]
            self.kv_store.merge_partitions(generator_partitions)

            # terminate child process
            iterator-=1
            all_processes[iterator].terminate()

    def run_generator(self, generator, iterator):

        # create a tmp kvstore to track work
        tmp_kv_store = KVStore()
        rv = {"err": None, "partitions": None}

        try:
#             client = VBucketAwareMemcached(
#                     RestConnection(self.server),
#                     self.bucket)
            if CHECK_FLAG:
                client = VBucketAwareMemcached(
                        RestConnection(self.server),
                        self.bucket)
            else:
                client = VBucketAwareMemcached(
                     RestConnection(self.server),
                    self.bucket, compression=self.compression)
            if self.op_types:
                self.op_type = self.op_types[iterator]
            if self.buckets:
                self.bucket = self.buckets[iterator]

            while generator.has_next() and not self.done():

                # generate
                key_value = generator.next_batch()

                # create
                self._create_batch_client(key_value, client)

                # cache
                self.cache_items(tmp_kv_store, key_value)

        except Exception as ex:
            rv["err"] = ex
        else:
            rv["partitions"] = tmp_kv_store.get_partitions()
        finally:
            # share the kvstore from this generator
            self.shared_kvstore_queue.put(rv)
            self.wait_queue.task_done()
            # release concurrency lock
            CONCURRENCY_LOCK.release()


    def cache_items(self, store, key_value):
        """
            unpacks keys,values and adds them to provided store
        """
        for key, value in key_value.iteritems():
            if self.only_store_hash:
                value = str(crc32.crc32_hash(value))
            store.partition(key)["partition"].set(
                key,
                value,
                self.exp,
                self.flag)

# class LoadDocumentsTask_java(Task):
#     def __init__(self, task_manager, server, bucket, num_items, start_from, k, v):
#         Task.__init__(self, "load_documents_javasdk", task_manager)
#         self.bucket = bucket
#         self.num_items = num_items
#         self.start_from = start_from
#         self.started = None
#         self.completed = None
#         self.loaded = 0
#         self.thread_used = None
#         self.exception = None
#         self.key = k
#         self.value = v
#         self.server = server
#         
#     def execute(self):
#         try:
# #             data = JsonObject.create().put("type", "user").put("name", "asdsfsdfsdf")
#             import json as python_json
#             var = str(python_json.dumps(self.value))
#             data = JsonTranscoder().stringToJsonObject(var);
#             cluster = CouchbaseCluster.create(self.server.ip);
#             cluster.authenticate(self.server.rest_username, self.server.rest_password)
#             bucket = cluster.openBucket(self.bucket);
#             print bucket
#             for i in xrange(self.num_items):
#                 doc = JsonDocument.create(self.key+str(i+self.start_from), data);
#                 response = bucket.upsert(doc);
#                 if i%(self.num_items/10) == 0:
#                     log.info("%s documents loaded in bucket %s"%(i,self.bucket))
#             bucket.close() and cluster.disconnect()
#             self.state = CHECKING
#             self.call()
#         except Exception as e:
#             self.state = FINISHED
#             bucket.close() and cluster.disconnect()
#             self.set_unexpected_exception(e)
#     
#     def check(self):
#         self.state = FINISHED
#         pass

class N1QLQueryTask(Task):
    def __init__(self,
                 server, task_manager, bucket,
                 query, n1ql_helper = None,
                 expected_result=None,
                 verify_results = True,
                 is_explain_query = False,
                 index_name = None,
                 retry_time=2,
                 scan_consistency = None,
                 scan_vector = None):
        Task.__init__(self, "query_n1ql_task", task_manager)
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
        self.scan_consistency = scan_consistency
        self.scan_vector = scan_vector

    def execute(self):
        try:
            # Query and get results
            log.debug(" <<<<< START Executing Query {0} >>>>>>".format(self.query))
            if not self.is_explain_query:
                self.msg, self.isSuccess = self.n1ql_helper.run_query_and_verify_result(
                    query = self.query, server = self.server, expected_result = self.expected_result,
                    scan_consistency = self.scan_consistency, scan_vector = self.scan_vector,
                    verify_results = self.verify_results)
            else:
                self.actual_result = self.n1ql_helper.run_cbq_query(query = self.query, server = self.server,
                 scan_consistency = self.scan_consistency, scan_vector = self.scan_vector)
                log.info(self.actual_result)
            log.debug(" <<<<< Done Executing Query {0} >>>>>>".format(self.query))
            self.state = CHECKING
            self.call()
        except N1QLQueryException as e:
            self.state = FINISHED
            # initial query failed, try again
            self.call()
            #task_manager.schedule(self, self.retry_time)

        # catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            self.set_unexpected_exception(e)

    def check(self):
        try:
           # Verify correctness of result set
           if self.verify_results:
            if not self.is_explain_query:
                if not self.isSuccess:
                    log.warning(" Query {0} results leads to INCORRECT RESULT ".format(self.query))
                    raise N1QLQueryException(self.msg)
            else:
                check = self.n1ql_helper.verify_index_with_explain(self.actual_result, self.index_name)
                if not check:
                    actual_result = self.n1ql_helper.run_cbq_query(query="select * from system:indexes", server=self.server)
                    log.info(actual_result)
                    raise Exception(" INDEX usage in Query {0} :: NOT FOUND {1} :: as observed in result {2}".format(
                        self.query, self.index_name, self.actual_result))
           log.info(" <<<<< Done Verifying Query {0} >>>>>>".format(self.query))
           self.set_result(True)
           self.state = FINISHED
        except N1QLQueryException as e:
            # subsequent query failed! exit
            self.state = FINISHED
            self. set_unexpected_exception(e)
        # catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            self.set_unexpected_exception(e)

class CreateIndexTask(Task):
    def __init__(self,
                 server, task_manager, bucket, index_name,
                 query, n1ql_helper = None,
                 retry_time=2, defer_build = False,
                 timeout = 240):
        Task.__init__(self, "create_index_task", task_manager)
        self.server = server
        self.bucket = bucket
        self.defer_build = defer_build
        self.query = query
        self.index_name = index_name
        self.n1ql_helper = n1ql_helper
        self.retry_time = 2
        self.timeout = timeout

    def execute(self):
        try:
            # Query and get results
            self.n1ql_helper.run_cbq_query(query = self.query, server = self.server)
            self.state = CHECKING
            self.call()
        except CreateIndexException as e:
            # initial query failed, try again
            self.state = FINISHED
            self.call()
        # catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            log.error(e)
            self. set_unexpected_exception(e)

    def check(self):
        try:
           # Verify correctness of result set
            check = True
            if not self.defer_build:
                check = self.n1ql_helper.is_index_online_and_in_list(self.bucket, self.index_name, server = self.server, timeout = self.timeout)
            if not check:
                raise CreateIndexException("Index {0} not created as expected ".format(self.index_name))
            self.set_result(True)
            self.state = FINISHED
        except CreateIndexException as e:
            # subsequent query failed! exit
            self.state = FINISHED
            log.error(e)
            self. set_unexpected_exception(e)
        # catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            log.error(e)
            self. set_unexpected_exception(e)

class BuildIndexTask(Task):
    def __init__(self,
                 server, task_manager, bucket,
                 query, n1ql_helper = None,
                 retry_time=2):
        Task.__init__(self, "build_index_task", task_manager)
        self.server = server
        self.bucket = bucket
        self.query = query
        self.n1ql_helper = n1ql_helper
        self.retry_time = 2

    def execute(self):
        try:
            # Query and get results
            self.n1ql_helper.run_cbq_query(query = self.query, server = self.server)
            self.state = CHECKING
            self.call()
        except CreateIndexException as e:
            # initial query failed, try again
            self.state = FINISHED
            self.call()

        # catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            self.set_unexpected_exception(e)

    def check(self):
        try:
           # Verify correctness of result set
            self.set_result(True)
            self.state = FINISHED
        except CreateIndexException as e:
            # subsequent query failed! exit
            self.state = FINISHED
            self. set_unexpected_exception(e)

        # catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            self.set_unexpected_exception(e)

class MonitorIndexTask(Task):
    def __init__(self,
                 server, task_manager, bucket, index_name,
                 n1ql_helper = None,
                 retry_time=2,
                 timeout = 240):
        Task.__init__(self, "build_index_task", task_manager)
        self.server = server
        self.bucket = bucket
        self.index_name = index_name
        self.n1ql_helper = n1ql_helper
        self.retry_time = 2
        self.timeout = timeout

    def execute(self):
        try:
            check = self.n1ql_helper.is_index_online_and_in_list(self.bucket, self.index_name,
             server = self.server, timeout = self.timeout)
            if not check:
                self.state = FINISHED
                raise CreateIndexException("Index {0} not created as expected ".format(self.index_name))
            self.state = CHECKING
            self.call()
        except CreateIndexException as e:
            # initial query failed, try again
            self.state = FINISHED
            self. set_unexpected_exception(e)
        # catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            self.set_unexpected_exception(e)

    def check(self):
        try:
            self.set_result(True)
            self.state = FINISHED
        except CreateIndexException as e:
            # subsequent query failed! exit
            self.state = FINISHED
            self. set_unexpected_exception(e)

        # catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            self.set_unexpected_exception(e)

class DropIndexTask(Task):
    def __init__(self,
                 server, task_manager, bucket, index_name,
                 query, n1ql_helper = None,
                 retry_time=2):
        Task.__init__(self, "drop_index_task", task_manager)
        self.server = server
        self.bucket = bucket
        self.query = query
        self.index_name = index_name
        self.n1ql_helper = n1ql_helper
        self.timeout = 900
        self.retry_time = 2

    def execute(self, task_manager):
        try:
            # Query and get results
            check = self.n1ql_helper._is_index_in_list(self.bucket, self.index_name, server = self.server)
            if not check:
                raise DropIndexException("index {0} does not exist will not drop".format(self.index_name))
            self.n1ql_helper.run_cbq_query(query = self.query, server = self.server)
            self.state = CHECKING
            task_manager.schedule(self)
        except N1QLQueryException as e:
            # initial query failed, try again
            self.state = FINISHED
            task_manager.schedule(self, self.retry_time)
        # catch and set all unexpected exceptions
        except DropIndexException as e:
            self.state = FINISHED
            self.set_unexpected_exception(e)

    def check(self, task_manager):
        try:
        # Verify correctness of result set
            check = self.n1ql_helper._is_index_in_list(self.bucket, self.index_name, server = self.server)
            if check:
                raise Exception("Index {0} not dropped as expected ".format(self.index_name))
            self.set_result(True)
            self.state = FINISHED
        except DropIndexException as e:
            # subsequent query failed! exit
            self.state = FINISHED
            self. set_unexpected_exception(e)
        # catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            self.set_unexpected_exception(e)

class FailoverTask(Task):
    def __init__(self, servers, task_manager, to_failover=[], wait_for_pending=0, graceful=False, use_hostnames=False):
        Task.__init__(self, "failover_task", task_manager)
        self.servers = servers
        self.to_failover = to_failover
        self.graceful = graceful
        self.wait_for_pending = wait_for_pending
        self.use_hostnames = use_hostnames

    def execute(self):
        try:
            self._failover_nodes(self.task_manager)
            log.info("{0} seconds sleep after failover, for nodes to go pending....".format(self.wait_for_pending))
            time.sleep(self.wait_for_pending)
            self.state = FINISHED
            self.set_result(True)

        except FailoverFailedException as e:
            self.state = FINISHED
            self. set_unexpected_exception(e)

        except Exception as e:
            self.state = FINISHED
            self.set_unexpected_exception(e)

    def _failover_nodes(self, task_manager):
        rest = RestConnection(self.servers[0])
        # call REST fail_over for the nodes to be failed over
        for server in self.to_failover:
            for node in rest.node_statuses():
                if (server.hostname if self.use_hostnames else server.ip) == node.ip and int(server.port) == int(node.port):
                    log.info("Failing over {0}:{1} with graceful={2}".format(node.ip, node.port, self.graceful))
                    rest.fail_over(node.id, self.graceful)

class BucketFlushTask(Task):
    def __init__(self, server, task_manager, bucket="default"):
        Task.__init__(self, "bucket_flush_task", task_manager)
        self.server = server
        self.bucket = bucket
        if isinstance(bucket, Bucket):
            self.bucket = bucket.name

    def execute(self):
        try:
            rest = BucketHelper(self.server)
            if rest.flush_bucket(self.bucket):
                self.state = CHECKING
                self.task_manager.schedule(self)
            else:
                self.state = FINISHED
                self.set_result(False)

        except BucketFlushFailed as e:
            self.state = FINISHED
            self. set_unexpected_exception(e)

        except Exception as e:
            self.state = FINISHED
            self.set_unexpected_exception(e)

    def check(self):
        try:
            # check if after flush the vbuckets are ready
            if MemcachedHelper.wait_for_vbuckets_ready_state(self.server, self.bucket):
                self.set_result(True)
            else:
                log.error("Unable to reach bucket {0} on server {1} after flush".format(self.bucket, self.server))
                self.set_result(False)
            self.state = FINISHED
        except Exception as e:
            self.state = FINISHED
            self.set_unexpected_exception(e)

class CompactBucketTask(Task):

    def __init__(self, server, task_manager, bucket="default"):
        Task.__init__(self, "bucket_compaction_task", task_manager)
        self.server = server
        self.bucket = bucket
        self.rest = RestConnection(server)
        self.retries = 20
        self.statuses = {}
        # get the current count of compactions

        nodes = self.rest.get_nodes()
        self.compaction_count = {}

        for node in nodes:
            self.compaction_count[node.ip] = 0

    def execute(self):

        try:
            status = BucketHelper(self.server).compact_bucket(self.bucket)
            self.state = CHECKING
            self.call()
        except BucketCompactionException as e:
            log.error("Bucket compaction failed for unknown reason")
            self. set_unexpected_exception(e)
            self.state = FINISHED
            self.set_result(False)

    def check(self):
        # check bucket compaction status across all nodes
        nodes = self.rest.get_nodes()
        current_compaction_count = {}

        for node in nodes:
            current_compaction_count[node.ip] = 0
            s = TestInputServer()
            s.ip = node.ip
            s.ssh_username = self.server.ssh_username
            s.ssh_password = self.server.ssh_password
            shell = RemoteMachineShellConnection(s)
            res = shell.execute_cbstats("", "raw", keyname="kvtimings", vbid="")


            for i in res[0]:
                # check for lines that look like
                #    rw_0:compact_131072,262144:        8
                if 'compact' in i:
                    current_compaction_count[node.ip] += int(i.split(':')[2])


        if cmp(current_compaction_count, self.compaction_count) == 1:
            # compaction count has increased
            self.set_result(True)
            self.state = FINISHED

        else:
            if self.retries > 0:
                # retry
                self.retries = self.retries - 1
                self.task_manager.schedule(self, 10)
            else:
                # never detected a compaction task running
                self.set_result(False)
                self.state = FINISHED

    def _get_disk_size(self):
        stats = self.rest.fetch_bucket_stats(bucket=self.bucket)
        total_disk_size = stats["op"]["samples"]["couch_total_disk_size"][-1]
        log.info("Disk size is = %d" % total_disk_size)
        return total_disk_size

class  CBASQueryExecuteTask(Task):
    def __init__(self, master, cbas_server, task_manager, cbas_endpoint, statement, bucket, mode=None, pretty=True):
        Task.__init__(self, "cbas_query_execute_task", task_manager)
        self.cbas_server = cbas_server
        self.master = master
        self.cbas_endpoint = cbas_endpoint
        self.statement = statement
        self.mode = mode
        self.pretty = pretty
        self.response = {}
        self.passed = True
        self.bucket = bucket

    def execute(self):
        try:
            from cbas.cbas_utils import cbas_utils
            utils = cbas_utils(self.master, self.cbas_server)
            utils.createConn(self.bucket)
            self.response, self.metrics, self.errors, self.results, self.handle = utils.execute_statement_on_cbas_util(self.statement)

            if self.response:
                self.state = CHECKING
                self.call()
            else:
                log.info("Some error")
                traceback.print_exc(file=sys.stdout)
                self.state = FINISHED
                self.passed = False
                self.set_result(False)
        # catch and set all unexpected exceptions

        except Exception as e:
            self.state = FINISHED
            self.passed = False
            self.set_unexpected_exception(e)

    def check(self):
        try:
            if self.mode != "async":
                if self.response:
                    self.set_result(True)
                    self.passed = True
                else:
                    log.info(errors)
                    self.passed = False
                    self.set_result(False)
            else:
                if self.response["status"] == "started":
                    self.set_result(True)
                    self.passed = True
                elif self.response["status"] == "running":
                    self.set_result(True)
                    self.passed = True
                else:
                    log.info(self.response["status"])
                    log.info(errors)
                    self.passed = False
                    self.set_result(False)
            self.state = FINISHED
        # catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            self.set_unexpected_exception(e)

class ValidateDataTask(GenericLoadingTask):
    def __init__(self, server, bucket, kv_store, max_verify=None, only_store_hash=True, replica_to_read=None, task_manager=None,
                 compression=True):
        GenericLoadingTask.__init__(self, server, task_manager, bucket, kv_store, compression=compression)
        self.valid_keys, self.deleted_keys = kv_store.key_set()
        self.num_valid_keys = len(self.valid_keys)
        self.num_deleted_keys = len(self.deleted_keys)
        self.itr = 0
        self.max_verify = self.num_valid_keys + self.num_deleted_keys
        self.only_store_hash = only_store_hash
        self.replica_to_read = replica_to_read
        if max_verify is not None:
            self.max_verify = min(max_verify, self.max_verify)
        log.info("%s items will be verified on %s bucket" % (self.max_verify, bucket))
        self.start_time = time.time()

    def has_next(self):
        if self.itr < (self.num_valid_keys + self.num_deleted_keys) and\
            self.itr < self.max_verify:
            if not self.itr % 50000:
                log.info("{0} items were verified".format(self.itr))
            return True
        time.sleep(5)
        log.info("{0} items were verified in {1} sec.the average number of ops\
            - {2} per second ".format(self.itr, time.time() - self.start_time,
                self.itr / (time.time() - self.start_time)).rstrip())
        return False

    def next(self):
        if self.itr < self.num_valid_keys:
            self._check_valid_key(self.valid_keys[self.itr])
        else:
            self._check_deleted_key(self.deleted_keys[self.itr - self.num_valid_keys])
        self.itr += 1

    def _check_valid_key(self, key):
        partition = self.kv_store.acquire_partition(key)

        value = partition.get_valid(key)
        flag = partition.get_flag(key)
        if value is None or flag is None:
            self.kv_store.release_partition(key)
            return

        try:
            if self.replica_to_read is None:
                o, c, d = self.client.get(key)
            else:
                o, c, d = self.client.getr(key, replica_index=self.replica_to_read)
            if self.only_store_hash:
                if crc32.crc32_hash(d) != int(value):
                    self.state = FINISHED
                    self.set_exception(Exception('Key: %s, Bad hash result: %d != %d for key %s' % (key, crc32.crc32_hash(d), int(value), key)))
            else:
                value = json.dumps(value)
                if d != json.loads(value):
                    self.state = FINISHED
                    self.set_exception(Exception('Key: %s, Bad result: %s != %s for key %s' % (key, json.dumps(d), value, key)))
            if CHECK_FLAG and o != flag:
                self.state = FINISHED
                self.set_exception(Exception('Key: %s, Bad result for flag value: %s != the value we set: %s' % (key, o, flag)))

        except MemcachedError as error:
            if error.status == ERR_NOT_FOUND and partition.get_valid(key) is None:
                pass
            else:
                self.state = FINISHED
                self.set_exception(error)
        except Exception as error:
            log.error("Unexpected error: %s" % str(error))
            self.state = FINISHED
            self.set_exception(error)
        self.kv_store.release_partition(key)

    def _check_deleted_key(self, key):
        partition = self.kv_store.acquire_partition(key)

        try:
            self.client.delete(key)
            if partition.get_valid(key) is not None:
                self.state = FINISHED
                self.set_exception(Exception('Not Deletes: %s' % (key)))
        except MemcachedError as error:
            if error.status == ERR_NOT_FOUND:
                pass
            else:
                self.state = FINISHED
                self.set_exception(error)
        except Exception as error:
            if error.rc != NotFoundError:
                self.state = FINISHED
                self.set_exception(error)
        self.kv_store.release_partition(key)
        
class BatchedValidateDataTask(GenericLoadingTask):
    def __init__(self, server, bucket, kv_store, max_verify=None, only_store_hash=True, batch_size=100, timeout_sec=5, task_manager=None,
                 compression=True):
        GenericLoadingTask.__init__(self, server, task_manager, bucket, kv_store, compression=compression)
        self.valid_keys, self.deleted_keys = kv_store.key_set()
        self.num_valid_keys = len(self.valid_keys)
        self.num_deleted_keys = len(self.deleted_keys)
        self.itr = 0
        self.max_verify = self.num_valid_keys + self.num_deleted_keys
        self.timeout_sec = timeout_sec
        self.only_store_hash = only_store_hash
        if max_verify is not None:
            self.max_verify = min(max_verify, self.max_verify)
        log.info("%s items will be verified on %s bucket" % (self.max_verify, bucket))
        self.batch_size = batch_size
        self.start_time = time.time()

    def has_next(self):
        has = False
        if self.itr < (self.num_valid_keys + self.num_deleted_keys) and self.itr < self.max_verify:
            has = True
        if math.fmod(self.itr, 10000) == 0.0:
                log.info("{0} items were verified".format(self.itr))
        if not has:
            log.info("{0} items were verified in {1} sec.the average number of ops\
                - {2} per second".format(self.itr, time.time() - self.start_time,
                self.itr / (time.time() - self.start_time)).rstrip())
        return has

    def next(self):
        if self.itr < self.num_valid_keys:
            keys_batch = self.valid_keys[self.itr:self.itr + self.batch_size]
            self.itr += len(keys_batch)
            self._check_valid_keys(keys_batch)
        else:
            self._check_deleted_key(self.deleted_keys[self.itr - self.num_valid_keys])
            self.itr += 1

    def _check_valid_keys(self, keys):
        partition_keys_dic = self.kv_store.acquire_partitions(keys)
        try:
            key_vals = self.client.getMulti(keys, parallel=True, timeout_sec=self.timeout_sec)
        except ValueError, error:
            self.state = FINISHED
            self.kv_store.release_partitions(partition_keys_dic.keys())
            self.set_unexpected_exception(error)
            return
        except BaseException, error:
        # handle all other exception, for instance concurrent.futures._base.TimeoutError
            self.state = FINISHED
            self.kv_store.release_partitions(partition_keys_dic.keys())
            self.set_unexpected_exception(error)
            log.info("Closing connections.")
            self.client.close()
            return
        for partition, keys in partition_keys_dic.items():
            self._check_validity(partition, keys, key_vals)
        self.kv_store.release_partitions(partition_keys_dic.keys())

    def _check_validity(self, partition, keys, key_vals):

        for key in keys:
            value = partition.get_valid(key)
            flag = partition.get_flag(key)
            if value is None:
                continue
            try:
                o, c, d = key_vals[key]

                if self.only_store_hash:
                    if crc32.crc32_hash(d) != int(value):
                        self.state = FINISHED
                        self.set_unexpected_exception(Exception('Key: %s Bad hash result: %d != %d' % (key, crc32.crc32_hash(d), int(value))))
                else:
                    #value = json.dumps(value)
                    if json.loads(d) != json.loads(value):
                        self.state = FINISHED
                        self.set_exception(Exception('Key: %s Bad result: %s != %s' % (key, json.dumps(d), value)))
                if CHECK_FLAG and o != flag:
                    self.state = FINISHED
                    self.set_exception(Exception('Key: %s Bad result for flag value: %s != the value we set: %s' % (key, o, flag)))
            except KeyError as error:
                self.state = FINISHED
                self.set_exception(error)

    def _check_deleted_key(self, key):
        partition = self.kv_store.acquire_partition(key)
        try:
            self.client.delete(key)
            if partition.get_valid(key) is not None:
                self.state = FINISHED
                self.set_exception(Exception('Not Deletes: %s' % (key)))
        except MemcachedError as error:
            if error.status == ERR_NOT_FOUND:
                pass
            else:
                self.state = FINISHED
                self.kv_store.release_partitions(key)
                self.set_exception(error)
        except Exception as error:
            if error.rc != NotFoundError:
                self.state = FINISHED
                self.kv_store.release_partitions(key)
                self.set_exception(error)
        self.kv_store.release_partition(key)
