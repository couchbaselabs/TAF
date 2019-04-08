import copy
from httplib import IncompleteRead
from java.util.concurrent import Callable, TimeUnit, CancellationException,\
    TimeoutException
import logging
import socket
import sys
import time
import traceback
from BucketLib.BucketOperations import BucketHelper
from BucketLib.MemcachedOperations import MemcachedHelper
from TestInput import TestInputServer
from membase.api.exception import BucketCreationException,\
    BucketCompactionException
from membase.api.exception import N1QLQueryException, DropIndexException
from membase.api.exception import CreateIndexException,\
    RebalanceFailedException
from membase.api.exception import FailoverFailedException, \
    ServerUnavailableException, BucketFlushFailed
from membase.api.rest_client import RestConnection
from memcached.helper.data_helper import MemcachedClientHelper
from remote.remote_util import RemoteMachineShellConnection
from testconstants import MIN_KV_QUOTA, INDEX_QUOTA, FTS_QUOTA, CBAS_QUOTA


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
        self.thread_name = name
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
                    log.info(self.errors)
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
                    log.info(self.errors)
                    self.passed = False
                    self.set_result(False)
            self.state = FINISHED
        # catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            self.set_unexpected_exception(e)