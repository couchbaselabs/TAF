"""
Created on Sep 14, 2017

@author: riteshagarwal
"""
import copy
import json
import json as Json
import os
import random
import socket
import threading
import time
from copy import deepcopy
import zlib
from httplib import IncompleteRead

import com.couchbase.test.transactions.SimpleTransaction as Transaction
from _threading import Lock
from com.couchbase.client.java.json import JsonObject
from java.lang import Thread
from java.util.concurrent import Callable
from reactor.util.function import Tuples

from BucketLib.BucketOperations import BucketHelper
from BucketLib.MemcachedOperations import MemcachedHelper
from BucketLib.bucket import Bucket
from Cb_constants import constants, CbServer, DocLoading
from CbasLib.CBASOperations import CBASHelper
from CbasLib.cbas_entity import Dataverse, CBAS_Collection, Dataset, Synonym, \
    CBAS_Index, CBAS_UDF
from Jython_tasks.task_manager import TaskManager
from cb_tools.cbstats import Cbstats
from collections_helper.collections_spec_constants import MetaConstants
from common_lib import sleep
from couchbase_helper.document import DesignDocument
from couchbase_helper.documentgenerator import BatchedDocumentGenerator, \
    SubdocDocumentGenerator
from error_simulation.cb_error import CouchbaseError
from global_vars import logger
from custom_exceptions.exception import \
    N1QLQueryException, DropIndexException, CreateIndexException, \
    DesignDocCreationException, QueryViewException, ReadDocumentException, \
    RebalanceFailedException, ServerUnavailableException, \
    BucketCreationException, AutoFailoverException, GetBucketInfoFailed, \
    CompactViewFailed, SetViewInfoNotFound, FailoverFailedException, \
    BucketFlushFailed
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteUtilHelper, RemoteMachineShellConnection
from sdk_exceptions import SDKException
from table_view import TableView, plot_graph
from gsiLib.GsiHelper_Rest import GsiHelper
from capella.capella_utils import CapellaUtils
from TestInput import TestInputServer
from capellaAPI.CapellaAPI import CapellaAPI
# from cluster_utils.cluster_ready_functions import CBCluster


class Task(Callable):
    def __init__(self, thread_name):
        self.thread_name = thread_name
        self.exception = None
        self.completed = False
        self.started = False
        self.start_time = None
        self.end_time = None
        self.log = logger.get("infra")
        self.test_log = logger.get("test")
        self.result = False
        self.sleep = sleep
        self.state = None

    def __str__(self):
        if self.exception:
            raise self.exception
        elif self.completed:
            self.log.info("Task %s completed on: %s"
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
        self.log.info("Thread '%s' started" % self.thread_name)

    def set_exception(self, exception):
        self.exception = exception
        self.complete_task()
        raise BaseException(self.exception)

    def set_warn(self, exception):
        self.exception = exception
        self.complete_task()
        self.log.warn("Warning from '%s': %s" % (self.thread_name, exception))

    def complete_task(self):
        self.completed = True
        self.end_time = time.time()
        self.log.info("Thread '%s' completed" % self.thread_name)

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
        logger.get("infra").debug(
            "Wait for expected condition to get satisfied")
        while not condition(value):
            now = time.time()
            if timeout_secs < 0 or now < stop_time:
                sleep(2 ** attempt * interval)
                attempt += 1
                value = value_getter()
            else:
                raise Exception('Timeout after {0} seconds and {1} attempts'
                                .format(now - start_time, attempt))
        return value


class FunctionCallTask(Task):
    """ A task that calls a given function `f` with arguments `args` and key-word arguments `kwds` """

    def __init__(self, f, args=(), kwds={}):
        """ The constructor.

        Args:
            f (function): The function to call.
            args (tuple): A tuple of arguments for the function `f`.
            kwds (dict): A dictionary of keyword arguments for the function `f`.
        """
        super(FunctionCallTask, self).__init__("FunctionCallTask: function:{} Args:{} Kwds:{}".format(f, args, kwds))
        self.f, self.args, self.kwds = f, args, kwds

    def call(self):
        """ Calls the function f """
        self.start_task()
        result = self.f(*self.args, **self.kwds)
        self.complete_task()
        self.set_result(result)
        return result


class TimerTask(Task):
    """ A task that repeats the given function `f` with arguments `args` and
    key-word arguments `kwds` at the given `interval` """


    def __init__(self, f, args=(), kwds={}, interval=5):
        """ The constructor.

        Args:
            f (function): The function to call.
            args (tuple): A tuple of arguments for the function `f`.
            kwds (dict): A dictionary of keyword arguments for the function `f`.
            interval (int): The time to wait in between function calls.
        """
        super(TimerTask, self).__init__("TimerTask: function:{} Args:{} Kwds:{} Interval:{}".format(f, args, kwds, interval))
        self.f, self.args, self.kwds, self.interval = f, args, kwds, interval

    def call(self):
        """ Calls the function f """
        self.start_task()
        while True:
            result = self.f(*self.args, **self.kwds)
            self.sleep(self.interval)
        self.complete_task()
        return result


class DeployCloud(Task):
    def __init__(self, pod, tenant, name, config, timeout=1800):
        Task.__init__(self, "Deploy_Cluster_{}".format(name))
        self.name = name
        self.config = config
        self.pod = pod
        self.tenant = tenant
        self.timeout = timeout

    def call(self):
        try:
            cluster_id, srv, servers = \
                    CapellaUtils.create_cluster(self.pod, self.tenant,
                                                self.config, self.timeout)
            self.cluster_id = cluster_id
            self.srv = srv
            self.servers = servers
            self.result = True
        except Exception as e:
            self.log.error(e)
            self.result = False
        return self.result


class RebalanceTaskCapella(Task):
    def __init__(self, cluster, scale_params=list(), timeout=1200):
        Task.__init__(self, "Scaling_task_{}".format(str(time.time())))
        self.cluster = cluster
        self.scale_params = {"servers": scale_params}
        self.timeout = timeout
        self.servers = None
        self.test_log.critical("Scale_params: %s" % scale_params)

    def call(self):
        CapellaUtils.scale(self.cluster, self.scale_params)
        self.cluster.cluster_config["servers"] = self.scale_params["servers"]
        capella_api = CapellaAPI(self.cluster.pod.url_public,
                                 self.cluster.tenant.api_secret_key,
                                 self.cluster.tenant.api_access_key,
                                 self.cluster.tenant.user,
                                 self.cluster.tenant.pwd)
        end = time.time() + self.timeout
        while end > time.time():
            try:
                content = CapellaUtils.jobs(capella_api,
                                            self.cluster.pod,
                                            self.cluster.tenant,
                                            self.cluster.id)
                state = CapellaUtils.get_cluster_state(
                    self.cluster.pod, self.cluster.tenant, self.cluster.id)
                if state in ["deployment_failed",
                             "deploymentFailed",
                             "redeploymentFailed",
                             "rebalance_failed",
                             "scaleFailed"]:
                    raise Exception("{} for cluster {}".format(
                        state, self.cluster.id))
                if content.get("data") or state != "healthy":
                    for data in content.get("data"):
                        data = data.get("data")
                        if data.get("clusterId") == self.cluster.id:
                            step, progress = data.get("currentStep"), \
                                             data.get("completionPercentage")
                            self.log.info("{}: Status=={}, State=={}, Progress=={}%".format("Scaling", state, step, progress))
                    time.sleep(2)
                else:
                    self.log.info("Scaling the cluster completed. State == {}".
                                  format(state))
                    self.sleep(300)
                    break
            except Exception as e:
                self.log.critical(e)
                self.result = False
                return self.result
        self.servers = CapellaUtils.get_nodes(
            self.cluster.pod, self.cluster.tenant, self.cluster.id)
        nodes = list()
        for server in self.servers:
            temp_server = TestInputServer()
            temp_server.ip = server.get("hostname")
            temp_server.hostname = server.get("hostname")
            temp_server.services = server.get("services")
            temp_server.port = "18091"
            temp_server.rest_username = self.cluster.username
            temp_server.rest_password = self.cluster.password
            temp_server.hosted_on_cloud = True
            temp_server.memcached_port = "11207"
            nodes.append(temp_server)

        self.cluster.refresh_object(nodes)
        self.result = True
        return self.result


class RebalanceTask(Task):
    def __init__(self, cluster, to_add=[], to_remove=[],
                 use_hostnames=False, services=None,
                 check_vbucket_shuffling=True,
                 retry_get_process_num=25, add_nodes_server_groups=None):
        super(RebalanceTask, self).__init__(
            "Rebalance_task_IN=[{}]_OUT=[{}]_{}"
            .format(",".join([node.ip for node in to_add]),
                    ",".join([node.ip for node in to_remove]),
                    str(time.time())))
        self.cluster = cluster
        self.servers = list()
        self.to_add = to_add
        self.to_remove = to_remove
        self.start_time = None
        self.services = services
        self.monitor_vbuckets_shuffling = False
        self.check_vbucket_shuffling = check_vbucket_shuffling
        self.result = False
        self.retry_get_process_num = retry_get_process_num
        self.server_groups_to_add = dict()

        if isinstance(add_nodes_server_groups, dict):
            """
            This will add one node to each of the AZ_1/2/3.
            {"AZ_1": 1, "AZ_2": 1, "AZ_3": 1}
            """
            if len(to_add) != sum(add_nodes_server_groups.values()):
                raise Exception("Server group map != len(to_add) servers")
            self.server_groups_to_add = add_nodes_server_groups

        try:
            self.rest = RestConnection(self.cluster.master)
        except ServerUnavailableException, e:
            self.test_log.error(e)
            raise e

        valid_membership = ["active", "inactiveAdded"]
        node_ips_to_remove = [node.ip for node in to_remove]
        # Get current 'active' nodes in the cluster
        # and update the nodes_in_cluster accordingly
        for r_node in self.rest.get_nodes(inactive=True):
            for server in self.cluster.servers:
                if r_node.ip == server.ip:
                    if r_node.ip not in node_ips_to_remove \
                            and r_node.clusterMembership in valid_membership:
                        self.servers.append(server)
                    break
        # Update the nodes_in_cluster value
        self.cluster.nodes_in_cluster = self.servers

        self.retry_get_progress = 0
        self.use_hostnames = use_hostnames
        self.previous_progress = 0
        self.old_vbuckets = dict()
        self.thread_used = "Rebalance_task"

        cluster_stats = self.rest.get_cluster_stats()
        self.table = TableView(self.test_log.info)
        self.table.set_headers(["Nodes", "Zone", "Services", "Version / Config",
                                "CPU", "Status", "Membership / Recovery"])
        for node, stat in cluster_stats.items():
            node_ip = node.split(':')[0]
            node_status = "Cluster node"
            if node_ip in node_ips_to_remove:
                node_status = "--- OUT --->"
            self.table.add_row([node_ip, stat["serverGroup"],
                                ", ".join(stat["services"]),
                                stat["version"] + " / " + CbServer.cluster_profile,
                                stat["cpu_utilization"], node_status,
                                stat["clusterMembership"] + " / " + stat["recoveryType"]])
            # Remove the 'out' node from services list
            self.cluster.kv_nodes = \
                [node for node in self.cluster.kv_nodes
                 if node.ip not in node_ips_to_remove]
            self.cluster.index_nodes = \
                [node for node in self.cluster.index_nodes
                 if node.ip not in node_ips_to_remove]
            self.cluster.query_nodes = \
                [node for node in self.cluster.query_nodes
                 if node.ip not in node_ips_to_remove]
            self.cluster.cbas_nodes = \
                [node for node in self.cluster.cbas_nodes
                 if node.ip not in node_ips_to_remove]
            self.cluster.eventing_nodes = \
                [node for node in self.cluster.eventing_nodes
                 if node.ip not in node_ips_to_remove]
            self.cluster.fts_nodes = \
                [node for node in self.cluster.fts_nodes
                 if node.ip not in node_ips_to_remove]
            self.cluster.backup_nodes = \
                [node for node in self.cluster.backup_nodes
                 if node.ip not in node_ips_to_remove]

        # Fetch last rebalance task to track starting of current rebalance
        self.prev_rebalance_status_id = None
        server_task = self.rest.ns_server_tasks(
            task_type="rebalance", task_sub_type="rebalance")
        if server_task and "statusId" in server_task:
            self.prev_rebalance_status_id = server_task["statusId"]
        self.log.debug("Last known rebalance status_id: %s"
                       % self.prev_rebalance_status_id)

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
                non_swap_servers = set(self.servers) - set(
                    self.to_remove) - set(self.to_add)
                if self.check_vbucket_shuffling:
                    self.old_vbuckets = BucketHelper(
                        self.servers[0])._get_vbuckets(non_swap_servers, None)
                if self.old_vbuckets and self.check_vbucket_shuffling:
                    self.monitor_vbuckets_shuffling = True
                if self.monitor_vbuckets_shuffling \
                        and node_version_check and self.services:
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
                    self.test_log.debug("Will monitor vbucket shuffling for "
                                        "swap rebalance")
            self.state = "add_nodes"
            self.add_nodes()
            self.state = "triggering"
            self.start_rebalance()
            self.state = "triggered"
            self.table.display("Rebalance Overview")

            check_timeout = int(time.time()) + 10
            rebalance_started = False
            # Wait till current rebalance statusId updates in cluster's task
            while not rebalance_started and int(time.time()) < check_timeout:
                server_task = self.rest.ns_server_tasks(
                    task_type="rebalance", task_sub_type="rebalance")
                if server_task and server_task["statusId"] \
                        != self.prev_rebalance_status_id:
                    rebalance_started = True
                    self.prev_rebalance_status_id = server_task["statusId"]
                    self.log.debug("New rebalance status_id: %s"
                                   % server_task["statusId"])

            self.state = "running"
            self.check()
            # self.task_manager.schedule(self)
        except Exception as e:
            self.exception = e
            self.result = False
            self.test_log.error(str(e))
            return self.result
        self.complete_task()
        self.result = True
        self.log.critical("Nodes in cluster: %s" % [node.ip for node in self.cluster.nodes_in_cluster])
        self.log.critical("KV nodes      : %s" % [node.ip for node in self.cluster.kv_nodes])
        self.log.critical("Index nodes   : %s" % [node.ip for node in self.cluster.index_nodes])
        self.log.critical("Query nodes   : %s" % [node.ip for node in self.cluster.query_nodes])
        self.log.critical("CBAS nodes    : %s" % [node.ip for node in self.cluster.cbas_nodes])
        self.log.critical("FTS nodes     : %s" % [node.ip for node in self.cluster.fts_nodes])
        self.log.critical("Eventing nodes: %s" % [node.ip for node in self.cluster.eventing_nodes])
        self.log.critical("Backup nodes  : %s" % [node.ip for node in self.cluster.backup_nodes])
        return self.result

    def add_nodes(self):
        master = self.servers[0]
        node_index = 0
        server_groups = self.server_groups_to_add.keys() \
            if self.server_groups_to_add else []
        server_group_index = 0
        services_for_node = [CbServer.Services.KV]
        for node in self.to_add:
            zone_name = None

            # Logic to get the next server_group for the adding node
            if server_groups:
                zone_name = server_groups[server_group_index]
                self.server_groups_to_add[zone_name] -= 1
                if self.server_groups_to_add[zone_name] == 0:
                    server_group_index += 1

            if self.services and self.services is not None:
                services_for_node = [self.services[node_index]]
                node_index += 1
            self.table.add_row([node.ip, str(zone_name),
                                ",".join(services_for_node), "", "",
                                "<--- IN ---", ""])
            if self.use_hostnames:
                self.rest.add_node(master.rest_username, master.rest_password,
                                   node.hostname, node.port,
                                   zone_name=zone_name,
                                   services=services_for_node)
            else:
                self.rest.add_node(master.rest_username, master.rest_password,
                                   node.ip, node.port,
                                   zone_name=zone_name,
                                   services=services_for_node)
            for services in services_for_node:
                for service in services.split(","):
                    if service == CbServer.Services.KV:
                        self.cluster.kv_nodes.append(node)
                        continue
                    if service == CbServer.Services.INDEX:
                        self.cluster.index_nodes.append(node)
                        continue
                    if service == CbServer.Services.N1QL:
                        self.cluster.query_nodes.append(node)
                        continue
                    if service == CbServer.Services.CBAS:
                        self.cluster.cbas_nodes.append(node)
                        continue
                    if service == CbServer.Services.EVENTING:
                        self.cluster.eventing_nodes.append(node)
                        continue
                    if service == CbServer.Services.FTS:
                        self.cluster.fts_nodes.append(node)
                        continue
                    if service == CbServer.Services.BACKUP:
                        self.cluster.backup_nodes.append(node)
                        continue

            self.cluster.nodes_in_cluster.append(node)

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
                        if server.hostname == node.ip \
                                and int(server.port) == int(node.port):
                            ejectedNodes.append(node.id)
                            self.test_log.debug(remove_node_msg
                                                .format(node.ip, node.port))
                    elif server.ip == node.ip \
                            and int(server.port) == int(node.port):
                        ejectedNodes.append(node.id)
                        self.test_log.debug(remove_node_msg.format(node.ip,
                                                                   node.port))

        self.rest.rebalance(otpNodes=[node.id for node in nodes],
                            ejectedNodes=ejectedNodes)
        self.start_time = time.time()

    def check(self):
        self.poll = True
        while self.poll:
            self.poll = False
            try:
                if self.monitor_vbuckets_shuffling:
                    non_swap_servers = set(self.servers) - set(
                        self.to_remove) - set(self.to_add)
                    new_vbuckets = BucketHelper(self.servers[0])._get_vbuckets(
                        non_swap_servers, None)
                    for vb_type in ["active_vb", "replica_vb"]:
                        for srv in non_swap_servers:
                            if set(self.old_vbuckets[srv][vb_type]) != set(
                                    new_vbuckets[srv][vb_type]):
                                msg = "%s vBuckets were shuffled on %s! " \
                                      "Expected: %s, Got: %s" \
                                      % (vb_type, srv.ip,
                                         self.old_vbuckets[srv][vb_type],
                                         new_vbuckets[srv][vb_type])
                                self.test_log.error(msg)
                                raise Exception(msg)
                (status, progress) = self.rest._rebalance_status_and_progress(
                        self.prev_rebalance_status_id)
                self.test_log.info("Rebalance - status: %s, progress: %s",
                                   status,
                                   progress)
                # if ServerUnavailableException
                if progress == -100:
                    self.retry_get_progress += 1
                elif self.previous_progress != progress:
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
            if self.rest.is_cluster_mixed():
                """ for mix cluster, rebalance takes longer """
                self.test_log.debug("Rebalance in mix cluster")
                self.retry_get_process_num *= 2
            # we need to wait for status to be 'none'
            # (i.e. rebalance actually finished and not just 'running' and at 100%)
            # before we declare ourselves done
            if progress != -1 and status != 'none':
                if self.retry_get_progress < self.retry_get_process_num:
                    self.log.debug("Wait before next rebalance progress check")
                    sleep(10, log_type="infra")
                    self.poll = True
                else:
                    self.result = False
                    self.rest.print_UI_logs()
                    raise RebalanceFailedException(
                        "seems like rebalance hangs. please check logs!")
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
    def __init__(self, cluster, bucket, client, batch_size=1,
                 timeout_secs=5, time_unit="seconds", compression=None,
                 retries=5,
                 suppress_error_table=False, sdk_client_pool=None,
                 scope=CbServer.default_scope,
                 collection=CbServer.default_collection,
                 preserve_expiry=None, sdk_retry_strategy=None):
        super(GenericLoadingTask, self).__init__("Loadgen_task_%s_%s_%s_%s"
                                                 % (bucket, scope, collection,
                                                    time.time()))
        self.batch_size = batch_size
        self.timeout = timeout_secs
        self.time_unit = time_unit
        self.cluster = cluster
        self.bucket = bucket
        self.scope = scope
        self.collection = collection
        self.client = client
        self.sdk_client_pool = sdk_client_pool
        self.random = random.Random()
        self.compression = compression
        self.retries = retries
        self.suppress_error_table = suppress_error_table
        self.docs_loaded = 0
        self.preserve_expiry = preserve_expiry
        self.sdk_retry_strategy = sdk_retry_strategy

    def call(self):
        self.start_task()
        try:
            while self.has_next():
                self.next()
        except Exception as e:
            self.test_log.error(e)
            self.set_exception(Exception(e.message))
            return
        self.complete_task()

    def has_next(self):
        raise NotImplementedError

    def next(self):
        raise NotImplementedError

    # start of batch methods
    def batch_create(self, key_val, client=None, persist_to=0,
                     replicate_to=0,
                     doc_type="json", durability="", skip_read_on_error=False):
        """
        standalone method for creating key/values in batch (sans kvstore)

        arguments:
            key_val -- array of key/value dicts to load size = self.batch_size
            client -- optional client to use for data loading
        """
        success = dict()
        fail = dict()
        try:
            client = client or self.client
            success, fail = client.set_multi(
                key_val, self.exp, exp_unit=self.exp_unit,
                persist_to=persist_to, replicate_to=replicate_to,
                timeout=self.timeout, time_unit=self.time_unit,
                doc_type=doc_type, durability=durability,
                sdk_retry_strategy=self.sdk_retry_strategy)
            if fail:
                failed_item_table = None
                if not self.suppress_error_table:
                    failed_item_table = TableView(self.test_log.info)
                    failed_item_table.set_headers(["Create Key",
                                                   "Exception"])
                if not skip_read_on_error:
                    self.log.debug(
                        "Sleep before reading the doc for verification")
                    Thread.sleep(self.timeout)
#                     self.test_log.debug("Reading values {0} after failure"
#                                         .format(fail.keys()))
                    read_map, _ = self.batch_read(fail.keys())
                    for key, value in fail.items():
                        if key in read_map and read_map[key]["cas"] != 0:
                            success[key] = value
                            success[key].pop("error")
                            fail.pop(key)
                        elif not self.suppress_error_table:
                            failed_item_table.add_row([key, value['error']])
                elif not self.suppress_error_table:
                    for key, value in fail.items():
                        failed_item_table.add_row([key, value['error']])
                if not self.suppress_error_table:
                    failed_item_table.display("Keys failed in %s:%s:%s"
                                              % (self.client.bucket.name,
                                                 self.scope,
                                                 self.collection))
            return success, copy.deepcopy(fail)
        except Exception as error:
            self.test_log.error(error)
        return success, copy.deepcopy(fail)

    def batch_update(self, key_val, client=None, persist_to=0,
                     replicate_to=0,
                     doc_type="json", durability="", skip_read_on_error=False):
        success = dict()
        fail = dict()
        try:
            client = client or self.client
            success, fail = client.upsert_multi(
                key_val, self.exp, exp_unit=self.exp_unit,
                persist_to=persist_to, replicate_to=replicate_to,
                timeout=self.timeout, time_unit=self.time_unit,
                doc_type=doc_type, durability=durability,
                preserve_expiry=self.preserve_expiry,
                sdk_retry_strategy=self.sdk_retry_strategy)

            if fail:
                key_val = dict(key_val)
                if not self.suppress_error_table:
                    failed_item_table = TableView(self.test_log.info)
                    failed_item_table.set_headers(["Update Key",
                                                   "Exception"])
                if not skip_read_on_error:
                    self.log.debug(
                        "Sleep before reading the doc for verification")
                    Thread.sleep(self.timeout)
                    self.test_log.debug("Reading values {0} after failure"
                                        .format(fail.keys()))
                    read_map, _ = self.batch_read(fail.keys())
                    for key, value in fail.items():
                        if key in read_map and read_map[key]["cas"] != 0 \
                                and value == read_map[key]["value"]:
                            success[key] = value
                            success[key].pop("error")
                            fail.pop(key)
                        elif not self.suppress_error_table:
                            failed_item_table.add_row([key, value['error']])
                elif not self.suppress_error_table:
                    for key, value in fail.items():
                        failed_item_table.add_row([key, value['error']])
                if not self.suppress_error_table:
                    failed_item_table.display("Keys failed in %s:%s:%s"
                                              % (self.client.bucket.name,
                                                 self.scope,
                                                 self.collection))
            return success, copy.deepcopy(fail)
        except Exception as error:
            self.test_log.error(error)
        return success, copy.deepcopy(fail)

    def batch_replace(self, key_val, client=None, persist_to=0,
                      replicate_to=0,
                      doc_type="json", durability="",
                      skip_read_on_error=False):
        success = dict()
        fail = dict()
        try:
            client = client or self.client
            success, fail = client.replace_multi(
                key_val, self.exp, exp_unit=self.exp_unit,
                persist_to=persist_to, replicate_to=replicate_to,
                timeout=self.timeout, time_unit=self.time_unit,
                doc_type=doc_type, durability=durability,
                preserve_expiry=self.preserve_expiry,
                sdk_retry_strategy=self.sdk_retry_strategy)
            if fail:
                if not self.suppress_error_table:
                    failed_item_table = TableView(self.test_log.info)
                    failed_item_table.set_headers(["Replace Key",
                                                   "Exception"])
                if not skip_read_on_error:
                    self.log.debug(
                        "Sleep before reading the doc for verification")
                    Thread.sleep(self.timeout)
                    self.test_log.debug("Reading values {0} after failure"
                                        .format(fail.keys()))
                    read_map, _ = self.batch_read(fail.keys())
                    for key, value in fail.items():
                        if key in read_map and read_map[key]["cas"] != 0:
                            success[key] = value
                            success[key].pop("error")
                            fail.pop(key)
                        elif not self.suppress_error_table:
                            failed_item_table.add_row([key, value['error']])
                elif not self.suppress_error_table:
                    for key, value in fail.items():
                        failed_item_table.add_row([key, value['error']])
                if not self.suppress_error_table:
                    failed_item_table.display("Keys failed in %s:%s:%s"
                                              % (self.client.bucket.name,
                                                 self.scope,
                                                 self.collection))
            return success, copy.deepcopy(fail)
        except Exception as error:
            self.test_log.error(error)
        return success, copy.deepcopy(fail)

    def batch_delete(self, key_val, client=None, persist_to=None,
                     replicate_to=None,
                     durability=""):
        client = client or self.client
        success, fail = client.delete_multi(
            dict(key_val).keys(),
            persist_to=persist_to,
            replicate_to=replicate_to,
            timeout=self.timeout,
            time_unit=self.time_unit,
            durability=durability,
            sdk_retry_strategy=self.sdk_retry_strategy)
        if fail and not self.suppress_error_table:
            failed_item_view = TableView(self.test_log.info)
            failed_item_view.set_headers(["Delete Key", "Exception"])
            for key, exception in fail.items():
                failed_item_view.add_row([key, exception])
            failed_item_view.display("Keys failed in %s:%s:%s"
                                     % (client.bucket.name,
                                        self.scope,
                                        self.collection))
        return success, fail

    def batch_touch(self, key_val, exp=0):
        success, fail = self.client.touch_multi(
            dict(key_val).keys(),
            exp=exp,
            timeout=self.timeout,
            time_unit=self.time_unit,
            sdk_retry_strategy=self.sdk_retry_strategy)
        if fail and not self.suppress_error_table:
            failed_item_view = TableView(self.test_log.info)
            failed_item_view.set_headers(["Touch Key", "Exception"])
            for key, exception in fail.items():
                failed_item_view.add_row([key, exception])
            failed_item_view.display("Keys failed in %s:%s:%s"
                                     % (self.client.bucket.name,
                                        self.scope,
                                        self.collection))
        return success, fail

    def batch_read(self, keys, client=None):
        client = client or self.client
        success, fail = client.get_multi(
            keys, timeout=self.timeout,
            time_unit=self.time_unit,
            sdk_retry_strategy=self.sdk_retry_strategy)
        if fail and not self.suppress_error_table:
            failed_item_view = TableView(self.test_log.info)
            failed_item_view.set_headers(["Read Key", "Exception"])
            for key, exception in fail.items():
                failed_item_view.add_row([key, exception])
            failed_item_view.display("Keys failed in %s:%s:%s"
                                     % (client.bucket.name,
                                        self.scope,
                                        self.collection))
        return success, fail

    def batch_sub_doc_insert(self, key_value,
                             persist_to=0, replicate_to=0,
                             durability="",
                             create_path=True, xattr=False,
                             store_semantics=None,
                             access_deleted=False,
                             create_as_deleted=False):
        success = dict()
        fail = dict()
        try:
            success, fail = self.client.sub_doc_insert_multi(
                key_value,
                exp=self.exp,
                exp_unit=self.exp_unit,
                persist_to=persist_to,
                replicate_to=replicate_to,
                timeout=self.timeout,
                time_unit=self.time_unit,
                durability=durability,
                create_path=create_path,
                xattr=xattr,
                preserve_expiry=self.preserve_expiry,
                sdk_retry_strategy=self.sdk_retry_strategy,
                store_semantics=store_semantics,
                create_as_deleted=create_as_deleted)
        except Exception as error:
            self.log.error(error)
            self.set_exception("Exception during sub_doc insert: {0}"
                               .format(error))
        return success, fail

    def batch_sub_doc_upsert(self, key_value,
                             persist_to=0, replicate_to=0,
                             durability="",
                             create_path=True, xattr=False,
                             store_semantics=None,
                             access_deleted=False,
                             create_as_deleted=False):
        success = dict()
        fail = dict()
        try:
            success, fail = self.client.sub_doc_upsert_multi(
                key_value,
                exp=self.exp,
                exp_unit=self.exp_unit,
                persist_to=persist_to,
                replicate_to=replicate_to,
                timeout=self.timeout,
                time_unit=self.time_unit,
                durability=durability,
                create_path=create_path,
                xattr=xattr,
                preserve_expiry=self.preserve_expiry,
                sdk_retry_strategy=self.sdk_retry_strategy,
                store_semantics=store_semantics,
                access_deleted=access_deleted,
                create_as_deleted=create_as_deleted)
        except Exception as error:
            self.log.error(error)
            self.set_exception("Exception during sub_doc upsert: {0}"
                               .format(error))
        return success, fail

    def batch_sub_doc_replace(self, key_value,
                              persist_to=0, replicate_to=0,
                              durability="",
                              xattr=False,
                              store_semantics=None,
                              access_deleted=False,
                              create_as_deleted=False):
        success = dict()
        fail = dict()
        try:
            success, fail = self.client.sub_doc_replace_multi(
                key_value,
                exp=self.exp,
                exp_unit=self.exp_unit,
                persist_to=persist_to,
                replicate_to=replicate_to,
                timeout=self.timeout,
                time_unit=self.time_unit,
                durability=durability,
                xattr=xattr,
                preserve_expiry=self.preserve_expiry,
                sdk_retry_strategy=self.sdk_retry_strategy,
                store_semantics=store_semantics,
                access_deleted=access_deleted,
                create_as_deleted=create_as_deleted)
        except Exception as error:
            self.log.error(error)
            self.set_exception("Exception during sub_doc upsert: {0}"
                               .format(error))
        return success, fail

    def batch_sub_doc_remove(self, key_value,
                             persist_to=0, replicate_to=0,
                             durability="", xattr=False,
                             access_deleted=False,
                             create_as_deleted=False):
        success = dict()
        fail = dict()
        try:
            success, fail = self.client.sub_doc_remove_multi(
                key_value,
                exp=self.exp,
                exp_unit=self.exp_unit,
                persist_to=persist_to,
                replicate_to=replicate_to,
                timeout=self.timeout,
                time_unit=self.time_unit,
                durability=durability,
                xattr=xattr,
                preserve_expiry=self.preserve_expiry,
                sdk_retry_strategy=self.sdk_retry_strategy,
                access_deleted=access_deleted,
                create_as_deleted=create_as_deleted)
        except Exception as error:
            self.log.error(error)
            self.set_exception("Exception during sub_doc remove: {0}"
                               .format(error))
        return success, fail

    def batch_sub_doc_read(self, key_value, xattr=False,
                           access_deleted=False):
        success = dict()
        fail = dict()
        try:
            success, fail = self.client.sub_doc_read_multi(
                key_value,
                timeout=self.timeout,
                time_unit=self.time_unit,
                xattr=xattr,
                access_deleted=access_deleted)
        except Exception as error:
            self.log.error(error)
            self.set_exception("Exception during sub_doc read: {0}"
                               .format(error))
        return success, fail


class LoadDocumentsTask(GenericLoadingTask):
    def __init__(self, cluster, bucket, client, generator, op_type,
                 exp, random_exp=False, exp_unit="seconds", flag=0,
                 persist_to=0, replicate_to=0, time_unit="seconds",
                 proxy_client=None, batch_size=1, timeout_secs=5,
                 compression=None, retries=5,
                 durability="", task_identifier="", skip_read_on_error=False,
                 suppress_error_table=False, sdk_client_pool=None,
                 scope=CbServer.default_scope,
                 collection=CbServer.default_collection,
                 track_failures=True,
                 skip_read_success_results=False,
                 preserve_expiry=None, sdk_retry_strategy=None):

        super(LoadDocumentsTask, self).__init__(
            cluster, bucket, client, batch_size=batch_size,
            timeout_secs=timeout_secs,
            time_unit=time_unit,
            compression=compression,
            retries=retries, suppress_error_table=suppress_error_table,
            sdk_client_pool=sdk_client_pool,
            scope=scope, collection=collection,
            preserve_expiry=preserve_expiry,
            sdk_retry_strategy=sdk_retry_strategy)
        self.thread_name = "LoadDocs_%s_%s_%s_%s_%s_%s" \
                           % (task_identifier,
                              op_type,
                              durability,
                              generator._doc_gen.start,
                              generator._doc_gen.end,
                              time.time())
        self.generator = generator
        self.skip_doc_gen_value = False
        self.op_type = op_type
        if self.op_type in [DocLoading.Bucket.DocOps.DELETE,
                            DocLoading.Bucket.DocOps.TOUCH,
                            DocLoading.Bucket.DocOps.READ]:
            self.skip_doc_gen_value = True
        self.exp = exp
        self.abs_exp = self.exp
        self.random_exp = random_exp
        self.exp_unit = exp_unit
        self.flag = flag
        self.persist_to = persist_to
        self.replicate_to = replicate_to
        self.time_unit = time_unit
        self.num_loaded = 0
        self.durability = durability
        self.fail = dict()
        self.success = dict()
        self.skip_read_on_error = skip_read_on_error
        self.track_failures = track_failures
        self.skip_read_success_results = skip_read_success_results

        if proxy_client:
            self.log.debug("Changing client to proxy %s:%s..."
                           % (proxy_client.host, proxy_client.port))
            self.client = proxy_client

    def has_next(self):
        return self.generator.has_next()

    def next(self, override_generator=None):
        doc_gen = override_generator or self.generator
        key_value = doc_gen.next_batch(self.skip_doc_gen_value)
        if self.random_exp:
            self.exp = random.randint(self.abs_exp / 2, self.abs_exp)
        if self.sdk_client_pool is not None:
            self.client = \
                self.sdk_client_pool.get_client_for_bucket(self.bucket,
                                                           self.scope,
                                                           self.collection)
        if self.op_type == DocLoading.Bucket.DocOps.CREATE:
            success, fail = self.batch_create(
                key_value,
                persist_to=self.persist_to, replicate_to=self.replicate_to,
                doc_type=self.generator.doc_type, durability=self.durability,
                skip_read_on_error=self.skip_read_on_error)
            if self.track_failures:
                self.fail.update(fail)

        elif self.op_type == DocLoading.Bucket.DocOps.UPDATE:
            success, fail = self.batch_update(
                key_value,
                persist_to=self.persist_to,
                replicate_to=self.replicate_to,
                doc_type=self.generator.doc_type,
                durability=self.durability,
                skip_read_on_error=self.skip_read_on_error)
            if self.track_failures:
                self.fail.update(fail)

        elif self.op_type == DocLoading.Bucket.DocOps.REPLACE:
            success, fail = self.batch_replace(
                key_value,
                persist_to=self.persist_to,
                replicate_to=self.replicate_to,
                doc_type=self.generator.doc_type,
                durability=self.durability,
                skip_read_on_error=self.skip_read_on_error)
            if self.track_failures:
                self.fail.update(fail)

        elif self.op_type == DocLoading.Bucket.DocOps.DELETE:
            success, fail = self.batch_delete(key_value,
                                              persist_to=self.persist_to,
                                              replicate_to=self.replicate_to,
                                              durability=self.durability)
            if self.track_failures:
                self.fail.update(fail)

        elif self.op_type == DocLoading.Bucket.DocOps.TOUCH:
            success, fail = self.batch_touch(key_value,
                                             exp=self.exp)
            if self.track_failures:
                self.fail.update(fail)

        elif self.op_type == DocLoading.Bucket.DocOps.READ:
            success, fail = self.batch_read(dict(key_value).keys())
            if self.track_failures:
                self.fail.update(fail)
            if not self.skip_read_success_results:
                self.success.update(success)
        else:
            self.set_exception(Exception("Bad operation: %s" % self.op_type))

        if self.sdk_client_pool is not None:
            self.sdk_client_pool.release_client(self.client)
            self.client = None

        self.docs_loaded += len(key_value)


class LoadSubDocumentsTask(GenericLoadingTask):
    def __init__(self, cluster, bucket, client, generator,
                 op_type, exp, create_paths=False,
                 xattr=False,
                 exp_unit="seconds", flag=0,
                 persist_to=0, replicate_to=0, time_unit="seconds",
                 batch_size=1, timeout_secs=5,
                 compression=None, retries=5,
                 durability="", task_identifier="",
                 sdk_client_pool=None,
                 scope=CbServer.default_scope,
                 collection=CbServer.default_collection,
                 skip_read_success_results=False,
                 preserve_expiry=None,
                 sdk_retry_strategy=None,
                 store_semantics=None,
                 access_deleted=False,
                 create_as_deleted=False,
                 track_failures=True):
        super(LoadSubDocumentsTask, self).__init__(
            cluster, bucket, client, batch_size=batch_size,
            timeout_secs=timeout_secs,
            time_unit=time_unit, compression=compression,
            sdk_client_pool=sdk_client_pool,
            scope=scope, collection=collection,
            preserve_expiry=preserve_expiry,
            sdk_retry_strategy=sdk_retry_strategy)
        self.thread_name = "LoadSubDocsTask-%s_%s_%s_%s_%s" % (
            task_identifier,
            generator._doc_gen.start,
            generator._doc_gen.end,
            op_type,
            durability)
        self.generator = generator
        self.skip_doc_gen_value = False
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
        self.store_semantics = store_semantics
        self.access_deleted = access_deleted
        self.create_as_deleted = create_as_deleted
        self.fail = dict()
        self.success = dict()
        self.skip_read_success_results = skip_read_success_results
        self.track_failures = track_failures

    def has_next(self):
        return self.generator.has_next()

    def next(self, override_generator=None):
        doc_gen = override_generator or self.generator
        key_value = doc_gen.next_batch(self.skip_doc_gen_value)
        if self.sdk_client_pool is not None:
            self.client = \
                self.sdk_client_pool.get_client_for_bucket(self.bucket,
                                                           self.scope,
                                                           self.collection)
        if self.op_type == DocLoading.Bucket.SubDocOps.INSERT:
            success, fail = self.batch_sub_doc_insert(
                key_value,
                persist_to=self.persist_to,
                replicate_to=self.replicate_to,
                durability=self.durability,
                create_path=self.create_path,
                xattr=self.xattr,
                store_semantics=self.store_semantics,
                access_deleted=self.access_deleted,
                create_as_deleted=self.create_as_deleted)
            if self.track_failures:
                self.fail.update(fail)
            # self.success.update(success)
        elif self.op_type == DocLoading.Bucket.SubDocOps.UPSERT:
            success, fail = self.batch_sub_doc_upsert(
                key_value,
                persist_to=self.persist_to,
                replicate_to=self.replicate_to,
                durability=self.durability,
                create_path=self.create_path,
                xattr=self.xattr,
                store_semantics=self.store_semantics,
                access_deleted=self.access_deleted,
                create_as_deleted=self.create_as_deleted)
            if self.track_failures:
                self.fail.update(fail)
            # self.success.update(success)
        elif self.op_type == DocLoading.Bucket.SubDocOps.REMOVE:
            success, fail = self.batch_sub_doc_remove(
                key_value,
                persist_to=self.persist_to,
                replicate_to=self.replicate_to,
                durability=self.durability,
                xattr=self.xattr,
                access_deleted=self.access_deleted,
                create_as_deleted=self.create_as_deleted)
            if self.track_failures:
                self.fail.update(fail)
            # self.success.update(success)
        elif self.op_type == "replace":
            success, fail = self.batch_sub_doc_replace(
                key_value,
                persist_to=self.persist_to,
                replicate_to=self.replicate_to,
                durability=self.durability,
                xattr=self.xattr,
                store_semantics=self.store_semantics,
                access_deleted=self.access_deleted,
                create_as_deleted=self.create_as_deleted)
            if self.track_failures:
                self.fail.update(fail)
            # self.success.update(success)
        elif self.op_type in ['read', 'lookup']:
            success, fail = self.batch_sub_doc_read(
                    key_value,
                    xattr=self.xattr,
                    access_deleted=self.access_deleted)
            if self.track_failures:
                self.fail.update(fail)
            if not self.skip_read_success_results:
                self.success.update(success)
        else:
            self.set_exception(Exception("Bad operation type: %s"
                                         % self.op_type))

        self.docs_loaded += len(key_value)

        if self.sdk_client_pool is not None:
            self.sdk_client_pool.release_client(self.client)
            self.client = None


class Durability(Task):
    def __init__(self, cluster, task_manager, bucket, clients, generator,
                 op_type, exp, exp_unit="seconds", flag=0,
                 persist_to=0, replicate_to=0, time_unit="seconds",
                 batch_size=1,
                 timeout_secs=5, compression=None, process_concurrency=8,
                 print_ops_rate=True, retries=5, durability="",
                 majority_value=0, check_persistence=False,
                 sdk_retry_strategy=None):

        super(Durability, self).__init__("DurabilityDocumentsMainTask_%s_%s"
                                         % (bucket, time.time()))
        self.majority_value = majority_value
        self.fail = dict()
        # self.success = dict()
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
        self.persist_to = persist_to
        self.replicate_to = replicate_to
        self.time_unit = time_unit
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
        self.sdk_retry_strategy = sdk_retry_strategy
        self.tasks = list()
        if isinstance(op_type, list):
            self.op_types = op_type
        else:
            self.op_type = op_type
        if isinstance(bucket, list):
            self.buckets = bucket
        else:
            self.bucket = bucket

    def call(self):
        generators = list()
        gen_start = int(self.generator.start)
        gen_end = max(int(self.generator.end), 1)
        gen_range = max(int((
                                    self.generator.end - self.generator.start) / self.process_concurrency),
                        1)
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
                persist_to=self.persist_to, replicate_to=self.replicate_to,
                time_unit=self.time_unit, batch_size=self.batch_size,
                timeout_secs=self.timeout_secs,
                compression=self.compression,
                instance_num=i, durability=self.durability,
                check_persistence=self.check_persistence,
                sdk_retry_strategy=self.sdk_retry_strategy)
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
                    self.sdk_acked_curd_failed.update(
                        task.sdk_acked_curd_failed)
                    self.sdk_exception_crud_succeed.update(
                        task.sdk_exception_crud_succeed)
                    self.sdk_acked_pers_failed.update(
                        task.sdk_acked_pers_failed)
                    self.sdk_exception_pers_succeed.update(
                        task.sdk_exception_pers_succeed)
        except Exception as e:
            self.set_exception(e)
        finally:
            self.log.debug("=== Tasks in DurabilityDocumentsMainTask pool ===")
            self.task_manager.print_tasks_in_pool()
            self.log.debug("=================================================")
            for task in self.tasks:
                self.task_manager.stop_task(task)
            for client in self.clients:
                client.close()

    class Loader(GenericLoadingTask):
        """
        1. Start inserting data into buckets
        2. Keep updating the write offset
        3. Start the reader thread
        4. Keep track of non durable documents
        """

        def __init__(self, cluster, bucket, client, generator, op_type,
                     exp, exp_unit,
                     flag=0, majority_value=0, persist_to=0, replicate_to=0,
                     time_unit="seconds",
                     batch_size=1, timeout_secs=5,
                     compression=None, retries=5,
                     instance_num=0, durability="", check_persistence=False,
                     sdk_client_pool=None,
                     scope=CbServer.default_scope,
                     collection=CbServer.default_collection,
                     sdk_retry_strategy=None):
            super(Durability.Loader, self).__init__(
                cluster, bucket, client, batch_size=batch_size,
                timeout_secs=timeout_secs,
                compression=compression,
                sdk_client_pool=sdk_client_pool,
                scope=scope, collection=collection,
                sdk_retry_strategy=sdk_retry_strategy)
            self.thread_name = "DurableDocLoaderTask_%d_%s_%d_%d_%s" \
                               % (instance_num, bucket,
                                  generator._doc_gen.start,
                                  generator._doc_gen.end,
                                  op_type)
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
            self.test_log.debug("Instance %s: doc loading starts from %s"
                                % (self.instance, generator._doc_gen.start))
            self.task_manager = TaskManager()

        def call(self):
            self.start_task()
            if self.check_persistence:
                persistence = threading.Thread(target=self.Persistence)
                persistence.start()
            reader = threading.Thread(target=self.Reader)
            reader.start()

            self.log.debug("Starting loader thread '%s'" % self.thread_name)
            try:
                while self.has_next():
                    self.next()
            except Exception as e:
                self.set_exception(Exception(e.message))
            self.log.debug("Loader thread '%s' completed" % self.thread_name)

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
                    replicate_to=self.replicate_to,
                    doc_type=self.generator.doc_type,
                    durability=self.durability)

                if len(f_docs) > 0:
                    self.create_failed.update(f_docs)
            elif self.op_type == 'update':
                keys_for_update = list()
                for item in key_value:
                    keys_for_update.append(item.getT1())
                self.docs_to_be_updated.update(
                    self.batch_read(keys_for_update)[0])
                _, f_docs = self.batch_update(
                    key_value, persist_to=self.persist_to,
                    replicate_to=self.replicate_to,
                    durability=self.durability,
                    doc_type=self.generator.doc_type)
                self.update_failed.update(f_docs)
            elif self.op_type == 'delete':
                keys_for_update = list()
                for item in key_value:
                    keys_for_update.append(item.getT1())
                self.docs_to_be_deleted.update(
                    self.batch_read(keys_for_update)[0])
                _, fail = self.batch_delete(
                    key_value,
                    persist_to=self.persist_to,
                    replicate_to=self.replicate_to,
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
                shells.update({server.ip: Cbstats(server)})

            while True:
                if self.persistence_offset < self.write_offset or self.persistence_offset == self.end:
                    self.test_log.debug(
                        "Persistence: ReadOffset=%s, WriteOffset=%s, Reader: FinalOffset=%s"
                        % (self.persistence_offset,
                           self.write_offset,
                           self.end))
                    if self.generator_persist._doc_gen.has_next():
                        doc = self.generator_persist._doc_gen.next()
                        key, val = doc[0], doc[1]
                        vBucket = (((zlib.crc32(key)) >> 16) & 0x7fff) & (
                                len(self.bucket.vbuckets) - 1)
                        nodes = [self.bucket.vbuckets[vBucket].master]
                        if self.durability \
                                == Bucket.DurabilityLevel.PERSIST_TO_MAJORITY:
                            nodes += self.bucket.vbuckets[vBucket].replica
                        count = 0
                        if self.op_type == 'create':
                            try:
                                for node in nodes:
                                    key_stat = shells[
                                        node.split(":")[0]].vkey_stat(
                                        self.bucket.name, key)
                                    if key_stat["is_dirty"].lower() == "true":
                                        self.test_log.error(
                                            "Node: %s, Key: %s, key_is_dirty = %s" % (
                                                node.split(":")[0], key,
                                                key_stat["is_dirty"]))
                                    else:
                                        self.test_log.info(
                                            "Node: %s, Key: %s, key_is_dirty = %s" % (
                                                node.split(":")[0], key,
                                                key_stat["is_dirty"]))
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
                                    self.sdk_acked_pers_failed.update(
                                        {key: val})
                                    self.test_log.error(
                                        "Key isn't persisted although SDK reports Durable, Key = %s" % key)
                            elif count > 0:
                                self.test_log.error(
                                    "SDK threw exception but document is present in the Server -> %s" % key)
                                self.sdk_exception_crud_succeed.update(
                                    {key: val})
                            else:
                                self.test_log.error(
                                    "Document is rolled back to nothing during create -> %s" % (
                                        key))

                        if self.op_type == 'update':
                            if key not in self.update_failed[
                                self.instance].keys():
                                try:
                                    for node in nodes:
                                        key_stat = shells[
                                            node.split(":")[0]].vkey_stat(
                                            self.bucket.name, key)
                                        if key_stat[
                                            "is_dirty"].lower() == "true":
                                            self.test_log.error(
                                                "Node: %s, Key: %s, key_is_dirty = %s" % (
                                                    node.split(":")[0], key,
                                                    key_stat["is_dirty"]))
                                        else:
                                            self.test_log.debug(
                                                "Node: %s, Key: %s, key_is_dirty = %s" % (
                                                    node.split(":")[0], key,
                                                    key_stat["is_dirty"]))
                                            count += 1
                                except:
                                    pass
                                if not count > 0:
                                    self.sdk_acked_pers_failed.update(
                                        {key: val})
                                    self.test_log.error(
                                        "Key isn't persisted although SDK reports Durable, Key = %s getFromAllReplica = %s" % key)
                        if self.op_type == 'delete':
                            for node in nodes:
                                try:
                                    key_stat = shells[
                                        node.split(":")[0]].vkey_stat(
                                        self.bucket.name, key)
                                    if key_stat["is_dirty"].lower() == "true":
                                        self.test_log.error(
                                            "Node: %s, Key: %s, key_is_dirty = %s" % (
                                                node.split(":")[0], key,
                                                key_stat["is_dirty"]))
                                    else:
                                        self.test_log.debug(
                                            "Node: %s, Key: %s, key_is_dirty = %s" % (
                                                node.split(":")[0], key,
                                                key_stat["is_dirty"]))
                                        count += 1
                                except Exception as e:
                                    pass
                            if key not in self.delete_failed[
                                self.instance].keys():
                                if count == (self.bucket.replicaNumber + 1):
                                    # if count > (self.bucket.replicaNumber+1 - self.majority_value):
                                    self.sdk_acked_pers_failed.update(
                                        {key: val})
                                    self.test_log.error(
                                        "Key isn't Persisted-Delete although SDK reports Durable, Key = %s getFromAllReplica = %s" % key)
                            elif count >= self.majority_value:
                                self.test_log.error(
                                    "Document is rolled back to original during delete -> %s" % (
                                        key))

                    if self.persistence_offset == self.end:
                        self.log.warning("Breaking thread persistence!!")
                        break
                    self.persistence_offset = self.write_offset

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
                        % (self.read_offset, self.op_type, self.write_offset,
                           self.end))
                    if self.generator_reader._doc_gen.has_next():
                        doc = self.generator_reader._doc_gen.next()
                        key, val = doc[0], doc[1]
                        if self.op_type == 'create':
                            result = self.client.get_from_all_replicas(key)
                            self.test_log.debug(
                                "Key = %s getFromAllReplica = %s" % (
                                    key, result))
                            if key not in self.create_failed.keys():
                                if len(result) == 0:
                                    # if len(result) < self.majority_value:
                                    self.sdk_acked_curd_failed.update(
                                        {key: val})
                                    self.test_log.error(
                                        "Key isn't durable although SDK reports Durable, Key = %s getFromAllReplica = %s"
                                        % (key, result))
                            elif len(result) > 0:
                                if not (
                                        SDKException.DurabilityAmbiguousException in
                                        self.create_failed[key]["error"] or
                                        SDKException.TimeoutException in
                                        self.create_failed[key]["error"]):
                                    self.test_log.error(
                                        "SDK threw exception but document is present in the Server -> %s:%s"
                                        % (key, result))
                                    self.sdk_exception_crud_succeed.update(
                                        {key: val})
                            else:
                                self.test_log.debug(
                                    "Document is rolled back to nothing during create -> %s:%s"
                                    % (key, result))
                        if self.op_type == 'update':
                            result = self.client.get_from_all_replicas(key)
                            if key not in self.update_failed.keys():
                                if len(result) == 0:
                                    # if len(result) < self.majority_value:
                                    self.sdk_acked_curd_failed.update(
                                        {key: val})
                                    self.test_log.error(
                                        "Key isn't durable although SDK reports Durable, Key = %s getFromAllReplica = %s"
                                        % (key, result))
                                else:
                                    # elif len(result) >= self.majority_value:
                                    temp_count = 0
                                    for doc in result:
                                        if doc["value"] == \
                                                self.docs_to_be_updated[key][
                                                    "value"]:
                                            self.test_log.error(
                                                "Doc content is not updated yet on few nodes, Key = %s getFromAllReplica = %s"
                                                % (key, result))
                                        else:
                                            temp_count += 1
                                    '''
                                    This make sure that value has been updated
                                    on at least 1 node
                                    '''
                                    if temp_count == 0:
                                        # if temp_count < self.majority_value:
                                        self.sdk_acked_curd_failed.update(
                                            {key: val})
                            else:
                                for doc in result:
                                    if doc["value"] != \
                                            self.docs_to_be_updated[key][
                                                "value"]:
                                        self.sdk_exception_crud_succeed.update(
                                            {key: val})
                                        self.test_log.error(
                                            "Doc content is updated although SDK threw exception, Key = %s getFromAllReplica = %s"
                                            % (key, result))
                        if self.op_type == 'delete':
                            result = self.client.get_from_all_replicas(key)
                            if key not in self.delete_failed.keys():
                                if len(result) > self.bucket.replicaNumber:
                                    self.sdk_acked_curd_failed.update(result[0])
                                    self.test_log.error(
                                        "Key isn't durably deleted although SDK reports Durable, Key = %s getFromAllReplica = %s"
                                        % (key, result))
                            elif len(result) == self.bucket.replicaNumber + 1:
                                self.test_log.warn(
                                    "Document is rolled back to original during delete -> %s:%s"
                                    % (key, result))
                    if self.read_offset == self.end:
                        self.test_log.fatal("BREAKING!!")
                        break
                    self.read_offset += 1


class LoadDocumentsGeneratorsTask(Task):
    def __init__(self, cluster, task_manager, bucket, clients, generators,
                 op_type, exp, exp_unit="seconds", random_exp=False, flag=0,
                 persist_to=0, replicate_to=0, time_unit="seconds",
                 batch_size=1,
                 timeout_secs=5, compression=None, process_concurrency=8,
                 print_ops_rate=True, retries=5, durability="",
                 task_identifier="", skip_read_on_error=False,
                 suppress_error_table=False,
                 sdk_client_pool=None,
                 scope=CbServer.default_scope,
                 collection=CbServer.default_collection,
                 monitor_stats=["doc_ops"],
                 track_failures=True,
                 preserve_expiry=None,
                 sdk_retry_strategy=None):
        super(LoadDocumentsGeneratorsTask, self).__init__(
            "LoadDocsGen_%s_%s_%s_%s_%s"
            % (bucket, scope, collection, task_identifier, time.time()))
        self.cluster = cluster
        self.exp = exp
        self.random_exp = random_exp
        self.exp_unit = exp_unit
        self.flag = flag
        self.persist_to = persist_to
        self.replicate_to = replicate_to
        self.time_unit = time_unit
        self.timeout_secs = timeout_secs
        self.compression = compression
        self.process_concurrency = process_concurrency
        self.clients = clients
        self.sdk_client_pool = sdk_client_pool
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
        self.suppress_error_table = suppress_error_table
        self.monitor_stats = monitor_stats
        self.scope = scope
        self.collection = collection
        self.preserve_expiry = preserve_expiry
        self.sdk_retry_strategy = sdk_retry_strategy
        if isinstance(op_type, list):
            self.op_types = op_type
        else:
            self.op_type = op_type
        if isinstance(bucket, list):
            self.buckets = bucket
        else:
            self.bucket = bucket
        self.num_loaded = 0
        self.track_failures = track_failures
        self.fail = dict()
        self.success = dict()
        self.print_ops_rate_tasks = list()

    def call(self):
        self.start_task()
        buckets_for_ops_rate_task = list()
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
        tasks = list()
        for generator in self.generators:
            if self.op_types:
                self.op_type = self.op_types[iterator]
            if self.buckets:
                self.bucket = self.buckets[iterator]
            tasks.extend(self.get_tasks(generator))
            iterator += 1
        if self.print_ops_rate:
            if self.buckets:
                buckets_for_ops_rate_task = self.buckets
            else:
                buckets_for_ops_rate_task = [self.bucket]
            for bucket in buckets_for_ops_rate_task:
                bucket.stats.manage_task(
                    "start", self.task_manager,
                    cluster=self.cluster,
                    bucket=bucket,
                    monitor_stats=self.monitor_stats,
                    sleep=1)
        try:
            for task in tasks:
                self.task_manager.add_new_task(task)
            for task in tasks:
                try:
                    self.task_manager.get_task_result(task)
                    self.log.debug("Items loaded in task {} are {}"
                                   .format(task.thread_name, task.docs_loaded))
                    i = 0
                    while task.docs_loaded < (task.generator._doc_gen.end -
                                              task.generator._doc_gen.start) \
                            and i < 60:
                        sleep(1, "Bug in java futures task. "
                                 "Items loaded in task %s: %s"
                              % (task.thread_name, task.docs_loaded),
                              log_type="infra")
                        i += 1
                except Exception as e:
                    self.test_log.error(e)
                finally:
                    self.success.update(task.success)
                    if self.track_failures:
                        self.fail.update(task.fail)
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
            if self.print_ops_rate:
                for bucket in buckets_for_ops_rate_task:
                    bucket.stats.manage_task("stop", self.task_manager)
            self.log.debug("========= Tasks in loadgen pool=======")
            self.task_manager.print_tasks_in_pool()
            self.log.debug("======================================")
            for task in tasks:
                self.task_manager.stop_task(task)
                self.log.debug("Task '{0}' complete. Loaded {1} items"
                               .format(task.thread_name, task.docs_loaded))
            if self.sdk_client_pool is None:
                for client in self.clients:
                    client.close()
        self.complete_task()
        return self.fail

    def get_tasks(self, generator):
        generators = []
        tasks = []
        gen_start = int(generator.start)
        gen_end = int(generator.end)
        gen_range = max(
            int((generator.end - generator.start) / self.process_concurrency),
            1)
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
                self.op_type, self.exp, self.random_exp, self.exp_unit,
                self.flag,
                persist_to=self.persist_to, replicate_to=self.replicate_to,
                time_unit=self.time_unit, batch_size=self.batch_size,
                timeout_secs=self.timeout_secs,
                compression=self.compression,
                durability=self.durability,
                task_identifier=self.thread_name,
                skip_read_on_error=self.skip_read_on_error,
                suppress_error_table=self.suppress_error_table,
                sdk_client_pool=self.sdk_client_pool,
                scope=self.scope, collection=self.collection,
                track_failures=self.track_failures,
                preserve_expiry=self.preserve_expiry,
                sdk_retry_strategy=self.sdk_retry_strategy)
            tasks.append(task)
        return tasks


class LoadSubDocumentsGeneratorsTask(Task):
    def __init__(self, cluster, task_manager, bucket, clients,
                 generators,
                 op_type, exp, create_paths=False,
                 xattr=False, exp_unit="seconds", flag=0,
                 persist_to=0, replicate_to=0, time_unit="seconds",
                 batch_size=1,
                 timeout_secs=5, compression=None,
                 process_concurrency=8,
                 print_ops_rate=True, retries=5, durability="",
                 task_identifier="",
                 sdk_client_pool=None,
                 scope=CbServer.default_scope,
                 collection=CbServer.default_collection,
                 preserve_expiry=None, sdk_retry_strategy=None,
                 store_semantics=None,
                 access_deleted=False,
                 create_as_deleted=False):
        thread_name = "SubDocumentsLoadGenTask_%s_%s_%s_%s_%s" \
                      % (task_identifier,
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
        self.persist_to = persist_to
        self.replicate_to = replicate_to
        self.time_unit = time_unit
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
        self.sdk_client_pool = sdk_client_pool
        self.scope = scope
        self.collection = collection
        self.preserve_expiry = preserve_expiry
        self.sdk_retry_strategy = sdk_retry_strategy
        self.store_semantics = store_semantics
        self.access_deleted = access_deleted
        self.create_as_deleted = create_as_deleted
        if isinstance(op_type, list):
            self.op_types = op_type
        else:
            self.op_type = op_type
        if isinstance(bucket, list):
            self.buckets = bucket
        else:
            self.bucket = bucket
        self.print_ops_rate_tasks = list()
        self.num_loaded = 0
        self.fail = dict()
        self.success = dict()

    def call(self):
        self.start_task()
        buckets_for_ops_rate_task = list()
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
        tasks = list()
        for generator in self.generators:
            if self.op_types:
                self.op_type = self.op_types[iterator]
            if self.buckets:
                self.bucket = self.buckets[iterator]
            tasks.extend(self.get_tasks(generator))
            iterator += 1
        if self.print_ops_rate:
            if self.buckets:
                buckets_for_ops_rate_task = self.buckets
            else:
                buckets_for_ops_rate_task = [self.bucket]
            for bucket in buckets_for_ops_rate_task:
                bucket.stats.manage_task(
                    "start", self.task_manager,
                    cluster=self.cluster,
                    bucket=bucket,
                    monitor_stats=["doc_ops"],
                    sleep=1)
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
                    self.log.warning("Failed to load {} sub_docs from {} "
                                     "to {}"
                                     .format(task.fail.__len__(),
                                             task.generator._doc_gen.start,
                                             task.generator._doc_gen.end))
        except Exception as e:
            self.log.error(e)
            self.set_exception(e)
        finally:
            if self.print_ops_rate:
                for bucket in buckets_for_ops_rate_task:
                    bucket.stats.manage_task("stop", self.task_manager)
            self.log.debug("===========Tasks in loadgen pool=======")
            self.task_manager.print_tasks_in_pool()
            self.log.debug("=======================================")
            for task in tasks:
                self.task_manager.stop_task(task)
            if self.sdk_client_pool is None:
                for client in self.clients:
                    client.close()
        self.complete_task()
        return self.fail

    def get_tasks(self, generator):
        generators = list()
        tasks = list()
        gen_start = int(generator.start)
        gen_end = int(generator.end)
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
            task = LoadSubDocumentsTask(
                self.cluster, self.bucket,
                self.clients[i], generators[i],
                self.op_type, self.exp,
                create_paths=self.create_path,
                xattr=self.xattr,
                exp_unit=self.exp_unit,
                flag=self.flag,
                persist_to=self.persist_to,
                replicate_to=self.replicate_to,
                time_unit=self.time_unit,
                batch_size=self.batch_size,
                timeout_secs=self.timeout_secs,
                compression=self.compression,
                retries=self.retries,
                durability=self.durability,
                scope=self.scope,
                collection=self.collection,
                sdk_client_pool=self.sdk_client_pool,
                preserve_expiry=self.preserve_expiry,
                sdk_retry_strategy=self.sdk_retry_strategy,
                store_semantics=self.store_semantics,
                access_deleted=self.access_deleted,
                create_as_deleted=self.create_as_deleted)
            tasks.append(task)
        return tasks


class ContinuousDocOpsTask(Task):
    def __init__(self, cluster, task_manager, bucket, clients, generator,
                 op_type="update", exp=0, flag=0, persist_to=0, replicate_to=0,
                 durability="", time_unit="seconds",
                 batch_size=1,
                 timeout_secs=5, compression=None,
                 process_concurrency=4,
                 scope=CbServer.default_scope,
                 collection=CbServer.default_collection,
                 sdk_client_pool=None, sdk_retry_strategy=None):
        super(ContinuousDocOpsTask, self).__init__(
            "ContDocOps_%s_%s_%s_%s"
            % (bucket.name, scope, collection, time.time()))
        self.cluster = cluster
        self.exp = exp
        self.flag = flag
        self.persist_to = persist_to
        self.replicate_to = replicate_to
        self.durability = durability
        self.time_unit = time_unit
        self.timeout_secs = timeout_secs
        self.compression = compression
        self.process_concurrency = process_concurrency
        self.clients = clients
        self.sdk_client_pool = sdk_client_pool
        self.task_manager = task_manager
        self.batch_size = batch_size
        self.generator = generator
        self.buckets = None
        # self.success = dict()
        self.fail = dict()

        self.key = self.generator.name
        self.doc_start_num = self.generator.start
        self.doc_end_num = self.generator.end
        self.doc_type = self.generator.doc_type
        self.op_type = op_type
        self.sdk_retry_strategy = sdk_retry_strategy
        self.itr_count = 0
        self.__stop_updates = False

        if isinstance(bucket, list):
            self.buckets = bucket
        else:
            self.bucket = bucket
        self.scope = scope
        self.collection = collection

    def end_task(self):
        self.__stop_updates = True

    def _start_doc_ops(self, bucket):
        self.test_log.info("Performing doc ops '%s' on %s" % (self.op_type,
                                                              bucket.name))
        while not self.__stop_updates:
            self.itr_count += 1
            doc_gens = list()
            doc_tasks = list()
            task_instance = 1
            for _ in self.clients:
                doc_gens.append(copy.deepcopy(self.generator))

            for index, generator in enumerate(doc_gens):
                batch_gen = BatchedDocumentGenerator(generator,
                                                     self.batch_size)
                task = LoadDocumentsTask(
                    self.cluster, bucket, self.clients[index],
                    batch_gen, self.op_type, self.exp,
                    task_identifier="%s_%s" % (self.thread_name,
                                               task_instance),
                    persist_to=self.persist_to,
                    replicate_to=self.replicate_to,
                    durability=self.durability,
                    batch_size=self.batch_size,
                    timeout_secs=self.timeout_secs,
                    scope=self.scope,
                    collection=self.collection,
                    sdk_client_pool=self.sdk_client_pool,
                    skip_read_success_results=True,
                    sdk_retry_strategy=self.sdk_retry_strategy)
                self.task_manager.add_new_task(task)
                doc_tasks.append(task)
                self.fail.update(task.fail)
                task_instance += 1

            for task in doc_tasks:
                self.task_manager.get_task_result(task)
                del task
        self.test_log.info("Closing SDK clients..")
        for client in self.clients:
            if client is not None:
                client.close()
        self.test_log.info("Done doc_ops on %s. Total iterations: %d"
                           % (bucket.name, self.itr_count))

    def call(self):
        self.start_task()
        if self.buckets:
            for bucket in self.buckets:
                self._start_doc_ops(bucket)
        else:
            self._start_doc_ops(self.bucket)
        self.complete_task()


class LoadDocumentsForDgmTask(LoadDocumentsGeneratorsTask):
    def __init__(self, cluster, task_manager, bucket, clients, doc_gen, exp,
                 batch_size=50,
                 persist_to=0, replicate_to=0,
                 durability="",
                 timeout_secs=5,
                 process_concurrency=4, print_ops_rate=True,
                 active_resident_threshold=99,
                 dgm_batch=5000,
                 scope=CbServer.default_scope,
                 collection=CbServer.default_collection,
                 skip_read_on_error=False,
                 suppress_error_table=False,
                 track_failures=True,
                 task_identifier="",
                 sdk_client_pool=None,
                 sdk_retry_strategy=None):
        super(LoadDocumentsForDgmTask, self).__init__(
            self, cluster, task_manager, bucket, clients, None,
            "create", exp,
            task_identifier="DGM_%s_%s_%s_%s" % (bucket.name, scope,
                                                 collection, time.time()))

        self.cluster = cluster
        self.exp = exp
        self.doc_gen = doc_gen
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
        self.dgm_batch = dgm_batch
        self.task_identifier = task_identifier
        self.skip_read_on_error = skip_read_on_error
        self.suppress_error_table = suppress_error_table
        self.track_failures = track_failures
        self.op_type = "create"
        self.rest_client = BucketHelper(self.cluster.master)
        self.doc_index = self.doc_gen.start
        self.docs_loaded_per_bucket = dict()
        if isinstance(bucket, list):
            self.buckets = bucket
        else:
            self.buckets = [bucket]
        self.scope = scope
        self.collection = collection
        self.sdk_client_pool = sdk_client_pool
        self.sdk_retry_strategy = sdk_retry_strategy

    def _get_bucket_dgm(self, bucket):
        """
        Returns a tuple of (active_rr, replica_rr)
        """
        try:
            active_resident_items_ratio = self.rest_client.fetch_bucket_stats(
                bucket.name)["op"]["samples"][
                "vb_active_resident_items_ratio"][-1]
            replica_resident_items_ratio = self.rest_client.fetch_bucket_stats(
                bucket.name)["op"]["samples"][
                "vb_replica_resident_items_ratio"][-1]
        except KeyError:
            active_resident_items_ratio = replica_resident_items_ratio = 100
        return active_resident_items_ratio, replica_resident_items_ratio

    def _load_next_batch_of_docs(self, bucket):
        doc_gens = [deepcopy(self.doc_gen)
                    for _ in range(self.process_concurrency)]
        doc_tasks = list()
        self.test_log.debug("Doc load from index %d" % self.doc_index)
        for index in range(self.process_concurrency):
            doc_gens[index].start = self.doc_index
            doc_gens[index].itr = self.doc_index
            doc_gens[index].end = self.doc_index + self.dgm_batch
            self.doc_index += self.dgm_batch
            self.docs_loaded_per_bucket[bucket] += self.dgm_batch

        # Start doc_loading tasks
        for index, doc_gen in enumerate(doc_gens):
            batch_gen = BatchedDocumentGenerator(doc_gen, self.batch_size)
            task = LoadDocumentsTask(
                self.cluster, bucket, self.clients[index], batch_gen,
                "create", self.exp,
                scope=self.scope,
                collection=self.collection,
                task_identifier=self.thread_name,
                persist_to=self.persist_to,
                replicate_to=self.replicate_to,
                durability=self.durability,
                timeout_secs=self.timeout_secs,
                skip_read_on_error=self.skip_read_on_error,
                suppress_error_table=self.suppress_error_table,
                track_failures=self.track_failures,
                sdk_client_pool=self.sdk_client_pool,
                sdk_retry_strategy=self.sdk_retry_strategy)
            self.task_manager.add_new_task(task)
            doc_tasks.append(task)

        # Wait for doc_loading tasks to complete
        for task in doc_tasks:
            self.task_manager.get_task_result(task)

    def _load_bucket_into_dgm(self, bucket):
        """
        Load bucket into dgm until either active_rr or replica_rr
        goes below self.active_resident_threshold
        """
        active_dgm_value, replica_dgm_value = self._get_bucket_dgm(bucket)
        self.test_log.info("DGM doc loading for '%s' to atleast %s%%"
                           % (bucket.name, self.active_resident_threshold))
        while active_dgm_value > self.active_resident_threshold and \
                replica_dgm_value > self.active_resident_threshold:
            self.test_log.info("Active_resident_items_ratio for {0} is {1}"
                               .format(bucket.name, active_dgm_value))
            self.test_log.info("Replica_resident_items_ratio for {0} is {1}"
                               .format(bucket.name, replica_dgm_value))
            self._load_next_batch_of_docs(bucket)
            active_dgm_value, replica_dgm_value = self._get_bucket_dgm(bucket)
        self.test_log.info(
            "Active DGM %s%% Replica DGM %s%% achieved for '%s'. Loaded docs: %s"
            % (active_dgm_value, replica_dgm_value, bucket.name,
               self.docs_loaded_per_bucket[bucket]))

    def call(self):
        self.test_log.info("Starting DGM doc loading task")
        self.start_task()
        for bucket in self.buckets:
            self.docs_loaded_per_bucket[bucket] = 0
            self._load_bucket_into_dgm(bucket)
            collection = bucket.scopes[self.scope].collections[self.collection]
            collection.num_items += self.docs_loaded_per_bucket[bucket]
            collection.doc_index = (collection.doc_index[0],
                                    collection.doc_index[1] +
                                    self.docs_loaded_per_bucket[bucket])

        # Close all SDK clients
        if self.sdk_client_pool is None:
            for client in self.clients:
                client.close()

        self.complete_task()
        self.test_log.info("Done loading docs for DGM")


class ValidateDocumentsTask(GenericLoadingTask):
    def __init__(self, cluster, bucket, client, generator, op_type, exp,
                 flag=0, proxy_client=None, batch_size=1,
                 timeout_secs=30, time_unit="seconds",
                 compression=None, check_replica=False,
                 sdk_client_pool=None,
                 sdk_retry_strategy=None,
                 scope=CbServer.default_scope,
                 collection=CbServer.default_collection,
                 is_sub_doc=False,
                 suppress_error_table=False):
        super(ValidateDocumentsTask, self).__init__(
            cluster, bucket, client, batch_size=batch_size,
            timeout_secs=timeout_secs,
            time_unit=time_unit,
            compression=compression,
            sdk_client_pool=sdk_client_pool,
            sdk_retry_strategy=sdk_retry_strategy,
            scope=scope, collection=collection)
        self.thread_name = "ValidateDocumentsTask-%s_%s_%s_%s_%s_%s_%s" % (
            bucket.name,
            self.scope,
            self.collection,
            generator._doc_gen.start,
            generator._doc_gen.end,
            op_type, time.time())

        self.generator = generator
        self.skip_doc_gen_value = False
        self.op_type = op_type
        if self.op_type in [DocLoading.Bucket.DocOps.DELETE]:
            self.skip_doc_gen_value = True
        self.exp = exp
        self.flag = flag
        self.suppress_error_table = suppress_error_table
        if not self.suppress_error_table:
            self.failed_item_table = TableView(self.test_log.info)
            self.failed_item_table.set_headers(["READ Key", "Exception"])
        self.missing_keys = []
        self.wrong_values = []
        self.failed_reads = dict()
        if proxy_client:
            self.log.debug("Changing client to proxy %s:%s..."
                           % (proxy_client.host, proxy_client.port))
            self.client = proxy_client
        self.check_replica = check_replica
        self.replicas = bucket.replicaNumber
        self.client = client
        self.is_sub_doc = is_sub_doc

    def has_next(self):
        return self.generator.has_next()

    def __validate_sub_docs(self, doc_gen):
        if self.sdk_client_pool is not None:
            self.client = \
                self.sdk_client_pool.get_client_for_bucket(self.bucket,
                                                           self.scope,
                                                           self.collection)
        key_value = dict(doc_gen.next_batch(self.skip_doc_gen_value))
        self.test_log.info(key_value)
        result_map, self.failed_reads = self.batch_read(key_value.keys())
        self.test_log.info(result_map)
        self.test_log.info(self.failed_reads)

        if self.sdk_client_pool:
            self.sdk_client_pool.release_client(self.client)
            self.client = None
        return

        op_failed_tbl = TableView(self.log.error)
        op_failed_tbl.set_headers(["Update Key",
                                   "Value"])
        for key, value in task.success.items():
            doc_value = value["value"]
            failed_row = [key, doc_value]
            if doc_value[0] != 0:
                op_failed_tbl.add_row(failed_row)
            elif doc_value[1] != "LastNameUpdate":
                op_failed_tbl.add_row(failed_row)
            elif doc_value[2] != "TypeChange":
                op_failed_tbl.add_row(failed_row)
            elif doc_value[3] != "CityUpdate":
                op_failed_tbl.add_row(failed_row)
            elif Json.loads(str(doc_value[4])) \
                    != ["get", "up"]:
                op_failed_tbl.add_row(failed_row)

        op_failed_tbl.display("Keys failed in %s:%s:%s"
                              % (self.bucket.name,
                                 self.scope,
                                 self.collection))
        if len(op_failed_tbl.rows) != 0:
            self.fail("Update failed for few keys")

        op_failed_tbl = TableView(self.log.error)
        op_failed_tbl.set_headers(["Delete Key",
                                   "Value"])

        for key, value in task.success.items():
            doc_value = value["value"]
            failed_row = [key, doc_value]
        if doc_value[0] != 2:
            op_failed_tbl.add_row(failed_row)
        for index in range(1, len(doc_value)):
            if doc_value[index] != "PATH_NOT_FOUND":
                op_failed_tbl.add_row(failed_row)

        for key, value in task.fail.items():
            op_failed_tbl.add_row([key, value["value"]])

        op_failed_tbl.display("Keys failed in %s:%s:%s"
                              % (self.bucket.name,
                                 self.scope,
                                 self.collection))
        if len(op_failed_tbl.rows) != 0:
            self.fail("Delete failed for few keys")

    def __validate_docs(self, doc_gen):
        if self.sdk_client_pool is not None:
            self.client = \
                self.sdk_client_pool.get_client_for_bucket(self.bucket,
                                                           self.scope,
                                                           self.collection)
        key_value = dict(doc_gen.next_batch(self.skip_doc_gen_value))
        if self.check_replica:
            # change to getFromReplica
            result_map = dict()
            self.failed_reads = dict()
            for key in key_value.keys():
                try:
                    result = self.client.get_from_all_replicas(key)
                    if all(_result for _result in result) \
                            and len(result) == min(self.replicas + 1,
                                                   len(
                                                       self.cluster.nodes_in_cluster)):
                        key = key.decode()
                        if result[0]["status"]:
                            result_map[key] = dict()
                            result_map[key]["value"] = result[0]["value"]
                            result_map[key]["cas"] = result[0]["cas"]
                    elif result:
                        self.failed_reads[key] = dict()
                        self.failed_reads[key]["cas"] = result[0]["cas"]
                        self.failed_reads[key]["error"] = result
                        self.failed_reads[key]["value"] = dict()
                    else:
                        self.failed_reads[key] = dict()
                        self.failed_reads[key]["cas"] = 0
                        self.failed_reads[key]["error"] = \
                            SDKException.DocumentNotFoundException
                        self.failed_reads[key]["value"] = dict()
                except Exception as error:
                    self.exception = error
                    return
        else:
            result_map, self.failed_reads = self.batch_read(key_value.keys())
        if self.sdk_client_pool:
            self.sdk_client_pool.release_client(self.client)
            self.client = None

        if not self.suppress_error_table:
            for key, value in self.failed_reads.items():
                if SDKException.DocumentNotFoundException \
                        not in str(self.failed_reads[key]["error"]):
                    self.failed_item_table.add_row([key, value['error']])
        missing_keys, wrong_values = self.validate_key_val(result_map,
                                                           key_value)
        if self.op_type == 'delete':
            not_missing = []
            if missing_keys.__len__() != key_value.keys().__len__():
                for key in key_value.keys():
                    if key not in missing_keys:
                        not_missing.append(key)
                if not_missing:
                    self.exception = Exception("Keys were not deleted. "
                                               "Keys not deleted: {}"
                                               .format(','.join(not_missing)))
        else:
            if missing_keys:
                self.exception = Exception("Total %d keys missing: %s"
                                           % (missing_keys.__len__(),
                                              missing_keys))
                self.missing_keys.extend(missing_keys)
            if wrong_values:
                self.exception = Exception("Total %s wrong key-values :: %s"
                                           % (wrong_values.__len__(),
                                              wrong_values))
                self.wrong_values.extend(wrong_values)
        if self.exception:
            raise(self.exception)

    def next(self, override_generator=None):
        doc_gen = override_generator or self.generator
        if self.is_sub_doc:
            self.__validate_sub_docs(doc_gen)
        else:
            self.__validate_docs(doc_gen)

    def validate_key_val(self, map, key_value):
        missing_keys = []
        wrong_values = []
        for key, value in key_value.items():
            if key in map:
                if type(value) == JsonObject:
                    expected_val = Json.loads(value.toString())
                else:
                    expected_val = Json.loads(value)
                if map[key]['cas'] != 0:
                    actual_val = Json.loads(map[key][
                                                'value'].toString())
                elif map[key]['error'] is not None:
                    actual_val = map[key]['error'].toString()
                else:
                    missing_keys.append(key)
                    continue
                actual_val["mutated"] = int(actual_val["mutated"])
                expected_val["mutated"] = int(expected_val["mutated"])
                if expected_val == actual_val:
                    continue
                else:
                    wrong_value = "Key: {} Expected: {} Actual: {}" \
                        .format(key, expected_val, actual_val)
                    wrong_values.append(wrong_value)
            elif SDKException.DocumentNotFoundException \
                    in str(self.failed_reads[key]["error"]):
                missing_keys.append(key)
        return missing_keys, wrong_values


class DocumentsValidatorTask(Task):
    def __init__(self, cluster, task_manager, bucket, clients, generators,
                 op_type, exp, flag=0, batch_size=1,
                 timeout_secs=60, time_unit="seconds",
                 compression=None,
                 process_concurrency=4, check_replica=False,
                 scope=CbServer.default_scope,
                 collection=CbServer.default_collection,
                 sdk_client_pool=None,
                 sdk_retry_strategy=None,
                 is_sub_doc=False,
                 suppress_error_table=False):
        super(DocumentsValidatorTask, self).__init__(
            "DocumentsValidatorTask_%s_%s_%s" % (
                bucket.name, op_type, time.time()))
        self.cluster = cluster
        self.exp = exp
        self.flag = flag
        self.timeout_secs = timeout_secs
        self.time_unit = time_unit
        self.compression = compression
        self.process_concurrency = process_concurrency
        self.clients = clients
        self.sdk_client_pool = sdk_client_pool
        self.sdk_retry_strategy = sdk_retry_strategy
        self.task_manager = task_manager
        self.batch_size = batch_size
        self.generators = generators
        self.input_generators = generators
        self.op_types = None
        self.buckets = None
        self.suppress_error_table = suppress_error_table
        if isinstance(op_type, list):
            self.op_types = op_type
        else:
            self.op_type = op_type
        if isinstance(bucket, list):
            self.buckets = bucket
        else:
            self.bucket = bucket
        self.scope = scope
        self.collection = collection
        self.check_replica = check_replica
        self.is_sub_doc = is_sub_doc

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
        exception = None
        for generator in self.generators:
            if self.op_types:
                self.op_type = self.op_types[iterator]
            if self.buckets:
                self.bucket = self.buckets[iterator]
            tasks.extend(self.get_tasks(generator))
            iterator += 1
        try:
            for task in tasks:
                self.task_manager.add_new_task(task)
            for task in tasks:
                self.task_manager.get_task_result(task)

                if not self.suppress_error_table:
                    task.failed_item_table.display(
                        "DocValidator failure for %s:%s:%s"
                        % (self.bucket.name, self.scope, self.collection))
        except Exception as e:
            self.result = False
            self.log.debug("========= Tasks in loadgen pool=======")
            self.task_manager.print_tasks_in_pool()
            self.log.debug("======================================")
            for task in tasks:
                self.task_manager.stop_task(task)
                self.log.debug("Task '%s' complete." % (task.thread_name))
            self.test_log.error(e)
            if not self.sdk_client_pool:
                for client in self.clients:
                    client.close()
            self.set_exception(e)

        self.complete_task()
        if not self.sdk_client_pool:
            for client in self.clients:
                client.close()
        self.complete_task()
        if exception:
            self.set_exception(exception)

    def get_tasks(self, generator):
        generators = []
        tasks = []
        gen_start = int(generator.start)
        gen_end = int(generator.end)
        gen_range = max(
            int((generator.end - generator.start) / self.process_concurrency),
            1)
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
            task = ValidateDocumentsTask(
                self.cluster, self.bucket, self.clients[i], generators[i],
                self.op_type, self.exp, self.flag, batch_size=self.batch_size,
                timeout_secs=self.timeout_secs,
                time_unit=self.time_unit,
                compression=self.compression, check_replica=self.check_replica,
                scope=self.scope, collection=self.collection,
                sdk_client_pool=self.sdk_client_pool,
                sdk_retry_strategy=self.sdk_retry_strategy,
                is_sub_doc=self.is_sub_doc,
                suppress_error_table=self.suppress_error_table)
            tasks.append(task)
        return tasks


class StatsWaitTask(Task):
    EQUAL = '=='
    NOT_EQUAL = '!='
    LESS_THAN = '<'
    LESS_THAN_EQ = '<='
    GREATER_THAN = '>'
    GREATER_THAN_EQ = '>='

    def __init__(self, servers, bucket, stat_cmd, stat, comparison,
                 value, timeout=300):
        super(StatsWaitTask, self).__init__("StatsWaitTask_%s_%s_%s"
                                            % (bucket.name,
                                               stat,
                                               str(time.time())))
        self.servers = servers
        self.bucket = bucket
        self.statCmd = stat_cmd
        self.stat = stat
        self.comparison = comparison
        self.value = value
        self.stop = False
        self.timeout = timeout
        self.cbstatObjList = list()

    def call(self):
        self.start_task()
        start_time = time.time()
        timeout = start_time + self.timeout
        for server in self.servers:
            self.cbstatObjList.append(Cbstats(server))
        try:
            while not self.stop and time.time() < timeout:
                if self.statCmd in ["all", "dcp"]:
                    self._get_all_stats_and_compare()
                elif self.statCmd == "checkpoint":
                    if self.bucket.bucketType != Bucket.Type.MEMBASE:
                        self.stop = True
                        break
                    self._get_checkpoint_stats_and_compare()
                else:
                    raise Exception("Not supported. Implement the stat call")
        finally:
            pass
        if time.time() > timeout:
            self.set_exception("Could not verify stat {} within timeout {}"
                               .format(self.stat, self.timeout))

        self.complete_task()

    def _get_all_stats_and_compare(self):
        stat_result = 0
        val_dict = dict()
        retry = 10
        while retry > 0:
            try:
                for cb_stat_obj in self.cbstatObjList:
                    tem_stat = cb_stat_obj.all_stats(self.bucket.name,
                                                     stat_name=self.statCmd)
                    val_dict[cb_stat_obj.server.ip] = tem_stat[self.stat]
                    if self.stat in tem_stat:
                        stat_result += int(tem_stat[self.stat])
                break
            except Exception as error:
                if retry > 0:
                    retry -= 1
                    sleep(5, "MC is down. Retrying.. %s" % str(error))
                    continue
                self.set_exception(error)
                self.stop = True

        if not self._compare(self.comparison, str(stat_result), self.value):
            self.log.debug("Not Ready: %s %s %s %s. "
                           "Received: %s for bucket '%s'"
                           % (self.stat, stat_result, self.comparison,
                              self.value, val_dict, self.bucket.name))
            self.log.debug("Wait before next StatWaitTask check")
            sleep(5, log_type="infra")
        else:
            self.test_log.debug("Ready: %s %s %s %s. "
                                "Received: %s for bucket '%s'"
                                % (self.stat, stat_result, self.comparison,
                                   self.value, val_dict, self.bucket.name))
            self.stop = True

    def _get_checkpoint_stats_and_compare(self):
        stat_result = 0
        val_dict = dict()
        retry = 5
        while retry > 0:
            try:
                for cb_stat_obj in self.cbstatObjList:
                    tem_stat = cb_stat_obj.checkpoint_stats(self.bucket.name)
                    node_stat_val = 0
                    for vb in tem_stat:
                        node_stat_val += tem_stat[vb][self.stat]
                    val_dict[cb_stat_obj.server.ip] = node_stat_val
                    stat_result += node_stat_val
                break
            except Exception as error:
                if retry > 0:
                    retry -= 1
                    sleep(5, "MC is down. Retrying.. %s" % str(error))
                    continue
                self.set_exception(error)
                self.stop = True
        if not self._compare(self.comparison, str(stat_result), self.value):
            self.test_log.debug("Not Ready: %s %s %s %s. "
                                "Received: %s for bucket '%s'"
                                % (self.stat, stat_result, self.comparison,
                                   self.value, val_dict, self.bucket.name))
            self.log.debug("Wait before next StatWaitTask check")
            sleep(5, log_type="infra")
        else:
            self.test_log.debug("Ready: %s %s %s %s. "
                                "Received: %s for bucket '%s'"
                                % (self.stat, stat_result, self.comparison,
                                   self.value, val_dict, self.bucket.name))
            self.stop = True

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
        super(ViewCreateTask, self).__init__("ViewCreateTask_%s_%s_%s"
                                             % (bucket, view, time.time()))
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
            self.log.debug("Waiting for potential concurrent ddoc updates to complete")
            sleep(2)
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
                                Exception(
                                    "design doc {0} doesn't contain spatial view {1}"
                                        .format(self.design_doc_name,
                                                self.view.name)))
                            return 0
                    else:
                        if self.view.name not in json_parsed["views"].keys():
                            self.set_exception(Exception(
                                "design doc {0} doesn't contain view {1}"
                                    .format(self.design_doc_name,
                                            self.view.name)))
                            return 0
                self.test_log.debug(
                    "View: {0} was created successfully in ddoc: {1}"
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
            if e.message.find('not_found') or e.message.find(
                    'view_undefined') > -1:
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
                        self.test_log.debug(
                            "Design Doc {0} version is not updated on node {1}:{2}. Retrying."
                                .format(self.design_doc_name,
                                        node.ip, node.port))
                        sleep(2)
                except ReadDocumentException as e:
                    if count < retry_count:
                        self.test_log.debug(
                            "Design Doc {0} not yet available on node {1}:{2}. Retrying."
                                .format(self.design_doc_name,
                                        node.ip, node.port))
                        sleep(2)
                    else:
                        self.test_log.error(
                            "Design Doc {0} failed to replicate on node {1}:{2}"
                                .format(self.design_doc_name, node.ip,
                                        node.port))
                        self.set_exception(e)
                        break
                except Exception as e:
                    if count < retry_count:
                        self.test_log.error("Unexpected exception: %s. "
                                            "Will retry after sleep.." % e)
                        sleep(2)
                    else:
                        self.set_exception(e)
                        break
            else:
                self.set_exception(Exception(
                    "Design Doc {0} version mismatch on node {1}:{2}"
                        .format(self.design_doc_name, node.ip, node.port)))


class ViewDeleteTask(Task):
    def __init__(self, server, design_doc_name, view, bucket="default"):
        Task.__init__(self, "Delete_view_task_%s_%s_%s"
                      % (bucket, view, time.time()))
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
                self.log.debug("Waiting for potential concurrent ddoc updates to complete")
                sleep(2)
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
            self.test_log.debug(
                "View: {0} was successfully deleted in ddoc: {1}"
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
        Task.__init__(self, "Query_view_task_%s_%s_%s"
                      % (bucket, design_doc_name, view_name))
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
                self.test_log.debug("Initial query failed. "
                                    "Will retry after sleep..")
                sleep(self.retry_time)
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

            self.test_log.debug(
                "Server: %s, Design Doc: %s, View: %s, (%d rows) expected, (%d rows) returned"
                % (self.server.ip, self.design_doc_name,
                   self.view_name, self.expected_rows,
                   len(content['rows'])))

            raised_error = content.get(u'error', '') or \
                           ''.join([str(item) for item in
                                    content.get(u'errors', [])])
            if raised_error:
                raise QueryViewException(self.view_name, raised_error)

            if len(content['rows']) == self.expected_rows:
                self.test_log.debug(
                    "Expected rows: '{0}' was found for view query"
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

                self.test_log.debug("Retry until expected results "
                                    "or task times out")
                sleep(self.retry_time)
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
                 scan_consistency=None, scan_vector=None, timeout=900):
        super(N1QLQueryTask, self).__init__("query_n1ql_task_%s_%s_%s"
                                            % (bucket, query, time.time()))
        self.server = server
        self.bucket = bucket
        self.query = query
        self.expected_result = expected_result
        self.n1ql_helper = n1ql_helper
        self.timeout = timeout
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
                    query=self.query, server=self.server)
                self.test_log.debug(self.actual_result)
            self.test_log.debug(" <<<<< Done Executing Query {0} >>>>>>"
                                .format(self.query))
            return_value = self.check()
            self.complete_task()
            return return_value
        except N1QLQueryException:
            self.test_log.debug("Initial query failed, will retry..")
            if self.retried < self.retry_time:
                self.retried += 1
                sleep(self.retry_time)
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
                        self.test_log.debug("Incorrect query results for %s"
                                            % self.query)
                        raise N1QLQueryException(self.msg)
                else:
                    check = self.n1ql_helper.verify_index_with_explain(
                        self.actual_result, self.index_name)
                    if not check:
                        actual_result = self.n1ql_helper.run_cbq_query(
                            query="select * from system:indexes",
                            server=self.server)
                        self.test_log.debug(actual_result)
                        raise Exception("INDEX usage in Query %s :: "
                                        "NOT FOUND %s :: "
                                        "as observed in result %s"
                                        % (self.query, self.index_name,
                                           self.actual_result))
            self.test_log.debug(" <<<<< Done VERIFYING Query {0} >>>>>>"
                                .format(self.query))
            return True
        except N1QLQueryException as e:
            # subsequent query failed! exit
            self.set_exception(e)
        # catch and set all unexpected exceptions
        except Exception as e:
            self.set_exception(e)


class RunQueriesTask(Task):
    """
        Task that runs CBAS and N1QL queries
        runs all queries given in the queries list
        Parameters:
          server - server on which n1ql query should run (TestInputServer)
          queries - List of either cbas or n1ql queries (list)
          task_manager - task manager object to run parallel tasks (TaskManager)
          helper - helper object either cbas or n1ql(CbasUtilV2, CbasUtil,
          N1QLHelper)
          query_type - type of query legal values are cbas, n1ql (string)
          is_prepared - Prepares the queries in the list if false (string)
                -- Unprepared query eg:
                select count(*) from {0} where mutated > 0;
    """

    def __init__(self, cluster, queries, task_manager, helper, query_type,
                 run_infinitely=False, parallelism=1, is_prepared=True,
                 record_results=True):
        super(RunQueriesTask, self).__init__("RunQueriesTask_started_%s"
                                             % (time.time()))
        self.cluster = cluster
        self.queries = queries
        self.query_type = query_type
        if query_type == "n1ql":
            self.n1ql_helper = helper
        if query_type == "cbas":
            self.cbas_util = helper
        self.task_manager = task_manager
        self.run_infinitely = run_infinitely
        self.parallelism = parallelism
        self.query_tasks = []
        self.result = []
        self.is_prepared = is_prepared
        self.debug_msg = self.query_type + "-DEBUG-"
        self.record_results = record_results

    def call(self):
        start = 0
        end = self.parallelism
        self.start_task()
        try:
            if not self.is_prepared:
                if self.query_type == "cbas":
                    self.prepare_cbas_queries()
                elif self.query_type == "n1ql":
                    self.prepare_n1ql_queries()
            while True:
                for query in self.queries[start:end]:
                    if hasattr(self, "n1ql_helper"):
                        query_task = N1QLQueryTask(
                            self.cluster.master, "", query,
                            n1ql_helper=self.n1ql_helper,
                            verify_results=False, is_explain_query=True)
                        self.task_manager.add_new_task(query_task)
                        self.query_tasks.append(query_task)
                    if hasattr(self, "cbas_util"):
                        query_task = CBASQueryExecuteTask(
                            self.cluster, self.cbas_util, None, query)
                        self.task_manager.add_new_task(query_task)
                        self.query_tasks.append(query_task)
                for query_task in self.query_tasks:
                    self.task_manager.get_task_result(query_task)
                    self.log.info(self.debug_msg + "ActualResult: " + str(
                        query_task.actual_result))
                    if self.record_results:
                        self.result.append(query_task.actual_result)
                self.query_tasks = []
                start = end
                end += self.parallelism
                if start >= len(self.queries):
                    if self.run_infinitely:
                        start = 0
                        end = self.parallelism
                    else:
                        break
        except Exception as e:
            self.test_log.error(e)
            self.set_exception(e)
            return
        self.complete_task()

    def prepare_cbas_queries(self):
        datasets = self.cbas_util.get_datasets(self.cluster, retries=20)
        if not datasets:
            self.set_exception(Exception("Datasets not available"))
        prepared_queries = []
        for query in self.queries:
            for dataset in datasets:
                prepared_queries.append(query.format(CBASHelper.format_name(
                    dataset)))
        self.queries = prepared_queries
        self.log.info(self.debug_msg + str(self.queries))

    def prepare_n1ql_queries(self):
        buckets = self.n1ql_helper.buckets
        prepared_queries = []
        bucket_helper = BucketHelper(self.cluster.master)
        for bucket in buckets:
            status, content = bucket_helper.list_collections(
                bucket.name)
            if status:
                content = json.loads(content)
                for scope in content["scopes"]:
                    for collection in scope["collections"]:
                        keyspace = CBASHelper.format_name(bucket.name,
                                                          scope["name"],
                                                          collection["name"])
                        prepared_queries.extend([query.format(keyspace) for
                                                 query in self.queries])
        self.queries = prepared_queries


class N1QLTxnQueryTask(Task):
    def __init__(self, stmts, n1ql_helper,
                 commit=True,
                 scan_consistency='REQUEST_PLUS'):
        super(N1QLTxnQueryTask, self).__init__("query_n1ql_task_%s"
                                               % (time.time()))
        self.stmt = stmts
        self.scan_consistency = scan_consistency
        self.commit = commit
        self.n1ql_helper = n1ql_helper

    def call(self):
        self.start_task()
        try:
            # Query and get results
            self.test_log.info(" <<<<< START Executing N1ql Transaction >>>>>>")
            sleep(5)
            self.query_params = self.n1ql_helper.create_txn()
            for query in self.stmt:
                result = self.n1ql_helper.run_cbq_query(query,
                                                        query_params=self.query_params)
                sleep(2)
            self.n1ql_helper.end_txn(self.query_params, self.commit)
            self.test_log.debug(" <<<<< Done Executing N1ql Transaction >>>>>>")
            self.test_log.info("Expected Query to fail but passed")
        # catch and set all unexpected exceptions
        except Exception:
            self.test_log.info(" <<<<< Query Failed as Expected >>>>>>")


class CreateIndexTask(Task):
    def __init__(self, server, bucket, index_name, query, n1ql_helper=None,
                 retry_time=2, defer_build=False, timeout=240):
        super(CreateIndexTask, self).__init__("Task_create_index_%s_%s"
                                              % (bucket, index_name))
        self.server = server
        self.bucket = bucket
        self.defer_build = defer_build
        self.query = query
        self.index_name = index_name
        self.n1ql_helper = n1ql_helper
        self.retry_time = retry_time
        self.retried = 0
        self.timeout = timeout

    def call(self):
        self.start_task()
        try:
            # Query and get results
            self.n1ql_helper.run_cbq_query(query=self.query, server=self.server)
            self.result = self.check()
            self.complete_task()
            return self.result
        except CreateIndexException as e:
            self.test_log.debug("Initial query failed. Will retry..")
            if self.retried < self.retry_time:
                self.retried += 1
                sleep(self.retry_time)
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
            return check
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
        super(BuildIndexTask, self).__init__("Task_Build_index_%s_%s"
                                             % (bucket, query))
        self.server = server
        self.bucket = bucket
        self.query = query
        self.n1ql_helper = n1ql_helper
        self.retry_time = retry_time
        self.retried = 0

    def call(self):
        self.start_task()
        try:
            # Query and get results
            self.n1ql_helper.run_cbq_query(query=self.query,
                                           server=self.server)
            self.result = self.check()
            self.complete_task()
            return self.result
        except CreateIndexException as e:
            self.test_log.debug("Initial query failed, will retry..")
            if self.retried < self.retry_time:
                self.retried += 1
                sleep(self.retry_time)
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
        super(MonitorIndexTask, self).__init__("build_index_task_%s_%s"
                                               % (bucket, index_name))
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
                raise DropIndexException(
                    "index {0} does not exist will not drop"
                        .format(self.index_name))
            self.n1ql_helper.run_cbq_query(query=self.query, server=self.server)
            return_value = self.check()
        except N1QLQueryException as e:
            self.test_log.debug("Initial query failed, will retry..")
            if self.retried < self.retry_time:
                self.retried += 1
                sleep(self.retry_timlib / membase / api / rest_client.pye)
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


class PrintBucketStats(Task):
    def __init__(self, cluster, bucket, monitor_stats=list(), sleep=1):
        super(PrintBucketStats, self).__init__("PrintBucketStats_%s_%s"
                                               % (bucket.name, time.time()))
        self.cluster = cluster
        self.bucket = bucket
        self.bucket_helper = BucketHelper(self.cluster.master)
        self.sleep = sleep
        self.monitor_stats = monitor_stats
        self.stop_task = False

        # To avoid running a dummy task
        if len(monitor_stats) == 0:
            self.stop_task = True

        # List of stats to track / plot
        self.ops_rate_trend = list()
        self.drain_rate_trend = list()

    def record_bucket_ops(self, bucket_stats, ops_rate):
        if 'op' in bucket_stats and \
                'samples' in bucket_stats['op'] and \
                'ops' in bucket_stats['op']['samples']:
            ops = bucket_stats['op']['samples']['ops'][-1]
            self.test_log.debug("Ops rate for '%s': %f"
                                % (self.bucket.name, ops))
            if ops_rate and ops_rate[-1] > ops:
                self.ops_rate_trend.append(ops_rate)
                ops_rate = list()
            ops_rate.append(ops)
        return ops_rate

    def print_ep_queue_size(self, bucket_stats):
        if 'op' in bucket_stats \
                and 'samples' in bucket_stats['op'] \
                and 'ep_queue_size' in bucket_stats['op']['samples']:
            ep_q_size = bucket_stats['op']['samples']['ep_queue_size'][-1]
            self.test_log.debug("ep_queue_size for {}: {}\
            ".format(self.bucket.name, ep_q_size))

    def plot_all_graphs(self):
        plot_graph(self.test_log, self.bucket.name, self.ops_rate_trend)

    def end_task(self):
        self.stop_task = True

    def call(self):
        self.start_task()
        ops_rate = list()
        while not self.stop_task:
            try:
                bucket_stats = \
                    self.bucket_helper.fetch_bucket_stats(self.bucket.name)
            except Exception as e:
                self.log.warning("Exception while fetching bucket stats: %s"
                                 % e)
                sleep(2, message="Updating BucketHelper with new master",
                      log_type="infra")
                self.bucket_helper = BucketHelper(self.cluster.master)
                continue

            if "doc_ops" in self.monitor_stats:
                ops_rate = self.record_bucket_ops(bucket_stats, ops_rate)
            elif "drain_rate" in self.monitor_stats:
                self.record_drain_rate(bucket_stats)
            if "ep_queue_size" in self.monitor_stats:
                self.print_ep_queue_size(bucket_stats)

            # Sleep before fetching next stats
            sleep(self.sleep, log_type="infra")

        if ops_rate:
            self.ops_rate_trend.append(ops_rate)

        self.plot_all_graphs()
        self.complete_task()


class BucketCreateTask(Task):
    def __init__(self, server, bucket):
        super(BucketCreateTask, self).__init__("bucket_%s_create_task"
                                               % bucket.name)
        self.server = server
        self.bucket = bucket
        self.bucket_priority = 8
        if self.bucket.priority is None \
                or self.bucket.priority == Bucket.Priority.LOW:
            self.bucket_priority = 3
        self.retries = 0

    def call(self):
        if self.bucket_priority is not None:
            self.bucket.threadsNumber = self.bucket_priority

        try:
            self.result = BucketHelper(self.server)\
                .create_bucket(self.bucket.__dict__)
            if self.result is False:
                self.test_log.critical("Bucket %s creation failed"
                                       % self.bucket.name)
            elif not self.bucket.num_vbuckets:
                # Set num_vbuckets to default it not provided by the user
                self.bucket.num_vbuckets = CbServer.total_vbuckets
                if CbServer.cluster_profile == "serverless":
                    self.bucket.num_vbuckets = CbServer.Serverless.VB_COUNT
        # catch and set all unexpected exceptions
        except Exception as e:
            self.result = False
            self.test_log.error(str(e))
        self.complete_task()

    def check(self):
        try:
            if MemcachedHelper.wait_for_memcached(self.server,
                                                  self.bucket.name):
                self.test_log.debug(
                    "Bucket '{0}' created with per node RAM quota: {1}"
                    .format(self.bucket, self.bucket.ramQuotaMB))
                return True
            else:
                self.test_log.error("Vbucket map not ready after try %s"
                                    % self.retries)
                if self.retries >= 5:
                    return False
        except Exception as e:
            self.test_log.warn(
                "Exception: {0}. vbucket map not ready after try {1}"
                .format(e, self.retries))
            if self.retries >= 5:
                self.result = False
                self.test_log.error(str(e))
        self.retries = self.retris + 1
        sleep(5, "Wait for vBucket map to be ready", log_type="infra")
        self.check()


class BucketCreateFromSpecTask(Task):
    def __init__(self, task_manager, kv_nodes, bucket_name, bucket_spec):
        super(BucketCreateFromSpecTask, self) \
            .__init__("Bucket_create_task_%s" % bucket_name)
        self.task_manager = task_manager
        self.servers = kv_nodes
        self.bucket_spec = bucket_spec
        self.bucket_spec["name"] = bucket_name
        self.retries = 0
        self.rest = RestConnection(self.servers[0])
        self.bucket_helper = BucketHelper(self.servers[0])
        # Used to store the Created Bucket() object, for appending into
        # bucket_utils.buckets list
        self.bucket_obj = Bucket()

    def call(self):
        self.result = True
        self.start_task()
        bucket_params = Bucket.get_params()
        for key, value in self.bucket_spec.items():
            if key in bucket_params:
                setattr(self.bucket_obj, key, value)

        self.create_bucket()
        if CbServer.default_collection not in \
                self.bucket_spec[
                    "scopes"][CbServer.default_scope][
                    "collections"].keys():
            self.bucket_helper.delete_collection(self.bucket_spec["name"],
                                                 CbServer.default_scope,
                                                 CbServer.default_collection)
            self.bucket_obj \
                .scopes[CbServer.default_scope] \
                .collections \
                .pop(CbServer.default_collection, None)

        if self.bucket_spec[
                MetaConstants.CREATE_COLLECTIONS_USING_MANIFEST_IMPORT]:
            self.create_collections_using_manifest_import()
            for scope_name, scope_spec in self.bucket_spec["scopes"].items():
                scope_spec["name"] = scope_name
                for collection_name, collection_spec \
                        in scope_spec["collections"].items():
                    if collection_name == CbServer.default_collection:
                        continue
                    collection_spec["name"] = collection_name
        else:
            for scope_name, scope_spec in self.bucket_spec["scopes"].items():
                scope_spec["name"] = scope_name
                scope_create_thread = threading.Thread(
                    target=self.create_scope_from_spec,
                    args=[scope_spec])
                scope_create_thread.start()
                scope_create_thread.join()

        self.complete_task()

    def create_collections_using_manifest_import(self):
        json_content = dict()
        json_content["scopes"] = list()
        for s_name, s_dict in self.bucket_spec["scopes"].items():
            scope = dict()
            scope["name"] = s_name
            scope["collections"] = list()
            for c_name, c_dict in s_dict["collections"].items():
                col = dict()
                col["name"] = c_name
                if "maxTTL" in c_dict:
                    col["maxTTL"] = c_dict["maxTTL"]
                scope["collections"].append(col)
            json_content["scopes"].append(scope)

        self.bucket_helper.import_collection_using_manifest(
            self.bucket_spec["name"], str(json_content).replace("'", '"'))

    def create_bucket(self):
        self.bucket_obj.threadsNumber = 3
        if str(self.bucket_obj.priority) == "high" \
                or str(self.bucket_obj.priority) == str(Bucket.Priority.HIGH):
            self.bucket_obj.threadsNumber = 8

        try:
            self.result = \
                self.bucket_helper.create_bucket(self.bucket_obj.__dict__)
            if self.result is False:
                self.test_log.critical("Bucket %s creation failed"
                                       % self.bucket_obj.name)
                self.set_exception(
                    BucketCreationException(ip=self.bucket_helper.ip,
                                            bucket_name=self.bucket_obj.name))
            elif not self.bucket_obj.num_vbuckets:
                # Set num_vbuckets to default it not provided by the user
                self.bucket_obj.num_vbuckets = CbServer.total_vbuckets
                if CbServer.cluster_profile == "serverless":
                    self.bucket_obj.num_vbuckets = CbServer.Serverless.VB_COUNT
        # catch and set all unexpected exceptions
        except Exception as e:
            self.result = False
            self.test_log.error(str(e))
            self.set_exception(e)

    def create_scope_from_spec(self, scope_spec):
        self.test_log.debug("Creating scope for '%s' - %s"
                            % (self.bucket_obj.name, scope_spec["name"]))
        if scope_spec["name"] != CbServer.default_scope:
            status, content = self.bucket_helper.create_scope(
                self.bucket_obj.name,
                scope_spec["name"])
            if status is False:
                self.set_exception("Create scope failed for %s:%s, "
                                   "Reason - %s"
                                   % (self.bucket_obj.name,
                                      scope_spec["name"],
                                      content))
                self.result = False
                return
            self.bucket_obj.stats.increment_manifest_uid()

        for collection_name, collection_spec \
                in scope_spec["collections"].items():
            if collection_name == CbServer.default_collection:
                continue

            collection_spec["name"] = collection_name
            collection_create_thread = threading.Thread(
                target=self.create_collection_from_spec,
                args=[scope_spec["name"], collection_spec])
            collection_create_thread.start()
            collection_create_thread.join(30)

    def create_collection_from_spec(self, scope_name, collection_spec):
        self.test_log.debug("Creating collection for '%s:%s' - %s"
                            % (self.bucket_obj.name, scope_name,
                               collection_spec["name"]))
        status, content = self.bucket_helper.create_collection(
            self.bucket_obj.name,
            scope_name,
            collection_spec)
        if status is False:
            self.result = False
            self.set_exception("Create collection failed for "
                               "%s:%s:%s, Reason - %s"
                               % (self.bucket_obj.name,
                                  scope_name,
                                  collection_spec["name"],
                                  content))
        self.bucket_obj.stats.increment_manifest_uid()


class MutateDocsFromSpecTask(Task):
    def __init__(self, cluster, task_manager, loader_spec,
                 sdk_client_pool,
                 batch_size=500,
                 process_concurrency=1,
                 print_ops_rate=True,
                 track_failures=True):
        super(MutateDocsFromSpecTask, self).__init__(
            "MutateDocsFromSpecTask_%s" % time.time())
        self.cluster = cluster
        self.task_manager = task_manager
        self.loader_spec = loader_spec
        self.process_concurrency = process_concurrency
        self.batch_size = batch_size
        self.print_ops_rate = print_ops_rate

        self.result = True
        self.load_gen_tasks = list()
        self.load_subdoc_gen_tasks = list()
        self.print_ops_rate_tasks = list()

        self.sdk_client_pool = sdk_client_pool
        self.track_failures = track_failures

    def call(self):
        self.start_task()
        self.get_tasks()
        self.execute_tasks(execute_tasks=self.load_gen_tasks)
        self.execute_tasks(execute_tasks=self.load_subdoc_gen_tasks)
        self.complete_task()
        return self.result

    def execute_tasks(self, execute_tasks):
        if self.print_ops_rate:
            for bucket in self.loader_spec.keys():
                bucket.stats.manage_task(
                    "start", self.task_manager,
                    cluster=self.cluster,
                    bucket=bucket,
                    monitor_stats=["doc_ops"],
                    sleep=1)
        try:
            for task in execute_tasks:
                self.task_manager.add_new_task(task)
            for task in execute_tasks:
                try:
                    self.task_manager.get_task_result(task)
                    self.log.debug("Items loaded in task %s are %s"
                                   % (task.thread_name, task.docs_loaded))
                    i = 0
                    while task.docs_loaded < (task.generator._doc_gen.end -
                                              task.generator._doc_gen.start) \
                            and i < 60:
                        sleep(1, "Bug in java futures task. "
                                 "Items loaded in task %s: %s"
                              % (task.thread_name, task.docs_loaded),
                              log_type="infra")
                        i += 1
                except Exception as e:
                    self.test_log.error(e)
                finally:
                    self.loader_spec[
                        task.bucket]["scopes"][
                        task.scope]["collections"][
                        task.collection][
                        task.op_type]["success"].update(task.success)
                    self.loader_spec[
                        task.bucket]["scopes"][
                        task.scope]["collections"][
                        task.collection][
                        task.op_type]["fail"].update(task.fail)
                    if task.fail.__len__() != 0:
                        target_log = self.test_log.error
                        self.result = False
                    else:
                        target_log = self.test_log.debug
                    target_log("Failed to load %d docs from %d to %d of thread_name %s"
                               % (task.fail.__len__(),
                                  task.generator._doc_gen.start,
                                  task.generator._doc_gen.end,
                                  task.thread_name))
        except Exception as e:
            self.test_log.error(e)
            self.set_exception(e)
        finally:
            if self.print_ops_rate:
                for bucket in self.loader_spec.keys():
                    bucket.stats.manage_task(
                        "stop", self.task_manager,
                        cluster=self.cluster,
                        bucket=bucket,
                        monitor_stats=["doc_ops"],
                        sleep=1)
            self.log.debug("========= Tasks in loadgen pool=======")
            self.task_manager.print_tasks_in_pool()
            self.log.debug("======================================")
            for task in execute_tasks:
                self.task_manager.stop_task(task)
                self.log.debug("Task '%s' complete. Loaded %s items"
                               % (task.thread_name, task.docs_loaded))

    def create_tasks_for_bucket(self, bucket, scope_dict):
        load_gen_for_scopes_create_threads = list()
        for scope_name, collection_dict in scope_dict.items():
            scope_thread = threading.Thread(
                target=self.create_tasks_for_scope,
                args=[bucket, scope_name, collection_dict["collections"]])
            scope_thread.start()
            load_gen_for_scopes_create_threads.append(scope_thread)
        for scope_thread in load_gen_for_scopes_create_threads:
            scope_thread.join(120)

    def create_tasks_for_scope(self, bucket, scope_name, collection_dict):
        load_gen_for_collection_create_threads = list()
        for c_name, c_data in collection_dict.items():
            collection_thread = threading.Thread(
                target=self.create_tasks_for_collections,
                args=[bucket, scope_name, c_name, c_data])
            collection_thread.start()
            load_gen_for_collection_create_threads.append(collection_thread)

        for collection_thread in load_gen_for_collection_create_threads:
            collection_thread.join(60)

    def create_tasks_for_collections(self, bucket, scope_name,
                                     col_name, col_meta):
        for op_type, op_data in col_meta.items():
            # Create success, fail dict per load_gen task
            op_data["success"] = dict()
            op_data["fail"] = dict()
            track_failures = op_data.get("track_failures",
                                         self.track_failures)

            generators = list()
            generator = op_data["doc_gen"]
            gen_start = int(generator.start)
            gen_end = int(generator.end)
            gen_range = max(int((generator.end - generator.start)
                                / self.process_concurrency),
                            1)
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
            for doc_gen in generators:
                task_id = "%s_%s_%s_%s_ttl=%s" % (self.thread_name,
                                                  bucket.name,
                                                  scope_name, col_name,
                                                  op_data["doc_ttl"])
                if op_type in DocLoading.Bucket.DOC_OPS:
                    doc_load_task = LoadDocumentsTask(
                        self.cluster, bucket, None, doc_gen,
                        op_type, op_data["doc_ttl"],
                        scope=scope_name, collection=col_name,
                        task_identifier=task_id,
                        sdk_client_pool=self.sdk_client_pool,
                        batch_size=self.batch_size,
                        durability=op_data["durability_level"],
                        timeout_secs=op_data["sdk_timeout"],
                        time_unit=op_data["sdk_timeout_unit"],
                        skip_read_on_error=op_data["skip_read_on_error"],
                        suppress_error_table=op_data["suppress_error_table"],
                        track_failures=track_failures,
                        skip_read_success_results=op_data[
                            "skip_read_success_results"])
                    self.load_gen_tasks.append(doc_load_task)
                elif op_type in DocLoading.Bucket.SUB_DOC_OPS:
                    subdoc_load_task = LoadSubDocumentsTask(
                        self.cluster, bucket, None, doc_gen,
                        op_type, op_data["doc_ttl"],
                        create_paths=True,
                        xattr=op_data["xattr_test"],
                        scope=scope_name, collection=col_name,
                        task_identifier=task_id,
                        sdk_client_pool=self.sdk_client_pool,
                        batch_size=self.batch_size,
                        durability=op_data["durability_level"],
                        timeout_secs=op_data["sdk_timeout"],
                        time_unit=op_data["sdk_timeout_unit"],
                        track_failures=track_failures,
                        skip_read_success_results=op_data[
                            "skip_read_success_results"])
                    self.load_subdoc_gen_tasks.append(subdoc_load_task)

    def get_tasks(self):
        tasks = list()
        load_gen_for_bucket_create_threads = list()

        for bucket, scope_dict in self.loader_spec.items():
            bucket_thread = threading.Thread(
                target=self.create_tasks_for_bucket,
                args=[bucket, scope_dict["scopes"]])
            bucket_thread.start()
            load_gen_for_bucket_create_threads.append(bucket_thread)

        for bucket_thread in load_gen_for_bucket_create_threads:
            bucket_thread.join(timeout=180)
        return tasks

class CompareIndexKVData(Task):
    def __init__(self, cluster, server, task_manager,
                 sdk_client_pool, query, bucket, scope, collection, index_name, offset, field='body',
                 track_failures=True):
        super(CompareIndexKVData, self).__init__(
            "CompareIndexKVData_%s_%s_%s" % (index_name, offset, time.time()))
        self.cluster = cluster
        self.task_manager = task_manager
        self.result = True
        self.sdk_client_pool = sdk_client_pool
        self.track_failures = track_failures
        self.query = query
        self.server = server
        self.bucket = bucket
        self.scope = scope
        self.collection = collection
        self.field = field

    def call(self):
        self.start_task()
        try:
            indexer_rest = GsiHelper(self.server, self.log)
            self.log.info("starting call")
            contentType = 'application/x-www-form-urlencoded'
            connection = 'close'
            status, content, header = indexer_rest.execute_query(server=self.server, query=self.query,
                                                                 contentType=contentType,
                                                                 connection=connection)
            newContent = json.loads(content)
            resultList = newContent['results']

            self.client = \
                    self.sdk_client_pool.get_client_for_bucket(self.bucket,
                                                               self.scope.name,
                                                               self.collection.name)
            keys = self.create_list(resultList, 'id')
            success, fail = self.client.get_multi(
                        keys)
            self.log.debug("field is: {}".format(self.field))
            self.set_result(self.compareResult(success, resultList, self.field))
        except Exception as e:
            self.log.error("Exception while comparing")
            self.test_log.error(e)
            self.set_exception(e)
        finally:
            self.sdk_client_pool.release_client(self.client)
            self.complete_task()


    def compareResult(self, kvList, indexList, field='body'):
        if len(kvList) != len(indexList):
            return False
        size = len(kvList)
        kvListItems = kvList.items()
        for x in range(size-1, -1, -1):
            isFound = False
            for y in range(size):
                if indexList[x]['id'] == kvListItems[y][0]:
                    isFound = True
                    if indexList[x][field] != json.loads(str(kvListItems[y][1]['value']))[field]:
                        return False
                    break
            if not isFound:
                return False
        return True


    def create_list(self, result, keyValue):
        keys = list()
        for key in result:
            keys.append(key[keyValue])
        return keys

class ValidateDocsFromSpecTask(Task):
    def __init__(self, cluster, task_manager, loader_spec,
                 sdk_client_pool, check_replica=False,
                 batch_size=500,
                 process_concurrency=1):
        super(ValidateDocsFromSpecTask, self).__init__(
            "ValidateDocsFromSpecTask_%s" % time.time())
        self.cluster = cluster
        self.task_manager = task_manager
        self.loader_spec = loader_spec
        self.process_concurrency = process_concurrency
        self.batch_size = batch_size
        self.check_replica = check_replica

        self.result = True
        self.validate_data_tasks = list()
        self.validate_data_tasks_lock = Lock()
        self.sdk_client_pool = sdk_client_pool

    def call(self):
        self.start_task()
        self.get_tasks()
        try:
            for task in self.validate_data_tasks:
                self.task_manager.add_new_task(task)
            for task in self.validate_data_tasks:
                self.task_manager.get_task_result(task)
        except Exception as e:
            self.result = False
            self.log.debug("========= Tasks in loadgen pool=======")
            self.task_manager.print_tasks_in_pool()
            self.log.debug("======================================")
            for task in self.validate_data_tasks:
                self.task_manager.stop_task(task)
                self.log.debug("Task '%s' complete. Loaded %s items"
                               % (task.thread_name, task.docs_loaded))
            self.test_log.error(e)
            self.set_exception(e)

        self.complete_task()
        return self.result

    def create_tasks_for_bucket(self, bucket, scope_dict):
        load_gen_for_scopes_create_threads = list()
        for scope_name, collection_dict in scope_dict.items():
            scope_thread = threading.Thread(
                target=self.create_tasks_for_scope,
                args=[bucket, scope_name, collection_dict["collections"]])
            scope_thread.start()
            load_gen_for_scopes_create_threads.append(scope_thread)
        for scope_thread in load_gen_for_scopes_create_threads:
            scope_thread.join(120)

    def create_tasks_for_scope(self, bucket, scope_name, collection_dict):
        load_gen_for_collection_create_threads = list()
        for c_name, c_data in collection_dict.items():
            collection_thread = threading.Thread(
                target=self.create_tasks_for_collections,
                args=[bucket, scope_name, c_name, c_data])
            collection_thread.start()
            load_gen_for_collection_create_threads.append(collection_thread)

        for collection_thread in load_gen_for_collection_create_threads:
            collection_thread.join(60)

    def create_tasks_for_collections(self, bucket, scope_name,
                                     col_name, col_meta):
        for op_type, op_data in col_meta.items():
            # Create success, fail dict per load_gen task
            op_data["success"] = dict()
            op_data["fail"] = dict()

            generators = list()
            generator = op_data["doc_gen"]
            gen_start = int(generator.start)
            gen_end = int(generator.end)
            gen_range = max(int((generator.end - generator.start)
                                / self.process_concurrency),
                            1)
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
            for doc_gen in generators:
                if op_type in DocLoading.Bucket.DOC_OPS:
                    if op_data["doc_ttl"] > 0:
                        op_type = DocLoading.Bucket.DocOps.DELETE
                    task = ValidateDocumentsTask(
                        self.cluster, bucket, None, doc_gen,
                        op_type, op_data["doc_ttl"],
                        None, batch_size=self.batch_size,
                        timeout_secs=op_data["sdk_timeout"],
                        compression=None, check_replica=self.check_replica,
                        scope=scope_name, collection=col_name,
                        sdk_client_pool=self.sdk_client_pool,
                        is_sub_doc=False,
                        suppress_error_table=op_data["suppress_error_table"])
                    self.validate_data_tasks.append(task)

    def get_tasks(self):
        tasks = list()
        bucket_validation_threads = list()

        for bucket, scope_dict in self.loader_spec.items():
            bucket_thread = threading.Thread(
                target=self.create_tasks_for_bucket,
                args=[bucket, scope_dict["scopes"]])
            bucket_thread.start()
            bucket_validation_threads.append(bucket_thread)

        for bucket_thread in bucket_validation_threads:
            bucket_thread.join(timeout=180)
        return tasks


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
        super(MonitorActiveTask, self).__init__("MonitorActiveTask_%s"
                                                % server.ip)
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
        tasks = self.rest.ns_server_tasks()
        for task in tasks:
            if task["type"] == self.type \
                    and ((self.target_key == "designDocument"
                          and task[self.target_key] == self.target_value)
                         or (self.target_key == "original_target"
                             and task[self.target_key][
                                 "type"] == self.target_value)
                         or (self.type == 'indexer')):
                self.current_progress = task["progress"]
                self.task = task
                self.test_log.debug(
                    "Monitoring active task was found:" + str(task))
                self.test_log.debug("Progress %s:%s - %s %%"
                                    % (self.type, self.target_value,
                                       task["progress"]))
                if self.current_progress >= self.wait_progress:
                    self.test_log.debug("Got expected progress: %s"
                                        % self.current_progress)
                    self.result = True
        if self.task is None:
            # task is not performed
            self.test_log.warning("Expected active task %s:%s was not found"
                                  % (self.type, self.target_value))
            self.result = True
        elif self.wait_task:
            self.test_log.debug("Polling for %s task to complete" % self.type)
            self.check()
        else:
            # task was completed
            self.test_log.debug("Task for monitoring %s:%s completed"
                                % (self.type, self.target_value))
            self.result = True

    def check(self):
        tasks = self.rest.ns_server_tasks()
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
                            sleep(2, "Wait for next progress update",
                                  log_type="infra")
                            self.current_iter += 1
                            self.check()
                        else:
                            self.test_log.error(
                                "Progress not changed for %s during %s sec"
                                % (self.type, 2 * self.num_iterations))
                            self.result = False
                            return
                    else:
                        self.test_log.error(
                            "Progress for task %s:%s changed direction!"
                            % (self.type, self.target_value))
                        self.result = False
                        return
        else:
            self.test_log.info("Task %s completed on server" % self.task)
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
        Task.__init__(self, "monitor_frag_db_task_%s_%s"
                      % (bucket_name, time.time()))
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
            except Exception as ex:
                self.set_result(False)
                self.set_exception(ex)
            self.test_log.debug("Wait for expected fragmentation level")
            sleep(2)
        self.complete_task()


class AutoFailoverNodesFailureTask(Task):
    def __init__(self, task_manager, master, servers_to_fail, failure_type,
                 timeout, pause=0, expect_auto_failover=True, timeout_buffer=3,
                 check_for_failover=True, failure_timers=None,
                 disk_timeout=0, disk_location=None, disk_size=200,
                 auto_reprovision=False):
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
                                             self.timeout_buffer + 180
        self.disk_timeout = disk_timeout
        self.disk_location = disk_location
        self.disk_size = disk_size
        if failure_timers is None:
            failure_timers = list()
        self.failure_timers = failure_timers
        self.rebalance_in_progress = False
        self.auto_reprovision = auto_reprovision

    def check_failure_timer_task_start(self, timer_task, retry_count=5):
        while retry_count != 0 and not timer_task.started:
            sleep(1, "Wait for failover timer to start", log_type="infra")
            retry_count -= 1
        if retry_count == 0:
            self.task_manager.stop_task(timer_task)
            self.set_exception("Node failure task failed to start")
            return False
        return True

    def call(self):
        self.start_task()
        rest = RestConnection(self.master)
        if rest._rebalance_progress_status(include_failover=False) == "running":
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
            status, stop_time = self._check_if_rebalance_in_progress(180)
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
        if self.expect_auto_failover and not self.auto_reprovision:
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
                    self.set_warn(AutoFailoverException(message))
                    return False
            else:
                message = "Autofailover of node {0} was not initiated after " \
                          "the expected timeout period of {1}".format(
                    self.current_failure_node.ip, self.timeout)
                rest.print_UI_logs(10)
                self.test_log.error(message)
                self.set_warn(AutoFailoverException(message))
                return False
        else:
            if autofailover_initiated:
                message = "Node {0} was autofailed over but no autofailover " \
                          "of the node was expected" \
                    .format(self.current_failure_node.ip)
                rest.print_UI_logs(10)
                self.test_log.error(message)
                if self.get_failover_count() == 1:
                    return True
                self.set_exception(AutoFailoverException(message))
                return False
            elif self.expect_auto_failover:
                self.test_log.error("Node not autofailed over as expected")
                rest.print_UI_logs(10)
                return False

    def has_next(self):
        return self.itr < self.num_servers_to_fail

    def next(self):
        if self.pause != 0:
            self.test_log.debug("Wait before reset_auto_failover")
            sleep(self.pause)
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
        self.check_failure_timer_task_start(node_failure_timer)
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
        self.check_failure_timer_task_start(node_failure_timer)
        shell = RemoteMachineShellConnection(node)
        shell.restart_couchbase()
        shell.disconnect()
        self.test_log.debug("{0} - Restarted couchbase server".format(node))
        self.task_manager.get_task_result(node_failure_timer)
        self.start_time = node_failure_timer.start_time

    def _stop_couchbase_server(self, node):
        node_failure_timer = self.failure_timers[self.itr]
        self.task_manager.add_new_task(node_failure_timer)
        self.check_failure_timer_task_start(node_failure_timer)
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
        self.check_failure_timer_task_start(node_failure_timer)
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
        self.check_failure_timer_task_start(node_failure_timer)
        shell = RemoteMachineShellConnection(node)
        command = "/sbin/reboot"
        shell.execute_command(command=command)
        shell.disconnect()
        self.task_manager.get_task_result(node_failure_timer)
        self.start_time = node_failure_timer.start_time

    def _stop_memcached(self, node):
        node_failure_timer = self.failure_timers[self.itr]
        self.task_manager.add_new_task(node_failure_timer)
        self.check_failure_timer_task_start(node_failure_timer)
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
        # we need to stop couchbase server before mounting partition to avoid inconsistencies
        shell.stop_couchbase()
        o, r = shell.mount_partition(self.disk_location)
        for line in o:
            if self.disk_location in line:
                self.test_log.debug("Mounted disk at location : {0} on {1}"
                                    .format(self.disk_location, node.ip))
                shell.start_couchbase()
                shell.disconnect()
                return
        self.set_exception(Exception("Failed mount disk at location {0} on {1}"
                                     .format(self.disk_location, node.ip)))
        shell.start_couchbase()
        shell.disconnect()
        raise Exception()

    def _disk_full_failure(self, node):
        shell = RemoteMachineShellConnection(node)
        output, error = shell.fill_disk_space(self.disk_location,
                                              self.disk_size)
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
        ui_logs = rest.get_logs(20)
        ui_logs_text = [t["text"] for t in ui_logs]
        ui_logs_time = [t["serverTime"] for t in ui_logs]
        if self.auto_reprovision:
            expected_log = "has been reprovisioned on following nodes: ['ns_1@{}']".format(
                failed_over_node.ip)
        else:
            expected_log = "Starting failing over ['ns_1@{}']".format(
                failed_over_node.ip)
        if expected_log in ui_logs_text:
            failed_over_time = ui_logs_time[ui_logs_text.index(expected_log)]
            return True, failed_over_time
        return False, None

    def get_failover_count(self):
        rest = RestConnection(self.master)
        cluster_status = rest.cluster_status()
        failover_count = 0
        # check for inactiveFailed
        for node in cluster_status['nodes']:
            if node['clusterMembership'] == "inactiveFailed":
                failover_count += 1
        return failover_count

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


class NodeFailureTask(Task):
    def __init__(self, task_manager, node, failure_type,
                 task_type="induce_failure"):
        super(NodeFailureTask, self).__init__("NodeFailureTask_%s_%s_%s"
                                              % (node.ip, failure_type,
                                                 task_type))
        self.task_manager = task_manager
        self.target_node = node
        self.failure_type = failure_type
        self.task_type = task_type
        self.shell = None
        self.set_result(True)

    def _fail_disk(self, node):
        output, error = self.shell.unmount_partition(self.disk_location)
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

    def _recover_disk(self, node):
        # we need to stop couchbase server before mounting partition to avoid inconsistencies
        self.shell.stop_couchbase()
        o, r = self.shell.mount_partition(self.disk_location)
        for line in o:
            if self.disk_location in line:
                self.test_log.debug("Mounted disk at location : {0} on {1}"
                                    .format(self.disk_location, node.ip))
                self.shell.start_couchbase()
                return
        self.test_log.critical("Failed mount disk at location %s on %s"
                               % (self.disk_location, node.ip))
        self.shell.start_couchbase()
        self.set_result(False)

    def _disk_full_failure(self, node):
        output, error = self.shell.fill_disk_space(self.disk_location,
                                                   self.disk_size)
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

    def _recover_disk_full_failure(self):
        delete_file = "{0}/disk-quota.ext3".format(self.disk_location)
        output, error = self.shell.execute_command("rm -f %s" % delete_file)
        if error:
            self.test_log.error(error)

    def __induce_node_failure(self):
        if self.failure_type == "firewall":
            RemoteUtilHelper.enable_firewall(self.target_node)
        elif self.failure_type == "restart_couchbase":
            self.shell.restart_couchbase()
        elif self.failure_type == "stop_couchbase":
            self.shell.stop_couchbase()
        elif self.failure_type == "restart_network":
            self.shell.stop_network(self.timeout + self.timeout_buffer + 30)
        elif self.failure_type == "restart_machine":
            self.shell.execute_command(command="/sbin/reboot")
            self.shell.disconnect()
            self.shell = None
        elif self.failure_type == CouchbaseError.STOP_MEMCACHED:
            self.shell.stop_memcached()
        elif self.failure_type == "network_split":
            for node in self.to_nodes:
                command = "iptables -A INPUT -s %s -j DROP" % node.ip
                self.shell.execute_command(command)
        elif self.failure_type == "disk_failure":
            self._fail_disk(self.target_node)
        elif self.failure_type == "disk_full":
            self._disk_full_failure(self.target_node)
        else:
            self.log.critical("NodeFailureTask - No action defined for %s"
                              % self.failure_type)
            self.set_result(False)

    def __revert_node_failure(self):
        if self.failure_type in ["firewall", "network_split"]:
            self.shell.disable_firewall()
        elif self.failure_type == "stop_couchbase":
            self.shell.start_couchbase()
        elif self.failure_type == "disk_failure":
            self._recover_disk(self.target_node)
        elif self.failure_type == "disk_full":
            self._recover_disk_full_failure()
        elif self.failure_type == CouchbaseError.STOP_MEMCACHED:
            self.shell.start_memcached()
        else:
            self.log.critical("NodeFailureTask - No revert action for %s"
                              % self.failure_type)

    def call(self):
        self.start_task()
        self.shell = RemoteMachineShellConnection(self.target_node)

        if self.task_type == "induce_failure":
            self.__induce_node_failure()
        elif self.task_type == "revert_failure":
            self.__revert_node_failure()

        # During restart_machine, shell will be closed and marked as None
        if self.shell:
            self.shell.disconnect()
        self.complete_task()


class ConcurrentFailoverTask(Task):
    def __init__(self, task_manager, master, servers_to_fail,
                 disk_location=None, disk_size=200,
                 expected_fo_nodes=1, monitor_failover=True,
                 task_type="induce_failure"):
        """
        :param servers_to_fail: Dict of nodes to fail mapped with their
                                corresponding failure method.
                                Eg: {'10.112.212.101': 'stop_server',
                                     '10.112.212.102': 'stop_memcached'}
        """
        super(ConcurrentFailoverTask, self)\
            .__init__("ConcurrentFO_%s_nodes" % len(servers_to_fail))

        # To assist failover monitoring
        self.rest = RestConnection(master)
        self.initial_fo_settings = self.rest.get_autofailover_settings()

        self.task_manager = task_manager
        self.master = master
        self.servers_to_fail = servers_to_fail
        self.timeout = self.initial_fo_settings.timeout
        self.grace_period_for_fo = 5

        # Takes either of induce_failure / revert_failure
        self.task_type = task_type

        # For Disk related failovers
        self.disk_timeout = \
            self.initial_fo_settings.failoverOnDataDiskIssuesTimeout
        self.disk_timeout = disk_location
        self.disk_size = disk_size

        # To track failover operation
        self.expected_nodes_to_fo = expected_fo_nodes
        self.monitor_failover = monitor_failover

        # To track NodeFailureTask per node
        self.sub_tasks = list()
        self.set_result(True)

        # Fetch last rebalance task to track starting of current rebalance
        self.prev_rebalance_status_id = None
        server_task = self.rest.ns_server_tasks(
            task_type="rebalance", task_sub_type="failover")
        if server_task and "statusId" in server_task:
            self.prev_rebalance_status_id = server_task["statusId"]
        self.log.debug("Last known failover status_id: %s"
                       % self.prev_rebalance_status_id)

    def wait_for_fo_attempt(self):
        start_time = time.time()
        expect_fo_after_time = start_time + self.timeout
        while int(time.time()) < expect_fo_after_time:
            curr_fo_settings = self.rest.get_autofailover_settings()
            if self.initial_fo_settings.count != curr_fo_settings.count:
                self.test_log.critical("Auto failover triggered before "
                                       "grace period. Initial %s vs %s current"
                                       % (self.initial_fo_settings.count,
                                          curr_fo_settings.count))
                self.set_result(False)

    def call(self):
        self.start_task()

        for node, failure_info in self.servers_to_fail.items():
            self.sub_tasks.append(
                NodeFailureTask(self.task_manager, node, failure_info,
                                task_type=self.task_type))
            self.task_manager.add_new_task(self.sub_tasks[-1])

        # Wait for failure tasks to complete
        for task in self.sub_tasks:
            self.task_manager.get_task_result(task)
            if task.result is False:
                self.log.critical("NodeFailureTask '%s' failed"
                                  % task.thread_name)
                self.set_result(task.result)

        if self.task_type == "revert_failure":
            self.complete_task()
            return

        if self.result:
            self.wait_for_fo_attempt()

            if self.result and self.monitor_failover:
                self.log.info("Wait for failover to actually start running")
                timeout = int(time.time()) + 15
                task_id_changed = False
                while not task_id_changed and int(time.time()) < timeout:
                    server_task = self.rest.ns_server_tasks(
                        task_type="rebalance", task_sub_type="failover")
                    if server_task and server_task["statusId"] != \
                            self.prev_rebalance_status_id:
                        task_id_changed = True
                        self.prev_rebalance_status_id = server_task["statusId"]
                        self.log.debug("New failover status id: %s"
                                       % server_task["statusId"])

                if task_id_changed:
                    status = self.rest.monitorRebalance()
                else:
                    status = False
                    self.test_log.critical("Failover not started as expected")

                if status is False:
                    self.set_result(False)
                    self.test_log.critical("Auto-Failover rebalance failed")
                else:
                    curr_fo_settings = self.rest.get_autofailover_settings()
                    if curr_fo_settings.count != self.expected_nodes_to_fo:
                        self.test_log.critical(
                            "Some nodes are yet to be failed over. "
                            "Num fo nodes - Expected %s, Actual %s"
                            % (self.expected_nodes_to_fo,
                               curr_fo_settings.count))
                        self.set_result(False)

        self.complete_task()


class NodeDownTimerTask(Task):
    def __init__(self, node, port=None, timeout=300):
        Task.__init__(self, "NodeDownTimerTask")
        self.test_log.debug("Initializing NodeDownTimerTask")
        self.node = node
        self.port = port
        self.timeout = timeout
        self.start_time = 0

    def call(self):
        self.start_task()
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
                    self.test_log.warning("Unexpected exception: %s" % e)
                    self.complete_task()
                    return True
                try:
                    self.start_time = time.time()
                    socket.socket().connect(("%s" % self.node,
                                             constants.port))
                    socket.socket().close()
                    socket.socket().connect(("%s" % self.node,
                                             constants.memcached_port))
                    socket.socket().close()
                except socket.error:
                    self.test_log.debug(
                        "Injected failure in %s. Caught due to ports"
                        % self.node)
                    self.complete_task()
                    return True
            else:
                try:
                    self.start_time = time.time()
                    socket.socket().connect(("%s" % self.node,
                                             int(self.port)))
                    socket.socket().close()
                    socket.socket().connect(("%s" % self.node,
                                             constants.memcached_port))
                    socket.socket().close()
                except socket.error:
                    self.test_log.debug("Injected failure in %s" % self.node)
                    self.complete_task()
                    return True
        if time.time() >= end_task:
            self.complete_task()
            self.test_log.error("Could not inject failure in %s" % self.node)
            return False


class Atomicity(Task):
    instances = 1
    num_items = 110
    mutations = num_items
    start_from = 0
    op_type = "insert"
    persist_to = 1
    replicate_to = 1

    task_manager = list()
    write_offset = list()

    def __init__(self, cluster, task_manager, bucket, clients,
                 generator, op_type, exp, flag=0,
                 persist_to=0, replicate_to=0, time_unit="seconds",
                 batch_size=1,
                 timeout_secs=5, compression=None,
                 process_concurrency=8, print_ops_rate=True, retries=5,
                 update_count=1, transaction_timeout=5,
                 commit=True, durability=None, sync=True, num_threads=5,
                 record_fail=False, defer=False):
        super(Atomicity, self).__init__("AtomicityDocLoadTask_%s_%s_%s_%s"
                                        % (op_type, generator[0].start,
                                           generator[0].end, time.time()))

        self.generators = generator
        self.cluster = cluster
        self.commit = commit
        self.record_fail = record_fail
        self.defer = defer
        self.num_docs = num_threads
        self.exp = exp
        self.flag = flag
        self.sync = sync
        self.persist_to = persist_to
        self.replicate_to = replicate_to
        self.time_unit = time_unit
        self.timeout_secs = timeout_secs
        self.transaction_timeout = transaction_timeout
        self.compression = compression
        self.process_concurrency = process_concurrency
        self.task_manager = task_manager
        self.batch_size = batch_size
        self.print_ops_rate = print_ops_rate
        self.op_type = op_type
        self.bucket = bucket
        self.clients = clients
        self.gen = list()
        self.retries = retries
        self.update_count = update_count
        self.transaction_app = Transaction()
        self.transaction = None
        if durability == Bucket.DurabilityLevel.MAJORITY:
            self.durability = 1
        elif durability == \
                Bucket.DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE:
            self.durability = 2
        elif durability == Bucket.DurabilityLevel.PERSIST_TO_MAJORITY:
            self.durability = 3
        elif durability == "ONLY_NONE":
            self.durability = 4
        else:
            self.durability = 0
        sleep(10, "Wait before txn load")

    def call(self):
        tasks = list()
        exception_seen = None
        self.start_task()
        if self.op_type == "time_out":
            transaction_config = self.transaction_app.createTransactionConfig(
                2, self.durability)
        else:
            self.test_log.info("Transaction timeout: %s"
                               % self.transaction_timeout)
            transaction_config = self.transaction_app.createTransactionConfig(
                self.transaction_timeout, self.durability)
        try:
            self.transaction = self.transaction_app.createTansaction(
                self.clients[0][0].cluster, transaction_config)
            self.test_log.info("Transaction: %s" % self.transaction)
        except Exception as e:
            self.set_exception(e)

        for generator in self.generators:
            tasks.extend(self.get_tasks(generator))

        self.test_log.debug("Adding new tasks")
        for task in tasks:
            self.task_manager.add_new_task(task)

        for task in tasks:
            self.task_manager.get_task_result(task)
            if task.exception is not None:
                exception_seen = task.exception

        self.transaction.close()

        for con in self.clients:
            for client in con:
                client.close()

        self.complete_task()
        if exception_seen:
            self.set_exception(exception_seen)

    def get_tasks(self, generator):
        generators = []
        tasks = []
        gen_start = int(generator.start)
        gen_end = int(generator.end)

        gen_range = max(
            int((generator.end - generator.start) / self.process_concurrency),
            1)
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
            task = self.Loader(self.cluster, self.bucket[i], self.clients,
                               generators[i], self.op_type,
                               self.exp, self.num_docs, self.update_count,
                               self.defer, self.sync, self.record_fail,
                               flag=self.flag,
                               persist_to=self.persist_to,
                               replicate_to=self.replicate_to,
                               time_unit=self.time_unit,
                               batch_size=self.batch_size,
                               timeout_secs=self.timeout_secs,
                               compression=self.compression,
                               instance_num=1,
                               transaction_app=self.transaction_app,
                               transaction=self.transaction,
                               commit=self.commit,
                               retries=self.retries)
            tasks.append(task)
        return tasks

    class Loader(GenericLoadingTask):
        """
        1. Start inserting data into buckets
        2. Keep updating the write offset
        3. Start the reader thread
        4. Keep track of non durable documents
        """

        def __init__(self, cluster, bucket, clients,
                     generator, op_type, exp,
                     num_docs, update_count, defer, sync, record_fail,
                     flag=0, persist_to=0, replicate_to=0, time_unit="seconds",
                     batch_size=1, timeout_secs=5,
                     compression=None, retries=5, instance_num=0,
                     transaction_app=None, transaction=None, commit=True,
                     sdk_client_pool=None,
                     scope=CbServer.default_scope,
                     collection=CbServer.default_collection):
            super(Atomicity.Loader, self).__init__(
                cluster, bucket, clients[0][0],
                batch_size=batch_size,
                timeout_secs=timeout_secs, compression=compression,
                retries=retries,
                sdk_client_pool=sdk_client_pool,
                scope=scope, collection=collection)

            self.generator = generator
            self.op_type = op_type.split(';')
            self.thread_name = "Atomicity_Loader-%s_%s_%s_%s_%s" \
                               % (op_type, bucket,
                                  generator._doc_gen.start,
                                  generator._doc_gen.end,
                                  time.time())
            self.exp = exp
            self.flag = flag
            self.persist_to = persist_to
            self.replicate_to = replicate_to
            self.compression = compression
            self.timeout_secs = timeout_secs
            self.time_unit = time_unit
            self.instance = instance_num
            self.transaction_app = transaction_app
            self.transaction = transaction
            self.commit = commit
            self.defer = defer
            self.clients = clients
            self.bucket = bucket
            self.exp_unit = "seconds"
            self.retries = retries
            self.key_value_list = list()
            self.exception = None
            self.num_docs = num_docs
            self.update_count = update_count
            self.sync = sync
            self.record_fail = record_fail

            if self.op_type[-1] == "delete":
                self.suppress_error_table = True

        def has_next(self):
            return self.generator.has_next()

        def call(self):
            self.start_task()
            self.test_log.info("Starting Atomicity load generation thread")
            self.all_keys = list()
            self.update_keys = list()
            self.delete_keys = list()
            docs = list()
            exception = None

            doc_gen = self.generator
            while self.has_next():
                self.batch = doc_gen.next_batch()
                self.key_value_list.extend(self.batch)

            for tuple in self.key_value_list:
                docs.append(tuple)
            last_batch = dict(self.key_value_list[-10:])

            self.all_keys = dict(self.key_value_list).keys()
            self.list_docs = list(self.__chunks(self.all_keys,
                                                self.num_docs))
            self.docs = list(self.__chunks(self.key_value_list, self.num_docs))

            for op_type in self.op_type:
                self.encoding = list()
                if op_type == 'general_create':
                    for client in self.clients[0]:
                        self.batch_create(
                            self.key_value_list, client,
                            persist_to=self.persist_to,
                            replicate_to=self.replicate_to,
                            doc_type=self.generator.doc_type)
                elif op_type == "create":
                    if len(self.op_type) != 1:
                        commit = True
                    else:
                        commit = self.commit
                    for self.doc in self.docs:
                        self.transaction_load(self.doc, commit,
                                              op_type="create")
                    if not commit:
                        self.all_keys = []
                elif op_type in ["update", "rebalance_only_update"]:
                    for doc in self.list_docs:
                        self.transaction_load(doc, self.commit,
                                              op_type="update")
                    if self.commit:
                        self.update_keys = self.all_keys
                elif op_type == "update_Rollback":
                    exception = self.transaction_app.RunTransaction(
                        self.clients[0][0].cluster,
                        self.transaction, self.bucket, [], self.update_keys,
                        [], False, True, self.update_count)
                elif op_type == "delete" or op_type == "rebalance_delete":
                    for doc in self.list_docs:
                        self.transaction_load(doc, self.commit,
                                              op_type="delete")
                    if self.commit:
                        self.delete_keys = self.all_keys
                elif op_type == "general_update":
                    for client in self.clients[0]:
                        self.batch_update(self.batch, client,
                                          persist_to=self.persist_to,
                                          replicate_to=self.replicate_to,
                                          doc_type=self.generator.doc_type)
                elif op_type == "general_delete":
                    self.test_log.debug("Performing delete for keys %s"
                                        % last_batch.keys())
                    for client in self.clients[0]:
                        _ = self.batch_delete(
                            self.batch, client,
                            persist_to=self.persist_to,
                            replicate_to=self.replicate_to,
                            durability="")
                    self.delete_keys = last_batch.keys()
                elif op_type in ["rebalance_update", "create_update"]:
                    for i in range(len(self.docs)):
                        self.transaction_load(self.docs[i], self.commit,
                                              self.list_docs[i],
                                              op_type="create")
                    if self.commit:
                        self.update_keys = self.all_keys
                elif op_type == "time_out":
                    err = self.transaction_app.RunTransaction(
                        self.clients[0][0].cluster,
                        self.transaction, self.bucket, docs, [], [],
                        True, True, self.update_count)
                    if "AttemptExpired" in str(err):
                        self.test_log.info("Transaction Expired as Expected")
                        for line in err:
                            self.test_log.info("%s" % line)
                        self.test_log.debug(
                            "End of First Transaction that is getting timeout")
                    else:
                        exception = err
                        # self.test_log.warning("Wait for txn to clean up")
                        # time.sleep(60)

                if self.defer:
                    self.test_log.info("Commit/rollback deffered transaction")
                    self.retries = 5
                    if op_type != "create":
                        commit = self.commit
                    for encoded in self.encoding:
                        err = self.transaction_app.DefferedTransaction(
                            self.clients[0][0].cluster,
                            self.transaction, commit, encoded)
                        if err:
                            while self.retries > 0:
                                if SDKException.DurabilityImpossibleException \
                                        in str(err):
                                    self.retries -= 1
                                    self.test_log.info(
                                        "Retrying due to D_Impossible seen during deferred-transaction")
                                    # sleep(60)
                                    err = self.transaction_app.DefferedTransaction(
                                        self.clients[0][0].cluster,
                                        self.transaction, self.commit, encoded)
                                    if err:
                                        continue
                                    break
                                else:
                                    exception = err
                                    break
                if exception:
                    if self.record_fail:
                        self.all_keys = list()
                    else:
                        self.exception = Exception(exception)
                        break

            self.test_log.info("Atomicity Load generation thread completed")
            self.inserted_keys = dict()
            for client in self.clients[0]:
                self.inserted_keys[client] = []
                self.inserted_keys[client].extend(self.all_keys)

            self.test_log.info("Starting Atomicity Verification thread")
            self.process_values_for_verification(self.key_value_list)
            for client in self.clients[0]:
                result_map = self.batch_read(self.all_keys, client)
                wrong_values = self.validate_key_val(result_map[0],
                                                     self.key_value_list,
                                                     client)

                if wrong_values:
                    self.exception = "Wrong key value: %s" \
                                     % ','.join(wrong_values)

                for key in self.delete_keys:
                    if key in self.inserted_keys[client]:
                        self.inserted_keys[client].remove(key)

                if self.inserted_keys[client] \
                        and "time_out" not in self.op_type:
                    self.exception = "Keys missing: %s" \
                                     % (','.join(self.inserted_keys[client]))

            self.test_log.info("Completed Atomicity Verification thread")
            self.complete_task()

        def transaction_load(self, doc, commit=True, update_keys=[],
                             op_type="create"):
            err = None
            if self.defer:
                if op_type == "create":
                    ret = self.transaction_app.DeferTransaction(
                        self.clients[0][0].cluster,
                        self.transaction, self.bucket, doc, update_keys, [], self.update_count)
                elif op_type == "update":
                    ret = self.transaction_app.DeferTransaction(
                        self.clients[0][0].cluster,
                        self.transaction, self.bucket, [], doc, [], self.update_count)
                elif op_type == "delete":
                    ret = self.transaction_app.DeferTransaction(
                        self.clients[0][0].cluster,
                        self.transaction, self.bucket, [], [], doc, self.update_count)
                err = ret.getT2()
            else:
                if op_type == "create":
                    err = self.transaction_app.RunTransaction(
                        self.clients[0][0].cluster,
                        self.transaction, self.bucket, doc, update_keys, [],
                        commit, self.sync, self.update_count)
                elif op_type == "update":
                    err = self.transaction_app.RunTransaction(
                        self.clients[0][0].cluster,
                        self.transaction, self.bucket, [], doc, [],
                        commit, self.sync, self.update_count)
                elif op_type == "delete":
                    err = self.transaction_app.RunTransaction(
                        self.clients[0][0].cluster,
                        self.transaction, self.bucket, [], [], doc,
                        commit, self.sync, self.update_count)
            if err:
                if self.record_fail:
                    self.all_keys = list()
                elif SDKException.DurabilityImpossibleException in str(err) \
                        and self.retries > 0:
                    self.test_log.debug(err)
                    self.test_log.info("D_ImpossibleException so retrying..")
                    # sleep(60)
                    self.retries -= 1
                    self.transaction_load(doc, commit, update_keys, op_type)
                # else:
                #     exception = err
            elif self.defer:
                self.encoding.append(ret.getT1())

        def __chunks(self, l, n):
            """Yield successive n-sized chunks from l."""
            for i in range(0, len(l), n):
                yield l[i:i + n]

        def validate_key_val(self, map, key_value, client):
            wrong_values = []
            for item in key_value:
                key = item.getT1()
                value = item.getT2()
                if key in map:
                    if self.op_type == "time_out":
                        expected_val = {}
                    else:
                        expected_val = Json.loads(value.toString())
                    actual_val = {}
                    if map[key]['cas'] != 0:
                        actual_val = Json.loads(map[key]['value'].toString())
                    elif map[key]['error'] is not None:
                        actual_val = map[key]['error'].toString()
                    if expected_val == actual_val or map[key]['cas'] == 0:
                        try:
                            self.inserted_keys[client].remove(key)
                        except:
                            pass
                    else:
                        wrong_values.append(key)
                        self.inserted_keys[client].remove(key)
                        self.test_log.info("Key %s - Actual value %s,"
                                           "Expected value: %s"
                                           % (key, actual_val, expected_val))
            return wrong_values

        def process_values_for_verification(self, key_val):
            for item in key_val:
                key = item.getT1()
                if key in self.update_keys or self.op_type == "verify":
                    try:
                        # New updated value, however it is not their
                        # in orignal code "LoadDocumentsTask"
                        value = item.getT2()
                        value.put('mutated', self.update_count)
                    except ValueError:
                        pass
                    finally:
                        key_val.remove(item)
                        item = Tuples.of(key, value)
                        key_val.append(item)


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

    def __init__(self, server, design_doc_name, fragmentation_value=10,
                 bucket="default"):

        Task.__init__(self, "monitor_frag_task")
        self.server = server
        self.bucket = bucket
        self.fragmentation_value = fragmentation_value
        self.design_doc_name = design_doc_name
        self.result = False

    def call(self):
        self.start_task()
        # sanity check of fragmentation value
        if self.fragmentation_value < 0 or self.fragmentation_value > 100:
            err_msg = "Invalid value for fragmentation %d" % self.fragmentation_value
            self.set_exception(Exception(err_msg))

        try:
            auto_compact_percentage = self._get_current_auto_compaction_percentage()
            if auto_compact_percentage != "undefined" \
                    and auto_compact_percentage < self.fragmentation_value:
                self.test_log.warn(
                    "Auto compaction is set to %s. "
                    "Therefore fragmentation_value %s may not be reached"
                    % (auto_compact_percentage, self.fragmentation_value))

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

        auto_compact_percentage = \
            content["autoCompactionSettings"]["viewFragmentationThreshold"][
                "percentage"]

        return auto_compact_percentage

    def check(self):

        rest = RestConnection(self.server)
        new_frag_value = 0
        timeout = 300
        while new_frag_value < self.fragmentation_value and timeout > 0:
            new_frag_value = MonitorViewFragmentationTask.calc_ddoc_fragmentation(
                rest, self.design_doc_name, bucket=self.bucket)
            self.test_log.info("%s: current amount of fragmentation = %d, \
                               required: %d"
                               % (self.design_doc_name,
                                  new_frag_value,
                                  self.fragmentation_value))
            if new_frag_value > self.fragmentation_value:
                self.result = True
                break
            timeout -= 1
            sleep(1, "Wait for fragmentation_level to reach", log_type="infra")

    @staticmethod
    def aggregate_ddoc_info(rest, design_doc_name, bucket="default",
                            with_rebalance=False):
        infra_log = logger.get("infra")
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
                infra_log.error(e)
                if "Error occured reading set_view _info" in str(
                        e) and with_rebalance:
                    infra_log.warning("Node {0} {1} not ready yet?: {2}"
                                      .format(node.id, node.port, e.message))
                else:
                    raise e
            if status:
                info.append(content)
        return info

    @staticmethod
    def calc_ddoc_fragmentation(rest, design_doc_name, bucket="default",
                                with_rebalance=False):

        total_disk_size = 0
        total_data_size = 0
        total_fragmentation = 0
        nodes_ddoc_info = \
            MonitorViewFragmentationTask.aggregate_ddoc_info(rest,
                                                             design_doc_name,
                                                             bucket,
                                                             with_rebalance)
        total_disk_size = sum(
            [content['disk_size'] for content in nodes_ddoc_info])
        total_data_size = sum(
            [content['data_size'] for content in nodes_ddoc_info])

        if total_disk_size > 0 and total_data_size > 0:
            total_fragmentation = \
                (total_disk_size - total_data_size) / float(
                    total_disk_size) * 100

        return total_fragmentation


class ViewCompactionTask(Task):
    """
        Executes view compaction for a given design doc. This is technicially view compaction
        as represented by the api and also because the fragmentation is generated by the
        keys emitted by map/reduce functions within views.  Task will check that compaction
        history for design doc is incremented and if any work was really done.
    """

    def __init__(self, server, design_doc_name, bucket="default",
                 with_rebalance=False):

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
            self.test_log.debug(
                "{0}: stats compaction before triggering it: ({1},{2})"
                    .format(self.design_doc_name,
                            self.compaction_revision,
                            self.precompacted_fragmentation))
            if self.precompacted_fragmentation == 0:
                self.test_log.warning(
                    "%s: There is nothing to compact, fragmentation is 0"
                    % self.design_doc_name)
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
            self.test_log.debug(
                "{0}: stats compaction:revision and fragmentation: ({1},{2})"
                    .format(self.design_doc_name,
                            new_compaction_revision,
                            fragmentation))

            if new_compaction_revision == self.compaction_revision and _compaction_running:
                # compaction ran successfully but compaction was not changed
                # perhaps we are still compacting
                self.test_log.debug("design doc {0} is compacting"
                                    .format(self.design_doc_name))
                self.check()
            elif new_compaction_revision > self.compaction_revision or \
                    self.precompacted_fragmentation > fragmentation:
                self.test_log.info(
                    "{1}: compactor was run, compaction revision was changed on {0}"
                        .format(new_compaction_revision,
                                self.design_doc_name))
                frag_val_diff = fragmentation - self.precompacted_fragmentation
                self.test_log.info("%s: fragmentation went from %d to %d"
                                   % (self.design_doc_name,
                                      self.precompacted_fragmentation,
                                      fragmentation))

                if frag_val_diff > 0:
                    # compaction ran successfully but datasize still same
                    # perhaps we are still compacting
                    if self._is_compacting():
                        self.check()
                    self.test_log.warning(
                        "Compaction completed, but fragmentation value {0} "
                        "is more than before compaction {1}"
                            .format(fragmentation,
                                    self.precompacted_fragmentation))
                    # Probably we already compacted, so nothing to do here
                    self.set_result(self.with_rebalance)
                else:
                    self.set_result(True)
            else:
                for i in xrange(20):
                    self.test_log.debug("Wait for compaction to start")
                    sleep(2)
                    if self._is_compacting():
                        self.check()
                    else:
                        new_compaction_revision, fragmentation = \
                            self._get_compaction_details()
                        self.test_log.info("{2}: stats compaction: ({0},{1})"
                                           .format(new_compaction_revision,
                                                   fragmentation,
                                                   self.design_doc_name))
                        # case of rebalance when with concurrent updates
                        # it's possible that compacttion value has
                        # not changed significantly
                        if new_compaction_revision > self.compaction_revision \
                                and self.with_rebalance:
                            self.test_log.info("Compaction revision increased")
                            self.set_result(True)
                            return
                        else:
                            continue
                # print details in case of failure
                self.test_log.info("design doc {0} is compacting:{1}"
                                   .format(self.design_doc_name,
                                           self._is_compacting()))
                new_compaction_revision, fragmentation = self._get_compaction_details()
                self.test_log.error("Stats compaction still: ({0},{1})"
                                    .format(new_compaction_revision,
                                            fragmentation))
                status, content = self.rest.set_view_info(self.bucket,
                                                          self.design_doc_name)
                stats = content["stats"]
                self.test_log.warn("General compaction stats:{0}"
                                   .format(stats))
                self.set_exception(
                    "Check system logs, looks like compaction failed to start")
        except SetViewInfoNotFound as ex:
            self.result = False
            self.set_exception(ex)
        # catch and set all unexpected exceptions
        except Exception as e:
            self.result = False
            self.set_exception(e)

    def _get_compaction_details(self):
        status, content = self.rest.set_view_info(self.bucket,
                                                  self.design_doc_name)
        curr_no_of_compactions = content["stats"]["compactions"]
        curr_ddoc_fragemtation = \
            MonitorViewFragmentationTask.calc_ddoc_fragmentation(self.rest,
                                                                 self.design_doc_name,
                                                                 self.bucket,
                                                                 self.with_rebalance)
        return (curr_no_of_compactions, curr_ddoc_fragemtation)

    def _is_compacting(self):
        status, content = self.rest.set_view_info(self.bucket,
                                                  self.design_doc_name)
        return content["compact_running"] == True


class CompactBucketTask(Task):
    def __init__(self, server, bucket, timeout=300):
        Task.__init__(self, "CompactionTask_%s" % bucket.name)
        self.server = server
        self.bucket = bucket
        self.progress = 0
        self.timeout = timeout
        self.rest = RestConnection(server)
        self.retries = 20
        self.statuses = dict()

        # get the current count of compactions
        nodes = self.rest.get_nodes()
        self.compaction_count = dict()
        for node in nodes:
            self.compaction_count[node.ip] = 0

    def call(self):
        self.start_task()

        status = BucketHelper(self.server).compact_bucket(self.bucket.name)
        if status is False:
            while self.retries != 0:
                sleep(60, "Wait before next compaction call", log_type="infra")
                status = BucketHelper(self.server).compact_bucket(self.bucket.name)
                if status is True:
                    self.set_result(True)
                    break
                self.set_result(False)
                self.retries -= 1
        else:
            self.set_result(True)

        if self.result is True:
            stop_time = time.time() + self.timeout
            while time.time() < stop_time:
                if self.timeout > 0 and time.time() > stop_time:
                    self.set_exception("API to check compaction status timed out in"
                                       "%s seconds" % self.timeout)
                    break
                status, self.progress = \
                    self.rest.check_compaction_status(self.bucket.name)
                if self.progress > 0:
                    self.test_log.debug("Compaction started for %s"
                                       % self.bucket.name)
                    break
                sleep(2, "Wait before next check compaction call", log_type="infra")

            stop_time = time.time() + self.timeout
            while time.time() < stop_time:
                if self.timeout > 0 and time.time() > stop_time:
                    self.set_exception("Compaction timed out to complete with "
                                       "%s seconds" % self.timeout)
                status, self.progress = \
                    self.rest.check_compaction_status(self.bucket.name)

                if status is True:
                    self.test_log.debug("%s compaction done: %s%%"
                                        % (self.bucket.name, self.progress))
                if status is False:
                    self.progress = 100
                    self.test_log.debug("Compaction completed for %s"
                                       % self.bucket.name)
                    self.test_log.info("%s compaction done: %s%%"
                                        % (self.bucket.name, self.progress))
                    break
                sleep(5, "Wait before next check compaction call", log_type="infra")
        else:
            self.test_log.error("Compaction failed to complete within "
                                "%s retries" % self.retries)

        self.complete_task()


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


class CBASQueryExecuteTask(Task):
    def __init__(self, cluster, cbas_util, cbas_endpoint, statement):
        super(CBASQueryExecuteTask, self).__init__("Cbas_query_task: %s"
                                                   % statement)
        self.cluster = cluster
        self.cbas_util = cbas_util
        self.cbas_endpoint = cbas_endpoint
        self.statement = statement

    def call(self):
        self.start_task()
        try:
            response, metrics, errors, results, handle = \
                self.cbas_util.execute_statement_on_cbas_util(
                    self.cluster, self.statement)

            if response:
                self.set_result(True)
                self.actual_result = results
            else:
                self.test_log.error("Error during CBAS query: %s" % errors)
                self.set_result(False)
        except Exception as e:
            self.log.error("CBASQueryExecuteTask EXCEPTION: " + e)
            self.set_result(False)
            self.set_exception(e)

        self.complete_task()


class NodeInitializeTask(Task):
    def __init__(self, server, disabled_consistent_view=None,
                 rebalanceIndexWaitingDisabled=None,
                 rebalanceIndexPausingDisabled=None,
                 maxParallelIndexers=None,
                 maxParallelReplicaIndexers=None,
                 port=None, quota_percent=None,
                 services=None, gsi_type='forestdb',
                 services_mem_quota_percent=None):
        Task.__init__(self, "node_init_task_%s_%s" %
                      (server.ip, server.port))
        self.server = server
        self.port = port or server.port
        self.services_mem_quota_percent = services_mem_quota_percent \
            if services_mem_quota_percent else dict()
        self.quota_percent = quota_percent
        self.disable_consistent_view = disabled_consistent_view
        self.rebalanceIndexWaitingDisabled = rebalanceIndexWaitingDisabled
        self.rebalanceIndexPausingDisabled = rebalanceIndexPausingDisabled
        self.maxParallelIndexers = maxParallelIndexers
        self.maxParallelReplicaIndexers = maxParallelReplicaIndexers
        self.services = services
        self.gsi_type = gsi_type
        self.total_memory = 0

    def __get_memory_quota_in_mb(self, service_name):
        mem_quota = 0

        if service_name in self.services_mem_quota_percent:
            percent = self.services_mem_quota_percent[service_name]
            mem_quota = int(self.total_memory* percent / 100)
        return mem_quota

    def call(self):
        self.start_task()
        rest = None
        service_quota = dict()
        try:
            rest = RestConnection(self.server)
        except ServerUnavailableException as error:
            self.set_exception(error)

        # Change timeout back to 10 after
        # https://issues.couchbase.com/browse/MB-40670 is resolved
        info = Task.wait_until(lambda: rest.get_nodes_self(),
                               lambda x: x.memoryTotal > 0 or x.storageTotalRam > 0, 30)
        self.test_log.debug("server: %s, nodes/self: %s", self.server,
                            info.__dict__)

        # Cluster-run case check
        if int(info.port) in range(9091, 9991):
            self.set_result(True)
            return

        timeout = time.time() + 300
        while self.total_memory <= 0 and time.time() < timeout:
            info = rest.get_nodes_self()
            self.total_memory = int(info.mcdMemoryReserved - 100)
        self.log.critical("mcdMemoryReserved reported in nodes/self is: %s"
                          % info.mcdMemoryReserved)
        if self.quota_percent:
            self.total_memory = int(self.total_memory * self.quota_percent
                                    / 100)

        override_kv_mem = True \
            if CbServer.Services.KV not in self.services_mem_quota_percent \
            else False

        if override_kv_mem and self.services and len(self.services) > 1:
            services_req_mem = CbServer.Services.services_require_memory()
            for service in self.services:
                if service in services_req_mem:
                    override_kv_mem = False
                    break

        if override_kv_mem:
            self.services_mem_quota_percent[CbServer.Services.KV] = 100

        service_quota[CbServer.Settings.KV_MEM_QUOTA] = \
            max(self.__get_memory_quota_in_mb(CbServer.Services.KV),
                CbServer.Settings.MinRAMQuota.KV)

        service_quota[CbServer.Settings.CBAS_MEM_QUOTA] = \
            max(self.__get_memory_quota_in_mb(CbServer.Services.CBAS),
                CbServer.Settings.MinRAMQuota.CBAS)

        service_quota[CbServer.Settings.INDEX_MEM_QUOTA] = \
            max(self.__get_memory_quota_in_mb(CbServer.Services.INDEX),
                CbServer.Settings.MinRAMQuota.INDEX)

        service_quota[CbServer.Settings.FTS_MEM_QUOTA] = \
            max(self.__get_memory_quota_in_mb(CbServer.Services.FTS),
                CbServer.Settings.MinRAMQuota.FTS)

        service_quota[CbServer.Settings.EVENTING_MEM_QUOTA] = \
            max(self.__get_memory_quota_in_mb(CbServer.Services.EVENTING),
                CbServer.Settings.MinRAMQuota.EVENTING)

        # Display memory quota allocated
        tbl = TableView(self.test_log.critical)
        tbl.set_headers(["Service", "RAM MiB"])
        for service, mem_quota in service_quota.items():
            tbl.add_row([service, str(mem_quota)])
        tbl.display("Memory quota allocated:")

        req_mem = sum([service_quota[service] for service in service_quota])
        if self.total_memory < req_mem:
            self.log.warning("Total memory requested %s > %s available"
                             % (req_mem, self.total_memory))

        username = self.server.rest_username
        password = self.server.rest_password

        rest.set_service_mem_quota(service_quota)
        rest.set_indexer_storage_mode(self.gsi_type)

        if self.services:
            status = rest.init_node_services(
                username=username,
                password=password,
                port=self.port,
                hostname=self.server.ip,
                services=self.services)
            if not status:
                self.set_exception(
                    Exception('unable to set services for server %s'
                              % self.server.ip))
        if self.disable_consistent_view is not None:
            rest.set_reb_cons_view(self.disable_consistent_view)
        if self.rebalanceIndexWaitingDisabled is not None:
            rest.set_reb_index_waiting(self.rebalanceIndexWaitingDisabled)
        if self.rebalanceIndexPausingDisabled is not None:
            rest.set_rebalance_index_pausing(
                self.rebalanceIndexPausingDisabled)
        if self.maxParallelIndexers is not None:
            rest.set_max_parallel_indexers(self.maxParallelIndexers)
        if self.maxParallelReplicaIndexers is not None:
            rest.set_max_parallel_replica_indexers(
                self.maxParallelReplicaIndexers)

        rest.init_cluster(username, password, self.port)
        self.server.port = self.port
        try:
            rest = RestConnection(self.server)
        except ServerUnavailableException as error:
            self.set_exception(error)
        info = rest.get_nodes_self()

        if info is None:
            self.set_exception(
                Exception(
                    'unable to get information on a server %s, it is available?'
                    % self.server.ip))
        self.set_result(self.total_memory)


class FailoverTask(Task):
    def __init__(self, servers, to_failover=[], wait_for_pending=0,
                 graceful=False, use_hostnames=False, allow_unsafe=False,
                 all_at_once=False):
        Task.__init__(self, "failover_task")
        self.servers = servers
        self.to_failover = to_failover
        self.graceful = graceful
        self.wait_for_pending = wait_for_pending
        self.use_hostnames = use_hostnames
        self.allow_unsafe = allow_unsafe
        self.all_at_once = all_at_once

    def call(self):
        self.start_task()
        try:
            self._failover_nodes()
            self.test_log.debug(
                "{0} seconds sleep after failover for nodes to go pending...."
                .format(self.wait_for_pending))
            self.sleep(self.wait_for_pending, log_type="infra")
            self.set_result(True)
            self.complete_task()

        except (FailoverFailedException, Exception) as e:
            self.set_result(False)
            self.set_exception(e)

    def _failover_nodes(self):
        rest = RestConnection(self.servers[0])

        # call REST fail_over for the nodes to be failed over all at once
        if self.all_at_once:
            otp_nodes = list()
            for server in self.to_failover:
                for node in rest.node_statuses():
                    if (
                            server.hostname if self.use_hostnames else server.ip) == node.ip and int(
                        server.port) == int(node.port):
                        otp_nodes.append(node.id)
            self.test_log.debug(
                "Failing over {0} with graceful={1}"
                    .format(otp_nodes, self.graceful))
            result = rest.fail_over(otp_nodes, self.graceful,
                                    self.allow_unsafe, self.all_at_once)
            if not result:
                self.set_exception("Node failover failed!!")
        else:
            # call REST fail_over for the nodes to be failed over one by one
            for server in self.to_failover:
                for node in rest.node_statuses():
                    if (
                            server.hostname if self.use_hostnames else server.ip) == node.ip and int(
                        server.port) == int(node.port):
                        self.test_log.debug(
                            "Failing over {0}:{1} with graceful={2}"
                                .format(node.ip, node.port, self.graceful))
                        result = rest.fail_over(node.id, self.graceful,
                                                self.allow_unsafe)
                        if not result:
                            self.set_exception("Node failover failed!!")
        rest.monitorRebalance()


class BucketFlushTask(Task):
    def __init__(self, server, task_manager, bucket="default", timeout=300):
        Task.__init__(self, "bucket_flush_task", task_manager)
        self.server = server
        self.bucket = bucket
        if isinstance(bucket, Bucket):
            self.bucket = bucket.name
        self.timeout = timeout

    def call(self):
        self.start_task()
        try:
            rest = BucketHelper(self.server)
            if rest.flush_bucket(self.bucket):
                if MemcachedHelper.wait_for_vbuckets_ready_state(
                        self.server, self.bucket,
                        timeout_in_seconds=self.timeout):
                    self.set_result(True)
                else:
                    self.test_log.error(
                        "Unable to reach bucket {0} on server {1} after flush"
                            .format(self.bucket, self.server))
                    self.set_result(False)
            else:
                self.set_result(False)
            self.complete_task()

        except (BucketFlushFailed, Exception) as e:
            self.set_result(False)
            self.set_exception(e)


class CreateDatasetsTask(Task):
    def __init__(self, cluster, bucket_util, cbas_util, cbas_name_cardinality=1,
                 kv_name_cardinality=1, remote_datasets=False,
                 creation_methods=None, ds_per_collection=1,
                 ds_per_dv=None):
        super(CreateDatasetsTask, self).__init__(
            "CreateDatasetsOnAllCollectionsTask")
        self.cluster = cluster
        self.bucket_util = bucket_util
        self.cbas_name_cardinality = cbas_name_cardinality
        self.kv_name_cardinality = kv_name_cardinality
        self.remote_datasets = remote_datasets
        self.ds_per_collection = ds_per_collection if ds_per_collection >= 1 \
            else 1
        if not creation_methods:
            self.creation_methods = ["cbas_collection", "cbas_dataset",
                                     "enable_cbas_from_kv"]
        else:
            self.creation_methods = creation_methods
        if self.ds_per_collection > 1:
            self.creation_methods = list(filter(lambda method: method !=
                                                 'enable_cbas_from_kv',
                        self.creation_methods))
        self.cbas_util = cbas_util
        self.ds_per_dv = ds_per_dv
        if remote_datasets:
            self.remote_link_objs = self.cbas_util.list_all_link_objs(
                "couchbase")
            self.creation_methods.remove("enable_cbas_from_kv")
        self.created_datasets = []

    def call(self):
        self.start_task()
        try:
            for bucket in self.cluster.buckets:
                if self.kv_name_cardinality > 1:
                    for scope in self.bucket_util.get_active_scopes(bucket):
                        for collection in \
                                self.bucket_util.get_active_collections(
                                bucket, scope.name):
                            self.init_dataset_creation(
                                bucket, scope, collection)
                else:
                    scope = self.bucket_util.get_scope_obj(bucket, "_default")
                    self.init_dataset_creation(
                        bucket, scope, self.bucket_util.get_collection_obj(
                            scope, "_default"))
            self.set_result(True)
            self.complete_task()
        except Exception as e:
            self.test_log.error(e)
            self.set_exception(e)
        return self.result

    def dataset_present(self, dataset_name, dataverse_name):
        names_present = list(filter(
            lambda ds: ds.name == dataset_name and
            ds.dataverse_name == dataverse_name,
            self.created_datasets))
        if names_present:
            return True
        return False

    def init_dataset_creation(self, bucket, scope, collection):
        for _ in range(self.ds_per_collection):
            creation_method = random.choice(self.creation_methods)
            dataverse = None
            if self.remote_datasets:
                link_name = random.choice(self.remote_link_objs).full_name
            else:
                link_name = None

            name = self.cbas_util.generate_name(
                name_cardinality=1, max_length=3, fixed_length=True)

            if creation_method == "enable_cbas_from_kv":
                enabled_from_KV = True
                if bucket.name + "." + scope.name in \
                        self.cbas_util.dataverses.keys():
                    dataverse = self.cbas_util.dataverses[
                        bucket.name + "." + scope.name]
                else:
                    dataverse = Dataverse(bucket.name + "." + scope.name)
                name = CBASHelper.format_name(collection.name)
            else:
                enabled_from_KV = False
                dataverses = list(
                    filter(lambda dv: (self.ds_per_dv is None) or (len(
                        dv.datasets.keys()) < self.ds_per_dv),
                           self.cbas_util.dataverses.values()))
                if dataverses:
                    dataverse = random.choice(dataverses)
                if self.cbas_name_cardinality > 1 and not dataverse:
                    dataverse = Dataverse(self.cbas_util.generate_name(
                        self.cbas_name_cardinality - 1, max_length=3,
                        fixed_length=True))
                elif not dataverse:
                    dataverse = self.cbas_util.get_dataverse_obj("Default")

            while self.dataset_present(name, dataverse.name):
                name = self.cbas_util.generate_name(
                    name_cardinality=1, max_length=3, fixed_length=True)
            num_of_items = collection.num_items

            if creation_method == "cbas_collection":
                dataset_obj = CBAS_Collection(
                    name=name, dataverse_name=dataverse.name, link_name=link_name,
                    dataset_source="internal", dataset_properties={},
                    bucket=bucket, scope=scope, collection=collection,
                    enabled_from_KV=enabled_from_KV, num_of_items=num_of_items)
            else:
                dataset_obj = Dataset(
                    name=name, dataverse_name=dataverse.name,
                    dataset_source="internal", dataset_properties={},
                    bucket=bucket, scope=scope, collection=collection,
                    enabled_from_KV=enabled_from_KV, num_of_items=num_of_items,
                    link_name=link_name)
            if not self.create_dataset(dataset_obj):
                raise N1QLQueryException(
                    "Could not create dataset " + dataset_obj.name + " on " +
                    dataset_obj.dataverse_name)
            self.created_datasets.append(dataset_obj)
            if dataverse.name not in self.cbas_util.dataverses.keys():
                self.cbas_util.dataverses[dataverse.name] = dataverse
            self.cbas_util.dataverses[dataverse.name].datasets[
                self.created_datasets[-1].full_name] = self.created_datasets[-1]

    def create_dataset(self, dataset):
        dataverse_name = str(dataset.dataverse_name)
        if dataverse_name == "Default":
            dataverse_name = None
        if dataset.enabled_from_KV:
            if self.kv_name_cardinality > 1:
                return self.cbas_util.enable_analytics_from_KV(
                    self.cluster, dataset.full_kv_entity_name, False, False,
                    None, None, None, 120, 120)
            else:
                return self.cbas_util.enable_analytics_from_KV(
                    self.cluster, dataset.get_fully_qualified_kv_entity_name(1),
                    False, False, None, None, None, 120, 120)
        else:
            if isinstance(dataset, CBAS_Collection):
                analytics_collection = True
            elif isinstance(dataset, Dataset):
                analytics_collection = False
            if self.kv_name_cardinality > 1 and self.cbas_name_cardinality > 1:
                return self.cbas_util.create_dataset(
                    self.cluster, dataset.name, dataset.full_kv_entity_name,
                    dataverse_name, False, False, None, dataset.link_name, None,
                    False, None, None, None, 120, 120, analytics_collection)
            elif self.kv_name_cardinality > 1 and \
                    self.cbas_name_cardinality == 1:
                return self.cbas_util.create_dataset(
                    self.cluster, dataset.name, dataset.full_kv_entity_name,
                    None, False, False, None, dataset.link_name, None, False,
                    None, None, None, 120, 120, analytics_collection)
            elif self.kv_name_cardinality == 1 and \
                    self.cbas_name_cardinality > 1:
                return self.cbas_util.create_dataset(
                    self.cluster, dataset.name,
                    dataset.get_fully_qualified_kv_entity_name(1),
                    dataverse_name, False, False, None, dataset.link_name, None,
                    False, None, None, None, 120, 120, analytics_collection)
            else:
                return self.cbas_util.create_dataset(
                    self.cluster, dataset.name,
                    dataset.get_fully_qualified_kv_entity_name(1),
                    None, False, False, None, dataset.link_name, None, False,
                    None, None, None, 120, 120, analytics_collection)


class CreateSynonymsTask(Task):
    def __init__(self, cluster, cbas_util, cbas_entity, dataverse,
                 synonyms_per_entity=1, synonym_on_synonym=False, prefix=None):
        super(CreateSynonymsTask, self).__init__("CreateSynonymsTask")
        self.cluster = cluster
        self.cbas_util = cbas_util
        self.cbas_entity = cbas_entity
        self.dataverse = dataverse
        self.synonyms_per_entity = synonyms_per_entity
        self.synonym_on_synonym = synonym_on_synonym
        self.prefix = prefix

    def call(self):
        self.start_task()
        results = []
        try:
            for _ in range(self.synonyms_per_entity):
                name = self.cbas_util.generate_name(
                    name_cardinality=1, max_length=3, fixed_length=True)
                while name in \
                        self.cbas_util.dataverses[
                            self.dataverse.name].synonyms.keys():
                    name = self.cbas_util.generate_name(
                        name_cardinality=1, max_length=3, fixed_length=True)
                synonym = Synonym(
                    name=name, cbas_entity_name=self.cbas_entity.name,
                    cbas_entity_dataverse=self.cbas_entity.dataverse_name,
                    dataverse_name=self.dataverse.name,
                    synonym_on_synonym=self.synonym_on_synonym)
                if not self.cbas_util.create_analytics_synonym(
                    self.cluster, synonym.full_name,
                    synonym.cbas_entity_full_name, if_not_exists=False,
                    validate_error_msg=False, expected_error=None, username=None,
                    password=None, timeout=300, analytics_timeout=300):
                    results.append(False)
                else:
                    self.cbas_util.dataverses[self.cbas_entity.dataverse_name].\
                        synonyms[synonym.name] = synonym
                    results.append(True)
            if not all(results):
                raise Exception(
                    "Failed to create all the synonyms on " + \
                    self.cbas_entity.name)
        except Exception as e:
            self.set_exception(e)
            return
        self.complete_task()


class CreateCBASIndexesTask(Task):
    def __init__(self, cluster, cbas_util, dataset, indexes_per_dataset=1,
                 prefix=None, index_fields=[]):
        super(CreateCBASIndexesTask, self).__init__(
            "CreateCBASIndexesTask")
        self.cluster = cluster
        self.cbas_util = cbas_util
        self.indexes_per_dataset = indexes_per_dataset
        self.prefix = prefix
        if not index_fields:
            index_fields = ["name:STRING", "age:BIGINT", "body:STRING",
                            "mutation_type:STRING", "mutated:BIGINT"]
        self.index_fields = index_fields
        self.creation_methods = ["cbas_index", "index"]
        self.dataset = dataset

    def call(self):
        self.start_task()
        try:
            for i in range(self.indexes_per_dataset):

                name = self.cbas_util.generate_name(
                    name_cardinality=1, max_length=3, fixed_length=True)
                index = CBAS_Index(
                    name=name, dataset_name=self.dataset.name,
                    dataverse_name=self.dataset.dataverse_name,
                    indexed_fields=random.choice(self.index_fields))

                creation_method = random.choice(self.creation_methods)
                if creation_method == "cbas_index":
                    index.analytics_index = True
                else:
                    index.analytics_index = False
                if not self.cbas_util.create_cbas_index(
                    self.cluster, index_name=index.name,
                    indexed_fields=index.indexed_fields,
                    dataset_name=index.full_dataset_name,
                    analytics_index=index.analytics_index,
                    validate_error_msg=False, expected_error=None,
                    username=None, password=None, timeout=300, analytics_timeout=300):
                    raise Exception(
                        "Failed to create index {0} on {1}({2})".format(
                            index.name, index.full_dataset_name,
                            str(index.indexed_fields)))
                self.cbas_util.dataverses[
                    self.dataset.dataverse_name].datasets[
                    self.dataset.full_name].indexes[index.name] = index
        except Exception as e:
            self.set_exception(e)
            return
        self.complete_task()


class CreateUDFTask(Task):
    def __init__(self, cluster, cbas_util, udf, dataverse, body, referenced_entities=[],
                 parameters=[]):
        super(CreateUDFTask, self).__init__("CreateUDFTask")
        self.cluster = cluster
        self.cbas_util = cbas_util
        self.dataverse = dataverse
        self.body = body
        self.udf = udf
        self.referenced_entities = referenced_entities
        self.parameters = parameters

    def call(self):
        self.start_task()
        try:
            if not self.cbas_util.create_udf(
                self.cluster, name=self.udf, dataverse=self.dataverse.name,
                or_replace=False, parameters=self.parameters, body=self.body,
                if_not_exists=False, query_context=False, use_statement=False,
                validate_error_msg=False, expected_error=None, username=None,
                password=None, timeout=120, analytics_timeout=120):
                raise Exception(
                    "Couldn't create UDF {0} on dataverse {1}: def :{2}".format(
                        self.udf, self.dataverse.name, self.body))
            udf_obj = CBAS_UDF(
                name=self.udf, dataverse_name=self.dataverse.name, parameters=[],
                body=self.body, referenced_entities=self.referenced_entities)
            self.cbas_util.dataverses[
                self.dataverse.name].udfs[udf_obj.full_name] = udf_obj
        except Exception as e:
            self.set_exception(e)
            return
        self.complete_task()


class DropUDFTask(Task):
    def __init__(self, cluster, cbas_util, dataverse):
        super(DropUDFTask, self).__init__("DropUDFTask")
        self.cluster = cluster
        self.cbas_util = cbas_util
        self.dataverse = dataverse

    def call(self):
        self.start_task()
        try:
            for udf in self.dataverse.udfs.values():
                if not self.cbas_util.drop_udf(
                    self.cluster, name=udf.name, dataverse=self.dataverse.name,
                    parameters=udf.parameters, if_exists=False,
                    use_statement=False, query_context=False,
                    validate_error_msg=False, expected_error=None, username=None,
                    password=None, timeout=120, analytics_timeout=120):
                    raise Exception("Could not drop {0} on {1}: def :".format(
                        udf.name, self.dataverse.name, udf.body))
        except Exception as e:
            self.set_exception(e)
            return
        self.complete_task()


class DropCBASIndexesTask(Task):
    def __init__(self, cluster, cbas_util, dataset):
        super(DropCBASIndexesTask, self).__init__("DropCBASIndexesTask")
        self.cluster = cluster
        self.cbas_util = cbas_util
        self.dataset = dataset

    def call(self):
        self.start_task()
        try:
            for index in self.dataset.indexes.values():
                if not self.cbas_util.drop_cbas_index(
                    self.cluster, index_name=index.name,
                    dataset_name=index.full_dataset_name,
                    analytics_index=index.analytics_index,
                    timeout=120, analytics_timeout=120):
                    raise Exception("Failed to drop index {0} on {1}".format(
                        index.name, index.full_dataset_name))
                self.cbas_util.dataverses[
                    self.dataset.dataverse_name].datasets[
                    self.dataset.full_name].indexes.pop(index.name)
        except Exception as e:
            self.set_exception(e)
            return
        self.complete_task()


class DropSynonymsTask(Task):
    def __init__(self, cluster, cbas_util):
        super(DropSynonymsTask, self).__init__("DropSynonymsTask")
        self.cluster = cluster
        self.cbas_util = cbas_util

    def call(self):
        self.start_task()
        try:
            for dv_name, dataverse in self.cbas_util.dataverses.items():
                for synonym in dataverse.synonyms.values():
                    if not self.cbas_util.drop_analytics_synonym(
                        self.cluster, synonym_full_name=synonym.full_name,
                        if_exists=True, timeout=120, analytics_timeout=120):
                        raise Exception(
                            "Unable to drop synonym " + synonym.full_name)
                    self.cbas_util.dataverses[dataverse.name].synonyms.pop(
                        synonym.name, None)
        except Exception as e:
            self.set_exception(e)
            return
        self.complete_task()


class DropDatasetsTask(Task):
    def __init__(self, cluster, cbas_util, kv_name_cardinality=1):
        super(DropDatasetsTask, self).__init__(
            "DropDatasetsTask")
        self.cluster = cluster
        self.cbas_util = cbas_util
        self.kv_name_cardinality = kv_name_cardinality

    def call(self):
        self.start_task()
        try:
            for dv_name, dataverse in self.cbas_util.dataverses.items():
                for ds_name, dataset in dataverse.datasets.items():
                    if dataset.enabled_from_KV:
                        if self.kv_name_cardinality > 1:
                            if not self.cbas_util.disable_analytics_from_KV(
                                self.cluster, dataset.full_kv_entity_name):
                                raise Exception(
                                    "Unable to disable analytics on " + \
                                    dataset.full_kv_entity_name)
                        else:
                            if not self.cbas_util.disable_analytics_from_KV(
                                self.cluster,
                                dataset.get_fully_qualified_kv_entity_name(1)):
                                raise Exception(
                                    "Unable to disable analytics on " + \
                                    dataset.get_fully_qualified_kv_entity_name(
                                        1))
                    else:
                        if not self.cbas_util.drop_dataset(
                            self.cluster, dataset.full_name):
                            raise Exception(
                                "Unable to drop dataset " + dataset.full_name)
                    dataverse.datasets.pop(dataset.full_name)
        except Exception as e:
            self.set_exception(e)
            return
        self.complete_task()


class DropDataversesTask(Task):
    def __init__(self, cluster, cbas_util):
        super(DropDataversesTask, self).__init__(
            "DropDataversesTask")
        self.cbas_util = cbas_util
        self.cluster = cluster

    def call(self):
        self.start_task()
        try:
            for dataverse in self.cbas_util.dataverses.values():
                if dataverse.name != "Default":
                    if not self.cbas_util.drop_dataverse(
                        self.cluster, dataverse.name):
                        raise Exception(
                            "Unable to drop dataverse " + dataverse.name)
        except Exception as e:
            self.set_exception(e)
            return
        self.complete_task()

class ExecuteQueryTask(Task):
    def __init__(self, server, query, isIndexerQuery=False, bucket=None, indexName=None, timeout=600, retry=10):
        super(ExecuteQueryTask, self).__init__("ExecuteQueriesTask_%sstarted%s"
                                               % (query, time.time()))
        self.server = server
        self.query = query
        self.timeout = timeout
        self.isIndexerQuery = isIndexerQuery
        self.rest = RestConnection(server)
        self.bucket = bucket
        self.index_name = indexName
        self.timeout = timeout
        self.isIndexerQuery = isIndexerQuery
        self.retry = retry
    def call(self):
        self.start_task()
        indexer_rest = GsiHelper(self.server, self.log)
        isException = False
        for x in range(self.retry):
            isException = False
            try:
                self.log.info("starting call")
                contentType = 'application/x-www-form-urlencoded'
                connection = 'keep-alive'
                status, content, header = indexer_rest.execute_query(server=self.server, query=self.query,
                                                                     contentType=contentType,
                                                                     connection=connection, isIndexerQuery=self.isIndexerQuery)
                newContent = json.loads(content)
                self.log.debug("Status of the query {}".format(status))
                self.set_result(status)
                self.log.info("check isIndexQuery status"+str(self.isIndexerQuery))
                break
            except Exception as e:
                self.log.info("Got exception:{0} with index name {1}".format(str(e), self.index_name))
                isException = True

        if self.isIndexerQuery:
            self.log.info("Waiting for polling status:" + self.index_name)
            result = indexer_rest.polling_create_index_status(self.bucket, index=self.index_name,
                                                              timeout=self.timeout)
            self.set_result(result)
        if isException:
            self.log.info("Got exception, marking task status as fail")
            self.set_result(False)
        self.complete_task()
