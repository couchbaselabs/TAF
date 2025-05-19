"""
Created on Jan 10, 2022

@author: bharathgp
"""
import os

import Jython_tasks.task as jython_tasks
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from couchbase_cli import CouchbaseCLI
from global_vars import logger
from membase.api.rest_client import RestConnection
from cb_tools.cb_cli import CbCli
from shell_util.remote_connection import RemoteMachineShellConnection
from py_constants.cb_constants.CBServer import CbServer
"""
An API for scheduling tasks for performing node related operations.

This module is contains the top-level API's for scheduling and
executing
tasks related to individual nodes using Remotemachine shell commands.
The API provides a way to run task do syncronously and asynchronously.
"""


class NodeUtils(object):

    def __init__(self, task_manager):
        self.jython_task_manager = task_manager
        self.log = logger.get("infra")
        self.test_log = logger.get("test")
        self.log.debug("Initiating NodeTasks")

    def cleanup_pcaps(self, servers):
        tasks = []
        for server in servers:
            task = jython_tasks.FunctionCallTask(self._cleanup_pcaps,
                                                 [server])
            self.jython_task_manager.schedule(task)
            tasks.append(task)
        for task in tasks:
            self.jython_task_manager.get_task_result(task)

    def start_collect_pcaps(self, servers):
        tasks = []
        for server in servers:
            task = jython_tasks.FunctionCallTask(self._collect_pcaps,
                                                 [server])
            self.jython_task_manager.schedule(task)
            tasks.append(task)
        for task in tasks:
            self.jython_task_manager.get_task_result(task)

    def start_fetch_pcaps(self, servers, log_path, is_test_failed):
        tasks = []
        for server in servers:
            task = jython_tasks.FunctionCallTask(self._fetch_pcaps,
                                                 [server, log_path,
                                                  is_test_failed])
            self.jython_task_manager.schedule(task)
            tasks.append(task)
        for task in tasks:
            self.jython_task_manager.get_task_result(task)

    def reset_cluster_nodes(self, cluster_util, cluster):
        self.log.info("Resetting cluster_nodes")
        tasks = list()
        for node in cluster.servers:
            rest = ClusterRestAPI(node)
            version = rest.cluster_info()[1]["implementationVersion"][:3]
            if float(version) == 0.0 or float(version) >= 7.6 or node.type == "columnar":
                status, _ = rest.reset_node()
                if not status:
                    raise Exception(f"Reset node {node.ip} failed")
                if '.com' in node.ip:
                    rest.rename_node(node.ip)
            else:
                # This is the old reset node code for upgrade scenarios
                task = jython_tasks.FunctionCallTask(
                    self.__reset_node, (cluster_util, cluster, node))
                self.jython_task_manager.schedule(task)
                tasks.append(task)
        for task in tasks:
            self.jython_task_manager.get_task_result(task)
        return tasks

    def async_enable_dp(self, server):
        task = jython_tasks.FunctionCallTask(self._enable_dp, [server])
        self.jython_task_manager.schedule(task)
        return task

    def async_enable_tls(self, server, level):
        task = jython_tasks.FunctionCallTask(self._enable_tls, [server, level])
        self.jython_task_manager.schedule(task)
        return task

    def async_disable_tls(self, server):
        task = jython_tasks.FunctionCallTask(self._disable_tls,
                                             [server])
        self.jython_task_manager.schedule(task)
        return task

    def async_get_trace(self, server, get_trace):
        task = jython_tasks.FunctionCallTask(self._get_trace,
                                             [server, get_trace])
        self.jython_task_manager.schedule(task)
        return task

    def _cleanup_pcaps(self, server):
        remote_client = RemoteMachineShellConnection(server)
        remote_client.cleanup_pcaps()
        remote_client.disconnect()

    def _collect_pcaps(self, server):
        remote_client = RemoteMachineShellConnection(server)
        remote_client.collect_pcaps()
        remote_client.disconnect()

    def _fetch_pcaps(self, server, log_path, test_failed):
        remote_client = RemoteMachineShellConnection(server)
        remote_client.fetch_pcaps(test_failed, log_path)
        remote_client.disconnect()

    def _enable_dp(self, server):
        shell_conn = RemoteMachineShellConnection(server)
        cb_cli = CbCli(shell_conn)
        cb_cli.enable_dp()
        shell_conn.disconnect()

    def _enable_tls(self, server, level="strict"):
        shell_conn = RemoteMachineShellConnection(server)
        cb_cli = CbCli(shell_conn)
        o = cb_cli.auto_failover(enable_auto_fo=0)
        self.log.info("%s: %s" % (server.ip, o))
        self.log.info(
            "Setting cluster encryption level to strict on cluster "
            "with node {0}".format(server))
        o = cb_cli.enable_n2n_encryption()
        self.log.info(o)
        o = cb_cli.set_n2n_encryption_level(level=level)
        self.log.info(o)
        shell_conn.disconnect()

    def _disable_tls(self, server):
        shell_conn = RemoteMachineShellConnection(server)
        cb_cli = CbCli(shell_conn)
        o = cb_cli.auto_failover(enable_auto_fo=0)
        self.log.info("%s: %s" % (server.ip, o))
        o = cb_cli.set_n2n_encryption_level(level="control")
        self.log.info("%s: %s" % (server.ip, o))
        o = cb_cli.disable_n2n_encryption()
        self.log.info("%s: %s" % (server.ip, o))
        shell_conn.disconnect()

    def _get_trace(self, server, get_trace):
        shell = RemoteMachineShellConnection(server)
        output, _ = shell.execute_command("ps -aef|grep %s" % get_trace)
        output = shell.execute_command("pstack %s"
                                       % output[0].split()[1].strip())
        self.log.debug(output[0])
        shell.disconnect()

    def __reset_node(self, cluster_util, cluster, node, crash_warning=False):
        shell = RemoteMachineShellConnection(node)
        rest = ClusterRestAPI(node)
        try:
            if '.com' in node.ip or ':' in node.ip:
                _, _ = rest.update_auto_failover_settings("false", 120)
                cli = CouchbaseCLI(node, node.rest_username,
                                   node.rest_password)
                output, err, result = cli.set_address_family("ipv6")
                if not result:
                    raise Exception("Addr family was not changed to ipv6")
                _, _ = rest.update_auto_failover_settings("true", 120)
            else:
                # Start node
                data_path = cluster_util.fetch_data_path(node)
                core_path = \
                    str(data_path).split("data")[0] + "crash/"
                if not os.path.isdir(core_path):
                    core_path = "/opt/couchbase/var/lib/couchbase/crash/"

                # Stop node
                cluster_util.stop_server(cluster, node)
                # Delete Path
                shell.cleanup_data_config(data_path)
                if not crash_warning:
                    shell.cleanup_data_config(core_path)

                cluster_util.start_server(cluster, node)
                if not cluster_util.is_ns_server_running(node):
                    self.log.error("%s ns_server not running" % node.ip)
        except Exception as e:
            self.log.critical(e)
        finally:
            shell.disconnect()
