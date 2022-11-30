"""
Created on Jan 10, 2022

@author: bharathgp
"""
import os

import Jython_tasks.task as jython_tasks
from couchbase_cli import CouchbaseCLI
from global_vars import logger
from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection
from cb_tools.cb_cli import CbCli

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

    def reset_cluster_nodes(self, cluster_util, cluster, async_run=False):
        tasks = list()
        try:
            for node in cluster.servers:
                task = jython_tasks.FunctionCallTask(
                    self.__reset_node, (cluster_util, cluster, node))
                self.jython_task_manager.schedule(task)
                tasks.append(task)
        except Exception as ex:
            self.log.critical(ex)

        if not async_run:
            for task in tasks:
                self.jython_task_manager.get_task_result(task)
        return tasks

    def async_enable_dp(self, server):
        task = jython_tasks.FunctionCallTask(self._enable_dp, [server])
        self.jython_task_manager.schedule(task)
        return task

    def async_enable_tls(self, server):
        task = jython_tasks.FunctionCallTask(self._enable_tls, [server])
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
        shell = RemoteMachineShellConnection(server)
        # Stop old instances of tcpdump if still running
        stop_tcp_cmd = "if [[ \"$(pgrep tcpdump)\" ]]; " \
                       "then kill -s TERM $(pgrep tcpdump); fi"
        _, _ = shell.execute_command(stop_tcp_cmd)
        shell.execute_command("rm -rf pcaps")
        shell.execute_command("rm -rf " + server.ip + "_pcaps.zip")
        shell.disconnect()

    def _collect_pcaps(self, server):
        shell = RemoteMachineShellConnection(server)
        # Create path for storing pcaps
        create_path = "mkdir -p pcaps"
        o, e = shell.execute_command(create_path)
        shell.log_command_output(o, e)
        # Install tcpdump command if it doesn't exist
        o, e = shell.execute_command("yum install -y tcpdump")
        shell.log_command_output(o, e)
        # Install screen command if it doesn't exist
        o, e = shell.execute_command("yum install -y screen")
        shell.log_command_output(o, e)
        # Execute the tcpdump command
        tcp_cmd = "screen -dmS test bash -c \"tcpdump -C 500 -W 10 " \
                  "-w pcaps/pack-dump-file.pcap  -i eth0 -s 0 tcp\""
        o, e = shell.execute_command(tcp_cmd)
        shell.log_command_output(o, e)
        shell.disconnect()

    def _fetch_pcaps(self, server, log_path, test_failed):
        remote_client = RemoteMachineShellConnection(server)
        # stop tcdump
        stop_tcp_cmd = "if [[ \"$(pgrep tcpdump)\" ]]; " \
                       "then kill -s TERM $(pgrep tcpdump); fi"
        o, e = remote_client.execute_command(stop_tcp_cmd)
        remote_client.log_command_output(o, e)
        if test_failed:
            # install zip unzip
            o, e = remote_client.execute_command(
                "yum install -y zip unzip")
            remote_client.log_command_output(o, e)
            # zip the pcaps folder
            zip_cmd = "zip -r " + server.ip + "_pcaps.zip pcaps"
            o, e = remote_client.execute_command(zip_cmd)
            remote_client.log_command_output(o, e)
            # transfer the zip file
            zip_file_copied = remote_client.get_file(
                "/root",
                os.path.basename(server.ip + "_pcaps.zip"),
                log_path)
            self.log.info(
                "%s node pcap zip copied on client : %s"
                % (server.ip, zip_file_copied))
            if zip_file_copied:
                # Remove the zips
                remote_client.execute_command("rm -rf "
                                              + server.ip + "_pcaps.zip")
        # Remove pcaps
        remote_client.execute_command("rm -rf pcaps")
        remote_client.disconnect()

    def _enable_dp(self, server):
        shell_conn = RemoteMachineShellConnection(server)
        cb_cli = CbCli(shell_conn)
        cb_cli.enable_dp()
        shell_conn.disconnect()

    def _enable_tls(self, server, level="strict"):
        RestConnection(server).update_autofailover_settings(False, 120)
        self.log.info(
            "Setting cluster encryption level to strict on cluster "
            "with node {0}".format(server))
        shell_conn = RemoteMachineShellConnection(server)
        cb_cli = CbCli(shell_conn)
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
        rest = RestConnection(node)
        try:
            if '.com' in node.ip or ':' in node.ip:
                _ = rest.update_autofailover_settings(False, 120)
                cli = CouchbaseCLI(node, node.rest_username,
                                   node.rest_password)
                output, err, result = cli.set_address_family("ipv6")
                if not result:
                    raise Exception("Addr family was not changed to ipv6")
                _ = rest.update_autofailover_settings(True, 120)
            else:
                # Start node
                data_path = rest.get_data_path()
                core_path = \
                    str(rest.get_data_path()).split("data")[0] + "crash/"
                if not os.path.isdir(core_path):
                    core_path = "/opt/couchbase/var/lib/couchbase/crash/"

                # Stop node
                cluster_util.stop_server(cluster, node)
                # Delete Path
                shell.cleanup_data_config(data_path)
                if not crash_warning:
                    shell.cleanup_data_config(core_path)

                cluster_util.start_server(cluster, node)
                if not RestConnection(node).is_ns_server_running():
                    self.log.error("%s ns_server not running" % node.ip)
        except Exception as e:
            self.log.critical(e)
        finally:
            shell.disconnect()
