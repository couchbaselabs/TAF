"""
Created on Sep 26, 2017

@author: riteshagarwal
"""

import copy
import json
import re
import time
import os

import testconstants

from Cb_constants import constants
from Jython_tasks.task import MonitorActiveTask
from TestInput import TestInputSingleton, TestInputServer
from cb_tools.cb_collectinfo import CbCollectInfo
from common_lib import sleep
from couchbase_cli import CouchbaseCLI
from global_vars import logger
from membase.api.rest_client import RestConnection, RestHelper
from remote.remote_util import RemoteMachineShellConnection, RemoteUtilHelper
from table_view import TableView


class CBCluster:
    def __init__(self, name="default", username="Administrator",
                 password="password", paths=None, servers=None):
        self.name = name
        self.username = username
        self.password = password
        self.paths = paths
        self.master = servers[0]
        self.ram_settings = dict()
        self.servers = servers
        self.kv_nodes = list()
        self.fts_nodes = list()
        self.cbas_nodes = list()
        self.index_nodes = list()
        self.query_nodes = list()
        self.eventing_nodes = list()
        self.backup_nodes = list()
        self.nodes_in_cluster = list()
        self.xdcr_remote_clusters = list()

    def __str__(self):
        return "Couchbase Cluster: %s, Nodes: %s" % (
            self.name, ', '.join([s.ip for s in self.servers]))

    def update_master_using_diag_eval(self, node_in_cluster=None):
        if node_in_cluster is None:
            node_in_cluster = self.master

        shell = RemoteMachineShellConnection(node_in_cluster)
        shell.enable_diag_eval_on_non_local_hosts()
        shell.disconnect()

        rest = RestConnection(node_in_cluster)
        status, content = rest.diag_eval("mb_master:master_node().")

        master_ip = content.split("@")[1].replace("\\", '').replace("'", "")
        self.master = [server for server in self.servers
                       if server.ip == master_ip][0]
        return status, content


class ClusterUtils:
    def __init__(self, cluster, task_manager):
        self.input = TestInputSingleton.input
        self.cluster = cluster
        self.task_manager = task_manager
        self.rest = RestConnection(cluster.master)
        self.vbuckets = self.input.param("vbuckets", 1024)
        self.upr = self.input.param("upr", None)
        self.log = logger.get("test")

    def find_orchestrator(self, node=None):
        """
        Update the orchestrator of the cluster
        :param node: Any node that is still part of the cluster or master
        :return:
        """
        retry_index = 0
        max_retry = 12
        orchestrator_node = None
        status = None
        node = self.cluster.master if node is None else node

        rest = RestConnection(node)
        while retry_index < max_retry:
            status, content = rest.get_terse_cluster_info()
            json_content = json.loads(content)
            orchestrator_node = json_content["orchestrator"]
            if orchestrator_node == "undefined":
                sleep(1, message="orchestrator='undefined'", log_type="test")
            else:
                break
        orchestrator_node = \
            orchestrator_node.split("@")[1] \
            .replace("\\", '').replace("'", "")

        self.cluster.master = [server for server in self.cluster.servers
                               if server.ip == orchestrator_node][0]
        # Type cast to str - match the previous dial/eval return value
        content = "ns_1@%s" % self.cluster.master.ip
        self.rest = RestConnection(self.cluster.master)
        return status, content

    def set_metadata_purge_interval(self, interval=0.04):
        # set it to 0.04 i.e. 1 hour if not given
        return self.rest.set_metadata_purge_interval(interval)

    def set_rebalance_moves_per_nodes(self, rebalanceMovesPerNode=4):
        body = dict()
        body["rebalanceMovesPerNode"] = rebalanceMovesPerNode
        self.rest.set_rebalance_settings(body)
        result = self.rest.get_rebalance_settings()
        self.log.info("Changed Rebalance settings: {0}".format(json.loads(result)))

    def cluster_cleanup(self, bucket_util):
        rest = RestConnection(self.cluster.master)
        if rest._rebalance_progress_status() == 'running':
            self.kill_memcached()
            self.log.warning("Rebalance still running, "
                             "test should be verified")
            stopped = rest.stop_rebalance()
            if not stopped:
                raise Exception("Unable to stop rebalance")
        bucket_util.delete_all_buckets(self.cluster.servers)
        self.cleanup_cluster(self.cluster.servers, master=self.cluster.master)
        self.wait_for_ns_servers_or_assert(self.cluster.servers)

    # wait_if_warmup=True is useful in tearDown method for (auto)failover tests
    def wait_for_ns_servers_or_assert(self, servers, wait_time=360):
        for server in servers:
            rest = RestConnection(server)
            self.log.debug("Waiting for ns_server @ {0}:{1}"
                           .format(server.ip, server.port))
            if RestHelper(rest).is_ns_server_running(wait_time):
                self.log.debug("ns_server @ {0}:{1} is running"
                               .format(server.ip, server.port))
            else:
                self.log.error("ns_server {0} is not running in {1} sec"
                               .format(server.ip, wait_time))
                return False
            return True

    def cleanup_cluster(self, wait_for_rebalance=True, master=None):
        if master is None:
            master = self.cluster.master
        rest = RestConnection(master)
        rest.remove_all_replications()
        rest.remove_all_remote_clusters()
        rest.remove_all_recoveries()
        helper = RestHelper(rest)
        helper.is_ns_server_running(timeout_in_seconds=testconstants.NS_SERVER_TIMEOUT)
        nodes = rest.node_statuses()
        master_id = rest.get_nodes_self().id
        for node in nodes:
            if int(node.port) in xrange(9091, 9991):
                rest.eject_node(node)
                nodes.remove(node)

        if len(nodes) > 1:
            self.log.debug("Rebalancing all nodes in order to remove nodes")
            rest.log_client_error("Starting rebalance from test, ejected nodes %s" %
                                  [node.id for node in nodes if node.id != master_id])
            removed = helper.remove_nodes(
                knownNodes=[node.id for node in nodes],
                ejectedNodes=[node.id for node in nodes
                              if node.id != master_id],
                wait_for_rebalance=wait_for_rebalance)
            success_cleaned = []
            for removed in [node for node in nodes if (node.id != master_id)]:
                removed.rest_password = self.cluster.master.rest_password
                removed.rest_username = self.cluster.master.rest_username
                try:
                    rest = RestConnection(removed)
                except Exception as ex:
                    self.log.error("Can't create rest connection after "
                                   "rebalance out for ejected nodes, will "
                                   "retry after 10 seconds according to "
                                   "MB-8430: {0}".format(ex))
                    sleep(10, "MB-8430")
                    rest = RestConnection(removed)
                start = time.time()
                while time.time() - start < 30:
                    if len(rest.get_pools_info()["pools"]) == 0:
                        success_cleaned.append(removed)
                        break
                    sleep(1)
                if time.time() - start > 10:
                    self.log.error("'pools' on node {0}:{1} - {2}"
                                   .format(removed.ip, removed.port,
                                           rest.get_pools_info()["pools"]))
            for node in set([node for node in nodes if (node.id != master_id)]) - set(success_cleaned):
                self.log.error("Node {0}:{1} was not cleaned after "
                               "removing from cluster"
                               .format(removed.ip, removed.port))
                try:
                    rest = RestConnection(node)
                    rest.force_eject_node()
                except Exception as ex:
                    self.log.error("Force_eject_node {0}:{1} failed: {2}"
                                   .format(removed.ip, removed.port, ex))
            if len(set([node for node in nodes if (node.id != master_id)])\
                    - set(success_cleaned)) != 0:
                raise Exception("Not all ejected nodes were cleaned successfully")

            self.log.debug("Removed all the nodes from cluster associated with {0} ? {1}"
                           .format(self.cluster.master,
                                   [(node.id, node.port) for node in nodes if (node.id != master_id)]))

    def get_nodes_in_cluster(self, master_node=None):
        rest = None
        if master_node is None:
            rest = RestConnection(self.cluster.master)
        else:
            rest = RestConnection(master_node)
        nodes = rest.node_statuses()
        server_set = []
        for node in nodes:
            for server in self.cluster.servers:
                if server.ip == node.ip:
                    server_set.append(server)
        return server_set

    def change_checkpoint_params(self):
        self.chk_max_items = self.input.param("chk_max_items", None)
        self.chk_period = self.input.param("chk_period", None)
        if self.chk_max_items or self.chk_period:
            for server in self.cluster.servers:
                rest = RestConnection(server)
                if self.chk_max_items:
                    rest.set_chk_max_items(self.chk_max_items)
                if self.chk_period:
                    rest.set_chk_period(self.chk_period)

    def change_password(self, new_password="new_password"):
        nodes = RestConnection(self.cluster.master).node_statuses()

        cli = CouchbaseCLI(self.cluster.master,
                           self.cluster.master.rest_username,
                           self.cluster.master.rest_password)
        output, err, result = cli.setting_cluster(
            data_ramsize=False, index_ramsize=False, fts_ramsize=False,
            cluster_name=None, cluster_username=None,
            cluster_password=new_password, cluster_port=False)

        # MB-10136 & MB-9991
        if not result:
            raise Exception("Password didn't change!")
        self.log.debug("New password '%s' on nodes: %s"
                       % (new_password, [node.ip for node in nodes]))
        for node in nodes:
            for server in self.cluster.servers:
                if server.ip == node.ip and int(server.port) == int(node.port):
                    server.rest_password = new_password
                    break

    def change_port(self, new_port="9090", current_port='8091'):
        nodes = RestConnection(self.cluster.master).node_statuses()
        remote_client = RemoteMachineShellConnection(self.cluster.master)
        options = "--cluster-port=%s" % new_port
        cli_command = "cluster-edit"
        output, error = remote_client.execute_couchbase_cli(
            cli_command=cli_command, options=options,
            cluster_host="localhost:%s" % current_port,
            user=self.cluster.master.rest_username,
            password=self.cluster.master.rest_password)
        self.log.debug(output)
        remote_client.disconnect()
        # MB-10136 & MB-9991
        if error:
            raise Exception("Port didn't change! %s" % error)
        self.port = new_port
        self.log.debug("New port '%s' on nodes: %s"
                       % (new_port, [node.ip for node in nodes]))
        for node in nodes:
            for server in self.cluster.servers:
                if server.ip == node.ip and int(server.port) == int(node.port):
                    server.port = new_port
                    break

    def change_env_variables(self):
        for server in self.cluster.servers:
            remote_client = RemoteMachineShellConnection(server)
            vb_on_node, _ = remote_client.execute_command("grep ^COUCHBASE_NUM_VBUCKETS \
            /opt/couchbase/bin/couchbase-server | cut -d \"=\" -f 2",)
            self.log.debug("Current vBuckets on node %s: %s" % (server, vb_on_node))
            if vb_on_node:
                vb_on_node = int(vb_on_node[0])
            else:
                vb_on_node = 1024
            if self.vbuckets != vb_on_node or self.upr is not None:
                env_dict = {}
                if self.vbuckets:
                    env_dict["COUCHBASE_NUM_VBUCKETS"] = self.vbuckets
                if self.upr is not None:
                    if self.upr:
                        env_dict["COUCHBASE_REPL_TYPE"] = "upr"
                    else:
                        env_dict["COUCHBASE_REPL_TYPE"] = "tap"
                if len(env_dict) >= 1:
                    remote_client.change_env_variables(env_dict)
            remote_client.disconnect()
            self.log.debug("========= CHANGED ENVIRONMENT SETTING ===========")

        self.log.debug("Wait for all the services to come up after "
                       "change_env_vars update")
        sleep(10, log_type="infra")

    def reset_env_variables(self):
        if self.upr is not None:
            for server in self.cluster.servers:
                if self.upr:
                    remote_client = RemoteMachineShellConnection(server)
                    remote_client.reset_env_variables()
                    remote_client.disconnect()
            self.log.debug("==== RESET ENVIRONMENT SETTING TO ORIGINAL ====")

    def change_log_info(self):
        for server in self.cluster.servers:
            remote_client = RemoteMachineShellConnection(server)
            remote_client.stop_couchbase()
            remote_client.change_log_level(self.log_info)
            remote_client.start_couchbase()
            remote_client.disconnect()
        self.log.debug("========= CHANGED LOG LEVELS ===========")

    def change_log_location(self):
        for server in self.cluster.servers:
            remote_client = RemoteMachineShellConnection(server)
            remote_client.stop_couchbase()
            remote_client.configure_log_location(self.log_location)
            remote_client.start_couchbase()
            remote_client.disconnect()
        self.log.debug("========= CHANGED LOG LOCATION ===========")

    def change_stat_info(self):
        for server in self.cluster.servers:
            remote_client = RemoteMachineShellConnection(server)
            remote_client.stop_couchbase()
            remote_client.change_stat_periodicity(self.stat_info)
            remote_client.start_couchbase()
            remote_client.disconnect()
        self.log.debug("========= CHANGED STAT PERIODICITY ===========")

    def change_port_info(self):
        for server in self.cluster.servers:
            remote_client = RemoteMachineShellConnection(server)
            remote_client.stop_couchbase()
            remote_client.change_port_static(self.port_info)
            server.port = self.port_info
            self.log.debug("New REST port %s" % server.port)
            remote_client.start_couchbase()
            remote_client.disconnect()
        self.log.debug("========= CHANGED ALL PORTS ===========")

    def force_eject_nodes(self):
        for server in self.cluster.servers:
            if server != self.cluster.servers[0]:
                try:
                    rest = RestConnection(server)
                    rest.force_eject_node()
                except BaseException, e:
                    self.log.error(e)

    def set_upr_flow_control(self, flow=True, servers=[]):
        servers = self.get_kv_nodes(servers)
        for bucket in self.buckets:
            for server in servers:
                rest = RestConnection(server)
                rest.set_enable_flow_control(bucket=bucket.name, flow=flow)
        for server in self.cluster.servers:
            remote_client = RemoteMachineShellConnection(server)
            remote_client.stop_couchbase()
            remote_client.start_couchbase()
            remote_client.disconnect()

    def kill_memcached(self):
        for server in self.cluster.servers:
            remote_client = RemoteMachineShellConnection(server)
            remote_client.kill_memcached()
            remote_client.disconnect()

    def kill_prometheus(self):
        for server in self.cluster.servers:
            remote_client = RemoteMachineShellConnection(server)
            remote_client.kill_prometheus()
            remote_client.disconnect()

    def get_nodes(self, server):
        """ Get Nodes from list of server """
        rest = RestConnection(self.cluster.master)
        nodes = rest.get_nodes()
        return nodes

    def stop_server(self, node):
        """ Method to stop a server which is subject to failover """
        for server in self.cluster.servers:
            if server.ip == node.ip:
                shell = RemoteMachineShellConnection(server)
                if shell.is_couchbase_installed():
                    shell.stop_couchbase()
                    self.log.debug("Couchbase stopped on {0}".format(server))
                else:
                    shell.stop_membase()
                    self.log.debug("Membase stopped on {0}".format(server))
                shell.disconnect()
                break

    def start_server(self, node):
        """ Method to start a server which is subject to failover """
        for server in self.cluster.servers:
            if server.ip == node.ip:
                shell = RemoteMachineShellConnection(server)
                if shell.is_couchbase_installed():
                    shell.start_couchbase()
                    self.log.debug("Couchbase started on {0}".format(server))
                else:
                    shell.start_membase()
                    self.log.debug("Membase started on {0}".format(server))
                shell.disconnect()
                break

    def kill_server_memcached(self, node):
        """ Method to start a server which is subject to failover """
        for server in self.cluster.servers:
            if server.ip == node.ip:
                remote_client = RemoteMachineShellConnection(server)
                remote_client.kill_memcached()
                remote_client.disconnect()

    def start_memcached_on_node(self, node):
        """ Method to start memcached on a server which is subject to failover """
        for server in self.cluster.servers:
            if server.ip == node.ip:
                remote_client = RemoteMachineShellConnection(server)
                remote_client.start_memcached()
                remote_client.disconnect()

    def stop_memcached_on_node(self, node):
        """ Method to stop memcached on a server which is subject to failover """
        for server in self.cluster.servers:
            if server.ip == node.ip:
                remote_client = RemoteMachineShellConnection(server)
                remote_client.stop_memcached()
                remote_client.disconnect()


    def start_firewall_on_node(self, node):
        """ Method to start a server which is subject to failover """
        for server in self.cluster.servers:
            if server.ip == node.ip:
                RemoteUtilHelper.enable_firewall(server)

    def stop_firewall_on_node(self, node):
        """ Method to start a server which is subject to failover """
        for server in self.cluster.servers:
            if server.ip == node.ip:
                remote_client = RemoteMachineShellConnection(server)
                remote_client.disable_firewall()
                remote_client.disconnect()

    def reset_cluster(self, crash_warning=False):
        try:
            for node in self.cluster.servers:
                shell = RemoteMachineShellConnection(node)
                # Start node
                rest = RestConnection(node)
                data_path = rest.get_data_path()
                core_path = str(rest.get_data_path()).split("data")[0] + "crash/"
                if not os.path.isdir(core_path):
                    core_path = "/opt/couchbase/var/lib/couchbase/crash/"

                # Stop node
                self.stop_server(node)
                # Delete Path
                shell.cleanup_data_config(data_path)
                if not crash_warning:
                    shell.cleanup_data_config(core_path)

                self.start_server(node)
                # TODO : this part of the code doesnt work anymore for IPv6
                # basically node doesnt come up after restart
                # if '.com' in node.ip or ':' in node.ip:
                #     shell.update_dist_type()
                shell.disconnect()
            # Wait after reset of cluster nodes
            sleep(10)
        except Exception, ex:
            self.log.error(ex)

    def update_cluster_nodes_service_list(self, cluster):
        def append_nodes_to_list(nodes, list_to_append):
            for t_node in nodes:
                t_node = t_node.split(":")
                for server in cluster.servers:
                    if server.ip == t_node[0] and server.port == t_node[1]:
                        list_to_append.append(server)
                        break

        service_map = self.get_services_map(cluster.master)
        cluster.kv_nodes = list()
        cluster.fts_nodes = list()
        cluster.cbas_nodes = list()
        cluster.index_nodes = list()
        cluster.query_nodes = list()
        cluster.eventing_nodes = list()
        cluster.backup_nodes = list()
        for service_type, node_list in service_map.items():
            if service_type == constants.Services.KV:
                append_nodes_to_list(node_list, cluster.kv_nodes)
            elif service_type == constants.Services.INDEX:
                append_nodes_to_list(node_list, cluster.index_nodes)
            elif service_type == constants.Services.N1QL:
                append_nodes_to_list(node_list, cluster.query_nodes)
            elif service_type == constants.Services.CBAS:
                append_nodes_to_list(node_list, cluster.cbas_nodes)
            elif service_type == constants.Services.EVENTING:
                append_nodes_to_list(node_list, cluster.eventing_nodes)
            elif service_type == constants.Services.FTS:
                append_nodes_to_list(node_list, cluster.fts_nodes)
            elif service_type == constants.Services.BACKUP:
                append_nodes_to_list(node_list, cluster.backup_nodes)

    def get_nodes_from_services_map(self, service_type="n1ql",
                                    get_all_nodes=False, servers=None,
                                    master=None):
        if not servers:
            servers = self.cluster.servers
        if not master:
            master = self.cluster.master
        services_map = self.get_services_map(master=master)
        if service_type not in services_map:
            self.log.warning("Cannot find service node {0} in cluster "
                             .format(service_type))
        else:
            node_list = []
            for server_info in services_map[service_type]:
                tokens = server_info.rsplit(":", 1)
                ip = tokens[0]
                port = int(tokens[1])
                for server in servers:
                    """ In tests use hostname, if IP in ini file use IP, we
                        need to convert it to hostname to compare it with
                        hostname in cluster """
                    if "couchbase.com" in ip and "couchbase.com" not in server.ip:
                        shell = RemoteMachineShellConnection(server)
                        hostname = shell.get_full_hostname()
                        self.log.debug("convert IP: {0} to hostname: {1}"
                                       .format(server.ip, hostname))
                        server.ip = hostname
                        shell.disconnect()
                    elif "couchbase.com" in server.ip and "couchbase.com" not in ip:
                        node = TestInputServer()
                        node.ip = ip
                        """ match node.ip to server in ini file to get correct credential """
                        for server in servers:
                            shell = RemoteMachineShellConnection(server)
                            ips = shell.get_ip_address()
                            ips_new = []
                            for ele in ips:
                                ele = ele.replace('\n', '')
                                ips_new.append(ele)
                            if node.ip in ips_new:
                                node.ssh_username = server.ssh_username
                                node.ssh_password = server.ssh_password
                                break

                        shell = RemoteMachineShellConnection(node)
                        hostname = shell.get_full_hostname()
                        self.log.info("convert IP: {0} to hostname: {1}" \
                                      .format(ip, hostname))
                        ip = hostname
                        shell.disconnect()
                    if (port != constants.port and port == int(server.port)) \
                            or (port == constants.port and server.ip == ip):
                        node_list.append(server)
            self.log.debug("All nodes in cluster: {0}".format(node_list))
            if get_all_nodes:
                return node_list
            else:
                return node_list[0]

    def get_services_map(self, reset=True, master=None):
        if not reset:
            return
        else:
            services_map = {}
        if not master:
            master = self.cluster.master
        rest = RestConnection(master)
        tem_map = rest.get_nodes_services()
        for key, val in tem_map.iteritems():
            for service in val:
                if service not in services_map.keys():
                    services_map[service] = []
                services_map[service].append(key)
        return services_map

    def get_services(self, tgt_nodes, tgt_services, start_node=1):
        services = []
        if tgt_services is None:
            for node in tgt_nodes[start_node:]:
                if node.services is not None and node.services != '':
                    services.append(node.services)
            if len(services) > 0:
                return services
            else:
                return None
        elif "-" in tgt_services:
            services = tgt_services.replace(":", ",").split("-")[start_node:]
        else:
            for node in range(start_node, len(tgt_nodes)):
                services.append(tgt_services.replace(":", ","))
        return services

    def generate_map_nodes_out_dist(self, nodes_out_dist=None,
                                    targetMaster=False,
                                    targetIndexManager=False):
        if nodes_out_dist is None:
            nodes_out_dist = []
        index_nodes_out = []
        nodes_out_list = []
        services_map = self.get_services_map(reset=True)
        if not nodes_out_dist:
            if len(self.cluster.servers) > 1:
                nodes_out_list.append(self.cluster.servers[1])
            return nodes_out_list, index_nodes_out
        for service_fail_map in nodes_out_dist.split("-"):
            tokens = service_fail_map.rsplit(":", 1)
            count = 0
            service_type = tokens[0]
            service_type_count = int(tokens[1])
            compare_string_master = "{0}:{1}".format(self.cluster.master.ip,
                                                     self.cluster.master.port)
            compare_string_index_manager = "{0}:{1}" \
                                           .format(self.cluster.master.ip,
                                                   self.cluster.master.port)
            if service_type in services_map.keys():
                for node_info in services_map[service_type]:
                    for server in self.cluster.servers:
                        compare_string_server = "{0}:{1}".format(server.ip,
                                                                 server.port)
                        addNode = False
                        if (targetMaster and (not targetIndexManager)) \
                                and (compare_string_server == node_info
                                     and compare_string_master == compare_string_server):
                            addNode = True
                            self.master = self.cluster.servers[1]
                        elif ((not targetMaster) and (not targetIndexManager)) \
                                and (compare_string_server == node_info
                                     and compare_string_master != compare_string_server
                                     and compare_string_index_manager != compare_string_server):
                            addNode = True
                        elif ((not targetMaster) and targetIndexManager) \
                                and (compare_string_server == node_info
                                     and compare_string_master != compare_string_server
                                     and compare_string_index_manager == compare_string_server):
                            addNode = True
                        if addNode and (server not in nodes_out_list) \
                                and count < service_type_count:
                            count += 1
                            if service_type == "index":
                                if server not in index_nodes_out:
                                    index_nodes_out.append(server)
                            nodes_out_list.append(server)
        return nodes_out_list, index_nodes_out

    def setDebugLevel(self, index_servers=None, service_type="kv"):
        index_debug_level = self.input.param("index_debug_level", None)
        if index_debug_level is None:
            return
        if index_servers is None:
            index_servers = self.get_nodes_from_services_map(service_type=service_type, get_all_nodes=True)
        json = {
            "indexer.settings.log_level": "debug",
            "projector.settings.log_level": "debug",
        }
        for server in index_servers:
            RestConnection(server).set_index_settings(json)

    def _version_compatability(self, compatible_version):
        rest = RestConnection(self.cluster.master)
        versions = rest.get_nodes_versions()
        for version in versions:
            if compatible_version <= version:
                return True
        return False

    def get_kv_nodes(self, servers=None, master=None):
        if not master:
            master = self.cluster.master
        rest = RestConnection(master)
        versions = rest.get_nodes_versions()
        if servers is None:
            servers = self.cluster.servers
        kv_servers = self.get_nodes_from_services_map(service_type="kv",
                                                      get_all_nodes=True,
                                                      servers=servers,
                                                      master=master)
        new_servers = []
        for server in servers:
            for kv_server in kv_servers:
                if kv_server.ip == server.ip \
                        and kv_server.port == server.port \
                        and server not in new_servers:
                    new_servers.append(server)
        return new_servers

    def get_protocol_type(self):
        rest = RestConnection(self.cluster.master)
        versions = rest.get_nodes_versions()
        for version in versions:
            if "3" > version:
                return "tap"
        return "dcp"

    def generate_services_map(self, nodes, services=None):
        service_map = dict()
        index = 0
        if services is None:
            return service_map
        for node in nodes:
            service_map[node.ip] = node.services
            index += 1
        return service_map

    def find_node_info(self, master, node):
        """ Get Nodes from list of server """
        target_node = None
        rest = RestConnection(master)
        nodes = rest.get_nodes()
        for server in nodes:
            if server.ip == node.ip:
                target_node = server
        return target_node

    def add_remove_servers(self, servers=[], server_list=[], remove_list=[],
                           add_list=[]):
        """ Add or Remove servers from server_list """
        initial_list = copy.deepcopy(server_list)
        for add_server in add_list:
            for server in self.cluster.servers:
                if add_server is not None and server.ip == add_server.ip:
                    initial_list.append(add_server)
        for remove_server in remove_list:
            for server in initial_list:
                if remove_server is not None and server.ip == remove_server.ip:
                    initial_list.remove(server)
        return initial_list

    def add_all_nodes_then_rebalance(self, nodes, wait_for_completion=True):
        otpNodes = []
        if len(nodes) >= 1:
            for server in nodes:
                '''
                This is the case, master node is running cbas service as well
                '''
                if self.cluster.master.ip != server.ip:
                    otpNodes.append(self.rest.add_node(
                        user=server.rest_username,
                        password=server.rest_password,
                        remoteIp=server.ip, port=constants.port,
                        services=server.services.split(",")))

            self.rebalance(wait_for_completion)
        else:
            self.log.warning("No Nodes provided to add in cluster")

        return otpNodes

    def rebalance(self, wait_for_completion=True, ejected_nodes=[]):
        nodes = self.rest.node_statuses()
        started = self.rest.rebalance(otpNodes=[node.id for node in nodes],
                                      ejectedNodes=ejected_nodes)
        if started and wait_for_completion:
            result = self.rest.monitorRebalance()
            self.log.debug("Successfully rebalanced cluster {0}".format(result))
        else:
            result = started
        return result

    def remove_all_nodes_then_rebalance(self, otpnodes=None, rebalance=True):
        return self.remove_node(otpnodes, rebalance)

    def add_node(self, node=None, services=None, rebalance=True,
                 wait_for_rebalance_completion=True):
        if not node:
            self.fail("There is no node to add to cluster.")
        if not services:
            services = node.services.split(",")
        otpnode = self.rest.add_node(user=node.rest_username,
                                     password=node.rest_password,
                                     remoteIp=node.ip, port=constants.port,
                                     services=services)
        if rebalance:
            self.rebalance(wait_for_completion=wait_for_rebalance_completion)
        return otpnode

    def remove_node(self, otpnode=None, wait_for_rebalance=True):
        nodes = self.rest.node_statuses()
        '''This is the case when master node is running cbas service as well'''
        if len(nodes) <= len(otpnode):
            return

        helper = RestHelper(self.rest)
        try:
            removed = helper.remove_nodes(
                knownNodes=[node.id for node in nodes],
                ejectedNodes=[node.id for node in otpnode],
                wait_for_rebalance=wait_for_rebalance)
        except Exception as e:
            self.log.error("First time rebalance failed on Removal. "
                           "Wait and try again. THIS IS A BUG.")
            sleep(5)
            removed = helper.remove_nodes(
                knownNodes=[node.id for node in nodes],
                ejectedNodes=[node.id for node in otpnode],
                wait_for_rebalance=wait_for_rebalance)
        # if wait_for_rebalance:
        #     self.assertTrue(
        #         removed,
        #         "Rebalance operation failed while removing %s," % otpnode)

    def get_victim_nodes(self, nodes, master=None, chosen=None,
                         victim_type="master", victim_count=1):
        victim_nodes = [master]
        if victim_type == "graceful_failover_node":
            victim_nodes = [chosen]
        elif victim_type == "other":
            victim_nodes = self.add_remove_servers(nodes, nodes, [chosen, self.cluster.master], [])
            victim_nodes = victim_nodes[:victim_count]
        return victim_nodes

    def print_cluster_stats(self):
        table = TableView(self.log.info)
        table.set_headers(["Node", "Services", "CPU_utilization",
                           "Mem_total", "Mem_free",
                           "Swap_mem_used",
                           "Active / Replica ", "Version"])
        rest = RestConnection(self.cluster.master)
        cluster_stat = rest.get_cluster_stats()
        for cluster_node, node_stats in cluster_stat.items():
            row = list()
            row.append(cluster_node.split(':')[0])
            row.append(", ".join(node_stats["services"]))
            row.append(str(node_stats["cpu_utilization"]))
            row.append(str(node_stats["mem_total"]))
            row.append(str(node_stats["mem_free"]))
            row.append(str(node_stats["swap_mem_used"]) + " / "
                       + str(node_stats["swap_mem_total"]))
            row.append(str(node_stats["active_item_count"]) + " / "
                       + str(node_stats["replica_item_count"]))
            row.append(node_stats["version"])
            table.add_row(row)
        table.display("Cluster statistics")

    def verify_replica_distribution_in_zones(self, nodes):
        """
        Verify the replica distribution in nodes in different zones.
        Validate that no replicas of a node are in the same zone.
        :param nodes: Map of the nodes in different zones.
        Each key contains the zone name and the ip of nodes in that zone.
        """
        shell = RemoteMachineShellConnection(self.cluster.master)
        info = shell.extract_remote_info().type.lower()
        if info == 'linux':
            cbstat_command = "{0:s}cbstats" \
                             .format(testconstants.LINUX_COUCHBASE_BIN_PATH)
        elif info == 'windows':
            cbstat_command = "{0:s}cbstats" \
                             .format(testconstants.WIN_COUCHBASE_BIN_PATH)
        elif info == 'mac':
            cbstat_command = "{0:s}cbstats" \
                             .format(testconstants.MAC_COUCHBASE_BIN_PATH)
        else:
            raise Exception("OS not supported.")
        saslpassword = ''
        versions = RestConnection(self.cluster.master).get_nodes_versions()
        for group in nodes:
            for node in nodes[group]:
                if versions[0][:5] in testconstants.COUCHBASE_VERSION_2:
                    command = "tap"
                    if not info == 'windows':
                        commands = "%s %s:%s %s -b %s -p \"%s\" | grep :vb_filter: |  awk '{print $1}' \
                            | xargs | sed 's/eq_tapq:replication_ns_1@//g'  | sed 's/:vb_filter://g' \
                            " % (cbstat_command, node,
                                 constants.memcached_port,
                                 command, "default", saslpassword)
                    else:
                        commands = "%s %s:%s %s -b %s -p \"%s\" | grep.exe :vb_filter: | gawk.exe '{print $1}' \
                               | sed.exe 's/eq_tapq:replication_ns_1@//g'  | sed.exe 's/:vb_filter://g' \
                               " % (cbstat_command, node,
                                    constants.memcached_port,
                                    command, "default", saslpassword)
                    output, error = shell.execute_command(commands)
                elif versions[0][:5] in testconstants.COUCHBASE_VERSION_3 or \
                        versions[0][:5] in testconstants.COUCHBASE_FROM_VERSION_4:
                    command = "dcp"
                    if not info == 'windows':
                        commands = "%s %s:%s %s -b %s -p \"%s\" | grep :replication:ns_1@%s |  grep vb_uuid | \
                                    awk '{print $1}' | sed 's/eq_dcpq:replication:ns_1@%s->ns_1@//g' | \
                                    sed 's/:.*//g' | sort -u | xargs \
                                   " % (cbstat_command, node,
                                        constants.memcached_port,
                                        command, "default", saslpassword,
                                        node, node)
                        output, error = shell.execute_command(commands)
                    else:
                        commands = "%s %s:%s %s -b %s -p \"%s\" | grep.exe :replication:ns_1@%s |  grep vb_uuid | \
                                    gawk.exe '{print $1}' | sed.exe 's/eq_dcpq:replication:ns_1@%s->ns_1@//g' | \
                                    sed.exe 's/:.*//g' \
                                   " % (cbstat_command, node,
                                        constants.memcached_port,
                                        command, "default", saslpassword,
                                        node, node)
                        output, error = shell.execute_command(commands)
                        output = sorted(set(output))
                shell.log_command_output(output, error)
                output = output[0].split(" ")
                if node not in output:
                    self.log.debug("{0}".format(nodes))
                    self.log.debug("replicas of node {0} are in nodes {1}"
                                   .format(node, output))
                    self.log.debug(
                        "Replicas of node %s are not in its zone %s"
                        % (node, group))
                else:
                    exception_str = \
                        "Replica of node %s are on its own zone %s" \
                        % (node, group)
                    self.log.error(exception_str)
                    raise Exception(exception_str)
        shell.disconnect()

    def async_monitor_active_task(self, servers, type_task,
                                  target_value, wait_progress=100,
                                  num_iteration=100, wait_task=True):
        """Asynchronously monitor active task.

           When active task reached wait_progress this method  will return.

        Parameters:
            servers - list of servers or The server to handle fragmentation config task. (TestInputServer)
            type_task - task type('indexer' , 'bucket_compaction', 'view_compaction' ) (String)
            target_value - target value (for example "_design/ddoc" for indexing, bucket "default"
                for bucket_compaction or "_design/dev_view" for view_compaction) (String)
            wait_progress - expected progress (int)
            num_iteration - failed test if progress is not changed during num iterations(int)
            wait_task - expect to find task in the first attempt(bool)

        Returns:
            list of MonitorActiveTask - A task future that is a handle to the scheduled task."""
        _tasks = []
        if type(servers) is not list:
            servers = [servers, ]
        for server in servers:
            _task = MonitorActiveTask(server, type_task, target_value,
                                      wait_progress, num_iteration, wait_task)
            self.task_manager.add_new_task(_task)
            _tasks.append(_task)
        return _tasks

    def pick_node(self, master):
        log = logger.get("infra")
        rest = RestConnection(master)
        nodes = rest.node_statuses()
        node_picked = None
        nodes_on_same_ip = True

        firstIp = nodes[0].ip
        for node in nodes:
            if node.ip != firstIp:
                nodes_on_same_ip = False
                break

        for node in nodes:
            node_picked = node
            if not nodes_on_same_ip:
                if node_picked.ip != master.ip:
                    log.info("Picked node ... {0}:{1}"
                             .format(node_picked.ip, node_picked.port))
                    break
            else:
                # temp fix: port numbers of master ip and localhost:9000 match
                if int(node_picked.port) == int(master.port):
                    log.info("Not picking the master node {0}:{1}..try again.."
                             .format(node_picked.ip, node_picked.port))
                else:
                    log.info("Picked  node {0}:{1}"
                             .format(node_picked.ip, node_picked.port))
                    break
        return node_picked

    def pick_nodes(self, master, howmany=1, target_node=None, exclude_nodes=None):
        rest = RestConnection(master)
        nodes = rest.node_statuses()
        if exclude_nodes:
            exclude_nodes_ips = [node.ip for node in exclude_nodes]
            nodes = [node for node in nodes if node.ip not in exclude_nodes_ips]
        picked = []
        for node_for_stat in nodes:
            if node_for_stat.ip != master.ip or str(node_for_stat.port) != master.port:
                if target_node is None:
                    picked.append(node_for_stat)
                elif target_node.ip == node_for_stat.ip:
                    picked.append(node_for_stat)
                    return picked
                if len(picked) == howmany:
                    break
        return picked

    def check_for_panic_and_mini_dumps(self, servers):
        panic_str = "panic"
        panic_count = 0
        for server in servers:
            shell = RemoteMachineShellConnection(server)
            output, error = shell.enable_diag_eval_on_non_local_hosts()
            if output is not None:
                if "ok" not in output:
                    self.log.error(
                        "Error in enabling diag/eval on non-local hosts on {}: Error: {}"
                        .format(server.ip, error))
                else:
                    self.log.debug("Enabled diag/eval for non-local hosts from {}"
                                   .format(server.ip))
            else:
                self.log.debug("Running in compatibility mode, not enabled diag/eval for non-local hosts")
            _, dir_name = RestConnection(server).diag_eval(
                'filename:absname(element(2, application:get_env(ns_server,error_logger_mf_dir))).')
            log = str(dir_name) + '/*'
            count, err = shell.execute_command("zgrep \"{0}\" {1} | wc -l".
                                               format(panic_str, log))
            if isinstance(count, list):
                count = int(count[0])
            else:
                count = int(count)
            if count > panic_count:
                self.log.warn("=== PANIC OBSERVED IN THE LOGS ON SERVER %s ==="
                              % server.ip)
                panic_trace, _ = shell.execute_command("zgrep \"{0}\" {1}".
                                                       format(panic_str, log))
                self.log.error("\n {0}".format(panic_trace))
                panic_count = count
            os_info = shell.extract_remote_info()
            if os_info.type.lower() == "windows":
                # This is a fixed path in all windows systems inside couchbase
                dir_name_crash = 'c://CrashDumps'
            else:
                dir_name_crash = str(dir_name) + '/../crash/'
            core_dump_count, err = shell.execute_command(
                "ls {0}| wc -l".format(dir_name_crash))
            if isinstance(core_dump_count, list):
                core_dump_count = int(core_dump_count[0])
            else:
                core_dump_count = int(core_dump_count)
            if core_dump_count > 0:
                self.log.error("=== CORE DUMPS SEEN ON SERVER %s: %s crashes seen ==="
                               % (server.ip, core_dump_count))
            shell.disconnect()

    def create_stats_snapshot(self, master):
        self.log.debug("Triggering stats snapshot")
        rest = RestConnection(master)
        return rest.create_stats_snapshhot()

    def trigger_cb_collect_on_cluster(self, rest, nodes, single_node=False):
        params = dict()
        node_ids = [node.id for node in nodes]
        params['nodes'] = ",".join(node_ids)
        if single_node:
            # In case of single node we have to pass ip as below
            params['nodes'] = 'ns_1@' + '127.0.0.1'

        self.log.info('Running cbcollect on node ' + params['nodes'])
        status, _, _ = rest.perform_cb_collect(params)
        sleep(10, "Wait for CB collect to start", log_type="infra")
        self.log.info("%s - cbcollect status: %s"
                      % (",".join(node_ids), status))
        return status

    def wait_for_cb_collect_to_complete(self, rest, retry_count=60):
        self.log.info("Polling active_tasks to check cbcollect status")
        retry = 0
        status = False
        while retry < retry_count:
            cb_collect_response = rest.ns_server_tasks("clusterLogsCollection")
            cb_collect_progress = int(cb_collect_response['progress'])
            cb_collect_status = cb_collect_response['status']
            self.log.debug("CBCollectInfo Iteration {} - {}"
                           .format(retry, cb_collect_status))
            if cb_collect_progress == 100:
                if cb_collect_status != 'completed':
                    self.log.warning("Cb collect completed with status '%s'"
                                     % cb_collect_status)
                status = True
                break
            else:
                retry += 1
                sleep(10, "CB collect still running", log_type="infra")
        return status

    def copy_cb_collect_logs(self, rest, nodes, cluster, log_path):
        status = True
        cb_collect_response = rest.ns_server_tasks("clusterLogsCollection")
        self.log.debug(cb_collect_response)
        node_ids = [node.id for node in nodes]
        if 'perNode' in cb_collect_response:
            for idx, node in enumerate(nodes):
                self.log.info("%s: Copying cbcollect ZIP file to Client"
                              % node_ids[idx])
                server = [server for server in cluster.servers if
                          server.ip == node.ip][0]
                remote_client = RemoteMachineShellConnection(server)
                cb_collect_path = \
                    cb_collect_response['perNode'][node_ids[idx]]['path']
                zip_file_copied = remote_client.get_file(
                    os.path.dirname(cb_collect_path),
                    os.path.basename(cb_collect_path),
                    log_path)
                if zip_file_copied:
                    remote_client.execute_command("rm -f %s"
                                                  % cb_collect_path)
                remote_client.disconnect()
                cb_collect_size = int(os.path.getsize(
                    log_path + "/" + os.path.basename(cb_collect_path)))
                if cb_collect_size == 0:
                    self.log.critical("%s cb_collect zip file size: %s"
                                      % (node.ip, cb_collect_size))
                    status = False
                self.log.error("%s node cb collect zip copied on client : %s"
                               % (node.ip, zip_file_copied))
        return status

    def run_cb_collect(self, node, file_name,
                       options="", result=dict()):
        """
        Triggers cb_collect_info on target node from command line (uses shell)
        """
        self.log.info("%s - Running cb_collect_info" % node.ip)
        shell = RemoteMachineShellConnection(node)
        output, error = \
            CbCollectInfo(shell).start_collection(
                file_name,
                options=options,
                compress_output=True)
        result["output"] = output
        result["error"] = error
        self.log.info("%s - cb_collect_info completed" % node.ip)
        shell.disconnect()

        self.validate_cb_collect_file_size(node, file_name, result)

    def validate_cb_collect_file_size(self, node, file_name, result=dict()):
        result["file_name"] = "NA"
        result["file_size"] = 0
        shell = RemoteMachineShellConnection(node)
        output, error = shell.execute_command("du -sh %s" % file_name)
        if error:
            self.log.error("%s - Error during cb_collect_file validation: %s"
                           % (node.ip, error))
            return
        output = "".join(output)

        du_output_pattern = "([0-9.A-Za-z]+)[\t ]+([0-9A-Za-z/_.-]+)"
        du_output_pattern = re.compile(du_output_pattern)
        du_match = du_output_pattern.match(output)
        if du_match:
            result["file_name"] = du_match.group(2)
            result["file_size"] = du_match.group(1)
            self.log.info("%s - %s::%s" % (node.ip, result["file_name"],
                                           result["file_size"]))
            if result["file_size"] == "0":
                self.log.warning("%s - file size is zero" % node.ip)
        else:
            self.log.error("%s - du command failure: %s" % (node.ip, output))
        shell.disconnect()
