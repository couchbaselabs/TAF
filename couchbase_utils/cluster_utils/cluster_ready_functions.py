"""
Created on Sep 26, 2017

@author: riteshagarwal
"""

import copy
import json
import re
import time
import os

from Cb_constants import constants, CbServer
from Jython_tasks.task import MonitorActiveTask, FunctionCallTask
from TestInput import TestInputSingleton, TestInputServer
from cb_tools.cb_collectinfo import CbCollectInfo
from common_lib import sleep, humanbytes
from couchbase_cli import CouchbaseCLI
from global_vars import logger
from membase.api.rest_client import RestConnection
from platform_constants.os_constants import Linux, Mac, Windows
from remote.remote_util import RemoteMachineShellConnection, RemoteUtilHelper
from table_view import TableView


class CBCluster:
    def __init__(self, name="default", username="Administrator",
                 password="password", paths=None, servers=None, vbuckets=1024):
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
        self.buckets = list()
        self.vbuckets = vbuckets
        # edition = community/enterprise
        self.edition = None
        self.cloud_cluster = False

        # Capella specific params
        self.pod = None
        self.tenant = None
        self.cluster_config = None

    def __str__(self):
        return "Couchbase Cluster: %s, Nodes: %s" % (
            self.name, ', '.join([s.ip for s in self.servers]))

    def refresh_object(self, servers):
        self.kv_nodes = list()
        self.fts_nodes = list()
        self.cbas_nodes = list()
        self.index_nodes = list()
        self.query_nodes = list()
        self.eventing_nodes = list()
        self.backup_nodes = list()
        self.nodes_in_cluster = list()

        for server in servers:
            if "Data" in server.services:
                self.kv_nodes.append(server)
            if "Query" in server.services:
                self.query_nodes.append(server)
            if "Index" in server.services:
                self.index_nodes.append(server)
            if "Eventing" in server.services:
                self.eventing_nodes.append(server)
            if "Analytics" in server.services:
                self.cbas_nodes.append(server)
            if "FTS" in server.services:
                self.fts_nodes.append(server)
            self.nodes_in_cluster.append(server)
        self.master = self.kv_nodes[0]

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
    def __init__(self, task_manager):
        self.input = TestInputSingleton.input
        self.task_manager = task_manager
        self.log = logger.get("test")

    @staticmethod
    def find_orchestrator(cluster, node=None):
        """
        Update the orchestrator of the cluster
        :param cluster: Target cluster object
        :param node: Any node that is still part of the cluster
        :return:
        """
        retry_index = 0
        max_retry = 12
        orchestrator_node = None
        status = None
        node = cluster.master if node is None else node

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

        cluster.master = [server for server in cluster.servers
                          if server.ip == orchestrator_node][0]
        # Type cast to str - match the previous dial/eval return value
        content = "ns_1@%s" % cluster.master.ip
        return status, content

    @staticmethod
    def set_metadata_purge_interval(cluster_node, interval=0.04):
        # set it to 0.04 i.e. 1 hour if not given
        rest = RestConnection(cluster_node)
        return rest.set_metadata_purge_interval(interval)

    @staticmethod
    def create_metakv_key(cluster_node, key, value):
        rest = RestConnection(cluster_node)
        return rest.create_metakv_key(key, value)

    @staticmethod
    def delete_metakv_key(cluster_node, key):
        rest = RestConnection(cluster_node)
        return rest.delete_metakv_key(key)

    @staticmethod
    def get_metakv_dicts(cluster_node, key=None):
        rest = RestConnection(cluster_node)
        metakv_key_count, metakv_dict = rest.get_metakv_dicts(key=key)
        return metakv_key_count, metakv_dict

    @staticmethod
    def is_enterprise_edition(cluster):
        """
        :param cluster: Cluster object
        :return: True if the cluster is enterprise edition
        """
        rest = RestConnection(cluster.master)
        api = rest.baseUrl + "pools/default"
        http_res, success = rest.init_http_request(api)
        if http_res == 'unknown pool':
            return False
        for node in http_res["nodes"]:
            if "community" in node["version"].split("-")[-1:]:
                return False
        return True

    def get_server_profile_type(self, cluster_node):
        """
        :param cluster_node: TestServer object
        :return str: Profile_type for the cluster node
        """
        profile_val = "default"
        cmd = "cat /etc/couchbase.d/config_profile"
        shell = RemoteMachineShellConnection(cluster_node)
        output, _ = shell.execute_command(cmd)
        if output and output[0].strip() == "serverless":
            profile_val = "serverless"
        shell.disconnect()
        self.log.debug("%s: Profile_type='%s'"
                       % (cluster_node.ip, profile_val))
        return profile_val

    def set_rebalance_moves_per_nodes(self, cluster_node,
                                      rebalanceMovesPerNode=4):
        body = dict()
        body["rebalanceMovesPerNode"] = rebalanceMovesPerNode
        rest = RestConnection(cluster_node)
        rest.set_rebalance_settings(body)
        result = rest.get_rebalance_settings()
        self.log.info("Changed Rebalance settings: {0}"
                      .format(json.loads(result)))

    def cluster_cleanup(self, cluster, bucket_util):
        rest = RestConnection(cluster.master)
        if rest._rebalance_progress_status() == 'running':
            self.kill_memcached(cluster)
            self.log.warning("Rebalance still running, "
                             "test should be verified")
            stopped = rest.stop_rebalance()
            if not stopped:
                raise Exception("Unable to stop rebalance")
        bucket_util.delete_all_buckets(cluster)
        self.cleanup_cluster(cluster, master=cluster.master)
        self.wait_for_ns_servers_or_assert(cluster.servers)

    # wait_if_warmup=True is useful in tearDown method for (auto)failover tests
    def wait_for_ns_servers_or_assert(self, servers, wait_time=360):
        tasks = []
        for server in servers:
            task = FunctionCallTask(self._wait_for_ns_servers,
                                    (server, wait_time))
            self.task_manager.schedule(task)
            tasks.append(task)
        for task in tasks:
            result = self.task_manager.get_task_result(task)
            if not result:
                self.log.error("Some machines aren't running yet.")
                return result
        return True

    def _wait_for_ns_servers(self, server, wait_time):
        self.log.debug("Waiting for ns_server @ {0}:{1}"
                       .format(server.ip, server.port))
        if RestConnection(server).is_ns_server_running(wait_time):
            self.log.debug("ns_server @ {0}:{1} is running"
                           .format(server.ip, server.port))
        else:
            self.log.error("ns_server {0} is not running in {1} sec"
                           .format(server.ip, wait_time))
            return False
        return True

    # this method will rebalance the cluster by passing the remote_node as
    # ejected node
    def remove_nodes(self, rest, knownNodes, ejectedNodes,
                     wait_for_rebalance=True):
        self.log.debug("Ejecting nodes '{0}' from cluster"
                       .format(ejectedNodes))
        if len(ejectedNodes) == 0:
            return False
        rest.rebalance(knownNodes, ejectedNodes)
        if wait_for_rebalance:
            return rest.monitorRebalance()
        else:
            return False

    def cleanup_cluster(self, cluster, wait_for_rebalance=True, master=None):
        if master is None:
            master = cluster.master
        rest = RestConnection(master)
        rest.remove_all_replications()
        rest.remove_all_remote_clusters()
        rest.remove_all_recoveries()
        rest.is_ns_server_running(timeout_in_seconds=120)
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
            _ = self.remove_nodes(
                rest,
                knownNodes=[node.id for node in nodes],
                ejectedNodes=[node.id for node in nodes
                              if node.id != master_id],
                wait_for_rebalance=wait_for_rebalance)
            success_cleaned = []
            for removed in [node for node in nodes if (node.id != master_id)]:
                removed.rest_password = cluster.master.rest_password
                removed.rest_username = cluster.master.rest_username
                removed.hosted_on_cloud = False
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
            for node in set([node for node in nodes
                             if (node.id != master_id)])-set(success_cleaned):
                self.log.error("Node {0}:{1} was not cleaned after "
                               "removing from cluster"
                               .format(removed.ip, removed.port))
                try:
                    rest = RestConnection(node)
                    rest.force_eject_node()
                except Exception as ex:
                    self.log.error("Force_eject_node {0}:{1} failed: {2}"
                                   .format(removed.ip, removed.port, ex))
            if len(set([node for node in nodes if (node.id != master_id)])
                    - set(success_cleaned)) != 0:
                raise Exception("Not all ejected nodes were cleaned")

            self.log.debug("Removed all the nodes from cluster associated with {0} ? {1}"
                           .format(cluster.master,
                                   [(node.id, node.port) for node in nodes if (node.id != master_id)]))

    @staticmethod
    def get_nodes_in_cluster(cluster):
        server_set = list()
        for node in RestConnection(cluster.master).node_statuses():
            for server in cluster.servers:
                if ":" not in server.ip and ":" not in node.ip:
                    """ server.ip maybe Ipv4 or hostname and node.ip is Ipv4 or hostname """
                    if server.ip == node.ip:
                        server_set.append(server)
                elif ":" not in server.ip and ":" in node.ip:
                    """ server.ip maybe Ipv4 or hostname and node.ip is raw Ipv6 """
                    shell = RemoteMachineShellConnection(server)
                    serverIP = shell.get_ip_address()
                    self.log.info("get raw IP from hostname. Hostname: {0} ; its IPs: {1}" \
                                      .format(server.ip, serverIP))
                    if node.ip in serverIP:
                        server_set.append(server)
                    shell.disconnect()
                elif ":" in server.ip and ":" in node.ip:
                    """ both server.ip and node.ip are raw Ipv6 """
                    if server.ip == node.ip:
                        server_set.append(server)
        return server_set

    def change_checkpoint_params(self, cluster):
        self.chk_max_items = self.input.param("chk_max_items", None)
        self.chk_period = self.input.param("chk_period", None)
        if self.chk_max_items or self.chk_period:
            for server in cluster.servers:
                rest = RestConnection(server)
                if self.chk_max_items:
                    rest.set_chk_max_items(self.chk_max_items)
                if self.chk_period:
                    rest.set_chk_period(self.chk_period)

    def change_password(self, cluster, new_password="new_password"):
        nodes = RestConnection(cluster.master).node_statuses()

        cli = CouchbaseCLI(cluster.master,
                           cluster.master.rest_username,
                           cluster.master.rest_password)
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
            for server in cluster.servers:
                if server.ip == node.ip and int(server.port) == int(node.port):
                    server.rest_password = new_password
                    break

    def change_port(self, cluster, new_port="9090", current_port='8091'):
        nodes = RestConnection(cluster.master).node_statuses()
        remote_client = RemoteMachineShellConnection(cluster.master)
        options = "--cluster-port=%s" % new_port
        cli_command = "cluster-edit"
        output, error = remote_client.execute_couchbase_cli(
            cli_command=cli_command, options=options,
            cluster_host="localhost:%s" % current_port,
            user=cluster.master.rest_username,
            password=cluster.master.rest_password)
        self.log.debug(output)
        remote_client.disconnect()
        # MB-10136 & MB-9991
        if error:
            raise Exception("Port didn't change! %s" % error)
        self.port = new_port
        self.log.debug("New port '%s' on nodes: %s"
                       % (new_port, [node.ip for node in nodes]))
        for node in nodes:
            for server in cluster.servers:
                if server.ip == node.ip and int(server.port) == int(node.port):
                    server.port = new_port
                    break

    def change_env_variables(self, cluster):
        for server in cluster.servers:
            remote_client = RemoteMachineShellConnection(server)
            vb_on_node, _ = remote_client.execute_command("grep ^COUCHBASE_NUM_VBUCKETS \
            /opt/couchbase/bin/couchbase-server | cut -d \"=\" -f 2",)
            self.log.debug("Current vBuckets on node %s: %s"
                           % (server, vb_on_node))
            if vb_on_node:
                vb_on_node = int(vb_on_node[0])
            else:
                vb_on_node = 1024
            if cluster.vbuckets != vb_on_node:
                # or self.upr is not None:
                env_dict = {}
                if cluster.vbuckets:
                    env_dict["COUCHBASE_NUM_VBUCKETS"] = cluster.vbuckets
                # if self.upr is not None:
                #     if self.upr:
                #         env_dict["COUCHBASE_REPL_TYPE"] = "upr"
                #     else:
                #         env_dict["COUCHBASE_REPL_TYPE"] = "tap"
                if len(env_dict) >= 1:
                    remote_client.change_env_variables(env_dict)
            remote_client.disconnect()
            self.log.debug("========= CHANGED ENVIRONMENT SETTING ===========")

        self.log.debug("Wait for all the services to come up after "
                       "change_env_vars update")
        sleep(10, log_type="infra")

    def reset_env_variables(self, cluster):
        # if self.upr is not None:
        #     for server in self.cluster.servers:
        #         if self.upr:
        #             remote_client = RemoteMachineShellConnection(server)
        #             remote_client.reset_env_variables()
        #             remote_client.disconnect()
        #     self.log.debug("==== RESET ENVIRONMENT SETTING TO ORIGINAL ====")
        return

    def change_log_info(self, cluster, log_info):
        for server in cluster.servers:
            remote_client = RemoteMachineShellConnection(server)
            remote_client.stop_couchbase()
            remote_client.change_log_level(log_info)
            remote_client.start_couchbase()
            remote_client.disconnect()
        self.log.debug("========= CHANGED LOG LEVELS ===========")

    def change_log_location(self, cluster, log_location):
        for server in cluster.servers:
            remote_client = RemoteMachineShellConnection(server)
            remote_client.stop_couchbase()
            remote_client.configure_log_location(log_location)
            remote_client.start_couchbase()
            remote_client.disconnect()
        self.log.debug("========= CHANGED LOG LOCATION ===========")

    def change_stat_info(self, cluster, stat_info):
        for server in cluster.servers:
            remote_client = RemoteMachineShellConnection(server)
            remote_client.stop_couchbase()
            remote_client.change_stat_periodicity(stat_info)
            remote_client.start_couchbase()
            remote_client.disconnect()
        self.log.debug("========= CHANGED STAT PERIODICITY ===========")

    def change_port_info(self, cluster, port_info):
        for server in cluster.servers:
            remote_client = RemoteMachineShellConnection(server)
            remote_client.stop_couchbase()
            remote_client.change_port_static(port_info)
            server.port = port_info
            self.log.debug("New REST port %s" % server.port)
            remote_client.start_couchbase()
            remote_client.disconnect()
        self.log.debug("========= CHANGED ALL PORTS ===========")

    def force_eject_nodes(self, cluster):
        """
        TODO: Not used anywhere at the moment
        :param cluster: Cluster object to play with
        :return:
        """
        for server in cluster.servers:
            if server != cluster.servers[0]:
                try:
                    rest = RestConnection(server)
                    rest.force_eject_node()
                except BaseException as e:
                    self.log.error(e)

    def set_upr_flow_control(self, cluster, flow=True, servers=[]):
        servers = self.get_kv_nodes(cluster, servers=servers)
        for bucket in cluster.buckets:
            for server in servers:
                rest = RestConnection(server)
                rest.set_enable_flow_control(bucket=bucket.name, flow=flow)
        for server in cluster.servers:
            remote_client = RemoteMachineShellConnection(server)
            remote_client.stop_couchbase()
            remote_client.start_couchbase()
            remote_client.disconnect()

    @staticmethod
    def kill_memcached(cluster, node=None):
        def kill_mc():
            remote_client = RemoteMachineShellConnection(server)
            remote_client.kill_memcached()
            remote_client.disconnect()

        if node is None:
            for server in cluster.servers:
                kill_mc()
        else:
            for server in cluster.servers:
                if server.ip != node.ip:
                    kill_mc()
                    break

    @staticmethod
    def kill_prometheus(cluster):
        for server in cluster.servers:
            remote_client = RemoteMachineShellConnection(server)
            remote_client.kill_prometheus()
            remote_client.disconnect()

    @staticmethod
    def get_nodes(cluster_node):
        """ Get Nodes from list of server """
        return RestConnection(cluster_node).get_nodes()

    def stop_server(self, cluster, node):
        """ Method to stop a server which is subject to failover """
        for server in cluster.servers:
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

    def start_server(self, cluster, node):
        """ Method to start a server which is subject to failover """
        for server in cluster.servers:
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

    @staticmethod
    def start_memcached_on_node(cluster, node):
        """ Method to start memcached on a server which is subject to failover """
        for server in cluster.servers:
            if server.ip == node.ip:
                remote_client = RemoteMachineShellConnection(server)
                remote_client.start_memcached()
                remote_client.disconnect()

    @staticmethod
    def stop_memcached_on_node(cluster, node):
        """ Method to stop memcached on a server which is subject to failover """
        for server in cluster.servers:
            if server.ip == node.ip:
                remote_client = RemoteMachineShellConnection(server)
                remote_client.stop_memcached()
                remote_client.disconnect()

    @staticmethod
    def start_firewall_on_node(cluster, node):
        """ Method to start a server which is subject to failover """
        for server in cluster.servers:
            if server.ip == node.ip:
                RemoteUtilHelper.enable_firewall(server)

    @staticmethod
    def stop_firewall_on_node(cluster, node):
        """ Method to start a server which is subject to failover """
        for server in cluster.servers:
            if server.ip == node.ip:
                remote_client = RemoteMachineShellConnection(server)
                remote_client.disable_firewall()
                remote_client.disconnect()

    def update_cluster_nodes_service_list(self, cluster):
        def append_nodes_to_list(nodes, list_to_append):
            for t_node in nodes:
                t_node = t_node.split(":")
                for server in cluster.servers:
                    if server.ip == t_node[0] and server.port == t_node[1]:
                        list_to_append.append(server)
                        break

        service_map = self.get_services_map(cluster)
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

    def get_nodes_from_services_map(self, cluster, service_type="n1ql",
                                    get_all_nodes=False, servers=None):
        if not servers:
            servers = cluster.servers
        services_map = self.get_services_map(cluster)
        if service_type not in services_map:
            self.log.warning("Cannot find service node {0} in cluster "
                             .format(service_type))
        else:
            node_list = list()
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
                                shell.disconnect()
                                break
                            shell.disconnect()

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

    @staticmethod
    def get_services_map(cluster, reset=True):
        if not reset:
            return
        services_map = dict()
        rest = RestConnection(cluster.master)
        tem_map = rest.get_nodes_services()
        for key, val in tem_map.iteritems():
            for service in val:
                if service not in services_map.keys():
                    services_map[service] = list()
                services_map[service].append(key)
        return services_map

    @staticmethod
    def get_services(tgt_nodes, tgt_services, start_node=1):
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

    def generate_map_nodes_out_dist(self, cluster,
                                    nodes_out_dist=None,
                                    targetMaster=False,
                                    targetIndexManager=False):
        if nodes_out_dist is None:
            nodes_out_dist = list()
        index_nodes_out = list()
        nodes_out_list = list()
        services_map = self.get_services_map(reset=True)
        if not nodes_out_dist:
            if len(cluster.servers) > 1:
                nodes_out_list.append(cluster.servers[1])
            return nodes_out_list, index_nodes_out
        for service_fail_map in nodes_out_dist.split("-"):
            tokens = service_fail_map.rsplit(":", 1)
            count = 0
            service_type = tokens[0]
            service_type_count = int(tokens[1])
            compare_string_master = "{0}:{1}".format(cluster.master.ip,
                                                     cluster.master.port)
            compare_string_index_manager = "{0}:{1}" \
                                           .format(cluster.master.ip,
                                                   cluster.master.port)
            if service_type in services_map.keys():
                for node_info in services_map[service_type]:
                    for server in cluster.servers:
                        compare_string_server = "{0}:{1}".format(server.ip,
                                                                 server.port)
                        addNode = False
                        if (targetMaster and (not targetIndexManager)) \
                                and (compare_string_server == node_info
                                     and compare_string_master == compare_string_server):
                            addNode = True
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
                            if service_type == CbServer.Services.INDEX:
                                if server not in index_nodes_out:
                                    index_nodes_out.append(server)
                            nodes_out_list.append(server)
        return nodes_out_list, index_nodes_out

    def setDebugLevel(self, index_servers=None, service_type="kv"):
        index_debug_level = self.input.param("index_debug_level", None)
        if index_debug_level is None:
            return
        if index_servers is None:
            index_servers = self.get_nodes_from_services_map(
                service_type=service_type, get_all_nodes=True)
        json = {
            "indexer.settings.log_level": "debug",
            "projector.settings.log_level": "debug",
        }
        for server in index_servers:
            RestConnection(server).set_index_settings(json)

    def get_kv_nodes(self, cluster, servers=None):
        if cluster.cloud_cluster:
            return cluster.kv_nodes

        if servers is None:
            servers = cluster.servers
        kv_servers = self.get_nodes_from_services_map(
            cluster,
            service_type=CbServer.Services.KV,
            get_all_nodes=True,
            servers=servers)
        new_servers = []
        for server in servers:
            for kv_server in kv_servers:
                if kv_server.ip == server.ip \
                        and kv_server.port == server.port \
                        and server not in new_servers:
                    new_servers.append(server)
        return new_servers

    @staticmethod
    def generate_services_map(nodes, services=None):
        service_map = dict()
        index = 0
        if services is None:
            return service_map
        for node in nodes:
            service_map[node.ip] = node.services
            index += 1
        return service_map

    @staticmethod
    def find_node_info(master, node):
        """ Get Nodes from list of server """
        target_node = None
        rest = RestConnection(master)
        nodes = rest.get_nodes()
        for server in nodes:
            if server.ip == node.ip:
                target_node = server
        return target_node

    @staticmethod
    def add_remove_servers(cluster, server_list=[],
                           remove_list=[], add_list=[]):
        """ Add or Remove servers from server_list """
        initial_list = copy.deepcopy(server_list)
        for add_server in add_list:
            for server in cluster.servers:
                if add_server is not None and server.ip == add_server.ip:
                    initial_list.append(add_server)
        for remove_server in remove_list:
            for server in initial_list:
                if remove_server is not None and server.ip == remove_server.ip:
                    initial_list.remove(server)
        return initial_list

    def is_cluster_healthy(self, rest_conn, timeout=120):
        # get the nodes and verify that all the nodes.status are healthy
        nodes = rest_conn.node_statuses(timeout)
        self.log.debug("Nodes: %s" % nodes)
        return all(node.status == 'healthy' for node in nodes)

    def add_all_nodes_then_rebalance(self, cluster, nodes,
                                     wait_for_completion=True):
        otp_nodes = list()
        rest = RestConnection(cluster.master)
        if len(nodes) >= 1:
            for server in nodes:
                '''
                This is the case, master node is running cbas service as well
                '''
                if cluster.master.ip != server.ip:
                    otp_nodes.append(rest.add_node(
                        user=server.rest_username,
                        password=server.rest_password,
                        remoteIp=server.ip, port=constants.port,
                        services=server.services.split(",")))

            self.rebalance(cluster.master, wait_for_completion)
        else:
            self.log.warning("No Nodes provided to add in cluster")

        return otp_nodes

    @staticmethod
    def rebalance(cluster, wait_for_completion=True, ejected_nodes=[]):
        rest = RestConnection(cluster.master)
        nodes = rest.node_statuses()
        result = rest.rebalance(otpNodes=[node.id for node in nodes],
                                ejectedNodes=ejected_nodes)
        if result and wait_for_completion:
            result = rest.monitorRebalance()
        return result

    def rebalance_reached(self, rest, percentage=100, wait_step=2,
                          num_retry=40):
        start = time.time()
        progress = 0
        previous_progress = 0
        retry = 0
        while progress is not -1 and progress < percentage and retry < num_retry:
            # -1 is error , -100 means could not retrieve progress
            progress = rest._rebalance_progress()
            if progress == -100:
                self.log.error("Unable to retrieve rebalance progress. "
                               "Retrying..")
                retry += 1
            else:
                if previous_progress == progress:
                    retry += 0.5
                else:
                    retry = 0
                    previous_progress = progress
            # Wait before fetching rebalance progress
            sleep(wait_step)
        if progress <= 0:
            self.log.error("Rebalance progress: {0}".format(progress))

            return False
        elif retry >= num_retry:
            self.log.error("Rebalance stuck at {0}%".format(progress))
            return False
        else:
            duration = time.time() - start
            self.log.info('Rebalance reached >{0}% in {1} seconds '
                          .format(progress, duration))
            return True

    # return true if cluster balanced, false if it needs rebalance
    def is_cluster_rebalanced(self, rest):
        command = "ns_orchestrator:needs_rebalance()"
        status, content = rest.diag_eval(command)
        if status:
            return content.lower() == "false"
        self.log.critical("Can't define if cluster balanced")
        return None

    def remove_all_nodes_then_rebalance(self, cluster, otpnodes=None,
                                        rebalance=True):
        return self.remove_node(cluster, otpnodes, rebalance)

    def add_node(self, cluster, node, services=None, rebalance=True,
                 wait_for_rebalance_completion=True):
        if not services:
            services = node.services.split(",")
        rest = RestConnection(cluster.master)
        otpnode = rest.add_node(user=node.rest_username,
                                password=node.rest_password,
                                remoteIp=node.ip, port=constants.port,
                                services=services)
        if rebalance:
            self.rebalance(cluster,
                           wait_for_completion=wait_for_rebalance_completion)
        return otpnode

    def remove_node(self, cluster, otpnode=None, wait_for_rebalance=True):
        rest = RestConnection(cluster.master)
        nodes = rest.node_statuses()
        '''This is the case when master node is running cbas service as well'''
        if len(nodes) <= len(otpnode):
            return

        try:
            _ = self.remove_nodes(
                rest,
                knownNodes=[node.id for node in nodes],
                ejectedNodes=[node.id for node in otpnode],
                wait_for_rebalance=wait_for_rebalance)
        except Exception as e:
            self.log.error("First time rebalance failed on Removal. "
                           "Wait and try again. THIS IS A BUG.")
            sleep(5)
            _ = self.remove_nodes(
                rest,
                knownNodes=[node.id for node in nodes],
                ejectedNodes=[node.id for node in otpnode],
                wait_for_rebalance=wait_for_rebalance)
        # if wait_for_rebalance:
        #     self.assertTrue(
        #         removed,
        #         "Rebalance operation failed while removing %s," % otpnode)

    def wait_for_node_status(self, node, rest, expected_status,
                             timeout_in_seconds):
        status_reached = False
        end_time = time.time() + timeout_in_seconds
        while time.time() <= end_time and not status_reached:
            nodes = rest.node_statuses()
            for n in nodes:
                if node.id == n.id:
                    self.log.debug('Node {0} status : {1}'
                                   .format(node.id, n.status))
                    if n.status.lower() == expected_status.lower():
                        status_reached = True
                    break
            if not status_reached:
                self.log.debug("Wait before reading the node.status")
                sleep(5)
        self.log.debug('Node {0} status_reached: {1}'
                       .format(node.id, status_reached))
        return status_reached

    @staticmethod
    def get_victim_nodes(cluster, nodes, master=None, chosen=None,
                         victim_type="master", victim_count=1):
        victim_nodes = [master]
        if victim_type == "graceful_failover_node":
            victim_nodes = [chosen]
        elif victim_type == "other":
            victim_nodes = ClusterUtils.add_remove_servers(
                cluster, nodes, [chosen, cluster.master], [])
            victim_nodes = victim_nodes[:victim_count]
        return victim_nodes

    def print_cluster_stats(self, cluster):
        table = TableView(self.log.info)
        table.set_headers(["Nodes", "Zone", "Services", "CPU",
                           "Mem_total", "Mem_free",
                           "Swap_mem_used",
                           "Active / Replica ", "Version / Config"])
        rest = RestConnection(cluster.master)
        cluster_stat = rest.get_cluster_stats()
        for cluster_node, node_stats in cluster_stat.items():
            row = list()
            row.append(cluster_node.split(':')[0])
            row.append(node_stats["serverGroup"])
            row.append(", ".join(node_stats["services"]))
            row.append(str(node_stats["cpu_utilization"])[0:6])
            row.append(humanbytes(str(node_stats["mem_total"])))
            row.append(humanbytes(str(node_stats["mem_free"])))
            row.append(humanbytes(str(node_stats["swap_mem_used"])) + " / "
                       + humanbytes(str(node_stats["swap_mem_total"])))
            row.append(str(node_stats["active_item_count"]) + " / "
                       + str(node_stats["replica_item_count"]))
            row.append(node_stats["version"] + " / " + CbServer.cluster_profile)
            table.add_row(row)
        table.display("Cluster statistics")

    def verify_replica_distribution_in_zones(self, cluster, nodes, bucket="default"):
        """
        Verify the replica distribution in nodes in different zones.
        Validate that no replicas of a node are in the same zone.
        :param cluster: Target cluster object
        :param nodes: Map of the nodes in different zones.
        Each key contains the zone name and the ip of nodes in that zone.
        """
        shell = RemoteMachineShellConnection(cluster.master)
        rest = RestConnection(cluster.master)
        info = shell.extract_remote_info().type.lower()
        if info == Linux.NAME:
            cbstat_command = "{0:s}cbstats".format(Linux.COUCHBASE_BIN_PATH)
        elif info == Windows.NAME:
            cbstat_command = "{0:s}cbstats".format(Windows.COUCHBASE_BIN_PATH)
        elif info == Mac.NAME:
            cbstat_command = "{0:s}cbstats".format(Mac.COUCHBASE_BIN_PATH)
        else:
            raise Exception("OS not supported.")
        versions = rest.get_nodes_versions()
        for group in nodes:
            for node in nodes[group]:
                command = "dcp"
                if not info == Windows.NAME:
                    commands = "%s %s:%s -u %s -p \"%s\" %s -b %s  | grep :replication:ns_1@%s |  grep vb_uuid | \
                                awk '{print $1}' | sed 's/eq_dcpq:replication:ns_1@%s->ns_1@//g' | \
                                sed 's/:.*//g' | sort -u | xargs \
                               " % (cbstat_command, node,
                                    constants.memcached_port,
                                    rest.username, rest.password,
                                    command, bucket,
                                    node, node)
                    output, error = shell.execute_command(commands)
                else:
                    commands = "%s %s:%s -u %s -p \"%s\" %s -b %s  | grep.exe :replication:ns_1@%s |  grep vb_uuid | \
                                gawk.exe '{print $1}' | sed.exe 's/eq_dcpq:replication:ns_1@%s->ns_1@//g' | \
                                sed.exe 's/:.*//g' \
                               " % (cbstat_command, node,
                                    constants.memcached_port,
                                    rest.username, rest.password,
                                    command, bucket,
                                    node, node)
                    output, error = shell.execute_command(commands)
                    output = sorted(set(output))
                shell.log_command_output(output, error)
                output = [element.strip() for element in output]
                if set(nodes[group]).isdisjoint(set(output)):
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

    @staticmethod
    def pick_nodes(master, howmany=1, target_node=None, exclude_nodes=None):
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

    def check_if_services_obey_tls(self, servers, port_map=CbServer.ssl_port_map):
        """
        Parameters:
        servers - list of servers on which to check
        port_map (optional) - a dict with key as non-ssl port
            and its value as tls-port. If not given, it will take the port map from
            CbServer.ssl_port_map

        Returns False if
        a. the non-ssl port is open on any other address other than localhost
        b. the tls port is not open on all (*) addresses
        else True
        """
        for server in servers:
            shell = RemoteMachineShellConnection(server)
            # service should listen on non-ssl port only on localhost/no-address
            for port in port_map.keys():
                addresses = shell.get_port_recvq(port)
                for address in addresses:
                    expected_address = "127.0.0.1:" + port + '\n'
                    if address != expected_address:
                        self.log.error(" On Server {0} Expected {1} Actual {2}".
                                       format(server.ip, expected_address, address))
                        shell.disconnect()
                        return False
            # service should listen on tls_port(if there is one) for all outside addresses
            for port in port_map.keys():
                ssl_port = CbServer.ssl_port_map.get(port)
                if ssl_port is None:
                    continue
                addresses = shell.get_port_recvq(ssl_port)
                for address in addresses:
                    expected_address = ["*:" + ssl_port + '\n',
                                        "0.0.0.0:" + ssl_port + '\n']
                    if address not in expected_address:
                        self.log.error("Server {0} Expected {1} Actual {2}".
                                       format(server, expected_address, address))
                        shell.disconnect()
                        return False
            shell.disconnect()
            return True

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
            if os_info.type.lower() == Windows.NAME:
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
        if 'perNode' in cb_collect_response and len(node_ids) > 0 and 'path' \
                in cb_collect_response['perNode'][node_ids[0]]:
            for idx, node in enumerate(nodes):
                self.log.info("%s: Copying cbcollect ZIP file to Client"
                              % node_ids[idx])
                server = [server for server in cluster.servers if
                          server.ip == node.ip][0]
                remote_client = RemoteMachineShellConnection(server)
                cb_collect_path = \
                    cb_collect_response['perNode'][node_ids[idx]]['path']
                if remote_client.info.type.lower() == "windows":
                    cb_collect_path = cb_collect_path \
                        .replace(":", "") \
                        .replace(" ", "\\ ")
                    cb_collect_path = os.path.join("/cygdrive",
                                                   cb_collect_path)

                remote_path = os.path.dirname(cb_collect_path)
                cb_collect_file_name = os.path.basename(cb_collect_path)
                zip_file_copied = remote_client.get_file(
                    remote_path, cb_collect_file_name, log_path)
                if zip_file_copied:
                    remote_client.execute_command(
                        "rm -f %s"
                        % os.path.join(remote_path, cb_collect_file_name))
                    cb_collect_size = int(os.path.getsize(
                        os.path.join(log_path, cb_collect_file_name)))
                    if cb_collect_size == 0:
                        status = False
                        self.log.critical("%s cb_collect zip file size: %s"
                                          % (node.ip, cb_collect_size))
                else:
                    status = False
                    self.log.error("%s - Failed to copy cb collect zip file"
                                   % node_ids[idx])
                remote_client.disconnect()
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

    def get_latest_ns_config_tombstones_purged_count(self, cluster, nodes=None):
        """
        grep debug log for the latest tombstones purged count
        Return dict with key = node_ip and value = ts purged count
        as string
        """
        ts_purged_count_dict = dict()
        if nodes is None:
            nodes = self.get_nodes_in_cluster(cluster)
        for node in nodes:
            shell = RemoteMachineShellConnection(node)
            command = "grep 'tombstone_agent:purge:' /opt/couchbase/var/lib/couchbase/logs/debug.log | tail -1"
            output, _ = shell.execute_command(command)
            self.log.info("On {0} {1}".format(node.ip, output))
            shell.disconnect()
            purged_count = re.findall("Purged [0-9]+", output[0])[0].split(" ")[1]
            ts_purged_count_dict[node.ip] = purged_count
        return ts_purged_count_dict

    def get_ns_config_deleted_keys_count(self, cluster, nodes=None):
        """
        get a dump of ns_config and grep for "_deleted" to get
        deleted keys count
        Return dict with key = node_ip and value = deleted key count
        as string
        """
        deleted_keys_count_dict = dict()
        rest = RestConnection(cluster.master)
        if nodes is None:
            nodes = self.get_nodes_in_cluster(cluster)
        for node in nodes:
            shell = RemoteMachineShellConnection(node)
            shell.enable_diag_eval_on_non_local_hosts()
            cmd = "curl --silent -u %s:%s http://localhost:8091/diag/eval " \
                  "-d 'ns_config:get()' | grep '_deleted' | wc -l" \
                  % (rest.username, rest.password)
            output, _ = shell.execute_command(cmd)
            shell.disconnect()
            deleted_keys_count_dict[node.ip] = int(output[0].strip('\n'))
        return deleted_keys_count_dict

    def check_ip_family_enforcement(self, cluster, ip_family="ipv4_only"):
        for server in ClusterUtils.get_nodes_in_cluster(cluster):
            shell = RemoteMachineShellConnection(server)
            if ip_family == "ipv4_only":
                processes = shell.get_processes_binding_to_ip_family(ip_family="ipv6")
            else:
                processes = shell.get_processes_binding_to_ip_family(ip_family="ipv4")
            self.log.info("{0} : {1} \n {2} \n\n".format(server.ip, len(processes), processes))
            if len(processes) != 0:
                return False
        return True

    def enable_disable_ip_address_family_type(
            self, cluster, enable=True, ipv4_only=False, ipv6_only=False):

        cli = CouchbaseCLI(
            cluster.master, cluster.master.rest_username,
            cluster.master.rest_password)

        def check_enforcement(ipv4_only=ipv4_only, ipv6_only=ipv6_only):
            if ipv4_only:
                if not self.check_ip_family_enforcement(
                    cluster, ip_family="ipv4_only"):
                    return False, "Ports are still binding to the opposite ip-family"
            if ipv6_only:
                if not self.cluster_util.check_ip_family_enforcement(
                    cluster, ip_family="ipv6_only"):
                    return False, "Ports are still binding to the opposite ip-family"
            return True, ""

        if enable:
            cli.setting_autofailover(0, 60)
            if ipv4_only:
                self.log.info("Enforcing IPv4")
                _, _, success = cli.set_address_family("ipv4only")
                if not success:
                    return False, "Unable to change ip-family to ipv4only"
            if ipv6_only:
                self.log.info("Enforcing IPv6")
                _, _, success = cli.set_address_family("ipv6only")
                if not success:
                    return False, "Unable to change ip-family to ipv6only"

            cli.setting_autofailover(1, 60)
            sleep(2)

            status, msg = check_enforcement(ipv4_only, ipv6_only)
            if not status:
                return status, msg
            else:
                return True, ""
        else:
            status, msg = check_enforcement(ipv4_only, ipv6_only)
            if not status:
                return status, msg
            else:
                cli.setting_autofailover(0, 60)
                if ipv4_only:
                    cli.set_address_family("ipv4")
                if ipv6_only:
                    cli.set_address_family("ipv6")
                stdout, _, success = cli.get_address_family()
                cli.setting_autofailover(1, 60)
                if ipv4_only:
                    if stdout[0] == "Cluster using ipv4":
                        return True, ""
                    else:
                        return False, "Failed to change IP family"
                if ipv6_only:
                    if stdout[0] == "Cluster using ipv6":
                        return True, ""
                    else:
                        return False, "Failed to change IP family"
    """
          Method to perform operation against indexer id obj
          arguments:
                   a. timeout
                   b. operation to perform i.e. compactAll, evictAll, persistAll and compressAll
          return: void
      """

    def indexer_id_ops(self, node, ops='compactAll', mvcc_purge_ratio=0.0, timeout=120, sleep_time=5):
        generic_url = "http://%s:%s/"
        ip = node.ip
        port = constants.index_port
        indexUrl = generic_url % (ip, port)
        api = "{0}plasmaDiag".format(indexUrl)
        self.log.debug("Api used {}".format(api))
        command = {'Cmd': 'listDBs'}
        rest = RestConnection(node)
        status, content, header = rest._http_request(api, 'POST', json.dumps(command), timeout=timeout)
        for l in list(iter(str(content).splitlines())):
            try:
                x, id = l.split(" : ")
                if id:
                    self.log.info("Triggering %s for instance id %s" % (ops, id))
                    if ops == 'purge':
                        compact_command = {'Cmd': ops, 'Args': [id,mvcc_purge_ratio]}
                    else:
                        compact_command = {'Cmd': ops, 'Args': [id]}
                    self.log.debug("compact command {}".format(compact_command))
                    status, content, header = rest._http_request(api, 'POST', json.dumps(compact_command))
                    self.log.debug("Triggering passed with status as {}".format(status))
                    sleep(sleep_time, "waiting in between before triggering another ops")
                    if not status:
                        self.log.error("Failed to trigger compaction :%s" % content)
            except ValueError:
                self.log.error("Issue with rest connection with plasma")
                pass
