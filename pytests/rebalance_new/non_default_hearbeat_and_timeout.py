import time

import Jython_tasks.task as jython_tasks
from cb_tools.cbstats import Cbstats
from couchbase_helper.documentgenerator import doc_generator
from membase.api.rest_client import RestConnection
from rebalance_base import RebalanceBaseTest
from remote.remote_util import RemoteMachineShellConnection
from sdk_exceptions import SDKException
from rebalance_new import rebalance_base
import datetime

retry_exceptions = rebalance_base.retry_exceptions +\
                    [SDKException.RequestCanceledException]


class NonDefaultOrchestratorHeartbeatAndTimeout(RebalanceBaseTest):
    def setUp(self):
        super(NonDefaultOrchestratorHeartbeatAndTimeout, self).setUp()
        self.rest = RestConnection(self.servers[0])
        self.rest.update_autofailover_settings(True, 5)
        self.heartbeat_interval = self.input.param("heartbeat_interval", 500)
        self.timeout_interval_count = self.input.param("timeout_interval_count", 3)
        self.lease_time = self.input.param("lease_time", 5000)
        self.lease_grace_time = self.input.param("lease_grace_time", 2000)
        self.lease_renew_after = self.input.param("lease_renew_after", 500)
        self.set_non_default_orchestrator_heartbeats_and_timeouts()

    def tearDown(self):
        self.remove_network_split()
        for server in self.servers:
            remote = RemoteMachineShellConnection(server)
            self.log.info("Starting couchbase server on : {0}".format(server.ip))
            remote.start_couchbase()
            remote.disconnect()
        super(NonDefaultOrchestratorHeartbeatAndTimeout, self).tearDown()

    def set_non_default_orchestrator_heartbeats_and_timeouts(self):
        shell = RemoteMachineShellConnection(self.cluster.master)
        shell.enable_diag_eval_on_non_local_hosts()
        shell.disconnect()
        self.rest_client = RestConnection(self.cluster.master)
        if self.heartbeat_interval:
            self.rest_client.set_heartbeat_interval(self.heartbeat_interval)
        if self.timeout_interval_count:
            self.rest_client.set_timeout_interval_count(self.timeout_interval_count)
        if self.lease_time:
            self.rest_client.set_lease_time(self.lease_time)
        if self.lease_grace_time:
            self.rest_client.set_lease_grace_time(self.lease_grace_time)
        if self.lease_renew_after:
            self.rest_client.set_lease_renew_after(self.lease_renew_after)

    def get_node(self, other_server, orchestrator=True):
        self.rest = RestConnection(other_server)
        output = self.rest.get_terse_cluster_info()
        orchestrator_node = output["orchestrator"]
        self.log.info("output : {0}".format(output))
        self.log.info("Orchestrator : {0}".format(orchestrator_node))
        self.cluster.master = other_server
        nodes = self.cluster_util.get_nodes(other_server)
        if orchestrator:
            for node in nodes:
                if orchestrator_node == node.id:
                    self.log.info("Returned Node : {0}".format(orchestrator_node))
                    return node
        else:
            for node in nodes:
                if orchestrator != node.id:
                    self.log.info("Returned Node : {0}".format(node))
                    return node

    def get_server_info(self, node):
        for server in self.servers:
            if server.ip == node.ip:
                return server

    def get_other_server_info(self, node):
        for server in self.cluster_util.get_kv_nodes():
            if server.ip != node.ip:
                return server

    def kill_babysittter_process(self, target_node):
        node = self.get_server_info(target_node)
        remote = RemoteMachineShellConnection(node)
        self.log.info("Killing babysitter on : {0}".format(node.ip))
        remote.kill_babysitter()
        remote.disconnect()

    def get_failover_count(self, node):
        rest = RestConnection(node)
        cluster_status = rest.cluster_status()
        failover_count = 0
        # check for inactiveFailed
        for node in cluster_status['nodes']:
            if node['clusterMembership'] == "inactiveFailed":
                failover_count += 1
        return failover_count

    def network_split(self, node1, node2):
        node1_server_info = self.get_server_info(node1)
        node2_server_info = self.get_server_info(node2)
        shell = RemoteMachineShellConnection(node1_server_info)
        self.log.debug("Blocking traffic from {0} in {1}"
                            .format(node2_server_info.ip, node1_server_info.ip))
        command = "iptables -A INPUT -s {0} -j DROP".format(node2_server_info.ip)
        shell.execute_command(command)
        shell.disconnect()

    def remove_network_split(self):
        for node in self.servers:
            shell = RemoteMachineShellConnection(node)
            command = "/sbin/iptables -F"
            shell.execute_command(command)
            shell.disconnect()

    def split_brain(self, node1, node2):
        self.network_split(node1, node2)
        self.network_split(node2, node1)

    def test_validate_new_leader_to_failover_quickly_when_configured(self):
        self.rest.update_autofailover_settings(False, 5)
        count = 0
        orchestrator = self.get_node(self.servers[0], orchestrator=True)
        other_server = self.get_other_server_info(orchestrator)
        other_server_conn = RestConnection(other_server)
        self.log.info("Chosen orchestrator: {0}".format(orchestrator.id))
        self.log.info("Other node: {0}".format(other_server))
        # kill babysitter process on the orchestrator
        self.kill_babysittter_process(orchestrator)
        before_kill = datetime.datetime.now()
        self.sleep(1)
        other_server_conn.fail_over(orchestrator.id, graceful=False)
        new_orchestrator = self.get_node(other_server, orchestrator=True)
        after_kill = datetime.datetime.now()
        while new_orchestrator == "undefined" and count < 100:
            new_orchestrator = self.get_node(other_server, orchestrator=True)
            after_kill = datetime.datetime.now()
            count += 1
        if count == 100:
            self.fail("New leader did not standup")
        else:
            if new_orchestrator != "undefined":
                self.log.info("New leader stood up in : {0}".format(after_kill-before_kill))
            else:
                self.fail("New leader did not standup")
        self.log.info("Chosen orchestrator after killing previous orchestrator: {0}".format(new_orchestrator.id))

    def test_validate_new_leader_to_stand_up_quickly_when_configured(self):
        count = 0
        orchestrator = self.get_node(self.servers[0], orchestrator=True)
        other_server = self.get_other_server_info(orchestrator)
        self.log.info("Chosen orchestrator: {0}".format(orchestrator.id))
        self.log.info("Other node: {0}".format(other_server))
        # kill babysitter process on the orchestrator
        self.kill_babysittter_process(orchestrator)
        before_kill = datetime.datetime.now()
        failover_count = self.get_failover_count(other_server)
        after_kill = datetime.datetime.now()
        while failover_count != 1 and count < 100:
            failover_count = self.get_failover_count(other_server)
            self.log.info("failover count : {0}".format(failover_count))
            self.sleep(1)
            after_kill = datetime.datetime.now()
            count += 1
        if count == 100:
            self.fail("New leader did not standup")
        else:
            new_orchestrator = self.get_node(other_server, orchestrator=True)
            if new_orchestrator != "undefined":
                self.log.info("New leader stood up in : {0}".format(after_kill-before_kill))
            else:
                self.fail("New leader did not standup")
        self.log.info("Chosen orchestrator after killing previous orchestrator: {0}".format(new_orchestrator.id))

    def test_validate_new_leader_with_network_split(self):
        count = 0
        self.network_split_type = self.input.param("network_split_type", 1)
        orchestrator = self.get_node(self.servers[0], orchestrator=True)
        other_server = self.get_other_server_info(orchestrator)
        other_server_conn = RestConnection(other_server)
        self.log.info("Chosen orchestrator: {0}".format(orchestrator.id))
        self.log.info("Other node: {0}".format(other_server))
        # network_split
        if self.network_split_type == 1:
            self.network_split(orchestrator, other_server)
        elif self.network_split_type == 2:
            self.network_split(other_server, orchestrator)
        elif self.network_split_type == 3:
            self.split_brain(orchestrator, other_server)
        self.sleep(30)
        before_kill = datetime.datetime.now()
        try:
          self.sleep(1)
          other_server_conn.fail_over(orchestrator.id, graceful=False, allowUnsafe=True)
        except Exception:
           pass
        self.remove_network_split()
        failover_count = self.get_failover_count(other_server)
        after_kill = datetime.datetime.now()
        while failover_count != 1 and count < 100:
            failover_count = self.get_failover_count(other_server)
            self.log.info("failover count : {0}".format(failover_count))
            self.sleep(1)
            after_kill = datetime.datetime.now()
            count += 1
        if count == 100:
            self.fail("New leader did not standup")
        else:
            new_orchestrator = self.get_node(other_server, orchestrator=True)
            if new_orchestrator != "undefined":
                self.log.info("New leader stood up in : {0}".format(after_kill-before_kill))
            else:
                self.fail("New leader did not standup")
        self.log.info("Chosen orchestrator after killing previous orchestrator: {0}".format(new_orchestrator.id))