# -*- coding: utf-8 -*-
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from rebalance_utils.rebalance_util import RebalanceUtil
from failover.failoverbasetest import FailoverBaseTest
from shell_util.remote_connection import RemoteMachineShellConnection


class NegativeFailoverTests(FailoverBaseTest):
    def setUp(self):
        super(NegativeFailoverTests, self).setUp()
        self.cluster.master = self.servers[0]

    def tearDown(self):
        super(NegativeFailoverTests, self).tearDown()

    def graceful_failover_when_rebalance_running(self):
        self.rest = ClusterRestAPI(self.cluster.master)
        nodes = self.cluster_util.get_nodes(self.cluster.master)
        chosen = self.cluster_util.pick_nodes(self.cluster.master,
                                              howmany=1)
        status, _ = self.rest.rebalance(
            known_nodes=[node.id for node in nodes],
            eject_nodes=[node.id for node in nodes[1:]])
        self.assertTrue(status, "Rebalance did not run ")
        status, content = self.rest.perform_graceful_failover(chosen[0].id)
        self.assertFalse(status, "Failover did not fail as expected")
        self.assertTrue(("Rebalance running" in str(content)),
                        f"Unexpected exception {content}")

    def graceful_failover_when_graceful_failover_running(self):
        self.rest = ClusterRestAPI(self.cluster.master)
        chosen = self.cluster_util.pick_nodes(self.cluster.master,
                                              howmany=1)
        status, _ = self.rest.perform_graceful_failover(chosen[0].id)
        self.assertTrue(status, "Failover failed")
        status, content = self.rest.perform_graceful_failover(chosen[0].id)
        self.assertFalse(status, "Failover did not fail as expected")
        self.assertTrue(("Rebalance running" in str(content)),
                        f"Unexpected exception {content}")

    def hard_failover_when_graceful_failover_running(self):
        self.rest = ClusterRestAPI(self.cluster.master)
        chosen = self.cluster_util.pick_nodes(self.cluster.master,
                                              howmany=1)
        status, _ = self.rest.perform_graceful_failover(chosen[0].id)
        self.assertTrue(status, "Failover failed")
        status, content = self.rest.perform_hard_failover(chosen[0].id)
        self.assertFalse(status,"Failover did not fail as expected")
        self.assertTrue(("Rebalance running" in str(content)),
                        f"Unexpected exception {content}")

    def hard_failover_nonexistant_node(self):
        self.rest = ClusterRestAPI(self.cluster.master)
        status, content = self.rest.perform_hard_failover("non-existant")
        self.assertFalse(status,"Failover did not fail as expected ")
        self.assertTrue(("Unknown server given" in str(content)),
                        f"Unexpected exception {content}")

    def graceful_failover_nonexistant_node(self):
        self.rest = ClusterRestAPI(self.cluster.master)
        status, content = self.rest.perform_graceful_failover("non-existant")
        self.assertFalse(status, "Failover did not fail as expected")
        self.assertTrue(("Unknown server given" in str(content)),
                        f"Unexpected exception {content}")

    def failover_failed_node(self):
        self.rest = ClusterRestAPI(self.cluster.master)
        chosen = self.cluster_util.pick_nodes(self.cluster.master,
                                              howmany=1)
        status, _ = self.rest.perform_hard_failover(chosen[0].id)
        self.assertTrue(status,"Failover did not happen as expected")
        status, content = self.rest.perform_hard_failover(chosen[0].id)
        self.assertFalse(status, "Failover of already failed node succeeded")
        self.assertTrue(("Inactive server given" in str(content)),
                        f"Unexpected exception {content}")

    def addback_non_existant_node(self):
        self.rest = ClusterRestAPI(self.cluster.master)
        chosen = self.cluster_util.pick_nodes(self.cluster.master,
                                              howmany=1)
        # Mark Node for failover
        status, content = self.rest.perform_hard_failover(chosen[0].id)
        # Mark Node for full recovery
        if status:
            _, content = self.rest.set_recovery_type(otp_node="non-existant",
                                                     recovery_type="delta")
        self.assertTrue(("invalid node name or node" in str(content)),
                        f"Unexpected exception {content}")

    def addback_an_unfailed_node(self):
        self.rest = ClusterRestAPI(self.cluster.master)
        chosen = self.cluster_util.pick_nodes(self.cluster.master,
                                              howmany=1)
        status, content = self.rest.set_recovery_type(otp_node=chosen[0].id,
                                                      recovery_type="delta")
        self.assertTrue(("invalid node name or node" in str(content)),
                        f"Unexpected exception {content}")

    def addback_with_incorrect_recovery_type(self):
        self.rest = ClusterRestAPI(self.cluster.master)
        chosen = self.cluster_util.pick_nodes(self.cluster.master,
                                              howmany=1)
        # Mark Node for failover
        status, content = self.rest.perform_hard_failover(chosen[0].id)
        # Mark Node for full recovery
        if status:
            _, content = self.rest.set_recovery_type(otp_node=chosen[0].id,
                                                     recovery_type="xxx")
        self.assertTrue(("recoveryType" in str(content)),
                        f"Unexpected exception {content}")

    def failure_recovery_delta_node_with_failover_node(self):
        self.rest = ClusterRestAPI(self.cluster.master)
        chosen = self.cluster_util.pick_nodes(self.cluster.master,
                                              howmany=2)
        # Mark Node(s) for failover
        # success_failed_over
        _ = self.rest.perform_hard_failover(chosen[0].id)
        status, content = self.rest.perform_hard_failover(chosen[1].id)
        # Mark Node for full recovery
        if status:
            status, _ = self.rest.set_recovery_type(otp_node=chosen[0].id,
                                                    recovery_type="delta")
            self.assertTrue(status,
                            f"Delta recovery failed for {chosen[0].id}")
        known_nodes = [node.id for node in
                       self.cluster_util.get_nodes(self.cluster.master,
                                                   inactive_added=True,
                                                   inactive_failed=True)]
        status, content = self.rest.rebalance(known_nodes=known_nodes,
                                              eject_nodes=[chosen[1].id])
        self.assertTrue(("deltaRecoveryNotPossible" in str(content)),
                        f"Unexpected exception {content}")

    def failure_recovery_delta_node_before_rebalance_in(self):
        self.rest = ClusterRestAPI(self.cluster.master)
        chosen = self.cluster_util.pick_nodes(self.cluster.master,
                                              howmany=1)
        # Mark Node for failover
        status, _ = self.rest.perform_hard_failover(chosen[0].id)
        # Mark Node for full recovery
        if status:
            status, _ = self.rest.set_recovery_type(otp_node=chosen[0].id,
                                                    recovery_type="delta")
            self.assertTrue(status,
                            f"Delta recovery failed for {chosen[0].id}")

        node = self.servers[self.nodes_init]
        self.rest.add_node(host_name=node.ip, username=node.rest_username,
                           password=node.rest_password)
        known_nodes = [node.id for node in
                       self.cluster_util.get_nodes(self.cluster.master,
                                                   inactive_added=True,
                                                   inactive_failed=True)]
        status, content = self.rest.rebalance(known_nodes=known_nodes)
        self.assertTrue(("deltaRecoveryNotPossible" in str(content)),
                        f"Unexpected exception {content}")

    def failure_recovery_delta_node_after_add_node(self):
        self.rest = ClusterRestAPI(self.cluster.master)
        chosen = self.cluster_util.pick_nodes(self.cluster.master,
                                              howmany=1)
        self.rest.add_node(self.servers[self.nodes_init].ip,
                           self.cluster.master.rest_username,
                           self.cluster.master.rest_password)
        # Mark Node for failover
        status, _ = self.rest.perform_hard_failover(chosen[0].id)
        # Mark Node for full recovery
        if status:
            status, _ = self.rest.set_recovery_type(
                otp_node=chosen[0].id, recovery_type="delta")
            self.assertTrue(status,
                            f"Delta recovery failed for {chosen[0].id}")
        known_nodes = [node.id for node in
                       self.cluster_util.get_nodes(self.cluster.master,
                                                   inactive_added=True,
                                                   inactive_failed=True)]
        status, content = self.rest.rebalance(known_nodes=known_nodes)
        self.assertTrue(("deltaRecoveryNotPossible" in str(content)),
                        f"Unexpected exception {content}")

    def failure_recovery_delta_node_after_eject_node(self):
        reb_util = RebalanceUtil(self.cluster)
        self.rest = ClusterRestAPI(self.cluster.master)
        eject_out_node = self.cluster_util.find_node_info(
            self.cluster.master,
            self.servers[self.nodes_init-1])
        chosen = self.cluster_util.pick_nodes(self.cluster.master,
                                              howmany=1)
        self.rest.eject_node(self.servers[self.nodes_init-1])
        # Mark Node for failover
        status, _ = self.rest.perform_hard_failover(chosen[0].id)
        # Mark Node for full recovery
        if status:
            self.rest.set_recovery_type(otp_node=chosen[0].id,
                                        recovery_type="delta")
        self.nodes = self.cluster_util.get_nodes(self.cluster.master)
        self.rest.rebalance(known_nodes=[node.id for node in self.nodes],
                            eject_nodes=[chosen[0].id, eject_out_node.id])
        self.assertFalse(reb_util.monitor_rebalance(stop_if_loop=True),
                         msg="Rebalance did not fail as expected")

    def failure_recovery_delta_node_before_rebalance_in_out(self):
        content = None
        reb_util = RebalanceUtil(self.cluster)
        self.rest = ClusterRestAPI(self.cluster.master)
        chosen = self.cluster_util.pick_nodes(self.cluster.master,
                                              howmany=1)
        # Mark Node for failover
        status, _ = self.rest.perform_hard_failover(chosen[0].id)
        # Mark Node for full recovery
        if status:
            status, content = self.rest.set_recovery_type(
                otp_node=chosen[0].id, recovery_type="delta")
        ejected_nodes = list()
        self.rest.add_node(self.cluster.master.rest_username,
                           self.cluster.master.rest_password,
                           self.servers[self.nodes_init].ip,
                           self.servers[self.nodes_init].port)
        self.rest.add_node(self.cluster.master.rest_username,
                           self.cluster.master.rest_password,
                           self.servers[self.nodes_init+1].ip,
                           self.servers[self.nodes_init+1].port)
        self.nodes = self.cluster_util.get_nodes(self.cluster.server)
        for server in self.nodes:
            if server.ip == self.servers[self.nodes_init].ip:
                ejected_nodes.append(server.id)
        self.rest.rebalance(known_nodes=[node.id for node in self.nodes],
                            eject_nodes=ejected_nodes)
        self.assertFalse(reb_util.monitor_rebalance(stop_if_loop=True),
                         msg="Rebalance did not fail as expected")
        self.assertTrue(("deltaRecoveryNotPossible" in str(content)),
                        f"Unexpected exception {content}")

    def graceful_failover_unhealthy_node_not_allowed(self):
        try:
            self.rest = ClusterRestAPI(self.cluster.master)
            self.stop_server(self.servers[1])
            # Mark Node for failover
            chosen = self.cluster_util.pick_nodes(self.cluster.master,
                                                  howmany=1)
            status, _ = self.rest.perform_hard_failover(chosen[0].id)
            self.assertFalse(status,
                             "Graceful failover allowed for unhealthy node")
        finally:
            self.start_server(self.servers[1])

    def stop_server(self, node):
        """ Method to stop a server which is subject to failover """
        for server in self.servers:
            if server.ip == node.ip:
                shell = RemoteMachineShellConnection(server)
                if shell.is_couchbase_installed():
                    shell.stop_couchbase()
                    self.log.info("Couchbase stopped")
                shell.disconnect()

    def start_server(self, node):
        """ Method to stop a server which is subject to failover """
        for server in self.servers:
            if server.ip == node.ip:
                shell = RemoteMachineShellConnection(server)
                if shell.is_couchbase_installed():
                    shell.start_couchbase()
                    self.log.info("Couchbase started")
                shell.disconnect()
                break
