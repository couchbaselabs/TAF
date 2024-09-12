from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from rebalance_new.rebalance_base import RebalanceBaseTest
from shell_util.remote_connection import RemoteMachineShellConnection


class NegativeRebalanceTests(RebalanceBaseTest):
    def setUp(self):
        super(NegativeRebalanceTests, self).setUp()
        self.rest = ClusterRestAPI(self.cluster.master)

    def tearDown(self):
        super(NegativeRebalanceTests, self).tearDown()

    def pass_no_arguments(self):
        status, _ = self.rest.rebalance(known_nodes=[])
        self.assertFalse(status, "Rebalance did not fail as expected")

    def add_no_nodes(self):
        nodes = self.get_nodes()
        status, resp = self.rest.rebalance(known_nodes=nodes, eject_nodes=[])
        self.assertTrue(status, "Rebalance did not fail as expected")
        self.assertTrue("empty_known_nodes" in str(resp),
                        f"Mismatch in error response: {resp}")

    def remove_all_nodes(self):
        nodes = self.get_nodes()
        status, resp = self.rest.rebalance(known_nodes=nodes,
                                           eject_nodes=nodes)
        self.assertTrue(status, "Rebalance did not fail as expected")
        self.assertTrue("No active nodes" in str(resp),
                        f"Mismatch in error response: {resp}")

    def pass_non_existant_nodes(self):
        status, resp = self.rest.rebalance(known_nodes=['non-existant'],
                                           eject_nodes=['non-existant'])
        self.assertFalse(status, "Rebalance did not fail as expected")
        self.assertTrue("mismatch" in str(resp),
                        f"Mismatch in error response: {resp}")

    def non_existant_recovery_bucket(self):
        nodes = self.get_nodes()
        chosen = self.cluster_util.pick_nodes(self.cluster.master,
                                              howmany=1)
        # Mark Node for failover
        success, _ = self.rest.perform_hard_failover(chosen[0].id)
        # Mark Node for full recovery
        if success:
            self.rest.set_recovery_type(otp_node=chosen[0].id,
                                        recovery_type="delta")
        status, resp = self.rest.rebalance(
            known_nodes=nodes,
            eject_nodes=nodes[1:],
            delta_recovery_buckets=['non-existant'])
        self.assertFalse(status, "Rebalance did not fail as expected")
        self.assertTrue("deltaRecoveryNotPossible" in str(resp),
                        f"Mismatch in error response: {resp}")

    def not_ready_for_recovery(self):
        nodes = self.get_nodes()
        chosen = self.cluster_util.pick_nodes(self.cluster.master,
                                              howmany=1)
        # Mark Node for failover
        success = self.rest.perform_hard_failover(chosen[0].id)
        self.stop_server(self.servers[1])
        # Mark Node for full recovery
        if success:
            self.rest.set_recovery_type(otp_node=chosen[0].id,
                                        recovery_type="delta")
        status, _ = self.rest.rebalance(known_nodes=nodes,
                                        eject_nodes=nodes[1:])
        self.start_server(self.servers[1])
        self.assertFalse(status, "Rebalance did not fail as expected ")

    def node_down_cannot_rebalance(self):
        nodes = self.get_nodes()
        self.stop_server(self.servers[1])
        status, _ = self.rest.rebalance(known_nodes=nodes,
                                        eject_nodes=nodes[1:])
        self.start_server(self.servers[1])
        self.assertFalse(status, "Rebalance did not fail as expected")

    def rebalance_running_cannot_rebalance(self):
        nodes = self.get_nodes()
        status, _ = self.rest.rebalance(known_nodes=nodes,
                                        eject_nodes=nodes[1:])
        self.assertTrue(status, "Rebalance did not start as expected")
        status, _ = self.rest.rebalance(known_nodes=nodes,
                                        eject_nodes=nodes[1:])
        self.assertFalse(status, "Rebalance did not fail as expected")

    def rebalance_graceful_failover_running_cannot_rebalance(self):
        nodes = self.get_nodes()
        chosen = self.cluster_util.pick_nodes(self.cluster.master, howmany=1)
        _ = self.rest.perform_graceful_failover(chosen[0].id)
        status, _ = self.rest.rebalance(known_nodes=nodes,
                                        eject_nodes=nodes[1:])
        self.assertFalse(status, "Rebalance did not fail as expected")

    def get_nodes(self):
        return [node.id for node in
                self.cluster_util.get_nodes(self.cluster.master)]

    def stop_server(self, node):
        """ Method to stop a server which is subject to failover """
        for server in self.servers:
            if server.ip == node.ip:
                shell = RemoteMachineShellConnection(server)
                if shell.is_couchbase_installed():
                    shell.stop_couchbase()
                    self.log.info("Couchbase stopped")
                else:
                    shell.stop_membase()
                    self.log.info("Membase stopped")
                shell.disconnect()
                break

    def start_server(self, node):
        """ Method to stop a server which is subject to failover """
        for server in self.servers:
            self.cluster_util.start_couchbase_server()
            if server.ip == node.ip:
                shell = RemoteMachineShellConnection(server)
                if shell.is_couchbase_installed():
                    shell.start_couchbase()
                    self.log.info("Couchbase started")
                else:
                    shell.start_membase()
                    self.log.info("Membase started")
                shell.disconnect()
                break
