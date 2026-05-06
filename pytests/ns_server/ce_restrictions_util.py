"""
Community Edition Restrictions Utility Module

Helper methods for CE restriction testing:
- Node management (add, eject, rebalance)
- CE edition verification
- Service restriction validation
"""

from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from cb_tools.cb_cli import CbCli
from couchbase_utils.cluster_utils.cluster_ready_functions import ClusterUtils
from couchbase_utils.rebalance_utils.rebalance_util import RebalanceUtil
from custom_exceptions.exception import RebalanceFailedException
from shell_util.remote_connection import RemoteMachineShellConnection


class CommunityEditionRestrictionsUtil:
    """Utility class for CE restriction test operations."""

    CE_NODE_LIMIT = 5
    CE_REBALANCE_ERROR_MSG = "Cannot rebalance with more than 5"

    def __init__(self, cluster, cluster_util, log, sleep_func):
        self.cluster = cluster
        self.cluster_util = cluster_util
        self.log = log
        self.sleep = sleep_func
        self.rest = ClusterRestAPI(cluster.master)

    def verify_ce_edition_via_diag_eval(self):
        """Verify cluster is running CE via diag/eval."""
        shell = RemoteMachineShellConnection(self.cluster.master)
        shell.enable_diag_eval_on_non_local_hosts()
        shell.disconnect()

        status, content = self.rest.diag_eval(
            "cluster_compat_mode:is_enterprise().")
        if not status:
            raise AssertionError("Failed to execute diag/eval")

        is_ce = content is False if isinstance(content, bool) \
            else str(content).strip().lower() == "false"

        if not is_ce:
            raise AssertionError("Expected CE edition. Got: %s" % content)

        self.log.info("Verified CE edition via diag/eval")
        return True

    def add_node_without_rebalance(self, node, services=None):
        """Add a node without triggering rebalance."""
        services = services or ["kv"]
        self.log.info("Adding node %s with services %s", node.ip, services)
        try:
            return self.cluster_util.add_node(
                self.cluster, node, services=services, rebalance=False)
        except Exception as e:
            raise AssertionError("Failed to add node %s: %s" % (node.ip, e))

    def verify_node_in_pending_state(self, node_ip):
        """Verify node is in inactiveAdded (pending) state."""
        nodes = ClusterUtils.get_nodes(self.cluster.master, inactive_added=True)
        for node in nodes:
            if node.ip == node_ip:
                if node.clusterMembership != "inactiveAdded":
                    raise AssertionError(
                        "Node %s expected 'inactiveAdded', got '%s'"
                        % (node_ip, node.clusterMembership))
                self.log.info("Node %s is in pending state", node_ip)
                return True
        raise AssertionError("Node %s not found in cluster" % node_ip)

    def attempt_rebalance_expect_failure(self, expected_error=None):
        """Attempt rebalance expecting CE restriction failure."""
        expected_error = expected_error or self.CE_REBALANCE_ERROR_MSG
        nodes = ClusterUtils.get_nodes(self.cluster.master, inactive_added=True)
        known_nodes = [n.id for n in nodes]

        self.log.info("Attempting rebalance with %d nodes (expecting failure)",
                      len(nodes))

        status, content = self.rest.rebalance(known_nodes=known_nodes,
                                              eject_nodes=[])
        if not status:
            error_msg = content.decode('utf-8') if isinstance(content, bytes) \
                else str(content)
            if expected_error not in error_msg:
                raise AssertionError("Expected CE error. Got: %s" % error_msg)
            self.log.info("Rebalance rejected: %s", error_msg)
            return error_msg

        # Monitor if rebalance started
        try:
            RebalanceUtil(self.cluster).monitor_rebalance()
            raise AssertionError("Rebalance should have failed but succeeded")
        except RebalanceFailedException as e:
            error_msg = str(e)
            if expected_error not in error_msg:
                raise AssertionError("Expected CE error. Got: %s" % error_msg)
            self.log.info("Rebalance failed: %s", error_msg)
            return error_msg

    def restart_couchbase_server(self, server):
        """Restart Couchbase Server and wait for ready."""
        self.log.info("Restarting server %s", server.ip)
        shell = RemoteMachineShellConnection(server)
        shell.restart_couchbase()
        shell.disconnect()
        self.cluster_util.wait_for_ns_servers_or_assert([server], wait_time=120)
        self.log.info("Server %s ready", server.ip)

    def rebalance_out_node(self, node_to_remove):
        """Rebalance out a node from the cluster."""
        nodes = ClusterUtils.get_nodes(self.cluster.master, inactive_added=True)
        otp_node = next((n.id for n in nodes if n.ip == node_to_remove.ip), None)

        if not otp_node:
            raise AssertionError("Node %s not found" % node_to_remove.ip)

        self.log.info("Rebalancing out %s", node_to_remove.ip)
        result = ClusterUtils.rebalance(
            self.cluster, wait_for_completion=True, ejected_nodes=[otp_node])

        if not result:
            raise AssertionError("Rebalance-out failed")

        self.cluster_util.update_cluster_nodes_service_list(self.cluster)
        self.log.info("Removed node %s", node_to_remove.ip)

    def get_active_node_count(self):
        """Get count of active nodes in cluster."""
        return len(ClusterUtils.get_nodes(self.cluster.master))

    def cleanup_pending_nodes(self):
        """Eject all pending (inactiveAdded) nodes."""
        cleaned = 0
        try:
            nodes = ClusterUtils.get_nodes(self.cluster.master,
                                           inactive_added=True)
            pending = [n for n in nodes if n.clusterMembership == "inactiveAdded"]
            for node in pending:
                try:
                    self.rest.eject_node(node.id)
                    cleaned += 1
                except Exception as e:
                    self.log.warning("Failed to eject %s: %s", node.id, e)
            if cleaned:
                self.log.info("Cleaned up %d pending nodes", cleaned)
        except Exception as e:
            self.log.warning("Cleanup error: %s", e)
        return cleaned

    def add_node_via_cli(self, node, services, expect_success=True):
        """
        Add node via couchbase-cli server-add.
        Returns (success, error_msg) tuple.
        """
        self._eject_node_by_ip(node.ip)
        self.sleep(8, "Wait after ejection")

        shell = RemoteMachineShellConnection(self.cluster.master)
        cb_cli = CbCli(shell, username=self.cluster.master.rest_username,
                       password=self.cluster.master.rest_password)

        try:
            output = cb_cli.add_node(node, services)
            output_str = "\n".join(output) if isinstance(output, list) \
                else str(output)
            shell.disconnect()

            if "ERROR:" in output_str:
                raise Exception(output_str)

            # Success - cleanup
            self._eject_node_by_ip(node.ip)
            self.sleep(10, "Wait after ejection")
            return (True, "") if expect_success else (False, "Should be rejected")

        except Exception as e:
            shell.disconnect()
            error_msg = str(e)
            return (True, error_msg) if not expect_success else (False, error_msg)

    def _eject_node_by_ip(self, node_ip):
        """Eject a pending node by IP."""
        try:
            nodes = ClusterUtils.get_nodes(self.cluster.master,
                                           inactive_added=True)
            for n in nodes:
                if n.ip == node_ip:
                    self.rest.eject_node(n.id)
                    return True
        except Exception as e:
            self.log.warning("Failed to eject %s: %s", node_ip, e)
        return False

    def add_node_via_rest_and_rebalance_in(self, node, services):
        """Add node via REST and rebalance in."""
        self._eject_node_by_ip(node.ip)
        self.sleep(5, "Wait after ejection")

        self.log.info("Adding %s with services=%s", node.ip, services)
        status, content = self.rest.add_node(
            host_name=node.ip,
            username=node.rest_username,
            password=node.rest_password,
            services=services)

        if not status:
            raise AssertionError("Failed to add node: %s" % content)

        self.cluster_util.update_cluster_nodes_service_list(
            self.cluster, inactive_added=True)
        self.verify_node_in_pending_state(node.ip)

        nodes = ClusterUtils.get_nodes(self.cluster.master, inactive_added=True)
        known_nodes = [n.id for n in nodes]

        status, content = self.rest.rebalance(known_nodes=known_nodes,
                                              eject_nodes=[])
        if not status:
            raise AssertionError("Failed to start rebalance: %s" % content)

        if not RebalanceUtil(self.cluster).monitor_rebalance():
            raise AssertionError("Rebalance-in failed")

        self.cluster_util.update_cluster_nodes_service_list(self.cluster)
