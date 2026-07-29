"""
Community Edition Restrictions Utility Module

Helper methods for CE restriction testing:
- Node management (add, eject, rebalance)
- CE edition verification
- Service restriction validation

Ported from TAF master's pytests/ns_server/ce_restrictions_util.py.

master_jython lacks the newer cb_server_rest_util wrapper modules
(ClusterRestAPI / ClusterInitializationProvision) and
couchbase_utils/rebalance_utils/rebalance_util.py that master's version
depends on. This port is rewritten against the classic monolithic
RestConnection class (lib/membase/api/rest_client.py) instead:

- ClusterRestAPI(master)              -> RestConnection(master)
- ClusterUtils.get_nodes(master, inactive_added=True)
                                       -> RestConnection.get_nodes(inactive_added=True)
  (ClusterUtils.get_nodes() on this branch takes no inactive_added kwarg;
  it always calls RestConnection.get_nodes() with defaults, so pending
  nodes must be fetched directly off the rest handle instead.)
- RestConnection.add_node()/eject_node() raise exceptions on failure
  instead of returning a (status, content) tuple, so call sites that
  need the failure message wrap the call in try/except.
- RebalanceUtil(cluster).monitor_rebalance() -> RestConnection.monitorRebalance()
  (both ultimately raise custom_exceptions.exception.RebalanceFailedException
  with the server's errorMessage on rebalance failure).
- shell_util.remote_connection.RemoteMachineShellConnection
                                       -> remote.remote_util.RemoteMachineShellConnection
"""

from cluster_utils.cluster_ready_functions import ClusterUtils
from cb_tools.cb_cli import CbCli
from custom_exceptions.exception import RebalanceFailedException
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection


class CommunityEditionRestrictionsUtil(object):
    """Utility class for CE restriction test operations."""

    CE_NODE_LIMIT = 5
    CE_REBALANCE_ERROR_MSG = "Cannot rebalance with more than 5"

    def __init__(self, cluster, cluster_util, log, sleep_func):
        self.cluster = cluster
        self.cluster_util = cluster_util
        self.log = log
        self.sleep = sleep_func
        self.rest = RestConnection(cluster.master)

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
        self.log.info("Adding node %s with services %s" % (node.ip, services))
        try:
            return self.cluster_util.add_node(
                self.cluster, node, services=services, rebalance=False)
        except Exception as e:
            raise AssertionError("Failed to add node %s: %s" % (node.ip, e))

    def verify_node_in_pending_state(self, node_ip):
        """Verify node is in inactiveAdded (pending) state."""
        nodes = self.rest.get_nodes(inactive_added=True)
        for node in nodes:
            if node.ip == node_ip:
                if node.clusterMembership != "inactiveAdded":
                    raise AssertionError(
                        "Node %s expected 'inactiveAdded', got '%s'"
                        % (node_ip, node.clusterMembership))
                self.log.info("Node %s is in pending state" % node_ip)
                return True
        raise AssertionError("Node %s not found in cluster" % node_ip)

    def attempt_rebalance_expect_failure(self, expected_error=None):
        """Attempt rebalance expecting CE restriction failure.

        RestConnection.rebalance() returns (status, content) and never
        raises on a rejected start. If the start is accepted, the failure
        (if any) is only reported once we poll progress, which is done
        here via monitorRebalance() -- this raises RebalanceFailedException
        with the server's errorMessage once ns_server reports the
        rebalance task in an error state.
        """
        expected_error = expected_error or self.CE_REBALANCE_ERROR_MSG
        nodes = self.rest.get_nodes(inactive_added=True)
        known_nodes = [n.id for n in nodes]

        self.log.info("Attempting rebalance with %d nodes (expecting failure)"
                      % len(nodes))

        status, content = self.rest.rebalance(otpNodes=known_nodes,
                                              ejectedNodes=[])
        if not status:
            error_msg = content.decode('utf-8') if isinstance(content, bytes) \
                else str(content)
            if expected_error not in error_msg:
                raise AssertionError("Expected CE error. Got: %s" % error_msg)
            self.log.info("Rebalance rejected: %s" % error_msg)
            return error_msg

        # Monitor if rebalance started
        try:
            self.rest.monitorRebalance()
            raise AssertionError("Rebalance should have failed but succeeded")
        except RebalanceFailedException as e:
            error_msg = str(e)
            if expected_error not in error_msg:
                raise AssertionError("Expected CE error. Got: %s" % error_msg)
            self.log.info("Rebalance failed: %s" % error_msg)
            return error_msg

    def restart_couchbase_server(self, server):
        """Restart Couchbase Server and wait for ready."""
        self.log.info("Restarting server %s" % server.ip)
        shell = RemoteMachineShellConnection(server)
        shell.restart_couchbase()
        shell.disconnect()
        self.cluster_util.wait_for_ns_servers_or_assert([server], wait_time=120)
        self.log.info("Server %s ready" % server.ip)

    def rebalance_out_node(self, node_to_remove):
        """Rebalance out a node from the cluster."""
        nodes = self.rest.get_nodes(inactive_added=True)
        otp_node = next((n.id for n in nodes if n.ip == node_to_remove.ip), None)

        if not otp_node:
            raise AssertionError("Node %s not found" % node_to_remove.ip)

        self.log.info("Rebalancing out %s" % node_to_remove.ip)
        result = ClusterUtils.rebalance(
            self.cluster, wait_for_completion=True, ejected_nodes=[otp_node])

        if not result:
            raise AssertionError("Rebalance-out failed")

        self.cluster_util.update_cluster_nodes_service_list(self.cluster)
        self.log.info("Removed node %s" % node_to_remove.ip)

    def get_active_node_count(self):
        """Get count of active nodes in cluster."""
        return len(self.rest.get_nodes())

    def cleanup_pending_nodes(self):
        """Eject all pending (inactiveAdded) nodes."""
        cleaned = 0
        try:
            nodes = self.rest.get_nodes(inactive_added=True)
            pending = [n for n in nodes if n.clusterMembership == "inactiveAdded"]
            for node in pending:
                try:
                    self.rest.eject_node(
                        user=self.cluster.master.rest_username,
                        password=self.cluster.master.rest_password,
                        otpNode=node.id)
                    cleaned += 1
                except Exception as e:
                    self.log.warning("Failed to eject %s: %s" % (node.id, e))
            if cleaned:
                self.log.info("Cleaned up %d pending nodes" % cleaned)
        except Exception as e:
            self.log.warning("Cleanup error: %s" % e)
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
            nodes = self.rest.get_nodes(inactive_added=True)
            for n in nodes:
                if n.ip == node_ip:
                    self.rest.eject_node(
                        user=self.cluster.master.rest_username,
                        password=self.cluster.master.rest_password,
                        otpNode=n.id)
                    return True
        except Exception as e:
            self.log.warning("Failed to eject %s: %s" % (node_ip, e))
        return False

    def add_node_via_rest_and_rebalance_in(self, node, services):
        """Add node via REST and rebalance in.

        :param services: comma-separated string, e.g. "kv,index,n1ql,fts"
        """
        self._eject_node_by_ip(node.ip)
        self.sleep(5, "Wait after ejection")

        self.log.info("Adding %s with services=%s" % (node.ip, services))
        try:
            self.rest.add_node(user=node.rest_username,
                               password=node.rest_password,
                               remoteIp=node.ip,
                               services=services.split(","))
        except Exception as e:
            raise AssertionError("Failed to add node: %s" % e)

        self.cluster_util.update_cluster_nodes_service_list(
            self.cluster, inactive_added=True)
        self.verify_node_in_pending_state(node.ip)

        nodes = self.rest.get_nodes(inactive_added=True)
        known_nodes = [n.id for n in nodes]

        status, content = self.rest.rebalance(otpNodes=known_nodes,
                                              ejectedNodes=[])
        if not status:
            raise AssertionError("Failed to start rebalance: %s" % content)

        try:
            reb_ok = self.rest.monitorRebalance()
        except RebalanceFailedException as e:
            raise AssertionError("Rebalance-in failed: %s" % e)
        if not reb_ok:
            raise AssertionError("Rebalance-in failed")

        self.cluster_util.update_cluster_nodes_service_list(self.cluster)
