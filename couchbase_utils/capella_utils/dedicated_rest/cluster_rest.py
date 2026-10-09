from capella_utils.dedicated import CapellaUtils
from capella_utils.dedicated_rest.context import CapellaContext
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import \
    ClusterRestAPI


class CapellaClusterRestAPI(ClusterRestAPI):
    """
    Capella dedicated variant of ClusterRestAPI.

    Returned automatically by ClusterRestAPI(server) when
    server.type == "dedicated" (see CBRestConnection.capella_variant).

    - cluster_tasks(): reports a running rebalance while the Capella
      control plane is still changing the cluster, so existing wait
      helpers (rebalance_reached, RebalanceUtil.monitor_rebalance) do not
      treat "no rebalance on the node yet" as "done".
    - Topology changes / failover by node are not exposed by Capella.
      They return (False, reason) like a failed REST call, and log a
      CAPELLA-GAP warning. Scaling goes through cluster.async_rebalance(),
      which already uses RebalanceTaskCapella for dedicated clusters.
    - Everything else is inherited and talks to the node as on-prem.

    Method names, arguments and (status, content) returns match
    ClusterRestAPI.
    """
    HEALTHY_STATE = "healthy"

    def __init__(self, server):
        # ClusterRestAPI keeps only ip/port/creds; the Capella context
        # lives on the server object, so keep a reference to it
        self.server = server
        super(CapellaClusterRestAPI, self).__init__(server)

    # POC-NOTE (remove later)
    # WHY: on Capella, a scale is accepted at once but the node only starts
    #      rebalancing minutes later. Old wait helpers saw "no rebalance"
    #      and treated it as "done" -> false pass
    # CAME FROM: test -> cluster_util.rebalance_reached()
    #      -> get_rebalance_status_and_progress() -> ClusterRestAPI(master)
    #      (became CapellaClusterRestAPI) -> cluster_tasks()
    #      also RebalanceUtil.get_rebalance_status / monitor_rebalance
    # DOES: asks the node as usual, then asks Capella for cluster state.
    #      Not "healthy" yet -> report a running rebalance so waits wait
    def cluster_tasks(self):
        status, content = super(CapellaClusterRestAPI, self).cluster_tasks()
        if not status or not isinstance(content, list):
            return status, content

        cp_state = self._control_plane_state()
        if cp_state is None or cp_state == self.HEALTHY_STATE:
            return status, content

        # Control plane is still working on the cluster (scaling etc.).
        # Report it as a running rebalance in the node's task format.
        tasks = [task for task in content
                 if task.get("type") != "rebalance"]
        tasks.append({"type": "rebalance",
                      "subtype": "rebalance",
                      "status": "running",
                      "progress": 0,
                      "capellaState": cp_state})
        return status, tasks

    # POC-NOTE (remove later)
    # WHY: Capella can't add/remove a specific node or fail one over.
    #      Faking success would make tests pass while testing nothing
    # CAME FROM: any direct ClusterRestAPI(capella_node).rebalance /
    #      add_node / eject_node / perform_*_failover call
    #      (normal scaling uses cluster.async_rebalance -> RebalanceTaskCapella
    #      which already existed and is not affected)
    # DOES: returns (False, "not supported on Capella ...") like a failed
    #      REST call, and logs a CAPELLA-GAP warning
    def rebalance(self, known_nodes, eject_nodes=None, topology=None,
                  defrag_options=None, delta_recovery_buckets=None,
                  plan_uuid=None):
        return self._unsupported(
            "rebalance",
            "use cluster.async_rebalance() which scales via the Capella API")

    def add_node(self, host_name, username=None, password=None,
                 services=None):
        return self._unsupported(
            "add_node",
            "Capella adds capacity by scaling, not by node address")

    def eject_node(self, otp_node):
        return self._unsupported(
            "eject_node",
            "Capella removes capacity by scaling, not by node address")

    def perform_hard_failover(self, otp_nodes, allow_unsafe=None,
                              timeout=300):
        return self._unsupported(
            "perform_hard_failover",
            "no Capella control-plane API to fail over a specific node")

    def perform_graceful_failover(self, otp_nodes, timeout=300):
        return self._unsupported(
            "perform_graceful_failover",
            "no Capella control-plane API to fail over a specific node")

    def _control_plane_state(self):
        context = CapellaContext.of(self.server)
        try:
            return CapellaUtils.get_cluster_state(
                context.pod, context.tenant, context.cluster_id)
        except Exception as e:
            self.log.warning("Unable to fetch Capella cluster state for %s, "
                             "using node view only: %s"
                             % (context.cluster_id, e))
            return None

    def _unsupported(self, operation, reason):
        message = "%s not supported on Capella dedicated: %s" \
                  % (operation, reason)
        self.log.warning("CAPELLA-GAP %s" % message)
        return False, message
