import time

from capella_utils.dedicated import CapellaUtils
from capella_utils.dedicated_rest.context import CapellaContext
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import \
    ClusterRestAPI
from global_vars import logger


class RebalanceReport(object):
    """
    What was seen of the ns_server rebalance(s) run by one Capella scale.
    Capella may retry a failed rebalance by itself, so every attempt
    (its rebalance report id) is recorded with its error (None = success).
    """

    def __init__(self):
        self.error = None       # control-plane failure / timeout
        self.attempts = dict()  # lastReportURI -> errorMessage or None
        self.methods = dict()   # bucket_name -> rebalance_method

    def failed_attempts(self):
        return [err for err in self.attempts.values() if err]

    def succeeded(self):
        return bool(self.attempts) \
            and list(self.attempts.values())[-1] is None

    def is_file_based(self):
        return set(self.methods.values()) == {"file_based"}


class RebalanceWatcher(object):
    """
    Follows a Capella scale until the control plane is healthy again and
    records the ns_server rebalance it triggers, read from the nodes'
    /pools/default/tasks (readable by the Capella DB user).

    Polls the node tasks fast on purpose: the ns_server rebalance often
    takes seconds and rebalance_method is only reported while it runs.
    The Capella state call is slow, so it is polled less often.
    """
    TASK_POLL_INTERVAL = 0.25   # ns_server recommendedRefreshPeriod
    STATE_POLL_INTERVAL = 10
    HEALTHY = "healthy"
    FAILED_STATES = ("deploymentFailed", "deployment_failed",
                     "redeploymentFailed", "rebalanceFailed",
                     "rebalance_failed", "scaleFailed")

    def __init__(self, cluster, timeout=1200):
        self.cluster = cluster
        self.context = CapellaContext.of(cluster.master)
        self.timeout = timeout
        self.report = RebalanceReport()
        self.state = None
        self.scaling_seen = False
        self.node = None
        self.log = logger.get("test")
        self.start_report = None

    def mark_start(self):
        """Remember the last rebalance, call before triggering the scale"""
        task = self._rebalance_task()
        self.start_report = task.get("lastReportURI") if task else None

    def watch(self):
        """Poll until the cluster is healthy again after scaling"""
        end = time.time() + self.timeout
        next_state_check = 0
        while time.time() < end:
            self._record(self._rebalance_task())
            if time.time() >= next_state_check:
                next_state_check = time.time() + self.STATE_POLL_INTERVAL
                if self._scale_done():
                    return self.report
            time.sleep(self.TASK_POLL_INTERVAL)
        self.report.error = "Scale not done in %s seconds" % self.timeout
        return self.report

    def _scale_done(self):
        """Read the Capella state; True once scaling ended (or failed)"""
        state = self._cp_state()
        if state != self.state:
            self.log.info("Capella cluster state: %s" % state)
            self.state = state
        if state in self.FAILED_STATES:
            self.report.error = "Capella state %s" % state
            return True
        self.scaling_seen = self.scaling_seen \
            or state not in (self.HEALTHY, None)
        return self.scaling_seen and state == self.HEALTHY

    def _record(self, task):
        if not task:
            return
        if task.get("status") == "running":
            self.report.methods.update(self._methods(task))
            return
        report = task.get("lastReportURI")
        if report in (None, self.start_report) \
                or report in self.report.attempts:
            return
        error = task.get("errorMessage")
        self.report.attempts[report] = error
        self.log.info("ns_server rebalance attempt %s: %s"
                      % (len(self.report.attempts), error or "success"))

    @staticmethod
    def _methods(task):
        details = task.get("stageInfo", {}).get("data", {}) \
            .get("details", {})
        return {bucket: info["rebalance_method"]
                for bucket, info in details.items()
                if info.get("rebalance_method")}

    def _rebalance_task(self):
        """ns_server rebalance task, last node that answered tried first"""
        nodes = sorted(self.cluster.nodes_in_cluster,
                       key=lambda node: node is not self.node)
        for node in nodes:
            try:
                status, tasks = ClusterRestAPI(node).node_cluster_tasks()
            except Exception:
                continue
            if status:
                self.node = node
                return next((t for t in tasks
                             if t.get("type") == "rebalance"), None)
        return None

    def _cp_state(self):
        try:
            return CapellaUtils.get_cluster_state(
                self.context.pod, self.context.tenant,
                self.context.cluster_id)
        except Exception as e:
            self.log.warning("Unable to read Capella cluster state: %s" % e)
            return None
