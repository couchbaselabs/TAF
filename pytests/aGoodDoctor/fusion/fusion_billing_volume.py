"""
Fusion Billing Volume Test — validates CP database billing entries after every rebalance.

Layer 3 test class. Extends VolumeTest to reuse all cluster setup, data loading,
and rebalance orchestration.  After every fusion rebalance completes (accelerator
nodes killed), queries the CP Couchbase database via kubectl to verify that the
correct PagerTask billing documents were written.

Key invariants validated per rebalance:
  - One PagerTask per completed accelerator node (no missing records)
  - shardSizeInBytes >= 0 for every task
  - downloadCompletedAt is non-zero and after registeredAt
  - usageCategory = "Fusion 2"
  - No duplicate nodeIDs (idempotency: the /complete handler is best-effort)
  - TTL (META.expiration) is set on every PagerTask
  - billing.variable creditQuantity matches usage*basePrice*uplift AND
    usage*live-credit-factors, cross-checked against summed PagerTask GiB
    (BillPagerTasks triggered on demand, see module docstring)

Separately, once per wall-clock UTC hour (not per rebalance — see below):
  - models.HourlyBillingRecord's billed node count for that hour is compared
    against the true peak live node count TAF itself observed during that
    hour, and every scaling operation that overlapped it is logged alongside

Required test parameters (in addition to VolumeTest params):
  cp_eks_cluster_name   CP EKS cluster name (e.g. "qe-7-cp-eks", required)
  cp_role_arn           IAM role to assume for CP cluster access, e.g.
                        jenkins-cp-cli (required)
  cp_external_id        External ID required by the role's trust policy
                        (optional, depends on the role)
  cp_namespace          k8s namespace where CP Couchbase runs (optional)

CP Couchbase credentials are never passed as test params — KubectlCPDBUtil
reads them from the cluster-wide cp-couchbase-auth Secret once connected.

test_billing_volume() verifies, per rebalance, the billing.variable
document — normally only written by its own daily cron job, but
FusionCPBillingMonitor's trigger_and_verify_variable_record() fires that
job on demand via the internal-support API, so this check runs inline
without waiting for cron. Controlled by verify_variable_and_hourly_billing
(default True, independent of billing_verification so it can be disabled
on its own) — the name is historical; it now also gates whether
HourlyBillingWindowTracker runs at all.

models.HourlyBillingRecord is deliberately NOT checked per-rebalance
anymore. It used to be, via the same kind of on-demand trigger as
billing.variable, but that gives an inaccurate/misleading picture of an
hour's billing — see the CAUTION note in fusion_cp_billing_monitor.py's
module docstring for why. Instead, HourlyBillingWindowTracker
(fusion_cp_billing_monitor.py) runs as a background thread for the
lifetime of test_billing_volume(), independently tracking the true peak
live node count and every scaling operation per wall-clock UTC hour, then
— once phone-home data has had time to settle (hourly_billing_report_delay_minutes,
default 20 minutes past the hour) — triggers ClusterBilling exactly once
per hour and logs a comparison. See fusion_cp_billing_monitor.py's
HourlyBillingWindowTracker docstring for the on-demand-trigger mechanics.
Set fail_on_hourly_billing_mismatch=True to turn a mismatch into a hard
test failure (default False — this surfaces a known, still-open CP gap,
not yet something every run should hard-fail on).

A minimum inter_rebalance_wait_secs gap (default 10 minutes) is enforced
between the end of one rebalance batch and the start of the next. After
every rebalance's billing checks, the billing.variable record of every prior
rebalance seen in the run so far is also re-fetched fresh from the CP
database (not reused/cached) and logged, so a later rebalance's on-demand
BillPagerTasks trigger silently overwriting an earlier rebalance's record
would show up in the logs.

test_verify_variable_billing_records() remains as a standalone utility for
checking billing.variable against arbitrary planUUIDs from a PAST
test_billing_volume() run (e.g. one that predates this on-demand-trigger
capability, or ran with verify_variable_and_hourly_billing=False) without
re-running the whole volume test — pass the planUUIDs logged by that earlier
run (grep its log for "Scheduled billing check:").
"""

import re
import threading
from datetime import datetime, timezone

from capellaAPI.capella.dedicated.CapellaAPI import CapellaAPI
from capella_utils.dedicated import CapellaUtils

from .fusion_volume import VolumeTest
from .fusion_cp_billing_monitor import FusionCPBillingMonitor, HourlyBillingWindowTracker
from .fusion_aws_util import FUSION_ASSUME_ROLE_NAME
from .kubectl_cp_db_util import KubectlCPDBUtil

# The CP Couchbase database always lives in this region regardless of which
# region a test's own CBS cluster targets (self.aws_region) -- never derive
# this from a test's region param, or CP DB access breaks for any run
# targeting a non-us-east-1 cluster.
CP_EKS_REGION = "us-east-1"

# CP EKS cluster naming convention mirrors the sandbox slug embedded in the
# pod URL, e.g. pod "cloudapi.qe-7.sandbox...avengers.com" -> cluster
# "qe-7-cp-eks", "sbx-10..." -> "sbx-10-cp-eks" -- same slug
# dedicatedbasetestcase.py itself already sniffs via url.find("qe-")/"sbx-"/"dev".
_SANDBOX_SLUG_RE = re.compile(r"(qe|sbx|dev)-\d+")


class FusionBillingVolumeTest(VolumeTest):
    """
    Fusion billing volume test.

    Runs the same horizontal scaling loop as VolumeTest.test_volume_scaling() and
    adds billing verification after every rebalance: queries the CP Couchbase
    database to confirm PagerTask records were written correctly.
    """

    def setUp(self):
        super().setUp()

        # CP EKS cluster access parameters -- auto-derived (see
        # _derive_cp_eks_cluster_name/_derive_cp_role_arn) so a billing run
        # doesn't need extra Jenkins-side params on top of the usual fusion
        # ones; still overridable via test params for a differently-named
        # CP cluster/role.
        self.cp_eks_cluster_name = self.input.param(
            "cp_eks_cluster_name", None) or self._derive_cp_eks_cluster_name()
        self.cp_role_arn = self.input.param(
            "cp_role_arn", None) or self._derive_cp_role_arn()
        # Same external ID already used elsewhere in this suite to assume
        # cp-cli-adjacent roles (jenkins_cpcli_role_external_id) -- cp_role_arn
        # is the same jenkins-cp-cli role, so its trust policy expects the
        # same external ID unless a test explicitly overrides cp_external_id.
        self.cp_external_id = self.input.param(
            "cp_external_id",
            self.input.param("jenkins_cpcli_role_external_id", None))
        self.cp_namespace = self.input.param("cp_namespace", None)
        if not self.cp_eks_cluster_name or not self.cp_role_arn:
            raise ValueError(
                "Could not derive cp_eks_cluster_name/cp_role_arn (pod URL "
                f"'{self.pod.url_public}' has no recognizable sandbox slug, "
                "and/or no account_id is available via tenant.account_id or "
                "the ini-wide [capella] account_id) -- pass them explicitly "
                "via -p cp_eks_cluster_name=<name>,cp_role_arn=<arn>"
            )

        # billing_verification=False disables CP DB checks (useful for debugging
        # connectivity issues without stopping the whole test) — so the initial
        # connect must be skipped too, not just the later assertions, otherwise
        # a broken CP connection still aborts setUp() regardless of this flag.
        self.billing_verification = self.input.param("billing_verification", True)

        # Independent of billing_verification: gates the on-demand
        # billing.variable / models.HourlyBillingRecord checks specifically,
        # since those mutate real billing state (trigger real CP jobs) rather
        # than just reading it, unlike the PagerTask checks.
        self.verify_variable_and_hourly_billing = self.input.param(
            "verify_variable_and_hourly_billing", True
        )

        if self.billing_verification:
            self._connect_cp_db()

        # Minimum gap enforced between the end of one rebalance batch and the
        # start of the next.
        self.inter_rebalance_wait_secs = self.input.param(
            "inter_rebalance_wait_secs", 600
        )

        # Tracks (cluster, plan_uuid) pairs collected during the current rebalance
        # batch; reset after each batch's billing check
        self._pending_billing_checks = []

        # Every (tenant, cluster, plan_uuid) triple seen over the whole test
        # run, never cleared -- used to re-verify, fresh from the CP database,
        # that older rebalances' billing.variable records are still intact
        # (not overwritten) after each new rebalance
        self._all_billing_checks = []

        # HourlyBillingWindowTracker params -- see module docstring on
        # fusion_cp_billing_monitor.py for why this replaces triggering
        # ClusterBilling immediately after every rebalance. Trackers
        # themselves are created per-cluster in test_billing_volume() once
        # clusters exist (initial_setup()), not here.
        self.hourly_billing_poll_interval_secs = self.input.param(
            "hourly_billing_poll_interval_secs",
            HourlyBillingWindowTracker.DEFAULT_POLL_INTERVAL_SECS,
        )
        self.hourly_billing_report_delay_minutes = self.input.param(
            "hourly_billing_report_delay_minutes",
            HourlyBillingWindowTracker.DEFAULT_REPORT_DELAY_MINUTES,
        )
        self.hourly_billing_flush_timeout_secs = self.input.param(
            "hourly_billing_flush_timeout_secs",
            (self.hourly_billing_report_delay_minutes + 10) * 60,
        )
        # Off by default -- the hourly/peak-node mismatch this tracker
        # surfaces is a known, still-open CP gap (AV-140399/AV-140402
        # follow-up), not yet something every run should hard-fail on. Flip
        # on once CP's fix lands to turn this into an enforced regression
        # check.
        self.fail_on_hourly_billing_mismatch = self.input.param(
            "fail_on_hourly_billing_mismatch", False
        )
        self._hourly_trackers = {}

    def _derive_cp_eks_cluster_name(self):
        """
        Derive the CP EKS cluster name from the pod URL's sandbox slug (see
        _SANDBOX_SLUG_RE above). Returns None if no slug is found (e.g. a
        "stage"/prod-style URL with no such convention) -- setUp then raises
        with a clear message rather than silently proceeding with None.
        """
        match = _SANDBOX_SLUG_RE.search(self.pod.url_public)
        if not match:
            return None
        return f"{match.group(0)}-cp-eks"

    def _derive_cp_role_arn(self):
        """
        Derive cp_role_arn using the same arn:aws:iam::{account_id}:role/
        jenkins-cp-cli convention as resolve_fusion_aws_credentials() in
        fusion_aws_util.py, and the same account_id source: the primary
        tenant's own account_id (multi-account runs), falling back to the
        ini-wide [capella] account_id (single-account, the common case).
        VolumeTest (this class's base) only ever exposes self.tenants (a
        list) -- there is no singular self.tenant here, unlike
        _FusionTestBase-derived test classes.
        Returns None if no account_id is available from either source.
        """
        primary_tenant = self.tenants[0] if self.tenants else None
        account_id = getattr(primary_tenant, "account_id", None) \
            or self.input.capella.get("account_id")
        if not account_id:
            return None
        return f"arn:aws:iam::{account_id}:role/{FUSION_ASSUME_ROLE_NAME}"

    def tearDown(self):
        # Defensive cleanup only -- the normal path is test_billing_volume()
        # itself calling _stop_hourly_billing_trackers() (which flushes
        # pending hours) before returning. If the test failed/raised before
        # reaching that call, any leftover trackers are still daemon
        # threads and won't block process exit, but signal them to stop
        # rather than leaving them polling against a connection tearDown is
        # about to close. Not a flush (no point waiting minutes in a
        # tearDown for a test that already failed).
        for tracker in getattr(self, "_hourly_trackers", {}).values():
            tracker.stop()
        self._hourly_trackers = {}

        kubectl_cp_db_util = getattr(self, "kubectl_cp_db_util", None)
        if kubectl_cp_db_util:
            try:
                kubectl_cp_db_util.disconnect()
            except Exception as e:
                self.log.warning(f"Error while disconnecting CP DB kubectl util: {e}")
        super().tearDown()

    # -------------------------------------------------------------------------
    # CP DB connection management
    # -------------------------------------------------------------------------

    def _connect_cp_db(self):
        """
        (Re)establish the CP EKS connection, replacing any previous connection.

        KubectlCPDBUtil must be given genuine BASE credentials so its own
        nested IAMLib can independently assume cp_role_arn -- never
        self.aws_iam.get_credentials(), which is the *already-assumed*
        jenkins-cp-cli session self.fusion_aws_util/etc. use for general
        EC2/S3/FIS access. Passing an already-assumed role's temp
        credentials through as if they were base creds makes KubectlCPDBUtil
        try to re-assume the SAME role from a session that's already that
        role, which AWS rejects (seen live: "arn:aws:sts::<acct>:assumed-
        role/jenkins-cp-cli/<session> isn't the right user to assume the
        role"). Same fix/reasoning as fusion_billing_test.py's
        _connect_cp_db (Gerrit 249816): use only explicit raw
        aws_access_key/aws_secret_key test-param overrides, or leave them
        unset entirely -- KubectlCPDBUtil's own IAMLib then falls back to
        the AWS_ACCESS_KEY_ID_004/AWS_SECRET_ACCESS_KEY_004 base ("004
        test IAM user") env vars and does its own one-time AssumeRole of
        cp_role_arn from that clean base, same as every other fusion
        test's default credential path (resolve_fusion_aws_credentials).

        This is called again at each rebalance batch boundary
        (_run_billing_checks_for_batch), not just once in setUp(), since
        cp_role_arn's own assumed session has its own TTL that a
        multi-hour billing-volume run can easily outlive.

        Known narrow race: any HourlyBillingWindowTracker resolves
        self.billing_monitor fresh on every use (see its
        get_billing_monitor_fn), specifically so it never holds a stale
        reference across a reconnect -- but if this method's old.disconnect()
        lands mid-query from a tracker's background thread, that one query
        can fail. Acceptable given reconnects are infrequent (once per
        rebalance batch) and queries take a few seconds; a tracker's own
        failed trigger/query just logs and gets picked up cleanly on that
        hour's would-be next check, it doesn't wedge anything.
        """
        old = getattr(self, "kubectl_cp_db_util", None)
        if old:
            try:
                old.disconnect()
            except Exception as e:
                self.log.warning(f"Error disconnecting previous CP DB kubectl util: {e}")

        # Raw test-param overrides only (never self.aws_access_key/secret_key/
        # session_token -- those may be the already-assumed session described
        # above). None falls through to KubectlCPDBUtil's own 004-env fallback.
        access_key = self.input.param("aws_access_key", None)
        secret_key = self.input.param("aws_secret_key", None)
        session_token = self.input.param("aws_session_token", None)

        # CP_EKS_REGION (always us-east-1), not self.aws_region: the base
        # access_key/secret_key/session_token above aren't region-scoped, but
        # the region KubectlCPDBUtil is constructed with is what it uses for
        # both the AssumeRole STS call and locating the CP EKS cluster itself
        # -- self.aws_region is the test's own CBS cluster region and can
        # legitimately differ (e.g. a non-us-east-1 volume test run), which
        # would otherwise break CP DB access entirely.
        self.kubectl_cp_db_util = KubectlCPDBUtil(
            access_key, secret_key, session_token, region=CP_EKS_REGION,
        )
        connected = self.kubectl_cp_db_util.connect(
            self.cp_eks_cluster_name, self.cp_role_arn, self.cp_external_id
        )
        if not connected:
            raise RuntimeError(
                f"Failed to connect to CP EKS cluster '{self.cp_eks_cluster_name}'"
            )
        self.billing_monitor = FusionCPBillingMonitor(
            self.log, self.kubectl_cp_db_util, namespace=self.cp_namespace
        )

    # -------------------------------------------------------------------------
    # Billing helpers
    # -------------------------------------------------------------------------

    def _collect_billing_info_for_rebalance(self, rebalance_task, uuid_before: int):
        """
        Record the (tenant, cluster, plan_uuid) triple produced by a single
        rebalance task.

        Must be called immediately after monitor_cluster_status() for that task,
        while self.fusion_rebalances still reflects the just-completed rebalance.

        :param rebalance_task: Completed rebalance task object
        :param uuid_before: len(self.fusion_rebalances) recorded BEFORE monitor_cluster_status()
        """
        uuid_after = len(self.fusion_rebalances)
        if uuid_after > uuid_before:
            # A new planUUID was appended by monitor_cluster_accelerator_instances()
            plan_uuid = self.fusion_rebalances[uuid_after - 1]
            self._pending_billing_checks.append(
                (rebalance_task.tenant, rebalance_task.cluster, plan_uuid)
            )
            self._all_billing_checks.append(
                (rebalance_task.tenant, rebalance_task.cluster, plan_uuid)
            )
            self.log.info(
                f"Scheduled billing check: cluster={rebalance_task.cluster.id}, "
                f"planUUID={plan_uuid}"
            )
        else:
            # No accelerator instances were launched (DCP rebalance, below threshold)
            self.log.info(
                f"No fusion planUUID recorded for cluster={rebalance_task.cluster.id} "
                f"— DCP rebalance or below threshold, skipping billing check"
            )

    def _build_capella_api(self, tenant) -> CapellaAPI:
        """
        Build a CapellaAPI instance carrying TOKEN_FOR_INTERNAL_SUPPORT
        (self.pod.TOKEN), for use with FusionCPBillingMonitor's on-demand
        job-trigger methods. Same construction pattern already used by
        CapellaUtils.create_tenant_feature_flag() for the same
        internal-support API.
        """
        return CapellaAPI(
            self.pod.url_public,
            tenant.api_secret_key,
            tenant.api_access_key,
            tenant.user,
            tenant.pwd,
            self.pod.TOKEN,
        )

    def _get_live_node_count(self, tenant, cluster) -> int:
        """Current live KV node count for a cluster, via the Capella nodes API."""
        return len(CapellaUtils.get_nodes(self.pod, tenant, cluster.id))

    def _get_live_node_ids(self, tenant, cluster) -> set:
        """Current live node hostnames for a cluster, via the Capella nodes API.

        Used by HourlyBillingWindowTracker to track every distinct node
        observed across an hour (not just the peak concurrent count) --
        see its get_live_node_ids_fn docstring.
        """
        return {n.get("hostname") for n in CapellaUtils.get_nodes(self.pod, tenant, cluster.id)}

    # -------------------------------------------------------------------------
    # HourlyBillingWindowTracker lifecycle
    # -------------------------------------------------------------------------

    def _start_hourly_billing_trackers(self):
        """
        Start one HourlyBillingWindowTracker per cluster once clusters exist
        (must be called after initial_setup()). No-op if billing
        verification is disabled entirely.
        """
        if not (self.billing_verification and self.verify_variable_and_hourly_billing):
            return
        for tenant in self.tenants:
            for cluster in tenant.clusters:
                tracker = HourlyBillingWindowTracker(
                    self.log, cluster.id,
                    # Callables, not frozen objects: self.billing_monitor is
                    # rebuilt (fresh CP DB connection) at every rebalance
                    # batch boundary -- see _connect_cp_db() docstring --
                    # and self.pod.TOKEN-backed CapellaAPI creds can
                    # likewise need rebuilding, so the tracker must always
                    # resolve the CURRENT object, not the one that existed
                    # when it was constructed.
                    get_billing_monitor_fn=lambda: self.billing_monitor,
                    get_capella_api_fn=lambda t=tenant: self._build_capella_api(t),
                    get_live_node_ids_fn=lambda t=tenant, c=cluster: self._get_live_node_ids(t, c),
                    poll_interval_secs=self.hourly_billing_poll_interval_secs,
                    report_delay_minutes=self.hourly_billing_report_delay_minutes,
                )
                tracker.start()
                self._hourly_trackers[cluster.id] = tracker

    def _stop_hourly_billing_trackers(self):
        """
        Stop every tracker, waiting for each to flush its still-pending
        hour(s). Call before the cluster/CP DB connection go away, and
        before final test assertions so mismatches (if
        fail_on_hourly_billing_mismatch) are known.
        """
        all_mismatches = []
        for cluster_id, tracker in self._hourly_trackers.items():
            tracker.stop_and_flush(timeout=self.hourly_billing_flush_timeout_secs)
            all_mismatches.extend(tracker.mismatches)
        self._hourly_trackers.clear()

        if all_mismatches:
            summary = "; ".join(
                f"cluster={m['cluster_id']} hour={m['hour']} "
                f"observed_max={m['observed_max_nodes']} billed={m['billed_nodes']} "
                f"(billingPeriod={m['billing_period']})"
                for m in all_mismatches
            )
            self.log.error(f"Hourly billing window mismatches: {summary}")
            if self.fail_on_hourly_billing_mismatch:
                self.fail(f"Hourly billing window mismatch(es) found: {summary}")

    def _run_billing_checks_for_batch(self):
        """
        Run CP DB billing verification for all rebalances collected in the current batch.

        Clears self._pending_billing_checks after processing.
        """
        if not self.billing_verification:
            self.log.info("billing_verification=False — skipping CP DB billing checks")
            self._pending_billing_checks.clear()
            return

        # Refresh the CP EKS connection with freshly resolved credentials at
        # this natural batch boundary — see _connect_cp_db() docstring.
        self._connect_cp_db()

        for tenant, cluster, plan_uuid in self._pending_billing_checks:
            self.log.info(
                f"=== Billing verification: cluster={cluster.id}, planUUID={plan_uuid} ==="
            )

            # Primary check: correct PagerTask documents exist in CP DB
            pager_tasks = self.billing_monitor.query_pager_tasks_for_rebalance(
                cluster.id, plan_uuid
            )
            result = self.billing_monitor.verify_pager_tasks_after_rebalance(
                cluster.id, plan_uuid
            )
            self.assertTrue(
                result,
                f"PagerTask billing verification FAILED for cluster={cluster.id}, "
                f"planUUID={plan_uuid}"
            )

            # Secondary check: idempotency — no duplicate nodeIDs
            dedup_result = self.billing_monitor.verify_no_duplicate_pager_tasks(
                cluster.id, plan_uuid
            )
            self.assertTrue(
                dedup_result,
                f"Duplicate PagerTask detected for cluster={cluster.id}, "
                f"planUUID={plan_uuid} — double-billing risk"
            )

            # Informational: log GiB summary for the rebalance
            self.billing_monitor.log_billing_summary(cluster.id, plan_uuid)

            # Tertiary check: billing.variable, triggered on demand (no more
            # waiting for cron) — see module docstring and
            # FusionCPBillingMonitor.trigger_and_verify_variable_record().
            #
            # models.HourlyBillingRecord is intentionally NOT checked here
            # anymore (it used to be, via trigger_and_verify_hourly_billing_records()
            # right after every rebalance) — see the CAUTION note in
            # fusion_cp_billing_monitor.py's module docstring for why
            # triggering ClusterBilling this early/often is actively
            # misleading. HourlyBillingWindowTracker (started in
            # test_billing_volume()) checks it instead, once per wall-clock
            # hour, after phone-home data has had time to settle.
            if self.verify_variable_and_hourly_billing:
                capella_api = self._build_capella_api(tenant)
                expected_gib = sum(
                    max(0, t.get("shardSizeInBytes", 0)) for t in (pager_tasks or [])
                ) / self.billing_monitor.BYTES_PER_GIB

                variable_result = self.billing_monitor.trigger_and_verify_variable_record(
                    capella_api, cluster.id, plan_uuid,
                    usage_date=datetime.now(timezone.utc), expected_gib=expected_gib,
                )
                self.assertTrue(
                    variable_result,
                    f"billing.variable verification FAILED for cluster={cluster.id}, "
                    f"planUUID={plan_uuid}"
                )

        self._pending_billing_checks.clear()

        # Re-fetch (fresh, uncached) billing.variable data for every rebalance
        # seen so far in this test run -- not just the one that just
        # completed -- and log it. This guards against the current
        # rebalance's on-demand BillPagerTasks trigger clobbering/blanking an
        # earlier rebalance's variable cost record instead of leaving it as
        # its own independent, untouched document.
        if self.verify_variable_and_hourly_billing:
            self.billing_monitor.log_all_variable_records_summary([
                (cluster.id, plan_uuid)
                for _, cluster, plan_uuid in self._all_billing_checks
            ])

    def _run_rebalance_batch_with_billing(self, rebalance_tasks, step_label=None):
        """
        Monitor a batch of rebalance tasks and verify billing after all complete.

        Mirrors the pattern in VolumeTest.test_volume_scaling():
          1. monitor_cluster_status() per task
          2. sleep 60s
          3. monitor_fusion_accelerator_nodes_killed_after_rebalance() per task
          4. Standard post-rebalance diagnostics (unchanged from parent)
          5. [NEW] billing verification per task
          6. [NEW] Enforce inter_rebalance_wait_secs gap before returning

        Diagnostics (step 4) intentionally run BEFORE billing verification
        (step 5): billing checks are the newest, least-proven code path here,
        and if they fail, we still want the same rebalance-report evidence any
        other VolumeTest failure would produce — an assertTrue failure inside
        billing verification must not suppress it.

        :param rebalance_tasks: List of rebalance task objects
        :param step_label: e.g. "4.2 (UP)" — recorded against
            HourlyBillingWindowTracker for whichever wall-clock hour(s) this
            step overlaps (no-op if that cluster has no tracker)
        """
        # Phase 1: Monitor rebalance progress (populates self.fusion_rebalances)
        for rebalance_task in rebalance_tasks:
            uuid_before = len(self.fusion_rebalances)
            tracker = self._hourly_trackers.get(rebalance_task.cluster.id)

            node_count_before = None
            if tracker:
                try:
                    node_count_before = self._get_live_node_count(
                        rebalance_task.tenant, rebalance_task.cluster
                    )
                except Exception as e:
                    self.log.warning(
                        f"Could not fetch pre-rebalance node count for "
                        f"hourly billing tracker: {e}"
                    )

            step_start = datetime.now(timezone.utc)
            self.monitor_cluster_status(
                rebalance_task.tenant, rebalance_task.cluster, rebalance_task
            )
            step_end = datetime.now(timezone.utc)
            self.fusion_monitor.get_fusion_uploader_map(
                rebalance_task.tenant, rebalance_task.cluster, self.find_master
            )
            self._collect_billing_info_for_rebalance(rebalance_task, uuid_before)

            if tracker:
                node_count_after = None
                try:
                    node_count_after = self._get_live_node_count(
                        rebalance_task.tenant, rebalance_task.cluster
                    )
                except Exception as e:
                    self.log.warning(
                        f"Could not fetch post-rebalance node count for "
                        f"hourly billing tracker: {e}"
                    )
                node_change = (
                    f"{node_count_before}->{node_count_after}"
                    if node_count_before is not None and node_count_after is not None
                    else "unknown"
                )
                tracker.record_scaling_operation(
                    step_label or "rebalance", node_change, step_start, step_end
                )

        self.sleep(60, "Wait 60s after rebalance before checking accelerator cleanup")

        # Phase 2: Verify accelerator nodes are killed
        for rebalance_task in rebalance_tasks:
            result = self.cp_monitor.monitor_fusion_accelerator_nodes_killed_after_rebalance(
                rebalance_task.cluster, timeout=self.fusion_infra_timeout
            )
            self.assertTrue(
                result,
                f"Fusion accelerator nodes not killed after rebalance "
                f"for cluster={rebalance_task.cluster.id}"
            )

        # Phase 3: Standard post-rebalance diagnostics — runs before billing
        # verification so a billing failure never costs us this evidence (see
        # docstring above). All log-scanning intentionally omitted here (dp-agent,
        # memcached, accelerator logs): the dp-agent/memcached scanners are not
        # bare self. calls on VolumeTest to begin with (only
        # _cp_monitor_for_tenant(tenant).scan_*_logs_for_errors(cluster),
        # differently-shaped per-tenant/cluster calls), and none of them are
        # needed for billing verification -- this test cares about the CP
        # database's billing documents, not server/accelerator log content.
        self.log_rebalance_report()
        self.check_asg_cleanup_after_rebalance()

        # Phase 4: Billing verification in CP database
        self._run_billing_checks_for_batch()

        # Phase 5: Enforce a minimum gap before the next rebalance starts
        self.sleep(
            self.inter_rebalance_wait_secs,
            f"Wait {self.inter_rebalance_wait_secs}s before next rebalance"
        )

    # -------------------------------------------------------------------------
    # Test method
    # -------------------------------------------------------------------------

    def test_billing_volume(self):
        """
        Fusion billing volume test: horizontal scaling with billing verification.

        For every rebalance iteration (scale-up and scale-down), verifies that the
        correct PagerTask and billing.variable documents were written to the CP
        Couchbase database. Separately, once per wall-clock UTC hour,
        HourlyBillingWindowTracker compares CP's billed node count for that
        hour against the true peak TAF itself observed — see module
        docstring for why this replaced a per-rebalance HourlyBillingRecord
        check.

        Parameters:
          h_scaling        Enable horizontal (node add/remove) scaling (default: True)
          iterations       Number of scale-up / scale-down steps (default: 10)
          rebl_steps       Hyphen-separated target node counts (default: "3-5-7-8")
          services         Service group for rebalance (default: "data")
          provider         Cloud provider: aws|azure|gcp (default: "aws")
          billing_verification  Set False to run test without CP DB checks (default: True)
          verify_variable_and_hourly_billing  Set False to skip the on-demand
                           billing.variable check and HourlyBillingWindowTracker
                           both, while keeping PagerTask checks (default: True)
          hourly_billing_poll_interval_secs  How often HourlyBillingWindowTracker
                           samples live node count (default: 30)
          hourly_billing_report_delay_minutes  Minutes past an hour's close
                           before checking its billing record (default: 20)
          hourly_billing_flush_timeout_secs  Max seconds to wait, at the end
                           of the run, for the final pending hour(s) to
                           become due (default: (report_delay_minutes+10)*60)
          fail_on_hourly_billing_mismatch  Set True to hard-fail the test on
                           any hourly billed-vs-observed mismatch (default:
                           False — the gap this surfaces is a known, still-
                           open CP issue, not yet enforced as a regression)
          inter_rebalance_wait_secs  Minimum gap enforced between the end of
                           one rebalance batch and the start of the next
                           (default: 600)
        """
        # Background store / DCP monitors (same as parent test_volume_scaling)
        self._log_store_stop_event = threading.Event()

        def _log_store_monitor():
            while not self._log_store_stop_event.is_set():
                self.log_fusion_log_store_data_size()
                self.log_fusion_pending_bytes()
                self._log_store_stop_event.wait(300)

        self._log_store_thread = threading.Thread(
            target=_log_store_monitor, name="log-store-monitor", daemon=True
        )
        self._log_store_thread.start()

        self.dcp_check_thread = threading.Thread(
            target=self.log_fusion_dcp_items_remaining,
            name="dcp-check-thread",
            daemon=True
        )
        self.dcp_check_thread.start()

        # Data load and cluster initialisation
        self.initial_setup()
        self._log_store_stop_event.set()

        # Start per-cluster HourlyBillingWindowTracker(s) now that clusters
        # exist -- see fusion_cp_billing_monitor.py's module docstring for
        # why hourly billing is checked this way (once per wall-clock hour,
        # after a settle buffer) instead of via an on-demand trigger
        # immediately after every rebalance.
        self._start_hourly_billing_trackers()

        # ---------------------------------------------------------------
        # Pre-test: verify billing factors exist in CP database
        # ---------------------------------------------------------------
        if self.billing_verification:
            self.PrintStep("Step 3: Verify fusion billing factors in CP database")
            factors_ok = self.billing_monitor.verify_billing_factors_exist()
            self.assertTrue(
                factors_ok,
                "Required fusion billing factors missing from CP database. "
                "Check billing.credit_factors collection."
            )

        # ---------------------------------------------------------------
        # Horizontal scaling loop
        # ---------------------------------------------------------------
        self.compute["data"] = self.input.param("fusion_compute", "m5.4xlarge")
        self.fusion_rebalances = list()

        h_scaling = self.input.param("h_scaling", True)
        if not h_scaling:
            self.log.info("h_scaling=False — skipping horizontal scaling loop")
            self._stop_hourly_billing_trackers()
            return

        self.rebl_steps = [
            int(n) for n in self.input.param("rebl_steps", "3-5-7-8").split("-")
        ]
        self.services = self.input.param("services", "data")
        self.rebl_services = self.input.param("rebl_services", self.services).split("-")
        self.cycles = self.input.param("cycles", 1)

        # Start background EBS monitoring (same as parent)
        ebs_cleanup_threads = []
        ebs_available_threads = []
        for tenant in self.tenants:
            for cluster in tenant.clusters:
                ebs_cleanup_thread = threading.Thread(
                    target=self.cp_monitor.check_ebs_guest_vol_deletion,
                    kwargs={
                        "tenant": tenant,
                        "cluster": cluster,
                        "fusion_monitor_util": self.fusion_monitor,
                        "stop_run_event": self.stop_run_event,
                        "find_master_func": self.find_master,
                    },
                    daemon=True,
                )
                ebs_cleanup_thread.start()
                ebs_cleanup_threads.append(ebs_cleanup_thread)

                ebs_available_thread = threading.Thread(
                    target=self.cp_monitor.monitor_available_volumes_by_fusion_rebalance,
                    kwargs={
                        "cluster": cluster,
                        "fusion_rebalances": self.fusion_rebalances,
                        "stop_run_event": self.stop_run_event,
                    },
                    daemon=True,
                )
                ebs_available_thread.start()
                ebs_available_threads.append(ebs_available_thread)

        # ---------------------------------------------------------------
        # Scale-up / scale-down cycles
        # ---------------------------------------------------------------
        for cycle in range(self.cycles):
            self.log.info(f"Starting scaling cycle {cycle + 1}/{self.cycles}")

            # Scale UP
            for rebl_step in range(self.iterations):
                self.log_fusion_log_store_data_size()
                self.log_fusion_pending_bytes()
                self.PrintStep(
                    f"Step 4.{rebl_step}: Scale UP with loading of docs "
                    f"(cycle {cycle + 1}/{self.cycles})"
                )
                for service in self.rebl_services:
                    rebalance_tasks = []
                    config = self.rebalance_config(service, self.rebl_steps[rebl_step])
                    for tenant in self.tenants:
                        for cluster in tenant.clusters:
                            rebalance_task = self.task.async_rebalance_capella(
                                self.pod, tenant, cluster, config,
                                timeout=self.rebalance_timeout
                            )
                            rebalance_tasks.append(rebalance_task)

                    self._run_rebalance_batch_with_billing(
                        rebalance_tasks, step_label=f"4.{rebl_step} (UP)"
                    )

                self.test_cluster_on_off()

            # Scale DOWN
            for rebl_step in range(self.iterations):
                self.log_fusion_log_store_data_size()
                self.log_fusion_pending_bytes()
                self.PrintStep(
                    f"Step 5.{rebl_step}: Scale DOWN with loading of docs "
                    f"(cycle {cycle + 1}/{self.cycles})"
                )
                for service in self.rebl_services:
                    rebalance_tasks = []
                    config = self.rebalance_config(
                        service, -self.rebl_steps[rebl_step]
                    )
                    for tenant in self.tenants:
                        for cluster in tenant.clusters:
                            rebalance_task = self.task.async_rebalance_capella(
                                self.pod, tenant, cluster, config,
                                timeout=self.rebalance_timeout
                            )
                            rebalance_tasks.append(rebalance_task)

                    self._run_rebalance_batch_with_billing(
                        rebalance_tasks, step_label=f"5.{rebl_step} (DOWN)"
                    )

                self.test_cluster_on_off()

        # ---------------------------------------------------------------
        # Stop hourly billing trackers, flushing any still-pending hour(s)
        # ---------------------------------------------------------------
        self._stop_hourly_billing_trackers()

        # ---------------------------------------------------------------
        # Final EBS cleanup verification
        # ---------------------------------------------------------------
        for tenant in self.tenants:
            for cluster in tenant.clusters:
                result = self.check_ebs_cleanup_for_cluster(cluster)
                self.assertTrue(
                    result,
                    f"EBS cleanup check failed for cluster={cluster.id}"
                )
                self.log.info(
                    f"EBS cleanup verified for cluster={cluster.id}"
                )

    def test_verify_variable_billing_records(self):
        """
        Standalone follow-up check for the daily billing.variable aggregate.

        Must be run SEPARATELY from test_billing_volume(), at least one full
        BillPagerTasks cron cycle (18:30 UTC daily, for the previous UTC day)
        after the rebalances being checked. Not wired into
        test_billing_volume() itself for that reason.

        This reuses FusionBillingVolumeTest.setUp() (same as
        test_billing_volume()) purely for its CP DB connection/credential
        plumbing — it does not exercise the cluster/rebalance/scaling
        machinery at all, so provisioning a fresh Capella cluster just to run
        this check is wasted setup cost. Left as-is for now since this is a
        first cut at the check; revisit with a lighter-weight setUp path if
        this becomes a frequently-run standalone job.

        Parameters:
          verify_cluster_id   Cluster ID whose rebalances to check (required)
          verify_plan_uuids   Hyphen-separated planUUIDs to check, e.g. the
                               ones logged as "Scheduled billing check:
                               cluster=..., planUUID=..." by an earlier
                               test_billing_volume() run (required)
        """
        cluster_id = self.input.param("verify_cluster_id", None)
        plan_uuids_param = self.input.param("verify_plan_uuids", None)
        if not cluster_id or not plan_uuids_param:
            self.fail(
                "verify_cluster_id and verify_plan_uuids are required, e.g. "
                "-p verify_cluster_id=<id>,verify_plan_uuids=<uuid1>-<uuid2>"
            )
        plan_uuids = plan_uuids_param.split("-")

        failures = []
        for plan_uuid in plan_uuids:
            self.log.info(
                f"=== Variable billing verification: cluster={cluster_id}, "
                f"planUUID={plan_uuid} ==="
            )

            pager_tasks = self.billing_monitor.query_pager_tasks_for_rebalance(
                cluster_id, plan_uuid
            )
            if not pager_tasks:
                failures.append(
                    f"planUUID={plan_uuid}: no PagerTask documents found — "
                    f"cannot compute an expected GiB total to cross-check against"
                )
                continue
            expected_gib = sum(
                max(0, t.get("shardSizeInBytes", 0)) for t in pager_tasks
            ) / self.billing_monitor.BYTES_PER_GIB

            result = self.billing_monitor.verify_variable_record_after_rebalance(
                cluster_id, plan_uuid, expected_gib=expected_gib
            )
            if not result:
                failures.append(f"planUUID={plan_uuid}: variable record verification FAILED")

        self.assertFalse(
            failures,
            f"Variable billing verification failed for {len(failures)}/{len(plan_uuids)} "
            f"rebalance(s):\n" + "\n".join(failures)
        )
