"""
Fusion Accelerator Chaos / Negative Tests
=========================================

Negative counterpart to fusion_accelerator_lifecycle_test.py. Where that file asserts the
happy path, these inject a fault at a stage boundary and assert the control plane recovers,
fails cleanly, or holds a safety invariant.

Each test maps to a row in STAGE_TEST_MATRIX.md §"Negative Tests by Stage Boundary", and
the behaviour being probed is documented in ACCELERATION.md §"Error Handling and Recovery":

  Boundary  Fault                                    Test                          Doc basis
  --------  ---------------------------------------  ----------------------------  --------------------
  E         node removed while its guest volumes     test_remove_node_with_        idempotent creates,
            are still attached                       attached_guest_volumes        force teardown
  C -> D    host out of guest volume slots (>22)     test_slot_exhaustion_         "Slot exhaustion
                                                     triggers_fallback_replacement fallback"
  D         CBS rebalance aborted mid-flight (once   test_abort_rebalance_         "Manifest
            ns_server is actually rebalancing, at    invalidates_manifest          invalidation"
            phase 7) -> Invalidated -> teardown
  E         memcached killed during extent           test_kill_memcached_during_   checkpoints /
            migration                                extent_migration              retry semantics
  A -> B    accelerator terminated mid-download      test_accelerator_node_        ASG health check
                                                     termination_resilience
  A -> B    accelerator STOPPED mid-download         test_accelerator_stopped_     ASG health check
                                                     mid_download
  B         download volume force-detached           test_accelerator_volume_      Phase 5 (volume ID
                                                     detached_during_download      baked in at launch)
  B         S3 download throttled below the log-     test_download_rate_limit_     Phase 2 lease /
            file lease TTL                           expires_lease_falls_back_     "Manifest
                                                     to_dcp                        invalidation"

Every test here inherits FusionAcceleratorLifecycleTest, so the whole validator library is
available unchanged — `_pause_migration`/`_resume_migration`, `_wait_for_accelerator_fleet_stable`,
`_validate_guest_volume_drain`, `_validate_teardown`, the soft-fail `_stage` machinery, and the
fusion-config/threshold appliers. Inheriting a TestCase also inherits its test methods, but TAF
selects tests by name from the conf, so the parent's positive tests do not run from this file's
conf.

Migration is frozen (`fusion_migration_rate_limit=0`) in every test here before the fault is
injected. That is not incidental: guest volumes are deleted per shard as migration completes, so
without the freeze the resource the fault is supposed to disturb may already be gone. Each test
resumes migration once the fault has been observed, then asserts the cluster still reclaims
everything. In the lease-expiry test the freeze serves the mirror-image purpose: nothing is
expected to reach a KV node at all, and the freeze is what makes "no guest volume was ever
attached to one" an observation rather than a poll-timing race.

Faults are injected with primitives that were verified to exist:
  - EC2 terminate / SSM shell commands via `fusion_aws_util.ec2`
  - `/controller/stopRebalance` via ClusterRestAPI, gated on `/pools/default/rebalanceProgress`
    and `/pools/default/tasks` so it lands while ns_server is genuinely rebalancing rather
    than during the CP-owned phases that precede it (see _wait_for_cbs_rebalance_running)
  - the fusion support config (minSplitSize / maxSlots) to force slot exhaustion, and
    (accelerator.download.rateLimit) to throttle the S3 download past the lease TTL

`/diag/eval` is not usable on Capella dedicated, so ns_server test conditions
(`testconditions:set(...)`) are not available to these tests. That rules out the on-prem trick
of stalling a rebalance to widen a fault window — see test_abort_rebalance_invalidates_manifest,
which has to race the vBucket movement instead.

Not implemented here, and why (see the matrix rows still marked gap/⬜):
  - EBS pause-IO / attach-failure faults: `FISLib.create_ebs_mount_failure_experiment` and
    `simulate_volume_attach_failure` both raise NotImplementedError.
  - accelerator-side faults (agent crash, disk full, process kill): these need SSM on the
    accelerator itself, which is untested from this framework. `_run_on_cluster_node` reaches
    KV nodes only; whether the accelerator AMI runs the SSM agent with a permitting instance
    profile is the open question, and nothing here answers it any more.
  - CP-internal faults (kill a CP job mid-teardown): no control-plane job API is exposed to TAF.
"""

import time

from bucket_utils.bucket_ready_functions import JavaDocLoaderUtils
from capella_utils.dedicated import CapellaUtils as CapellaAPI
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from .fusion_accelerator_lifecycle_test import FusionAcceleratorLifecycleTest


class FusionAcceleratorChaosTest(FusionAcceleratorLifecycleTest):
    """Fault-injection tests for the fusion accelerator lifecycle."""

    def setUp(self):
        super().setUp()
        self._test_start = time.time()
        # Set by a stage that has established the CP is stuck and will not progress. Every
        # long wait after that point checks it and skips, because a rebalance that is never
        # going to end makes those waits pure cost: one run spent 8h in the default
        # wait_for_rebalances monitor, 2h in the settle wait and 20min in the orphan check
        # after the diagnosis was already complete, then looped in tearDown for another day.
        self._hung = None

    # ------------------------------------------------------------------
    # Shared chaos helpers
    # ------------------------------------------------------------------

    def _wait_until_writable(self, label, timeout=None):
        """Poll until the cluster will accept a spec update. Returns a bool.

        Must be called before triggering a rebalance on a cluster that may still be settling
        after a fault. CapellaUtils.scale() retries a rejected spec update in an UNBOUNDED
        loop, waiting `timeout` between attempts — so a cluster wedged in 'scaling' (which
        rejects every update with EntityStateInvalid) makes it retry at rebalance_timeout
        intervals forever. Observed: retries 6h apart that ran for a day and buried the real
        failure. Nothing inside the test can interrupt that once it starts, so the only
        defence is not to call it.

        Returns False if the cluster never becomes healthy, and the caller must then skip the
        rebalance rather than attempt it.
        """
        timeout = timeout if timeout is not None else int(
            self.input.param("writable_wait_timeout", 900))
        deadline = time.time() + timeout
        state = None
        while time.time() < deadline:
            try:
                state = CapellaAPI.get_cluster_state(
                    self.pod, self.tenant, self.cluster.id)
            except Exception as e:
                self.log.warning(f"[writable] {label}: state read failed: {e}")
                state = None
            if state and state.lower() == "healthy":
                return True
            if state and state.lower() in {s.lower() for s in self._FAILED_STATES}:
                self.log.error(
                    f"[writable] {label}: cluster is in failed state '{state}' — it will "
                    f"not accept a spec update")
                return False
            self.log.info(
                f"[writable] {label}: cluster_state={state}, "
                f"{int(deadline - time.time())}s left")
            if self._budget_exhausted(f"waiting for a writable cluster ({label})"):
                return False
            time.sleep(20)
        self.log.error(
            f"[writable] {label}: cluster is still '{state}' after {timeout}s — not "
            f"triggering the rebalance, because CapellaUtils.scale() would retry the "
            f"rejected spec update indefinitely")
        return False

    def _budget_exhausted(self, label):
        """True once the test has burned its wall-clock budget; records it once.

        A backstop for hangs no specific stage recognises. `test_budget_secs` is the whole
        test's budget, measured from setUp; 0 disables it. Checked at stage boundaries and in
        the long polling loops, so it bounds the run without needing every possible stall to
        be anticipated individually.
        """
        budget = int(self.input.param("test_budget_secs", 14400))
        if not budget:
            return False
        elapsed = time.time() - self._test_start
        if elapsed < budget:
            return False
        if not self._hung:
            self._hung = (f"the test exceeded its {budget}s wall-clock budget "
                          f"(test_budget_secs) at {label}")
            self._record_issue(
                "Test budget exhausted",
                f"{elapsed:.0f}s elapsed against a {budget}s budget at {label}. Remaining "
                f"waits are skipped so the run ends with a usable report instead of "
                f"blocking. Raise test_budget_secs if the run legitimately needs longer.")
        return True

    def _cluster_instance_ids(self):
        """Instance IDs of the KV nodes currently in the cluster."""
        return {
            i.get("InstanceId") for i in self.fusion_aws_util.list_instances(
                self.fusion_aws_util._cluster_filter(self.cluster.id),
                log="ClusterNodes", suppress_log=True)}

    def _run_on_cluster_node(self, instance_id, command, timeout=60):
        """Run a shell command on a KV node over SSM; return the result dict."""
        self.log.info(f"[ssm] {instance_id}: {command}")
        result = self.fusion_aws_util.ec2.run_shell_command(
            instance_id, command, timeout=timeout)
        self.log.info(
            f"[ssm] {instance_id} -> success={result.get('success')} "
            f"rc={result.get('return_code')} stdout={result.get('stdout', '')!r} "
            f"stderr={result.get('stderr', '')!r}")
        return result

    def _wait_for_no_fusion_infra(self, timeout=None, poll_interval=15):
        """Wait until no accelerators, ASGs or guest volumes remain. Returns a bool.

        Used by the abort/invalidation tests, where the expected recovery is that the CP
        tears the orphaned infrastructure down rather than leaving it running.
        """
        timeout = timeout or self.cp_monitor.EBS_CLEANUP_TIMEOUT
        deadline = time.time() + timeout
        last = None
        while time.time() < deadline:
            accel = self.fusion_aws_util.list_accelerator_instances(
                self._accelerator_filter(), log="OrphanCheck")
            asgs = self.fusion_aws_util.list_cluster_fusion_asg(self.cluster.id)
            vols = self._list_accelerator_volumes(guest_only=True)
            last = (len(accel), len(asgs), len(vols))
            self.log.info(
                f"[orphan-check] accelerators={last[0]} asgs={last[1]} "
                f"guest_volumes={last[2]} (elapsed "
                f"{timeout - (deadline - time.time()):.0f}s)")
            if last == (0, 0, 0):
                return True
            if self._budget_exhausted("orphan check"):
                self.log.error(
                    f"Abandoning the orphan check with accelerators={last[0]} "
                    f"asgs={last[1]} guest_volumes={last[2]}")
                return False
            time.sleep(poll_interval)
        self.log.error(
            f"Fusion infrastructure still present after {timeout}s: "
            f"accelerators={last[0]} asgs={last[1]} guest_volumes={last[2]}")
        return False

    # ------------------------------------------------------------------
    # CBS-side rebalance state (phase 7 onward)
    # ------------------------------------------------------------------

    def _start_background_load(self, label):
        """Launch a non-blocking mutation workload; return the loader tasks (or []).

        The third lever for widening the abort window, and the one that acts on the vBucket
        movement itself rather than around it. A fusion rebalance is quick precisely because
        the data is already on the guest volumes when CBS starts moving vBuckets — but
        mutations landing DURING that movement are not on any guest volume, so each vBucket
        move has to chase a moving target through DCP. More to move means longer to move it.

        Deliberately update-only: creates would push the item count past what the rest of the
        test (and tearDown's validation) expects, and deletes would fight the read workload at
        A7. Rate comes from `rebl_ops_rate`, the same param the volume suite uses for
        during-rebalance mutations.

        Each bucket's loadDefn["ops"] is saved to bucket.original_ops and restored by
        _stop_background_load, so a leftover rate cannot change later loads in the same run.
        """
        rate = int(self.input.param("rebl_ops_rate", 5000))
        buckets = list(self.cluster.buckets)
        if not buckets:
            self.log.warning(f"[bg-load] {label}: no buckets to mutate")
            return []
        update_end = int(self.input.param(
            "bg_load_update_end", self.input.param("create_end", 20_000_000)))
        for bucket in buckets:
            JavaDocLoaderUtils.generate_docs(
                bucket=bucket, doc_ops=["update"],
                update_start=0, update_end=update_end)
            bucket.original_ops = bucket.loadDefn.get("ops")
            bucket.loadDefn["ops"] = rate
        try:
            tasks = JavaDocLoaderUtils.perform_load(
                cluster=self.cluster, buckets=buckets,
                overRidePattern={"create": 0, "read": 0, "update": 100,
                                 "delete": 0, "expiry": 0},
                wait_for_load=False, validate_data=False, wait_for_stats=False,
                suppress_error_table=True, track_failures=False)
        except Exception as e:
            self.log.warning(
                f"[bg-load] {label}: could not start the background load ({e}) — the "
                f"rebalance window will be shorter than intended")
            return []
        # perform_load returns False on a task-creation failure, [] when nothing ran.
        if not tasks:
            self.log.warning(
                f"[bg-load] {label}: no loader tasks were created — continuing without "
                f"background mutations")
            return []
        self.log.info(
            f"[bg-load] {label}: {len(tasks)} update task(s) running at "
            f"{rate} ops/s over items [0, {update_end}) across {len(buckets)} bucket(s)")
        return tasks

    def _stop_background_load(self, tasks, label):
        """Stop the background mutation tasks and restore each bucket's ops rate.

        Never raises: this runs on the cleanup path, where a task that has already finished
        or a loader that has gone away must not mask the finding the test was after.
        """
        for task in (tasks or []):
            try:
                self.task_manager.stop_task(task)
            except Exception as e:
                self.log.warning(
                    f"[bg-load] {label}: could not stop {getattr(task, 'thread_name', task)}"
                    f" ({e}) — it may already have finished")
        for bucket in list(self.cluster.buckets):
            original = getattr(bucket, "original_ops", None)
            if original is not None:
                bucket.loadDefn["ops"] = original
        if tasks:
            self.log.info(f"[bg-load] {label}: background load stopped")

    def _wait_for_instances_terminated(self, instance_ids, timeout, poll_interval=15):
        """Wait until none of `instance_ids` is a running accelerator. Returns a bool.

        Scoped to specific IDs rather than "no accelerators at all" because the CP may
        legitimately start a fresh attempt after an abort, whose accelerators are a new and
        entirely valid fleet.
        """
        if not instance_ids:
            return True
        deadline = time.time() + timeout
        remaining = set(instance_ids)
        while time.time() < deadline:
            live = {i.get("InstanceId") for i in
                    self._list_accelerator_instances_by_tag(log="ReleaseCheck")}
            remaining = set(instance_ids) & live
            self.log.info(
                f"[release-check] {len(remaining)} of {len(instance_ids)} original "
                f"accelerator(s) still running{': ' + str(sorted(remaining)) if remaining else ''} "
                f"(total live now: {len(live)})")
            if not remaining:
                return True
            time.sleep(poll_interval)
        self.log.error(
            f"Original accelerator(s) still running after {timeout}s: {sorted(remaining)}")
        return False

    def _cbs_rebalance_state(self, rest=None):
        """What CBS itself says about a rebalance: (running, progress_pct, detail).

        A fusion rebalance spends most of its life in CP-owned phases where ns_server has
        no rebalance at all — accelerators launch (4), download shards (5), volumes are
        transferred to the KV nodes (6) — and only at phase 7 does the CP call
        `POST /controller/rebalance` and hand the actual vBucket movement to CBS. So
        "the rebalance task is running" from the Capella task's point of view says nothing
        about whether ns_server has anything to stop.

        Two endpoints are read because they fail differently: rebalanceProgress reports
        `status: none` between rebalances and per-node fractions while one runs, whereas
        /pools/default/tasks carries the rebalance task's own status and progress and
        survives the moment where progress is still empty. Either reporting 'running' is
        taken as running.

        progress_pct is None when nothing reports a number yet — that is normal in the
        first seconds and must not be read as 0% progress.
        """
        rest = rest or ClusterRestAPI(self.cluster.master)
        running = False
        progress = None
        detail = dict()

        status, content = rest.rebalance_progress()
        if status and isinstance(content, dict):
            detail["rebalanceProgress"] = content
            if str(content.get("status", "")).lower() == "running":
                running = True
                fractions = [v for k, v in content.items()
                             if k != "status" and isinstance(v, (int, float))]
                if fractions:
                    progress = 100.0 * sum(fractions) / len(fractions)

        status, tasks = rest.cluster_tasks()
        if status and isinstance(tasks, list):
            for task in tasks:
                if task.get("type") != "rebalance":
                    continue
                detail["task"] = task
                if str(task.get("status", "")).lower() == "running":
                    running = True
                    if task.get("progress") is not None:
                        try:
                            progress = float(task["progress"])
                        except (TypeError, ValueError):
                            pass
                break
        return running, progress, detail

    def _wait_for_cbs_rebalance_running(self, rebalance_task, timeout,
                                        min_progress=0.0, poll_interval=None):
        """Block until CBS reports a rebalance running; return (running, progress, detail).

        This is the gate for any fault that has to land on the ns_server rebalance rather
        than on the CP's earlier phases. Returns running=False if the window never opened —
        either the rebalance finished first (a fusion rebalance moves vBuckets quickly,
        since the data is already on the guest volumes) or it failed before phase 7.

        min_progress > 0 waits for the movement to be demonstrably under way rather than
        merely accepted, at the cost of a smaller window before it completes.

        find_master() is deliberately NOT called up front. It took 160s on a cluster that was
        mid-scale-out, and every one of those seconds is spent blind: one run's first sample
        landed 11s after CBS had started rebalancing, on a rebalance that lasted 17s in total.
        The existing master answers these two read-only endpoints regardless of whether it is
        still the orchestrator, so the refresh is deferred to the error path.

        poll_interval defaults to `cbs_rebalance_poll_interval` (5s). It has to be short: on
        Capella there is no way to stall the rebalance, so the window can be as narrow as the
        ~17s a measured run took end to end, and a 15s cadence would routinely sample once
        and miss it.
        """
        poll_interval = poll_interval if poll_interval is not None else int(
            self.input.param("cbs_rebalance_poll_interval", 5))
        self.fusion_monitor.set_admin_credentials(self.cluster)
        rest = ClusterRestAPI(self.cluster.master)
        deadline = time.time() + timeout
        while time.time() < deadline:
            if rebalance_task.state in self._FAILED_STATES:
                self.log.error(
                    f"Capella reports the rebalance failed ({rebalance_task.state}) before "
                    f"CBS ever started one — phase 7 was never reached")
                return False, None, dict()
            if rebalance_task.state == "healthy":
                self.log.warning(
                    "The rebalance completed before CBS was observed rebalancing — the "
                    "vBucket movement window was missed entirely")
                return False, None, dict()
            try:
                running, progress, detail = self._cbs_rebalance_state(rest)
            except Exception as e:
                self.log.warning(
                    f"Could not read the CBS rebalance state ({e}); refreshing master")
                self.find_master(self.tenant, self.cluster)
                rest = ClusterRestAPI(self.cluster.master)
                time.sleep(poll_interval)
                continue
            self.log.info(
                f"[cbs-rebalance] running={running} progress="
                f"{'?' if progress is None else f'{progress:.1f}%'} "
                f"(need >= {min_progress}%) task_state={rebalance_task.state} "
                f"{int(deadline - time.time())}s left")
            if running and (progress or 0.0) >= min_progress:
                return True, progress, detail
            if self._budget_exhausted("waiting for the CBS rebalance to start"):
                return False, None, dict()
            time.sleep(poll_interval)
        return False, None, dict()

    @staticmethod
    def _rebalance_task_note(detail):
        """Whatever the rebalance task says about how it ended, for abort evidence.

        ns_server reports `status: notRunning` both for a rebalance that was stopped and one
        that finished on its own, so the status alone cannot confirm an abort. Any
        errorMessage and the rebalance report URI are the closest available discriminators.
        """
        task = (detail or {}).get("task") or {}
        return {k: task.get(k) for k in
                ("status", "errorMessage", "lastReportURI", "rebalanceId", "statusId")
                if task.get(k) is not None}

    def _resync_kv_node_count(self):
        """Set self.num_nodes["data"] to the KV count ns_server actually reports; return it.

        rebalance_config() increments num_nodes when a rebalance is REQUESTED, so after an
        aborted one the framework records the topology that was asked for rather than the one
        that exists. Triggering the next rebalance off that stale number can produce a spec
        identical to the current topology — a no-op with nothing for the CP to move, which
        then looks like "fusion did not launch accelerators".
        """
        self.fusion_monitor.set_admin_credentials(self.cluster)
        self.find_master(self.tenant, self.cluster)
        status, content = ClusterRestAPI(self.cluster.master).cluster_details()
        if not status or not isinstance(content, dict):
            raise Exception(f"GET /pools/default failed: {content}")
        kv_nodes = [n for n in (content.get("nodes") or [])
                    if "kv" in (n.get("services") or [])]
        actual = len(kv_nodes)
        booked = self.num_nodes["data"]
        if actual != booked:
            self.log.warning(
                f"KV node count: ns_server reports {actual}, the framework had {booked} "
                f"booked — resyncing to {actual} so the next rebalance spec is a real "
                f"topology change")
        else:
            self.log.info(f"KV node count agrees at {actual}")
        self.num_nodes["data"] = actual
        return actual

    def _run_recovery_rebalance(self, s3_bucket_name, label):
        """Run one normal fusion rebalance and assert it behaves, after a fault.

        The point is not the rebalance itself but that the cluster is still *usable*: an
        Invalidated manifest must not leave the CP believing a rebalance is still active.
        ACCELERATION.md §Manifest Status Lifecycle is explicit that `Pending`,
        `DownloadComplete` and `BackgroundMigration` block new parallel rebalances while
        `Invalidated`/`TearingDown`/`Complete` do not — so a fresh rebalance being accepted
        and taking the fusion path is the direct check that the abort left the manifest in an
        inactive state rather than wedging the cluster.

        Direction is derived from the ACTUAL topology rather than the requested one, and
        moves back toward the initial node count where that is possible, so this doubles as
        the topology restore tearDown would otherwise have to do.
        """
        # The abort may have left the cluster mid-transition. Triggering a rebalance now
        # would call CapellaUtils.scale(), which retries a rejected spec update forever.
        if not self._wait_until_writable(label):
            self._record_issue(
                f"{label}: recovery rebalance skipped",
                "the cluster never returned to 'healthy' after the fault, so it would "
                "reject a spec update. That is itself a finding — the abort left the "
                "cluster unable to accept another rebalance — but it is reported here "
                "rather than by blocking on an unbounded retry loop.")
            return

        actual = self._resync_kv_node_count()
        direction = self.input.param(
            "post_abort_rebalance_type",
            "out" if actual > self._MIN_KV_NODES else "in")
        self.log.info(
            f"Recovery rebalance ({label}): cluster has {actual} KV node(s), "
            f"rebalancing '{direction}'")

        task = self._trigger_rebalance(direction)
        self.sleep(30, "Wait for the recovery rebalance to start")

        with self._stage(f"{label}a: the recovery rebalance takes the fusion path"):
            instances = self._poll_until_accelerators_appear(task)
            self.assertGreater(
                len(instances), 0,
                f"The recovery rebalance launched no accelerators. Either the CP refused "
                f"the fusion path after the abort, or it silently used DCP — both mean the "
                f"cluster did not fully recover its fusion capability. (A blocked-by-active-"
                f"manifest refusal shows up as the rebalance never starting at all.)")
            self.log.info(
                f"Recovery rebalance launched {len(instances)} accelerator(s) — fusion is "
                f"working again after the fault")

        self.wait_for_rebalances([task])
        CapellaAPI.wait_until_done(
            self.pod, self.tenant, self.cluster.id,
            timeout=self.input.param("recovery_settle_timeout", 1800))

        with self._stage(f"{label}b: the recovery rebalance completed cleanly"):
            self.assertNotIn(
                task.state, self._FAILED_STATES,
                f"The recovery rebalance failed ({task.state}) — the cluster did not "
                f"return to a state where a normal fusion rebalance succeeds")
            state = CapellaAPI.get_cluster_state(
                self.pod, self.tenant, self.cluster.id)
            self.assertEqual(
                state.lower(), "healthy",
                f"Cluster is not healthy after the recovery rebalance: {state}")

        with self._stage(f"{label}c: the recovery rebalance tore down cleanly"):
            self._validate_teardown(s3_bucket_name)

    # ------------------------------------------------------------------
    # Boundary E: node removed while its guest volumes are attached
    # ------------------------------------------------------------------

    def test_remove_node_with_attached_guest_volumes(self):
        """
        Remove a KV node while guest volumes are still attached to the cluster, with
        background migration frozen so nothing has been reclaimed yet.

        STAGE_TEST_MATRIX §Negative, boundary E: "node removed while its guest volumes are
        migrating". The risk is an orphan: a volume attached to a node that no longer
        exists, or a volume the CP forgets about because its owner disappeared between the
        manifest being written and migration finishing.

        Sequence (kv_nodes=3):
          1. freeze migration, so volumes persist for the whole test
          2. rebalance IN  3 -> 4, guest volumes land on the KV nodes
          3. record which node holds which volumes
          4. rebalance OUT 4 -> 3 with those volumes still attached
          5. assert the removal completes and leaves no volume attached to an instance
             that is no longer in the cluster
          6. resume migration, assert everything drains and teardown is clean

        Both outcomes at step 5 are acceptable and recorded: the CP may re-home the
        departing node's volumes onto the remaining nodes, or release and re-create them.
        What is NOT acceptable is a volume still attached to a removed instance, or a
        volume with no owner once migration has finished.
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")
        self._apply_fusion_config_from_params()
        self._apply_fusion_sync_threshold()
        s3_bucket_name, _ = self._capture_s3_log_store_baseline()
        self._load_above_threshold()

        if not self._pause_migration():
            self.fail(
                "Could not freeze background migration — without it the guest volumes may "
                "be reclaimed before the node removal, and there is no attached volume "
                "left for this test to disturb")

        try:
            # ---- rebalance IN: create the guest volumes -------------------------------
            with self._stage("Chaos E1: rebalance in, guest volumes attached"):
                task_in = self._trigger_rebalance("in")
                self.sleep(30, "Wait for the scale-out to start")
                self._wait_for_accelerator_fleet_stable(task_in)
            self.wait_for_rebalances([task_in])
            CapellaAPI.wait_until_done(
                self.pod, self.tenant, self.cluster.id, timeout=600)

            before = self._log_guest_volume_placement("before node removal")
            attached_before = {i: v for i, v in before.items() if i != "unattached"}
            self.assertTrue(
                attached_before,
                "No guest volumes are attached to any cluster node after the scale-out, "
                "even with migration frozen — nothing for this test to remove a node "
                "underneath")
            instances_before = self._cluster_instance_ids()

            # ---- rebalance OUT: remove a node while volumes are attached --------------
            with self._stage("Chaos E2: rebalance out with volumes still attached"):
                # scale() retries a rejected spec update forever, so never trigger on a
                # cluster that is not writable (see _wait_until_writable).
                self.assertTrue(
                    self._wait_until_writable("Chaos E2"),
                    "Cluster did not return to 'healthy' after the scale-out, so the "
                    "scale-in cannot be triggered — a spec update would be rejected and "
                    "retried indefinitely")
                task_out = self._trigger_rebalance("out")
                self.sleep(30, "Wait for the scale-in to start")
                self.assertNotIn(
                    task_out.state, self._FAILED_STATES,
                    f"Scale-in failed immediately after being triggered with guest "
                    f"volumes attached: {task_out.state}")
            self.wait_for_rebalances([task_out])
            CapellaAPI.wait_until_done(
                self.pod, self.tenant, self.cluster.id, timeout=600)

            # ---- the invariant: no volume left on a departed instance -----------------
            with self._stage("Chaos E3: no guest volume orphaned on the removed node"):
                after = self._log_guest_volume_placement("after node removal")
                instances_after = self._cluster_instance_ids()
                removed = instances_before - instances_after
                self.log.info(
                    f"Instances removed by the scale-in: {sorted(removed) or 'none'}")

                orphaned = {inst: vols for inst, vols in after.items()
                            if inst not in ("unattached",)
                            and inst not in instances_after}
                self.assertEqual(
                    orphaned, {},
                    f"Guest volume(s) are still attached to instance(s) that are no "
                    f"longer part of the cluster: {orphaned}. The node was removed "
                    f"without its volumes being re-homed or released.")

                # Report which recovery path the CP took — both are valid.
                moved = {i: v for i, v in after.items() if i != "unattached"}
                if removed and any(i in removed for i in before if i != "unattached"):
                    self.log.info(
                        "A node holding guest volumes was removed; volumes are now on "
                        f"{sorted(moved)} (unattached: "
                        f"{len(after.get('unattached', []))})")
                else:
                    self.log.info(
                        "The CP removed a node that held no guest volumes — the orphan "
                        "invariant is still checked, but the interesting path was not "
                        "exercised. Raise fusion_min_split_size_gb coverage or rerun; "
                        "which node is removed is the CP's choice.")

            # ---- resume and confirm everything is still reclaimable ------------------
            baseline_du = self._avg_main_volume_usage()
            with self._stage("Chaos E4: resume migration"):
                self._resume_migration()
            with self._stage("Chaos E5: all guest volumes drain after the removal"):
                self._validate_guest_volume_drain(baseline_du=baseline_du)
            with self._stage("Chaos E6: teardown clean after the removal"):
                self._validate_teardown(s3_bucket_name)
        finally:
            self._restore_migration_rate_limit()

        self._assert_no_stage_issues()

    # ------------------------------------------------------------------
    # Boundary C -> D: guest volume slot exhaustion
    # ------------------------------------------------------------------

    def test_slot_exhaustion_triggers_fallback_replacement(self):
        """
        Saturate a host's guest volume slots, then ask for another fusion rebalance.

        STAGE_TEST_MATRIX §Negative, boundary C→D. ACCELERATION.md §Error Handling —
        "Slot exhaustion fallback": when a host runs out of guest volume slots
        (`ErrMaximumVolumeSlotsUsed`), the deployer can fall back to fusion fallback
        replacement, swapping the saturated node for a fresh one with all 22 slots free.

        Method: `fusion_min_split_size_gb` is set low enough that
        ceil(hostData / minSplitSize) exceeds `fusion_max_slots`, so the cap binds and each
        host carries its maximum number of guest volumes. Migration stays frozen so those
        slots are still occupied when the second rebalance is requested.

        This test deliberately does not assert one specific recovery. Three outcomes are
        acceptable, and which one occurred is logged and recorded:
          * fallback replacement — new KV instances appear, taking over from saturated ones
          * clean refusal — the rebalance fails with a CP error rather than hanging
          * accommodation — the CP finds slots anyway (e.g. it rebalanced fewer shards)
        What is NOT acceptable: the rebalance hangs past its timeout, or infrastructure is
        left orphaned once migration resumes.
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")
        split_gb, max_slots = self._apply_fusion_config_from_params()
        self._apply_fusion_sync_threshold()
        self.assertTrue(
            split_gb and max_slots,
            "This test needs both fusion_min_split_size_gb and fusion_max_slots set so "
            "the slot cap actually binds — see the conf entry for values that saturate "
            "the slots at this data size")
        s3_bucket_name, _ = self._capture_s3_log_store_baseline()
        self._load_above_threshold()

        if not self._pause_migration():
            self.fail(
                "Could not freeze background migration — the slots would be released as "
                "migration completes and never reach saturation")

        try:
            with self._stage("Chaos S1: first rebalance fills the guest volume slots"):
                task_one = self._trigger_rebalance("in")
                self.sleep(30, "Wait for the first rebalance to start")
                self._wait_for_accelerator_fleet_stable(task_one)
            self.wait_for_rebalances([task_one])
            CapellaAPI.wait_until_done(
                self.pod, self.tenant, self.cluster.id, timeout=600)

            with self._stage("Chaos S2: slots are saturated before the second rebalance"):
                placement = self._log_guest_volume_placement("after first rebalance")
                attached = {i: v for i, v in placement.items() if i != "unattached"}
                self.assertTrue(
                    attached, "No guest volumes attached after the first rebalance")
                busiest = max(len(v) for v in attached.values())
                self.log.info(
                    f"Busiest node holds {busiest} guest volume(s) against "
                    f"fusion_max_slots={max_slots} (minSplitSize={split_gb} GB)")
                if busiest < max_slots:
                    self._record_issue(
                        "Chaos S2: slots not saturated",
                        f"the busiest node holds {busiest} of {max_slots} slots, so the "
                        f"cap did not bind and the second rebalance will not hit "
                        f"ErrMaximumVolumeSlotsUsed. Lower fusion_min_split_size_gb or "
                        f"raise create_end so ceil(hostData/minSplitSize) > "
                        f"fusion_max_slots.")

            # ---- second rebalance against saturated slots ----------------------------
            instances_before = self._cluster_instance_ids()
            outcome = None
            with self._stage("Chaos S3: second rebalance against saturated slots"):
                # A refusal is one of the accepted outcomes of this test, but it has to come
                # from the CP rejecting the REBALANCE, not from scale() being unable to
                # submit the spec at all — the latter retries indefinitely.
                self.assertTrue(
                    self._wait_until_writable("Chaos S3"),
                    "Cluster did not return to 'healthy' after the first rebalance, so the "
                    "second one cannot be submitted — a spec update would be rejected and "
                    "retried indefinitely, which is not the slot-exhaustion refusal this "
                    "test is looking for")
                task_two = self._trigger_rebalance("out")
                self.sleep(30, "Wait for the second rebalance to start")
                deadline = time.time() + self.rebalance_timeout
                while time.time() < deadline:
                    if task_two.state in self._FAILED_STATES:
                        outcome = f"clean refusal (task state={task_two.state})"
                        break
                    if task_two.state == "healthy":
                        outcome = "accommodated (rebalance completed)"
                        break
                    new_instances = self._cluster_instance_ids() - instances_before
                    if new_instances:
                        outcome = f"fallback replacement (new nodes {sorted(new_instances)})"
                        break
                    time.sleep(15)
                self.assertIsNotNone(
                    outcome,
                    f"Second rebalance neither completed, failed, nor replaced a node "
                    f"within {self.rebalance_timeout}s — it hung with slots saturated, "
                    f"which is the one outcome the slot-exhaustion path must not produce")
                self.log.info(f"Slot exhaustion outcome: {outcome}")
                self._record_issue(
                    "Chaos S3: slot exhaustion outcome (informational)",
                    f"observed: {outcome}", severity="INFO")

            # A refused rebalance still has to leave the cluster usable and clean.
            try:
                self.wait_for_rebalances([task_two])
            except Exception as e:
                self.log.warning(
                    f"Second rebalance did not complete cleanly ({e}) — continuing to "
                    f"the cleanup assertions, which is where a refusal must still behave")
            CapellaAPI.wait_until_done(
                self.pod, self.tenant, self.cluster.id, timeout=900)

            baseline_du = self._avg_main_volume_usage()
            with self._stage("Chaos S4: resume migration"):
                self._resume_migration()
            with self._stage("Chaos S5: saturated slots are all released"):
                self._validate_guest_volume_drain(baseline_du=baseline_du)
            with self._stage("Chaos S6: no orphaned infrastructure after saturation"):
                self._validate_teardown(s3_bucket_name)
        finally:
            self._restore_migration_rate_limit()

        self._assert_no_stage_issues()

    # ------------------------------------------------------------------
    # Boundary D: rebalance aborted -> manifest invalidated -> teardown
    # ------------------------------------------------------------------

    def test_abort_rebalance_invalidates_manifest(self):
        """
        Abort the CBS rebalance mid-flight and assert the CP cleans up after itself.

        STAGE_TEST_MATRIX §Negative, boundary D. ACCELERATION.md §Error Handling —
        "Manifest invalidation": if CBS loses its plan UUID state (e.g. after a failed
        rebalance clears ns_server's tracking) the accelerator detects
        `ErrFusionPlanNotFound` and reconciles — marking the manifest `Invalidated` and
        queueing teardown to clean up the orphaned infrastructure.

        The fault is `POST /controller/stopRebalance`, and WHEN it is sent is the whole
        test. A fusion rebalance is not one long ns_server rebalance: phases 4-6 (launch
        accelerators, download shards from S3, transfer the guest volumes onto the KV nodes)
        are entirely CP-owned, and ns_server has no rebalance at all during them. Only at
        phase 7 does the CP call `POST /controller/rebalance` with the plan UUID and hand
        the actual vBucket movement to CBS. Sending stopRebalance before that point aborts
        nothing — ns_server answers "not rebalancing", there is no plan-UUID state to lose,
        and the invalidation path under test is never entered.

        So Stage A2 gates on CBS's own view (`/pools/default/rebalanceProgress` and
        `/pools/default/tasks`, see _wait_for_cbs_rebalance_running) and only fires the abort
        once ns_server confirms it is rebalancing. Stage A3 sends it and Stage A4 confirms
        CBS stopped — an accepted-but-ineffective stopRebalance would otherwise let the rest
        of the test pass on a rebalance that simply ran to completion.

        The window is genuinely narrow, and narrow in a direction worth knowing about: by
        phase 7 the data is already sitting on the guest volumes attached to the KV nodes, so
        the vBucket movement CBS still has to do is fast. One measured run completed the whole
        CBS rebalance in ~17s, and the abort landed 12s in at 31.5% progress — about 5s from
        natural completion. Since ns_server reports `notRunning` for a stopped rebalance and a
        finished one alike, an abort that loses that race is indistinguishable from one that
        worked.

        So the window is widened by the two levers available on Capella:

        1. `fusion_min_split_size_gb` — a smaller minimum shard size splits the same data into
           many more shards, so many more guest volumes have to be mounted onto the KV nodes.
           That mounting is a stage INSIDE the CBS rebalance (it shows up as
           `stageInfo.mountingVolumes`, measured at 3.0s for 5 volumes), so more shards means
           a longer rebalance. It costs one accelerator per shard, so it is a lever to turn
           knowingly, and it does not increase the vBucket movement work itself.
        2. `abort_bg_load` (default on) — mutations landing during the movement are on no
           guest volume, so each vBucket move has to chase them through DCP. This is the only
           lever that lengthens the movement itself rather than the stages around it. Rate
           from `rebl_ops_rate`; see _start_background_load.

        The sharpest lever is NOT available here. On-prem,
        `test_abort_rebalance_lease_handling` sets
        `testconditions:set(rebalance_start, {delay, N})` to hold ns_server in `running`
        without moving a vBucket, which makes the abort trivially easy to land. `/diag/eval`
        is not usable on Capella dedicated, so that approach was tried and removed. What
        remains is a genuine race against the vBucket movement, and the two levers above only
        lengthen it — they do not make it deterministic.

        `abort_progress_ceiling_pct` is therefore load-bearing rather than a formality: it
        records an issue whenever the abort lands above that much progress, which is the
        signal that the run probably raced natural completion and its pass should not be
        trusted. Check it on every run.

        WHAT "CLEANED UP" MEANS HERE, precisely. Stage A5 asserts the ACCELERATOR INSTANCES
        captured at A1 are terminated, and nothing more, because two other things are
        legitimate and were both observed:
          * the CP starts a fresh attempt after the abort — ASGs went 0 -> 3 and two new
            accelerators appeared about 50s later. Those belong to a new plan, so a
            "no accelerators at all" check would fail on correct behaviour;
          * the aborted plan's guest volumes stay attached to the KV nodes, because migration
            is frozen and a frozen cluster cannot reclaim them. All 5 were still there 20
            minutes later, which is exactly right and not an orphan.
        Guest volume cleanup is therefore asserted at A9c instead, by which point migration
        has been resumed. A5 records the CP's response — new accelerators, new ASGs, how many
        original volumes remain — as an informational note.

        Migration is frozen first so the volumes are certain to exist at the moment of the
        abort rather than having been reclaimed already.

        Finally, stages A8-A9 resume migration and run one NORMAL fusion rebalance. Cleanup
        assertions only show the abort tidied up after itself; whether the cluster is still
        usable is a different question, and the manifest lifecycle is exactly where it could
        fail — `Pending`/`DownloadComplete`/`BackgroundMigration` block new parallel
        rebalances, and only `Invalidated`/`TearingDown`/`Complete` do not. A manifest the CP
        still counts as active would refuse the next rebalance outright, so a fresh rebalance
        that is accepted, takes the fusion path and tears down cleanly is the direct evidence
        the abort left the cluster workable. Set `post_abort_rebalance=False` to skip it.
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")
        self._apply_fusion_config_from_params()
        self._apply_fusion_sync_threshold()
        s3_bucket_name, _ = self._capture_s3_log_store_baseline()
        self._load_above_threshold()

        if not self._pause_migration():
            self.fail(
                "Could not freeze background migration — the guest volumes this test "
                "aborts on top of may be gone before the abort lands")

        accel_ids = set()
        guest_vol_ids = set()
        bg_tasks = []

        try:
            # The only lever left for widening the abort window at runtime (the other is the
            # shard count, set via fusion_min_split_size_gb in the conf). Mutations arriving
            # during the vBucket movement are on no guest volume, so each move has to chase
            # them through DCP.
            if self.input.param("abort_bg_load", True):
                bg_tasks = self._start_background_load("during the aborted rebalance")

            rebalance_task = self._trigger_rebalance()
            self.sleep(30, "Wait for the rebalance to start")

            with self._stage("Chaos A1: accelerators and guest volumes exist"):
                instances, _ = self._wait_for_accelerator_fleet_stable(rebalance_task)
                self.assertGreater(
                    len(instances), 0,
                    "No accelerators launched — nothing to invalidate")
                accel_ids = {i["InstanceId"] for i in instances}
                guest_vol_ids = self._capture_guest_volume_ids(rebalance_task)
                self.log.info(
                    f"About to abort with {len(instances)} accelerator(s) "
                    f"{sorted(accel_ids)} and {len(guest_vol_ids)} guest volume(s) in "
                    f"flight")

            # ---- wait for phase 7: ns_server is actually rebalancing ------------------
            cbs_rebalancing = False
            with self._stage("Chaos A2: CBS is rebalancing (phase 7 reached)"):
                cbs_rebalancing, progress, detail = \
                    self._wait_for_cbs_rebalance_running(
                        rebalance_task,
                        timeout=self.input.param("cbs_rebalance_wait_timeout", 3600),
                        min_progress=float(
                            self.input.param("abort_at_progress_pct", 0.0)))
                self.assertTrue(
                    cbs_rebalancing,
                    f"CBS never reported a rebalance in progress. The CP's phases 4-6 "
                    f"(accelerator launch, S3 download, guest volume transfer) do not "
                    f"involve ns_server, so stopRebalance sent now would abort nothing and "
                    f"the manifest-invalidation path would never be entered. Either the "
                    f"rebalance never reached phase 7, or its vBucket movement finished "
                    f"inside the poll interval — raise cbs_rebalance_wait_timeout, or "
                    f"increase create_end so there is more for CBS to move.")
                self.log.info(
                    f"CBS is rebalancing at "
                    f"{'unknown' if progress is None else f'{progress:.1f}%'} progress — "
                    f"aborting now. State: {detail}")
                # An abort that lands on a rebalance which was about to finish anyway proves
                # nothing: the "it stopped" observation below would be satisfied by natural
                # completion just as well. With no way to stall the rebalance on Capella
                # (/diag/eval is unavailable, so the on-prem rebalance_start delay is out),
                # this is the only guard against reporting a raced run as a pass.
                ceiling = float(self.input.param("abort_progress_ceiling_pct", 90.0))
                if (progress or 0.0) > ceiling:
                    self._record_issue(
                        "Chaos A2: aborting a nearly-complete rebalance",
                        f"CBS was already {progress:.1f}% through the vBucket movement "
                        f"(ceiling {ceiling}%) when the abort was sent, so it may simply "
                        f"have finished on its own — ns_server reports 'notRunning' for both "
                        f"outcomes, so the stages below cannot tell them apart. Treat this "
                        f"run's result as unproven. Lengthen the movement with a smaller "
                        f"fusion_min_split_size_gb (more shards to mount) or a higher "
                        f"rebl_ops_rate (more mutations to chase), or shorten the detection "
                        f"lag with a smaller cbs_rebalance_poll_interval.")

            with self._stage("Chaos A3: abort the rebalance via stopRebalance"):
                self.assertTrue(
                    cbs_rebalancing,
                    "Skipping the abort: CBS was never observed rebalancing, so "
                    "stopRebalance would be sent into a phase that has no rebalance to "
                    "stop (see Chaos A2)")
                status, content = ClusterRestAPI(
                    self.cluster.master).stop_rebalance()
                self.log.info(
                    f"POST /controller/stopRebalance -> status={status} "
                    f"content={content}")
                self.assertTrue(
                    status,
                    f"stopRebalance was rejected, so the abort never happened and this "
                    f"test cannot observe the invalidation path: {content}")

            # An accepted stopRebalance that changes nothing would let every later stage
            # pass on a rebalance that simply ran to completion.
            with self._stage("Chaos A4: CBS actually stopped rebalancing"):
                self.assertTrue(
                    cbs_rebalancing,
                    "Skipping: no abort was sent (see Chaos A2)")
                stopped = False
                deadline = time.time() + self.input.param("stop_rebalance_timeout", 300)
                while time.time() < deadline:
                    running, progress, detail = self._cbs_rebalance_state()
                    if not running:
                        stopped = True
                        self.log.info(
                            f"CBS reports no rebalance running after the abort. Task says: "
                            f"{self._rebalance_task_note(detail)}")
                        break
                    self.log.info(
                        f"[post-abort] CBS still rebalancing at "
                        f"{'?' if progress is None else f'{progress:.1f}%'}")
                    time.sleep(10)
                self.assertTrue(
                    stopped,
                    f"CBS still reports a rebalance running after stopRebalance returned "
                    f"success — the abort was accepted but had no effect, so nothing below "
                    f"this point is evidence about the invalidation path")

            # The accelerators the aborted plan deployed must be released. Scoped to the
            # instance IDs captured at A1, NOT "no accelerators at all": the CP legitimately
            # starts a fresh attempt after an abort, and its accelerators are a new and valid
            # fleet (observed: ASGs 0->3 and two new accelerators appearing ~50s after the
            # abort). Guest volumes are deliberately NOT asserted here — with migration frozen
            # and the volumes already handed to the KV nodes they cannot be reclaimed yet, so
            # that check belongs to A9c, which runs after migration is resumed.
            with self._stage("Chaos A5: the aborted plan's accelerators are released"):
                self.assertTrue(
                    self._wait_for_instances_terminated(
                        accel_ids,
                        timeout=self.input.param("accel_release_timeout",
                                                 self.cp_monitor.EBS_CLEANUP_TIMEOUT)),
                    f"Accelerator instance(s) from the aborted plan are still running: "
                    f"{sorted(accel_ids)}. The manifest-invalidation path did not reclaim "
                    f"the compute it had deployed for a plan CBS no longer knows about.")

                # Record what the CP did next — a retry is expected and worth seeing.
                live_accel = {i.get("InstanceId") for i in
                              self._list_accelerator_instances_by_tag(log="PostAbort")}
                asgs = self.fusion_aws_util.list_cluster_fusion_asg(self.cluster.id)
                placement = self._log_guest_volume_placement("after the abort")
                current_vols = {v for vols in placement.values() for v in vols}
                self._record_issue(
                    "Chaos A5: CP response to the abort (informational)",
                    f"new accelerator(s): {sorted(live_accel - accel_ids) or 'none'}; "
                    f"fusion ASGs now: {len(asgs)}; "
                    f"original guest volume(s) still present: "
                    f"{len(guest_vol_ids & current_vols)} of {len(guest_vol_ids)} "
                    f"(expected while migration is frozen); "
                    f"new guest volume(s): {len(current_vols - guest_vol_ids)}. "
                    f"A fresh attempt here is legitimate — volume cleanup is asserted at "
                    f"A9c, once migration has been resumed.", severity="INFO")

            # The abort has been observed; the mutations have done their job. Stop them here
            # so A6/A7 settle on a quiet cluster and the recovery rebalance at A9 is a clean
            # baseline rather than another loaded one.
            self._stop_background_load(bg_tasks, "after the abort")
            bg_tasks = []

            with self._stage("Chaos A6: cluster healthy and fusion still enabled"):
                CapellaAPI.wait_until_done(
                    self.pod, self.tenant, self.cluster.id, timeout=1800)
                state = CapellaAPI.get_cluster_state(
                    self.pod, self.tenant, self.cluster.id)
                self.assertEqual(
                    state.lower(), "healthy",
                    f"Cluster is not healthy after the aborted rebalance: {state}")
                fusion_state = CapellaAPI.get_fusion_status(
                    self.pod, self.tenant, self.cluster.id).get("state")
                self.assertEqual(
                    fusion_state, "enabled",
                    f"Fusion is not 'enabled' after the aborted rebalance: "
                    f"{fusion_state}")

            with self._stage("Chaos A7: data still readable after the abort"):
                self._run_read_workload("after aborted rebalance")

            # Everything above shows the abort was cleaned up. Whether the cluster is still
            # USABLE is a separate question, and only a fresh rebalance answers it: an
            # Invalidated manifest that the CP still counts as active would block the next
            # rebalance outright (ACCELERATION.md §Manifest Status Lifecycle).
            if self.input.param("post_abort_rebalance", True):
                resumed = False
                with self._stage("Chaos A8: resume migration for the recovery rebalance"):
                    self._resume_migration()
                    resumed = True
                if resumed:
                    self._run_recovery_rebalance(s3_bucket_name, "Chaos A9")
                else:
                    self._record_issue(
                        "Chaos A9: recovery rebalance skipped",
                        "background migration could not be resumed, so a recovery rebalance "
                        "would run with migration frozen and its teardown assertions would "
                        "fail for a reason unrelated to the abort")
        finally:
            self._stop_background_load(bg_tasks, "cleanup")
            self._restore_migration_rate_limit()

        self._assert_no_stage_issues()

    # ------------------------------------------------------------------
    # Boundary E: memcached killed during extent migration
    # ------------------------------------------------------------------

    def test_kill_memcached_during_extent_migration(self):
        """
        Kill memcached on a KV node while it is migrating extents from its guest volumes.

        STAGE_TEST_MATRIX §Negative, boundary E. Extent migration is server-side work
        (`ep_fusion_migration_*`), so losing memcached mid-copy is the sharpest test of
        whether that work is checkpointed and resumable rather than silently abandoned —
        ACCELERATION.md §Error Handling describes checkpointing with attempt history for
        exactly these long-running steps.

        Sequence: freeze migration across the rebalance so a full set of guest volumes is
        attached, resume it, wait until the copy is demonstrably under way, then
        `pkill -9 memcached` on the node holding the most guest volumes. ns_server restarts
        memcached; the assertions are that migration still finishes, every volume is still
        released, no migration failures are reported, and the data is readable afterwards.

        This is destructive by design. The node is expected to come back on its own; if it
        does not, that is the finding.
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")
        self._apply_fusion_config_from_params()
        self._apply_fusion_sync_threshold()
        s3_bucket_name, _ = self._capture_s3_log_store_baseline()
        self._load_above_threshold()

        if not self._pause_migration():
            self.fail(
                "Could not freeze background migration — this test has to kill memcached "
                "while a known set of volumes is mid-copy, which needs the freeze to set "
                "up deterministically")

        try:
            rebalance_task = self._trigger_rebalance()
            self.sleep(30, "Wait for the rebalance to start")
            with self._stage("Chaos M1: guest volumes attached before the kill"):
                self._wait_for_accelerator_fleet_stable(rebalance_task)
            self.wait_for_rebalances([rebalance_task])
            CapellaAPI.wait_until_done(
                self.pod, self.tenant, self.cluster.id, timeout=600)

            placement = self._log_guest_volume_placement("before killing memcached")
            attached = {i: v for i, v in placement.items() if i != "unattached"}
            self.assertTrue(
                attached,
                "No guest volumes attached to any KV node — there is no migration for "
                "the kill to interrupt")
            target_instance = max(attached, key=lambda i: len(attached[i]))
            self.log.info(
                f"Target for the kill: {target_instance}, holding "
                f"{len(attached[target_instance])} guest volume(s)")

            baseline_du = self._avg_main_volume_usage()
            with self._stage("Chaos M2: resume migration"):
                self._resume_migration()

            # Wait for the copy to be demonstrably running before injecting the fault, so
            # the test is not just killing an idle process.
            with self._stage("Chaos M3: migration is under way before the kill"):
                kill_deadline = time.time() + self.input.param(
                    "migration_start_timeout", 600)
                progressed = False
                while time.time() < kill_deadline:
                    completed = self._sum_cbstat_across_nodes(
                        "ep_fusion_migration_completed_bytes")
                    if completed > 0:
                        progressed = True
                        self.log.info(
                            f"Migration has copied {completed} byte(s) — injecting the "
                            f"fault now")
                        break
                    time.sleep(15)
                self.assertTrue(
                    progressed,
                    "ep_fusion_migration_completed_bytes never moved off 0 after "
                    "resuming, so migration was not running and killing memcached would "
                    "prove nothing")

            with self._stage("Chaos M4: kill memcached on the busiest node"):
                result = self._run_on_cluster_node(
                    target_instance,
                    "sudo pkill -9 memcached || pkill -9 memcached")
                self.assertTrue(
                    result.get("success"),
                    f"Could not run the kill command over SSM on {target_instance}: "
                    f"{result}")
                # ns_server restarts memcached; confirm it actually came back.
                back_deadline = time.time() + self.input.param(
                    "memcached_restart_timeout", 300)
                restarted = False
                while time.time() < back_deadline:
                    check = self._run_on_cluster_node(
                        target_instance, "pgrep -c memcached || true")
                    if (check.get("stdout") or "").strip() not in ("", "0"):
                        restarted = True
                        break
                    time.sleep(10)
                self.assertTrue(
                    restarted,
                    f"memcached did not come back on {target_instance} within the "
                    f"restart timeout — ns_server did not restart it")
                self.log.info(f"memcached is running again on {target_instance}")

            with self._stage("Chaos M5: migration still completes after the kill"):
                self._validate_guest_volume_drain(baseline_du=baseline_du)

            with self._stage("Chaos M6: no migration failures were recorded"):
                failures = self._sum_cbstat_across_nodes(
                    "ep_fusion_migration_failures")
                self.assertEqual(
                    failures, 0,
                    f"ep_fusion_migration_failures = {failures} after memcached was "
                    f"killed mid-migration — the copy did not resume cleanly")

            with self._stage("Chaos M7: teardown clean and data readable"):
                self._validate_teardown(s3_bucket_name)
        finally:
            self._restore_migration_rate_limit()

        self._assert_no_stage_issues()

    # ==================================================================
    # AWS infrastructure faults
    #
    # The tests above disturb the fusion flow through CBS or the CP. These
    # attack the AWS layer underneath it: an instance going away, an ASG
    # unable to launch, a guest volume running out of space, a volume
    # yanked off its node mid-transfer. Each uses only Layer-1 primitives
    # (EC2Lib / FISLib), per architecture.md — no boto3 in test code.
    # ==================================================================

    # ------------------------------------------------------------------
    # Moved here from fusion_accelerator_lifecycle_test.py (was test 11):
    # it is a fault-injection test, so it belongs with the chaos suite.
    # ------------------------------------------------------------------

    def test_accelerator_node_termination_resilience(self):
        """
        Terminate one accelerator EC2 instance while a fusion rebalance is in progress.
        The CP must detect the terminated instance via the ASG health check, relaunch a
        replacement, and the fusion rebalance must continue and complete successfully.

        Uses 200M items (create_end=200000000) so accelerators are deployed long enough
        that termination can be issued while phase 5 (S3 download / hydration) is still
        running.

        Validates:
        - Fusion rebalance takes the accelerator path (instances launched)
        - A specific accelerator instance is successfully terminated via the AWS API
        - The terminated instance's own ASG (one ASG per accelerator) relaunches a
          replacement (new InstanceId in that same ASG), OR the rebalance completes
          without entering a failed state (both are valid recovery outcomes — the CP
          may finish the phase before the ASG replaces the node). Checked against that
          one ASG rather than the whole fleet, since other ASGs launching their first
          instance while this fleet is still ramping up would otherwise look
          indistinguishable from "our instance got replaced".
        - The rebalance does NOT transition to any failed state throughout
        - The rebalance completes successfully (cluster reaches 'healthy')
        - EBS guest volumes and ASGs are cleaned up post-rebalance

        Parameters:
        - create_end       — doc count for above-threshold load (default via _load_above_threshold)
        - relaunch_timeout — seconds to wait for a replacement accelerator (default: 600)
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")
        # Shard sizing: applies fusion_min_split_size_gb / fusion_max_slots when the
        # conf sets them (no-op otherwise). Lowering minSplitSize is what lets a small
        # data set still produce a multi-accelerator fleet.
        self._apply_fusion_config_from_params()
        self._apply_fusion_sync_threshold()
        self._load_above_threshold()

        rebalance_task = self._trigger_rebalance()
        self.sleep(30, "Wait for rebalance to start")

        # Wait for the accelerator fleet to stabilise (phase 4 — deploy accelerators)
        # before picking a target. Sampling the first accelerator to appear would pick
        # a fleet that is still ramping up, and later diffing instance IDs against a
        # still-changing fleet cannot tell "the ASG replaced our instance" apart from
        # "some other ASG launched its first instance in the meantime" — both look like
        # a new ID appearing.
        initial_instances, asgs = self._wait_for_accelerator_fleet_stable(rebalance_task)
        self.assertGreater(
            len(initial_instances), 0,
            "No accelerator instances appeared — fusion rebalance did not launch "
            "accelerators; cannot test termination resilience")

        # One ASG per accelerator (see _wait_for_accelerator_fleet_stable), so mapping
        # instance -> owning ASG lets the replacement check below poll that ASG
        # specifically instead of diffing the whole fleet.
        asg_by_instance_id = {
            inst["InstanceId"]: asg.get("AutoScalingGroupName")
            for asg in asgs for inst in asg.get("Instances", [])
        }

        target_instance = initial_instances[0]
        target_id = target_instance["InstanceId"]
        target_asg_name = asg_by_instance_id.get(target_id)
        self.assertIsNotNone(
            target_asg_name,
            f"Could not find the owning ASG for accelerator {target_id} — one ASG per "
            f"accelerator is assumed, so there is no ASG to poll for a replacement")
        self.log.info(
            f"Terminating accelerator instance {target_id} (ASG {target_asg_name}) "
            f"({len(initial_instances)} total accelerators active)")

        terminated = self.fusion_aws_util.ec2.terminate_instance(target_id)
        self.assertTrue(terminated,
                        f"AWS API failed to terminate accelerator instance {target_id}")
        termination_time = time.time()
        self.log.info(
            f"Accelerator instance {target_id} terminated — "
            f"waiting for ASG {target_asg_name} to relaunch a replacement")

        # Poll the SAME ASG the terminated instance belonged to for a new InstanceId, or
        # the rebalance completing without entering a failed state (both are valid
        # recovery outcomes). Checking that one ASG — rather than the whole fleet — is
        # what makes this deterministic: other ASGs launching their own first instance
        # during the same window must not be mistaken for this instance's replacement.
        relaunch_timeout = self.input.param("relaunch_timeout", 600)
        deadline = termination_time + relaunch_timeout
        replacement_seen = False

        while time.time() < deadline:
            if rebalance_task.state in self._FAILED_STATES:
                self.fail(
                    f"Rebalance entered failed state after accelerator termination: "
                    f"{rebalance_task.state}")

            current_asgs = self.fusion_aws_util.list_cluster_fusion_asg(self.cluster.id)
            target_asg = next(
                (a for a in current_asgs
                 if a.get("AutoScalingGroupName") == target_asg_name), None)
            current_asg_instance_ids = (
                {i["InstanceId"] for i in target_asg.get("Instances", [])}
                if target_asg else set())
            new_ids = current_asg_instance_ids - {target_id}
            if new_ids:
                elapsed = time.time() - termination_time
                replacement_seen = True
                self.log.info(
                    f"ASG {target_asg_name} relaunched a replacement after "
                    f"{elapsed:.1f}s: {new_ids}")
                break

            if rebalance_task.state == "healthy":
                # Rebalance completed before an explicit relaunch was observed —
                # the CP recovered and finished without needing a fresh accelerator.
                replacement_seen = True
                self.log.info(
                    "Rebalance completed before replacement accelerator was observed — "
                    "CP handled termination gracefully without relaunch")
                break

            self.log.info(
                f"Waiting for ASG {target_asg_name} to relaunch {target_id} — "
                f"current instances in that ASG: {current_asg_instance_ids or 'none'} — "
                f"{int(deadline - time.time())}s remaining")
            time.sleep(10)

        self.assertTrue(
            replacement_seen,
            f"ASG {target_asg_name} did not relaunch a replacement for {target_id} "
            f"within {relaunch_timeout}s, and the rebalance did not complete either; "
            f"rebalance state: {rebalance_task.state}")

        # Guest volumes are only deleted once hydration (S3 download, then transfer to
        # KV nodes) finishes — checking EBS cleanup before that races the CP instead of
        # validating it. Wait for the attached-volume count to fall back to 0 while the
        # rebalance is still tracked, before asserting overall completion.
        hydration_completed = self.cp_monitor.monitor_fusion_guest_volumes(
            self.tenant, self.cluster, rebalance_task,
            self.fusion_monitor, [],
            wait_for_hydration_complete=True,
            timeout=self.hydration_timeout,
            find_master_func=self.find_master,
        )
        self.assertTrue(
            hydration_completed,
            "EBS guest volume hydration did not complete successfully after "
            "accelerator instance termination")

        # Assert the rebalance completes successfully despite the disruption
        self.wait_for_rebalances([rebalance_task])
        CapellaAPI.wait_until_done(
            self.pod, self.tenant, self.cluster.id, timeout=600)

        # Verify all EBS guest volumes and ASGs are cleaned up post-rebalance
        cleaned = self.cp_monitor.monitor_ebs_cleanup(
            self.cluster, self.stop_run_event,
            timeout=self.cp_monitor.EBS_CLEANUP_TIMEOUT)
        self.assertTrue(cleaned,
                        "EBS guest volumes were not cleaned up after rebalance")

        self.cp_monitor.check_asg_cleanup_after_rebalance([self.cluster])
        self.log.info(
            "Fusion rebalance completed and all resources cleaned up successfully "
            "after accelerator instance termination")

    # ------------------------------------------------------------------
    # A→B: accelerator instance STOPPED (not terminated) mid-download
    # ------------------------------------------------------------------

    def test_accelerator_stopped_mid_download(self):
        """
        Stop — not terminate — an accelerator EC2 instance while it is downloading.

        Distinct from test_accelerator_node_termination_resilience in a way that matters:
        a terminated instance leaves the ASG, so the ASG's own capacity check notices a
        missing member and launches a replacement. A STOPPED instance is still a member;
        it simply fails its health check. Recovery therefore depends on the ASG's health
        check replacing an unhealthy member, or on the CP noticing the download never
        completes — a different code path with a different timeout.

        Accepted outcomes, all logged:
          * the ASG replaces the stopped instance (new InstanceId appears)
          * the CP restarts or reuses the instance (it returns to 'running')
          * the rebalance completes anyway (the phase finished before the stop landed)
        Not accepted: the rebalance enters a failed state, or it hangs with the stopped
        instance still sitting there past `stopped_recovery_timeout`.
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")
        self._apply_fusion_config_from_params()
        self._apply_fusion_sync_threshold()
        s3_bucket_name, _ = self._capture_s3_log_store_baseline()
        self._load_above_threshold()

        rebalance_task = self._trigger_rebalance()
        self.sleep(30, "Wait for the rebalance to start")

        # Wait for a stable fleet before picking a target — otherwise the "ASG replaced
        # it" check below cannot tell this instance's replacement apart from an
        # unrelated ASG still launching its first instance during ramp-up (see
        # test_accelerator_node_termination_resilience for the same fix and why).
        instances, asgs = self._wait_for_accelerator_fleet_stable(rebalance_task)
        self.assertGreater(
            len(instances), 0,
            "No accelerators launched — nothing to stop")
        asg_by_instance_id = {
            inst["InstanceId"]: asg.get("AutoScalingGroupName")
            for asg in asgs for inst in asg.get("Instances", [])
        }
        target_id = instances[0]["InstanceId"]
        target_asg_name = asg_by_instance_id.get(target_id)
        self.assertIsNotNone(
            target_asg_name,
            f"Could not find the owning ASG for accelerator {target_id} — one ASG per "
            f"accelerator is assumed, so there is no ASG to poll for a replacement")

        with self._stage("Chaos AWS1: stop an accelerator instance"):
            self.log.info(
                f"Stopping accelerator {target_id} (ASG {target_asg_name}) "
                f"(stop, not terminate — it stays an ASG member)")
            self.assertTrue(
                self.fusion_aws_util.ec2.stop_instance(target_id),
                f"AWS rejected the stop request for {target_id}")
            # Confirm it actually reached a non-running state, so the fault is real.
            reached = self.fusion_aws_util.ec2.wait_for_instance_state(
                target_id, "stopped",
                timeout=self.input.param("instance_stop_timeout", 300))
            state = self.fusion_aws_util.ec2.get_instance_state(target_id)
            self.log.info(f"{target_id} state after the stop request: {state}")
            if not reached:
                self._record_issue(
                    "Chaos AWS1: stop did not reach 'stopped'",
                    f"{target_id} reported state={state} instead — the CP or ASG may have "
                    f"acted on it first, which is itself a valid recovery; downstream "
                    f"assertions still apply", severity="WARNING")

        with self._stage("Chaos AWS2: fleet recovers or the rebalance completes"):
            outcome = None
            deadline = time.time() + self.input.param("stopped_recovery_timeout", 900)
            while time.time() < deadline:
                self.assertNotIn(
                    rebalance_task.state, self._FAILED_STATES,
                    f"Rebalance failed after an accelerator was stopped: "
                    f"{rebalance_task.state}")
                if rebalance_task.state == "healthy":
                    outcome = "rebalance completed despite the stopped accelerator"
                    break
                # Check the terminated instance's own ASG, not the whole fleet: other
                # ASGs launching their own first instance in this window would
                # otherwise be indistinguishable from "our ASG replaced it".
                current_asgs = self.fusion_aws_util.list_cluster_fusion_asg(
                    self.cluster.id)
                target_asg = next(
                    (a for a in current_asgs
                     if a.get("AutoScalingGroupName") == target_asg_name), None)
                current_asg_instance_ids = (
                    {i["InstanceId"] for i in target_asg.get("Instances", [])}
                    if target_asg else set())
                new_ids = current_asg_instance_ids - {target_id}
                if new_ids:
                    outcome = (f"ASG {target_asg_name} replaced it "
                               f"(new: {sorted(new_ids)})")
                    break
                if self.fusion_aws_util.ec2.get_instance_state(target_id) == "running":
                    outcome = f"{target_id} was restarted and is running again"
                    break
                time.sleep(20)
            self.assertIsNotNone(
                outcome,
                f"No recovery observed within the timeout: {target_id} is still not "
                f"running, no replacement appeared, and the rebalance has not completed. "
                f"A stopped accelerator that nothing reacts to stalls the download phase.")
            self.log.info(f"Stopped-accelerator outcome: {outcome}")
            self._record_issue(
                "Chaos AWS2: stopped-accelerator outcome (informational)",
                outcome, severity="INFO")

        # Guest volumes are only deleted once hydration (S3 download, then transfer to
        # KV nodes) finishes — stopping an accelerator mid-download means whichever
        # instance ends up serving that shard may still be hydrating here. Wait for the
        # attached-volume count to settle back to 0 before _validate_teardown checks
        # EBS cleanup, rather than racing the CP.
        with self._stage("Chaos AWS2b: guest volume hydration settles after recovery"):
            hydration_completed = self.cp_monitor.monitor_fusion_guest_volumes(
                self.tenant, self.cluster, rebalance_task,
                self.fusion_monitor, [],
                wait_for_hydration_complete=True,
                timeout=self.hydration_timeout,
                find_master_func=self.find_master,
            )
            self.assertTrue(
                hydration_completed,
                "EBS guest volume hydration did not complete successfully after the "
                "accelerator was stopped mid-download")

        self.wait_for_rebalances([rebalance_task])
        CapellaAPI.wait_until_done(self.pod, self.tenant, self.cluster.id, timeout=1800)
        with self._stage("Chaos AWS3: teardown clean after the stop"):
            self._validate_teardown(s3_bucket_name)

        self._assert_no_stage_issues()

    # ------------------------------------------------------------------
    # B: guest volume detached from the ACCELERATOR mid-download
    # ------------------------------------------------------------------

    def test_accelerator_volume_detached_during_download(self):
        """
        Force-detach a guest volume from its accelerator while the shard is downloading.

        Boundary B. The volume here is the download target, so pulling it out is the harshest
        interruption the download can suffer: the agent's writes land on a filesystem whose
        block device has vanished.

        Grounding for what should happen, from ACCELERATION.md §Phase 5:
          * the agent boots with its NodeID, compute ID and **volume ID baked into static
            config at launch**, then attaches -> mounts -> downloads -> unmounts ->
            reports Complete;
          * "the accelerator waits (with exponential backoff) for each node to reach
            `Complete`".
        So the node cannot report Complete after losing its volume, and the CP is sitting in
        a backoff loop waiting for it. Recovery has to come from one of two shapes, and
        because the volume ID is baked in, which one it is tells you something real:
          * the SAME volume is re-attached and the download retried — the agent recovered
            using the volume ID it already had;
          * a NEW volume (and usually a new accelerator) is deployed — the CP gave up on
            the node document and redeployed, which is the "idempotent creates" path.
        Both are accepted and recorded. The failure this test exists to catch is neither
        happening: the node stuck `Registered` with no volume while the CP backs off
        forever, which shows up here as no accelerator having a guest volume attached again
        within `detach_download_recovery_timeout`.

        Migration is frozen only to keep the aftermath observable; the fault lands during
        download, long before migration starts.
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")
        self._apply_fusion_config_from_params()
        self._apply_fusion_sync_threshold()
        s3_bucket_name, _ = self._capture_s3_log_store_baseline()
        self._load_above_threshold()

        if not self._pause_migration():
            self.log.warning(
                "Could not freeze background migration — the fault still lands during "
                "download, but the aftermath will be harder to observe")

        try:
            rebalance_task = self._trigger_rebalance()
            self.sleep(30, "Wait for the rebalance to start")

            with self._stage("Chaos DD1: accelerator with an attached download volume"):
                instances = self._poll_until_accelerators_appear(rebalance_task)
                self.assertGreater(
                    len(instances), 0, "No accelerators launched — nothing to detach from")
                accel_ids_before = {i["InstanceId"] for i in instances}
                placement = self._log_guest_volume_placement("during download")
                on_accelerators = {i: v for i, v in placement.items()
                                   if i in accel_ids_before}
                self.assertTrue(
                    on_accelerators,
                    f"No guest volume attached to any accelerator "
                    f"({sorted(accel_ids_before)}) — the download volume is what this "
                    f"test detaches")
                victim_host = sorted(on_accelerators)[0]
                victim_volume = sorted(on_accelerators[victim_host])[0]
                volumes_before = {v for vols in placement.values() for v in vols}
                self.log.info(
                    f"Detaching download volume {victim_volume} from accelerator "
                    f"{victim_host} mid-download")

            with self._stage("Chaos DD2: force-detach the download volume"):
                self.assertTrue(
                    self.fusion_aws_util.ec2.detach_volume(
                        victim_volume, force=True, instance_id=victim_host),
                    f"AWS rejected the force-detach of {victim_volume} from "
                    f"{victim_host}")
                left = self.fusion_aws_util.ec2.wait_for_volume_state(
                    victim_volume, "available",
                    timeout=self.input.param("detach_timeout", 300))
                self.log.info(
                    f"{victim_volume} reported 'available' after the detach: {left}")
                if not left:
                    self._record_issue(
                        "Chaos DD2: volume did not report 'available'",
                        f"{victim_volume} did not settle in 'available' — it may have been "
                        f"re-attached or deleted immediately, both of which are valid CP "
                        f"responses to losing a download volume", severity="WARNING")

            with self._stage("Chaos DD3: a volume is attached again and download retried"):
                outcome = None
                deadline = time.time() + self.input.param(
                    "detach_download_recovery_timeout", 1200)
                while time.time() < deadline:
                    if rebalance_task.state in self._FAILED_STATES:
                        outcome = f"clean failure ({rebalance_task.state})"
                        break
                    # Tag-based listing: a freshly deployed accelerator whose volume has
                    # not attached yet would not match the 16000-IOPS filter.
                    accel_now = {i.get("InstanceId") for i in
                                 self._list_accelerator_instances_by_tag(
                                     log="DetachRecovery")}
                    placement_now = self._guest_volumes_by_instance()
                    attached_to_accel = {i: v for i, v in placement_now.items()
                                         if i in accel_now}
                    if attached_to_accel:
                        vols_now = {v for vols in attached_to_accel.values()
                                    for v in vols}
                        new_vols = vols_now - volumes_before
                        new_hosts = accel_now - accel_ids_before
                        if victim_volume in vols_now:
                            outcome = (f"the SAME volume {victim_volume} was re-attached "
                                       f"(host(s) {sorted(attached_to_accel)}) — the agent "
                                       f"recovered with the volume ID baked in at launch")
                            break
                        if new_vols:
                            outcome = (f"a NEW volume was deployed: {sorted(new_vols)} on "
                                       f"{sorted(attached_to_accel)}"
                                       + (f", with new accelerator(s) {sorted(new_hosts)}"
                                          if new_hosts else "")
                                       + " — the CP redeployed rather than reusing the "
                                         "node document")
                            break
                    if rebalance_task.state == "healthy":
                        outcome = ("rebalance completed without any accelerator holding a "
                                   "volume again")
                        break
                    time.sleep(20)
                self.assertIsNotNone(
                    outcome,
                    f"No accelerator had a guest volume attached again within the recovery "
                    f"timeout, and the rebalance neither completed nor failed. The node "
                    f"cannot reach 'Complete' without its volume, so the CP is backing off "
                    f"on a download that can never finish — the stall this test exists to "
                    f"catch.")
                self.log.info(f"Detach-during-download outcome: {outcome}")
                if outcome.startswith("rebalance completed without"):
                    self._record_issue(
                        "Chaos DD3: rebalance completed with no volume re-attached",
                        f"the rebalance reported healthy although {victim_volume} was "
                        f"detached mid-download and no accelerator picked up a volume "
                        f"again. The shard cannot have finished downloading, so check what "
                        f"the KV nodes actually received.")
                else:
                    self._record_issue(
                        "Chaos DD3: detach-during-download outcome (informational)",
                        outcome, severity="INFO")

            with self._stage("Chaos DD4: the detached volume does not dangle"):
                vol = self.fusion_aws_util.ec2.get_ebs_volume_by_id(victim_volume)
                if vol is None:
                    self.log.info(f"{victim_volume} was deleted by the CP")
                else:
                    atts = vol.get("Attachments") or []
                    state = vol.get("State")
                    self.log.info(
                        f"{victim_volume} final state={state} "
                        f"attached_to={atts[0].get('InstanceId') if atts else None}")
                    self.assertNotEqual(
                        state, "available",
                        f"{victim_volume} is still sitting unattached in 'available' after "
                        f"the recovery window — a download volume detached from its "
                        f"accelerator was left dangling rather than re-attached or "
                        f"deleted")
        finally:
            self._restore_migration_rate_limit()

        try:
            self.wait_for_rebalances([rebalance_task])
        except Exception as e:
            self.log.warning(
                f"Rebalance did not complete cleanly ({e}) — continuing to the cleanup "
                f"assertions, which a clean failure still has to satisfy")
        CapellaAPI.wait_until_done(self.pod, self.tenant, self.cluster.id, timeout=1800)

        with self._stage("Chaos DD5: no orphaned infrastructure"):
            self.assertTrue(
                self._wait_for_no_fusion_infra(),
                "Accelerators, ASGs or guest volumes remain after a download volume was "
                "detached — the interrupted phase left infrastructure behind")
        with self._stage("Chaos DD6: data readable after the interrupted download"):
            self._run_read_workload("after detach during download")

        self._assert_no_stage_issues()

    # ------------------------------------------------------------------
    # B: download throttled below the log-file lease TTL -> DCP fallback
    # ------------------------------------------------------------------

    def test_download_rate_limit_expires_lease_falls_back_to_dcp(self):
        """
        Throttle the accelerator's S3 download so far below the shard size that the
        download cannot finish inside the log-file lease, and assert the rebalance falls
        back to DCP instead of completing on top of expired leases.

        Boundary B (phase 5, "Download Shards"), and the only fault in this file injected
        purely through configuration rather than AWS or CBS. The knob is
        `accelerator.download.rateLimit` on the fusion support config
        (PATCH /internal/support/configs/{clusterId}/fusion with
        {"value": {"accelerator": {"download": {"rateLimit": <bytes/sec>}}}}), reached here
        via the `fusion_download_rate_limit` param -> _apply_fusion_config_from_params.

        WHY THE LEASE IS THE POINT. `prepareRebalance` (ACCELERATION.md §Phase 2) hands back
        a PlanUUID and, with it, leases on the S3 log files the plan is built from — CBS
        holds those files still so the accelerators can download from a stable snapshot.
        The lease is finite (`fusion_lease_seconds`, one hour by default). A download that
        outruns it leaves the accelerators pulling against a plan whose leases CBS has
        released, so the plan can no longer be trusted: the CP must abandon the fusion path
        and move the data over DCP instead. Completing the rebalance "successfully" through
        the accelerators after the leases expired is the bug this test exists to catch —
        the data those shards were built from may no longer be what the log store holds.

        SIZING. The conf loads ~100 GB across 3 KV nodes and adds a 4th. Roughly a quarter
        of the data — ~25 GB — has to move, and that is what gets downloaded from S3.

        The fleet does NOT come out as one big shard, and the shards are wildly uneven. An
        observed run produced 4 shards: one holding ~26 GB and three holding tens of MB. The
        three small ones finished downloading inside a minute; the large one was still going
        40 minutes later. This shape is normal — the manifest assigns data ranges per host,
        and a scale-out touches every host to some degree.

        Two consequences the test has to respect:

        1. EBS volume size tells you nothing about shard data size. Volumes are provisioned
           at ceil(shard/1GB) + 10% but with a **50 GiB floor** (min_volume_size_gb, asserted
           by test_accelerator_ebs_volume_min_size), so the run above reported
           [50, 50, 50, 50] GiB for shards ranging from ~10 MB to ~26 GB. Stage RL2 therefore
           uses the volume size only as an UPPER BOUND, giving a one-sided check: if even the
           largest possible shard would download inside the lease, the conf is definitely
           wrong. If it would not, nothing is proven, and RL2 says so.

        2. Shards are handed off to KV nodes INDIVIDUALLY as each finishes, not all at once.

        HOW "IT TOOK THE DCP PATH" IS ESTABLISHED. Not by reading a CP status, and not by
        demanding that nothing reach a KV node — small shards legitimately complete and get
        handed over. The fusion rebalance can only finish once EVERY shard is in place, so
        one shard the throttle keeps from finishing is enough to force the whole plan to be
        abandoned. Stage RL4 is that assertion: **at least `min_stuck_shards` (default 1)
        guest volumes must never reach a KV node.**

        Stage RL5 then cross-checks CBS's own view while migration is still frozen:
        `/fusion/activeGuestVolumes` must report no more volumes than actually completed
        their download. A volume CBS believes it can serve from, whose shard never finished,
        would mean an incomplete download was presented as usable storage.

        This is why migration is frozen (`fusion_migration_rate_limit=0`) here as everywhere
        else in this file. A transferred volume would otherwise be drained and deleted per
        shard as its migration completed — quite possibly between two polls of the watch loop
        — so the handoff tally RL4 rests on would undercount, and RL5's count would be gone
        entirely (observed: 4 -> 2 -> 0 within 110s of resuming migration). The freeze keeps
        those volumes attached and countable. It does not defeat CP teardown, which deletes
        guest volumes on the fallback path regardless; the evidence that matters is what the
        watch loop accumulated while the rebalance was live.

        Accepted outcomes at Stage RL3, all recorded:
          * the accelerator fleet is torn down while the rebalance is still running, and
            the rebalance then completes — the DCP fallback this test is written for
          * the rebalance fails cleanly, leaving the cluster recoverable
        Not accepted:
          * every shard finished downloading and was handed to a KV node (Stage RL4)
          * CBS claims more active guest volumes than completed downloads (Stage RL5)
          * nothing happens at all — the CP sits on a download that cannot finish inside
            the lease and the rebalance hangs past `lease_expiry_recovery_timeout`

        This test is long by construction — it has to outlast a one-hour lease. Budget
        `rebalance_timeout` and `lease_expiry_recovery_timeout` accordingly.
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")
        self._apply_fusion_config_from_params()
        rate_limit = self._fusion_download_rate_limit
        self.assertTrue(
            rate_limit,
            "This test needs fusion_download_rate_limit set (bytes/sec) — without a "
            "throttle the download finishes in minutes, the lease never expires, and "
            "there is no fallback to observe. See the conf entry for a value sized to "
            "the data set.")
        self._apply_fusion_sync_threshold()
        s3_bucket_name, _ = self._capture_s3_log_store_baseline()
        self._load_above_threshold()

        lease_secs = int(self.input.param("fusion_lease_seconds", 3600))
        recovery_timeout = int(self.input.param(
            "lease_expiry_recovery_timeout", lease_secs + 1800))
        self.log.info(
            f"Download throttled to {rate_limit} B/s "
            f"({rate_limit / (1024 ** 2):.2f} MiB/s) against a {lease_secs}s log-file "
            f"lease; allowing {recovery_timeout}s after the download starts for the CP to "
            f"react")

        if not self._pause_migration():
            self.fail(
                "Could not freeze background migration — the central assertion of this "
                "test is that NO guest volume is ever attached to a KV node, and without "
                "the freeze a volume that did get transferred would be deleted per shard "
                "as its migration completed, quite possibly between two polls. A missed "
                "attachment would read as a clean DCP fallback when the fusion path had "
                "in fact run.")

        # Initialised up front so a soft-failed RL1 leaves the later stages with empty
        # inputs to report on rather than a NameError (see _stage).
        accel_ids_before = set()
        volumes = []
        download_start = time.time()

        try:
            rebalance_task = self._trigger_rebalance("in")
            rebalance_start = time.time()
            self.sleep(30, "Wait for the rebalance to start")

            # ---- the fusion path was taken, and the download has started --------------
            with self._stage("Chaos RL1: accelerators launched and downloading"):
                instances = self._poll_until_accelerators_appear(rebalance_task)
                self.assertGreater(
                    len(instances), 0,
                    "No accelerators launched — the rebalance took the DCP path from the "
                    "start, so the throttle was never exercised and this test proves "
                    "nothing about lease expiry. Check fusion_threshold_gib against the "
                    "data loaded.")
                # _poll_until_accelerators_appear returns on the FIRST accelerator and
                # filters by 16000 IOPS, so it reports a fraction of the fleet (observed:
                # 1 of 4). Re-read by tag to get every accelerator actually running, or
                # RL3 starts out misclassifying the ones it never saw as KV nodes.
                accel_ids_before = {i.get("InstanceId") for i in
                                   self._list_accelerator_instances_by_tag(
                                       log="AcceleratorFleet", suppress_log=False)}
                accel_ids_before |= {i["InstanceId"] for i in instances}
                volumes = self._poll_until_accelerator_volumes_appear(
                    rebalance_task, timeout=self.input.param("gv_launch_timeout", 1200))
                self.assertTrue(
                    volumes,
                    "Accelerators launched but no guest volume was created — the download "
                    "target does not exist, so nothing is being throttled")
                self.log.info(
                    f"{len(accel_ids_before)} accelerator(s) {sorted(accel_ids_before)} "
                    f"with {len(volumes)} download volume(s)")
            download_start = time.time()

            # ---- could the download conceivably beat the lease? -----------------------
            with self._stage("Chaos RL2: the throttle is not obviously too generous"):
                # EBS volume size is NOT a measure of shard data. Volumes are provisioned
                # at ceil(shard/1GB) + 10% but with a 50 GiB FLOOR (see
                # test_accelerator_ebs_volume_min_size / min_volume_size_gb), so a shard
                # holding 10 MB and one holding 26 GB both come back as size=50GiB —
                # observed exactly that: 4 shards, all reported [50, 50, 50, 50] GiB while
                # three of them finished downloading inside a minute.
                #
                # So the only sound use of this number is as an UPPER BOUND on the largest
                # shard, giving a one-sided check: if even the upper bound downloads inside
                # the lease, the conf is definitely wrong. If it does not, nothing is
                # proven — the real verdict is RL4, which is empirical.
                sizes_gib = [v.get("Size") for v in volumes if v.get("Size")]
                self.assertTrue(
                    sizes_gib,
                    f"AWS reported no size for any guest volume ({volumes}) — even the "
                    f"upper-bound check this stage makes cannot be computed")
                upper_bound_bytes = max(sizes_gib) * (1024 ** 3)
                upper_bound_secs = upper_bound_bytes / rate_limit
                self.log.info(
                    f"{len(sizes_gib)} shard(s) across the fleet, guest volume sizes "
                    f"{sorted(sizes_gib)} GiB (50 GiB is the provisioning floor, so these "
                    f"are upper bounds, not shard data sizes). Largest possible shard "
                    f"{upper_bound_bytes / (1024 ** 3):.1f} GiB at {rate_limit} B/s => at "
                    f"most ~{upper_bound_secs / 3600:.2f}h, against a "
                    f"{lease_secs / 3600:.2f}h lease. Whether any shard actually outlasts "
                    f"the lease is decided empirically at RL4.")
                self.assertGreater(
                    upper_bound_secs, lease_secs,
                    f"Even the LARGEST possible shard ({upper_bound_bytes / (1024 ** 3):.1f} "
                    f"GiB, the full provisioned volume) downloads in "
                    f"{upper_bound_secs / 3600:.2f}h at {rate_limit} B/s, inside the "
                    f"{lease_secs / 3600:.2f}h lease. Every shard is therefore guaranteed "
                    f"to finish in time, no lease can expire, and no fallback can be "
                    f"observed. Lower fusion_download_rate_limit to below "
                    f"{int(upper_bound_bytes / lease_secs)} B/s.")

            # ---- what does the CP do once the lease is gone? --------------------------
            # Two inventories drive the verdict at RL4, both accumulated across the whole
            # watch rather than sampled once:
            #   all_volumes       — every guest volume this plan ever produced
            #   transferred       — {volume_id: (kv_instance, elapsed)} for the ones that
            #                       finished downloading and were handed to a KV node
            # Migration is frozen, so a transferred volume stays attached to be seen.
            all_volumes = {v.get("VolumeId") for v in volumes if v.get("VolumeId")}
            transferred = dict()
            outcome = None
            with self._stage("Chaos RL3: fusion is abandoned once the lease expires"):
                deadline = download_start + recovery_timeout
                # _cluster_instance_ids() matches on the cluster-id tag, which accelerators
                # carry too, so every accelerator ever seen has to come back out of it — an
                # accelerator holding its own download volume is not a transfer to a KV
                # node.
                accel_seen = set(accel_ids_before)
                while time.time() < deadline:
                    elapsed = time.time() - download_start
                    if rebalance_task.state in self._FAILED_STATES:
                        outcome = (f"clean failure after {elapsed:.0f}s "
                                   f"({rebalance_task.state})")
                        break

                    accel_now = {i.get("InstanceId") for i in
                                 self._list_accelerator_instances_by_tag(log="LeaseWatch")}
                    accel_seen |= accel_now
                    kv_instances = self._cluster_instance_ids() - accel_seen
                    placement = self._guest_volumes_by_instance()
                    for inst, vols in placement.items():
                        all_volumes.update(v for v in vols if v)
                        if inst not in kv_instances:
                            continue
                        for vol in vols:
                            if vol in transferred:
                                continue
                            transferred[vol] = (inst, elapsed)
                            self.log.warning(
                                f"Guest volume {vol} is attached to KV node {inst} at "
                                f"{elapsed:.0f}s — that shard finished downloading and "
                                f"was handed off")

                    if not accel_now and rebalance_task.state != "healthy":
                        outcome = (f"accelerator fleet torn down after {elapsed:.0f}s with "
                                   f"the rebalance still running — the CP abandoned the "
                                   f"fusion path")
                        break
                    if rebalance_task.state == "healthy":
                        outcome = (f"rebalance completed after {elapsed:.0f}s "
                                   f"(accelerators still visible: {sorted(accel_now)})")
                        break

                    self.log.info(
                        f"[lease-watch] elapsed={elapsed:.0f}s of {recovery_timeout}s "
                        f"(lease={lease_secs}s, {time.time() - rebalance_start:.0f}s since "
                        f"the rebalance was triggered) accelerators={len(accel_now)} "
                        f"shards={len(all_volumes)} handed_off={len(transferred)} "
                        f"task_state={rebalance_task.state}")
                    if self._budget_exhausted("Chaos RL3 lease watch"):
                        break
                    time.sleep(60)

                if outcome is None:
                    # The CP never reacted. Record it for every stage below so none of them
                    # blocks on a rebalance that is never going to end — waiting out the
                    # default 8h monitor plus a 2h settle plus a 20min orphan check turned a
                    # 2h finding into a 31h run, and the diagnosis was already complete here.
                    self._hung = (
                        f"the CP did not react within {recovery_timeout}s of the download "
                        f"starting, {lease_secs}s lease included")
                self.assertIsNotNone(
                    outcome,
                    f"Nothing happened within {recovery_timeout}s of the download "
                    f"starting: the accelerators are still up, the rebalance is neither "
                    f"healthy nor failed, and the {lease_secs}s lease has long since "
                    f"passed. The CP is waiting on a download that cannot finish inside "
                    f"its lease — the hang this test exists to catch.")
                self.log.info(f"Lease-expiry outcome: {outcome}")
                self._record_issue(
                    "Chaos RL3: lease-expiry outcome (informational)",
                    outcome, severity="INFO")

            # ---- the verdict: at least one shard never made it ------------------------
            stuck = set()
            with self._stage("Chaos RL4: at least one shard never reached a KV node"):
                # Shards are handed off INDIVIDUALLY as each one finishes downloading —
                # observed directly: of 4 shards, 3 completed and moved to KV nodes within
                # ~10 minutes while the 4th was still downloading 40 minutes later. Shard
                # data sizes are wildly uneven (one held ~26 GB, the others tens of MB), and
                # the 50 GiB volume floor hides that completely.
                #
                # So "no volume ever reached a KV node" is the wrong invariant — small
                # shards legitimately complete. What must hold is that the throttle keeps at
                # least one shard from finishing, because the fusion rebalance can only
                # complete when ALL shards are in place. One stuck shard is enough to force
                # the plan to be abandoned, which is the fallback this test is about.
                stuck = all_volumes - set(transferred)
                min_stuck = int(self.input.param("min_stuck_shards", 1))
                self.log.info(
                    f"Shard outcome: {len(all_volumes)} total, {len(transferred)} handed "
                    f"off to KV nodes, {len(stuck)} still stuck on their accelerators.\n"
                    + "\n".join(
                        f"    handed off: {vol} -> {inst} at {at:.0f}s"
                        for vol, (inst, at) in sorted(transferred.items()))
                    + ("\n" if transferred and stuck else "")
                    + "\n".join(f"    stuck:      {vol}" for vol in sorted(stuck)))
                self.assertGreaterEqual(
                    len(stuck), min_stuck,
                    f"Every one of the {len(all_volumes)} shard(s) finished downloading and "
                    f"was handed to a KV node "
                    + "; ".join(f"{vol} -> {inst} at {at:.0f}s"
                               for vol, (inst, at) in sorted(transferred.items()))
                    + f". At least {min_stuck} shard(s) had to remain stuck: a download "
                      f"throttled to {rate_limit} B/s is not supposed to complete inside a "
                      f"{lease_secs}s lease, so the fusion rebalance ran to completion on "
                      f"shards built from log files CBS may already have released, instead "
                      f"of being abandoned for DCP. Either the throttle is not being applied "
                      f"or the shards are far smaller than the run assumed — check the "
                      f"handoff timings above against the lease.")
                self.log.info(
                    f"{len(stuck)} shard(s) never reached a KV node, so the fusion "
                    f"rebalance could not complete and the data had to move over DCP")

            # ---- however it recovered, the data still has to move ---------------------
            if self._hung:
                self.log.error(
                    f"Skipping the wait for the rebalance to finish: {self._hung}. It is "
                    f"not going to finish, and wait_for_rebalances defaults to an 8h "
                    f"monitor timeout — waiting it out adds nothing to a diagnosis that is "
                    f"already complete.")
            else:
                try:
                    self.wait_for_rebalances(
                        [rebalance_task], timeout=self.rebalance_timeout)
                except Exception as e:
                    self.log.warning(
                        f"Rebalance did not complete cleanly ({e}) — continuing to the "
                        f"cleanup assertions, which a clean failure still has to satisfy")

            # ---- CBS's own view, while migration is still frozen ----------------------
            # Deliberately inside the try, BEFORE _restore_migration_rate_limit(): once
            # migration resumes, CBS drains and releases these volumes within a couple of
            # minutes (observed: 4 -> 2 -> 0 in 110s), and the count is gone.
            with self._stage("Chaos RL5: activeGuestVolumes counts only completed shards"):
                # The scale-out can move the orchestrator, and this reads through
                # cluster.master — refresh it or the count comes from a stale node.
                self.find_master(self.tenant, self.cluster)
                active = self._get_active_guest_volume_count(self.cluster)
                self.log.info(
                    f"/fusion/activeGuestVolumes reports {active} volume(s); "
                    f"{len(transferred)} shard(s) completed their download and "
                    f"{len(stuck)} did not")
                self.assertLessEqual(
                    active, len(transferred),
                    f"CBS reports {active} active guest volume(s) but only "
                    f"{len(transferred)} shard(s) ever finished downloading and were handed "
                    f"over. A volume CBS believes it can serve from, whose shard never "
                    f"completed, means an incomplete download was presented as usable "
                    f"storage — the shard's data would be short. Stuck shard(s): "
                    f"{sorted(stuck)}")
                if active < len(transferred):
                    self._record_issue(
                        "Chaos RL5: CBS released some completed volumes early",
                        f"activeGuestVolumes reports {active}, fewer than the "
                        f"{len(transferred)} handed over. Expected with migration frozen "
                        f"only if the CP already told CBS to release them while abandoning "
                        f"the plan, which is a legitimate part of the fallback.",
                        severity="INFO")
        finally:
            self._restore_migration_rate_limit()

        # A hung rebalance never settles, so the post-fallback checks below are not
        # applicable — they would assert on the aftermath of a fallback that never happened,
        # report misleading failures ("the plan was dropped but the infrastructure was left
        # running" — it was not dropped), and cost hours doing it. Collapse them to a short
        # snapshot instead.
        settle_timeout = int(self.input.param("post_fallback_settle_timeout", 3600))
        if self._hung:
            settle_timeout = int(self.input.param("hung_settle_timeout", 120))
            self.log.error(
                f"Rebalance is hung ({self._hung}); capping the settle wait at "
                f"{settle_timeout}s and reporting the state as-is rather than asserting on "
                f"a fallback that never happened")
        CapellaAPI.wait_until_done(
            self.pod, self.tenant, self.cluster.id, timeout=settle_timeout)

        if self._hung:
            with self._stage("Chaos RL6: state snapshot of the hung rebalance"):
                state = CapellaAPI.get_cluster_state(
                    self.pod, self.tenant, self.cluster.id)
                accel = self._list_accelerator_instances_by_tag(log="HungSnapshot")
                asgs = self.fusion_aws_util.list_cluster_fusion_asg(self.cluster.id)
                vols = self._list_accelerator_volumes(guest_only=True)
                self.log.error(
                    f"[hung] cluster_state={state} task_state={rebalance_task.state} "
                    f"accelerators={len(accel)} asgs={len(asgs)} guest_volumes={len(vols)} "
                    f"shards={len(all_volumes)} handed_off={len(transferred)}")
                self._record_issue(
                    "Chaos RL6: rebalance hung — infrastructure left in place",
                    f"cluster is '{state}', {len(accel)} accelerator(s), {len(asgs)} ASG(s) "
                    f"and {len(vols)} guest volume(s) still present, {len(transferred)} of "
                    f"{len(all_volumes)} shard(s) handed off. RL7/RL8 are skipped: the "
                    f"fallback never happened, so there is no post-fallback state to check. "
                    f"The cluster is left mid-rebalance and needs manual attention.")
        else:
            with self._stage("Chaos RL6: the cluster is healthy after the fallback"):
                state = CapellaAPI.get_cluster_state(
                    self.pod, self.tenant, self.cluster.id)
                self.assertEqual(
                    state.lower(), "healthy",
                    f"Cluster is not healthy after the throttled download was abandoned: "
                    f"{state}")
                fusion_state = CapellaAPI.get_fusion_status(
                    self.pod, self.tenant, self.cluster.id).get("state")
                self.assertEqual(
                    fusion_state, "enabled",
                    f"Fusion is not 'enabled' after the fallback: {fusion_state} — falling "
                    f"back to DCP for one rebalance must not disable fusion on the cluster")

            with self._stage("Chaos RL7: no fusion infrastructure survives the fallback"):
                self.assertTrue(
                    self._wait_for_no_fusion_infra(),
                    "Accelerators, ASGs or guest volumes are still present after the "
                    "throttled download was abandoned — the plan was dropped but the "
                    "infrastructure it had deployed was left running")

            with self._stage("Chaos RL8: the S3 log store survived and data is readable"):
                if s3_bucket_name:
                    objects = self._get_s3_object_count_for_buckets(
                        s3_bucket_name, self.cluster.buckets)
                    self.log.info(
                        f"S3 log store '{s3_bucket_name}' holds {objects} object(s) after "
                        f"the fallback")
                    self.assertGreater(
                        objects, 0,
                        f"S3 log store '{s3_bucket_name}' is empty after the fallback — "
                        f"abandoning a fusion plan must release the log file leases, not "
                        f"delete the log store")
                self._run_read_workload("after the DCP fallback")

        self._assert_no_stage_issues()
