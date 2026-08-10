"""
Fusion Accelerator Lifecycle Tests

Validates the lifecycle of fusion accelerator instances and EBS guest volumes during
Couchbase rebalances on AWS-backed Capella clusters (§2 and §7 of the E2E test plan).

Every test drives its topology change through _trigger_rebalance(), which honours the
`rebalance_type` param: in (default, scale-out) / out (scale-in) / swap (same node count,
different compute). Fusion acceleration applies to any topology change, so every
assertion here holds for all three. `rebalance_type=out` needs kv_nodes high enough to
stay >= 3 after removal.

Numbering gaps: test 5 (fusion state stays 'enabled') now runs as a background watcher
for the whole of test 19 via _start_fusion_state_watcher(), with the point-in-time check
still in _validate_teardown; test 12 (instance type validation) runs inside
test_accelerator_deployment; test 15 (CloudWatch download progress) was removed, so the
accelerator download phase is not validated from TAF. Test 6 stays separate because it is
the only test needing two rebalances, and test 20 stays separate from test 4 because it
asserts the opposite invariant.

Tests 13-18 each drive their own rebalance so a failure isolates to one stage; test 19
walks the whole lifecycle through one rebalance and so also catches ordering/handoff
defects the per-stage tests cannot see. Both call the same _validate_* stage validators,
so they cannot drift apart. Stage E is the exception: test 17 uses the detailed
_validate_background_migration (cbstats, REST API, failure counters), test 19 the simpler
_validate_guest_volume_drain (count falls to 0, main-volume du rises).
"""

import contextlib
import threading
import time

from capella_utils.dedicated import CapellaUtils as CapellaAPI
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from constants.cloud_constants.capella_constants import AWS, AZURE, GCP
from couchbase_utils.cb_server_rest_util.fusion.fusion_api import FusionRestAPI
from membase.api.rest_client import RestConnection
from .fusion_test_base import _FusionTestBase


class FusionAcceleratorLifecycleTest(_FusionTestBase):
    """
    Tests for fusion accelerator instance and EBS guest volume lifecycle.

    Each test calls _ensure_fusion_state() to reach its required starting state.
    setUp creates a fresh bucket; tearDown removes it and resets the node count.
    """

    _FAILED_STATES = frozenset([
        "deployment_failed", "deploymentFailed", "redeploymentFailed",
        "rebalance_failed", "rebalanceFailed", "scaleFailed",
    ])

    # Capella dedicated requires at least this many KV nodes; a rebalance-out
    # must not drop the cluster below it.
    _MIN_KV_NODES = 3

    def setUp(self):
        super().setUp()
        self.log.info(f"[setUp] cluster={self.cluster.id}")
        # Record stage failures and keep going, so one rebalance surfaces every problem;
        # all issues are re-raised as a single failure at the end. False = fail-fast.
        self.soft_fail_stages = self.input.param("soft_fail_stages", True)
        self.stage_issues = []
        # Fusion support-config override state (see _apply_fusion_config_from_params).
        self._fusion_config_modified = False
        self._prior_fusion_config = None
        self._fusion_min_split_size_gb = None
        self._fusion_max_slots = None
        self._fusion_download_rate_limit = None
        self._cached_data_path = None
        # Fusion state watcher (see _start_fusion_state_watcher).
        self._fusion_state_thread = None
        self._fusion_state_stop = None
        self._fusion_state_expected = "enabled"
        self._fusion_state_violations = []
        self._fusion_state_reads = 0
        self._fusion_state_read_failures = 0
        # Fusion sync threshold state (see _apply_fusion_sync_threshold).
        self._sync_threshold_modified = False
        self._prior_sync_threshold_mb = None
        # Background migration freeze state (see _pause_migration).
        self._migration_paused = False
        self._prior_migration_rate_limit = None
        # Guest-volume IOPS scale-down watch handed from Stage C to Stage E.
        self._iops_watch = {}
        self._iops_settled_unscaled = {}
        self._iops_scaled_count = 0
        self._iops_total_count = 0
        CapellaAPI.wait_until_done(
            self.pod, self.tenant, self.cluster.id,
            "Wait for healthy cluster state before bucket cleanup", timeout=600)
        self.initial_kv_nodes = self.input.param("kv_nodes", 3)
        # For tearDown to restore after a swap rebalance, which mutates compute["data"].
        self.initial_kv_compute = self.compute["data"]
        for bucket in self.cluster.buckets:
            try:
                self._delete_bucket_with_s3_cleanup(bucket)
            except Exception:
                pass
        self.cluster.buckets = []
        self.create_buckets(self.pod, self.tenant, self.cluster)
        if self.cluster.buckets:
            bucket = self.cluster.buckets[0]
            rest = RestConnection(self.cluster.master)
            info = rest.get_bucket_details(bucket_name=bucket.name)
            bucket.bucket_uuid = info.get("uuid", None)

    def tearDown(self):
        # Stop the watcher before logging the summary so a violation it saw is included.
        # The summary is re-logged here because a body that raised before
        # _assert_no_stage_issues() would otherwise lose every earlier soft-failure.
        try:
            self._stop_fusion_state_watcher()
        except Exception as e:
            self.log.warning(f"Could not stop the fusion state watcher: {e}")
        try:
            self._log_stage_issue_summary()
        except Exception as e:
            self.log.warning(f"Could not log stage issue summary: {e}")
        # A leftover freeze or minSplitSize would silently change every later test.
        self._restore_migration_rate_limit()
        self._restore_fusion_sync_threshold()
        self._restore_fusion_config()
        # Restore the original topology on the shared cluster. A swap changes compute
        # without changing the count, so check both; one rebalance_config call fixes both.
        needs_restore = (self.num_nodes["data"] != self.initial_kv_nodes) or \
            (self.compute["data"] != self.initial_kv_compute)
        if needs_restore:
            delta = self.initial_kv_nodes - self.num_nodes["data"]
            self.compute["data"] = self.initial_kv_compute
            reset_timeout = int(self.input.param("topology_reset_timeout", 1800))
            try:
                CapellaAPI.wait_until_done(
                    self.pod, self.tenant, self.cluster.id,
                    "Wait for healthy state before topology reset",
                    timeout=reset_timeout)
                # wait_until_done returns on timeout rather than raising, so the state has
                # to be checked explicitly. A cluster still stuck mid-rebalance rejects
                # every spec update, and attempting the reset anyway is what turns a failed
                # test into a multi-day run (see CapellaUtils.scale).
                state = CapellaAPI.get_cluster_state(
                    self.pod, self.tenant, self.cluster.id)
                if state.lower() != "healthy":
                    self.log.error(
                        f"Skipping the topology reset: cluster {self.cluster.id} is "
                        f"'{state}', not healthy, after waiting {reset_timeout}s. It will "
                        f"not accept a spec update, so the reset is abandoned rather than "
                        f"retried. The cluster is left at {self.num_nodes['data']} data "
                        f"node(s) / {self.compute['data']} and needs manual attention "
                        f"before reuse.")
                else:
                    self.wait_for_rebalances([self.task.async_rebalance_capella(
                        self.pod, self.tenant, self.cluster,
                        self.rebalance_config("data", delta),
                        timeout=self.rebalance_timeout)],
                        timeout=self.rebalance_timeout)
            except Exception as e:
                self.log.error(
                    f"Failed to reset topology to {self.initial_kv_nodes} nodes / "
                    f"{self.initial_kv_compute} compute: {e}")
        for bucket in list(self.cluster.buckets):
            try:
                self._delete_bucket_with_s3_cleanup(bucket)
            except Exception:
                pass
        self.cluster.buckets = []
        super().tearDown()

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _accelerator_filter(self):
        """Return EC2 filters scoped to fusion accelerators on this cluster."""
        return self.fusion_aws_util._cluster_filter(self.cluster.id)

    # ------------------------------------------------------------------
    # Observability: query logging + soft-fail issue collection
    # ------------------------------------------------------------------

    def _log_query(self, label, filters, results):
        """Log an AWS query verbatim: the exact filter alongside its result.

        Every instance/volume/ASG lookup goes through here, so a lookup that silently
        returns the wrong population (an IOPS-filtered subset, a tag key that does not
        exist) can be told apart from "the resource isn't there".
        """
        self.log.info(f"[query] {label}\n    filters={filters}\n    -> {results}")

    @staticmethod
    def _describe_volumes(volumes):
        """One-line-per-volume description for query logs."""
        if not volumes:
            return "0 volume(s)"
        rows = []
        for v in volumes:
            atts = v.get("Attachments") or []
            rows.append(
                f"{v.get('VolumeId')} size={v.get('Size')}GiB iops={v.get('Iops')} "
                f"state={v.get('State')} "
                f"attached_to={atts[0].get('InstanceId') if atts else None} "
                f"az={v.get('AvailabilityZone')}")
        return f"{len(volumes)} volume(s):\n      " + "\n      ".join(rows)

    @staticmethod
    def _describe_instances(instances):
        """One-line-per-instance description for query logs."""
        if not instances:
            return "0 instance(s)"
        rows = [f"{i.get('InstanceId')} type={i.get('InstanceType')} "
                f"public_ip={i.get('PublicIpAddress')}" for i in instances]
        return f"{len(instances)} instance(s):\n      " + "\n      ".join(rows)

    def _log_active_guest_volumes(self, label):
        """Log the guest-volume REST API response verbatim; return (count, content).

        The count alone hides which node still holds volumes, and hides ns_server
        reporting 0 while AWS still shows attached volumes. Returns (None, None) on
        failure — callers treat this as informational, not an assertion.
        """
        try:
            self.fusion_monitor.set_admin_credentials(self.cluster)
            status, content = FusionRestAPI(
                self.cluster.master).get_active_guest_volumes()
            if not status:
                self.log.warning(
                    f"[{label}] get_active_guest_volumes returned status={status}: "
                    f"{content}")
                return None, content
            if isinstance(content, dict):
                count = sum(len(v or []) for v in content.values())
                per_node = ", ".join(
                    f"{node}={len(vols or [])}" for node, vols in content.items())
            else:
                count = len(content or [])
                per_node = "n/a (non-dict response)"
            self.log.info(
                f"[{label}] guest-volume API: count={count} | per node: {per_node}\n"
                f"    raw={content}")
            return count, content
        except Exception as e:
            self.log.warning(f"[{label}] guest-volume API read failed: {e}")
            return None, None

    def _start_fusion_state_watcher(self, expected="enabled", poll_interval=5):
        """Poll fusion state in a background thread for as long as the test runs.

        Fusion must never leave `expected` while a rebalance, a migration freeze/resume or
        a teardown is in flight, and a single check before or after cannot show that.
        Findings are reported by _stop_fusion_state_watcher().

        Polling continues after a violation (so the log shows whether the state
        recovered), and REST failures are counted rather than killing the thread — a
        watcher that died on one API blip, or never read successfully at all, would
        otherwise look like a clean pass.
        """
        self._fusion_state_stop = threading.Event()
        self._fusion_state_expected = expected
        self._fusion_state_violations = []
        self._fusion_state_reads = 0
        self._fusion_state_read_failures = 0

        def _poll():
            while not self._fusion_state_stop.is_set():
                try:
                    status = CapellaAPI.get_fusion_status(
                        self.pod, self.tenant, self.cluster.id)
                    self._fusion_state_reads += 1
                    current = (status or {}).get("state", "unknown")
                    if current != expected:
                        self._fusion_state_violations.append(current)
                        self.log.error(
                            f"[fusion-state] observed '{current}', expected "
                            f"'{expected}' (violation "
                            f"#{len(self._fusion_state_violations)})")
                except Exception as e:
                    self._fusion_state_read_failures += 1
                    self.log.warning(f"[fusion-state] read failed: {e}")
                self._fusion_state_stop.wait(poll_interval)

        self._fusion_state_thread = threading.Thread(
            target=_poll, daemon=True, name="fusion-state-watcher")
        self._fusion_state_thread.start()
        self.log.info(
            f"Fusion state watcher started — expecting '{expected}' every "
            f"{poll_interval}s for the rest of the test")

    def _stop_fusion_state_watcher(self):
        """Stop the watcher and record any departure from the expected fusion state.

        Idempotent, so tearDown can call it as a safety net.
        """
        thread = getattr(self, "_fusion_state_thread", None)
        if thread is None:
            return
        self._fusion_state_stop.set()
        thread.join(timeout=30)
        self._fusion_state_thread = None

        expected = self._fusion_state_expected
        violations = self._fusion_state_violations
        reads = self._fusion_state_reads
        failures = self._fusion_state_read_failures

        if violations:
            distinct = sorted(set(violations))
            message = (
                f"Fusion state left '{expected}' {len(violations)} time(s) out of "
                f"{reads} successful read(s); observed state(s): {distinct}")
            if not self.soft_fail_stages:
                self.fail(message)
            self._record_issue("Fusion state watcher", message)
        elif reads == 0:
            message = (
                f"Fusion state was never read successfully ({failures} failure(s)), so "
                f"it was not actually monitored during this test")
            if not self.soft_fail_stages:
                self.fail(message)
            self._record_issue("Fusion state watcher", message)
        else:
            self.log.info(
                f"Fusion state stayed '{expected}' across {reads} read(s)"
                + (f" ({failures} read failure(s))" if failures else ""))

    def _record_issue(self, stage, message, severity="ERROR"):
        """Record a stage problem and keep going (see _assert_no_stage_issues)."""
        self.stage_issues.append((stage, severity, message))
        log_fn = self.log.error if severity == "ERROR" else self.log.warning
        log_fn(f"[{severity}] {stage}: {message}")

    @contextlib.contextmanager
    def _stage(self, name):
        """Run one lifecycle stage, recording rather than raising on failure.

        In soft-fail mode any exception inside the stage is logged and collected and the
        test moves on, so one rebalance yields observations for every stage. With
        soft_fail_stages=False it propagates as usual.

        Callers must cope with a failed stage leaving its outputs unset: the end-to-end
        test initialises those to empty and records a skip for stages that cannot run.
        """
        self.log.info(f"########## {name}: START ##########")
        start = time.time()
        try:
            yield
        except AssertionError as e:
            if not self.soft_fail_stages:
                raise
            self._record_issue(name, str(e))
            self.log.error(f"########## {name}: FAILED after "
                           f"{time.time() - start:.0f}s (continuing) ##########")
        except Exception as e:
            if not self.soft_fail_stages:
                raise
            self._record_issue(name, f"unexpected {type(e).__name__}: {e}")
            self.log.error(f"########## {name}: ERRORED after "
                           f"{time.time() - start:.0f}s (continuing) ##########")
        else:
            self.log.info(f"########## {name}: PASSED in "
                          f"{time.time() - start:.0f}s ##########")

    def _log_stage_issue_summary(self):
        """Log the collected issues as a numbered summary (no assertion)."""
        if not self.stage_issues:
            self.log.info("Stage issue summary: none — all stages passed")
            return
        lines = [f"  {n}. [{sev}] {stage}: {msg}"
                 for n, (stage, sev, msg) in enumerate(self.stage_issues, 1)]
        self.log.error(
            f"Stage issue summary — {len(self.stage_issues)} issue(s):\n"
            + "\n".join(lines))

    def _assert_no_stage_issues(self):
        """Fail with every ERROR-severity issue collected; pass if there were none.

        WARNING/INFO entries appear in the summary but do not fail, so a stage can record
        an observation without turning it into a verdict.
        """
        self._log_stage_issue_summary()
        failures = [(stage, sev, msg) for stage, sev, msg in self.stage_issues
                    if sev == "ERROR"]
        noted = len(self.stage_issues) - len(failures)
        if failures:
            lines = [f"{n}. [{sev}] {stage}: {msg}"
                     for n, (stage, sev, msg) in enumerate(failures, 1)]
            self.fail(
                f"{len(failures)} stage(s) reported problems during this fusion "
                f"rebalance"
                + (f" ({noted} further non-fatal note(s) in the summary above)"
                   if noted else "")
                + ":\n" + "\n".join(lines))

    def _log_cluster_data_summary(self, label):
        """Log what is actually in the cluster at a checkpoint.

        The views that matter for sizing a fusion run: cluster stats, per-bucket ns_server
        stats, per-bucket fusion log-store size, main-volume disk usage, S3 log-store
        object count. Never asserts; each lookup is wrapped so a transient SSM/REST/S3
        failure degrades the summary instead of the test.
        """
        self.log.info(f"===== Cluster data summary [{label}] =====")
        try:
            self.cluster_util.print_cluster_stats(self.cluster)
        except Exception as e:
            self.log.warning(f"[{label}] print_cluster_stats failed: {e}")

        # One REST call for items/RAM/disk/resident ratio, where the equivalent cbstats
        # would be an SSM round trip per node — so curr_items is not read over SSM below.
        try:
            self.bucket_util.print_bucket_stats(self.cluster)
        except Exception as e:
            self.log.warning(f"[{label}] print_bucket_stats failed: {e}")

        for bucket in self.cluster.buckets:
            for stat in ("ep_fusion_log_store_data_size",
                         "ep_fusion_log_store_garbage_size"):
                try:
                    total = self._sum_cbstat_across_nodes(stat, bucket=bucket)
                    self.log.info(
                        f"[{label}] {bucket.name}: {stat} = "
                        f"{total / (1024 ** 3):.2f} GiB ({total} bytes)")
                except Exception as e:
                    self.log.warning(f"[{label}] cbstat {stat} on {bucket.name} "
                                     f"failed: {e}")
        try:
            usage = self.cp_monitor.get_main_volume_disk_usage_percent(self.cluster)
            self.log.info(f"[{label}] main volume disk usage per node: {usage} "
                          f"(avg {self._avg_main_volume_usage():.1f}%)")
        except Exception as e:
            self.log.warning(f"[{label}] main volume disk usage failed: {e}")

        try:
            s3_bucket_name = self._get_s3_bucket_name_from_uri(self.cluster)
            if s3_bucket_name:
                count = self._get_s3_object_count_for_buckets(
                    s3_bucket_name, self.cluster.buckets)
                self.log.info(f"[{label}] S3 log store '{s3_bucket_name}': "
                              f"{count} object(s)")
        except Exception as e:
            self.log.warning(f"[{label}] S3 log store count failed: {e}")

        # Accelerator/guest-volume state at this checkpoint (expected empty pre-rebalance).
        try:
            self._log_query(
                f"{label}: accelerator instances (by tag)",
                self.fusion_aws_util._cluster_filter(
                    self.cluster.id,
                    [{"Name": "tag:couchbase-cloud-function",
                      "Values": ["fusion-accelerator"]}]),
                self._describe_instances(self._list_accelerator_instances_by_tag()))
            self._log_query(
                f"{label}: guest volumes",
                self._accelerator_volume_filters(guest_only=True),
                self._describe_volumes(self._list_accelerator_volumes()))
        except Exception as e:
            self.log.warning(f"[{label}] accelerator/guest-volume snapshot failed: {e}")
        self.log.info(f"===== end cluster data summary [{label}] =====")

    def _sum_cbstat_across_nodes(self, stat_key, bucket=None):
        """Sum a cbstat across nodes (via SSM), for one bucket or all buckets.

        Logs how many nodes reported, so a total of 0 can be told apart from "no node
        answered".
        """
        buckets = [bucket] if bucket else list(self.cluster.buckets)
        total, ok_nodes, bad_nodes = 0, 0, 0
        for bkt in buckets:
            rows = self.fusion_monitor.run_cbstats_on_all_nodes(
                self.cluster, bkt, stat_key=stat_key)
            for instance_id, _public_ip, value, status in (rows or []):
                if status != "Success":
                    bad_nodes += 1
                    self.log.warning(
                        f"[cbstat] {stat_key} on {bkt.name}@{instance_id}: "
                        f"status={status}")
                    continue
                try:
                    total += int(value)
                    ok_nodes += 1
                except (ValueError, TypeError):
                    bad_nodes += 1
                    self.log.warning(
                        f"[cbstat] {stat_key} on {bkt.name}@{instance_id}: "
                        f"unparseable value {value!r}")
        self.log.info(
            f"[cbstat] {stat_key} total={total} across {ok_nodes} node/bucket "
            f"reading(s)" + (f", {bad_nodes} failed" if bad_nodes else ""))
        return total

    def _sum_migration_stat(self, stat_key):
        """Sum an ep_fusion_migration_* cbstat across all buckets and nodes (via SSM)."""
        return self._sum_cbstat_across_nodes(stat_key)

    def _data_path(self):
        """Server data path for this cluster, resolved once and cached.

        On Capella dedicated this is /var/cb/data, not the on-prem
        /opt/couchbase/var/lib/couchbase. Resolved from ns_server rather than assumed: df
        against the wrong path measures an unrelated filesystem.
        """
        if getattr(self, "_cached_data_path", None) is None:
            self._cached_data_path = self.cp_monitor.resolve_data_path(self.cluster)
        return self._cached_data_path

    def _avg_main_volume_usage(self):
        """Average main persistent-volume disk-usage percent across cluster nodes (SSM df)."""
        usage = self.cp_monitor.get_main_volume_disk_usage_percent(
            self.cluster, data_path=self._data_path())
        vals = [v for v in usage.values() if isinstance(v, int)]
        return (sum(vals) / len(vals)) if vals else 0.0

    # ------------------------------------------------------------------
    # Fusion sync threshold (settings/fusion :: enableSyncThresholdMB)
    # ------------------------------------------------------------------

    def _get_fusion_settings(self):
        """Read /settings/fusion. Returns the parsed body (logStoreURI + threshold)."""
        self.fusion_monitor.set_admin_credentials(self.cluster)
        status, content = FusionRestAPI(
            self.cluster.master).manage_fusion_settings()
        if not status or not isinstance(content, dict):
            raise Exception(f"GET /settings/fusion failed: {content}")
        return content

    def _apply_fusion_sync_threshold(self):
        """Set enableSyncThresholdMB from `fusion_threshold_gib`; return the MB applied.

        This is the knob that decides fusion vs DCP for a rebalance. Leave it unset and
        the run inherits the cluster's own threshold (observed: 102400 MB = 100 GB), so
        anything smaller silently takes the DCP path and launches no accelerators — which
        surfaces as "no accelerators appeared" three stages later.

        The endpoint POSTs the whole document, so logStoreURI is read first and sent back
        unchanged; omitting it would blank the log store. The prior threshold is captured
        for tearDown, since this suite shares a long-lived cluster.
        """
        threshold_gib = self.input.param("fusion_threshold_gib", None)
        if not threshold_gib:
            self.log.info(
                "fusion_threshold_gib not set — leaving enableSyncThresholdMB as the "
                "cluster has it")
            return None

        threshold_mb = int(threshold_gib) * 1024
        try:
            current = self._get_fusion_settings()
        except Exception as e:
            self._record_issue(
                "Fusion sync threshold",
                f"could not read /settings/fusion ({type(e).__name__}: {e}), so "
                f"enableSyncThresholdMB was left alone — the rebalance may take the DCP "
                f"path if the cluster's threshold is above this test's data volume")
            return None

        log_store_uri = current.get("logStoreURI")
        self._prior_sync_threshold_mb = current.get("enableSyncThresholdMB")
        self.log.info(
            f"/settings/fusion before override: enableSyncThresholdMB="
            f"{self._prior_sync_threshold_mb}, logStoreURI={log_store_uri}")

        status, content = FusionRestAPI(
            self.cluster.master).manage_fusion_settings(
                log_store_uri=log_store_uri, enable_sync_threshold=threshold_mb)
        self.assertTrue(
            status,
            f"POST /settings/fusion with enableSyncThresholdMB={threshold_mb} failed: "
            f"{content}")
        self._sync_threshold_modified = True

        applied = self._get_fusion_settings()
        self.log.info(f"/settings/fusion after override: {applied}")
        self.assertEqual(
            applied.get("enableSyncThresholdMB"), threshold_mb,
            f"enableSyncThresholdMB read back as {applied.get('enableSyncThresholdMB')}, "
            f"expected {threshold_mb} ({threshold_gib} GiB)")
        self.assertEqual(
            applied.get("logStoreURI"), log_store_uri,
            f"logStoreURI changed while setting the threshold: was {log_store_uri}, now "
            f"{applied.get('logStoreURI')}")
        self.log.info(
            f"Fusion sync threshold set to {threshold_mb} MB ({threshold_gib} GiB) — "
            f"data above this takes the fusion path, below it uses DCP")
        return threshold_mb

    def _restore_fusion_sync_threshold(self):
        """Put back the pre-test enableSyncThresholdMB. Never raises."""
        if not getattr(self, "_sync_threshold_modified", False):
            return
        try:
            prior = self._prior_sync_threshold_mb
            current = self._get_fusion_settings()
            if prior is None:
                self.log.warning(
                    "No prior enableSyncThresholdMB captured — leaving "
                    f"{current.get('enableSyncThresholdMB')} in place")
                return
            status, content = FusionRestAPI(
                self.cluster.master).manage_fusion_settings(
                    log_store_uri=current.get("logStoreURI"),
                    enable_sync_threshold=prior)
            if status:
                self.log.info(f"Restored enableSyncThresholdMB to {prior}")
                self._sync_threshold_modified = False
            else:
                self.log.error(
                    f"Failed to restore enableSyncThresholdMB to {prior}: {content}")
        except Exception as e:
            self.log.error(
                f"Failed to restore enableSyncThresholdMB — later tests may run against "
                f"this test's threshold: {e}")

    # ------------------------------------------------------------------
    # Background migration rate limit (freeze / resume)
    # ------------------------------------------------------------------

    def _get_migration_rate_limit(self):
        """Read fusion_migration_rate_limit from the global memcached settings."""
        self.fusion_monitor.set_admin_credentials(self.cluster)
        status, content = ClusterRestAPI(
            self.cluster.master).manage_global_memcached_setting()
        if not status or not isinstance(content, dict):
            raise Exception(f"GET memcached global settings failed: {content}")
        return content.get("fusion_migration_rate_limit")

    def _set_migration_rate_limit(self, value, reason):
        """Set fusion_migration_rate_limit cluster-wide and verify the read-back.

        Retries once behind find_master, since a fusion rebalance can replace the node
        cluster.master points at. Raises on failure — callers decide whether that is fatal.
        """
        def _post():
            self.fusion_monitor.set_admin_credentials(self.cluster)
            status, content = ClusterRestAPI(
                self.cluster.master).manage_global_memcached_setting(
                    fusion_migration_rate_limit=value)
            if not status:
                raise Exception(f"POST fusion_migration_rate_limit={value} failed: "
                                f"{content}")

        self.log.info(f"Setting fusion_migration_rate_limit={value} "
                      f"({value / (1024 * 1024):.1f} MB/s) — {reason}")
        try:
            _post()
        except Exception as e:
            self.log.warning(
                f"Setting the migration rate limit failed ({e}); refreshing master and "
                f"retrying once")
            self.find_master(self.tenant, self.cluster)
            _post()
        applied = self._get_migration_rate_limit()
        self.log.info(f"fusion_migration_rate_limit read back as {applied}")
        if applied != value:
            raise Exception(
                f"fusion_migration_rate_limit read back as {applied}, expected {value}")

    def _pause_migration(self):
        """Freeze background extent migration (rate limit 0). Returns True on success.

        Must be set BEFORE the rebalance is triggered. Without it the CP deletes each
        guest volume as its shard's copy finishes (observed: three of six gone before
        Stage C could look), making the handoff check and drain observation a race.

        It also makes the Stage C read workload meaningful: with migration stopped the
        data still lives only on the guest volumes, so a successful read proves it is
        served from there rather than from an already-migrated main volume.

        The prior value is captured so _resume_migration() restores what was actually set.
        """
        try:
            self._prior_migration_rate_limit = self._get_migration_rate_limit()
            self.log.info(
                f"fusion_migration_rate_limit before freeze: "
                f"{self._prior_migration_rate_limit}")
            self._set_migration_rate_limit(
                0, "freeze background migration so guest volumes persist through Stage C")
            self._migration_paused = True
            return True
        except Exception as e:
            self._record_issue(
                "Stage 0: pause background migration",
                f"could not freeze background migration ({type(e).__name__}: {e}); "
                f"continuing without the freeze, so guest volumes may be deleted before "
                f"Stage C and Stage E can observe them")
            return False

    def _resume_migration(self):
        """Restore the migration rate limit so background migration proceeds."""
        target = self._prior_migration_rate_limit
        if not target:
            # Nothing useful captured (or it was already 0): fall back to the default,
            # matching onPrem_basetestcase's fusion_migration_rate_limit.
            target = int(self.input.param("fusion_migration_rate_limit", 78643200))
            self.log.info(
                f"No usable prior migration rate limit captured — resuming at the "
                f"default {target} ({target / (1024 * 1024):.1f} MB/s)")
        self._set_migration_rate_limit(
            target, "resume background migration after the Stage C validation")
        self._migration_paused = False

    def _restore_migration_rate_limit(self):
        """tearDown safety net: never hand a migration-frozen cluster to the next test."""
        if not getattr(self, "_migration_paused", False):
            return
        try:
            self._resume_migration()
            self.log.info("Migration rate limit restored during tearDown")
        except Exception as e:
            self.log.error(
                f"Failed to restore fusion_migration_rate_limit — background migration "
                f"may still be frozen for later tests: {e}")

    # ------------------------------------------------------------------
    # Fusion support config (minSplitSize)
    # ------------------------------------------------------------------

    def _apply_fusion_config_from_params(self):
        """Apply the fusion support-config overrides given as params; return (gb, slots).

        Any of these may be set by a conf line:
          fusion_min_split_size_gb — manifest.minSplitSize (CP default 50 GB), the minimum
              shard size. Lowering it splits the same data into more shards, so more
              accelerators, ASGs and guest volumes.
          fusion_max_slots — manifest.maxSlots (CP default 22), the cap on shards per host.
              Lowering it forces the same data into fewer, therefore BIGGER, volumes; at 1
              each host gets a single volume holding all its data.
          fusion_download_rate_limit — accelerator.download.rateLimit in BYTES PER SECOND,
              the throttle on the accelerator agent's S3 download (phase 5). Unset/0 means
              unlimited. Lowering it stretches the download out in wall-clock time, which
              is what test_download_rate_limit_expires_lease_falls_back_to_dcp needs.

        The CP creates min(ceil(hostData / minSplitSize), maxSlots) shards per host, so
        between them the first two knobs decide whether extra data becomes more volumes or
        bigger ones. All go out in a single PUT — see _apply_fusion_config_overrides.

        The return value stays (split_gb, max_slots) for the callers that unpack it; the
        applied rate limit is available as self._fusion_download_rate_limit.
        """
        split = self.input.param("fusion_min_split_size_gb", None)
        slots = self.input.param("fusion_max_slots", None)
        rate = self.input.param("fusion_download_rate_limit", None)
        return self._apply_fusion_config_overrides(
            min_split_size_gb=float(split) if split else None,
            max_slots=int(slots) if slots else None,
            download_rate_limit=int(rate) if rate else None)

    def _apply_fusion_config_overrides(self, min_split_size_gb=None, max_slots=None,
                                       download_rate_limit=None):
        """Apply fusion support-config overrides in ONE PUT; return (split_gb, max_slots).

        set_fusion_config replaces the whole config, so every override must go in the same
        call — setting minSplitSize and then maxSlots separately drops the first. The prior
        config is captured for tearDown to restore.

        Below the maxSlots cap each new shard is one more volume at the floor size, and
        only once the cap binds does per-shard size have to grow. Pinning maxSlots low is
        therefore the only practical way to observe volume size scaling with data (see
        test_guest_volume_size_scales_with_data).

        download_rate_limit lands under accelerator.download.rateLimit rather than
        manifest.*: it does not change how the data is split, only how fast the agent is
        allowed to pull each shard out of S3.
        """
        if not min_split_size_gb and not max_slots and not download_rate_limit:
            self.log.info(
                "No fusion config overrides requested — leaving the config alone (CP "
                "defaults: minSplitSize 50 GB, maxSlots 22, download rate unlimited)")
            return None, None

        try:
            self._prior_fusion_config = CapellaAPI.get_fusion_config(
                self.pod, self.tenant, self.cluster.id)
        except Exception as e:
            self._prior_fusion_config = None
            self.log.warning(f"Could not read the existing fusion config: {e}")
        self.log.info(f"Fusion config before override: {self._prior_fusion_config}")

        min_split_bytes = int(min_split_size_gb * (1024 ** 3)) if min_split_size_gb else None
        CapellaAPI.set_fusion_config(
            self.pod, self.tenant, self.cluster.id,
            min_split_size=min_split_bytes, max_slots=max_slots,
            download_rate_limit=download_rate_limit)
        self._fusion_config_modified = True
        self._fusion_min_split_size_gb = min_split_size_gb
        self._fusion_max_slots = max_slots
        self._fusion_download_rate_limit = download_rate_limit
        self.log.info(
            f"Set fusion config for cluster {self.cluster.id}: "
            f"minSplitSize={min_split_size_gb} GB ({min_split_bytes} bytes), "
            f"maxSlots={max_slots}, "
            f"download rateLimit={download_rate_limit} B/s"
            + (f" ({download_rate_limit / (1024 ** 2):.2f} MiB/s)"
               if download_rate_limit else ""))

        # Read back so the run log records what the CP actually stored.
        try:
            applied = CapellaAPI.get_fusion_config(
                self.pod, self.tenant, self.cluster.id)
            self.log.info(f"Fusion config after override: {applied}")
            manifest = applied.get("manifest") or {}
            download = (applied.get("accelerator") or {}).get("download") or {}
            if download_rate_limit is not None:
                self.assertEqual(
                    download.get("rateLimit"), download_rate_limit,
                    f"Fusion config read-back shows accelerator.download.rateLimit="
                    f"{download.get('rateLimit')}, expected {download_rate_limit} B/s — "
                    f"the throttle was not stored, so the download will run at full speed")
            if min_split_bytes is not None:
                self.assertEqual(
                    manifest.get("minSplitSize"), min_split_bytes,
                    f"Fusion config read-back shows minSplitSize="
                    f"{manifest.get('minSplitSize')}, expected {min_split_bytes} "
                    f"({min_split_size_gb} GB)")
            if max_slots is not None:
                self.assertEqual(
                    manifest.get("maxSlots"), max_slots,
                    f"Fusion config read-back shows maxSlots="
                    f"{manifest.get('maxSlots')}, expected {max_slots}")
        except AssertionError:
            raise
        except Exception as e:
            self.log.warning(f"Could not read back the fusion config: {e}")
        return min_split_size_gb, max_slots

    def _restore_fusion_config(self):
        """Put back the fusion config captured by _apply_fusion_config_from_params().

        Restores the prior config verbatim, or deletes it so the resource reverts to CP
        defaults. Never raises — tearDown must still reach the topology reset and cleanup.
        """
        if not getattr(self, "_fusion_config_modified", False):
            return
        try:
            prior = getattr(self, "_prior_fusion_config", None) or {}
            manifest = prior.get("manifest") or {}
            accelerator = prior.get("accelerator") or {}
            guest_volumes = accelerator.get("guestVolumes") or {}
            download = accelerator.get("download") or {}
            if manifest or guest_volumes or download:
                CapellaAPI.set_fusion_config(
                    self.pod, self.tenant, self.cluster.id,
                    min_split_size=manifest.get("minSplitSize"),
                    max_slots=manifest.get("maxSlots"),
                    iops=guest_volumes.get("iops"),
                    throughput=guest_volumes.get("throughput"),
                    download_rate_limit=download.get("rateLimit"))
                self.log.info(f"Restored the prior fusion config: {prior}")
            else:
                CapellaAPI.delete_fusion_config(
                    self.pod, self.tenant, self.cluster.id)
                self.log.info(
                    "Deleted this test's fusion config — resource reverted to CP defaults")
            self._fusion_config_modified = False
        except Exception as e:
            self.log.error(
                f"Failed to restore the fusion config for cluster {self.cluster.id} — "
                f"minSplitSize may still be overridden for later tests: {e}")

    def _load_above_threshold(self, create_start=0):
        """Load documents past the fusion threshold, then wait for the S3 upload to catch up.

        create_end is PER COLLECTION and these buckets get 2 collections by default, so
        the data loaded is (create_end - create_start) x collections x doc_size.

        :param create_start: first document index to create. Pass the count already loaded
            when a test loads in more than one pass, or the second pass re-creates the
            first pass's keys and the loader reports "document exists" for all of them.

        The wait matters as much as the load: a rebalance triggered before the uploader has
        flushed plans against an incomplete log store, which shows up as smaller shards,
        fewer accelerators, or the DCP path being chosen. See _wait_for_log_store_sync.

        The data summary logged afterwards is the first thing to check when a stage reports
        it had nothing to observe — fusion stage windows scale with the data volume.
        """
        create_end = self.input.param("create_end", 20_000_000)
        doc_size = self.input.param("doc_size", 1024)
        collections = self.input.param("collections", 2)
        upload_interval = int(self.input.param("fusion_upload_interval", 600))
        settle = int(self.input.param("fusion_upload_settle", 120))
        self.assertGreater(
            create_end, create_start,
            f"create_end ({create_end}) must exceed create_start ({create_start}) — "
            f"there is nothing to load otherwise")
        new_docs = create_end - create_start
        total_items = new_docs * collections * len(self.cluster.buckets)
        self.log.info(
            f"[load] docs [{create_start}, {create_end}) = {new_docs} x {collections} "
            f"collection(s) x {len(self.cluster.buckets)} bucket(s) = {total_items} new "
            f"items at doc_size={doc_size} B => "
            f"~{total_items * doc_size / (1024 ** 3):.1f} GiB logical; "
            f"fusion_threshold_gib={self.input.param('fusion_threshold_gib', None)}")

        load_start = time.time()
        self._load_data(
            self.cluster, create_start=create_start, create_end=create_end)
        load_secs = time.time() - load_start
        self._wait_for_log_store_sync(load_secs)
        self._log_cluster_data_summary("after initial load")

    def _wait_for_log_store_sync(self, load_secs):
        """Wait for the fusion uploader to catch up after a load, then check S3.

        Shared by every load path so none of them races the uploader. Waits for BOTH
        floors: `fusion_upload_interval` since the load started, so a short load cannot
        outrun the uploader, and `fusion_upload_settle` since it ended, so the final batch
        can flush. Set settle == interval for the strict worst case (a doc written in the
        last moment of the load needs a full interval after the load ENDS).

        The cloud base classes do not define fusion_upload_interval (only
        onPrem_basetestcase does, at 60s), so it defaults to 600s here to match the Capella
        server-side interval; override it if your cluster differs.
        """
        upload_interval = int(self.input.param("fusion_upload_interval", 600))
        settle = int(self.input.param("fusion_upload_settle", 120))
        wait_secs = max(settle, upload_interval - load_secs)
        self.log.info(
            f"[load] load took {load_secs:.0f}s; fusion_upload_interval="
            f"{upload_interval}s, fusion_upload_settle={settle}s => waiting "
            f"{wait_secs:.0f}s more (total {load_secs + wait_secs:.0f}s since the load "
            f"started) so the S3 log store catches up before the rebalance")
        self.sleep(wait_secs, "Allow the fusion uploader to sync to S3 before rebalancing")

        # Direct evidence the upload actually happened, rather than trusting the clock.
        try:
            s3_bucket_name = self._get_s3_bucket_name_from_uri(self.cluster)
            objects = (self._get_s3_object_count_for_buckets(
                s3_bucket_name, self.cluster.buckets) if s3_bucket_name else 0)
            if objects:
                self.log.info(
                    f"[load] S3 log store '{s3_bucket_name}' holds {objects} object(s) "
                    f"after the wait")
            else:
                self.log.warning(
                    f"[load] S3 log store '{s3_bucket_name}' still reports 0 objects "
                    f"after waiting {wait_secs:.0f}s — the rebalance may plan against an "
                    f"empty log store. Check fusion_upload_interval against the cluster's "
                    f"actual setting.")
        except Exception as e:
            self.log.warning(f"[load] could not check the S3 log store after the wait: {e}")

    def _default_rebalance_type(self):
        """Rebalance type for this run, from the `rebalance_type` param (default 'in')."""
        return self.input.param("rebalance_type", "in").lower()

    def _alt_compute(self):
        """Return a compute type distinct from the initial one, for swap rebalances.

        Prefers `swap_compute`; otherwise derives a SAME-FAMILY alternate so the swap does
        not cross architectures, which may be invalid for a fusion cluster. Returns None
        when no same-family alternate is known, and the caller then requires swap_compute.
        """
        alt = self.input.param("swap_compute", None)
        if alt:
            return alt
        provider = self.input.param("provider", "aws").lower()
        compute_list = {"aws": AWS.compute, "gcp": GCP.compute,
                        "azure": AZURE.compute}.get(provider, AWS.compute)
        initial = self.initial_kv_compute
        family = initial.split(".")[0] if "." in initial else initial
        same_family = [c for c in compute_list
                       if c != initial and (c.split(".")[0] if "." in c else c) == family]
        return same_family[0] if same_family else None

    def _inverse_rebalance_type(self, rebalance_type=None):
        """Reverse of a rebalance type: in <-> out; swap is its own inverse."""
        rtype = rebalance_type or self._default_rebalance_type()
        return {
            "in": "out", "scale_out": "out", "rebalance_in": "out",
            "out": "in", "scale_in": "in", "rebalance_out": "in",
            "swap": "swap",
        }.get(rtype, "in")

    def _trigger_rebalance(self, rebalance_type=None):
        """Trigger a fusion-eligible rebalance of the requested type; return the async task.

        Type comes from the `rebalance_type` param (default 'in') unless passed explicitly:

          in  / scale_out / rebalance_in  -> add `rebalance_delta` data nodes (default 1)
          out / scale_in  / rebalance_out -> remove `rebalance_delta` data nodes (default 1)
          swap                            -> same node count, different compute (toggles
                                             between the initial compute and _alt_compute())
        """
        rtype = rebalance_type or self._default_rebalance_type()
        delta = int(self.input.param("rebalance_delta", 1))

        if rtype in ("in", "scale_out", "rebalance_in"):
            config = self.rebalance_config("data", +delta)
        elif rtype in ("out", "scale_in", "rebalance_out"):
            if self.num_nodes["data"] - delta < self._MIN_KV_NODES:
                self.fail(
                    f"rebalance_type='out' needs at least {self._MIN_KV_NODES + delta} "
                    f"data nodes to remove {delta} and stay >= {self._MIN_KV_NODES}; "
                    f"current data nodes={self.num_nodes['data']}. Increase kv_nodes.")
            config = self.rebalance_config("data", -delta)
        elif rtype == "swap":
            current = self.compute["data"]
            # Toggle: away from the initial compute on the first swap, back to it on
            # the next — keeps repeated swaps (back-to-back / two-phase tests) valid.
            target = self._alt_compute() if current == self.initial_kv_compute \
                else self.initial_kv_compute
            if not target or target == current:
                self.fail(
                    f"swap rebalance needs a compute type distinct from '{current}' with "
                    f"no known safe same-family alternate for '{self.initial_kv_compute}'. "
                    f"Pass swap_compute=<instance_type> (same family as the fusion compute).")
            self.log.info(f"Swap rebalance: compute {current} -> {target}")
            self.compute["data"] = target
            config = self.rebalance_config("data", 0)
        else:
            self.fail(f"Unsupported rebalance_type: {rtype!r} "
                      "(expected one of: in, out, swap)")

        return self.task.async_rebalance_capella(
            self.pod, self.tenant, self.cluster, config, timeout=self.rebalance_timeout)

    def _trigger_scale_out(self):
        """Deprecated: use _trigger_rebalance(). Kept as an explicit +1 scale-out."""
        return self._trigger_rebalance("in")

    def _poll_until_accelerators_appear(self, rebalance_task, timeout=1800):
        """Poll until at least one accelerator instance appears or the rebalance ends.

        Returns the instances, possibly empty if the rebalance finished without launching
        any — the caller should assert on that.

        Returns as soon as the FIRST accelerator is visible, so it is only the right gate
        for "at least one exists". Any fleet-wide assertion must use
        _wait_for_accelerator_fleet_stable() or it judges a partially launched fleet.
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            if rebalance_task.state in self._FAILED_STATES:
                self.fail(f"Rebalance failed with state: {rebalance_task.state}")
            instances = self.fusion_aws_util.list_accelerator_instances(
                self._accelerator_filter(), log="Fusion Accelerator")
            if instances:
                return instances
            if rebalance_task.state == "healthy":
                break
            time.sleep(10)
        return []

    def _wait_for_cp_rebalance_complete(self, rebalance_task, timeout=None,
                                        poll_interval=5):
        """Return as soon as the control plane reports the rebalance finished.

        Exists purely to remove dead time before Stage C. wait_for_rebalances() is the
        authoritative check but slow to RETURN: it sleeps 10s + 60s after ns_server reports
        done. Guest volumes are released per shard as migration completes, so that dead time
        cost 4 of 5 volumes in one run. The caller must still run wait_for_rebalances()
        afterwards — this is a head start, not a replacement.

        Reading "healthy" cannot be a false positive: RebalanceTaskCapella.__init__ already
        blocks until the CP moves the cluster out of healthy into scaling.
        """
        timeout = timeout or self.rebalance_timeout
        deadline = time.time() + timeout
        start = time.time()
        while time.time() < deadline:
            if rebalance_task.state in self._FAILED_STATES:
                self.fail(f"Rebalance failed while waiting for completion: "
                          f"{rebalance_task.state}")
            try:
                state = CapellaAPI.get_cluster_state(
                    self.pod, self.tenant, self.cluster.id)
            except Exception as e:
                self.log.warning(f"[completion] cluster state read failed: {e}")
                state = None
            if state and state.lower() == "healthy":
                self.log.info(
                    f"Control plane reports the rebalance complete after "
                    f"{time.time() - start:.0f}s (cluster state={state}) — starting "
                    f"post-completion checks immediately")
                return True
            self.log.info(
                f"[completion] elapsed={time.time() - start:.0f}s cluster_state={state} "
                f"task_state={rebalance_task.state}")
            time.sleep(poll_interval)
        self.log.warning(
            f"Control plane did not report completion within {timeout}s — continuing to "
            f"wait_for_rebalances() for the authoritative result")
        return False

    def _list_accelerator_instances_by_tag(self, log="AcceleratorFleet",
                                           suppress_log=True):
        """Running accelerator instances counted by TAG, with no IOPS filter.

        fusion_aws_util.list_accelerator_instances() only matches an instance once it has a
        16000-IOPS volume attached, so it reports a moving subset during phase 4 and stops
        matching once the handoff scales the volume down. Comparing that against a
        tag-based ASG count yields e.g. "4 ASGs vs 1 accelerator" on a healthy fleet. This
        lists exactly what the ASG side does: couchbase-cloud-function=fusion-accelerator.
        """
        filters = self.fusion_aws_util._cluster_filter(
            self.cluster.id,
            [{"Name": "tag:couchbase-cloud-function",
              "Values": ["fusion-accelerator"]}])
        instances = self.fusion_aws_util.list_instances(
            filters, log=log, suppress_log=suppress_log)
        self._log_query("accelerator instances (by tag, no IOPS filter)", filters,
                        self._describe_instances(instances))
        return instances

    def _wait_for_accelerator_fleet_stable(self, rebalance_task, timeout=None):
        """Wait for the accelerator fleet to stop changing; return (instances, asgs).

        Stage A's assertions are all fleet-wide, but accelerators come up gradually during
        phase 4 and each attaches its guest volume independently, so asserting on the first
        sample that shows any accelerator judges a fraction of the fleet.

        Two exits, both returning a self-consistent snapshot:

          1. Fast path — one sample with the fleet fully up: one InService instance per ASG,
             tagged instance count == ASG count, every instance holding its guest volume.
          2. Stability path — the tagged count held steady for `fleet_stable_samples`
             consecutive polls and equals the ASG count. Needed because on a fast rebalance
             volumes attach and detach per shard, so "every instance has its guest volume"
             may never be true in a single sample.

        Stability is measured on CONCURRENT samples, not by unioning instance IDs over the
        phase: an ASG relaunching a terminated accelerator (which the chaos test does on
        purpose) would inflate a cumulative union and fail parity spuriously.

        Fails with the observed history if the fleet never settles, since a bare "4 != 1"
        would hide the real problem.

        Polls every `fleet_poll_interval` seconds (default 5): accelerators launch and
        scale back down per shard, so a slow cadence can miss the fleet's peak entirely —
        e.g. a fleet that ramps to full parity and starts draining again within a single
        10s poll never gets the 3 consecutive samples the stability path needs.
        """
        timeout = timeout or self.gv_launch_timeout
        stable_samples = int(self.input.param("fleet_stable_samples", 3))
        poll_interval = int(self.input.param("fleet_poll_interval", 5))
        history = []
        instances, asgs = [], []

        def _in_service(asg):
            return [i for i in asg.get("Instances", [])
                    if i.get("LifecycleState") == "InService"]

        deadline = time.time() + timeout
        while time.time() < deadline:
            if rebalance_task.state in self._FAILED_STATES:
                self.fail(f"Rebalance failed while waiting for the accelerator fleet: "
                          f"{rebalance_task.state}")

            instances = self._list_accelerator_instances_by_tag()
            asgs = self.fusion_aws_util.list_cluster_fusion_asg(self.cluster.id)
            self._log_query(
                "fusion ASGs", f"cluster-id={self.cluster.id} + function=fusion-accelerator",
                f"{len(asgs)} ASG(s): "
                + ", ".join(
                    f"{a.get('AutoScalingGroupName')}"
                    f"[desired={a.get('DesiredCapacity')},max={a.get('MaxSize')},"
                    f"in_service={len(_in_service(a))}]" for a in asgs))
            with_guest_vol = len(self.fusion_aws_util.list_accelerator_instances(
                self._accelerator_filter(), log="FleetGuestVolume"))
            history.append((len(instances), len(asgs), with_guest_vol))
            self.log.info(
                f"[fleet] accelerators(tagged)={len(instances)} asgs={len(asgs)} "
                f"with_guest_volume={with_guest_vol}")

            one_per_asg = bool(asgs) and all(len(_in_service(a)) == 1 for a in asgs)
            parity = bool(asgs) and len(instances) == len(asgs)

            if one_per_asg and parity and with_guest_vol == len(instances):
                self.log.info(
                    f"Accelerator fleet fully up: {len(instances)} instance(s), "
                    f"{len(asgs)} ASG(s), all guest volumes attached")
                return instances, asgs

            recent = [h[0] for h in history[-stable_samples:]]
            if (len(recent) == stable_samples and len(set(recent)) == 1
                    and one_per_asg and parity):
                self.log.info(
                    f"Accelerator fleet stable at {len(instances)} instance(s) across "
                    f"{stable_samples} polls, matching {len(asgs)} ASG(s) "
                    f"({with_guest_vol} with a guest volume attached at this sample)")
                return instances, asgs

            if rebalance_task.state == "healthy":
                break
            time.sleep(poll_interval)

        counts = ", ".join(f"({t}i/{a}asg/{g}gv)" for t, a, g in history) or "no samples"
        if history and all(h[0] == 0 for h in history):
            self.fail(
                f"No accelerator instances were ever tagged for this cluster over "
                f"{len(history)} poll(s) — the rebalance did not take the fusion path. "
                f"Observed (tagged instances / ASGs / with guest volume): {counts}")
        self.fail(
            f"Accelerator fleet never stabilised within {timeout}s: the tagged instance "
            f"count never held steady for {stable_samples} consecutive polls while "
            f"matching the ASG count with one InService instance each. Fleet-wide "
            f"Stage A assertions are not meaningful against a moving fleet. Observed "
            f"(tagged instances / ASGs / with guest volume): {counts}. If the counts "
            f"rise and fall, the accelerator lifecycle is completing faster than this "
            f"gate can sample it — raise create_end, or lower fleet_poll_interval "
            f"(currently {poll_interval}s).")

    # ==================================================================
    # Stage validators — one per stage of the fusion rebalance lifecycle,
    # shared by the per-stage tests (12-18) and test_fusion_scaling_lifecycle
    # so the standalone and end-to-end runs cannot drift apart.
    #
    # None of these wait for the rebalance to finish: the caller owns
    # wait_for_rebalances()/wait_until_done(), because the stage boundary
    # differs between the standalone and end-to-end flows.
    # ==================================================================

    def _accelerator_volume_filters(self, guest_only=True):
        """Tag filters for accelerator EBS volumes (see _list_accelerator_volumes)."""
        filters = {
            "couchbase-cloud-cluster-id": self.cluster.id,
            "couchbase-cloud-function": "fusion-accelerator",
        }
        if guest_only:
            filters["couchbase-cloud-fusion-guest-volume"] = "true"
        return filters

    def _list_accelerator_volumes(self, guest_only=True, log_query=True):
        """List accelerator EBS volumes for this cluster.

        guest_only=True restricts to the fusion guest volumes, excluding each accelerator's
        root volume, which has different size/IOPS characteristics.

        list_volumes_by_cluster_id turns every filter key into a tag: filter, so volume
        attributes such as State must be filtered client-side.
        """
        filters = self._accelerator_volume_filters(guest_only=guest_only)
        volumes = self.fusion_aws_util.ec2.list_volumes_by_cluster_id(filters=filters)
        if log_query:
            self._log_query(
                f"accelerator volumes (guest_only={guest_only})", filters,
                self._describe_volumes(volumes))
        return volumes

    def _poll_until_accelerator_volumes_appear(self, rebalance_task,
                                               guest_only=True, timeout=1800):
        """Poll until accelerator EBS volumes appear, or the rebalance ends."""
        volumes = []
        deadline = time.time() + timeout
        while time.time() < deadline:
            if rebalance_task.state in self._FAILED_STATES:
                self.fail(f"Rebalance failed before volumes appeared: "
                          f"{rebalance_task.state}")
            volumes = self._list_accelerator_volumes(guest_only=guest_only)
            if volumes:
                self.log.info(f"Found {len(volumes)} accelerator EBS volume(s)")
                break
            if rebalance_task.state == "healthy":
                break
            time.sleep(10)
        return volumes

    def _guest_volumes_by_instance(self):
        """Return {instance_id: [volume_id, ...]} for the current guest volumes.

        'unattached' collects volumes with no attachment — the valid window between CBS
        releasing a volume and the CP deleting it.
        """
        mapping = dict()
        for vol in self._list_accelerator_volumes(guest_only=True):
            atts = vol.get("Attachments") or []
            inst = atts[0].get("InstanceId") if atts else "unattached"
            mapping.setdefault(inst, []).append(vol.get("VolumeId"))
        return mapping

    def _log_guest_volume_placement(self, label):
        """Log where the guest volumes currently sit, and return the mapping."""
        mapping = self._guest_volumes_by_instance()
        total = sum(len(v) for v in mapping.values())
        self.log.info(
            f"[{label}] {total} guest volume(s) across {len(mapping)} attachment(s):\n"
            + "\n".join(f"    {inst}: {len(vols)} {sorted(vols)}"
                        for inst, vols in sorted(mapping.items())))
        return mapping

    def _run_read_workload(self, label, read_items=None):
        """Run a read-only workload to prove data is accessible at this point."""
        read_items = read_items or self.input.param("read_workload_items", 100000)
        self.log.info(f"Running read workload over {read_items} items ({label})")
        self._load_data(
            self.cluster, doc_ops=["read"], read_start=0, read_end=read_items,
            overRidePattern={"create": 0, "read": 100, "update": 0,
                             "delete": 0, "expiry": 0},
            wait_for_load=True)
        self.log.info(f"Read workload completed — data accessible ({label})")

    def _capture_s3_log_store_baseline(self):
        """S3 log-store bucket name and object count; returns (bucket_name, count).

        Must be called BEFORE the rebalance so Stage F can prove teardown left S3 intact.
        """
        s3_bucket_name = self._get_s3_bucket_name_from_uri(self.cluster)
        s3_objects_before = (
            self._get_s3_object_count_for_buckets(s3_bucket_name, self.cluster.buckets)
            if s3_bucket_name else 0)
        self.log.info(f"S3 log store '{s3_bucket_name}' objects before rebalance: "
                      f"{s3_objects_before}")
        return s3_bucket_name, s3_objects_before

    def _validate_accelerator_instance_type(self, instances):
        """Stage A: accelerators are homogeneous and of the expected instance type.

        Asserts one instance type across the fleet, that exact type when
        `expected_accelerator_instance_type` is set, and membership of the ASG
        mixed-instances override list. Returns the observed type.
        """
        types_seen = {inst.get("InstanceType") for inst in instances}
        self.log.info(
            f"Accelerator instance types observed ({len(instances)} instances): {types_seen}")
        self.assertEqual(
            len(types_seen), 1,
            f"Accelerators are not homogeneous — multiple instance types found: {types_seen}")
        actual_type = next(iter(types_seen))

        expected_type = self.input.param("expected_accelerator_instance_type", None)
        if expected_type:
            self.assertEqual(
                actual_type, expected_type,
                f"Accelerator instance type {actual_type!r} != expected {expected_type!r}")
            self.log.info(f"All accelerators are the expected type {actual_type!r}")
        else:
            self.log.info(
                "No expected_accelerator_instance_type param set — asserted homogeneity "
                f"only (all accelerators are {actual_type!r})")

        # The launched type must be one of the ASG mixed-instances overrides.
        try:
            override_types = self.fusion_aws_util.get_asg_ordered_instance_types(
                self.cluster.id)
            if override_types:
                self.log.info(f"ASG instance-type override list: {override_types}")
                self.assertIn(
                    actual_type, override_types,
                    f"Launched accelerator type {actual_type!r} not in ASG override "
                    f"list {override_types}")
        except Exception as e:
            self.log.warning(f"Could not read ASG instance-type override list: {e}")

        return actual_type

    def _validate_asg_topology(self, rebalance_task, instances=None, asgs=None):
        """Stage A: one ASG per accelerator, each DesiredCapacity/MaxSize == 1.

        Expects the stable-fleet snapshot from _wait_for_accelerator_fleet_stable(), and
        takes one itself when `instances`/`asgs` are not supplied, so both the standalone
        and end-to-end tests assert against a settled fleet.

        Parity counts accelerators by TAG on BOTH sides — see
        _list_accelerator_instances_by_tag for why the IOPS-filtered list is wrong here.
        Returns the ASG list.
        """
        def _in_service(asg):
            return [i for i in asg.get("Instances", [])
                    if i.get("LifecycleState") == "InService"]

        if instances is None or asgs is None:
            instances, asgs = self._wait_for_accelerator_fleet_stable(rebalance_task)

        self.assertGreater(len(asgs), 0, "No fusion ASGs found during rebalance")

        failures = []
        for asg in asgs:
            name = asg.get("AutoScalingGroupName", "unknown")
            desired = asg.get("DesiredCapacity")
            max_size = asg.get("MaxSize")
            in_service = _in_service(asg)
            self.log.info(
                f"ASG {name}: desired={desired} max={max_size} "
                f"in_service={len(in_service)}")
            if desired != 1:
                failures.append(f"{name}: DesiredCapacity={desired}, expected 1")
            if max_size != 1:
                failures.append(f"{name}: MaxSize={max_size}, expected 1")
            if len(in_service) != 1:
                failures.append(
                    f"{name}: {len(in_service)} InService instances, expected 1")
        self.assertEqual(
            len(failures), 0,
            "ASG desired-capacity violations:\n" + "\n".join(failures))

        # One ASG per accelerator instance — both sides counted by tag.
        self.assertEqual(
            len(asgs), len(instances),
            f"Expected one ASG per accelerator: {len(asgs)} ASGs vs "
            f"{len(instances)} tagged accelerator instance(s) "
            f"({sorted(i.get('InstanceId') for i in instances)})")
        self.log.info(
            f"All {len(asgs)} fusion ASGs have DesiredCapacity==1 with one InService "
            f"instance — one ASG per accelerator confirmed")
        return asgs

    def _validate_accelerator_volume_specs(self, rebalance_task):
        """Stage A: every accelerator guest volume is >= 50 GB at 16000 IOPS.

        Volume size is ceil(shardStorageSize/1GB) + 10% with a floor. Volumes are read by
        cluster/function/guest-volume tag, NOT by IOPS, so the IOPS assertion is not
        circular. Returns the volume list.
        """
        # The floor is minVolumeSize = 50 GB (ACCELERATION.md §Phase 3: "also the minimum
        # EBS volume size we create"). minSplitSize does NOT lower it — a run with
        # minSplitSize=5 GB still produced 50 GiB volumes — so take the larger of the two;
        # deriving from minSplitSize alone would assert a floor of 5 and pass on anything.
        min_size_gb = self.input.param(
            "min_volume_size_gb",
            max(50, self._fusion_min_split_size_gb or 0))
        expected_iops = self.input.param("expected_iops", 16000)
        self.log.info(
            f"Expecting guest volumes >= {min_size_gb} GiB at {expected_iops} IOPS "
            f"(fusion config overrides — minSplitSize: "
            f"{self._fusion_min_split_size_gb or 'CP default'}, maxSlots: "
            f"{self._fusion_max_slots or 'CP default'})")

        volumes = self._poll_until_accelerator_volumes_appear(rebalance_task)
        self.assertGreater(
            len(volumes), 0,
            "No accelerator EBS volumes appeared during the fusion rebalance")

        failures = []
        for vol in volumes:
            vol_id = vol.get("VolumeId", "unknown")
            size = vol.get("Size")
            iops = vol.get("Iops")
            self.log.info(f"Accelerator volume {vol_id}: Size={size} GiB, Iops={iops}")
            if size is None or size < min_size_gb:
                failures.append(f"{vol_id}: Size={size} GiB < {min_size_gb} GiB floor")
            if iops != expected_iops:
                failures.append(f"{vol_id}: Iops={iops}, expected {expected_iops}")

        self.assertEqual(
            len(failures), 0,
            "Accelerator EBS volume violations:\n" + "\n".join(failures))
        self.log.info(
            f"All {len(volumes)} accelerator EBS volume(s) >= {min_size_gb} GiB "
            f"at {expected_iops} IOPS")
        return volumes

    def _capture_guest_volume_ids(self, rebalance_task):
        """Capture the guest volume IDs while they are still on the accelerators.

        Must run before Stage C: the 16000-IOPS-based helpers stop matching a volume once
        the CP scales it down on handoff.
        """
        guest_vol_ids = set()
        deadline = time.time() + self.gv_launch_timeout
        while time.time() < deadline:
            if rebalance_task.state in self._FAILED_STATES:
                self.fail(f"Rebalance failed capturing guest volumes: "
                          f"{rebalance_task.state}")
            vols = self._list_accelerator_volumes()
            guest_vol_ids.update(v["VolumeId"] for v in vols if v.get("VolumeId"))
            if guest_vol_ids:
                break
            time.sleep(10)
        self.assertGreater(len(guest_vol_ids), 0,
                           "No guest volumes appeared during the rebalance")
        self.log.info(f"Tracking {len(guest_vol_ids)} guest volume(s): {guest_vol_ids}")
        return guest_vol_ids

    def _report_iops_scale_down(self, still_live, settled, scaled_count, total_count,
                                window_note):
        """Report the guest-volume IOPS scale-down as its own issue (never a handoff fail).

        `still_live` and `settled` are {vol_id: last IOPS seen} for volumes never observed
        below the download IOPS: the first still existed when observation stopped, the
        second were already deleted and so can never scale. Only the second is conclusive.

        "Scaled down" means strictly below `expected_iops` (16000), not equal to any
        particular value. The CP settles at the gp3 baseline (`scaled_iops`, 3000) but that
        figure is not the invariant — staying at download IOPS is the defect, and any lower
        value means the scale-down happened. `scaled_iops` is log context only.
        """
        expected_iops = self.input.param("expected_iops", 16000)
        scaled_iops = self.input.param("scaled_iops", 3000)
        unscaled = dict(settled)
        unscaled.update(still_live)
        if not unscaled:
            self.log.info(
                f"All {scaled_count}/{total_count} guest volume(s) scaled below the "
                f"{expected_iops} download IOPS (CP default target is {scaled_iops})")
            return
        detail = "\n".join(
            f"  {v}: last iops={unscaled[v] if unscaled[v] is not None else 'never read'}"
            f"{' (deleted before scale-down)' if v in settled else ' (still present)'}"
            for v in sorted(unscaled))
        message = (
            f"{len(unscaled)} of {total_count} guest volume(s) were never observed below "
            f"the {expected_iops} download IOPS (expected a scale-down {window_note}; the "
            f"CP normally settles at {scaled_iops}); {scaled_count} did scale. The handoff "
            f"itself succeeded for all volumes — this is only about the IOPS "
            f"property.\n{detail}")
        if not self.soft_fail_stages:
            self.fail(message)
        self._record_issue("Guest volume IOPS scale-down", message)

    def _validate_guest_volume_drain(self, expected_start=None, baseline_du=None,
                                     timeout=None, poll_interval=30, iops_watch=None):
        """Stage E (simplified): watch the guest volumes drain while the main volume fills.

        A deliberately small view of background migration, using only cheap and unambiguous
        signals: the guest EBS volume count, which must fall monotonically to zero as each
        shard's copy completes, and average main-volume disk usage, which must end above the
        pre-migration baseline. Both series are logged per poll so the shape of the drain is
        visible, not just its endpoint. Call AFTER rebalance completion — migration only
        starts once the rebalance has finished.

        _validate_background_migration() is the detailed variant used by
        test_background_migration_progress.

        :param expected_start: guest volume count observed earlier, used to tell "drained
            before we looked" apart from "never anything to drain" — both fail, different fix.
        :param baseline_du: main-volume usage sampled immediately after rebalance completion,
            the last point at which no extents have migrated. Sampled here if not supplied,
            which weakens the growth check since copying may already be under way.
        :param iops_watch: {vol_id: last IOPS} handed over by Stage C for volumes not yet
            seen below the download IOPS. This loop already describes every volume each
            poll, so it finishes that observation for free and watches until each volume is
            actually deleted. Reported at the end as its own issue, never as a drain failure.
        """
        timeout = timeout or self.hydration_timeout
        if baseline_du is None:
            baseline_du = self._avg_main_volume_usage()
            self.log.warning(
                f"No pre-migration disk baseline supplied — sampling now "
                f"({baseline_du:.1f}%); growth may already be partly included")
        self.log.info(
            f"Stage E: draining guest volumes (expected_start={expected_start}, "
            f"baseline_du={baseline_du:.1f}%, timeout={timeout}s)")

        expected_iops = self.input.param("expected_iops", 16000)
        iops_watch = dict(iops_watch or {})
        iops_scaled_late = dict()
        if iops_watch:
            self.log.info(
                f"Stage E also watching the IOPS scale-down of {len(iops_watch)} "
                f"volume(s) handed over by Stage C: {sorted(iops_watch)}")

        counts, du_series, api_series = [], [], []
        drained = False
        start = time.time()
        while time.time() - start < timeout:
            volumes = self._list_accelerator_volumes(guest_only=True)
            attached = [v for v in volumes if (v.get("Attachments") or [])]
            du_now = self._avg_main_volume_usage()
            counts.append(len(volumes))
            du_series.append(du_now)

            for vol in volumes:
                vid = vol.get("VolumeId")
                if vid in iops_watch:
                    iops_watch[vid] = vol.get("Iops")
                    # Scaled down = anything below the download IOPS, not one exact value.
                    if (vol.get("Iops") or expected_iops) < expected_iops:
                        iops_scaled_late[vid] = iops_watch.pop(vid)
                        self.log.info(
                            f"[drain] {vid} IOPS scaled to {vol.get('Iops')}, below the "
                            f"{expected_iops} download value (observed during the drain, "
                            f"after Stage C ended)")

            api_count, _ = self._log_active_guest_volumes("drain")
            api_series.append(api_count)

            self.log.info(
                f"[drain] elapsed={time.time() - start:.0f}s "
                f"guest_volumes={len(volumes)} (attached={len(attached)}, "
                f"api={api_count if api_count is not None else 'unavailable'}) "
                f"main_du={du_now:.1f}% at {self._data_path()} "
                f"(baseline {baseline_du:.1f}%, delta {du_now - baseline_du:+.1f})\n"
                f"    volume ids: {sorted(v.get('VolumeId') for v in volumes)}")

            if not volumes:
                drained = True
                break
            time.sleep(poll_interval)

        max_du = max(du_series) if du_series else baseline_du
        self.log.info(
            f"Stage E series over {len(counts)} poll(s):\n"
            f"    guest volume count (AWS): {counts}\n"
            f"    guest volume count (API): {api_series}\n"
            f"    main volume du % ({self._data_path()}): "
            f"{[round(d, 1) for d in du_series]}")

        # Nothing to observe: distinguish "already gone" from "never there".
        if counts and counts[0] == 0:
            if expected_start:
                self.fail(
                    f"Guest volumes were already gone when Stage E started "
                    f"({expected_start} were present earlier) — the drain completed "
                    f"before it could be observed. Raise create_end so migration takes "
                    f"minutes, or shorten the gap before this stage.")
            self.fail(
                "No guest volumes present at the start of Stage E and none were seen "
                "earlier — there was never a migration to observe (was the fusion path "
                "taken at all?)")

        # Close out Stage C's deferred IOPS watch BEFORE this stage's own assertions, so the
        # verdict is recorded even if the drain assertions below fail.
        settled_at_handover = dict(getattr(self, "_iops_settled_unscaled", {}) or {})
        if iops_watch or iops_scaled_late or settled_at_handover:
            never = dict(settled_at_handover)
            never.update(iops_watch)
            scaled_before = getattr(self, "_iops_scaled_count", 0)
            total = (getattr(self, "_iops_total_count", 0)
                     or len(never) + len(iops_scaled_late))
            self._report_iops_scale_down(
                still_live={}, settled=never,
                scaled_count=scaled_before + len(iops_scaled_late),
                total_count=total,
                window_note="before the volume was deleted, watched through the drain")

        # The count must only ever fall: nothing adds guest volumes during migration.
        rises = [(i, counts[i - 1], counts[i])
                 for i in range(1, len(counts)) if counts[i] > counts[i - 1]]
        self.assertEqual(
            rises, [],
            f"Guest volume count increased during migration at poll(s) "
            f"{[r[0] for r in rises]} ({rises}) — migration should only release "
            f"volumes. Full series: {counts}. A rise means either another rebalance "
            f"overlapped this one or the EBS listing was inconsistent.")

        self.assertTrue(
            drained,
            f"Guest volumes did not drain to 0 within {timeout}s — "
            f"{counts[-1] if counts else '?'} still present. Full series: {counts}")
        self.log.info(f"All guest volumes drained to 0 in {time.time() - start:.0f}s")

        self.assertGreater(
            max_du, baseline_du,
            f"Main-volume usage did not increase during migration (baseline "
            f"{baseline_du:.1f}%, max {max_du:.1f}%) — the extent copy into the main "
            f"volume was not observed. Full series: {[round(d, 1) for d in du_series]}")
        self.log.info(
            f"Main-volume usage grew {baseline_du:.1f}% -> {max_du:.1f}% as extents "
            f"were copied in")

    def _validate_guest_volume_transfer(self, rebalance_task, accel_ids, guest_vol_ids,
                                        iops_grace=None, run_read_workload=True):
        """Stage C (phase 6): the volume handoff from accelerators to KV nodes.

        Asserts every guest volume that was on an accelerator is observed ATTACHED to a KV
        node, all accelerator instances are terminated, the guest-volume API reports the
        volumes, and a read workload succeeds. Returns the API count (Stage E's start).

        Time here is not free: guest volumes are deleted as migration completes, so every
        second spent is a second of the drain Stage E cannot observe. Two knobs:

          iops_grace=0        exit as soon as the handoff is complete and defer the IOPS
                              verdict to Stage E, which polls the same volumes anyway and
                              can watch until each is deleted — the real deadline. Non-zero
                              lingers here, which suits the standalone test.
          run_read_workload   False when a later stage validates reads, so the drain window
                              is not spent loading documents.

        The IOPS scale-down is tracked but reported SEPARATELY. Folding it into the transfer
        condition mis-attributes a healthy handoff: in one run four volumes sat at
        `state=in-use attached_to=<KV node> iops=16000` — plainly transferred — yet counted
        as pending purely because IOPS had not scaled, then were deleted, so the stage failed
        with "handoff no longer observable" when the truth was "handoff fine, IOPS never
        scaled". Attachment is the handoff; IOPS is a later property of the volume.

        Each volume is LATCHED the first time it is seen on a KV node rather than requiring
        all of them in one sample. The CP hands off, migrates and releases each shard
        independently, so when per-shard migration beats the poll cadence the "all
        transferred simultaneously" conjunction is never true — volume 1 is released before
        volume 3 attaches — and fails on a perfectly healthy rebalance.

        Once every unlatched volume is deleted the target state is unreachable, so this fails
        immediately with that diagnosis rather than burning the whole gv_launch_timeout.
        """
        expected_iops = self.input.param("expected_iops", 16000)
        # Sample fast: volumes are released per shard, so a slow cadence loses volumes
        # handed off between two polls.
        poll_interval = int(self.input.param("transfer_poll_interval", 5))
        if iops_grace is None:
            iops_grace = int(self.input.param("scaled_iops_grace", 120))

        transferred_on = dict()   # vol_id -> KV instance it was first seen attached to
        iops_scaled = dict()      # vol_id -> IOPS, once seen below expected_iops on a KV node
        last_iops = dict()        # vol_id -> most recent IOPS reading
        last_seen = dict()        # vol_id -> last observation, for diagnostics
        deleted = set()           # vol_ids that vanished before being latched
        accel_terminated = False
        all_transferred_at = None
        api_failures = 0

        transfer_deadline = time.time() + self.gv_launch_timeout
        while time.time() < transfer_deadline:
            if rebalance_task.state in self._FAILED_STATES:
                self.fail(f"Rebalance failed during transfer: {rebalance_task.state}")

            live_accel = self.fusion_aws_util.list_accelerator_instances(
                self._accelerator_filter(), log="TransferCheck")
            if not live_accel:
                accel_terminated = True
            cluster_ids = {
                i.get("InstanceId") for i in self.fusion_aws_util.list_instances(
                    self.fusion_aws_util._cluster_filter(self.cluster.id),
                    log="ClusterNodes", suppress_log=True)}

            for vid in guest_vol_ids:
                if vid in deleted:
                    continue
                # Nothing further to learn once both handoff and scale-down are known.
                if vid in transferred_on and vid in iops_scaled:
                    continue
                vol = self.fusion_aws_util.ec2.get_ebs_volume_by_id(vid)
                if not vol:
                    deleted.add(vid)
                    if vid in transferred_on:
                        last_seen[vid] = (
                            f"deleted after handoff (last {last_seen.get(vid, 'n/a')})")
                    else:
                        last_seen[vid] = "deleted before being observed on a KV node"
                    continue
                atts = vol.get("Attachments") or []
                inst = atts[0].get("InstanceId") if atts else None
                iops = vol.get("Iops")
                last_iops[vid] = iops
                last_seen[vid] = (f"state={vol.get('State')} attached_to={inst} "
                                  f"iops={iops}")
                on_kv_node = (inst is not None and inst not in accel_ids
                              and inst in cluster_ids)
                if on_kv_node and vid not in transferred_on:
                    transferred_on[vid] = inst
                    self.log.info(
                        f"[transfer] {vid} handed off to KV node {inst} "
                        f"(iops={iops}, scale-down below {expected_iops} tracked "
                        f"separately)")
                # Scaled down = strictly below the download IOPS; the exact landing value
                # is not the invariant, staying at download IOPS is the defect.
                if (on_kv_node and iops is not None and iops < expected_iops
                        and vid not in iops_scaled):
                    iops_scaled[vid] = iops
                    self.log.info(
                        f"[transfer] {vid} IOPS scaled to {iops} on KV node {inst} "
                        f"(below the {expected_iops} download value)")

            # The REST view is the only signal for WHICH node ns_server thinks holds
            # volumes. Best effort — give up after 3 failures rather than warn every poll.
            if api_failures < 3:
                api_count, _ = self._log_active_guest_volumes("transfer")
                if api_count is None:
                    api_failures += 1
                    if api_failures == 3:
                        self.log.warning(
                            "[transfer] guest-volume API unreadable 3 times — stopping "
                            "in-loop API sampling for this stage")

            pending = [v for v in guest_vol_ids if v not in transferred_on]
            unscaled = [v for v in transferred_on
                        if v not in iops_scaled and v not in deleted]
            self.log.info(
                f"[transfer] on KV nodes {len(transferred_on)}/{len(guest_vol_ids)} "
                f"| iops_scaled={len(iops_scaled)} | accelerators_alive={len(live_accel)} "
                f"| pending_handoff={len(pending)} | awaiting_iops={len(unscaled)}"
                + ("".join(f"\n    pending {v}: {last_seen.get(v, 'not yet read')}"
                           for v in pending) if pending else "")
                + ("".join(f"\n    awaiting IOPS {v}: {last_seen.get(v, 'not yet read')}"
                           for v in unscaled) if unscaled else ""))

            if not pending and accel_terminated:
                # Handoff complete — the assertion this stage owns is satisfied.
                if iops_grace <= 0:
                    if unscaled:
                        self.log.info(
                            f"[transfer] all {len(transferred_on)} volume(s) on KV nodes; "
                            f"leaving now and handing the IOPS watch for "
                            f"{len(unscaled)} volume(s) to the drain stage, so the drain "
                            f"window is not spent waiting here")
                    break
                # Otherwise linger for the scale-down, bounded so a never-scaling volume
                # delays the stage by iops_grace at most.
                if all_transferred_at is None:
                    all_transferred_at = time.time()
                    if unscaled:
                        self.log.info(
                            f"[transfer] all {len(transferred_on)} volume(s) on KV nodes; "
                            f"waiting up to {iops_grace}s for the IOPS scale-down of "
                            f"{len(unscaled)} volume(s)")
                if not unscaled or (time.time() - all_transferred_at) > iops_grace:
                    break

            # Every unlatched volume is gone, so no further polling can observe the handoff.
            if pending and all(v in deleted for v in pending):
                self.fail(
                    f"Guest volume handoff is no longer observable: "
                    f"{len(transferred_on)}/{len(guest_vol_ids)} volume(s) were seen "
                    f"attached to a KV node, and the remaining {len(pending)} were "
                    f"DELETED before ever being observed there "
                    f"({', '.join(sorted(pending))}). The accelerator lifecycle finished "
                    f"faster than this stage could sample it — raise create_end so the "
                    f"download/migration windows last minutes, or lower "
                    f"transfer_poll_interval. Last state per volume:\n"
                    + "\n".join(f"  {v}: {last_seen.get(v, 'never read')}"
                                for v in sorted(pending)))
            time.sleep(poll_interval)

        # 1: every guest volume was observed attached to a KV node (independent of IOPS)
        pending = [v for v in guest_vol_ids if v not in transferred_on]
        self.assertEqual(
            len(pending), 0,
            f"{len(pending)} of {len(guest_vol_ids)} guest volume(s) were never observed "
            f"attached to a KV node within {self.gv_launch_timeout}s. Last state per "
            f"volume:\n"
            + "\n".join(f"  {v}: {last_seen.get(v, 'never read')}"
                        for v in sorted(pending)))
        self.log.info(
            f"All {len(transferred_on)} guest volume(s) handed off to KV nodes: "
            + ", ".join(f"{v}->{i}" for v, i in sorted(transferred_on.items())))

        # 1b: IOPS scale-down, reported separately from the handoff — a volume can be
        # correctly transferred and never scaled (observed: 16000 -> deleted).
        never_scaled = [v for v in guest_vol_ids if v not in iops_scaled]
        # Published for a later stage to continue the watch. Volumes already deleted can
        # never scale, so they are settled rather than watchable.
        self._iops_watch = {v: last_iops.get(v) for v in never_scaled if v not in deleted}
        self._iops_settled_unscaled = {v: last_iops.get(v) for v in never_scaled
                                       if v in deleted}
        self._iops_scaled_count = len(iops_scaled)
        self._iops_total_count = len(guest_vol_ids)

        if not never_scaled:
            self.log.info(
                f"All {len(iops_scaled)} guest volume(s) scaled below the {expected_iops} "
                f"download IOPS after handoff: "
                + ", ".join(f"{v}={i}" for v, i in sorted(iops_scaled.items())))
        elif iops_grace <= 0:
            self.log.info(
                f"IOPS verdict deferred: {len(iops_scaled)}/{len(guest_vol_ids)} volume(s) "
                f"seen below {expected_iops} IOPS so far; {len(self._iops_watch)} still live "
                f"and being handed to the drain stage to watch "
                f"({sorted(self._iops_watch)}), {len(self._iops_settled_unscaled)} already "
                f"deleted without scaling ({sorted(self._iops_settled_unscaled)})")
        else:
            self._report_iops_scale_down(
                self._iops_watch, self._iops_settled_unscaled, len(iops_scaled),
                len(guest_vol_ids), f"within {iops_grace}s of the handoff completing")

        # 2: all accelerator instances terminated
        self.assertTrue(
            accel_terminated,
            "Accelerator instances were still running throughout the transfer window — "
            "compute was never torn down after download")
        self.assertEqual(
            len(self.fusion_aws_util.list_accelerator_instances(
                self._accelerator_filter(), log="PostTransfer")), 0,
            "Accelerator instances still present after transfer")

        # 3: no guest volume attached to an instance outside this cluster. ('available'
        # volumes are ignored — the valid window between CBS releasing and CP deleting.)
        self.assertTrue(
            self.cp_monitor.verify_guest_volumes_attached_to_cluster(self.cluster),
            "Not all guest volumes are attached to cluster instances")

        # 4: guest volume API reports the guest volumes. Parity with AWS is only assertable
        # while nothing has drained; once the CP starts releasing volumes the API
        # legitimately reports fewer, so assert the weaker invariant instead of failing a
        # healthy fast rebalance. find_master() first — a fusion rebalance can replace the
        # node cluster.master pointed at.
        try:
            self.find_master(self.tenant, self.cluster)
        except Exception as e:
            self.log.warning(
                f"find_master failed before the guest-volume API read: {e}")
        api_count, _ = self._log_active_guest_volumes("post-transfer")

        if api_count is None:
            message = ("guest-volume API read failed, so API/AWS parity was not checked "
                       "(see the preceding warning for the cause)")
            if not self.soft_fail_stages:
                self.fail(message)
            self._record_issue("Stage C: guest volume API", message)
        elif deleted:
            self.assertLessEqual(
                api_count, len(guest_vol_ids),
                f"Guest volume API reports {api_count} volume(s), more than the "
                f"{len(guest_vol_ids)} AWS ever created")
            self.log.warning(
                f"Guest volume API reports {api_count} of {len(guest_vol_ids)} volume(s); "
                f"parity not asserted because {len(deleted)} volume(s) had already been "
                f"released by the CP during the transfer window")
        else:
            self.assertEqual(
                api_count, len(guest_vol_ids),
                f"Guest volume API reports {api_count} volume(s) but AWS shows "
                f"{len(guest_vol_ids)}")
            self.log.info(f"Guest volume API reports all {api_count} guest volume(s)")

        # 5: read workload succeeds — data accessible through the transfer. Skipped when a
        # later stage covers reads, since loading here delays Stage E's drain observation.
        if run_read_workload:
            self._run_read_workload("Stage C transfer")
        else:
            self.log.info(
                "Skipping the Stage C read workload — a later stage validates reads, and "
                "the guest volumes are draining while this stage runs")
        # Fall back to the latched count when the API read failed, so Stage E still has a
        # non-zero starting reference for its drain.
        return api_count if api_count else len(transferred_on)

    def _validate_background_migration(self, rebalance_task, gv_start=None,
                                       baseline_du=None):
        """Stage E (phase 8): the background copy from guest volumes to main volumes.

        Call AFTER the rebalance has completed — the extent copy only starts then, so
        monitoring earlier watches a migration that has not begun. Guest volumes outlive
        rebalance completion until their copy is done, which is what makes this observable.
        `rebalance_task` is only used to abort early on a failed cluster state.

        Tracks three independent signals until migration finishes:
          1. cbstats: ep_fusion_migration_completed_bytes climbs toward
             ep_fusion_migration_total_bytes, ep_fusion_migration_failures stays 0.
          2. Guest-volume API: count drops N -> 0 as each volume's copy finishes.
          3. Main-volume disk usage (SSM df): rises as extents are copied in.

        gv_start / baseline_du can be passed in by a caller that already measured them.
        Sample baseline_du immediately after rebalance completion — the last point at which
        the main volume holds no migrated extents; a baseline taken during the rebalance
        counts the rebalance's own writes as migration growth.
        """
        if gv_start is None:
            appear_deadline = time.time() + self.gv_launch_timeout
            gv_start = 0
            while time.time() < appear_deadline:
                if rebalance_task.state in self._FAILED_STATES:
                    self.fail(f"Rebalance failed before migration: {rebalance_task.state}")
                try:
                    gv_start = self._get_active_guest_volume_count(self.cluster)
                except Exception as e:
                    self.log.warning(f"guest-volume API read failed: {e}")
                    gv_start = 0
                if gv_start > 0:
                    break
                time.sleep(15)
        self.assertGreater(
            gv_start, 0,
            "No guest volumes reported by the guest-volume API — migration source never "
            "appeared; cannot monitor Stage E")
        self.log.info(f"Migration starting with {gv_start} guest volume(s)")

        if baseline_du is None:
            baseline_du = self._avg_main_volume_usage()
        self.log.info(f"Main-volume usage baseline: {baseline_du:.1f}%")

        # Monitor migration to completion.
        max_du = baseline_du
        failures_seen = 0
        migrated = False
        gv_now = gv_start
        mon_deadline = time.time() + self.hydration_timeout
        while time.time() < mon_deadline:
            if rebalance_task.state in self._FAILED_STATES:
                self.fail(f"Rebalance failed during migration: {rebalance_task.state}")
            completed = self._sum_migration_stat("ep_fusion_migration_completed_bytes")
            total = self._sum_migration_stat("ep_fusion_migration_total_bytes")
            failures_seen = max(
                failures_seen,
                self._sum_migration_stat("ep_fusion_migration_failures"))
            try:
                gv_now = self._get_active_guest_volume_count(self.cluster)
            except Exception as e:
                self.log.warning(f"guest-volume API read failed: {e}")
            du_now = self._avg_main_volume_usage()
            max_du = max(max_du, du_now)
            pct = (completed / total * 100) if total else 0
            self.log.info(
                f"[migration] completed={completed}/{total} ({pct:.1f}%) | "
                f"guest_volumes={gv_now}/{gv_start} | main_du={du_now:.1f}% "
                f"(baseline {baseline_du:.1f}%) | failures={failures_seen}")
            # Complete when the copy is done OR all guest volumes have been released.
            if (total > 0 and completed >= total) or gv_now == 0:
                migrated = True
                break
            time.sleep(30)

        self.assertEqual(
            failures_seen, 0,
            f"ep_fusion_migration_failures = {failures_seen} during migration")
        self.assertTrue(
            migrated,
            f"Background migration did not complete within {self.hydration_timeout}s "
            f"(last guest_volumes={gv_now})")

        # Guest-volume API must return to 0 (all copies finished / volumes released).
        final_deadline = time.time() + 300
        while time.time() < final_deadline:
            try:
                gv_now = self._get_active_guest_volume_count(self.cluster)
            except Exception:
                gv_now = -1
            if gv_now == 0:
                break
            time.sleep(15)
        self.assertEqual(
            gv_now, 0,
            f"Guest-volume API still reports {gv_now} volume(s) after migration")

        # Main-volume usage must have grown as extents were copied in.
        self.assertGreater(
            max_du, baseline_du,
            f"Main-volume usage did not increase during migration "
            f"(baseline {baseline_du:.1f}%, max {max_du:.1f}%) — copy not observed")
        self.log.info(
            f"Background migration complete: guest volumes drained to 0, main_du "
            f"{baseline_du:.1f}% -> {max_du:.1f}%, no failures")

    def _validate_teardown(self, s3_bucket_name):
        """Stage F (phase 8 teardown): full infra cleanup and data durability.

        Call only after the rebalance has completed and the cluster is back to a
        settled state. Asserts guest volumes and ASGs are deleted with no orphans,
        migration stats are clean, the cluster is healthy with fusion still enabled,
        the S3 log store survived, and reads still work.
        """
        # 1: EBS guest volumes deleted
        cleaned = self.cp_monitor.monitor_ebs_cleanup(
            self.cluster, self.stop_run_event,
            timeout=self.cp_monitor.EBS_CLEANUP_TIMEOUT)
        self.assertTrue(cleaned, "EBS guest volumes were not cleaned up after teardown")
        remaining = self.cp_monitor.get_current_guest_volume_ids(self.cluster)
        self.assertEqual(len(remaining), 0,
                         f"Guest volumes still present after teardown: {remaining}")

        # 2: no orphaned 'available' (detached-but-undeleted) guest volumes. State is a
        # volume attribute, not a tag, so it must be filtered client-side.
        available = [v for v in self._list_accelerator_volumes(guest_only=False)
                     if v.get("State") == "available"]
        self.assertEqual(
            len(available), 0,
            f"Orphaned 'available' guest volumes remain after teardown: "
            f"{[v.get('VolumeId') for v in available]}")

        # 3: all fusion ASGs deleted (check_asg_cleanup_after_rebalance only logs)
        asgs = self.fusion_aws_util.list_cluster_fusion_asg(self.cluster.id)
        self.assertEqual(len(asgs), 0,
                         f"Fusion ASGs still present after teardown: {len(asgs)}")

        # 4: all accelerator instances terminated
        accel = self.fusion_aws_util.list_accelerator_instances(
            self._accelerator_filter(), log="PostTeardown")
        self.assertEqual(len(accel), 0,
                         f"Accelerator instances still present after teardown: {len(accel)}")

        # 5: no migration failures, and all ep_fusion_migration_* stats settled to 0
        failures = self._sum_migration_stat("ep_fusion_migration_failures")
        self.assertEqual(failures, 0,
                         f"ep_fusion_migration_failures = {failures} after teardown")
        completed = self._sum_migration_stat("ep_fusion_migration_completed_bytes")
        total = self._sum_migration_stat("ep_fusion_migration_total_bytes")
        self.assertEqual(
            (completed, total), (0, 0),
            f"ep_fusion_migration_* not settled to 0 after teardown "
            f"(completed={completed}, total={total})")
        self.log.info("Migration stats clean: failures=0, completed=0, total=0")

        # 6: cluster healthy and fusion still enabled
        state = CapellaAPI.get_cluster_state(self.pod, self.tenant, self.cluster.id)
        self.assertEqual(state.lower(), "healthy",
                         f"Cluster not healthy after teardown: {state}")
        fusion_state = CapellaAPI.get_fusion_status(
            self.pod, self.tenant, self.cluster.id).get("state")
        self.assertEqual(fusion_state, "enabled",
                         f"Fusion not 'enabled' after teardown: {fusion_state}")

        # 7: S3 log store intact — teardown deletes guest EBS volumes, not S3 data
        if s3_bucket_name:
            s3_objects_after = self._get_s3_object_count_for_buckets(
                s3_bucket_name, self.cluster.buckets)
            self.log.info(f"S3 log store objects after teardown: {s3_objects_after}")
            self.assertGreater(
                s3_objects_after, 0,
                "S3 log store is empty after teardown — teardown must not delete the "
                "S3 data, only the guest EBS volumes")

        # 8: read workload succeeds — data accessible after teardown
        self._run_read_workload("Stage F teardown")
        self.log.info("Stage F validated: full cleanup, no failures, data accessible")

    # ------------------------------------------------------------------
    # Test 1: Accelerator deployment (phase 4) — merges former tests 1 and 12
    # ------------------------------------------------------------------

    def test_accelerator_deployment(self):
        """
        Validate accelerator deployment on one fusion-eligible rebalance (phase 4).

        Validates:
        - the accelerator fleet appears and stabilises, with ASGs
        - every accelerator's instance type is valid
                                             [_validate_accelerator_instance_type]

        `fusion_min_split_size_gb` applies here: a smaller shard floor splits the same data
        into more shards, so the rebalance launches more accelerators — the cheapest way to
        exercise a multi-accelerator fleet without loading proportionally more data.
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")

        # Applied before the load so the CP splits the manifest with it at rebalance time.
        split_gb, max_slots = self._apply_fusion_config_from_params()
        self._apply_fusion_sync_threshold()
        self._load_above_threshold()

        rebalance_task = self._trigger_rebalance()
        self.sleep(30, "Wait for rebalance to start before checking accelerators")

        # The whole fleet, not just the first accelerator: the instance-type assertion is
        # fleet-wide, and a partial snapshot judges a fraction of it.
        instances, asgs = self._wait_for_accelerator_fleet_stable(rebalance_task)
        self.assertGreater(
            len(instances), 0,
            "No accelerator instances found — the fusion rebalance did not launch "
            "accelerators above threshold")
        self.assertGreater(
            len(asgs), 0,
            "No ASGs found for the fusion accelerators during the rebalance")
        self.log.info(
            f"Fusion rebalance launched {len(instances)} accelerator(s) and "
            f"{len(asgs)} ASG(s) (fusion config overrides — minSplitSize: "
            f"{split_gb or 'CP default'}, maxSlots: {max_slots or 'CP default'})")

        self._validate_accelerator_instance_type(instances)

        self.wait_for_rebalances([rebalance_task])

    # ------------------------------------------------------------------
    # Test 2: Accelerators and ASGs are cleaned up after rebalance (phase 8)
    # ------------------------------------------------------------------

    def test_accelerator_termination_after_rebalance(self):
        """
        After a fusion rebalance completes, verify that all accelerator EC2 instances
        are terminated and all ASGs are deleted (8-phase lifecycle, phase 8 — Teardown).

        Validates:
        - Rebalance completes successfully
        - Accelerator instances are terminated within timeout
        - EBS guest volumes reach count = 0 within EBS_CLEANUP_TIMEOUT
        - ASGs are deleted post-rebalance
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")
        self._apply_fusion_config_from_params()
        self._apply_fusion_sync_threshold()
        self._load_above_threshold()

        rebalance_task = self._trigger_rebalance()
        self.sleep(30, "Wait for rebalance to start")

        self._poll_until_accelerators_appear(rebalance_task)
        self.wait_for_rebalances([rebalance_task])

        CapellaAPI.wait_until_done(
            self.pod, self.tenant, self.cluster.id, timeout=600)

        terminated = self.cp_monitor.monitor_fusion_accelerator_nodes_killed_after_rebalance(
            self.cluster, timeout=self.fusion_infra_timeout)
        self.assertTrue(terminated,
                        "Accelerator instances were not terminated after rebalance")

        cleaned = self.cp_monitor.monitor_ebs_cleanup(
            self.cluster, self.stop_run_event,
            timeout=self.cp_monitor.EBS_CLEANUP_TIMEOUT)
        self.assertTrue(cleaned,
                        "EBS guest volumes were not cleaned up after rebalance")

        self.cp_monitor.check_asg_cleanup_after_rebalance([self.cluster])
        self.log.info("All accelerator resources cleaned up after rebalance")

    # ------------------------------------------------------------------
    # Test 3: Full EBS guest volume lifecycle
    # ------------------------------------------------------------------

    def test_ebs_guest_volume_full_lifecycle(self):
        """
        Verify the complete EBS guest volume lifecycle during a fusion rebalance:
        created → attached to accelerators (hydration) → transferred to KV nodes → count=0.

        Validates:
        - 0 guest volumes exist before rebalance starts
        - Volumes appear and hydrate during rebalance
        - After rebalance, guest volume count returns to 0
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")
        self._apply_fusion_config_from_params()
        self._apply_fusion_sync_threshold()
        self._load_above_threshold()

        pre_volumes = self.cp_monitor.get_current_guest_volume_ids(self.cluster)
        self.assertEqual(len(pre_volumes), 0,
                         f"Expected 0 guest volumes before rebalance, "
                         f"found {len(pre_volumes)}")

        fusion_rebalances = []
        rebalance_task = self._trigger_rebalance()
        self.sleep(30, "Wait for rebalance to start")

        hydration_completed = self.cp_monitor.monitor_fusion_guest_volumes(
            self.tenant, self.cluster, rebalance_task,
            self.fusion_monitor, fusion_rebalances,
            wait_for_hydration_complete=True,
            timeout=self.gv_launch_timeout,
            find_master_func=self.find_master,
        )
        self.assertTrue(hydration_completed,
                        "EBS guest volume hydration did not complete successfully")

        self.wait_for_rebalances([rebalance_task])
        CapellaAPI.wait_until_done(
            self.pod, self.tenant, self.cluster.id, timeout=600)

        cleaned = self.cp_monitor.monitor_ebs_cleanup(
            self.cluster, self.stop_run_event,
            timeout=self.cp_monitor.EBS_CLEANUP_TIMEOUT)
        self.assertTrue(cleaned,
                        "EBS guest volumes were not cleaned up after rebalance")

        post_volumes = self.cp_monitor.get_current_guest_volume_ids(self.cluster)
        self.assertEqual(
            len(post_volumes), 0,
            f"Expected 0 guest volumes after rebalance, "
            f"found {len(post_volumes)}: {post_volumes}")
        self.log.info("EBS guest volume full lifecycle validated successfully")

    # ------------------------------------------------------------------
    # Test 4: Back-to-back rebalances leave no orphaned volumes
    # ------------------------------------------------------------------

    def test_back_to_back_rebalances_no_orphaned_volumes(self):
        """
        Run two consecutive fusion-eligible rebalances and verify that no EBS guest
        volumes are orphaned between or after runs. Run 1 uses the configured
        rebalance type; run 2 uses its inverse so the topology stays valid and returns
        toward baseline (in<->out; swap toggles compute back) for any rebalance_type.

        Validates:
        - First rebalance completes with 0 guest volumes remaining
        - Second rebalance starts cleanly (0 pre-existing guest volumes)
        - Second rebalance completes with 0 guest volumes remaining
        - No ASGs from the first rebalance linger into the second

        See test_guest_volume_accumulation_with_migration_paused (test 20) for the
        counterpart: the same two rebalances with migration frozen, where volumes are
        expected to accumulate rather than reach 0 between runs.
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")
        self._apply_fusion_config_from_params()
        self._apply_fusion_sync_threshold()
        self._load_above_threshold()

        run_types = [self._default_rebalance_type(), self._inverse_rebalance_type()]
        for run in range(1, 3):
            run_type = run_types[run - 1]
            self.log.info(f"--- Starting rebalance run {run} of 2 (type={run_type}) ---")

            pre_run_volumes = self.cp_monitor.get_current_guest_volume_ids(self.cluster)
            self.assertEqual(
                len(pre_run_volumes), 0,
                f"Run {run}: expected 0 guest volumes before rebalance, "
                f"found {len(pre_run_volumes)}: {pre_run_volumes}")

            rebalance_task = self._trigger_rebalance(run_type)
            self.sleep(30, f"Wait for rebalance run {run} to start")

            self._poll_until_accelerators_appear(rebalance_task)
            self.wait_for_rebalances([rebalance_task])
            CapellaAPI.wait_until_done(
                self.pod, self.tenant, self.cluster.id, timeout=600)

            cleaned = self.cp_monitor.monitor_ebs_cleanup(
                self.cluster, self.stop_run_event,
                timeout=self.cp_monitor.EBS_CLEANUP_TIMEOUT)
            self.assertTrue(cleaned,
                            f"Orphaned EBS guest volumes remain after rebalance run {run}")

            post_run_volumes = self.cp_monitor.get_current_guest_volume_ids(self.cluster)
            self.assertEqual(
                len(post_run_volumes), 0,
                f"Run {run}: expected 0 guest volumes after rebalance, "
                f"found {len(post_run_volumes)}: {post_run_volumes}")
            self.log.info(f"Run {run}: guest volume cleanup confirmed — no orphaned volumes")

        self.log.info("Back-to-back rebalances validated — no orphaned volumes in either run")

    # ------------------------------------------------------------------
    # Test 5 ("fusion state stays 'enabled'") now runs as a background watcher
    # for the whole of test_fusion_scaling_lifecycle — see the module docstring.
    # ------------------------------------------------------------------


    # ------------------------------------------------------------------
    # Test 6: Accelerator instance count matches data size
    # ------------------------------------------------------------------

    def test_accelerator_instance_count_matches_data_size(self):
        """
        Verify that the accelerator count scales with data size: below the fusion threshold
        0 accelerators (DCP path), above it at least expected_accelerator_count.

        The one test here that deliberately drives TWO rebalances, since the
        below-threshold case can only be shown by rebalancing with a small data set. The
        second uses the inverse rebalance type so the topology stays valid; accelerator
        count depends on data size, not direction, so the assertions hold either way.

        `fusion_min_split_size_gb` applies only to the above-threshold phase — minSplitSize
        splits the manifest after the fusion path has been chosen, and that choice is made
        by `fusion_threshold_gib`.

        Parameters:
        - small_create_end: document count for the below-threshold load (default 100)
        - expected_accelerator_count: minimum above threshold (default 1)
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")

        # Applied before either load so both phases run under the same shard config.
        split_gb, max_slots = self._apply_fusion_config_from_params()
        self._apply_fusion_sync_threshold()

        small_end = self.input.param("small_create_end", 100)
        expected_count = self.input.param("expected_accelerator_count", 1)
        threshold_gib = self.input.param("fusion_threshold_gib", None)

        # Phase A: below-threshold load — expect 0 accelerators (DCP rebalance)
        self._load_data(self.cluster, create_start=0, create_end=small_end)
        small_task = self._trigger_rebalance()
        self.sleep(30, "Wait for small rebalance to start")

        deadline = time.time() + 600
        while time.time() < deadline:
            if small_task.state in (self._FAILED_STATES | {"healthy"}):
                break
            time.sleep(5)

        instances_small = self.fusion_aws_util.list_accelerator_instances(
            self._accelerator_filter(), log="BelowThreshold")
        self.assertEqual(
            len(instances_small), 0,
            f"Expected 0 accelerators for below-threshold data ({small_end} docs, "
            f"fusion_threshold_gib={threshold_gib}), found {len(instances_small)} — the "
            f"rebalance took the fusion path where it should have used DCP")
        self.wait_for_rebalances([small_task])
        self.log.info("Below-threshold rebalance used DCP — 0 accelerators confirmed")

        # Phase B: above-threshold load — expect >= expected_count accelerators.
        # create_start=small_end so this pass does not re-create Phase A's documents;
        # inverse rebalance type so the topology stays valid after Phase A.
        self._load_above_threshold(create_start=small_end)
        large_task = self._trigger_rebalance(self._inverse_rebalance_type())
        self.sleep(30, "Wait for large rebalance to start")

        instances_large = self._poll_until_accelerators_appear(large_task)
        self.assertGreaterEqual(
            len(instances_large), expected_count,
            f"Expected >= {expected_count} accelerators for above-threshold data, "
            f"found {len(instances_large)}")
        self.log.info(
            f"Above-threshold rebalance launched {len(instances_large)} accelerator(s) "
            f"(expected >= {expected_count}; fusion config overrides — minSplitSize: "
            f"{split_gb or 'CP default'}, maxSlots: {max_slots or 'CP default'})")
        self.wait_for_rebalances([large_task])

    # ------------------------------------------------------------------
    # Test 7: No public IP on accelerator nodes
    # ------------------------------------------------------------------

    def test_no_public_ip_on_accelerator_nodes(self):
        """
        Verify that accelerator EC2 instances do not have a public IP address.
        Accelerators must run in private subnets only — no public exposure.

        Validates:
        - Accelerator instances are launched during a fusion rebalance
        - None of the accelerator instances have a PublicIpAddress in AWS
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")
        self._apply_fusion_config_from_params()
        self._apply_fusion_sync_threshold()
        self._load_above_threshold()

        rebalance_task = self._trigger_rebalance()
        self.sleep(30, "Wait for rebalance to start")

        deadline = time.time() + 1800
        accelerators_seen = False
        instances_with_public_ip = []

        while time.time() < deadline:
            if rebalance_task.state in self._FAILED_STATES:
                self.fail(f"Rebalance failed: {rebalance_task.state}")

            raw_instances = self.fusion_aws_util.list_instances(
                self._accelerator_filter(), log="PublicIPCheck")
            if raw_instances:
                accelerators_seen = True
                instances_with_public_ip = [
                    inst for inst in raw_instances
                    if inst.get("PublicIpAddress") not in (None, "", "N/A")
                ]
                # Check immediately; if any violation found, stop early
                if instances_with_public_ip:
                    break

            if rebalance_task.state == "healthy":
                break
            time.sleep(10)

        self.assertTrue(
            accelerators_seen,
            "No accelerator instances appeared during rebalance — "
            "cannot validate public IP absence")
        self.assertEqual(
            len(instances_with_public_ip), 0,
            f"Security violation: {len(instances_with_public_ip)} accelerator instance(s) "
            f"have public IPs: "
            f"{[i.get('InstanceId') for i in instances_with_public_ip]}")
        self.log.info("Confirmed: no accelerator instance has a public IP address")

        self.wait_for_rebalances([rebalance_task])

    # ------------------------------------------------------------------
    # Test 8: Guest volume storage properties, encryption, tags, and AZ
    # ------------------------------------------------------------------

    def test_guest_volume_properties(self):
        """
        During a fusion rebalance, inspect every EBS guest volume via the AWS API
        and assert that all required properties are correct:

        Storage class:
          - VolumeType = gp3
          - Iops       = expected_iops  (default 16 000)
          - Throughput = expected_throughput MB/s (default 2 000)

        Encryption:
          - Encrypted  = True
          - KmsKeyId   is a non-empty string

        Tags (required for CP tag-based lifecycle management):
          - couchbase-cloud-function   = fusion-accelerator
          - couchbase-cloud-cluster-id = <cluster.id>

        AZ colocation:
          - Each volume's AvailabilityZone must match one of the KV node AZs. EBS volumes
            only attach within an AZ, so a mismatch is a hard attach failure in phase 6.
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")
        self._apply_fusion_config_from_params()
        self._apply_fusion_sync_threshold()

        # Before the rebalance no accelerators exist yet, so every cluster instance is a
        # CBS/KV node and phase-4 accelerators cannot be mixed into the AZ set.
        pre_rebalance_instances = self.fusion_aws_util.list_instances(
            self.fusion_aws_util._cluster_filter(self.cluster.id),
            log="KVNodesAZ")
        kv_azs = {
            inst.get("Placement", {}).get("AvailabilityZone")
            for inst in pre_rebalance_instances
        } - {None}
        self.assertGreater(len(kv_azs), 0,
                           "Could not determine KV node AZs before rebalance")
        self.log.info(f"KV node AZs (pre-rebalance): {kv_azs}")

        self._load_above_threshold()
        rebalance_task = self._trigger_rebalance()
        self.sleep(30, "Wait for rebalance to start before polling for guest volumes")

        expected_iops = self.input.param("expected_iops", 16000)
        expected_throughput = self.input.param("expected_throughput", 2000)

        # Poll until at least one guest volume appears in AWS
        volumes = []
        deadline = time.time() + 1800
        while time.time() < deadline:
            if rebalance_task.state in self._FAILED_STATES:
                self.fail(f"Rebalance failed before guest volumes appeared: "
                          f"{rebalance_task.state}")
            volumes = self.fusion_aws_util.ec2.list_volumes_by_cluster_id(filters={
                "couchbase-cloud-cluster-id": self.cluster.id,
                "couchbase-cloud-function": "fusion-accelerator",
            })
            if volumes:
                self.log.info(
                    f"Found {len(volumes)} guest volume(s) — inspecting properties")
                break
            if rebalance_task.state == "healthy":
                break
            time.sleep(10)

        self.assertGreater(
            len(volumes), 0,
            "No EBS guest volumes appeared during the fusion rebalance — "
            "cannot validate properties")

        failures = []
        for vol in volumes:
            vol_id = vol.get("VolumeId", "unknown")
            tag_dict = {t["Key"]: t["Value"] for t in vol.get("Tags", [])}

            checks = [
                # (description, actual, expected, assert_type)
                ("VolumeType",
                 vol.get("VolumeType"), "gp3", "eq"),
                ("Iops",
                 vol.get("Iops"), expected_iops, "eq"),
                ("Throughput",
                 vol.get("Throughput"), expected_throughput, "eq"),
                ("Encrypted",
                 vol.get("Encrypted"), True, "eq"),
                ("KmsKeyId non-empty",
                 bool(vol.get("KmsKeyId")), True, "eq"),
                ("tag:couchbase-cloud-function",
                 tag_dict.get("couchbase-cloud-function"), "fusion-accelerator", "eq"),
                ("tag:couchbase-cloud-cluster-id",
                 tag_dict.get("couchbase-cloud-cluster-id"), self.cluster.id, "eq"),
                ("AvailabilityZone in KV AZs",
                 vol.get("AvailabilityZone"), kv_azs, "in"),
            ]

            for desc, actual, expected, assert_type in checks:
                ok = (actual == expected) if assert_type == "eq" else (actual in expected)
                status = "OK" if ok else "FAIL"
                self.log.info(f"  [{status}] {vol_id} {desc}: {actual!r}")
                if not ok:
                    failures.append(
                        f"{vol_id} — {desc}: got {actual!r}, expected {expected!r}")

        self.assertEqual(
            len(failures), 0,
            "Guest volume property violations:\n" + "\n".join(failures))
        self.log.info(
            f"All {len(volumes)} guest volume(s) passed property validation")

        self.wait_for_rebalances([rebalance_task])

    # ------------------------------------------------------------------
    # Test 9: Guest volume size scales with data size
    # ------------------------------------------------------------------

    def test_guest_volume_size_scales_with_data(self):
        """
        Verify that guest-volume provisioning scales with the amount of data, across two
        rebalances at different data volumes.

        Read the sizing rule before changing this test. The CP creates
        `min(ceil(hostData / minSplitSize), maxSlots)` shards per host, one volume per
        shard, each `ceil(shardStorageSize/1GB) + 10%` with a floor. So with the defaults
        extra data buys MORE volumes at the floor, and per-volume size does not budge until
        a host needs more than 22 x 50 GB = 1.1 TB — a naive
        "max(size_B) > max(size_A)" fails against correct behaviour at any practical size.

        So each dimension is asserted only where it holds:
          * total provisioned capacity must always grow with the data, whether the CP
            delivered that as more volumes, bigger ones, or both;
          * per-volume size only when `fusion_max_slots` pins the shard count so size is
            the only dimension left (the conf pins it to 1). Without the pin the size check
            is logged as skipped rather than asserted.

        Phase A loads `small_create_end` docs, Phase B tops up to `create_end`, each
        followed by a rebalance. Volumes are read while the fleet is up, and each phase
        waits for cleanup to 0 so the two samples are disjoint.

        Sizing, and it bites twice. Phase A must be big enough to (a) take the fusion path
        and (b) clear the volume floor, or the comparison has nothing to show.
          (a) Neither ep_fusion_log_store_data_size nor the S3 object count is a usable
              proxy for the CP's fusion-vs-DCP decision — both sat at ~0.21 GiB / ~178
              objects in a run where some rebalances took the fusion path and others did
              not. Empirically, on 3 nodes with 2 collections and replicas=1,
              create_end=10000000 launched accelerators and 5000000 did not.
          (b) The floor is minVolumeSize = 50 GB, NOT minSplitSize: a run with
              minSplitSize=5 GB still produced 50 GiB volumes in both phases, so the size
              comparison failed 50 vs 50. Per-volume size cannot move until per-host data
              exceeds 50/1.1 ~= 45.5 GiB, which is reachable with the main conf's data
              volumes but not the low-data one — hence that entry leaves fusion_max_slots
              unset and relies on count/total growth instead.
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")

        small_end = self.input.param("small_create_end", 1_000_000)
        large_end = self.input.param("create_end", 20_000_000)
        max_slots = self.input.param("fusion_max_slots", None)
        split_gb = self.input.param("fusion_min_split_size_gb", None)
        self.assertGreater(
            large_end, small_end,
            f"create_end ({large_end}) must exceed small_create_end ({small_end}) for "
            f"this test to compare two different data volumes")

        split_gb, max_slots = self._apply_fusion_config_from_params()
        self._apply_fusion_sync_threshold()
        floor_gb = split_gb or 50

        # Both phases use the SAME direction, deliberately. The inverse (as the other
        # two-rebalance tests use) changes the NODE COUNT between measurements, and volume
        # count tracks node count — an in-then-out pair measured 4 volumes then 3, so total
        # capacity "fell" 200 -> 150 GiB purely because a node left.
        run_type = self._default_rebalance_type()
        phases = [("A", small_end, run_type), ("B", large_end, run_type)]
        sizes = {}
        loaded_upto = 0

        for label, target_docs, run_type in phases:
            self.log.info(
                f"--- Phase {label}: loading up to {target_docs} docs, then a "
                f"'{run_type}' rebalance ---")
            phase_load_start = time.time()
            self._load_data(self.cluster, create_start=loaded_upto,
                            create_end=target_docs)
            loaded_upto = target_docs
            self._wait_for_log_store_sync(time.time() - phase_load_start)
            self._log_cluster_data_summary(f"Phase {label}: after load")

            rebalance_task = self._trigger_rebalance(run_type)
            self.sleep(30, f"Phase {label}: wait for the rebalance to start")

            # Read sizes while the fleet is up: sizes are fixed at creation, and reading
            # here avoids racing the per-shard deletion that follows migration.
            self._wait_for_accelerator_fleet_stable(rebalance_task)
            volumes = self._poll_until_accelerator_volumes_appear(rebalance_task)
            self.assertGreater(
                len(volumes), 0,
                f"Phase {label}: no guest volumes appeared, so there is nothing to "
                f"measure")
            phase_sizes = sorted(v.get("Size") or 0 for v in volumes)
            sizes[label] = phase_sizes
            self.log.info(
                f"Phase {label}: {len(phase_sizes)} guest volume(s), sizes={phase_sizes} "
                f"GiB, total={sum(phase_sizes)} GiB, max={max(phase_sizes)} GiB")

            undersized = [s for s in phase_sizes if s < floor_gb]
            self.assertEqual(
                undersized, [],
                f"Phase {label}: {len(undersized)} volume(s) below the {floor_gb} GiB "
                f"minSplitSize floor: {undersized}")

            self.wait_for_rebalances([rebalance_task])
            CapellaAPI.wait_until_done(
                self.pod, self.tenant, self.cluster.id, timeout=600)

            # Leave a clean slate so the next phase measures only its own volumes.
            self.assertTrue(
                self.cp_monitor.monitor_ebs_cleanup(
                    self.cluster, self.stop_run_event,
                    timeout=self.cp_monitor.EBS_CLEANUP_TIMEOUT),
                f"Phase {label}: guest volumes were not cleaned up, so Phase B would "
                f"measure a mix of both phases' volumes")

        small_sizes, large_sizes = sizes["A"], sizes["B"]
        self.log.info(
            f"Phase A ({small_end} docs): {len(small_sizes)} volume(s), "
            f"total {sum(small_sizes)} GiB, max {max(small_sizes)} GiB\n"
            f"Phase B ({large_end} docs): {len(large_sizes)} volume(s), "
            f"total {sum(large_sizes)} GiB, max {max(large_sizes)} GiB")

        # Always-true invariant: more data => more provisioned guest-volume capacity.
        self.assertGreater(
            sum(large_sizes), sum(small_sizes),
            f"Total provisioned guest-volume capacity did not grow with the data: "
            f"{sum(small_sizes)} GiB for {small_end} docs vs {sum(large_sizes)} GiB for "
            f"{large_end} docs (counts {len(small_sizes)} -> {len(large_sizes)}, "
            f"max size {max(small_sizes)} -> {max(large_sizes)} GiB). If the count fell, "
            f"check that both phases rebalanced in the same direction — volume count "
            f"tracks node count, so an in-then-out pair shrinks the fleet regardless of "
            f"data. If the count grew but max size is pinned at 50 GiB in both phases, "
            f"that is the minVolumeSize floor and only more per-host data can move it.")
        self.log.info(
            f"Provisioned capacity grew {sum(small_sizes)} -> {sum(large_sizes)} GiB "
            f"(volume count {len(small_sizes)} -> {len(large_sizes)})")

        # Per-volume size only has to grow when the shard count is pinned.
        if max_slots:
            self.assertGreater(
                max(large_sizes), max(small_sizes),
                f"With maxSlots pinned to {max_slots}, per-volume size is the only way "
                f"to absorb more data, but the largest volume did not grow: "
                f"{max(small_sizes)} GiB for {small_end} docs vs {max(large_sizes)} GiB "
                f"for {large_end} docs. If both are exactly the {floor_gb} GiB floor, "
                f"Phase A's data was too small to clear it — raise small_create_end.")
            self.log.info(
                f"With maxSlots={max_slots}, largest guest volume grew "
                f"{max(small_sizes)} -> {max(large_sizes)} GiB")
        else:
            self.log.info(
                f"fusion_max_slots not set — per-volume size is expected to stay at the "
                f"{floor_gb} GiB floor while the CP adds volumes instead "
                f"({len(small_sizes)} -> {len(large_sizes)}), so only total capacity is "
                f"asserted. Pin fusion_max_slots (the conf uses 1) to assert size growth.")

    # ------------------------------------------------------------------
    # Test 10: ASGs deleted within 5 minutes of rebalance completion
    # ------------------------------------------------------------------

    def test_asg_deleted_after_rebalance_within_5_mins(self):
        """
        After a fusion rebalance completes, verify the CP deletes every accelerator ASG
        within 5 minutes.

        The SLA clock starts the moment the CBS rebalance task reports 'healthy' — before
        wait_until_done — to measure the true end-to-end cleanup latency from completion.

        Validates:
        - at least one ASG exists during the rebalance (the fusion path was taken)
        - list_cluster_fusion_asg returns 0 within 300s of completion
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")
        self._apply_fusion_config_from_params()
        self._apply_fusion_sync_threshold()
        self._load_above_threshold()

        rebalance_task = self._trigger_rebalance()
        self.sleep(30, "Wait for rebalance to start before checking for ASGs")

        # Confirm at least one ASG was created (fusion path was taken)
        asgs_seen = self._poll_until_accelerators_appear(rebalance_task)
        self.assertGreater(
            len(asgs_seen), 0,
            "No accelerator instances appeared — fusion rebalance may not have "
            "triggered; cannot validate ASG deletion SLA")

        mid_asgs = self.fusion_aws_util.list_cluster_fusion_asg(self.cluster.id)
        self.assertGreater(
            len(mid_asgs), 0,
            "No ASGs found during rebalance despite accelerator instances being present")
        self.log.info(f"ASGs present during rebalance: {len(mid_asgs)}")

        # Wait for CBS to report rebalance complete, then start the 5-minute clock
        self.wait_for_rebalances([rebalance_task])
        sla_start = time.time()
        sla_deadline = sla_start + 300
        self.log.info(
            f"CBS rebalance complete — polling for ASG deletion within 300s")

        asgs_deleted = False
        while time.time() < sla_deadline:
            remaining_asgs = self.fusion_aws_util.list_cluster_fusion_asg(
                self.cluster.id)
            if not remaining_asgs:
                asgs_deleted = True
                elapsed = time.time() - sla_start
                self.log.info(
                    f"All ASGs deleted {elapsed:.1f}s after rebalance completion "
                    f"(SLA: 300s)")
                break
            self.log.info(
                f"ASGs still present: {len(remaining_asgs)} — "
                f"{int(sla_deadline - time.time())}s remaining in SLA window")
            time.sleep(15)

        elapsed = time.time() - sla_start
        self.assertTrue(
            asgs_deleted,
            f"Fusion accelerator ASGs were not deleted within 300s of rebalance "
            f"completion (elapsed: {elapsed:.1f}s, "
            f"remaining: {len(self.fusion_aws_util.list_cluster_fusion_asg(self.cluster.id))} ASGs)")


    # ------------------------------------------------------------------
    # Test 12 ("accelerators are of a specific EC2 instance type") now runs
    # inside test_accelerator_deployment, against its stable-fleet snapshot.
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # Test 13: One ASG per accelerator, each with DesiredCapacity == 1
    # ------------------------------------------------------------------

    def test_asg_desired_capacity_one_per_shard(self):
        """
        Stage A / phase 4: one ASG per shard/accelerator, each with DesiredCapacity == 1
        and exactly one InService instance.

        Validates (see _validate_asg_topology):
        - every fusion ASG has DesiredCapacity == 1 and MaxSize == 1
        - every fusion ASG has exactly one InService instance
        - #ASGs == #accelerator instances
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")
        self._apply_fusion_config_from_params()
        self._apply_fusion_sync_threshold()
        self._load_above_threshold()

        rebalance_task = self._trigger_rebalance()
        self.sleep(30, "Wait for rebalance to start before checking ASGs")

        instances, asgs = self._wait_for_accelerator_fleet_stable(rebalance_task)
        self.assertGreater(
            len(instances), 0,
            "No accelerator instances appeared — cannot validate ASG topology")

        self._validate_asg_topology(rebalance_task, instances, asgs)

        self.wait_for_rebalances([rebalance_task])

    # ------------------------------------------------------------------
    # Test 14: Accelerator EBS volume minimum size and IOPS
    # ------------------------------------------------------------------

    def test_accelerator_ebs_volume_min_size(self):
        """
        Phase 3/4: every accelerator guest EBS volume meets the design minimums.

        Validates (see _validate_accelerator_volume_specs):
        - every volume Size >= 50 GB (or `min_volume_size_gb`)
        - every volume Iops == 16000 (or `expected_iops`)
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")
        self._apply_fusion_config_from_params()
        self._apply_fusion_sync_threshold()
        self._load_above_threshold()

        rebalance_task = self._trigger_rebalance()
        self.sleep(30, "Wait for rebalance to start before polling for volumes")

        # Gate on the full fleet first: a mid-launch sample sees only the volumes of the
        # accelerators that happen to be up, and passes on a fraction of the fleet.
        self._wait_for_accelerator_fleet_stable(rebalance_task)
        self._validate_accelerator_volume_specs(rebalance_task)

        self.wait_for_rebalances([rebalance_task])

    # ------------------------------------------------------------------
    # Test 15 (CloudWatch download-progress monitoring) was removed.
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # Test 16: Guest volume transfer from accelerators to cluster nodes (Stage C)
    # ------------------------------------------------------------------

    def test_guest_volume_transfer_to_cluster(self):
        """
        Stage C (phase 6): validate the volume handoff from accelerators to KV nodes.

        Validates (see _validate_guest_volume_transfer):
          - all accelerator EC2 instances are terminated
          - every guest volume that was on an accelerator is now attached to a KV node
          - the fusion guest-volume API reports all guest volumes
          - a read workload succeeds — data is accessible through the transfer
          - the IOPS scale-down is tracked and reported separately

        Background migration is FROZEN across the rebalance and resumed at the end, so the
        handoff, the IOPS scale-down and the read workload are all observed against a stable
        full set of volumes instead of racing the CP's per-shard deletion.

        Best-effort: a failure to set the rate limit is recorded and the validation falls
        back to the time-critical mode (no IOPS grace period, no read workload).

        Run with enough data (create_end) that the migration window lasts long enough to
        observe.
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")
        self._apply_fusion_config_from_params()
        self._apply_fusion_sync_threshold()
        self._load_above_threshold()

        # Freeze before triggering: migration starts at rebalance completion and releases
        # volumes per shard, which is exactly what this test needs to still be there.
        migration_paused = self._pause_migration()

        rebalance_task = self._trigger_rebalance()
        self.sleep(30, "Wait for rebalance to start")

        accel_instances = self._poll_until_accelerators_appear(rebalance_task)
        self.assertGreater(
            len(accel_instances), 0,
            "No accelerator instances appeared — cannot validate transfer")
        accel_ids = {i["InstanceId"] for i in accel_instances}
        self.log.info(f"Accelerators during download: {accel_ids}")

        guest_vol_ids = self._capture_guest_volume_ids(rebalance_task)

        # Let the rebalance finish first: by then the accelerators are torn down and the
        # volumes have landed on their target KV nodes, which is the end state asserted.
        # Safe to wait — with migration frozen the slow wait_for_rebalances() path cannot
        # cost us any volumes.
        self.wait_for_rebalances([rebalance_task])
        CapellaAPI.wait_until_done(self.pod, self.tenant, self.cluster.id, timeout=600)

        try:
            # Frozen: wait out the async IOPS scale-down and run the read workload, which
            # here proves the data is readable while it lives only on the guest volumes.
            self._validate_guest_volume_transfer(
                rebalance_task, accel_ids, guest_vol_ids,
                iops_grace=None if migration_paused else 0,
                run_read_workload=migration_paused)
        finally:
            # Idempotent and never raises, so it cannot mask a validation failure.
            self._restore_migration_rate_limit()

    # ------------------------------------------------------------------
    # Test 17: Background (extent) migration progress (Stage E)
    # ------------------------------------------------------------------

    def test_background_migration_progress(self):
        """
        Stage E (phase 8): monitor the background copy of data from the guest EBS volumes
        into each KV node's own persistent (main) EBS volume, and assert it completes.

        Validates (see _validate_background_migration):
          - at least one guest volume was reported before migration (N > 0)
          - ep_fusion_migration_failures == 0 throughout
          - migration reaches completion (completed_bytes == total_bytes) AND/OR the
            guest-volume API count returns to 0
          - main-volume usage increased over its pre-migration baseline

        The background copy starts only AFTER the rebalance completes, so this waits for
        completion before monitoring; the accelerator check during the rebalance is just
        evidence that the fusion path was taken.

        Background migration is FROZEN across the rebalance and resumed immediately before
        monitoring starts. That is what makes this deterministic: migration begins at a known
        instant from the complete set of guest volumes, so the monitor sees the whole N -> 0
        drain instead of joining a copy already in progress. It also makes the disk baseline
        exact — nothing has been copied into the main volume when it is sampled.

        Best-effort: if the rate limit cannot be set the failure is recorded and the test
        still runs, with migration already under way when monitoring starts.

        Run with enough data that migration lasts long enough to observe, and size kv_disk
        so the copied data is a visible fraction of the main volume.
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")
        self._apply_fusion_config_from_params()
        self._apply_fusion_sync_threshold()
        self._load_above_threshold()

        # Freeze before triggering, so no extent is copied while the rebalance runs.
        migration_paused = self._pause_migration()

        try:
            rebalance_task = self._trigger_rebalance()
            self.sleep(30, "Wait for rebalance to start")

            # Confirm the fusion path was taken before waiting.
            self._poll_until_accelerators_appear(rebalance_task)

            self.wait_for_rebalances([rebalance_task])
            CapellaAPI.wait_until_done(
                self.pod, self.tenant, self.cluster.id, timeout=600)

            # With migration frozen this is exactly the pre-migration state, however long
            # the rebalance took.
            baseline_du = self._avg_main_volume_usage()
            self.log.info(
                f"Pre-migration main-volume usage baseline: {baseline_du:.1f}% at "
                f"{self._data_path()}"
                + ("" if migration_paused else
                   " (migration was NOT frozen, so some copying may already be included)"))

            # Migration starts here, at a known instant and from the full set of volumes.
            if migration_paused:
                self._resume_migration()

            self._validate_background_migration(rebalance_task, baseline_du=baseline_du)
        finally:
            # No-op when the resume above already ran; the safety net for paths that did not.
            self._restore_migration_rate_limit()

    # ------------------------------------------------------------------
    # Test 18: Teardown after rebalance (Stage F)
    # ------------------------------------------------------------------

    def test_teardown_after_rebalance(self):
        """
        Stage F (phase 8 teardown): validate full infrastructure cleanup and data
        durability after a fusion rebalance completes.

        Validates (see _validate_teardown):
          - all EBS guest volumes are deleted (count -> 0)
          - no orphaned 'available' guest volumes linger (teardown deletes, not just detaches)
          - all fusion ASGs are deleted
          - all accelerator EC2 instances are terminated
          - no migration failures occurred and all ep_fusion_migration_* stats are 0
          - the cluster is healthy and fusion is still 'enabled'
          - the S3 log store is intact (teardown deletes guest EBS volumes, not S3 data)
          - a read workload succeeds — data is accessible after teardown

        Background migration is FROZEN across the rebalance and resumed just before the
        teardown check. Every assertion in _validate_teardown is of the form "none of X
        remains", which passes trivially if X never existed or vanished before the test
        looked; the freeze lets this test first establish that N guest volumes really were
        there, turning "nothing is present" into "N were present and now nothing is".

        Best-effort: if the rate limit cannot be set the failure is recorded and the teardown
        assertions still run, but without that non-vacuity guarantee.
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")
        self._apply_fusion_config_from_params()
        self._apply_fusion_sync_threshold()

        s3_bucket_name, _ = self._capture_s3_log_store_baseline()

        self._load_above_threshold()

        # Freeze before triggering, so the guest volumes are still there to be counted
        # after the rebalance completes.
        migration_paused = self._pause_migration()

        try:
            rebalance_task = self._trigger_rebalance()
            self.sleep(30, "Wait for rebalance to start")
            self._poll_until_accelerators_appear(rebalance_task)

            self.wait_for_rebalances([rebalance_task])
            CapellaAPI.wait_until_done(
                self.pod, self.tenant, self.cluster.id, timeout=600)

            # Non-vacuity: prove there is something for teardown to destroy.
            if migration_paused:
                pre_teardown = self._list_accelerator_volumes(guest_only=True)
                self.assertGreater(
                    len(pre_teardown), 0,
                    "No guest volumes present after the rebalance even with migration "
                    "frozen — the teardown assertions below would pass vacuously, since "
                    "there would be nothing left to delete")
                self.log.info(
                    f"{len(pre_teardown)} guest volume(s) present before resuming "
                    f"migration; teardown must destroy all of them: "
                    f"{sorted(v.get('VolumeId') for v in pre_teardown)}")
                self._resume_migration()

            # _validate_teardown waits for the EBS cleanup first, so it covers the drain
            # that the resume above sets off.
            self._validate_teardown(s3_bucket_name)
        finally:
            self._restore_migration_rate_limit()

    # ------------------------------------------------------------------
    # Test 19: End-to-end fusion scaling lifecycle (Stages A -> F)
    # ------------------------------------------------------------------

    def test_fusion_scaling_lifecycle(self):
        """
        End-to-end fusion scaling lifecycle: drive ONE fusion rebalance and validate every
        stage of the accelerator lifecycle as it happens, in order.

        The combined form of tests 12-18, which each drive their own rebalance. Observing
        the stages within a single rebalance also catches ordering/handoff problems the
        per-stage tests cannot see — volumes that transfer correctly but never drain, or a
        teardown that succeeds only because migration never ran.

        Stage order:

          Stage 0 — freeze background migration BEFORE the rebalance is triggered, so no
            guest volume is deleted while Stages A-C observe them        [_pause_migration]
          Stage A — accelerators launched (>= 1), homogeneous instance type present in the
            ASG override list, one ASG per accelerator at DesiredCapacity 1, every guest
            volume >= 50 GB at 16000 IOPS   [_validate_accelerator_instance_type,
                                             _validate_asg_topology,
                                             _validate_accelerator_volume_specs]
          Stage B — guest volume IDs captured while still on the accelerators at 16000 IOPS
                                                             [_capture_guest_volume_ids]

          --- rebalance completes here; every stage below runs post-completion ---

          Stage B2 — main-volume disk baseline, exact because migration is frozen
          Stage C — accelerators terminated, every captured volume observed ATTACHED to a KV
            node, guest-volume API agrees, read workload succeeds (with migration frozen the
            data can only be served from the guest volumes, so this proves they are readable
            in place). IOPS scale-down is its own issue, not a handoff failure
                                                     [_validate_guest_volume_transfer]
          Stage D — resume migration, once every slow call is behind us  [_resume_migration]
          Stage E — guest volume count falls monotonically to 0 while main-volume usage rises
                                                        [_validate_guest_volume_drain]
          Stage F — volumes and ASGs deleted with no orphans, migration stats settled, cluster
            healthy with fusion enabled, S3 intact, reads succeed      [_validate_teardown]

        Honours `rebalance_type` (in / out / swap). `fusion_min_split_size_gb` lowers the
        shard floor, giving more accelerators and longer download/migration windows for the
        same create_end — the cheapest way to make the stages observable.

        Soft-fail (default): a failing stage is recorded and the run continues, so ONE
        rebalance produces observations for the whole lifecycle; everything is reported
        together as a single numbered failure at the end. Stages whose inputs are missing
        because an earlier stage failed are recorded as skipped rather than passing silently.

        Sizing: Stage C handoff and Stage E migration are both observed on one rebalance, so
        the data volume must be large enough that both windows are observable. Use the same
        create_end as tests 16-17. The "after initial load" summary is the first thing to
        check when a stage reports it had nothing to observe.
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")

        # Applied before the load so the CP splits the manifest with it at rebalance time.
        self._apply_fusion_config_from_params()
        self._apply_fusion_sync_threshold()

        # Stage F needs the pre-rebalance S3 log-store baseline.
        s3_bucket_name, _ = self._capture_s3_log_store_baseline()

        self._load_above_threshold()

        # Freeze BEFORE triggering: guest volumes are otherwise deleted as each shard's copy
        # finishes, turning Stages C and E into a race against the CP.
        migration_paused = False
        if self.input.param("freeze_migration_until_stage_c", True):
            with self._stage("Stage 0: freeze background migration"):
                migration_paused = self._pause_migration()

        # Was test 5. Covers the whole lifecycle: the state has to hold through the freeze,
        # handoff, resume, drain and teardown. Stopped before the final assertion below, and
        # by tearDown if the body exits early.
        self._start_fusion_state_watcher()

        rebalance_task = self._trigger_rebalance()
        self.sleep(30, "Wait for rebalance to start before checking accelerators")

        # Initialised empty so a failed stage in soft-fail mode leaves downstream stages
        # able to detect what they are missing rather than raising NameError.
        instances, asgs, accel_ids = [], [], set()
        guest_vol_ids = set()
        baseline_du, gv_count = None, None

        # ---------------- Stage A: deploy accelerators (phase 4) ----------------
        # One stable-fleet snapshot feeds all three Stage A validators, so they judge the
        # same settled fleet instead of racing the phase-4 launch independently.
        with self._stage("Stage A1: accelerator fleet launched"):
            instances, asgs = self._wait_for_accelerator_fleet_stable(rebalance_task)
            self.assertGreater(
                len(instances), 0,
                "No accelerator instances appeared — fusion rebalance did not launch "
                "accelerators above threshold")
            accel_ids = {i["InstanceId"] for i in instances}
            self.log.info(f"Accelerators launched: {accel_ids}")

        if instances:
            with self._stage("Stage A2: accelerator instance types"):
                self._validate_accelerator_instance_type(instances)
            with self._stage("Stage A3: ASG topology"):
                self._validate_asg_topology(rebalance_task, instances, asgs)
        else:
            self._record_issue(
                "Stage A2/A3: instance types + ASG topology",
                "skipped — no stable accelerator fleet snapshot was obtained")

        with self._stage("Stage A4: accelerator guest volume specs"):
            self._validate_accelerator_volume_specs(rebalance_task)

        # -------- Stage B: capture the pre-completion state (no download monitoring) ----
        # Must happen while the volumes are still on the accelerators at 16000 IOPS: Stage C
        # follows these exact IDs, and the IOPS-based helpers stop matching them once the CP
        # scales them down.
        with self._stage("Stage B: capture guest volume IDs"):
            guest_vol_ids = self._capture_guest_volume_ids(rebalance_task)
            self.log.info(f"Captured {len(guest_vol_ids)} guest volume(s)")

        # ---------------- Rebalance completion (cheap poll) ----------------
        # Detect completion ourselves rather than waiting for wait_for_rebalances() to
        # return: every second before Stage C is a second in which volumes can be deleted.
        # wait_for_rebalances() still runs below for the real validation.
        self._wait_for_cp_rebalance_complete(rebalance_task)

        # Taken before Stage C so it is as close to the pre-migration state as possible.
        with self._stage("Stage B2: post-completion disk baseline"):
            baseline_du = self._avg_main_volume_usage()
            self.log.info(
                f"Pre-migration main-volume usage baseline (post-completion): "
                f"{baseline_du:.1f}% at {self._data_path()}")

        # ---------------- Stage C: handoff to KV nodes (phase 6) ----------------
        # First among the post-completion checks and before any slow reporting: the volumes
        # it inspects are being deleted as migration progresses.
        if guest_vol_ids:
            with self._stage("Stage C: guest volume transfer to cluster nodes"):
                # Frozen: wait out the async IOPS scale-down and run the read workload.
                # Otherwise fall back to the time-critical mode — exit immediately, defer the
                # IOPS watch to Stage E, skip the reads, so the drain window is not consumed.
                gv_count = self._validate_guest_volume_transfer(
                    rebalance_task, accel_ids, guest_vol_ids,
                    iops_grace=None if migration_paused else 0,
                    run_read_workload=migration_paused)
        else:
            self._record_issue(
                "Stage C: guest volume transfer",
                "skipped — no guest volume IDs were captured, so the handoff cannot "
                "be followed")

        # ---------------- Authoritative rebalance validation ----------------
        # Deferred until after Stage C: it asserts the rebalance result but is slow to return,
        # and none of that dead time is worth losing guest volumes over. It also runs BEFORE
        # the resume, so it cannot drain the volumes Stage E is about to watch.
        self.wait_for_rebalances([rebalance_task])
        CapellaAPI.wait_until_done(self.pod, self.tenant, self.cluster.id, timeout=600)
        self._log_cluster_data_summary("after rebalance completion")

        # ---------------- Stage D: resume migration ----------------
        # Every slow call is behind us, so Stage E watches from a known full set of volumes
        # rather than whatever survived the race.
        if migration_paused:
            with self._stage("Stage D: resume background migration"):
                self._resume_migration()

        # ---------------- Stage E: background migration (phase 8) ----------------
        with self._stage("Stage E: guest volume drain + main volume growth"):
            self._validate_guest_volume_drain(
                expected_start=gv_count or len(guest_vol_ids),
                baseline_du=baseline_du,
                iops_watch=getattr(self, "_iops_watch", None))

        # ---------------- Stage F: teardown (phase 8) ----------------
        with self._stage("Stage F: teardown and durability"):
            self._validate_teardown(s3_bucket_name)

        # Close the watch before aggregating, so a state violation seen anywhere in the
        # lifecycle lands in the same numbered failure.
        self._stop_fusion_state_watcher()

        # Every stage has run; now fail once with everything that went wrong.
        self._assert_no_stage_issues()
        self.log.info(
            "Fusion scaling lifecycle validated end to end: accelerators deployed, "
            "volumes handed off to KV nodes, guest volumes drained into the main "
            "volumes, infrastructure torn down with data intact, and fusion stayed "
            "'enabled' throughout")

    # ------------------------------------------------------------------
    # Test 20: Guest volumes accumulate across back-to-back rebalances while
    #          migration is paused, then all of it is destroyed once resumed
    # ------------------------------------------------------------------

    def test_guest_volume_accumulation_with_migration_paused(self):
        """
        Run two back-to-back fusion rebalances with background migration frozen, so guest
        volumes ACCUMULATE instead of being released, then resume migration and verify
        every one of them — and the surrounding infrastructure — is destroyed.

        This is the counterpart to test_back_to_back_rebalances_no_orphaned_volumes, not a
        variant of it: that test asserts 0 guest volumes before and after each run, which
        is the precise opposite of the invariant here. Both matter — one checks that a
        normal rebalance cleans up as it goes, this one checks that a backlog of volumes
        from several rebalances is still fully reclaimed once migration catches up.

        Shape (with kv_nodes=3 and the default rebalance_type=in):

          freeze migration (fusion_migration_rate_limit = 0)
          run 1: rebalance IN  3 -> 4 nodes  — the new node gets guest volumes
          run 2: rebalance OUT 4 -> 3 nodes  — more guest volumes across the KV nodes
          assert: run 1's volumes are ALL still present (nothing hydrated or released)
                  and the total has not shrunk
          resume migration (back to the pre-freeze rate, 75 MB/s by default)
          assert: guest volumes drain to 0, and teardown destroys everything —
                  no orphaned volumes, no ASGs, no accelerators, migration stats clean,
                  cluster healthy with fusion enabled, S3 log store intact, reads work

        Run 2 uses the inverse rebalance type so the topology stays valid for any
        `rebalance_type` (in<->out; swap toggles compute back). With kv_nodes=3 the
        in-then-out pair also returns the cluster to its starting size.

        The freeze is a hard prerequisite: without it the CP releases each volume as its
        shard finishes copying, and there is no accumulation to observe.
        """
        rebalance_flow = self.input.param("rebalance_flow", None) # provide in the form of a string, in:in, in:out:in, etc
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")

        split_gb, max_slots = self._apply_fusion_config_from_params()
        self._apply_fusion_sync_threshold()
        s3_bucket_name, _ = self._capture_s3_log_store_baseline()
        self._load_above_threshold()

        # Not wrapped in a stage: if the freeze does not take, every assertion below
        # becomes meaningless, so fail here rather than reporting confusing downstream
        # failures. _pause_migration() has already recorded why it failed.
        if not self._pause_migration():
            self.fail(
                "Could not freeze background migration (fusion_migration_rate_limit=0), "
                "and this test is entirely about what accumulates while it is frozen — "
                "see the recorded issue above for the cause")

        if not rebalance_flow:
            run_types = [self._default_rebalance_type(), self._inverse_rebalance_type()]
        else:
            run_types = rebalance_flow.split(":")
        volumes_after_run = {}      # run -> {volume_id: host_instance_id}
        instances_after_run = {}    # run -> set of cluster instance IDs

        for run, run_type in enumerate(run_types, 1):
            self.log.info(
                f"--- rebalance run {run} of 2 (type={run_type}, "
                f"data nodes before={self.num_nodes['data']}) ---")
            rebalance_task = self._trigger_rebalance(run_type)
            self.sleep(30, f"Wait for rebalance run {run} to start")

            with self._stage(f"Run {run}: accelerator fleet launched"):
                instances, asgs = self._wait_for_accelerator_fleet_stable(rebalance_task)
                self.assertGreater(
                    len(instances), 0,
                    f"Run {run}: no accelerators launched, so this rebalance did not "
                    f"take the fusion path and cannot add guest volumes")
                self.log.info(
                    f"Run {run}: {len(instances)} accelerator(s), {len(asgs)} ASG(s) "
                    f"(fusion config overrides — minSplitSize: {split_gb or 'CP default'}, "
                    f"maxSlots: {max_slots or 'CP default'})")

            # Outside the stage wrapper: the next iteration triggers another rebalance, so
            # this one must be complete whether or not the assertions above passed.
            self.wait_for_rebalances([rebalance_task])
            CapellaAPI.wait_until_done(
                self.pod, self.tenant, self.cluster.id, timeout=600)

            with self._stage(f"Run {run}: guest volumes present and attached to KV nodes"):
                volumes = self._list_accelerator_volumes(guest_only=True)
                # Keep volume -> host, not just the IDs: a volume whose host is removed by
                # a later scale-in is destroyed with it, which is correct behaviour and
                # must not be mistaken for a leak.
                volumes_after_run[run] = {
                    v["VolumeId"]: ((v.get("Attachments") or [{}])[0].get("InstanceId")
                                    or "unattached")
                    for v in volumes if v.get("VolumeId")}
                instances_after_run[run] = {
                    i.get("InstanceId") for i in self.fusion_aws_util.list_instances(
                        self.fusion_aws_util._cluster_filter(self.cluster.id),
                        log="ClusterNodes", suppress_log=True)}
                self.assertGreater(
                    len(volumes_after_run[run]), 0,
                    f"Run {run}: no guest volumes present after the rebalance, even "
                    f"though migration is frozen — they should not have been released")

                # Where the volumes landed: with migration frozen they stay put, so this
                # is a stable view of the per-node spread.
                per_node = dict()
                for vol in volumes:
                    atts = vol.get("Attachments") or []
                    inst = atts[0].get("InstanceId") if atts else "unattached"
                    per_node.setdefault(inst, []).append(vol.get("VolumeId"))
                self.log.info(
                    f"Run {run}: {len(volumes_after_run[run])} guest volume(s) across "
                    f"{len(per_node)} node(s):\n" + "\n".join(
                        f"    {inst}: {len(vols)} volume(s) {sorted(vols)}"
                        for inst, vols in sorted(per_node.items())))
                self.assertTrue(
                    self.cp_monitor.verify_guest_volumes_attached_to_cluster(self.cluster),
                    f"Run {run}: not all guest volumes are attached to cluster instances")

        # ---------------- Accumulation across the two runs ----------------
        # A scale-in destroys the node it removes, and with it any guest volume attached to
        # that node. That is correct — the volume cannot outlive its host — so the
        # persistence invariant only covers volumes whose run-1 host is STILL in the
        # cluster after run 2. Volumes lost with a departed host are reported instead.
        first = volumes_after_run.get(1, {})
        second = volumes_after_run.get(2, {})
        surviving_hosts = instances_after_run.get(2, set())
        with self._stage("Guest volumes accumulated across both rebalances"):
            self.assertTrue(
                first and second,
                f"Cannot judge accumulation: run 1 saw {len(first)} volume(s) and run 2 "
                f"saw {len(second)} — at least one run recorded nothing")

            departed_hosts = {h for h in first.values()
                              if h != "unattached" and h not in surviving_hosts}
            expected_to_survive = {v for v, h in first.items()
                                   if h != "unattached" and h in surviving_hosts}
            lost_with_host = {v: h for v, h in first.items()
                              if h in departed_hosts and v not in second}
            unattached_at_run1 = {v for v, h in first.items() if h == "unattached"}

            released = expected_to_survive - set(second)
            self.assertEqual(
                sorted(released), [],
                f"{len(released)} guest volume(s) from run 1 were released before run 2 "
                f"finished even though their host is still in the cluster, with migration "
                f"frozen: {sorted(released)}. Either the freeze did not hold or something "
                f"other than migration deletes guest volumes.")

            if departed_hosts:
                self.log.info(
                    f"Run 2 ({run_types[1]}) removed host(s) {sorted(departed_hosts)}; "
                    f"{len(lost_with_host)} volume(s) went with them (expected): "
                    f"{sorted(lost_with_host)}")
            if unattached_at_run1:
                self.log.info(
                    f"{len(unattached_at_run1)} volume(s) were unattached when run 1 was "
                    f"observed, so they are excluded from the persistence check: "
                    f"{sorted(unattached_at_run1)}")

            added = set(second) - set(first)
            self.log.info(
                f"Accumulation: run 1 left {len(first)} volume(s) "
                f"({len(expected_to_survive)} on hosts that survived), run 2 added "
                f"{len(added)}, total now {len(second)}")
            if not added:
                self._record_issue(
                    "Accumulation: second rebalance added no guest volumes",
                    f"run 2 ({run_types[1]}) added no new volume IDs to the "
                    f"{len(first)} from run 1 — expected it to bring up more guest "
                    f"volumes across the KV nodes. It may not have taken the fusion path.")

        # Disk baseline for the drain. Migration is still frozen, so this is genuinely
        # pre-migration no matter how long the two rebalances took.
        baseline_du = self._avg_main_volume_usage()
        self.log.info(
            f"Pre-migration main-volume usage baseline: {baseline_du:.1f}% at "
            f"{self._data_path()}")
        self._log_cluster_data_summary("after both rebalances, migration still frozen")

        # ---------------- Resume migration ----------------
        with self._stage("Resume background migration"):
            self._resume_migration()

        # ---------------- Everything must be reclaimed ----------------
        with self._stage("Drain: every accumulated guest volume released"):
            self._validate_guest_volume_drain(
                expected_start=len(second) or None, baseline_du=baseline_du)

        with self._stage("Teardown: all fusion infrastructure destroyed"):
            self._validate_teardown(s3_bucket_name)

        self._assert_no_stage_issues()
        self.log.info(
            f"Migration-paused accumulation validated: {len(second)} guest volume(s) "
            f"built up across two rebalances, then all of them and the surrounding "
            f"infrastructure were destroyed once migration resumed")

    # ------------------------------------------------------------------
    # Test 21: Resource tags across the full accelerator fleet
    # ------------------------------------------------------------------

    def test_accelerator_resource_tags(self):
        """
        During a fusion rebalance, assert that every temporary AWS resource the
        acceleration process creates carries the tags the control plane relies on for
        tag-based discovery and lifecycle management (see _cluster_filter /
        list_accelerator_instances / list_cluster_fusion_asg /
        monitor_full_cluster_teardown):

          couchbase-cloud-cluster-id = <cluster.id>
          couchbase-cloud-function   = fusion-accelerator

        Covers accelerator EC2 instances, their ASGs, guest EBS volumes, the S3
        log-store bucket, and the accelerator IAM instance profile/role -- the five
        resource kinds the CP provisions and tears down per rebalance.

        Guest-volume tags are also checked by test_guest_volume_properties; they are
        repeated here so this test is a single place proving ALL temporary resources are
        tagged, not just some of them. The EC2/ASG/volume checks re-verify a tag value
        that already had to match for the resource to be *found* (they are discovered by
        that same tag filter), so they mainly guard against a future regression that
        widens the filter. The S3 bucket and IAM instance-profile/role checks are
        different: those two are looked up by name/ARN, never by tag filter, so this is
        the first real assertion anywhere in this suite that the CP tags them at all.
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")
        self._apply_fusion_config_from_params()
        self._apply_fusion_sync_threshold()

        s3_bucket_name, _ = self._capture_s3_log_store_baseline()
        self.assertIsNotNone(
            s3_bucket_name, "No S3 log-store bucket found before rebalance — "
            "cannot validate bucket tags")

        self._load_above_threshold()
        rebalance_task = self._trigger_rebalance()
        self.sleep(30, "Wait for rebalance to start before polling for the fleet")

        instances, asgs = self._wait_for_accelerator_fleet_stable(rebalance_task)
        volumes = self._list_accelerator_volumes(guest_only=True)
        self.assertGreater(
            len(volumes), 0,
            "No guest volumes appeared alongside the accelerator fleet — "
            "cannot validate guest volume tags")

        iam_profile_name = self.fusion_aws_util.ec2.get_instance_iam_profile_name(
            instances[0]["InstanceId"])
        self.assertIsNotNone(
            iam_profile_name,
            f"Accelerator instance {instances[0]['InstanceId']} has no IAM instance "
            f"profile attached — cannot validate IAM tags")
        iam_tags = self.fusion_aws_util.ec2.get_iam_resource_tags(iam_profile_name)
        s3_tags = self.fusion_aws_util.s3.get_bucket_tags(s3_bucket_name)

        expected = {
            "couchbase-cloud-cluster-id": self.cluster.id,
            "couchbase-cloud-function": "fusion-accelerator",
        }

        failures = []

        def _check(resource_label, resource_id, tag_dict):
            for key, value in expected.items():
                actual = tag_dict.get(key)
                status = "OK" if actual == value else "FAIL"
                self.log.info(
                    f"  [{status}] {resource_label} {resource_id} tag:{key}: {actual!r}")
                if actual != value:
                    failures.append(
                        f"{resource_label} {resource_id} -- tag:{key}: "
                        f"got {actual!r}, expected {value!r}")

        for inst in instances:
            _check("accelerator instance", inst.get("InstanceId"),
                   {t["Key"]: t["Value"] for t in inst.get("Tags", [])})

        for asg in asgs:
            _check("ASG", asg.get("AutoScalingGroupName"),
                   {t["Key"]: t["Value"] for t in asg.get("Tags", [])})

        for vol in volumes:
            _check("guest volume", vol.get("VolumeId"),
                   {t["Key"]: t["Value"] for t in vol.get("Tags", [])})

        _check("S3 log-store bucket", s3_bucket_name, s3_tags)
        _check("IAM instance profile", iam_profile_name,
               iam_tags.get("instance_profile_tags", {}))
        for role_name, role_tags in iam_tags.get("role_tags", {}).items():
            _check("IAM role", role_name, role_tags)

        self.assertEqual(
            len(failures), 0,
            "Resource tag violations across the accelerator fleet:\n"
            + "\n".join(failures))
        self.log.info(
            f"All accelerator resources correctly tagged: {len(instances)} instance(s), "
            f"{len(asgs)} ASG(s), {len(volumes)} guest volume(s), 1 S3 bucket, "
            f"1 IAM instance profile"
            + (f" + {len(iam_tags.get('role_tags', {}))} role(s)"
               if iam_tags.get("role_tags") else ""))

        self.wait_for_rebalances([rebalance_task])
