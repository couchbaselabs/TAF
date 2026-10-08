"""
Fusion failure-log collection.

Layer 2 -- FusionLogCollector
    Collects, for one or more Capella clusters:
      * cbcollect_info from every Couchbase node, through the Capella
        internal-support log-collection API (the same endpoint
        ProvisionedBaseTestCase.tearDown uses with get-cbcollect-info=True).
        CP uploads the per-node zips to S3; their URLs are logged and
        written to disk.
      * the dp-agent journal from every Couchbase node EC2 instance over
        SSM. See FusionAWSUtil.collect_dp_agent_logs_from_cluster_instances.
      * fusion accelerator logs that accelerators upload to
        s3://cbc-storage-<cluster>/logs/fusion/<plan_uuid>/, only for the
        rebalance plans created during the test.
      * logs pulled over SSM from any fusion accelerator still running (same
        content as the S3 upload), so a plan with no S3 logs yet, or a failed
        upload, is still covered.

Layer 3 hook -- FusionFailureLogsMixin
    Mixed into the fusion cloud base classes. Runs the collection once, when
    the test failed or errored, before any tearDown of the test class runs.
    That is, before scale-in, bucket deletes, or cluster destroy change the
    cluster.

Test params (all optional):
    collect_logs_on_failure      master switch (default True)
    force_collect_logs           collect even when the test passed (default False)
    get-cbcollect-info           an explicit False skips the cbcollect part only
    failure_logs_dp_agent        collect dp-agent journals (default True)
    failure_logs_accelerator_s3  download accelerator S3 logs (default True)
    failure_logs_accelerator_live  collect from running accelerators over SSM
                                 (default True)
    dp_agent_log_days            journalctl --since window in days (default 1)
    dp_agent_log_units           ':'-separated systemd units (default dp-agent)
    failure_logs_s3_bucket       staging bucket for the node->TAF transfer;
                                 default cbc-storage-<cluster_id>, 'none' for
                                 the inline-SSM-only path
    cbcollect_timeout            seconds to wait for cbcollect (default 1800)
    cbcollect_ticket             ticketId passed to the log-collection API
"""

import json
import os
import threading
import time
from datetime import datetime, timedelta, timezone

from prettytable import PrettyTable

from TestInput import TestInputSingleton


class FusionLogCollector:
    """Layer 2: failure-time log collection for Capella fusion clusters.

    Never raises from collect(): every failure is logged and reported in the
    returned summary, so a collection problem cannot replace the test's own
    failure.
    """

    LOG_COLLECTION_TASK_TYPE = "clusterLogsCollection"
    SUMMARY_FILE = "collection_summary.log"

    def __init__(self, logger):
        self.log = logger

    # ------------------------------------------------------------------
    # cbcollect via the Capella internal-support API
    # ------------------------------------------------------------------

    def get_log_collection_task(self, capella_api, cluster_id):
        """Return the cluster's clusterLogsCollection task (ns_server keeps
        only the latest one), or None if there is none or the call failed."""
        resp = capella_api.get_cluster_tasks(cluster_id)
        if resp is None or resp.status_code != 200:
            self.log.warning(
                f"[failure-logs] GET tasks for cluster {cluster_id} returned "
                f"{getattr(resp, 'status_code', None)}: {getattr(resp, 'content', b'')[:300]}")
            return None
        tasks = json.loads(resp.content)
        return next((t for t in tasks
                     if t.get("type") == self.LOG_COLLECTION_TASK_TYPE), None)

    def collect_cbcollect(self, capella_api, cluster_id, timeout=1800,
                          ticket_id="", poll_interval=20):
        """
        Trigger cbcollect_info on every node of the cluster and wait for it.

        POST /internal/support/logcollections/clusters/{id}, then poll
        /internal/support/clusters/{id}/pools/default/tasks. The cluster is
        usually shared across tests and may already hold a finished
        collection task, so the task's start timestamp is recorded first and
        only a newer task counts.

        :return: {'status': completed|cancelled|timeout|failed,
                  'per_node': {node: {status, url, path}}, 'error': str}
        """
        result = {"status": "failed", "per_node": {}, "error": ""}
        try:
            previous = self.get_log_collection_task(capella_api, cluster_id)
            if previous and previous.get("status") == "running":
                self.log.info(
                    f"[failure-logs] cbcollect already running on {cluster_id}; waiting for it")
                previous = None
            else:
                resp = capella_api.trigger_log_collection(cluster_id, ticketId=ticket_id)
                if resp is None or resp.status_code != 201:
                    content = getattr(resp, "content", b"") or b""
                    if b"already started" not in content:
                        result["error"] = (
                            f"trigger returned {getattr(resp, 'status_code', None)}: "
                            f"{content[:300]}")
                        return result
                    previous = None
                self.log.info(f"[failure-logs] cbcollect triggered on cluster {cluster_id}")

            # A finished task from an earlier collection must not be taken for
            # ours: require a different start ts, or (if the task carries no
            # ts) having seen it running after the trigger.
            seen_running = False

            def _is_ours(t):
                if not t:
                    return False
                if previous is None:
                    return True
                if previous.get("ts") is not None:
                    return t.get("ts") != previous.get("ts")
                return seen_running

            deadline = time.time() + timeout
            task = None
            # Give up early if the task never shows up (e.g. the cluster is
            # being destroyed) instead of burning the whole timeout.
            not_visible_deadline = time.time() + max(10 * poll_interval, 120)
            while time.time() < deadline:
                task = self.get_log_collection_task(capella_api, cluster_id)
                if task and task.get("status") == "running":
                    seen_running = True
                if not _is_ours(task) and not seen_running \
                        and time.time() > not_visible_deadline:
                    result["error"] = "log-collection task never appeared after trigger"
                    break
                if _is_ours(task):
                    self.log.info(
                        f"[failure-logs] cbcollect on {cluster_id}: status="
                        f"{task.get('status')} progress={task.get('progress')}%")
                    if task.get("status") in ("completed", "cancelled"):
                        result["status"] = task["status"]
                        break
                time.sleep(poll_interval)
            else:
                result["status"] = "timeout"
                result["error"] = f"not completed within {timeout}s"
            if _is_ours(task):
                result["per_node"] = task.get("perNode", {}) or {}
        except Exception as e:
            result["error"] = f"{type(e).__name__}: {e}"
        return result

    # ------------------------------------------------------------------
    # Orchestration
    # ------------------------------------------------------------------

    def collect_cluster_logs(self, target, dest_root, cbcollect=True,
                             dp_agent=True, accelerator_s3=True,
                             cbcollect_timeout=1800, cbcollect_ticket="",
                             dp_agent_days=1, dp_agent_units=("dp-agent",),
                             s3_bucket="default", since=None,
                             accelerator_plan_baseline=None,
                             accelerator_live=True, accelerator_plan_uuids=None):
        """
        Collect everything for one cluster. cbcollect runs in a background
        thread while the dp-agent and accelerator logs are pulled, so the
        total time is roughly max(cbcollect, dp-agent), not the sum.

        :param target: {'cluster_id', 'capella_api' (or None), 'aws_util' (or None)}
        :param s3_bucket: staging bucket for dp-agent transfer; 'default'
                          means cbc-storage-<cluster_id>, None/'' means inline only
        :param accelerator_plan_baseline: {cluster_id: set(plan_uuid)} of the
                          accelerator log plans present when the test started;
                          only plans not in it are downloaded. A cluster with
                          no baseline (created mid-test, or the listing failed
                          at setUp) falls back to plans with logs newer than since.
        :param accelerator_live: also collect from running accelerators over SSM
        :param accelerator_plan_uuids: explicit plan UUIDs; overrides the
                          baseline/since selection and limits the live
                          accelerators to these plans
        :return: list of summary rows [source, node/instance, status, location]
        """
        cluster_id = target["cluster_id"]
        dest_dir = os.path.join(dest_root, cluster_id)
        os.makedirs(dest_dir, exist_ok=True)
        rows = []

        cb_result = {}
        cb_thread = None
        capella_api = target.get("capella_api")
        if cbcollect and capella_api is not None:
            def _run_cbcollect():
                cb_result.update(self.collect_cbcollect(
                    capella_api, cluster_id, timeout=cbcollect_timeout,
                    ticket_id=cbcollect_ticket))
            cb_thread = threading.Thread(
                target=_run_cbcollect, name=f"cbcollect_{cluster_id}", daemon=True)
            cb_thread.start()
        elif cbcollect:
            rows.append(["cbcollect", "-", "skipped", "no Capella API client"])

        aws_util = target.get("aws_util")
        bucket = f"cbc-storage-{cluster_id}" if s3_bucket == "default" else s3_bucket
        if aws_util is None and (dp_agent or accelerator_s3 or accelerator_live):
            rows.append(["dp-agent", "-", "skipped", "no AWS access"])
        if aws_util is not None and dp_agent:
            try:
                results = aws_util.collect_dp_agent_logs_from_cluster_instances(
                    cluster_id, os.path.join(dest_dir, "dp-agent"),
                    days=dp_agent_days, units=dp_agent_units, s3_bucket=bucket or None)
                for r in results:
                    rows.append([
                        r["unit"], f"{r['role']} {r['instance_id']} ({r['private_ip']})",
                        r["status"] if not r["method"] else f"{r['status']} [{r['method']}]",
                        r["path"] or r["error"]])
            except Exception as e:
                rows.append(["dp-agent", "-", "failed", f"{type(e).__name__}: {e}"])
        if aws_util is not None and accelerator_s3:
            try:
                baseline = (accelerator_plan_baseline or {}).get(cluster_id)
                if accelerator_plan_uuids:
                    plans = set(accelerator_plan_uuids)
                elif baseline is not None:
                    current = aws_util.list_fusion_accelerator_log_plans(cluster_id)
                    plans = (current or set()) - baseline
                else:
                    plans = aws_util.list_fusion_accelerator_log_plans(
                        cluster_id, since=since) or set()
                if not plans:
                    rows.append(["accelerator S3 logs", "-", "none",
                                 "no rebalance plans during the test"])
                else:
                    plan_dir = os.path.join(dest_dir, "accelerator-s3")
                    files = aws_util.download_fusion_accelerator_s3_logs(
                        cluster_id, plan_dir, plan_uuids=plans)
                    rows.append(["accelerator S3 logs", ", ".join(sorted(plans)),
                                 f"{len(files)} file(s)" if files else "none",
                                 plan_dir if files else "no logs in S3 for these plans"])
            except Exception as e:
                rows.append(["accelerator S3 logs", "-", "failed", f"{type(e).__name__}: {e}"])
        if aws_util is not None and accelerator_live:
            try:
                results = aws_util.collect_live_accelerator_logs(
                    cluster_id, os.path.join(dest_dir, "accelerator-live"),
                    plan_uuids=set(accelerator_plan_uuids) if accelerator_plan_uuids else None,
                    s3_bucket=bucket or None)
                if not results:
                    rows.append(["accelerator (live)", "-", "none", "no running accelerators"])
                for r in results:
                    rows.append([
                        "accelerator (live)",
                        f"{r['instance_id']} plan {r['plan_uuid']}",
                        r["status"] if not r["method"] else f"{r['status']} [{r['method']}]",
                        r["path"] or r["error"]])
            except Exception as e:
                rows.append(["accelerator (live)", "-", "failed", f"{type(e).__name__}: {e}"])

        if cb_thread is not None:
            # collect_cbcollect enforces its own deadline; the join margin
            # only guards against a hung HTTP call.
            cb_thread.join(timeout=cbcollect_timeout + 300)
            if cb_thread.is_alive():
                rows.append(["cbcollect", "-", "timeout", "collection thread still running"])
            else:
                status = cb_result.get("status", "failed")
                if cb_result.get("error"):
                    rows.append(["cbcollect", "-", status, cb_result["error"]])
                for node, info in sorted(cb_result.get("per_node", {}).items()):
                    rows.append(["cbcollect", node, info.get("status", status),
                                 info.get("url") or info.get("path") or ""])
        return rows

    def collect(self, targets, dest_root, reason="", **kwargs):
        """
        Collect logs for every target cluster in parallel, log one summary
        table and write it to <dest_root>/<cluster_id>/collection_summary.log.
        Never raises.

        :param targets: list of {'cluster_id', 'capella_api', 'aws_util'}
        :param dest_root: local root directory
        :param reason: free text recorded in the summary header
        :param kwargs: forwarded to collect_cluster_logs
        :return: {cluster_id: rows}
        """
        summary = {}

        def _one(target):
            try:
                summary[target["cluster_id"]] = self.collect_cluster_logs(
                    target, dest_root, **kwargs)
            except Exception as e:
                summary[target["cluster_id"]] = [
                    ["all", "-", "failed", f"{type(e).__name__}: {e}"]]

        threads = [threading.Thread(target=_one, args=(t,), daemon=True,
                                    name=f"failure_logs_{t['cluster_id']}")
                   for t in targets]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        for cluster_id, rows in summary.items():
            table = PrettyTable()
            table.field_names = ["Source", "Node / Instance", "Status", "Location"]
            table.align = "l"
            for row in rows:
                table.add_row(row)
            header = (f"Failure logs for cluster {cluster_id}"
                      f"{f' ({reason})' if reason else ''} -> "
                      f"{os.path.join(dest_root, cluster_id)}")
            self.log.info(f"{header}\n{table}")
            try:
                with open(os.path.join(dest_root, cluster_id, self.SUMMARY_FILE), "w") as f:
                    f.write(f"{header}\n{table}\n")
            except Exception as e:
                self.log.warning(f"[failure-logs] Could not write summary file: {e}")
        return summary


class FusionFailureLogsMixin:
    """
    Layer 3 hook: collect cbcollect + dp-agent logs when a fusion cloud test
    fails. Must come before unittest.TestCase in the MRO.

    The hook overrides unittest's _callTearDown (python >= 3.8) rather than
    tearDown. Subclass tearDowns run state-changing cleanup (scale-in, bucket
    deletes, cluster destroy) before they call super().tearDown(). Hooking
    _callTearDown means the logs are collected before any tearDown code runs.
    unittest does not call _callTearDown when setUp failed, so a setUp failure
    is not covered.

    Subclasses whose clusters are not in self.tenants override
    _fusion_failure_log_targets().
    """

    def _callSetUp(self):  # noqa: N802 (unittest override)
        self._fusion_test_started_at = datetime.now(timezone.utc)
        self._fusion_failure_logs_collected = False
        self._fusion_accelerator_plan_baseline = {}
        super()._callSetUp()
        try:
            self._snapshot_accelerator_log_plans()
        except Exception as e:
            self.log.warning(
                f"[failure-logs] Could not snapshot accelerator log plans: {e}")

    def _snapshot_accelerator_log_plans(self):
        """Record the rebalance plan UUIDs that already have accelerator logs
        in each target cluster's storage bucket, so a failure only downloads
        the plans this test created."""
        params = TestInputSingleton.input
        if not params.param("collect_logs_on_failure", True) \
                or not params.param("failure_logs_accelerator_s3", True):
            return
        for target in self._fusion_failure_log_targets():
            aws_util = target.get("aws_util")
            if aws_util is None:
                continue
            plans = aws_util.list_fusion_accelerator_log_plans(target["cluster_id"])
            if plans is not None:
                self._fusion_accelerator_plan_baseline[target["cluster_id"]] = plans

    def _callTearDown(self):  # noqa: N802 (unittest override)
        try:
            self.collect_fusion_failure_logs_if_needed()
        except Exception as e:
            # Must never mask or replace the test's own failure.
            self.log.warning(f"[failure-logs] Log collection on failure errored: {e}")
        super()._callTearDown()

    def _fusion_test_has_failed(self):
        """True if the test body failed or errored. Same signal as
        CouchbaseBaseTest.is_test_failed() on python 3.10; falls back to the
        result object on 3.11+, where _Outcome.errors no longer exists."""
        outcome = getattr(self, "_outcome", None)
        if outcome is None:
            return False
        errors = getattr(outcome, "errors", None)
        if errors is not None:
            return any(exc_info is not None for _, exc_info in errors)
        result = getattr(outcome, "result", None)
        reported = list(getattr(result, "errors", [])) + list(getattr(result, "failures", []))
        return any(test is self for test, _ in reported)

    def collect_fusion_failure_logs_if_needed(self):
        """Run the collection once per test, if enabled and the test failed
        (or force_collect_logs=True)."""
        if getattr(self, "_fusion_failure_logs_collected", False):
            return
        params = TestInputSingleton.input
        if not params.param("collect_logs_on_failure", True):
            return
        failed = self._fusion_test_has_failed()
        if not failed and not params.param("force_collect_logs", False):
            return
        self._fusion_failure_logs_collected = True
        self.collect_fusion_failure_logs(
            reason="test failed" if failed else "force_collect_logs=True")

    def collect_fusion_failure_logs(self, reason=""):
        """Collect logs for every cluster this test touched. Can also be
        called directly (e.g. from a debug session) and never raises."""
        params = TestInputSingleton.input
        try:
            targets = self._fusion_failure_log_targets()
        except Exception as e:
            self.log.warning(f"[failure-logs] Could not resolve target clusters: {e}")
            return {}
        if not targets:
            self.log.warning("[failure-logs] No clusters to collect logs from")
            return {}

        # An explicit get-cbcollect-info=False opts out of the cbcollect part.
        cbcollect = str(params.test_params.get("get-cbcollect-info", True)).lower() != "false"
        s3_bucket = params.param("failure_logs_s3_bucket", "default")
        if str(s3_bucket).lower() in ("none", "false", ""):
            s3_bucket = None
        units = [u for u in str(params.param("dp_agent_log_units", "dp-agent")).split(":") if u]
        dest_root = os.path.join(params.param("logs_folder", "/tmp"), "fusion_failure_logs")
        started = getattr(self, "_fusion_test_started_at", None)
        since = started - timedelta(minutes=5) if started else None

        self.log.info(
            f"[failure-logs] Collecting logs ({reason}) for cluster(s) "
            f"{[t['cluster_id'] for t in targets]} into {dest_root}: "
            f"cbcollect={cbcollect}, dp-agent units={units}")
        summary = FusionLogCollector(self.log).collect(
            targets, dest_root, reason=reason,
            cbcollect=cbcollect,
            dp_agent=params.param("failure_logs_dp_agent", True),
            accelerator_s3=params.param("failure_logs_accelerator_s3", True),
            accelerator_live=params.param("failure_logs_accelerator_live", True),
            cbcollect_timeout=int(params.param("cbcollect_timeout", 1800)),
            cbcollect_ticket=str(params.param("cbcollect_ticket", "")),
            dp_agent_days=int(params.param("dp_agent_log_days", 1)),
            dp_agent_units=units,
            s3_bucket=s3_bucket,
            since=since,
            accelerator_plan_baseline=getattr(
                self, "_fusion_accelerator_plan_baseline", {}))
        if cbcollect:
            # ProvisionedBaseTestCase.tearDown would otherwise trigger a second
            # cbcollect (after the cluster may already have been changed).
            self.get_cbcollect_info = False
        return summary

    # ------------------------------------------------------------------
    # Target resolution (override in bases that do not use self.tenants)
    # ------------------------------------------------------------------

    def _fusion_failure_log_capella_api(self, tenant):
        """Internal-support-capable Capella API client, built the same way
        as CapellaUtils does (pod.TOKEN = *_token_for_internal_support)."""
        from capellaAPI.capella.dedicated.CapellaAPI import CapellaAPI
        pod = getattr(self, "pod", None)
        if pod is None or tenant is None:
            return None
        if not pod.TOKEN:
            self.log.warning(
                "[failure-logs] pod.TOKEN (internal-support token) is not set; "
                "the cbcollect request is expected to be rejected")
        return CapellaAPI(pod.url_public, tenant.api_secret_key,
                          tenant.api_access_key, tenant.user, tenant.pwd,
                          pod.TOKEN)

    def _fusion_failure_log_aws_util(self, tenant):
        per_tenant = getattr(self, "_fusion_aws_util_for_tenant", None)
        if per_tenant is not None and tenant is not None:
            try:
                return per_tenant(tenant)
            except Exception as e:
                self.log.warning(f"[failure-logs] Per-tenant AWS util unavailable: {e}")
        return getattr(self, "fusion_aws_util", None)

    def _fusion_failure_log_targets(self):
        """Default: every cluster in self.tenants, plus self.secondary_cluster
        (dynamically created clone target) if it is not among them."""
        targets, seen = [], set()
        tenants = getattr(self, "tenants", None) or []
        for tenant in tenants:
            api = self._fusion_failure_log_capella_api(tenant)
            aws_util = self._fusion_failure_log_aws_util(tenant)
            for cluster in tenant.clusters:
                if cluster.id in seen:
                    continue
                seen.add(cluster.id)
                targets.append({"cluster_id": cluster.id, "capella_api": api,
                                "aws_util": aws_util})
        secondary = getattr(self, "secondary_cluster", None)
        if secondary is not None and getattr(secondary, "id", None) not in seen:
            tenant = getattr(self, "primary_tenant", None) or (tenants[0] if tenants else None)
            targets.append({"cluster_id": secondary.id,
                            "capella_api": self._fusion_failure_log_capella_api(tenant),
                            "aws_util": self._fusion_failure_log_aws_util(tenant)})
        return targets
