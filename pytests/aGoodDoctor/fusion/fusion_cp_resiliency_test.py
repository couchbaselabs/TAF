"""
Fusion CP Resiliency Tests
==========================

Automates one row of COVERAGE.md §9 "CP Resiliency" / the "Negative Tests by Stage
Boundary" table in STAGE_TEST_MATRIX.md (boundary B):

    dp-accelerator agent crashes mid-download -> test_dp_accelerator_crash_during_download

Every other §9 row is still a gap: most need a control-plane job API or an S3-permission
failure injected on the CP's own credentials, neither of which is exposed to TAF (see the
"blocked"/"gap" rows in STAGE_TEST_MATRIX.md). This one row is reachable because the
accelerator EC2 instance is SSM-managed like any other Capella-deployed instance, and
`dp-accelerator` (ACCELERATION.md's Component Responsibilities table: "dp-accelerator
(agent) | Agent on accelerator EC2 instances | Downloads shard data from S3...") runs as a
systemd unit on it — confirmed by direct discovery on a real accelerator instance:

    dp-accelerator.service   loaded  activating auto-restart  DP Accelerator Service
        responsible for downloading Fusion data from cloud storage

(An earlier version of this file assumed the unit was named `dp-agent`, carried over from
`fusion_aws_util.check_dp_agent_health_on_cluster_instances` / `scan_dp_agent_logs_for_
errors_on_cluster_instances` — those are only ever exercised against KV/"cluster" nodes
elsewhere in this codebase, never against an accelerator specifically, so that name was
never actually validated for this instance population. `dp-agent` does not exist as a unit
on the accelerator; every is-active check against it fell through to its own `|| echo
inactive` fallback, which is why every candidate always came back 'inactive' regardless of
what was actually happening.)

The unit's observed substate is `activating (auto-restart)`, not `active` — it legitimately
cycles through short-lived runs rather than sitting resident, so `systemctl is-active ==
active` would stay flaky even against the right name. This file instead checks for a live
PID via `pgrep -f dp-accelerator`, which is both simpler and the literal precondition for
what the test is about to do to it (`pkill`).

Grounding for what should happen, from ACCELERATION.md §Phase 5 and §Error Handling:
  * the agent registers with the CP ("Registered"), downloads its shard from S3 to the
    attached EBS volume, then reports Complete; "the accelerator waits (with exponential
    backoff) for each node to reach Complete status before proceeding."
  * download is one of the checkpointed long-running operations ("Checkpoints: Long-
    running operations (deploy, register, download) record progress via a non-blocking
    checkpoint system... attempt history with start/end times, success/failure").
So a killed dp-accelerator process has two ways to recover, and both are accepted here:
  * systemd restarts it on the SAME instance and the download resumes/retries against the
    checkpointed state;
  * the CP's backoff gives up on that node and redeploys a NEW accelerator instead (the
    fusion-fallback-replace path already exercised elsewhere, e.g. fusion_fallback_test.py).
Not accepted: the instance sits with no dp-accelerator process and nothing else reacts,
which would strand the rebalance on a download that can never finish.

Migration is frozen before the kill only so the aftermath stays observable, matching every
other download-phase fault in fusion_accelerator_chaos_test.py — the fault itself lands
during download, long before migration starts. It is resumed BEFORE the teardown/EBS-
cleanup checks run: guest volumes are only reclaimed as background migration completes, so
validating teardown against a still-frozen cluster hangs until EBS_CLEANUP_TIMEOUT with no
chance of passing (observed: a 2925s timeout, fully wasted, in an early run of this test).

Target selection (see _find_downloading_accelerator) does not simply grab the first
accelerator to appear. Fusion shards are wildly uneven in size — an earlier run of this
test observed 2 of 4 shards for a single scale-out already handed off to a KV node (IOPS
scaled 16000 -> 3000) within ~10-20s of their accelerator even launching, and the one
picked as a target had also finished before the test's very next check 15s later. Grabbing
`instances[0]` and asserting on it therefore fails on a fast shard for a reason that has
nothing to do with the fault under test. Instead this searches the currently-downloading
fleet for one whose dp-accelerator process is actually still running before committing to
it. `fusion_download_rate_limit` (see the conf) throttles the S3 download to widen that
window further.

ONE rate limit is used for the whole test, not a low-then-high pair: an earlier version
set it very low (100 KB/s) to guarantee the crash landed mid-download, then tried to raise
it back up once recovery was confirmed so the rest of the test did not also crawl. That
does not work — changing accelerator.download.rateLimit mid-flight has no effect on a
download already in progress (confirmed with the fusion/CP team), so a run at 100 KB/s
simply never finished; it hung for the rest of the test regardless of what was PATCHed in
afterward. The single value in the conf (50 MB/s) was chosen from real observation (a run
at that rate still had multiple accelerators mid-download 122s into the search, so the
window is real) balanced against needing every shard to plausibly finish within the ~30
minute range the rest of the test budgets for.

Two more races the same fast-cycling behaviour caused, both fixed:
  * _crash_dp_accelerator checks for a live PID and kills it in ONE remote command
    instead of a separate pgrep-then-pkill round trip. A run that confirmed a PID alive
    in _find_downloading_accelerator, then sent a SEPARATE kill command ~5s later (stage
    transition + a fresh SSM round trip), got "no process matched" back — the process
    had already cycled in that gap. Checking and killing in the same remote command
    removes that gap; retrying a few times covers the smaller remaining chance a single
    attempt still lands inside the process's own restart-backoff window.
  * accel_ids_before (CPR3's "did the CP redeploy" baseline) is captured right before
    the kill, not at the start of CPR1. That stage's search can take minutes (observed:
    122s) while the fleet is still ramping up for other shards — a run that captured
    the baseline too early saw 4 accelerators that were already running (visible in the
    fleet listing 16s before the target was even picked) get reported as "newly
    redeployed after the kill", a false positive with nothing to do with the fault.

An earlier discovery pass covered a DIFFERENT §9 row instead of leaving it a pure gap:
"Keep crashing CP job N times (retry resilience)". At the time, every job-related endpoint
wrapped anywhere in this framework — lib/capellaAPI/'s CapellaAPI.jobs()/.deployement_jobs()/
get_dataplane_job_info/get_maintenance_job(s), couchbase_utils/capella_utils/'s
CapellaUtils.get_deployment_jobs, even fusion_cp_billing_monitor.trigger_internal_job's one
write path — was either read-only or (for trigger_internal_job) only made a cron job run
EARLY, never cancelled/killed one. So rather than guess at a target, that pass triggered an
ordinary fusion rebalance and polled CapellaUtils.get_deployment_jobs verbatim through its
whole lifecycle (plus a grace window after, to catch the async ScaleDown/TearDown jobs
ACCELERATION.md describes running once the rebalance call itself returns), asserting
nothing — it existed purely to produce the job type(s)/id(s)/timing data a real chaos test
would need, which nothing in this codebase had actually captured before. What it found: a
fusion rebalance is tracked by exactly ONE deployment job (type "DeployG2Cluster"), and —
with no injected fault at all — a real run still saw the CP retry that same job id 3 times,
past two genuine transient failures (a shard-download timeout, then a guest-volume-detach
timeout), before it succeeded. That discovery test has since been removed (its job served —
_log_deployment_jobs_snapshot below is the one piece of it still in active use), but the
finding stands and is what test_kill_cp_job_during_scaling below builds on.

A kill endpoint surfaced afterward via a Capella support Slack thread:
`POST /internal/support/jobs/{jobId}/kill`, used there to abort a job already stuck
retrying against a bad cluster spec, as incident remediation before applying a fix. It is
not wrapped in lib/capellaAPI (a submodule this repo does not modify) or anywhere else in
this framework, so `CapellaUtils.kill_deployment_job` (couchbase_utils/capella_utils/
dedicated.py) adds it directly, calling `CapellaAPI._urllib_request` rather than the more
commonly-used `do_internal_request` — see that method's docstring for why: the latter
recurses on every 401 with no attempt cap, which is fine for a token that merely expired
but not for a persistent permissions problem, and a first run against this endpoint hit
exactly that (~470s of silent recursion ending in "maximum recursion depth exceeded",
before this was fixed to fail fast with the real status code instead).

test_kill_cp_job_during_scaling is what that endpoint enables: it finds the DeployG2Cluster
job driving an in-flight fusion rebalance and kills it ONCE while "processing" (no repeat
kills — one is enough to exercise the interesting behaviour below). Confirmed with the
fusion/CP team: a job this test kills does NOT get auto-retried the way a job
that fails for a genuine transient reason does — it stays "killed" forever, leaving the
cluster mid-scale with no further progress (observed directly: a real run's cluster
oscillated "not running"/"scaling" for 25+ minutes after a kill). The sanctioned
remediation is a "no-op deploy" — re-submitting the exact same target spec the rebalance
was already driving toward, which either resumes the same job id or has the CP mint a new
one; the test accepts and records either. After that, it asserts the cluster ends up in a
RESOLVED state (rebalance completes, or fails cleanly and the cluster stays usable) with
no orphaned accelerator instances or guest volumes left behind, rather than stuck. Caveat
worth remembering: the Slack thread only confirms the kill endpoint itself is sanctioned
for a job that is ALREADY failing — nothing there (or anywhere else in this codebase)
confirms killing an otherwise-healthy, currently-succeeding job behaves the same way.

NO-OP DEPLOY MECHANISM — CapellaUtils.redeploy_cluster_spec_v4, not redeploy_cluster_spec.
The first attempt at "re-submit the same target spec" POSTed to the INTERNAL
`/v2/.../clusters/{clusterId}/specs` endpoint (redeploy_cluster_spec, the same one
async_rebalance_capella already uses to trigger every rebalance in this framework) —
confirmed against a real cluster, that endpoint has no diff-and-noop detection at all: it
treats the resubmission as a brand new competing scale and rejects it outright with
"scaling already in progress", since the killed job already left the cluster mid-scale.
Per Capella support documentation on triggering a no-op deploy, the PUBLIC v4 Management
API (`PATCH /v4/organizations/{orgId}/projects/{projectId}/clusters/{clusterId}` with the
cluster's OWN current `serviceGroups`, fetched immediately beforehand) is what actually
gets treated as a no-op: the CP calculates zero diff against what it already has on
record, so Fleet Manager still runs its normal DeployG2Cluster pipeline checks but
completes in seconds with `"reason": "noOp"`, signalled by `202 Accepted` — the same status
code this test gates `accepted` on. redeploy_cluster_spec_v4 (couchbase_utils/
capella_utils/dedicated.py) implements exactly that GET-then-PUT-unmodified sequence
against the submodule's `ClusterOperationsAPIs.update_cluster`/`fetch_cluster_info`
(lib/capellaAPI/capella/dedicated/CapellaAPI_v4.py), using a bearer token minted
per-attempt via CapellaUtils.create_v4_api_key() — v4 calls need a real bearer token, not
tenant.api_secret_key/api_access_key the way v2 calls in this codebase do.

KNOWN, CONFIRMED-TERMINAL LIMITATION (jenkins_output9.log): redeploy_cluster_spec_v4 ALSO
gets rejected while the cluster sits in "scaling" — `422 {"message": "Unable to modify the
cluster specifications at this time. The clusters status is 'scaling' is not valid for
performing a deployment. Only the status' Draft or Healthy are allowed."}`. So
update_cluster gates on cluster.status itself before it ever reaches any diff-against-
current-state logic; the documented "resubmit unchanged serviceGroups -> zero diff -> noOp"
recipe only applies to a cluster that is ALREADY Draft/Healthy, not one left mid-scale by a
killed job. That same run's cluster was still "scaling" 7+ hours later with no sign of
self-resolving. As of this run, there is no confirmed way through either redeploy_
cluster_spec (v2) or redeploy_cluster_spec_v4 (v4) — the only two endpoints this codebase
has tried — to un-stick a cluster left mid-scale by a killed job; both are blocked by the
same underlying cluster.status precondition. CPJ3 now detects this specific rejection and
stops retrying immediately (every attempt gets the identical answer — it is not a
"still settling from the kill" condition retrying can wait out), recording it as a WARNING-
severity finding rather than quietly exhausting cp_noop_deploy_attempts. This is itself
exactly the kind of finding this test exists to produce, per the caveat several paragraphs
up — worth raising with the fusion/CP team directly rather than something this test's own
client code can work around.

RELATED BUG, FIXED: CPJ4's self.wait_for_rebalances([rebalance_task]) call never passed a
`timeout`, so it silently used hostedOPD.wait_for_rebalances' own default of 28800s (8h)
regardless of this test's `rebalance_timeout` conf param (3600s) — confirmed directly: with
the no-op deploy rejected and the cluster stuck in "scaling", CPJ4 sat polling "Rebalance
is not running"/"Rebalance task status: scaling" every ~65s for 7+ hours in that same run
with no sign of ever exiting. Fixed by passing `timeout=self.rebalance_timeout` (already
collected by the shared base class's setUp from that same conf param, just never wired
through at this call site) — a real timeout now still surfaces as a caught exception after
a bounded wait, and CPJ5/CPJ6 read the cluster's actual final state instead of the stage
hanging for hours first.

The credential-tier question above WAS the problem, and is now resolved: a run against the
fixed (non-recursing) kill_deployment_job got back a clean, fast
`401 {"errorType":"Unauthorized","message":"Unauthorized"}`, confirming the ordinary
tenant JWT session (get_authorization_internal(), the same one get_deployment_jobs uses
successfully for reads) is not authorized for this write endpoint. The fix is
`pytests/dedicatedbasetestcase.py`'s existing override-token mechanism: for a
qe-/sbx-/dev-/stage- pod it reads `{sbx,dev,stage}_token_for_internal_support` from the
environment into `pod.TOKEN`, which `CommonCapellaAPI.__init__` already turns into
`self.cbc_api_request_headers` (`Authorization: Bearer <pod.TOKEN>`) — a SEPARATE header
dict from the JWT one, that this codebase's only other write call against
`/internal/support/jobs/...` (fusion_cp_billing_monitor.trigger_internal_job) already uses
for exactly this reason. kill_deployment_job now uses that header too, and logs a warning
if `pod.TOKEN` is unset (the request would otherwise carry `Authorization: Bearer None`
and fail just as predictably, for a much more obvious reason).

test_restart_kv_node_during_guest_volume_mounting / test_terminate_kv_node_during_
guest_volume_mounting automate COVERAGE.md §9's "Restart / terminate node during guest
volume mounting" row. Both are a materially bigger deal than every fault above: those
targeted an ephemeral accelerator instance (disposable by design, per ACCELERATION.md) or
a CP job; these two act on a live, data-serving KV node's own EC2 instance, mid-Phase-6
(ACCELERATION.md: detach from accelerator -> scale down IOPS -> attach to the KV node ->
dp-agent mounts at /{planUUID}/{shardNo}/guest{slot}). The attach step is AWS-visible; the
mount that follows it is not, so "the instant a guest volume first shows attached to a KV
instance" (see _find_kv_node_receiving_guest_volume) is the closest externally-observable
approximation of "mid-mount" available — there is no throttle knob for a mount the way
fusion_download_rate_limit widens the download window, since mounting is not data-transfer
bound. Both tests accept a broad range of CP recovery outcomes (documented on each test)
because how Capella's node-health/replacement machinery actually responds to a KV node
disappearing mid-rebalance is not something this codebase has exercised before either;
their own outcome is part of what answers that, same as test_kill_cp_job_during_scaling's
caveat about killing a healthy job.

Restart (`sudo reboot` over SSM) is recoverable — the instance ID and its attached EBS
volumes survive, and Couchbase Server / dp-agent are expected to come back on their own
once the OS does. Terminate (EC2 TerminateInstances) is NOT reversible for that instance;
recovery depends entirely on Capella's own node-replacement flow doing the right thing for
a KV node lost mid-topology-change, which is unconfirmed going in. Both were written and
are provided because they were explicitly requested with that risk understood — read each
test's docstring before running it against a cluster anything else depends on staying up.

test_terminate_kv_node_after_guest_volumes_attached automates COVERAGE.md §9's "Terminate
node after all guest volumes attached" row — a LATER boundary than the mounting-phase
tests above. The rebalance (Phase 7) has already completed and every guest volume is
already attached; the fault lands during Phase 8's background/extent migration, which
copies each guest volume's data into the node's own managed storage after the rebalance
call returns. Unlike the mounting-phase tests, this window CAN be widened with a knob:
fusion_migration_rate_limit (the same setting _pause_migration/_resume_migration manage
elsewhere in this file) is set low BEFORE the rebalance triggers, so migration is still
genuinely copying data when the termination lands, then raised back to the default
(75 MB/s) once a replacement node is confirmed holding the re-attached guest volume(s) —
so the rest of the test does not also crawl, same reasoning as
fusion_download_rate_limit's role in test_dp_accelerator_crash_during_download.

test_restart_accelerator_node_mid_download automates COVERAGE.md §9's "Restart
accelerator nodes mid-download" row — a THIRD accelerator-instance fault distinct from
the two already covered elsewhere: test_dp_accelerator_crash_during_download (this file)
kills only the dp-accelerator PROCESS via pkill, leaving the instance itself untouched;
test_accelerator_node_termination_resilience / test_accelerator_stopped_mid_download
(fusion_accelerator_chaos_test.py) terminate/stop the instance. This one reboots the
accelerator's OS itself over SSM — dp-accelerator, and everything else on the instance,
restarts fresh along with it. Reuses _find_downloading_accelerator to pick a target
actually caught mid-download and _instance_uptime_seconds to confirm the reboot really
happened, the same machinery test_restart_kv_node_during_guest_volume_mounting uses for
a KV node. fusion_download_rate_limit widens the window the same single-value way as
test_dp_accelerator_crash_during_download — see that test's docstring for why a
low-then-high pair does not work (mid-flight rate changes have no effect).

test_delete_s3_log_file_accelerator_cli_failure automates COVERAGE.md §9's "Delete log
file from S3 -> accelerator-cli failure" row — a FOURTH accelerator-side fault, but
unlike the three above (process kill, instance reboot/terminate) it never touches the
accelerator at all: the accelerator process and instance are both healthy, but an object
its manifest told it to download from S3 is simply gone, so accelerator-cli itself fails
to read it. Uses FusionAWSUtil.corrupt_fusion_log_store (also used by
fusion_cluster_destroy_test.py's test_destroy_in_scale_failed_state) with num_folders=0,
which deletes only individual file objects rather than whole vBucket/shard folders — i.e.
"a log file" (singular, matching the row title), not "a shard's whole log store". That
distinction matters: a missing individual file is the narrower fault the CP's per-shard
DCP fallback is expected to actually recover from, whereas a missing whole folder wipes a
shard's data with nothing to fall back to. COVERAGE.md's separate "Delete S3 bucket -> CP
disables fusion, DCP fallback" row is a different, much larger-blast-radius scenario (the
entire bucket, expected to disable fusion outright) and is intentionally NOT what this
test does.

KNOWN RISK carried over directly from test_destroy_in_scale_failed_state's own findings:
corrupting the log store before a rebalance has not reliably been observed to make the CP
transition to a failed state — it can sit in "scaling" indefinitely instead of ever
detecting the missing objects (deleting the ENTIRE bucket was worse still: Jenkins build
16485 saw the CP hot-loop "Replacing Node" forever). Where the destroy test treats that as
a non-issue (its actual point is validating destroy-time cleanup regardless of outcome),
that exact hang IS the failure mode this test exists to catch, so it is a hard failure
here rather than a soft/informational one.

test_kill_memcached_during_rebalance automates STAGE_TEST_MATRIX.md §Negative boundary D's
"ns_server loses PlanUUID (ErrFusionPlanNotFound)" row, previously a gap. It kills
memcached on every KV node holding an attached guest volume while CBS is actively
rebalancing (phase 7 — the PlanUUID-driven vBucket movement fusion hands off to ns_server
once every guest volume is mounted), gated on the same _wait_for_cbs_rebalance_running helper
test_abort_rebalance_invalidates_manifest uses to land its stopRebalance call at the right
boundary — moved, along with _cbs_rebalance_state and _run_on_cluster_node, from
fusion_accelerator_chaos_test.py up into the shared FusionAcceleratorLifecycleTest base so
both files can reach them. Unlike the on-prem equivalent
(storage/fusion/fusion_failover_rebalance.py::test_fusion_rebalance_crash_retry), which
manually re-invokes its rebalance script after the crash, this test never re-triggers
anything: an earlier discovery pass (see the module docstring above) already established
that a fusion rebalance is tracked by exactly one CP job that retries itself past genuine
transient failures, so this kills memcached once and just keeps waiting on the ORIGINAL
rebalance_task, on the
expectation that the same job-level retry covers a transient memcached restart. It reuses
_newest_job_since (built for test_kill_cp_job_during_scaling) to record the job's
attempts/errors before and after the kill as evidence of what the CP actually did — a
cluster that reaches 'healthy' on its own is the passing outcome; if it does not, that
gap between "expected to self-heal" and "actually got stuck" is the finding.

KNOWN BUG, FIXED: a real Jenkins run (jenkins_output30.log) showed the kill landing on
accelerator instances, not KV nodes — the target selection read a guest volume's AWS
Attachments field and treated anything not literally "unattached" as a valid target,
but that field reports whatever instance CURRENTLY holds the volume, which is the
ACCELERATOR until the CP hands it off to a KV node (phase 6). The fix cross-checks
against _kv_instance_ids() (the same positive couchbase-cloud-function=couchbase tag
match the guest-volume-mounting tests use) and polls until at least one guest volume is
actually observed on a real KV node, rather than reading placement once right after
_wait_for_accelerator_fleet_stable — which only confirms phases 4-5 (download) are
done, not that phase 6 (transfer to a KV node) has happened yet.
"""

import datetime
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from bucket_utils.bucket_ready_functions import JavaDocLoaderUtils
from capella_utils.dedicated import CapellaUtils as CapellaAPI
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from cb_server_rest_util.fusion.fusion_api import FusionRestAPI
from membase.api.rest_client import RestConnection

from .fusion_accelerator_lifecycle_test import FusionAcceleratorLifecycleTest


class FusionCPResiliencyTest(FusionAcceleratorLifecycleTest):
    """CP / accelerator-agent resiliency fault-injection tests (§9 of the E2E test plan)."""

    # ------------------------------------------------------------------
    # dp-accelerator helpers
    # ------------------------------------------------------------------

    def _crash_dp_accelerator(self, instance_id, attempts=5, retry_interval=3):
        """Kill the dp-accelerator process on `instance_id`; return the last SSM result.

        Finds the live PID and kills it in the SAME remote command, rather than a
        separate pgrep-then-pkill round trip: the unit cycles fast enough ('activating
        auto-restart' — see the module docstring) that a process caught alive by an
        EARLIER, separate SSM call (_find_downloading_accelerator's pgrep check) can
        already have exited by the time a LATER, separate kill command reaches the
        instance. That gap — stage transition + a fresh SSM round trip — was enough to
        miss it entirely in one observed run (kill sent 5s after selection came back
        "no process matched"). Checking and killing inside one remote command removes
        the SSM-latency round trip from that race; retrying a few times covers the
        smaller remaining chance that a single attempt still lands inside the process's
        own restart-backoff gap.

        Returns as soon as an attempt actually finds and kills a PID (stdout contains
        "KILLED="). If every attempt finds nothing, returns the last result — which by
        then means the process was genuinely never caught running across the whole
        retry window, not just one unlucky sample.
        """
        cmd = (
            "PIDS=$(pgrep -f dp-accelerator || true); "
            "if [ -n \"$PIDS\" ]; then "
            "sudo kill -9 $PIDS 2>/dev/null || kill -9 $PIDS; "
            "echo \"KILLED=$PIDS\"; "
            "else "
            "echo NO_PID; "
            "fi")
        result = {}
        for attempt in range(1, attempts + 1):
            self.log.info(f"[ssm] {instance_id}: kill attempt {attempt}/{attempts}")
            result = self.fusion_aws_util.ec2.run_shell_command(instance_id, cmd)
            stdout = result.get("stdout", "")
            self.log.info(
                f"[ssm] {instance_id} attempt {attempt}/{attempts} -> "
                f"success={result.get('success')} rc={result.get('return_code')} "
                f"stdout={stdout!r} stderr={result.get('stderr', '')!r}")
            if result.get("success") and "KILLED=" in stdout:
                return result
            if attempt < attempts:
                time.sleep(retry_interval)
        return result

    def _dp_accelerator_state(self, instance_id):
        """(is_running, pids, n_restarts) for the dp-accelerator process on `instance_id`.

        Checked by live PID (`pgrep -f dp-accelerator`), not systemd ActiveState:
        discovery on a real accelerator caught dp-accelerator.service in 'activating
        (auto-restart)', not 'active', meaning it legitimately cycles through
        short-lived runs rather than sitting resident — is-active would stay flaky even
        against the right unit name. A live PID is the literal precondition for what
        this test is about to do (pkill), so it is checked directly instead.

        NRestarts is still read from systemd (informational only, not gated on) since
        it is otherwise-unavailable evidence of how often the unit has cycled.
        """
        result = self.fusion_aws_util.ec2.run_shell_command(
            instance_id,
            "PIDS=$(pgrep -f dp-accelerator || true); "
            "RESTARTS=$(systemctl show dp-accelerator --property=NRestarts --value "
            "2>/dev/null || echo unknown); "
            "echo \"PIDS=$PIDS\"; echo \"RESTARTS=$RESTARTS\"")
        pids, n_restarts = "", "unknown"
        for line in (result.get("stdout") or "").splitlines():
            if line.startswith("PIDS="):
                pids = line.split("=", 1)[1].strip()
            elif line.startswith("RESTARTS="):
                n_restarts = line.split("=", 1)[1].strip()
        return bool(pids), pids, n_restarts

    def _debug_verify_ssm(self, instance_id):
        """TEMPORARY — remove once SSM reachability on accelerators is independently
        confirmed. Runs a trivial, dp-accelerator-unrelated command over SSM and logs
        the raw result with a distinctive banner, so the Jenkins log has an unambiguous,
        human-readable proof that SSM actually reached this instance.
        """
        result = self.fusion_aws_util.ec2.run_shell_command(
            instance_id, "echo SSM_DEBUG_CHECK_OK; hostname; date -u; whoami")
        self.log.info(
            f"########## TEMP SSM DEBUG CHECK on {instance_id} ##########\n"
            f"success={result.get('success')} return_code={result.get('return_code')}\n"
            f"stdout:\n{result.get('stdout')}\n"
            f"stderr:\n{result.get('stderr')}\n"
            f"########## END TEMP SSM DEBUG CHECK ##########")
        return result

    def _discover_agent_processes(self, instance_id):
        """TEMPORARY diagnostic. Already served its purpose once — a prior run's output
        is what confirmed the real unit is `dp-accelerator.service` (not `dp-agent`) and
        caught it in `activating (auto-restart)` rather than `active`. Left in place for
        one more confirmation pass now that the rest of this file has been corrected to
        match; safe to remove once that is verified.
        """
        result = self.fusion_aws_util.ec2.run_shell_command(
            instance_id,
            "echo ---SYSTEMD_UNITS---; "
            "systemctl list-units --type=service --all --no-legend 2>&1 "
            "| grep -iE 'dp-|accel|fusion' || echo NONE_MATCHED; "
            "echo ---PROCESSES---; "
            "ps -eo pid,cmd 2>&1 | grep -iE 'dp-|accel|fusion' | grep -v grep "
            "|| echo NONE_MATCHED")
        self.log.info(
            f"########## TEMP AGENT-NAME DISCOVERY on {instance_id} ##########\n"
            f"success={result.get('success')} return_code={result.get('return_code')}\n"
            f"stdout:\n{result.get('stdout')}\n"
            f"stderr:\n{result.get('stderr')}\n"
            f"########## END TEMP AGENT-NAME DISCOVERY ##########")
        return result

    def _find_downloading_accelerator(self, rebalance_task, timeout=None):
        """Search the fleet for an accelerator whose dp-accelerator process is running.

        Returns (instance_id, tried) on success, or (None, tried) if the search window
        elapses or the rebalance finishes first. `tried` is every (instance_id, state)
        pair observed, for a clear diagnosis when nothing qualifies.

        Guest-volume attach at 16000 IOPS (what list_accelerator_instances matches)
        starts before the download and lasts until the volume is handed off to a KV
        node, but shards vary wildly in size and some finish in single-digit seconds.
        Rather than grab the first candidate and assert on it — which fails on a fast
        shard for a reason unrelated to the fault under test — this tries every
        currently-downloading accelerator and returns the first one actually caught
        with a live dp-accelerator process. Instances seen without one are not rechecked.
        """
        timeout = timeout or int(self.input.param("dp_accelerator_target_timeout", 600))
        deadline = time.time() + timeout
        tried = []
        seen = set()
        while time.time() < deadline:
            if rebalance_task.state in self._FAILED_STATES:
                self.fail(
                    f"Rebalance failed while searching for a downloading accelerator: "
                    f"{rebalance_task.state}")
            candidates = self.fusion_aws_util.list_accelerator_instances(
                self._accelerator_filter(), log="DpAcceleratorCrashCandidates")
            for inst in candidates:
                instance_id = inst["InstanceId"]
                if instance_id in seen:
                    continue
                seen.add(instance_id)
                if not self.fusion_aws_util.ec2.is_instance_ssm_ready(instance_id):
                    tried.append((instance_id, "ssm-not-ready"))
                    continue
                is_running, pids, n_restarts = self._dp_accelerator_state(instance_id)
                tried.append((instance_id, f"pid={pids}" if is_running else "no-pid"))
                if is_running:
                    self.log.info(
                        f"Selected {instance_id} as the dp-accelerator crash target "
                        f"(pid(s)={pids}, NRestarts={n_restarts} before the kill)")
                    return instance_id, tried
            if rebalance_task.state == "healthy":
                break
            self.log.info(
                f"[target-search] {len(candidates)} candidate(s) this poll, "
                f"{len(seen)} inspected so far, none with a live process yet — "
                f"{int(deadline - time.time())}s remaining")
            time.sleep(10)
        return None, tried

    # ------------------------------------------------------------------
    # Boundary B: dp-accelerator agent crashes mid-download
    # ------------------------------------------------------------------

    def test_dp_accelerator_crash_during_download(self):
        """
        Kill the dp-accelerator process on an accelerator while it is downloading its
        shard from S3, and assert the CP still gets the data onto the cluster.

        STAGE_TEST_MATRIX §Negative, boundary B. See the module docstring for the
        ACCELERATION.md grounding, the two accepted recovery shapes, and why target
        selection searches the fleet instead of grabbing the first accelerator.

        Sequence:
          1. load past the fusion threshold (the shared _load_above_threshold /
             _wait_for_log_store_sync wait now also accounts for how long the S3 sync
             itself takes to transfer that much data, not just when it starts), then
             trigger a rebalance
          2. search the fleet for an accelerator actually caught with a live
             dp-accelerator process (TEMPORARY plain-command SSM + agent-discovery
             checks also run here — see _debug_verify_ssm / _discover_agent_processes —
             to give an unambiguous log trail independent of whether a target is found)
          3. pkill -9 dp-accelerator on it
          4. accept either: systemd brings it back on the SAME instance, or a NEW
             accelerator appears that was not part of the fleet before the kill
             (fallback-replace); reject: neither, with the rebalance still stuck
          5. let the rebalance run to completion, resume migration, then validate full
             teardown
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

        target_id = None
        accel_ids_before = set()
        try:
            rebalance_task = self._trigger_rebalance()
            self.sleep(30, "Wait for the rebalance to start")

            with self._stage("CPR1: find an accelerator still downloading"):
                # Run the TEMPORARY SSM-proof + agent-name-discovery checks against the
                # FIRST accelerator seen, unconditionally — i.e. BEFORE the search
                # below, which can fail and would otherwise skip these checks entirely.
                # Costs nothing extra: this instance is already about to be queried by
                # the search anyway.
                probe_instances = self._poll_until_accelerators_appear(rebalance_task)
                if probe_instances:
                    probe_id = probe_instances[0]["InstanceId"]
                    self._debug_verify_ssm(probe_id)
                    self._discover_agent_processes(probe_id)
                else:
                    self.log.warning(
                        "No accelerator appeared to probe — skipping the SSM-proof and "
                        "agent-name-discovery checks")

                target_id, tried = self._find_downloading_accelerator(rebalance_task)
                self.assertIsNotNone(
                    target_id,
                    f"No accelerator was ever caught with a live dp-accelerator "
                    f"process — {len(tried)} candidate(s) inspected: {tried}. See the "
                    f"TEMP AGENT-NAME DISCOVERY output above for this stage. Shards "
                    f"likely finished downloading faster than this could sample them; "
                    f"raise create_end, lower fusion_download_rate_limit, or ease off "
                    f"fusion_min_split_size_gb to widen the download window, or raise "
                    f"dp_accelerator_target_timeout to search longer.")

                # Snapshot the WHOLE fleet right NOW, at the moment the fault is about
                # to land — NOT at the start of this stage, which can take minutes
                # (observed: 122s) while the fleet is still ramping up. CPR3 diffs
                # against this to tell "the CP redeployed because of the kill" apart
                # from "another shard's accelerator that was already running finally
                # got listed". Capturing this too early caused exactly that false
                # positive in an earlier run: 4 instances already visible 16s before
                # the target was even picked were reported as "new" after the kill.
                accel_ids_before = {i.get("InstanceId") for i in
                                    self._list_accelerator_instances_by_tag(
                                        log="DpAcceleratorCrashFleet")}

            with self._stage("CPR2: kill dp-accelerator"):
                result = self._crash_dp_accelerator(target_id)
                self.assertIn(
                    "KILLED=", result.get("stdout", ""),
                    f"Never found a live dp-accelerator process on {target_id} to "
                    f"kill across every retry attempt (last result: {result}) — "
                    f"either it finished/cycled out faster than the retries could "
                    f"catch it, or SSM itself failed to deliver the command.")

            outcome = None
            recovery_timeout = int(
                self.input.param("dp_accelerator_recovery_timeout", 900))
            with self._stage("CPR3: the CP recovers from the crash"):
                deadline = time.time() + recovery_timeout
                while time.time() < deadline:
                    if rebalance_task.state in self._FAILED_STATES:
                        outcome = f"clean failure ({rebalance_task.state})"
                        break
                    is_running, pids, n_restarts = self._dp_accelerator_state(target_id)
                    if is_running:
                        outcome = (f"systemd restarted dp-accelerator on {target_id} "
                                   f"(pid(s)={pids}, NRestarts={n_restarts})")
                        break
                    live_by_tag = {i.get("InstanceId") for i in
                                   self._list_accelerator_instances_by_tag(
                                       log="DpAcceleratorCrashRecovery")}
                    new_instances = live_by_tag - accel_ids_before
                    if new_instances:
                        outcome = (f"CP redeployed: new accelerator(s) "
                                   f"{sorted(new_instances)} appeared after the kill, "
                                   f"{target_id} still has no dp-accelerator process")
                        break
                    if rebalance_task.state == "healthy":
                        outcome = "rebalance completed without dp-accelerator recovering"
                        break
                    self.log.info(
                        f"[dp-accelerator-crash] {target_id} running={is_running} "
                        f"task_state={rebalance_task.state} — "
                        f"{int(deadline - time.time())}s remaining")
                    time.sleep(20)
                self.assertIsNotNone(
                    outcome,
                    f"Neither systemd nor the CP reacted to the crash within "
                    f"{recovery_timeout}s: {target_id} still has no dp-accelerator "
                    f"process, no replacement accelerator appeared, and the rebalance "
                    f"neither completed nor failed — the download is stuck on a dead "
                    f"agent.")
                self.log.info(f"dp-accelerator-crash outcome: {outcome}")
                self._record_issue(
                    "CPR3: dp-accelerator-crash outcome (informational)", outcome,
                    severity="INFO")

            try:
                self.wait_for_rebalances([rebalance_task])
            except Exception as e:
                self.log.warning(
                    f"Rebalance did not complete cleanly ({e}) — continuing to the "
                    f"cleanup assertions, which a clean failure still has to satisfy")
        finally:
            # Must happen BEFORE the teardown/EBS-cleanup checks below: guest volumes
            # are only reclaimed as background migration completes, so validating
            # teardown while still frozen hangs until EBS_CLEANUP_TIMEOUT with no
            # chance of passing.
            self._restore_migration_rate_limit()

        CapellaAPI.wait_until_done(
            self.pod, self.tenant, self.cluster.id, timeout=1800)

        with self._stage("CPR4: cluster healthy and data readable"):
            state = CapellaAPI.get_cluster_state(
                self.pod, self.tenant, self.cluster.id)
            self.assertEqual(
                state.lower(), "healthy",
                f"Cluster is not healthy after the dp-accelerator crash: {state}")
            self._run_read_workload("after dp-accelerator crash during download")

        with self._stage("CPR5: teardown clean after the crash"):
            self._validate_teardown(s3_bucket_name)

        self._assert_no_stage_issues()

    def _log_deployment_jobs_snapshot(self, jobs, label):
        """Log every entry from CapellaUtils.get_deployment_jobs verbatim; return
        {job_id: job_dict} so the caller can accumulate what has been seen across polls.

        Each entry is {"job": {id, type, status, attempts, errors (last 10), createdAt,
        startedAt, completedAt, ...}, "plan": {...}} — see get_deployment_jobs's own
        docstring in couchbase_utils/capella_utils/dedicated.py.
        """
        seen = {}
        if not jobs:
            self.log.info(f"[cp-jobs] {label}: 0 job(s) returned")
            return seen
        lines = []
        for entry in jobs:
            job = entry.get("job") or {}
            job_id = job.get("id")
            seen[job_id] = job
            lines.append(
                f"    id={job_id} type={job.get('type')} status={job.get('status')} "
                f"attempts={job.get('attempts')} createdAt={job.get('createdAt')} "
                f"startedAt={job.get('startedAt')} completedAt={job.get('completedAt')} "
                f"errors={job.get('errors')}")
        self.log.info(f"[cp-jobs] {label}: {len(jobs)} job(s):\n" + "\n".join(lines))
        return seen

    # ------------------------------------------------------------------
    # Boundary F: kill the CP job driving a fusion rebalance, once
    # ------------------------------------------------------------------

    def _newest_job_since(self, since_time, label="cp-job-kill poll"):
        """Return the most-recently-created job dict with createdAt >= since_time, or
        None if none exist yet. A single snapshot — callers poll it themselves.

        Deliberately NOT scoped to a specific job id, and re-resolved by every caller
        on every check rather than cached: whether the CP reuses the same job id when
        it requeues a KILLED job, or mints a brand new one, is exactly the kind of
        thing this test cannot assume in advance (see the module docstring's caveat).
        Always asking "whichever job is newest for this rebalance right now" is what
        lets the kill loop notice and start tracking a replacement job if one shows up
        — the same approach wait_for_deployment_job (fusion_cp_resource_monitor.py)
        already uses for a cluster that accumulates many historical jobs, for the same
        underlying reason (picking a fixed id once could mean tracking a stale job
        instead of the one now doing the work).

        Every call logs the full raw get_deployment_jobs response via
        _log_deployment_jobs_snapshot, not just whatever this method narrows it down
        to — so the Jenkins log always shows every job the CP is tracking at each
        check, not only the one this test currently cares about.
        """
        try:
            jobs = CapellaAPI.get_deployment_jobs(
                self.pod, self.tenant, self.cluster.id)
        except Exception as e:
            self.log.warning(f"[cp-job-kill] get_deployment_jobs failed: {e}")
            return None
        self._log_deployment_jobs_snapshot(jobs, label)
        candidates = []
        for entry in (jobs or []):
            job = entry.get("job") or {}
            created_at = job.get("createdAt")
            if created_at and self.cp_monitor._parse_cp_timestamp(created_at) >= since_time:
                candidates.append(job)
        if not candidates:
            return None
        return max(candidates, key=lambda j: j.get("createdAt") or "")

    def _find_job_for_rebalance(self, since_time, timeout=180):
        """Poll _newest_job_since until the job created for this rebalance appears.

        An earlier discovery pass (see the module docstring above) already
        established that a fusion rebalance creates exactly one relevant job (type
        DeployG2Cluster) to start; this waits for it and returns its id, or None if
        it never shows up within timeout.
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            job = self._newest_job_since(since_time, label="CPJ1: searching for the job")
            if job:
                self.log.info(
                    f"[cp-job-kill] tracking job {job.get('id')} "
                    f"(type={job.get('type')}) for this rebalance")
                return job.get("id")
            time.sleep(5)
        return None

    def test_kill_cp_job_during_scaling(self):
        """
        Kill the CP job driving a fusion rebalance once, while it is actively
        processing, then re-submit the SAME target cluster spec (a "no-op deploy") to
        un-stick it, and assert the cluster ends up RESOLVED — the rebalance
        completes, or fails cleanly and the cluster stays usable — with no orphaned
        accelerator instances or guest volumes left behind, rather than stuck.

        COVERAGE.md §9 "Keep crashing CP job N times (retry resilience)" — despite the
        row title, ONE kill is enough to exercise the interesting behaviour (a job
        this test kills never auto-recovers on its own, see NO-OP DEPLOY below), so
        this does not repeat the kill. See the module docstring for the full
        background: `CapellaUtils.kill_deployment_job` (POST /internal/support/jobs/
        {jobId}/kill) was surfaced via a Capella support Slack thread where it aborted
        a job ALREADY stuck retrying against a bad spec, as incident remediation.
        Nothing confirms in advance that killing an otherwise-healthy,
        currently-succeeding job behaves the same way — this test's own result is
        part of what answers that, so treat a first run's outcome as a finding to
        discuss with the fusion/CP team, not an established baseline.

        An earlier discovery pass (see the module docstring above) already
        established the ground truth this needs: a fusion rebalance is tracked by
        exactly ONE deployment job (type DeployG2Cluster), and a run with no injected
        fault at all still saw the CP retry that same job id 3 times, past two
        genuine transient failures, before succeeding — so "the CP retries a failed
        job" is already known-working without any help from this test. What this
        test adds is deliberately causing one such failure instead of waiting for a
        real one.

        The kill step is skipped if the job is never observed "processing" (nothing
        in flight to kill) or it already reached a terminal status before this test
        could get to it.

        NO-OP DEPLOY (CPJ3). Confirmed directly with the fusion/CP team: unlike a job
        that fails for a genuine transient reason (auto-retried, per the earlier
        discovery pass described in the module docstring above), a job this test
        KILLS does not
        get auto-retried — it stays "killed" forever and the cluster is left mid-scale
        with no further progress on its own (observed directly: a real run's cluster
        oscillated "not running"/"scaling" for 25+ minutes after a kill with no sign of
        resolving). The sanctioned remediation is to re-submit the exact same target
        cluster spec this rebalance was already driving toward — CapellaUtils.
        redeploy_cluster_spec_v4 (the PUBLIC v4 Management API: GET the cluster's
        current name/description/support/serviceGroups, PUT them back unmodified),
        NOT redeploy_cluster_spec (the internal v2 /specs POST) or CapellaUtils.scale()
        — confirmed directly against a real cluster, the v2 POST gets rejected outright
        with "scaling already in progress" instead of being treated as a no-op, since
        the killed job already left the cluster mid-scale; and scale() wraps that same
        v2 call in an unbounded retry loop with no real exit if the cluster stays stuck
        (see both methods' docstrings in couchbase_utils/capella_utils/dedicated.py).
        The v4 bearer token this needs is minted per-attempt via
        CapellaUtils.create_v4_api_key() (tenant.api_secret_key/api_access_key are not
        usable for v4 calls at all) and torn down via delete_v4_api_key() once CPJ3
        finishes, success or not. Either the SAME job id resumes (status flips off
        "killed") or the CP mints a brand NEW job id for the same target; CPJ3 accepts
        both and only records whichever happened, since neither is confirmed to be more
        "correct" than the other ahead of time.
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")
        self._apply_fusion_config_from_params()
        self._apply_fusion_sync_threshold()
        self._load_above_threshold()

        job_find_timeout = int(self.input.param("cp_job_find_timeout", 180))
        processing_wait_timeout = int(
            self.input.param("cp_job_processing_wait_timeout", 300))
        post_kill_settle = int(self.input.param("cp_job_post_kill_settle_secs", 30))

        rebalance_task = self._trigger_rebalance()
        since_time = getattr(rebalance_task, "since_time", None) or \
            datetime.datetime.now(datetime.timezone.utc)

        job_id = None
        with self._stage("CPJ1: find the CP job driving this rebalance"):
            job_id = self._find_job_for_rebalance(since_time, timeout=job_find_timeout)
            self.assertIsNotNone(
                job_id,
                f"No deployment job appeared for this rebalance within "
                f"{job_find_timeout}s (since {since_time}) — nothing to kill.")

        kills_done = 0
        with self._stage("CPJ2: kill the job once while processing"):
            deadline = time.time() + processing_wait_timeout
            job = None
            while time.time() < deadline:
                job = self._newest_job_since(
                    since_time, label="CPJ2: waiting for 'processing'")
                if job and job.get("status") in ("processing", "complete", "failed"):
                    break
                time.sleep(5)
            if not job:
                self._record_issue(
                    "CPJ2: kill attempt",
                    f"no job could be read for this rebalance before the "
                    f"{processing_wait_timeout}s wait expired — nothing to kill",
                    severity="WARNING")
            elif job.get("id") != job_id:
                self.log.info(
                    f"[cp-job-kill] now tracking job {job.get('id')} "
                    f"(previously {job_id}) — the CP requeued this rebalance "
                    f"under a different job id")
                job_id = job.get("id")

            if job and job.get("status") != "processing":
                self.log.info(
                    f"[cp-job-kill] job {job_id} reached status="
                    f"{job.get('status')} before the kill could land — "
                    f"nothing left in flight to kill")
            elif job:
                attempts_before = job.get("attempts")
                try:
                    status_code, content = CapellaAPI.kill_deployment_job(
                        self.pod, self.tenant, job_id)
                    self.log.info(
                        f"[cp-job-kill] kill on job {job_id} (was processing, "
                        f"attempts={attempts_before}) -> status_code="
                        f"{status_code} content={content}")
                    if status_code in (200, 202, 204):
                        kills_done += 1
                    else:
                        self._record_issue(
                            "CPJ2: kill attempt rejected",
                            f"kill_deployment_job returned "
                            f"status_code={status_code} content={content} — "
                            f"the CP may not support killing a "
                            f"currently-healthy job the way it does an "
                            f"already-stuck one (see the module docstring's "
                            f"caveat).", severity="INFO")
                except Exception as e:
                    self._record_issue(
                        "CPJ2: kill attempt raised", str(e), severity="WARNING")

                if kills_done:
                    time.sleep(post_kill_settle)
                    after = self._newest_job_since(
                        since_time, label="CPJ2: after kill")
                    self.log.info(
                        f"[cp-job-kill] job {job_id} after the kill: {after}")
                    self._record_issue(
                        "CPJ2: kill outcome (informational)",
                        f"before={job}, after={after}", severity="INFO")

        self.log.info(f"[cp-job-kill] performed {kills_done} kill(s) on job {job_id}")

        with self._stage("CPJ3: no-op deploy to un-stick a job the CP killed"):
            if kills_done == 0:
                self.log.info(
                    "No kill actually landed (kills_done=0) — nothing to "
                    "un-stick, skipping the no-op deploy")
            else:
                # Per the fusion/CP team, a killed deployment job does NOT get
                # auto-retried the way a job that failed for a real transient
                # reason does (an earlier discovery pass saw a no-fault run get
                # retried 3 times on its own) -- it just stays "killed" forever,
                # with the cluster left mid-scale and no further progress.
                # Re-submitting the current target spec is the CP-sanctioned
                # way to un-stick that.
                #
                # CapellaUtils.redeploy_cluster_spec (the internal v2 /specs
                # POST) does NOT work for this -- confirmed directly against a
                # real cluster, the CP rejects it outright with "scaling
                # already in progress", since the killed job already left the
                # cluster mid-scale and that endpoint has no diff-and-noop
                # detection; it is treated as a brand new competing scale, not
                # a replay. redeploy_cluster_spec_v4 (the public v4 Management
                # API) is what actually works: it fetches the cluster's OWN
                # current name/description/support/serviceGroups and PUTs them
                # back unmodified, so the CP calculates zero diff against what
                # it already has on record and completes the DeployG2Cluster
                # pipeline in seconds with "reason": "noOp" instead of
                # rejecting a competing scale. See both methods' docstrings in
                # couchbase_utils/capella_utils/dedicated.py.
                v2_key_id = v4_key_id = bearer_token = None
                try:
                    v2_key_id, v4_key_id, bearer_token = \
                        CapellaAPI.create_v4_api_key(
                            self.pod, self.tenant,
                            name_prefix="fusion-cp-job-noop")
                    self.assertIsNotNone(
                        bearer_token,
                        "Failed to mint a v4 API key/bearer token needed for "
                        "the no-op deploy")

                    max_attempts = int(
                        self.input.param("cp_noop_deploy_attempts", 5))
                    retry_interval = int(
                        self.input.param(
                            "cp_noop_deploy_retry_interval_secs", 15))
                    # EXPERIMENTAL -- see redeploy_cluster_spec_v4's own
                    # docstring for why: the only evidence for a "Force" flag
                    # on this endpoint is that the killed DeployG2Cluster
                    # job's own payload carried 'Force': False, and the Fleet
                    # Manager UI's "redeploy" button was reported to work
                    # against the same kind of stuck cluster where this
                    # test's plain (force=False) resubmit got a clean 422.
                    # Default True so this gets exercised without extra conf
                    # plumbing; set cp_noop_deploy_force=False to go back to
                    # the plain resubmit if this turns out to be a red
                    # herring.
                    force = self.input.param("cp_noop_deploy_force", True)
                    accepted = False
                    for attempt in range(1, max_attempts + 1):
                        try:
                            status_code, content = \
                                CapellaAPI.redeploy_cluster_spec_v4(
                                    self.pod, self.tenant, self.cluster,
                                    bearer_token, force=force)
                        except Exception as e:
                            self._record_issue(
                                f"CPJ3: no-op deploy attempt {attempt} raised",
                                str(e), severity="WARNING")
                            break
                        self.log.info(
                            f"[cp-noop-deploy] attempt {attempt}/"
                            f"{max_attempts} -> status_code={status_code} "
                            f"content={content}")
                        if status_code == 202:
                            accepted = True
                            break
                        # KNOWN, CONFIRMED-TERMINAL rejection (real run,
                        # jenkins_output9.log): update_cluster gates on
                        # cluster.status itself BEFORE it ever gets to diffing
                        # the payload -- "The clusters status is 'scaling' is
                        # not valid for performing a deployment. Only the
                        # status' Draft or Healthy are allowed." A killed job
                        # leaves the cluster in exactly that "scaling" status
                        # (confirmed: still "scaling" 7+ hours later in that
                        # run, never self-resolving), so this is not a
                        # transient "still settling from the kill" condition
                        # retrying can wait out -- every attempt gets the
                        # identical rejection. Stop immediately instead of
                        # burning the rest of max_attempts pointlessly, and
                        # record it as a real (not merely informational)
                        # finding: as of this run, there is no confirmed way
                        # to un-stick a cluster left mid-scale by a killed
                        # job through either redeploy_cluster_spec (v2) or
                        # redeploy_cluster_spec_v4 (v4) -- both are blocked by
                        # the same underlying cluster.status gate.
                        content_text = (
                            content.decode("utf-8", "replace")
                            if isinstance(content, (bytes, bytearray))
                            else str(content))
                        if (status_code == 422
                                and "draft or healthy" in content_text.lower()):
                            self._record_issue(
                                f"CPJ3: no-op deploy rejected — cluster "
                                f"status gate, not a transient condition",
                                f"status_code={status_code} "
                                f"content={content_text} — the CP rejects "
                                f"ANY cluster spec update while status is "
                                f"'scaling', regardless of whether the "
                                f"payload is unchanged; a killed job leaves "
                                f"the cluster in exactly that status with no "
                                f"confirmed way out via this endpoint. "
                                f"Stopping after attempt {attempt}/"
                                f"{max_attempts} rather than retrying "
                                f"identically-rejected attempts.",
                                severity="WARNING")
                            break
                        last = attempt == max_attempts
                        self._record_issue(
                            f"CPJ3: no-op deploy attempt {attempt}"
                            + (" (final)" if last else " not accepted"),
                            f"status_code={status_code} content={content}"
                            + ("" if last else " — the cluster may still be "
                               "settling from the kill; retrying"),
                            severity="WARNING" if last else "INFO")
                        if not last:
                            time.sleep(retry_interval)

                    if accepted:
                        self.sleep(
                            post_kill_settle,
                            "Wait after the no-op deploy for the CP to react")
                        # Per the module docstring's caveat, either the SAME
                        # job id resuming (status off "killed") or a brand NEW
                        # job id are both acceptable outcomes here -- this is
                        # purely informational, not a pass/fail gate.
                        after = self._newest_job_since(
                            since_time, label="CPJ3: after no-op deploy")
                        self._record_issue(
                            "CPJ3: job state after no-op deploy "
                            "(informational)",
                            (f"job_id={after.get('id')} "
                             f"status={after.get('status')} "
                             f"attempts={after.get('attempts')} (previously "
                             f"tracked job was {job_id}) — either the same "
                             f"job id resumed or the CP minted a new one; "
                             f"both are acceptable"
                             if after else
                             "get_deployment_jobs returned no job for this "
                             "rebalance after the no-op deploy was accepted"),
                            severity="INFO")
                finally:
                    if v2_key_id or v4_key_id:
                        CapellaAPI.delete_v4_api_key(
                            self.pod, self.tenant, v2_key_id, v4_key_id,
                            bearer_token)

        with self._stage("CPJ4: the rebalance resolves (completes or fails cleanly)"):
            try:
                # KNOWN BUG, FIXED: wait_for_rebalances defaults to timeout=28800
                # (8h) when not given one explicitly, and this call site never
                # passed one -- confirmed directly (jenkins_output9.log): after
                # the no-op deploy was rejected (see CPJ3's cluster.status gate
                # finding above) and the cluster never left "scaling", this sat
                # polling "Rebalance is not running"/"Rebalance task status:
                # scaling" every ~65s for 7+ hours with no sign of ever exiting.
                # self.rebalance_timeout (from the `rebalance_timeout` conf
                # param, 3600s for this test) was already being collected by
                # the base class but never actually wired through here. Passing
                # it bounds the wait to what the conf already declared as the
                # intended budget; a real timeout still surfaces as a caught
                # exception below, then CPJ5/CPJ6 read the cluster's actual
                # final state instead of this stage hanging for hours first.
                self.wait_for_rebalances(
                    [rebalance_task], timeout=self.rebalance_timeout)
            except Exception as e:
                self.log.warning(
                    f"Rebalance did not complete cleanly ({e}) — continuing to the "
                    f"cleanup assertions, which a clean failure still has to satisfy")
            CapellaAPI.wait_until_done(
                self.pod, self.tenant, self.cluster.id, timeout=1800)
            state = CapellaAPI.get_cluster_state(
                self.pod, self.tenant, self.cluster.id)
            self.assertIn(
                state.lower(), {"healthy"} | {s.lower() for s in self._FAILED_STATES},
                f"Cluster is stuck in a non-terminal state ({state}) after "
                f"{kills_done} CP-job kill(s) — the rebalance neither completed "
                f"nor failed cleanly.")
            self.log.info(f"Cluster state after the kill loop: {state}")

        with self._stage("CPJ5: cluster is usable afterward"):
            state = CapellaAPI.get_cluster_state(
                self.pod, self.tenant, self.cluster.id)
            if state.lower() == "healthy":
                self._run_read_workload("after killing the CP job")
            else:
                self._record_issue(
                    "CPJ5: cluster not healthy after the kill loop (informational)",
                    f"cluster state={state} after {kills_done} kill(s) — CPJ4 "
                    f"already accepted this as a clean-failure outcome; skipping "
                    f"the read-workload check since there is no completed "
                    f"rebalance to have served it", severity="INFO")

        with self._stage("CPJ6: no orphan accelerator instances or volumes"):
            # Independent of whether CPJ4 resolved healthy or clean-failed —
            # a killed job's ephemeral accelerator fleet/guest volumes must
            # not survive it either way.
            self._assert_no_orphan_accelerator_resources(
                label="killing the CP job")

        self._assert_no_stage_issues()

    # ------------------------------------------------------------------
    # Boundary D: memcached killed during the CBS rebalance itself
    # ------------------------------------------------------------------

    def test_kill_memcached_during_rebalance(self):
        """
        Kill memcached on a KV node while CBS is actively rebalancing (phase 7 — the
        PlanUUID-driven vBucket movement fusion hands off to ns_server once every guest
        volume is mounted), then keep waiting on the SAME rebalance/CP job — no manual
        retry — and assert the cluster reaches 'healthy' on its own.

        STAGE_TEST_MATRIX.md §Negative, boundary D: "ns_server loses PlanUUID
        (ErrFusionPlanNotFound)". Distinct from test_abort_rebalance_invalidates_manifest
        (same boundary, fusion_accelerator_chaos_test.py), which aborts via an explicit,
        CP-visible `/controller/stopRebalance` call. This fault is a node losing memcached
        mid-move: ns_server itself decides how to react, and per ACCELERATION.md's
        Manifest Status Lifecycle a rebalance that fails here risks CBS losing track of the
        PlanUUID it was handed for this plan (-> ErrFusionPlanNotFound). Also distinct from
        test_kill_memcached_during_extent_migration (fusion_accelerator_chaos_test.py),
        which kills memcached during BACKGROUND migration (phase 8, after the CBS
        rebalance has already returned healthy) — this lands the kill DURING the CBS
        rebalance call itself, the boundary this row is actually about.

        THE WINDOW IS SMALL. A fusion rebalance moves vBuckets fast because the data is
        already local on the guest volumes — test_abort_rebalance_invalidates_manifest's
        own module docstring measured one run at 17s start-to-finish. Landing a kill
        inside that requires the same two levers that test uses to widen it (there is no
        third — /diag/eval test-conditions to stall the rebalance are not available on
        Capella dedicated):
          - `fusion_min_split_size_gb` (conf-time): smaller shards -> more of them -> more
            vBuckets moving in phase 7. Kept low here (matching the abort test's conf) for
            exactly this reason.
          - background mutation load (`kill_bg_load`, runtime): started BEFORE the
            rebalance triggers and left running through the kill. Mutations landing on a
            vBucket while it is mid-move are not yet on any guest volume, so that move has
            to chase them through DCP instead of a flat copy — the abort test's docstring:
            "more to move means longer to move it".
        A third lever, not about widening the window but about not wasting it: the kill
        lands on EVERY KV node holding an attached guest volume, not just the busiest one.
        A single-node kill risks the CP simply routing the affected vBucket movement
        through an unaffected node and the fault barely registering; hitting every node
        actually participating in phase 7 makes it far more likely the fault meaningfully
        disrupts the rebalance CBS is running, not just one shard's worth of it.

        The kill targets and CP-job baseline are both captured BEFORE the CBS-running
        wait (KMR2), not after — the kill commands fire the instant KMR2 returns True,
        with no extra AWS/CP round trips in between eating into the window. A progress
        ceiling (`kill_progress_ceiling_pct`, default 90) flags — does not fail — a run
        where the kill lands on a rebalance already nearly done, mirroring the abort
        test's own `abort_progress_ceiling_pct` safeguard against reporting a raced run
        as a clean pass.

        MIGRATION IS FROZEN (`_pause_migration`, rate limit 0) for the whole test, same
        lever `_pause_migration`'s own docstring and every chaos test that calls it uses.
        Without the freeze, guest volumes for any vBucket whose migration happens to
        finish during this test would be reclaimed on their own — a normal, unrelated
        event that would be indistinguishable from the kill having caused something to
        go wrong. With it frozen, NOTHING should be able to make a guest volume
        disappear before KMR4 below checks for exactly that, so a volume going missing
        there is real evidence of the PlanUUID-loss failure mode this test targets (CBS
        abandoning the plan and reclaiming volumes it should not have yet), not
        migration doing its normal job. Resumed (`_resume_migration`) only once KMR6
        confirms the cluster reached 'healthy' — same ordering constraint as every other
        chaos test here: guest volumes only get reclaimed as background migration
        completes, so validating teardown while still frozen would hang with no chance
        of passing.

        On-prem inspiration: storage/fusion/fusion_failover_rebalance.py::
        test_fusion_rebalance_crash_retry does the on-prem version of this fault (kill
        memcached mid Fusion-rebalance, then manually re-run run_rebalance() to retry).
        There is no on-prem parallel for what happens next, though: on-prem's
        FusionBase.run_rebalance() is a synchronous CLI script that test manually
        re-invokes on failure. A Capella rebalance is driven by a single CP job —
        An earlier discovery pass (see the module docstring above) established that a
        fusion rebalance is tracked by exactly one job (type DeployG2Cluster), and a
        completely unfaulted run
        still saw the CP retry that job 3 times past two genuine transient failures before
        succeeding — so unlike the on-prem test, this one does NOT re-trigger anything
        itself. It kills memcached once and keeps waiting on the ORIGINAL rebalance_task,
        on the expectation that the CP's own job-level retry already covers a transient
        memcached restart the same way it covers a shard-download timeout. If that
        expectation is wrong — if the retry does not actually cover this failure — the
        cluster sitting stuck below is the finding, not a bug in this test.

        Sequence:
          1. freeze background migration (_pause_migration); optionally start background
             mutation load (kill_bg_load, default True)
          2. load past the fusion threshold, trigger a rebalance
          3. wait for the accelerator fleet to stabilise (_wait_for_accelerator_fleet_
             stable) — every accelerator finished downloading and holds its guest
             volume, i.e. phases 4-5 done. This does NOT mean the volume has moved to
             its target KV node yet — see step 4.
          4. wait for phase 6 (guest volumes actually transferred to KV nodes),
             filtering on real KV-node instance IDs (_kv_instance_ids(), not just
             "attached to something") — a guest volume's AWS attachment reports
             whatever instance currently holds it, which is the ACCELERATOR until the
             CP hands it off, and a naive "not unattached" filter here previously
             ended up killing memcached on accelerator instances instead of KV nodes.
             Killing memcached before this proves nothing about phase D specifically;
             it would just be another way to fail phase 5/6.
          5. capture the kill targets (every KV node holding an attached guest volume),
             their guest volume IDs, and the CP job baseline now — all read-only, all
             done BEFORE the timed wait below, so none of it eats into the window once
             CBS is confirmed rebalancing
          6. wait for CBS to actually report a rebalance running (_wait_for_cbs_rebalance_
             running, phase 7) — the same gate test_abort_rebalance_invalidates_manifest
             uses to land its stopRebalance call at the right boundary; flag (not fail) a
             kill landing above kill_progress_ceiling_pct as an unproven run
          7. pkill -9 memcached on every one of those nodes concurrently, immediately
          8. confirm ns_server restarts memcached on all of them; stop the background load
          9. assert every guest volume captured at step 5 is still present — with
             migration frozen, none of them should have been reclaimed by anything
          10. re-read the job and record before/after as evidence of whether — and how —
              the CP reacted; scan its `errors` for a PlanNotFound/FusionPlan signature
              (informational: nothing here confirms in advance that the string will be
              present or spelled this way, only that the row's expectation is worth
              checking for)
          11. keep waiting on the ORIGINAL rebalance_task (no manual retry) until it
              resolves; assert the cluster reaches 'healthy' — not merely "resolved":
              a single recoverable, transient memcached restart is not accepted as a
              legitimate reason for the rebalance to end up permanently failed
          12. resume background migration (_resume_migration), only now that the cluster
              is confirmed healthy — must happen before step 13 or its guest-volume
              cleanup check hangs against a still-frozen cluster
          13. teardown clean (no orphaned accelerator instances or EBS volumes) and data
              readable
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")
        self._apply_fusion_config_from_params()
        self._apply_fusion_sync_threshold()
        s3_bucket_name, _ = self._capture_s3_log_store_baseline()
        self._load_above_threshold()

        if not self._pause_migration():
            self.fail(
                "Could not freeze background migration — this test needs migration "
                "frozen so nothing but the kill's own side effects could make a guest "
                "volume disappear before KMR4 checks for exactly that")

        bg_tasks = []
        try:
            if self.input.param("kill_bg_load", True):
                bg_tasks = self._start_background_load(
                    "during the memcached-kill rebalance")

            rebalance_task = self._trigger_rebalance()
            since_time = getattr(rebalance_task, "since_time", None) or \
                datetime.datetime.now(datetime.timezone.utc)
            self.sleep(30, "Wait for the rebalance to start")

            with self._stage("KMR1: accelerator fleet stable (phases 4-5 done)"):
                # Confirms every accelerator finished downloading and holds its guest
                # volume — NOT that the volume has moved to its target KV node yet.
                # _list_accelerator_instances/_accelerator_filter (what "fleet stable"
                # samples) specifically matches volumes still at 16000 IOPS, i.e. still
                # ON the accelerator. Phase 6 (the actual handoff) is a separate wait,
                # immediately below.
                self._wait_for_accelerator_fleet_stable(rebalance_task)

            # KMR1a: wait for phase 6 (handoff to KV nodes) — NOT just "attached to
            # something". A guest volume's Attachments field reports whatever instance
            # currently holds it, which is the ACCELERATOR until the CP hands it off; a
            # naive "attached != unattached" filter was caught by a real run killing
            # memcached on accelerator instances instead of KV nodes. _kv_instance_ids()
            # (positive tag match, same helper the guest-volume-mounting tests use) is
            # the only reliable way to tell the two apart. Polling here, not a single
            # read: per ACCELERATION.md the CP only calls the CBS rebalance once ALL
            # shards are mounted on ALL nodes, so this should resolve quickly, but a
            # premature read (right after fleet-stable, before any handoff has actually
            # landed) is exactly what produced the accelerator-only snapshot above.
            # Happens BEFORE the timed CBS-running wait below, not after — burning time
            # here is fine, the kill still fires the instant that wait returns with no
            # further AWS/CP round trip in between.
            with self._stage("KMR1a: guest volumes transferred to KV nodes (phase 6)"):
                placement = {}
                attached = {}
                transfer_deadline = time.time() + self.input.param(
                    "guest_volume_transfer_timeout", self.gv_launch_timeout)
                while time.time() < transfer_deadline:
                    if rebalance_task.state in self._FAILED_STATES:
                        self.fail(
                            f"Rebalance failed while waiting for guest volumes to "
                            f"reach a KV node: {rebalance_task.state}")
                    placement = self._log_guest_volume_placement(
                        "waiting for guest volumes to reach a KV node")
                    kv_ids = self._kv_instance_ids()
                    attached = {i: v for i, v in placement.items() if i in kv_ids}
                    if attached:
                        break
                    if rebalance_task.state == "healthy":
                        break
                    time.sleep(10)
                on_accelerator = sorted(set(placement) - self._kv_instance_ids())
                self.assertTrue(
                    attached,
                    f"No guest volumes were ever observed attached to an actual KV "
                    f"node within {int(transfer_deadline - time.time())}s — "
                    + (f"they were still on accelerator instance(s) {on_accelerator}"
                       if on_accelerator else "none were attached anywhere") +
                    f". Phase 6 (transfer to KV nodes) must complete before phase 7 "
                    f"(the CBS rebalance) starts, so there is no real target for the "
                    f"kill yet.")
            target_instances = sorted(attached.keys())
            guest_vol_ids_before = {v for vols in attached.values() for v in vols}
            self.log.info(
                f"Targets for the kill: {len(target_instances)} node(s), "
                f"{len(guest_vol_ids_before)} guest volume(s) total — "
                + ", ".join(f"{i} ({len(attached[i])} guest volume(s))"
                           for i in target_instances))
            job_before = self._newest_job_since(
                since_time, label="KMR1b: job baseline before the kill")
            if job_before is None:
                self._record_issue(
                    "KMR1b: no CP job found before the kill",
                    "get_deployment_jobs returned nothing for this rebalance — the "
                    "before/after job evidence in KMR5 below will be incomplete",
                    severity="WARNING")

            cbs_rebalancing = False
            with self._stage("KMR2: CBS is rebalancing (phase 7 reached) — kill now"):
                cbs_rebalancing, progress, detail = self._wait_for_cbs_rebalance_running(
                    rebalance_task,
                    timeout=self.input.param("cbs_rebalance_wait_timeout", 3600),
                    min_progress=float(self.input.param("kill_at_progress_pct", 0.0)))
                self.assertTrue(
                    cbs_rebalancing,
                    f"CBS never reported a rebalance in progress. The CP's phases 4-6 "
                    f"(accelerator launch, S3 download, guest volume transfer) do not "
                    f"involve ns_server, so killing memcached now would land on a phase "
                    f"that has no CBS rebalance to disrupt. Either the rebalance never "
                    f"reached phase 7, or its vBucket movement finished inside the poll "
                    f"interval — raise cbs_rebalance_wait_timeout, lower "
                    f"fusion_min_split_size_gb for more shards to move, or raise "
                    f"rebl_ops_rate so there is more for CBS to chase through DCP.")
                self.log.info(
                    f"CBS is rebalancing at "
                    f"{'unknown' if progress is None else f'{progress:.1f}%'} progress — "
                    f"killing memcached on {len(target_instances)} node(s) now: "
                    f"{target_instances}. State: {detail}")
                # A kill landing on a rebalance already nearly done proves nothing: it
                # may simply have finished on its own regardless of the kill. Mirrors
                # test_abort_rebalance_invalidates_manifest's abort_progress_ceiling_pct.
                ceiling = float(self.input.param("kill_progress_ceiling_pct", 90.0))
                if (progress or 0.0) > ceiling:
                    self._record_issue(
                        "KMR2: killing memcached on a nearly-complete rebalance",
                        f"CBS was already {progress:.1f}% through the vBucket movement "
                        f"(ceiling {ceiling}%) when the kill landed, so the rebalance "
                        f"may simply have finished on its own — treat this run's result "
                        f"as unproven. Lengthen the movement with a smaller "
                        f"fusion_min_split_size_gb (more shards) or a higher "
                        f"rebl_ops_rate (more mutations to chase), or shorten the "
                        f"detection lag with a smaller cbs_rebalance_poll_interval.")

            with self._stage(
                    "KMR3: kill memcached on every node with an attached guest volume"):
                # Fired concurrently, not one after another: a sequential loop would
                # spread the kills across however many SSM round trips that takes,
                # letting the CP route around the first node before the last one is
                # even hit. One thread per target lands them all at once instead.
                kill_results = {}

                def _kill(instance_id):
                    kill_results[instance_id] = self._run_on_cluster_node(
                        instance_id, "sudo pkill -9 memcached || pkill -9 memcached")

                kill_threads = [threading.Thread(target=_kill, args=(i,))
                                for i in target_instances]
                for t in kill_threads:
                    t.start()
                for t in kill_threads:
                    t.join()
                failed = [i for i in target_instances
                         if not kill_results.get(i, {}).get("success")]
                self.assertFalse(
                    failed,
                    f"Could not run the kill command over SSM on: {failed} "
                    f"(results: {kill_results})")

                back_deadline = time.time() + self.input.param(
                    "memcached_restart_timeout", 300)
                pending = set(target_instances)
                while pending and time.time() < back_deadline:
                    for instance_id in list(pending):
                        check = self._run_on_cluster_node(
                            instance_id, "pgrep -c memcached || true")
                        if (check.get("stdout") or "").strip() not in ("", "0"):
                            pending.discard(instance_id)
                    if pending:
                        time.sleep(10)
                self.assertFalse(
                    pending,
                    f"memcached did not come back on {sorted(pending)} within the "
                    f"restart timeout — ns_server did not restart it there")
                self.log.info(
                    f"memcached is running again on all {len(target_instances)} "
                    f"killed node(s): {target_instances}")

            # The kill has landed; background mutations have done their job (chasing
            # each in-flight vBucket move). Stop them here so the rest of this test
            # settles on a quiet cluster, same as the abort test does post-abort.
            self._stop_background_load(bg_tasks, "after the kill")
            bg_tasks = []

            with self._stage("KMR4: guest volumes still intact after the kill"):
                placement_after = self._log_guest_volume_placement(
                    "after killing memcached")
                current_vol_ids = {v for vols in placement_after.values() for v in vols}
                missing = guest_vol_ids_before - current_vol_ids
                self.assertFalse(
                    missing,
                    f"{len(missing)} of {len(guest_vol_ids_before)} guest volume(s) "
                    f"present before the kill are gone after it: {sorted(missing)}. "
                    f"Migration has been frozen (rate limit 0) the entire time, so "
                    f"nothing should have been able to reclaim them — if losing "
                    f"memcached made CBS lose the PlanUUID and tear down the plan "
                    f"(reclaiming guest volumes it should not have yet), this is "
                    f"exactly where that would show up.")
                self.log.info(
                    f"All {len(guest_vol_ids_before)} guest volume(s) present before "
                    f"the kill are still present after it")

            with self._stage("KMR5: CP job evidence around the kill (informational)"):
                job_after = self._newest_job_since(since_time, label="KMR5: after the kill")
                errors_after = (job_after or {}).get("errors") or []
                plan_not_found = any(
                    "plan" in str(e).lower() and "found" in str(e).lower()
                    for e in errors_after)
                self._record_issue(
                    "KMR5: CP job before/after the kill (informational)",
                    f"before={job_before}, after={job_after}, "
                    f"plan-not-found-signature-seen={plan_not_found}",
                    severity="INFO")

            with self._stage("KMR6: the rebalance resolves to healthy (CP retry covers the crash)"):
                recovery_timeout = int(
                    self.input.param("kill_memcached_recovery_timeout", 3600))
                try:
                    self.wait_for_rebalances([rebalance_task])
                except Exception as e:
                    self.log.warning(
                        f"wait_for_rebalances did not complete cleanly ({e}) — checking "
                        f"the cluster's own state next, which is the actual verdict")
                CapellaAPI.wait_until_done(
                    self.pod, self.tenant, self.cluster.id, timeout=recovery_timeout)
                state = CapellaAPI.get_cluster_state(
                    self.pod, self.tenant, self.cluster.id)
                self.assertEqual(
                    state.lower(), "healthy",
                    f"Cluster did not reach 'healthy' after memcached was killed mid-CBS-"
                    f"rebalance (state={state}). Either the CP's job-level retry does not "
                    f"cover this fault, or it needs longer than {recovery_timeout}s — see "
                    f"the KMR5 job evidence above for what the CP actually did.")

            # Only now that the cluster is confirmed healthy: resuming any earlier
            # risks migration reclaiming guest volumes while KMR4 above still needed
            # the freeze to hold.
            self._resume_migration()
        finally:
            # Safety net: if anything above raised before the explicit resume was
            # reached, this still guarantees the next test does not inherit a
            # migration-frozen cluster. A no-op if _resume_migration() already ran.
            self._restore_migration_rate_limit()
            self._stop_background_load(bg_tasks, "cleanup")

        with self._stage("KMR7: teardown clean and data readable"):
            self._validate_teardown(s3_bucket_name)

        self._assert_no_stage_issues()

    # ------------------------------------------------------------------
    # Restart/terminate a KV node during guest volume mounting (Phase 6)
    # ------------------------------------------------------------------

    # _kv_instance_ids() moved to the shared FusionAcceleratorLifecycleTest base
    # (fusion_accelerator_lifecycle_test.py) — fusion_node_health_test.py needs it
    # too; inherited here unchanged.

    def _find_kv_node_receiving_guest_volume(self, rebalance_task,
                                             baseline_volume_ids, timeout=None):
        """Poll for the first KV node a NEW guest volume attaches to (Phase 6).

        Returns (instance_id, [volume_id, ...]) — only volume IDs not already in
        `baseline_volume_ids` — as soon as any KV instance shows one attached, or
        (None, []) if the search window elapses or the rebalance finishes first.

        `baseline_volume_ids` must be captured by the caller BEFORE triggering the
        rebalance (every caller does this): on a shared test cluster, a guest
        volume can be left attached from an earlier test whose teardown did not
        fully clean up. Without excluding it, this could match instantly on a
        stale, unrelated volume rather than waiting for THIS rebalance's own
        Phase 6 handoff — the volume would still be real (this function was never
        at risk of returning an instance with zero volumes at all), just not the
        one this test means to land the fault on. Capturing the baseline any
        later than right before the trigger would risk the opposite problem:
        excluding a volume that attached for real, in the gap between the
        trigger and the caller getting around to calling this.

        The attach step is the earliest externally-observable signal for "a guest
        volume is being mounted onto this node" — the dp-agent mount that follows
        it happens inside the node and is not visible via AWS APIs, so this is the
        closest approximation of "mid-mount" reachable from outside the CP. Polls
        every 2s deliberately tight: unlike the S3 download (throttleable via
        fusion_download_rate_limit), a filesystem mount is not data-transfer bound,
        so there is no equivalent knob to widen this window.
        """
        timeout = timeout or int(self.input.param("kv_node_target_timeout", 900))
        deadline = time.time() + timeout
        while time.time() < deadline:
            if rebalance_task.state in self._FAILED_STATES:
                self.fail(
                    f"Rebalance failed while searching for a KV node receiving a "
                    f"guest volume: {rebalance_task.state}")
            kv_ids = self._kv_instance_ids()
            placement = self._guest_volumes_by_instance()
            for inst, vols in placement.items():
                if inst not in kv_ids:
                    continue
                new_vols = [v for v in vols if v not in baseline_volume_ids]
                if new_vols:
                    self.log.info(
                        f"NEW guest volume(s) {new_vols} attached to KV node "
                        f"{inst}" + (f" (pre-existing on it: "
                                     f"{sorted(set(vols) - set(new_vols))})"
                                     if len(new_vols) < len(vols) else ""))
                    return inst, new_vols
            if rebalance_task.state == "healthy":
                break
            time.sleep(2)
        return None, []

    def _instance_uptime_seconds(self, instance_id):
        """Seconds since boot on `instance_id` (KV node or accelerator — the command
        works on either), or None if unreachable/unparseable."""
        result = self.fusion_aws_util.ec2.run_shell_command(
            instance_id, "cat /proc/uptime")
        stdout = (result.get("stdout") or "").strip()
        try:
            return float(stdout.split()[0])
        except (IndexError, ValueError):
            return None

    def _assert_guest_volumes_not_dangling(self, volume_ids, label):
        """Assert none of `volume_ids` are sitting unattached in 'available'.

        A guest volume that was mid-mount when its node got disrupted has to end
        up one of: attached to SOME real instance (the same one, if it merely
        rebooted; a different one, if the CP redeployed), or deleted by the CP as
        part of abandoning that plan. Left detached-but-undeleted ("available")
        means the CP lost track of it — silently orphaned, and if that shard's
        data existed nowhere else, effectively lost. Mirrors the equivalent check
        in the historical accelerator-side chaos suite
        (test_accelerator_volume_detached_during_download's "does not dangle").

        Logs, but does not assert on, WHERE each volume ended up — callers that
        know the expected instance (e.g. the restart test, where EBS is expected
        to stay put) should check that themselves; this only rules out the one
        outcome that is never acceptable.
        """
        for vol_id in volume_ids:
            vol = self.fusion_aws_util.ec2.get_ebs_volume_by_id(vol_id)
            if vol is None:
                self.log.info(f"[{label}] {vol_id} was deleted by the CP")
                continue
            atts = vol.get("Attachments") or []
            state = vol.get("State")
            attached_to = atts[0].get("InstanceId") if atts else None
            self.log.info(
                f"[{label}] {vol_id} state={state} attached_to={attached_to}")
            self.assertNotEqual(
                state, "available",
                f"[{label}] {vol_id} is sitting unattached in 'available' — a "
                f"guest volume that was mounting when its node was disrupted "
                f"was left dangling rather than re-attached or deleted")

    def test_restart_kv_node_during_guest_volume_mounting(self):
        """
        Reboot a KV node's EC2 instance the instant a guest volume attaches to it
        (Phase 6), and assert the cluster still recovers.

        COVERAGE.md §9 "Restart / terminate node during guest volume mounting" —
        the restart half. See the module docstring for the ACCELERATION.md Phase 6
        grounding and why "the instant a guest volume attaches" is the best
        available proxy for "mid-mount".

        `sudo reboot` over SSM predictably drops the SSM connection before it can
        report a clean result — that is expected, not a failure signal; recovery is
        confirmed separately by polling /proc/uptime until it resets below the
        pre-reboot baseline (i.e. the instance actually rebooted, not just a
        transient SSM hiccup).

        Migration is frozen first so the guest volume this test targets is still
        there when the reboot lands, and so the aftermath (drain, teardown) stays
        observable — same reasoning as every other fault in this file.
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")
        self._apply_fusion_config_from_params()
        self._apply_fusion_sync_threshold()
        s3_bucket_name, _ = self._capture_s3_log_store_baseline()
        self._load_above_threshold()

        if not self._pause_migration():
            self.fail(
                "Could not freeze background migration — without it the guest "
                "volume this test targets may already be reclaimed before the "
                "reboot lands, and the aftermath would not be observable.")

        kv_target_timeout = int(self.input.param("kv_node_target_timeout", 900))
        kv_recovery_timeout = int(self.input.param("kv_node_recovery_timeout", 900))

        baseline_volume_ids = {
            v for vols in self._guest_volumes_by_instance().values() for v in vols}

        try:
            rebalance_task = self._trigger_rebalance()
            self.sleep(30, "Wait for the rebalance to start")

            target_id = None
            target_volumes = []
            with self._stage("KVR1: find a KV node receiving a guest volume"):
                target_id, target_volumes = self._find_kv_node_receiving_guest_volume(
                    rebalance_task, baseline_volume_ids, timeout=kv_target_timeout)
                self.assertIsNotNone(
                    target_id,
                    f"No KV node was ever observed with a guest volume attached "
                    f"within {kv_target_timeout}s — either the rebalance never "
                    f"reached Phase 6 (handoff), or it moved through it faster "
                    f"than this could sample. Raise kv_node_target_timeout, lower "
                    f"fusion_min_split_size_gb for more/slower handoffs, or check "
                    f"fusion_threshold_gib against the load.")

            baseline_uptime = None
            with self._stage("KVR2: reboot the KV node"):
                baseline_uptime = self._instance_uptime_seconds(target_id)
                self.log.info(
                    f"Rebooting {target_id} (guest volume(s) {target_volumes} "
                    f"just attached; uptime before reboot: {baseline_uptime}s)")
                result = self.fusion_aws_util.ec2.run_shell_command(
                    target_id, "sudo reboot", timeout=30)
                self.log.info(
                    f"[ssm] reboot command result on {target_id}: {result} (a "
                    f"dropped/failed response here is expected — the reboot "
                    f"kills the SSM agent connection before it can report back)")

            outcome = None
            with self._stage("KVR3: the node comes back up"):
                deadline = time.time() + kv_recovery_timeout
                while time.time() < deadline:
                    if rebalance_task.state in self._FAILED_STATES:
                        outcome = f"clean failure ({rebalance_task.state})"
                        break
                    uptime = self._instance_uptime_seconds(target_id)
                    if uptime is not None and (
                            baseline_uptime is None or uptime < baseline_uptime):
                        outcome = f"{target_id} rebooted (uptime now {uptime:.0f}s)"
                        break
                    if rebalance_task.state == "healthy":
                        outcome = "rebalance completed"
                        break
                    self.log.info(
                        f"[kv-reboot] {target_id} not yet confirmed rebooted "
                        f"(uptime={uptime}) task_state={rebalance_task.state} — "
                        f"{int(deadline - time.time())}s remaining")
                    time.sleep(15)
                self.assertIsNotNone(
                    outcome,
                    f"{target_id} never came back within {kv_recovery_timeout}s "
                    f"(uptime never reset below the {baseline_uptime}s baseline), "
                    f"and the rebalance neither completed nor failed — a "
                    f"rebooted KV node should recover on its own.")
                self.log.info(f"KV reboot outcome: {outcome}")
                self._record_issue(
                    "KVR3: KV reboot outcome (informational)", outcome,
                    severity="INFO")

            try:
                self.wait_for_rebalances([rebalance_task])
            except Exception as e:
                self.log.warning(
                    f"Rebalance did not complete cleanly ({e}) — continuing to "
                    f"the cleanup assertions, which a clean failure still has to "
                    f"satisfy")

            with self._stage("KVR4: target guest volume(s) still where expected"):
                # Unlike terminate, a reboot never detaches EBS at the AWS level —
                # so target_volumes should still be on target_id, full stop. This
                # is a stronger, more specific check than "not dangling": if any
                # of these are no longer attached to target_id, something other
                # than the reboot itself moved them, which is worth knowing.
                for vol_id in target_volumes:
                    vol = self.fusion_aws_util.ec2.get_ebs_volume_by_id(vol_id)
                    self.assertIsNotNone(
                        vol,
                        f"{vol_id} no longer exists after the reboot — it "
                        f"should never have been deleted, since the instance "
                        f"was only rebooted, not terminated")
                    atts = vol.get("Attachments") or []
                    attached_to = atts[0].get("InstanceId") if atts else None
                    self.log.info(
                        f"[KVR4] {vol_id} attached_to={attached_to} "
                        f"(target was {target_id})")
                    self.assertEqual(
                        attached_to, target_id,
                        f"{vol_id} is no longer attached to {target_id} after "
                        f"a reboot — EBS volumes are expected to stay attached "
                        f"across a reboot (unlike a terminate); it is now "
                        f"attached_to={attached_to}, state="
                        f"{vol.get('State')}")

            baseline_du = self._avg_main_volume_usage()
            with self._stage("KVR5: resume migration"):
                self._resume_migration()
            with self._stage("KVR6: guest volumes still drain after the reboot"):
                self._validate_guest_volume_drain(baseline_du=baseline_du)
            with self._stage("KVR7: teardown clean after the reboot"):
                self._validate_teardown(s3_bucket_name)
        finally:
            self._restore_migration_rate_limit()

        self._assert_no_stage_issues()

    def test_terminate_kv_node_during_guest_volume_mounting(self):
        """
        Terminate a KV node's EC2 instance the instant a guest volume attaches to
        it (Phase 6), and assert the cluster still ends up resolved.

        COVERAGE.md §9 "Restart / terminate node during guest volume mounting" —
        the terminate half. See the module docstring: unlike the restart variant,
        this is NOT reversible for that instance, and nothing in this codebase has
        exercised how Capella's node-replacement flow responds to a KV node lost
        mid-rebalance before now. Do not run this against a cluster anything else
        currently depends on staying up.

        Accepted outcomes, all logged (see KVT3): a replacement KV node appears
        (CP healed the topology), the terminated node is simply gone with the
        rebalance still reporting healthy (no 1:1 replacement observed), or the
        rebalance fails cleanly. Not accepted: no resolution at all within
        kv_node_terminate_recovery_timeout — the cluster stuck believing a node
        that no longer exists is still part of the plan.
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")
        self._apply_fusion_config_from_params()
        self._apply_fusion_sync_threshold()
        s3_bucket_name, _ = self._capture_s3_log_store_baseline()
        self._load_above_threshold()

        if not self._pause_migration():
            self.fail(
                "Could not freeze background migration — without it the guest "
                "volume this test targets may already be reclaimed before the "
                "termination lands, and the aftermath would not be observable.")

        kv_target_timeout = int(self.input.param("kv_node_target_timeout", 900))
        kv_recovery_timeout = int(
            self.input.param("kv_node_terminate_recovery_timeout", 1800))

        baseline_volume_ids = {
            v for vols in self._guest_volumes_by_instance().values() for v in vols}

        try:
            rebalance_task = self._trigger_rebalance()
            self.sleep(30, "Wait for the rebalance to start")

            target_id = None
            kv_ids_before = set()
            with self._stage("KVT1: find a KV node receiving a guest volume"):
                kv_ids_before = self._kv_instance_ids()
                target_id, target_volumes = self._find_kv_node_receiving_guest_volume(
                    rebalance_task, baseline_volume_ids, timeout=kv_target_timeout)
                self.assertIsNotNone(
                    target_id,
                    f"No KV node was ever observed with a guest volume attached "
                    f"within {kv_target_timeout}s — either the rebalance never "
                    f"reached Phase 6 (handoff), or it moved through it faster "
                    f"than this could sample. Raise kv_node_target_timeout, lower "
                    f"fusion_min_split_size_gb for more/slower handoffs, or check "
                    f"fusion_threshold_gib against the load.")

            with self._stage("KVT2: terminate the KV node"):
                self.log.info(
                    f"Terminating {target_id} (guest volume(s) {target_volumes} "
                    f"just attached)")
                terminated = self.fusion_aws_util.ec2.terminate_instance(target_id)
                self.assertTrue(
                    terminated,
                    f"AWS API rejected the termination of KV node {target_id}")

            outcome = None
            replacement_ids = set()
            with self._stage("KVT3: the CP responds — replacement or clean failure"):
                deadline = time.time() + kv_recovery_timeout
                while time.time() < deadline:
                    if rebalance_task.state in self._FAILED_STATES:
                        outcome = f"clean failure ({rebalance_task.state})"
                        break
                    kv_ids_now = self._kv_instance_ids()
                    new_ids = kv_ids_now - kv_ids_before
                    if new_ids:
                        # A new instance id alone doesn't say anything about
                        # THIS shard — check whether target_volumes (or a fresh
                        # replacement volume) actually landed on it.
                        replacement_ids = new_ids
                        placement_now = self._guest_volumes_by_instance()
                        on_replacement = {inst: vols for inst, vols in
                                         placement_now.items()
                                         if inst in new_ids and vols}
                        target_on_replacement = {
                            v for vols in on_replacement.values() for v in vols
                            if v in target_volumes}
                        if target_on_replacement:
                            outcome = (
                                f"CP provisioned replacement KV node(s) "
                                f"{sorted(new_ids)} and re-attached the "
                                f"original guest volume(s) "
                                f"{sorted(target_on_replacement)} to it")
                        elif on_replacement:
                            outcome = (
                                f"CP provisioned replacement KV node(s) "
                                f"{sorted(new_ids)}, which hold a DIFFERENT "
                                f"guest volume {on_replacement} — the shard "
                                f"was re-downloaded from scratch rather than "
                                f"the original volume being reused")
                        else:
                            outcome = (
                                f"CP provisioned replacement KV node(s) "
                                f"{sorted(new_ids)}, but none show ANY guest "
                                f"volume attached yet — checked further in "
                                f"KVT4")
                        break
                    if target_id not in kv_ids_now and rebalance_task.state == "healthy":
                        outcome = (f"rebalance completed; {target_id} no longer "
                                   f"listed as a cluster node (no 1:1 "
                                   f"replacement observed)")
                        break
                    if rebalance_task.state == "healthy":
                        outcome = "rebalance completed"
                        break
                    self.log.info(
                        f"[kv-terminate] waiting on {target_id}'s termination — "
                        f"task_state={rebalance_task.state} — "
                        f"{int(deadline - time.time())}s remaining")
                    time.sleep(15)
                self.assertIsNotNone(
                    outcome,
                    f"No resolution observed within {kv_recovery_timeout}s after "
                    f"terminating {target_id}: no replacement node appeared, and "
                    f"the rebalance neither completed nor failed — the cluster "
                    f"appears stuck on a node that no longer exists.")
                self.log.info(f"KV terminate outcome: {outcome}")
                self._record_issue(
                    "KVT3: KV terminate outcome (informational)", outcome,
                    severity="INFO")

            try:
                self.wait_for_rebalances([rebalance_task])
            except Exception as e:
                self.log.warning(
                    f"Rebalance did not complete cleanly ({e}) — continuing to "
                    f"the cleanup assertions, which a clean failure still has to "
                    f"satisfy")

            with self._stage("KVT4: target guest volume(s) not left dangling"):
                # Termination DOES detach (or delete, if DeleteOnTermination) the
                # volume — unlike the reboot test, ending up on a DIFFERENT
                # instance (or deleted) is a legitimate outcome here, not a
                # finding by itself. What must never happen is "available" and
                # simply forgotten, which is what this checks.
                self._assert_guest_volumes_not_dangling(target_volumes, "KVT4")

                # A "replacement node appeared" outcome from KVT3 is not itself
                # proof the shard was actually picked back up — cross-check that
                # the replacement now shows SOME guest volume (the original,
                # re-attached, or a fresh one from a re-download), giving the
                # settling time KVT3's break-on-first-sight didn't allow for.
                if replacement_ids:
                    placement_now = self._guest_volumes_by_instance()
                    on_replacement = {inst: vols for inst, vols in
                                     placement_now.items()
                                     if inst in replacement_ids and vols}
                    self.log.info(
                        f"[KVT4] replacement node(s) {sorted(replacement_ids)} "
                        f"now hold: {on_replacement or 'nothing'}")
                    self.assertTrue(
                        on_replacement,
                        f"CP provisioned replacement KV node(s) "
                        f"{sorted(replacement_ids)} but none of them show ANY "
                        f"guest volume attached — the shard that "
                        f"{target_volumes} belonged to appears to have been "
                        f"abandoned on the replacement rather than picked "
                        f"back up, whether via re-attach or re-download.")

            baseline_du = self._avg_main_volume_usage()
            with self._stage("KVT5: resume migration"):
                self._resume_migration()
            with self._stage("KVT6: guest volumes still drain after the termination"):
                self._validate_guest_volume_drain(baseline_du=baseline_du)
            with self._stage("KVT7: teardown clean after the termination"):
                self._validate_teardown(s3_bucket_name)
        finally:
            self._restore_migration_rate_limit()

        self._assert_no_stage_issues()

    # ------------------------------------------------------------------
    # Terminate a KV node after guest volumes are attached (Phase 8, migration)
    # ------------------------------------------------------------------

    def test_terminate_kv_node_after_guest_volumes_attached(self):
        """
        Terminate a KV node's EC2 instance while background migration (Phase 8) is
        actively copying its guest volume(s) into managed storage, and assert a
        replacement node comes up, the SAME guest volume(s) get re-attached to it,
        and migration then resumes and finishes.

        COVERAGE.md §9 "Terminate node after all guest volumes attached". This is a
        LATER boundary than test_terminate_kv_node_during_guest_volume_mounting:
        that one lands during Phase 6 (attach/mount, before the rebalance call);
        this one lands after Phase 7's rebalance call has already succeeded, once
        every guest volume is attached and Phase 8's background migration is
        already under way. Same caveat as the other two KV-node tests: this acts on
        a live, data-serving instance and is NOT reversible for it — nothing in
        this codebase has exercised how Capella's node-replacement flow responds to
        a KV node lost mid-migration before now.

        Sequence:
          1. set fusion_migration_rate_limit LOW before triggering the rebalance —
             unlike the mounting-phase tests, migration IS data-transfer bound, so
             this is a real knob to widen the window (mirrors
             fusion_download_rate_limit's role in test_dp_accelerator_crash_
             during_download, just for Phase 8 instead of Phase 5)
          2. let the rebalance run to completion — every guest volume attached
          3. confirm migration is genuinely progressing
             (ep_fusion_migration_completed_bytes > 0) on the busiest KV node
             before touching anything, and that its guest volumes are still there
             (not already fully drained at the low rate)
          4. terminate that node
          5. wait for a replacement KV instance to appear holding at least one of
             the same guest volume(s)
          6. once confirmed, raise the migration rate limit back to the default so
             the rest of the test does not also crawl
          7. validate migration completes, guest volumes fully drain, no
             migration failures, teardown clean
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")
        self._apply_fusion_config_from_params()
        self._apply_fusion_sync_threshold()
        s3_bucket_name, _ = self._capture_s3_log_store_baseline()
        self._load_above_threshold()

        low_rate = int(self.input.param("kv_terminate_migration_rate_limit", 1048576))
        default_rate = int(self.input.param("fusion_migration_rate_limit", 78643200))
        migration_start_timeout = int(self.input.param("migration_start_timeout", 600))
        recovery_timeout = int(
            self.input.param("kv_node_migration_terminate_recovery_timeout", 1800))

        # Captured before the rebalance so the "busiest KV node" pick below can be
        # restricted to volumes THIS rebalance attached — a guest volume left
        # dangling by an earlier test's incomplete teardown on the same shared
        # cluster would otherwise be able to inflate a node's count (or even make
        # it the target) without this test's own migration ever touching it.
        baseline_volume_ids = {
            v for vols in self._guest_volumes_by_instance().values() for v in vols}

        try:
            self._set_migration_rate_limit(
                low_rate,
                "slow background migration before the rebalance so there is a "
                "real window to terminate a KV node while it is still copying")
        except Exception as e:
            self.fail(
                f"Could not set a low migration rate limit before the rebalance "
                f"({type(e).__name__}: {e}) — without it migration may finish "
                f"before the termination can land, proving nothing.")

        try:
            rebalance_task = self._trigger_rebalance()
            self.sleep(30, "Wait for the rebalance to start")

            with self._stage("KVM1: rebalance completes with guest volumes attached"):
                self.wait_for_rebalances([rebalance_task])
                CapellaAPI.wait_until_done(
                    self.pod, self.tenant, self.cluster.id, timeout=1800)
                state = CapellaAPI.get_cluster_state(
                    self.pod, self.tenant, self.cluster.id)
                self.assertEqual(
                    state.lower(), "healthy",
                    f"Cluster is not healthy after the rebalance: {state}")
                placement = self._log_guest_volume_placement(
                    "after rebalance, before termination")
                attached = {i: v for i, v in placement.items() if i != "unattached"}
                self.assertTrue(
                    attached,
                    "No guest volumes attached after the rebalance — nothing for "
                    "this test to migrate/terminate under")
                new_attached = {
                    i: [v for v in vols if v not in baseline_volume_ids]
                    for i, vols in attached.items()}
                new_attached = {i: v for i, v in new_attached.items() if v}
                self.assertTrue(
                    new_attached,
                    "Guest volumes are attached, but every one of them predates "
                    "this rebalance (all were already present in the baseline "
                    "snapshot taken before it) — nothing NEW for this test to "
                    "migrate/terminate under")

            target_instance = max(new_attached, key=lambda i: len(new_attached[i]))
            target_volumes = []
            with self._stage("KVM2: migration is genuinely in progress"):
                deadline = time.time() + migration_start_timeout
                progressed = False
                while time.time() < deadline:
                    completed = self._sum_cbstat_across_nodes(
                        "ep_fusion_migration_completed_bytes")
                    if completed > 0:
                        progressed = True
                        self.log.info(
                            f"Migration has copied {completed} byte(s) — "
                            f"proceeding to terminate {target_instance}")
                        break
                    time.sleep(15)
                self.assertTrue(
                    progressed,
                    f"ep_fusion_migration_completed_bytes never moved off 0 "
                    f"within {migration_start_timeout}s — migration was not "
                    f"running, so terminating a node now would prove nothing. "
                    f"Lower kv_terminate_migration_rate_limit or raise "
                    f"migration_start_timeout.")
                # Re-check right before acting: at a low but nonzero rate a small
                # shard could still have fully drained (and its volume released)
                # in the time the poll above took.
                still_attached = [
                    v for v in self._guest_volumes_by_instance().get(
                        target_instance, [])
                    if v not in baseline_volume_ids]
                self.assertTrue(
                    still_attached,
                    f"{target_instance} no longer holds any guest volume "
                    f"attached by this rebalance — migration already finished "
                    f"before the termination could land. Lower "
                    f"kv_terminate_migration_rate_limit further or increase "
                    f"create_end.")
                target_volumes = still_attached

            kv_ids_before = set()
            with self._stage("KVM3: terminate the KV node mid-migration"):
                kv_ids_before = self._kv_instance_ids()
                self.log.info(
                    f"Terminating {target_instance} while migrating guest "
                    f"volume(s) {target_volumes}")
                terminated = self.fusion_aws_util.ec2.terminate_instance(
                    target_instance)
                self.assertTrue(
                    terminated,
                    f"AWS API rejected the termination of KV node "
                    f"{target_instance}")

            replacement_id = None
            with self._stage(
                    "KVM4: replacement node appears with guest volume(s) re-attached"):
                deadline = time.time() + recovery_timeout
                reattached = set()
                while time.time() < deadline:
                    new_ids = self._kv_instance_ids() - kv_ids_before
                    placement_now = self._guest_volumes_by_instance()
                    for inst in new_ids:
                        hit = set(placement_now.get(inst, [])) & set(target_volumes)
                        if hit:
                            replacement_id = inst
                            reattached = hit
                            break
                    if replacement_id:
                        break
                    time.sleep(15)
                self.assertIsNotNone(
                    replacement_id,
                    f"No replacement KV node was ever observed holding any of "
                    f"{target_volumes} within {recovery_timeout}s after "
                    f"terminating {target_instance}.")
                self.log.info(
                    f"Guest volume(s) {sorted(reattached)} re-attached to "
                    f"replacement node {replacement_id}")
                missing = set(target_volumes) - reattached
                if missing:
                    self._record_issue(
                        "KVM4: not all target guest volumes re-attached",
                        f"{sorted(missing)} did not show up on {replacement_id} "
                        f"— check whether they were deleted/re-downloaded "
                        f"instead of re-attached", severity="INFO")
                self._assert_guest_volumes_not_dangling(target_volumes, "KVM4")

            with self._stage("KVM5: restore the default migration rate limit"):
                self._set_migration_rate_limit(
                    default_rate,
                    "termination and re-attachment confirmed — let migration "
                    "finish at normal speed")

            baseline_du = self._avg_main_volume_usage()
            with self._stage("KVM6: migration completes and guest volumes drain"):
                self._validate_guest_volume_drain(baseline_du=baseline_du)
            with self._stage("KVM7: no migration failures recorded"):
                failures = self._sum_migration_stat("ep_fusion_migration_failures")
                self.assertEqual(
                    failures, 0,
                    f"ep_fusion_migration_failures={failures} after the "
                    f"termination — migration did not resume cleanly")
            with self._stage("KVM8: teardown clean after the termination"):
                self._validate_teardown(s3_bucket_name)
        finally:
            try:
                self._set_migration_rate_limit(
                    default_rate, "tearDown safety net — restore the default "
                    "migration rate regardless of how the test above ended")
            except Exception as e:
                self.log.error(
                    f"Failed to restore the default migration rate limit — "
                    f"background migration may still be throttled for later "
                    f"tests: {e}")

        self._assert_no_stage_issues()

    # ------------------------------------------------------------------
    # Restart an accelerator node mid-download (Phase 5)
    # ------------------------------------------------------------------

    def test_restart_accelerator_node_mid_download(self):
        """
        Reboot an accelerator's EC2 instance while it is actively downloading its
        shard from S3 (Phase 5), and assert the CP still gets the data onto the
        cluster.

        COVERAGE.md §9 "Restart accelerator nodes mid-download". See the module
        docstring for how this differs from test_dp_accelerator_crash_during_
        download (kills only the dp-accelerator process, same instance) and the
        terminate/stop tests in fusion_accelerator_chaos_test.py — this reboots
        the whole instance, so dp-accelerator restarts fresh along with the OS.

        fusion_download_rate_limit throttles the S3 download to widen the window
        — ONE value for the whole test, same lesson as test_dp_accelerator_crash_
        during_download: changing it mid-flight has no effect on a download
        already in progress.

        Sequence:
          1. load past the fusion threshold, trigger a rebalance
          2. search the fleet for an accelerator actually caught with a live
             dp-accelerator process (_find_downloading_accelerator)
          3. reboot it over SSM; confirm via /proc/uptime resetting, same idiom
             as test_restart_kv_node_during_guest_volume_mounting
          4. accept either: the SAME instance comes back (dp-accelerator running
             again, checked but not required to be "running" for the outcome to
             count — the instance recovering at all is the main signal), or a
             NEW accelerator appears that was not part of the fleet before the
             reboot (fallback-replace); reject: neither, with the rebalance
             still stuck
          5. the download volume must still be attached somewhere, not dangling
             (reboot should never detach EBS, unlike a terminate)
          6. let the rebalance run to completion, resume migration, then
             validate full teardown
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")
        self._apply_fusion_config_from_params()
        self._apply_fusion_sync_threshold()
        s3_bucket_name, _ = self._capture_s3_log_store_baseline()
        self._load_above_threshold()

        if not self._pause_migration():
            self.log.warning(
                "Could not freeze background migration — the fault still lands "
                "during download, but the aftermath will be harder to observe")

        target_id = None
        target_volumes = []
        accel_ids_before = set()
        try:
            rebalance_task = self._trigger_rebalance()
            self.sleep(30, "Wait for the rebalance to start")

            with self._stage("ARN1: find an accelerator still downloading"):
                target_id, tried = self._find_downloading_accelerator(rebalance_task)
                self.assertIsNotNone(
                    target_id,
                    f"No accelerator was ever caught with a live dp-accelerator "
                    f"process — {len(tried)} candidate(s) inspected: {tried}. "
                    f"Raise create_end, lower fusion_download_rate_limit, or "
                    f"ease off fusion_min_split_size_gb to widen the download "
                    f"window, or raise dp_accelerator_target_timeout to search "
                    f"longer.")
                # Snapshot right now, at the moment the fault is about to land —
                # NOT at the start of the (possibly long) search above. See
                # test_dp_accelerator_crash_during_download's module-docstring
                # note on the false positive this ordering fixed there.
                accel_ids_before = {i.get("InstanceId") for i in
                                    self._list_accelerator_instances_by_tag(
                                        log="AcceleratorRestartFleet")}
                target_volumes = self._guest_volumes_by_instance().get(
                    target_id, [])

            baseline_uptime = None
            with self._stage("ARN2: reboot the accelerator"):
                baseline_uptime = self._instance_uptime_seconds(target_id)
                self.log.info(
                    f"Rebooting {target_id} (guest volume(s) {target_volumes} "
                    f"mid-download; uptime before reboot: {baseline_uptime}s)")
                result = self.fusion_aws_util.ec2.run_shell_command(
                    target_id, "sudo reboot", timeout=30)
                self.log.info(
                    f"[ssm] reboot command result on {target_id}: {result} (a "
                    f"dropped/failed response here is expected — the reboot "
                    f"kills the SSM agent connection before it can report back)")

            outcome = None
            recovery_timeout = int(
                self.input.param("accelerator_restart_recovery_timeout", 900))
            with self._stage("ARN3: the CP recovers from the reboot"):
                deadline = time.time() + recovery_timeout
                while time.time() < deadline:
                    if rebalance_task.state in self._FAILED_STATES:
                        outcome = f"clean failure ({rebalance_task.state})"
                        break
                    uptime = self._instance_uptime_seconds(target_id)
                    if uptime is not None and (
                            baseline_uptime is None or uptime < baseline_uptime):
                        is_running, pids, n_restarts = self._dp_accelerator_state(
                            target_id)
                        outcome = (
                            f"{target_id} rebooted (uptime now {uptime:.0f}s), "
                            f"dp-accelerator " +
                            (f"running again (pid(s)={pids}, "
                             f"NRestarts={n_restarts})" if is_running
                             else f"not yet running (NRestarts={n_restarts})"))
                        break
                    live_by_tag = {i.get("InstanceId") for i in
                                   self._list_accelerator_instances_by_tag(
                                       log="AcceleratorRestartRecovery")}
                    new_instances = live_by_tag - accel_ids_before
                    if new_instances:
                        outcome = (
                            f"CP redeployed: new accelerator(s) "
                            f"{sorted(new_instances)} appeared after the "
                            f"reboot, {target_id} not confirmed back yet")
                        break
                    if rebalance_task.state == "healthy":
                        outcome = "rebalance completed"
                        break
                    self.log.info(
                        f"[accel-reboot] {target_id} not yet confirmed "
                        f"rebooted (uptime={uptime}) "
                        f"task_state={rebalance_task.state} — "
                        f"{int(deadline - time.time())}s remaining")
                    time.sleep(15)
                self.assertIsNotNone(
                    outcome,
                    f"Neither {target_id} rebooting nor a replacement "
                    f"accelerator appeared within {recovery_timeout}s, and the "
                    f"rebalance neither completed nor failed — the download "
                    f"is stuck on an unresponsive accelerator.")
                self.log.info(f"Accelerator reboot outcome: {outcome}")
                self._record_issue(
                    "ARN3: accelerator reboot outcome (informational)", outcome,
                    severity="INFO")

            if target_volumes:
                with self._stage("ARN4: download volume(s) not left dangling"):
                    self._assert_guest_volumes_not_dangling(target_volumes, "ARN4")

            try:
                self.wait_for_rebalances([rebalance_task])
            except Exception as e:
                self.log.warning(
                    f"Rebalance did not complete cleanly ({e}) — continuing "
                    f"to the cleanup assertions, which a clean failure still "
                    f"has to satisfy")
        finally:
            self._restore_migration_rate_limit()

        CapellaAPI.wait_until_done(
            self.pod, self.tenant, self.cluster.id, timeout=1800)

        with self._stage("ARN5: cluster healthy and data readable"):
            state = CapellaAPI.get_cluster_state(
                self.pod, self.tenant, self.cluster.id)
            self.assertEqual(
                state.lower(), "healthy",
                f"Cluster is not healthy after the accelerator reboot: {state}")
            self._run_read_workload("after accelerator reboot mid-download")

        with self._stage("ARN6: teardown clean after the reboot"):
            self._validate_teardown(s3_bucket_name)

        self._assert_no_stage_issues()

    # ------------------------------------------------------------------
    # Delete a log file from S3 (Phase 5) -> accelerator-cli failure
    # ------------------------------------------------------------------

    def test_delete_s3_log_file_accelerator_cli_failure(self):
        """
        Delete a large chunk of individual log-file objects (NOT whole shard folders)
        from the fusion S3 log store before triggering a rebalance, so accelerator-cli
        hits a read failure trying to download whichever shard(s) those objects belonged
        to, and assert the CP falls back to DCP for the affected shard(s) instead of
        failing or hanging the whole rebalance — no orphan guest volumes or accelerator
        instances survive, and the cluster ends up healthy.

        COVERAGE.md §9 "Delete log file from S3 -> accelerator-cli failure". See the
        module docstring for how this differs from every other accelerator-side fault in
        this file (it never touches the accelerator process or instance at all) and from
        COVERAGE.md's separate, much bigger "Delete S3 bucket -> CP disables fusion" row
        (out of scope here), plus the known real risk of the CP simply never reacting to
        a corrupted log store, carried over from fusion_cluster_destroy_test.py's
        test_destroy_in_scale_failed_state.

        SIZING s3_log_files_to_delete. A real run's S3 log store held 256 objects for one
        bucket at this conf's data size — 2 log files per vBucket across the default 128
        vBuckets. A given accelerator only ever downloads the vBuckets that landed in ITS
        shard (per _load_above_threshold's sibling docstrings, roughly a quarter of the
        keyspace moves on a 3->4 node scale-out), so deleting only a couple of files out
        of 256 gives a low chance of ever landing on a vBucket actually in this
        rebalance's manifest — most likely outcome would be a clean, unaffected rebalance
        that proves nothing. Deleting around HALF of them (default 128) makes it very
        likely at least one deleted object falls inside whatever subset of vBuckets this
        run's shard(s) actually cover, regardless of exactly which vBuckets those turn
        out to be. Recompute this default against the S3 object count logged by
        _capture_s3_log_store_baseline/_wait_for_log_store_sync if create_end, collections,
        or the vBucket count differ from this conf.

        Sequence:
          1. load data above the fusion threshold, wait for the S3 log store to sync
          2. delete a few individual log-file objects from S3 (num_folders=0 — never a
             whole shard folder)
          3. trigger a rebalance, confirm the fusion path is taken (accelerators launch
             and start downloading against a log store now missing those objects)
          4. wait for the rebalance to resolve — healthy is the ONLY accepted outcome; a
             failed rebalance or a hang past s3_log_corruption_recovery_timeout both fail
             this test (unlike the more permissive KV-node chaos tests elsewhere in this
             file, a single missing log file is expected to be a fully recoverable,
             per-shard DCP fallback, not something that should take the whole rebalance
             down or stall it)
          5. full teardown validation (_validate_teardown): no orphan guest volumes,
             ASGs, or accelerator instances; no migration failures; cluster healthy with
             fusion still enabled; S3 log store intact; reads succeed
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")
        self._apply_fusion_config_from_params()
        self._apply_fusion_sync_threshold()
        s3_bucket_name, _ = self._capture_s3_log_store_baseline()
        self.assertIsNotNone(
            s3_bucket_name,
            "Fusion S3 URI not available — ensure fusion is enabled and a bucket "
            "exists")
        self._load_above_threshold()

        num_files = int(self.input.param("s3_log_files_to_delete", 128))
        recovery_timeout = int(
            self.input.param("s3_log_corruption_recovery_timeout", 1800))

        bucket = self.cluster.buckets[0]
        if not getattr(bucket, "bucket_uuid", None):
            info = RestConnection(self.cluster.master).get_bucket_details(
                bucket_name=bucket.name)
            bucket.bucket_uuid = info.get("uuid")
        self.assertIsNotNone(
            bucket.bucket_uuid, f"Could not resolve UUID for bucket {bucket.name}")

        with self._stage("S3L1: delete individual log-file objects from S3"):
            corrupted = self.fusion_aws_util.corrupt_fusion_log_store(
                s3_bucket_name, bucket.bucket_uuid, num_folders=0,
                num_files=num_files)
            self.assertTrue(
                corrupted["files_deleted"],
                f"Failed to delete any individual log-file object under "
                f"kv/{bucket.bucket_uuid} in {s3_bucket_name} — nothing for "
                f"accelerator-cli to fail on")
            self.assertFalse(
                corrupted["folders_deleted"],
                f"num_folders=0 should never delete a whole shard folder, but "
                f"{corrupted['folders_deleted']} was/were deleted — this test "
                f"targets individual log files, not whole shards")
            self.log.info(
                f"Deleted {len(corrupted['files_deleted'])} individual "
                f"log-file object(s) from the S3 log store before triggering "
                f"the rebalance: {corrupted['files_deleted']}")

        rebalance_task = self._trigger_rebalance()
        self.sleep(30, "Wait for the rebalance to start")

        with self._stage("S3L2: fusion path taken, accelerators launched"):
            instances = self._poll_until_accelerators_appear(rebalance_task)
            self.assertGreater(
                len(instances), 0,
                "No accelerators launched — the rebalance took the DCP path "
                "from the start, so the deleted log file was never on the "
                "accelerator download path and this test proves nothing. "
                "Check fusion_threshold_gib against the data loaded.")
            self.log.info(
                f"{len(instances)} accelerator instance(s) launched, "
                f"downloading against a log store missing {num_files} "
                f"object(s)")

        with self._stage(
                "S3L3: CP falls back to DCP and the rebalance completes healthy"):
            self.wait_for_rebalances([rebalance_task], timeout=recovery_timeout)
            CapellaAPI.wait_until_done(
                self.pod, self.tenant, self.cluster.id, timeout=1800)
            state = CapellaAPI.get_cluster_state(
                self.pod, self.tenant, self.cluster.id)
            self.assertEqual(
                state.lower(), "healthy",
                f"Cluster is not healthy after deleting {num_files} S3 "
                f"log-file object(s) and rebalancing: {state}. If the "
                f"rebalance is still 'scaling' rather than failed, the CP "
                f"never detected the missing object and is hung — the exact "
                f"risk documented in this test's docstring. Raise "
                f"s3_log_corruption_recovery_timeout if the CP just needs "
                f"more time, otherwise this is the bug this test exists to "
                f"catch.")

        with self._stage("S3L4: no orphans, migration clean, cluster healthy"):
            self._validate_teardown(s3_bucket_name)

        self._assert_no_stage_issues()

    # ------------------------------------------------------------------
    # Corrupt log files on guest volumes (junk bytes)
    # ------------------------------------------------------------------

    def _run_on_all_kv_nodes(self, command, timeout=60):
        """Run a shell command on every current KV node over SSM; {instance_id: result}.

        No generic multi-node fan-out helper existed before this — _run_on_cluster_node
        is single-instance, and fusion_monitor_util.run_cbstats_on_all_nodes is hardcoded
        to a cbstats command line. Modeled on FusionCPResourceMonitor.
        get_main_volume_disk_usage_percent's ThreadPoolExecutor + tag-filtered instance
        list shape, generalised to an arbitrary command.
        """
        instance_ids = sorted(self._kv_instance_ids())
        results = {}
        if not instance_ids:
            return results
        with ThreadPoolExecutor(max_workers=len(instance_ids)) as executor:
            futures = {
                executor.submit(self._run_on_cluster_node, i, command, timeout): i
                for i in instance_ids}
            for future in as_completed(futures):
                instance_id = futures[future]
                try:
                    results[instance_id] = future.result()
                except Exception as e:
                    results[instance_id] = {"success": False, "error": str(e)}
        return results

    def _discover_guest_volume_mounts_via_mount(self, instance_id):
        """Independent discovery attempt: parse `mount`'s own MOUNTPOINT field
        (not a blind substring grep over the whole line, which could false-positive
        on a device path) for anything with 'guest' in it, then `find` under each
        match — the same shape _discover_guest_volume_files returns, so the two
        mechanisms' results are directly comparable.

        A first real run's naive `mount | grep -oE '/[^ ]*guest[0-9]+'` found
        NOTHING on any of 4 KV nodes, even though guest volumes were genuinely
        attached and migrating — a second real run's GET /fusion/
        activeGuestVolumes response then confirmed the actual path is
        /{planUUID}/{shardNo}/guest (no numeric slot suffix on 'guest' itself),
        which is exactly why that digit-anchored pattern could never have matched.
        This version drops the digit requirement and parses the mountpoint field
        properly, so it is a genuine second opinion now, not a doomed-from-the-
        start probe — run and reported side by side with the activeGuestVolumes-
        driven mechanism in CLG2, not treated as the primary source of truth
        (the API is CBS's own authoritative view; this is a filesystem-side
        cross-check in case the API is ever unavailable or wrong).

        Returns {mount_path: [file_path, ...]}, possibly {}.
        """
        result = self._run_on_cluster_node(
            instance_id,
            "MOUNTS=$(mount | awk '{for(i=1;i<=NF;i++) if($i==\"on\") "
            "print $(i+1)}' | grep -i guest || true); "
            "if [ -z \"$MOUNTS\" ]; then echo NO_GUEST_MOUNT_VIA_MOUNT; else "
            "for m in $MOUNTS; do echo \"MOUNT=$m\"; "
            "find \"$m\" -type f 2>/dev/null | sed 's/^/FILE=/'; done; fi")
        stdout = result.get("stdout") or ""
        mounts = {}
        current_mount = None
        for line in stdout.splitlines():
            line = line.strip()
            if line == "NO_GUEST_MOUNT_VIA_MOUNT" or not line:
                continue
            if line.startswith("MOUNT="):
                current_mount = line[len("MOUNT="):].strip()
                mounts[current_mount] = []
            elif line.startswith("FILE=") and current_mount:
                mounts[current_mount].append(line[len("FILE="):].strip())
        if mounts:
            self.log.info(
                f"[guest-vol-discovery][via-mount] {instance_id}: "
                f"{len(mounts)} guest mount(s): "
                + ", ".join(f"{m} ({len(files)} file(s))"
                           for m, files in mounts.items()))
        else:
            self.log.warning(
                f"[guest-vol-discovery][via-mount] {instance_id}: no guest "
                f"mount found (raw output: {stdout!r})")
        return mounts

    def _get_active_guest_volume_paths_by_instance(self):
        """Map {instance_id: [guest_volume_path, ...]} from CBS's own
        GET /fusion/activeGuestVolumes — the exact paths CBS itself currently
        believes hold guest-volume data, keyed by ns_server node and remapped here
        to the AWS instance ID that node actually is.

        This is ground truth in a way `mount | grep guest<N>` is not (see
        _discover_guest_volume_mounts_via_mount's docstring for what that approach
        found instead: nothing). ACCELERATION.md documents Phase 7's payload to CBS
        as exactly this same shape — "GuestVolumes": ["/{planUUID}/{shardNo}/
        guest{slot}", ...] per node — so activeGuestVolumes is CBS echoing back
        the very data structure that told it where to look; asking CBS directly
        removes the guesswork entirely instead of re-deriving the same path from
        the filesystem side.

        NODE-KEY MAPPING. A real run's response showed keys like
        'ns_1@svc-d-node-001.<pod>.sandbox...' — a Capella-managed per-node
        HOSTNAME, not a bare IP the way an on-prem ns_1@<ip> key would read. An
        earlier version of this method compared that hostname directly against
        fusion_aws_util.ec2.get_instance_private_ip(), which never matches (a
        DNS name isn't an IP literal). Fixed by reusing FusionMonitorUtil.
        get_hostname_public_ip_mapping — the same hostname -> public-IP resolution
        this codebase already established for exactly this gotcha (fusion_monitor_
        util.py's own get_attached_guest_volumes-adjacent code resolves node.ip via
        socket.gethostbyname for the same reason) — then matching that public IP
        against fusion_aws_util.ec2.get_instance_public_ip() per KV instance.
        """
        self.fusion_monitor.set_admin_credentials(self.cluster)
        status, content = FusionRestAPI(
            self.cluster.master).get_active_guest_volumes()
        self.assertTrue(
            status, f"Failed to fetch /fusion/activeGuestVolumes: {content}")
        self.log.info(f"[guest-vol-discovery] /fusion/activeGuestVolumes: {content}")
        if not isinstance(content, dict):
            self.log.warning(
                f"[guest-vol-discovery] unexpected activeGuestVolumes shape "
                f"(expected a dict keyed by node): {content!r}")
            return {}

        self.fusion_monitor.get_hostname_public_ip_mapping(
            self.cluster, suppress_log=True)
        hostname_to_public_ip = self.cluster.hostname_public_ip_mapping

        kv_ids = self._kv_instance_ids()
        public_ip_to_instance = {
            self.fusion_aws_util.ec2.get_instance_public_ip(i): i
            for i in kv_ids}

        result = {}
        for node_key, paths in content.items():
            hostname = node_key.split("@", 1)[-1] if "@" in node_key else node_key
            public_ip = hostname_to_public_ip.get(hostname)
            if not public_ip:
                self.log.warning(
                    f"[guest-vol-discovery] no public-IP mapping for node "
                    f"{node_key} (hostname={hostname}) — cannot map its "
                    f"guest volume path(s) {paths} to an SSM target")
                continue
            instance_id = public_ip_to_instance.get(public_ip)
            if not instance_id:
                self.log.warning(
                    f"[guest-vol-discovery] {hostname} resolved to public "
                    f"IP {public_ip}, but no KV instance's public IP "
                    f"matched it (guest volume path(s) {paths})")
                continue
            if paths:
                result[instance_id] = paths
        return result

    def _discover_guest_volume_files(self, instance_id, guest_paths):
        """List what is actually under each of instance_id's guest-volume paths
        (from _get_active_guest_volume_paths_by_instance), including whether the
        path itself is a symlink — `ls -la`'s leading 'l' + '-> target' confirms or
        refutes that directly, rather than guessing.

        Returns {guest_path: [file_path, ...]}.
        """
        if not guest_paths:
            return {}
        script_parts = []
        for p in guest_paths:
            escaped = p.replace("'", "'\\''")
            script_parts.append(
                f"echo \"PATH={escaped}\"; "
                f"ls -la '{escaped}' 2>&1 | sed 's/^/LS=/'; "
                f"find '{escaped}' -type f 2>/dev/null | sed 's/^/FILE=/'")
        result = self._run_on_cluster_node(
            instance_id, "; ".join(script_parts))
        stdout = result.get("stdout") or ""
        files_by_path = {}
        current = None
        for line in stdout.splitlines():
            line = line.rstrip()
            if line.startswith("PATH="):
                current = line[len("PATH="):].strip()
                files_by_path[current] = []
            elif line.startswith("FILE=") and current:
                files_by_path[current].append(line[len("FILE="):].strip())
            elif line.startswith("LS="):
                self.log.info(f"[guest-vol-discovery] {instance_id}: {line}")
        if files_by_path:
            self.log.info(
                f"[guest-vol-discovery] {instance_id}: "
                + ", ".join(f"{p} ({len(f)} file(s))"
                           for p, f in files_by_path.items()))
        return files_by_path

    def _corrupt_files_with_junk_bytes(self, instance_id, file_paths, num_bytes=64):
        """Append num_bytes of junk (dd from /dev/urandom) to each file in file_paths.

        Matches the on-prem "append" corruption_type in fusion_log_corruption.py:
        appending past the end of a structured binary log file is enough to corrupt it
        without needing to know its internal format. Returns the subset of file_paths
        dd actually reported success for.
        """
        if not file_paths:
            return []
        script_lines = []
        for f in file_paths:
            escaped = f.replace("'", "'\\''")
            script_lines.append(
                f"SIZE=$(stat -c%s '{escaped}' 2>/dev/null || echo 0); "
                f"dd if=/dev/urandom of='{escaped}' bs=1 count={num_bytes} "
                f"seek=$SIZE conv=notrunc 2>/dev/null && "
                f"echo \"CORRUPTED={escaped}\" || echo \"FAILED={escaped}\"")
        result = self._run_on_cluster_node(
            instance_id, "; ".join(script_lines), timeout=120)
        stdout = result.get("stdout") or ""
        corrupted = [line[len("CORRUPTED="):].strip()
                    for line in stdout.splitlines() if line.startswith("CORRUPTED=")]
        failed = [line[len("FAILED="):].strip()
                 for line in stdout.splitlines() if line.startswith("FAILED=")]
        if failed:
            self.log.warning(f"{instance_id}: dd failed on {failed}")
        return corrupted

    def _read_failure_total(self):
        """Sum ep_data_read_failed across every KV node/bucket (cbstats over SSM).

        Never read anywhere else in this cloud suite before this test — the on-prem
        equivalent (fusion_log_corruption.py's _get_read_failure_stats) uses
        Cbstats(server) directly over SSH, unreachable on Capella dedicated. Reuses the
        already-generic _sum_cbstat_across_nodes rather than adding new plumbing.
        """
        return self._sum_cbstat_across_nodes("ep_data_read_failed")

    def _run_full_read_workload_with_watchdog(self, read_items, timeout, label):
        """Run a read-only workload over [0, read_items) with a bounded wait, force-
        stopping the task if it does not finish within timeout.

        _run_read_workload's wait_for_load=True blocks inside _load_data with no
        external timeout — fine against a healthy cluster, but a read against a
        corrupted guest-volume log file can stall per key far longer than any
        reasonable build budget instead of failing fast. Mirrors pytests/storage/
        fusion/fusion_log_corruption.py's on-prem _wait_for_tasks_with_watchdog, built
        directly on JavaDocLoaderUtils.perform_load(wait_for_load=False) + self.
        task_manager the same way _start_background_load/_stop_background_load do,
        since _load_data itself never returns its tasks when asked to wait.
        """
        buckets = list(self.cluster.buckets)
        if not buckets:
            self.log.warning(f"[{label}] no buckets to read")
            return
        for bucket in buckets:
            JavaDocLoaderUtils.generate_docs(
                bucket=bucket, doc_ops=["read"], read_start=0, read_end=read_items)
        tasks = JavaDocLoaderUtils.perform_load(
            cluster=self.cluster, buckets=buckets,
            overRidePattern={"create": 0, "read": 100, "update": 0,
                             "delete": 0, "expiry": 0},
            wait_for_load=False, validate_data=False, wait_for_stats=False)
        if not tasks:
            self.log.warning(f"[{label}] no read task(s) were created")
            return
        done = threading.Event()

        def _watchdog():
            if not done.wait(timeout):
                self.log.warning(
                    f"[{label}] read workload exceeded {timeout}s; force-stopping "
                    f"{len(tasks)} task(s)")
                for task in tasks:
                    try:
                        self.task_manager.stop_task(task)
                    except Exception as e:
                        self.log.warning(f"[{label}] error stopping task: {e}")

        watchdog = threading.Thread(target=_watchdog)
        watchdog.start()
        try:
            for task in tasks:
                self.task_manager.get_task_result(task)
        finally:
            done.set()
            watchdog.join()
        self.log.info(f"[{label}] read workload over [0, {read_items}) finished")

    def _delete_files(self, instance_id, file_paths):
        """Delete each file in file_paths on instance_id; return the ones actually
        removed.

        Confirmed via a post-delete existence check (`[ ! -e ... ]`), not just
        `rm`'s own exit code — a permission issue on a root-owned file could
        otherwise still report a misleading success.
        """
        if not file_paths:
            return []
        script_lines = []
        for f in file_paths:
            escaped = f.replace("'", "'\\''")
            script_lines.append(
                f"sudo rm -f '{escaped}' 2>/dev/null || rm -f '{escaped}' "
                f"2>/dev/null; "
                f"if [ ! -e '{escaped}' ]; then echo \"DELETED={escaped}\"; "
                f"else echo \"FAILED={escaped}\"; fi")
        result = self._run_on_cluster_node(
            instance_id, "; ".join(script_lines), timeout=60)
        stdout = result.get("stdout") or ""
        deleted = [line[len("DELETED="):].strip()
                  for line in stdout.splitlines() if line.startswith("DELETED=")]
        failed = [line[len("FAILED="):].strip()
                 for line in stdout.splitlines() if line.startswith("FAILED=")]
        if failed:
            self.log.warning(f"{instance_id}: could not delete {failed}")
        return deleted

    def _confirm_files_absent(self, instance_id, file_paths, attempts=3,
                              retry_interval=5):
        """Independent double-check that file_paths are actually gone: a SEPARATE
        `ls -la` SSM round trip after the delete, not just the delete command's own
        inline existence check.

        Returns the list of paths `ls` still found (should be empty). Logs the raw
        `ls` output for every path either way, so a real run's evidence is visible
        in the log without having to trust the delete command's own report alone.

        KNOWN BUG, FIXED: a real run (jenkins_output8.log) had the SSM round trip
        itself fail outright ("Waiter encountered a terminal failure state ...
        Status: Failed", stdout=''), yet this method still logged "confirmed all
        N path(s) absent" -- an empty stdout parses to zero still-present entries
        the same way a genuinely clean `ls` output would, so an SSM-level failure
        to even RUN the check was silently indistinguishable from a successful
        check that found nothing. This retries the round trip a few times (SSM
        failures like that one are typically transient, same reasoning as
        _crash_dp_accelerator's retry loop), and if every attempt still fails to
        execute, records an INCONCLUSIVE finding instead of a false confirmation
        -- returning [] either way (an infra hiccup unrelated to the delete itself
        should not hard-fail the test), but the stage issue summary now shows
        which outcome actually happened.
        """
        if not file_paths:
            return []
        script_lines = []
        for f in file_paths:
            escaped = f.replace("'", "'\\''")
            script_lines.append(
                f"echo \"PATH={escaped}\"; ls -la '{escaped}' 2>&1")
        script = "; ".join(script_lines)

        result = {}
        for attempt in range(1, attempts + 1):
            result = self._run_on_cluster_node(instance_id, script, timeout=60)
            if result.get("success"):
                break
            self.log.warning(
                f"[confirm-absent] {instance_id}: ls round trip attempt "
                f"{attempt}/{attempts} failed at the SSM level "
                f"(stderr={result.get('stderr')!r})")
            if attempt < attempts:
                time.sleep(retry_interval)

        if not result.get("success"):
            self._record_issue(
                "confirm-absent: ls round trip never executed",
                f"{instance_id}: the independent `ls -la` check could not be "
                f"run at all after {attempts} attempt(s) (SSM-level failure "
                f"every time, last stderr={result.get('stderr')!r}) -- this is "
                f"INCONCLUSIVE, not confirmation either way. The delete "
                f"command's own inline check already reported success for "
                f"{file_paths}; this failure just means that report has no "
                f"independent corroboration for this run.", severity="WARNING")
            return []

        stdout = result.get("stdout") or ""
        self.log.info(
            f"[confirm-absent] {instance_id} `ls -la` on {len(file_paths)} "
            f"deleted path(s):\n{stdout}")
        still_present = []
        current = None
        for line in stdout.splitlines():
            line = line.strip()
            if line.startswith("PATH="):
                current = line[len("PATH="):].strip()
                continue
            if current is None or not line:
                continue
            # The one content line following a PATH= marker: absent shows
            # "No such file or directory" (ls writes this to stderr, merged in
            # via 2>&1); anything else means `ls` found something there.
            if "No such file or directory" not in line:
                still_present.append(current)
            current = None
        if still_present:
            self.log.error(
                f"[confirm-absent] {instance_id}: `ls` still finds "
                f"{still_present} — the delete did not actually take, "
                f"despite the delete command's own report")
        else:
            self.log.info(
                f"[confirm-absent] {instance_id}: confirmed all "
                f"{len(file_paths)} path(s) absent via a separate `ls` call")
        return still_present

    def _monitor_active_guest_volumes(self, duration, poll_interval, label):
        """Poll GET /fusion/activeGuestVolumes for `duration` seconds, logging the
        raw response and total count each time.

        Deletion is a strictly more aggressive fault than a junk-byte append (see
        test_corrupt_log_files_on_guest_volumes' own null result), so once
        migration resumes after a delete, this watches how CBS's own view of
        guest-volume state evolves while it drains -- does the count trend to 0
        the way a clean drain would (same signal fusion_accelerator_chaos_test.py's
        "Chaos RL5: activeGuestVolumes counts only completed shards" stage reads),
        does anything look stuck, or does an entry ever look wrong. Purely
        observational -- never asserts, since nothing in this codebase has
        established what a deleted-file's effect on this endpoint should look
        like yet; the log history is the finding.

        Stops early once the total reaches 0 (migration has genuinely finished
        draining every guest volume) rather than always waiting out the full
        `duration` — a real run found the count getting stuck at a nonzero total
        for the ENTIRE default window with no sign of draining, so `duration`
        needs to be generous (long enough to plausibly reach 0), and an early
        exit is what keeps a lighter, faster-draining run from wasting that
        whole window regardless.

        Returns the list of per-poll totals observed (possibly empty if every
        poll failed).
        """
        self.fusion_monitor.set_admin_credentials(self.cluster)
        deadline = time.time() + duration
        history = []
        while time.time() < deadline:
            try:
                status, content = FusionRestAPI(
                    self.cluster.master).get_active_guest_volumes()
                if status and isinstance(content, dict):
                    total = sum(len(v or []) for v in content.values())
                    history.append(total)
                    self.log.info(
                        f"[active-guest-volumes][{label}] {content} "
                        f"(total={total})")
                    if total == 0:
                        self.log.info(
                            f"[active-guest-volumes][{label}] reached 0 "
                            f"after {len(history)} poll(s) — stopping early "
                            f"rather than waiting out the full {duration}s")
                        break
                else:
                    self.log.warning(
                        f"[active-guest-volumes][{label}] unexpected "
                        f"response: status={status} content={content}")
            except Exception as e:
                self.log.warning(
                    f"[active-guest-volumes][{label}] could not read: {e}")
            time.sleep(poll_interval)
        else:
            if history and history[-1] != 0:
                self.log.warning(
                    f"[active-guest-volumes][{label}] never reached 0 "
                    f"within {duration}s (last total={history[-1]}) — "
                    f"migration may still have been draining when this "
                    f"stopped watching; raise "
                    f"active_guest_volume_monitor_duration if so")
        self.log.info(
            f"[active-guest-volumes][{label}] history over {duration}s: "
            f"{history}")
        return history

    def _run_guest_volume_log_tamper_test(
            self, tamper_action, tamper_verb, tamper_verb_base, files_per_node,
            read_workload_timeout, no_effect_hint, stage_label,
            monitor_active_guest_volumes=False):
        """Shared driver for test_corrupt_log_files_on_guest_volumes and
        test_delete_log_files_from_guest_volumes_after_attachment — the two
        COVERAGE.md §9 rows differ only in WHAT is done to a discovered log
        file (junk-byte append vs. delete), never in how the file is found,
        how the outcome is measured, or how the cluster is cleaned up
        afterward, so that shared machinery lives here once.

        Sequence: freeze migration (rate limit 0) BEFORE the rebalance, so
        guest volumes attach but nothing copies out of them yet -- the tamper
        has to land before migration could have already safely copied the
        real data elsewhere, or it proves nothing (same reasoning as the
        on-prem fusion_log_corruption.py test this is descended from) ->
        rebalance completes with guest volumes attached -> discover guest-
        volume path(s) and the files under them on every KV node holding one,
        via TWO independent mechanisms run side by side (`mount`'s own
        mountpoint field grepped for 'guest' + find, and GET /fusion/
        activeGuestVolumes + ls/find over SSM -- see
        _discover_guest_volume_mounts_via_mount's and
        _get_active_guest_volume_paths_by_instance's docstrings for why
        neither is trusted alone), logged and compared, then unioned into one
        target set -> tamper_action() on up to files_per_node file(s) per
        node -> clear each KV node's page cache so reads hit disk, not a
        stale cached copy -> capture the ep_data_read_failed baseline,
        resume migration, run a bounded (watchdog-guarded) read workload over
        every loaded document -> record whether read failures and/or
        migration failures increased, and whether any node's
        clusterMembership changed or the cluster's own state moved away from
        "healthy" (informational -- neither this codebase nor a first run of
        either variant has established what "correct" looks like here) ->
        no orphaned accelerator/guest-volume resources.

        :param tamper_action: bound method(instance_id, [file_path, ...]) ->
            [file_path actually tampered with, ...] -- the only thing that
            differs between the two callers
        :param tamper_verb: past-tense verb for log/assertion messages (e.g.
            "corrupted", "deleted")
        :param tamper_verb_base: infinitive form of tamper_verb (e.g. "corrupt",
            "delete") -- kept as its own explicit parameter rather than derived
            from tamper_verb by string surgery, since English past-tense
            formation isn't a fixed suffix ("corrupt" -> "corrupted" adds "ed",
            but "delete" -> "deleted" adds only "d") and stripping a character
            SET (str.rstrip) is not the same operation as removing a suffix
            either way
        :param files_per_node: how many files per node to pass to
            tamper_action
        :param read_workload_timeout: bound on the post-tamper read workload
        :param no_effect_hint: appended to the "no observable effect" WARNING
            if nothing was; the two tamper types have different plausible
            innocent explanations for a null result, so each caller supplies
            its own
        :param stage_label: stage-name prefix (e.g. "CLG", "DLG") so two
            tests' logs stay visually distinct even interleaved in one run
        :param monitor_active_guest_volumes: if True, poll GET /fusion/
            activeGuestVolumes for active_guest_volume_monitor_duration seconds
            right after resuming migration, logging the trend as it drains
            (see _monitor_active_guest_volumes) -- opt-in per caller, not the
            default, since it adds real wall-clock time to the test
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")
        self._apply_fusion_config_from_params()
        self._apply_fusion_sync_threshold()
        s3_bucket_name, _ = self._capture_s3_log_store_baseline()
        self._load_above_threshold()
        total_items = int(self.input.param("create_end", 20_000_000))

        if not self._pause_migration():
            self.fail(
                f"Could not freeze background migration — the tamper has "
                f"to land before migration could already have copied the "
                f"real data elsewhere, or the outcome stage cannot tell "
                f"whether it was ever actually read.")

        try:
            rebalance_task = self._trigger_rebalance()
            self.sleep(30, "Wait for the rebalance to start")

            attached = {}
            with self._stage(
                    f"{stage_label}1: rebalance completes with guest "
                    f"volumes attached"):
                self.wait_for_rebalances([rebalance_task])
                CapellaAPI.wait_until_done(
                    self.pod, self.tenant, self.cluster.id, timeout=1800)
                state = CapellaAPI.get_cluster_state(
                    self.pod, self.tenant, self.cluster.id)
                self.assertEqual(
                    state.lower(), "healthy",
                    f"Cluster is not healthy after the rebalance: {state}")
                placement = self._log_guest_volume_placement(
                    f"after rebalance, before {tamper_verb.replace('ed', 'ing')} "
                    f"log files")
                attached = {i: v for i, v in placement.items()
                           if i != "unattached"}
                self.assertTrue(
                    attached,
                    f"No guest volumes attached after the rebalance — "
                    f"nothing for this test to {tamper_verb_base}")

            mounts_by_instance = {}
            with self._stage(
                    f"{stage_label}2: discover guest-volume mount(s) and "
                    f"files"):
                # Two independent discovery mechanisms, run and compared side
                # by side rather than one gating the other — see both
                # helpers' docstrings for why neither was assumed reliable
                # going in.
                via_mount_by_instance = {}
                for instance_id in attached:
                    files = self._discover_guest_volume_mounts_via_mount(
                        instance_id)
                    if files:
                        via_mount_by_instance[instance_id] = files

                paths_by_instance = self._get_active_guest_volume_paths_by_instance()
                via_api_by_instance = {}
                for instance_id, guest_paths in paths_by_instance.items():
                    files = self._discover_guest_volume_files(
                        instance_id, guest_paths)
                    if files:
                        via_api_by_instance[instance_id] = files

                self.log.info(
                    f"[guest-vol-discovery] comparison — via `mount`: "
                    f"{len(via_mount_by_instance)} node(s) with file(s) "
                    f"({sorted(via_mount_by_instance)}); via "
                    f"activeGuestVolumes: {len(via_api_by_instance)} "
                    f"node(s) with file(s) ({sorted(via_api_by_instance)})")
                self._record_issue(
                    f"{stage_label}2: which discovery mechanism found "
                    f"files (informational)",
                    f"mount-based={sorted(via_mount_by_instance)}, "
                    f"activeGuestVolumes-based={sorted(via_api_by_instance)}"
                    + (" — both agree" if bool(via_mount_by_instance) ==
                       bool(via_api_by_instance) else " — they DISAGREE, "
                       "worth understanding why"),
                    severity="INFO")

                # Union: either mechanism finding a real file is good enough
                # to act on — merge per-instance dicts rather than
                # preferring one.
                for instance_id, files in via_mount_by_instance.items():
                    mounts_by_instance.setdefault(instance_id, {}).update(files)
                for instance_id, files in via_api_by_instance.items():
                    mounts_by_instance.setdefault(instance_id, {}).update(files)

                self.assertTrue(
                    mounts_by_instance,
                    f"Neither discovery mechanism found a single file on "
                    f"any of {sorted(attached)}: `mount`-based search found "
                    f"{sorted(via_mount_by_instance)}, activeGuestVolumes-"
                    f"based search found {sorted(via_api_by_instance)} (API "
                    f"itself reported path(s) for "
                    f"{sorted(paths_by_instance)}). Check the "
                    f"[guest-vol-discovery] log lines above for the raw "
                    f"`mount`/API/`ls -la`/`find` output.")
                self.log.info(
                    f"Guest-volume mounts found on "
                    f"{len(mounts_by_instance)} node(s): "
                    + ", ".join(f"{i}: {list(m)}"
                               for i, m in mounts_by_instance.items()))

            tampered_by_instance = {}
            with self._stage(f"{stage_label}3: {tamper_verb} log files"):
                for instance_id, mounts in mounts_by_instance.items():
                    all_files = [f for files in mounts.values() for f in files]
                    targets = all_files[:files_per_node]
                    if not targets:
                        self.log.warning(
                            f"{instance_id}: guest mount(s) {list(mounts)} "
                            f"found but contain no files to {tamper_verb_base}")
                        continue
                    tampered = tamper_action(instance_id, targets)
                    if tampered:
                        tampered_by_instance[instance_id] = tampered
                self.assertTrue(
                    tampered_by_instance,
                    f"No files were actually {tamper_verb} on any node, "
                    f"even though guest mount(s) were found: "
                    f"{mounts_by_instance}")
                self.log.info(
                    f"{tamper_verb.capitalize()} file(s): "
                    f"{tampered_by_instance}")

            with self._stage(
                    f"{stage_label}4: clear page cache on every KV node"):
                self._run_on_all_kv_nodes(
                    "sync; echo 1 | sudo tee /proc/sys/vm/drop_caches "
                    "2>/dev/null || echo 1 > /proc/sys/vm/drop_caches "
                    "2>/dev/null || true")

            read_failures_before = self._read_failure_total()
            migration_failures_before = self._sum_migration_stat(
                "ep_fusion_migration_failures")
            af_before = set()
            try:
                af_before = {n.ip for n in RestConnection(
                    self.cluster.master).get_nodes(
                        active=False, inactive_failed=True)}
            except Exception as e:
                self.log.warning(
                    f"Could not read node statuses before tampering: {e}")

            self._resume_migration()

            if monitor_active_guest_volumes:
                with self._stage(
                        f"{stage_label}4b: monitor activeGuestVolumes after "
                        f"resuming migration"):
                    monitor_duration = int(self.input.param(
                        "active_guest_volume_monitor_duration", 1800))
                    monitor_interval = int(self.input.param(
                        "active_guest_volume_monitor_interval", 20))
                    self._monitor_active_guest_volumes(
                        monitor_duration, monitor_interval,
                        f"after {tamper_verb} log files")

            with self._stage(
                    f"{stage_label}5: read workload over every loaded "
                    f"document"):
                self._run_full_read_workload_with_watchdog(
                    total_items, read_workload_timeout,
                    f"after {tamper_verb} guest-volume log files")

            with self._stage(
                    f"{stage_label}6: {tamper_verb} detected / node-health "
                    f"reaction (findings)"):
                read_failures_after = self._read_failure_total()
                migration_failures_after = self._sum_migration_stat(
                    "ep_fusion_migration_failures")
                state_after = CapellaAPI.get_cluster_state(
                    self.pod, self.tenant, self.cluster.id)
                af_after = set()
                try:
                    af_after = {n.ip for n in RestConnection(
                        self.cluster.master).get_nodes(
                            active=False, inactive_failed=True)}
                except Exception as e:
                    self.log.warning(
                        f"Could not read node statuses after tampering: {e}")
                newly_failed = af_after - af_before
                read_delta = read_failures_after - read_failures_before
                migration_delta = (
                    migration_failures_after - migration_failures_before)
                self.log.info(
                    f"[{tamper_verb}-outcome] read_failures "
                    f"{read_failures_before} -> {read_failures_after} "
                    f"(delta={read_delta}), migration_failures "
                    f"{migration_failures_before} -> "
                    f"{migration_failures_after} (delta={migration_delta}), "
                    f"cluster_state={state_after}, newly inactiveFailed "
                    f"node(s)={sorted(newly_failed)}")
                self._record_issue(
                    f"{stage_label}6: {tamper_verb} outcome (informational)",
                    f"read_failures_delta={read_delta}, "
                    f"migration_failures_delta={migration_delta}, "
                    f"cluster_state={state_after}, "
                    f"newly_failed_over={sorted(newly_failed)} — see this "
                    f"test's docstring for why this is recorded rather than "
                    f"asserted: ep_data_read_failed-over-cbstats-over-SSM "
                    f"is an unconfirmed mechanism as of this test's first "
                    f"run.",
                    severity="INFO")
                if not newly_failed and read_delta <= 0 and \
                        migration_delta <= 0:
                    self._record_issue(
                        f"{stage_label}6: no observable effect from the "
                        f"{tamper_verb} files",
                        f"Neither read failures, migration failures, nor a "
                        f"new auto-failover were observed after "
                        f"{tamper_verb} "
                        f"{sum(len(v) for v in tampered_by_instance.values())} "
                        f"file(s) — {no_effect_hint}", severity="WARNING")
        finally:
            self._restore_migration_rate_limit()

        with self._stage(
                f"{stage_label}7: no orphaned accelerator/guest-volume "
                f"resources"):
            self._assert_no_orphan_accelerator_resources(
                label=f"the log-file {tamper_verb} test")

        self._assert_no_stage_issues()

    def test_corrupt_log_files_on_guest_volumes(self):
        """
        Corrupt (junk-byte append) log files on a KV node's attached guest volume
        while migration is frozen, then resume migration and run a read workload
        over every loaded document, checking whether the corruption is detected
        (read failures and/or migration failures) and whether it triggers any
        node-health reaction (auto-failover / cluster state change).

        COVERAGE.md §9 "Corrupt log files on guest volumes (junk bytes)". Cloud
        analog of pytests/storage/fusion/fusion_log_corruption.py's on-prem
        test_fusion_log_corruption_during_extent_migration (corrupt_log_files_on_
        guest_volumes), rebuilt for Capella dedicated: guest volumes there are files
        under an NFS export reachable from the CBS master over SSH; here they are
        individual EBS volumes mounted directly on the KV node's own EC2 instance,
        reachable only over SSM. See _run_guest_volume_log_tamper_test (the shared
        driver this and test_delete_log_files_from_guest_volumes_after_attachment
        both call) for the full sequence and how the guest-volume files are found.

        FINDING THE FILES: NOT via `mount`. A first real run of this test found NO
        mount whose path contained 'guestN' on any of 4 KV nodes, even though guest
        volumes were genuinely attached and migrating (see
        _discover_guest_volume_mounts_via_mount's docstring) — ACCELERATION.md
        documents the MOUNT PATH format as /{planUUID}/{shardNo}/guest{slot}, but a
        second real run's GET /fusion/activeGuestVolumes response showed the actual
        path as /{planUUID}/{shardNo}/guest -- no numeric slot suffix on 'guest'
        itself (the shard number is the slot indicator, one directory up), which is
        exactly why the mount-based `guest[0-9]+` pattern never matched anything. A
        third real run, after both discovery mechanisms were fixed, confirmed both
        agree and both find real files (e.g. .../guest/kv/<bucket_uuid>/kvstore-
        <N>/log-<X>.<Y>) — that same run corrupted 9 files (junk-byte append) but
        observed NO read or migration failures, most likely because appending past
        EOF leaves the original, still-valid content intact and any per-record
        checksum/length framing in fusion's log format may never reach the
        appended garbage. See test_delete_log_files_from_guest_volumes_after_
        attachment for the more aggressive variant this finding motivated.

        NODE-KEY MAPPING. activeGuestVolumes' keys are Capella-managed per-node
        HOSTNAMES (ns_1@svc-d-node-NNN.<pod>.sandbox...), not bare IPs — see
        _get_active_guest_volume_paths_by_instance's docstring for how this is
        resolved (FusionMonitorUtil.get_hostname_public_ip_mapping, the same
        hostname/IP gotcha this codebase already solved elsewhere, reused here
        rather than re-derived).

        Only "a guest-volume file was found" and "files were actually corrupted"
        are hard requirements — whether the corruption is DETECTED (read/migration
        failures, node-health reaction) is recorded as a finding, not a hard
        pass/fail gate, per a real run's own null result above.
        """
        files_per_node = int(self.input.param("corrupt_files_per_node", 3))
        read_workload_timeout = int(
            self.input.param("corrupt_read_workload_timeout", 3600))
        self._run_guest_volume_log_tamper_test(
            tamper_action=self._corrupt_files_with_junk_bytes,
            tamper_verb="corrupted",
            tamper_verb_base="corrupt",
            files_per_node=files_per_node,
            read_workload_timeout=read_workload_timeout,
            no_effect_hint=(
                "worth confirming the corrupted files were actually on the "
                "read/migration path for the data just loaded, not stale "
                "files from an earlier test on this shared cluster — also "
                "plausible that a junk-byte append PAST EOF simply never "
                "gets read (the original content is still intact, and any "
                "per-record checksum/length framing may make the reader "
                "stop before ever reaching the appended garbage); see "
                "test_delete_log_files_from_guest_volumes_after_attachment "
                "for a more aggressive variant"),
            stage_label="CLG")

    # ------------------------------------------------------------------
    # Delete log files from guest volumes after attachment
    # ------------------------------------------------------------------

    def test_delete_log_files_from_guest_volumes_after_attachment(self):
        """
        Delete (not just corrupt) log files on a KV node's attached guest volume
        while migration is frozen, then resume migration and run a read workload
        over every loaded document, checking whether the deletion is detected
        (read failures and/or migration failures) and whether it triggers any
        node-health reaction (auto-failover / cluster state change).

        COVERAGE.md §9 "Delete log files from guest volumes after attachment".
        Sibling of test_corrupt_log_files_on_guest_volumes — same discovery,
        same read-workload/outcome machinery (both call the shared
        _run_guest_volume_log_tamper_test driver), the ONLY difference is the
        tamper action: unlink the file outright instead of appending junk bytes
        past its end.

        WHY THIS TEST EXISTS. test_corrupt_log_files_on_guest_volumes's own
        first real run corrupted 9 files (junk-byte append) and observed ZERO
        read or migration failures — plausibly because the original, still-
        valid content survives a trailing append untouched. Deleting the file
        outright is a strictly more aggressive fault: there is no "original
        content" left for a reader to fall back on, so if this ALSO shows no
        effect, that is a much stronger signal something about the read/
        migration path, or the outcome-detection mechanism itself (ep_data_
        read_failed over cbstats-via-SSM, never confirmed working in this
        cloud suite before either sibling test), needs a closer look.

        Only "a guest-volume file was found" and "files were actually deleted"
        are hard requirements — whether the deletion is DETECTED is recorded as
        a finding, not a hard pass/fail gate, same posture as the corruption
        sibling and for the same reason: neither test has established what
        "correct" looks like here yet.

        Unlike the corruption sibling, this also monitors GET /fusion/
        activeGuestVolumes for active_guest_volume_monitor_duration seconds right
        after resuming migration (monitor_active_guest_volumes=True, see
        _monitor_active_guest_volumes) — purely observational, logging how CBS's
        own view of guest-volume state evolves while migration drains a volume
        whose files were just deleted out from under it.
        """
        files_per_node = int(self.input.param("delete_files_per_node", 3))
        read_workload_timeout = int(
            self.input.param("delete_read_workload_timeout", 3600))
        self._run_guest_volume_log_tamper_test(
            tamper_action=self._delete_files,
            tamper_verb="deleted",
            tamper_verb_base="delete",
            files_per_node=files_per_node,
            read_workload_timeout=read_workload_timeout,
            no_effect_hint=(
                "a missing log file is a strictly more aggressive fault than "
                "a junk-byte append (test_corrupt_log_files_on_guest_volumes "
                "found no observable effect from that) — if this also shows "
                "no effect, check whether the deleted file(s) were actually "
                "on the read/migration path for the data just loaded, not "
                "stale files from an earlier test on this shared cluster, "
                "and whether ep_data_read_failed over cbstats-via-SSM is "
                "even working as expected in this cloud suite"),
            stage_label="DLG",
            monitor_active_guest_volumes=True)

    # ------------------------------------------------------------------
    # Delete log files from guest volumes during download (Phase 5)
    # ------------------------------------------------------------------

    def _discover_accelerator_download_files(self, instance_id):
        """Find log files under an accelerator's OWN local guest-volume download,
        while it is still in Phase 5 (before Phase 6 hands the volume to a KV node).

        UNLIKE test_delete_log_files_from_guest_volumes_after_attachment's target,
        /fusion/activeGuestVolumes cannot be used here: per ACCELERATION.md, CBS
        only learns a guest volume exists once the CP calls the Phase 7 rebalance
        with a GuestVolumes payload -- an accelerator still downloading in Phase 5
        is entirely CP-managed and invisible to CBS. There is also no confirmed
        local mount-path convention for the ACCELERATOR side the way Phase 6's
        /{planUUID}/{shardNo}/guest is documented for the KV-node side.

        So this searches broadly instead of assuming a mount path: ACCELERATION.md
        says Phase 5's download "recreates the exact directory structure CBS
        expects", and the KV-node side (test_delete_log_files_from_guest_volumes_
        after_attachment) empirically confirmed that structure is kv/<bucket_uuid>/
        kvstore-<N>/log-<X>.<Y> -- the same relative layout `corrupt_fusion_log_
        store`'s S3-side kv/<bucket_uuid>/ prefix uses. `find` looks for that
        relative pattern anywhere under root (pruning /proc, /sys, /dev, which are
        either irrelevant or unsafe to traverse), rather than a specific assumed
        parent mount.

        Returns [file_path, ...], possibly empty (logged either way).
        """
        result = self._run_on_cluster_node(
            instance_id,
            "find / -maxdepth 10 "
            "\\( -path '/proc/*' -o -path '/sys/*' -o -path '/dev/*' \\) -prune "
            "-o -path '*/kv/*/kvstore-*/log-*' -type f -print 2>/dev/null "
            "| head -50", timeout=90)
        stdout = result.get("stdout") or ""
        files = [line.strip() for line in stdout.splitlines() if line.strip()]
        if files:
            self.log.info(
                f"[accel-download-discovery] {instance_id}: {len(files)} "
                f"file(s) found: {files}")
        else:
            self.log.warning(
                f"[accel-download-discovery] {instance_id}: no matching file "
                f"found (raw output: {stdout!r})")
        return files

    def test_delete_log_files_from_guest_volumes_during_download(self):
        """
        Delete log files from an accelerator's OWN guest volume WHILE it is still
        downloading from S3 (Phase 5) -- an earlier boundary than test_delete_log_
        files_from_guest_volumes_after_attachment (Phase 6+, KV-node-side, after
        CBS already knows the volume exists) -- and observe how the CP/accelerator
        react: does the download still "complete" over now-missing local files,
        does the CP detect a problem and redeploy/fall back, and does the shard's
        data end up correct once migration and reads run.

        COVERAGE.md §9 "Delete log files from guest volumes during download".
        Reuses _find_downloading_accelerator (already proven by test_dp_
        accelerator_crash_during_download and test_restart_accelerator_node_mid_
        download) to land on a target actually caught mid-download, and
        fusion_download_rate_limit to widen that window the same single-value way
        those two siblings do (see test_dp_accelerator_crash_during_download's
        module-docstring note on why a low-then-high rate pair does not work).

        UNLIKE the KV-node-side sibling, this does NOT touch fusion_migration_
        rate_limit: the fault lands entirely within Phase 5, long before Phase 8
        migration is even reachable, so there is nothing to freeze against.

        See _discover_accelerator_download_files' docstring for how the target
        files are found without a confirmed mount-path convention on the
        accelerator side. Only "a downloading accelerator was found" and "files
        were actually deleted" are hard requirements; whether the deletion is
        detected (rebalance failure, accelerator redeploy, eventual read/
        migration failures) is recorded as a finding, not asserted, for the same
        reason every other log-file-tamper test in this file treats detection as
        exploratory: nothing in this codebase has established what "correct"
        behavior looks like here yet.

        Sequence:
          1. trigger a rebalance, find an accelerator actually caught mid-download
          2. discover files under its own local guest-volume download
          3. delete up to delete_files_per_node of them
          4. do NOT otherwise touch the accelerator -- let the download/rebalance
             proceed unattended; wait (bounded) for the rebalance to resolve one
             way or another and record which. While waiting, also poll CBS's own
             rebalanceProgress/tasks view (_cbs_rebalance_state) and log the
             actual ns_server rebalance percentage once phase 7 starts -- the
             same read DDD2 already used once, just to prove the delete landed
             BEFORE phase 7; here it runs throughout DDD3 to show progress DURING
             it
          5. resume migration, then monitor GET /fusion/activeGuestVolumes for
             active_guest_volume_monitor_duration seconds, logging how CBS's own
             view of guest-volume state evolves (purely observational -- see
             _monitor_active_guest_volumes)
          6. if healthy, run a read workload and check migration/read failures;
             record the outcome either way
          7. no orphaned accelerator instances/guest volumes
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")
        self._apply_fusion_config_from_params()
        self._apply_fusion_sync_threshold()
        s3_bucket_name, _ = self._capture_s3_log_store_baseline()
        self._load_above_threshold()

        files_per_node = int(self.input.param("delete_files_per_node", 3))
        recovery_timeout = int(
            self.input.param("delete_during_download_recovery_timeout", 1800))

        if not self._pause_migration():
            self.log.warning(
                "Could not freeze background migration — the fault still "
                "lands during download, but the aftermath will be harder "
                "to observe")

        target_id = None
        try:
            rebalance_task = self._trigger_rebalance()
            self.sleep(30, "Wait for the rebalance to start")

            with self._stage("DDD1: find an accelerator still downloading"):
                target_id, tried = self._find_downloading_accelerator(
                    rebalance_task)
                self.assertIsNotNone(
                    target_id,
                    f"No accelerator was ever caught with a live "
                    f"dp-accelerator process — {len(tried)} candidate(s) "
                    f"inspected: {tried}. Raise create_end, lower "
                    f"fusion_download_rate_limit, or ease off "
                    f"fusion_min_split_size_gb to widen the download "
                    f"window, or raise dp_accelerator_target_timeout to "
                    f"search longer.")

            deleted = []
            with self._stage(
                    "DDD2: discover and delete files on the accelerator's "
                    "own download"):
                files = self._discover_accelerator_download_files(target_id)
                targets = files[:files_per_node]
                self.assertTrue(
                    targets,
                    f"No file matching kv/<uuid>/kvstore-<N>/log-<X>.<Y> "
                    f"was found anywhere on {target_id} — see the "
                    f"[accel-download-discovery] log line above for the "
                    f"raw `find` output. Either the download had not "
                    f"written any files yet (raise dp_accelerator_target_"
                    f"timeout so DDD1 lands later in the download), or the "
                    f"accelerator-side directory structure does not match "
                    f"the KV-node-side one this test assumed.")
                deleted = self._delete_files(target_id, targets)
                self.assertTrue(
                    deleted,
                    f"No files were actually deleted on {target_id}, even "
                    f"though {len(targets)} were found: {targets}")
                self.log.info(f"Deleted on {target_id}: {deleted}")

                # Independent double-check: a SEPARATE `ls -la` SSM round trip,
                # not just the delete command's own inline existence check.
                still_present = self._confirm_files_absent(target_id, deleted)
                self.assertFalse(
                    still_present,
                    f"A separate `ls -la` still finds {still_present} on "
                    f"{target_id} after the delete reported success — see "
                    f"the [confirm-absent] log line above for the raw `ls` "
                    f"output.")

                # Prove the delete landed BEFORE ns_server's own Phase-7
                # rebalance call (the one hostedOPD.monitor_rebalance's
                # newMonitorRebalance polls), not just before OUR polling loop
                # noticed "healthy" in DDD3 below. _cbs_rebalance_state reads
                # /pools/default/rebalanceProgress + /pools/default/tasks
                # directly -- the same authoritative signal test_abort_
                # rebalance_invalidates_manifest and test_kill_memcached_
                # during_rebalance already gate their own timing on, rather
                # than inferring order from code structure alone.
                cbs_running, cbs_progress, cbs_detail = \
                    self._cbs_rebalance_state()
                self.log.info(
                    f"[cbs-rebalance-check] immediately after the delete: "
                    f"CBS rebalance running={cbs_running} "
                    f"progress={cbs_progress} detail={cbs_detail}")
                if cbs_running:
                    self._record_issue(
                        "DDD2: CBS rebalance was ALREADY running at delete "
                        "time",
                        f"progress={cbs_progress} detail={cbs_detail} — the "
                        f"delete landed AFTER ns_server's own Phase-7 "
                        f"rebalance call started, not strictly during "
                        f"Phase 5 as this test intends. Lower "
                        f"dp_accelerator_target_timeout or raise "
                        f"fusion_min_split_size_gb so DDD1 lands the target "
                        f"earlier.", severity="WARNING")
                else:
                    self.log.info(
                        "[cbs-rebalance-check] confirmed: CBS rebalance "
                        "(Phase 7) had NOT started yet when the delete "
                        "landed")

            outcome = None
            with self._stage(
                    "DDD3: the rebalance resolves (completes or fails "
                    "cleanly)"):
                # Beyond just waiting for the Capella task-level state to resolve,
                # also poll CBS's own view (_cbs_rebalance_state, the same
                # rebalanceProgress/tasks read DDD2 used to prove the delete
                # landed BEFORE phase 7) so the actual ns_server rebalance
                # percentage is visible in the log while phase 7 is under way,
                # not just the coarse "processing"/"healthy" task state. A
                # transient read failure here only skips one progress line — it
                # never affects `outcome`, which is decided by rebalance_task.
                cbs_rest = ClusterRestAPI(self.cluster.master)
                deadline = time.time() + recovery_timeout
                while time.time() < deadline:
                    if rebalance_task.state in self._FAILED_STATES:
                        outcome = f"clean failure ({rebalance_task.state})"
                        break
                    if rebalance_task.state == "healthy":
                        outcome = "rebalance completed"
                        break
                    try:
                        cbs_running, cbs_progress, _ = \
                            self._cbs_rebalance_state(cbs_rest)
                        if cbs_running:
                            self.log.info(
                                f"[cbs-rebalance] running=True progress="
                                f"{'?' if cbs_progress is None else f'{cbs_progress:.1f}%'} "
                                f"task_state={rebalance_task.state}")
                    except Exception as e:
                        self.log.warning(
                            f"[cbs-rebalance] could not read progress: {e}")
                    time.sleep(5)
                self.assertIsNotNone(
                    outcome,
                    f"Rebalance neither completed nor failed within "
                    f"{recovery_timeout}s of deleting {len(deleted)} "
                    f"file(s) from {target_id}'s own download — the "
                    f"accelerator/CP appears stuck rather than detecting "
                    f"the missing file(s).")
                self.log.info(
                    f"delete-during-download outcome: {outcome}")
                self._record_issue(
                    "DDD3: delete-during-download outcome (informational)",
                    outcome, severity="INFO")

            try:
                self.wait_for_rebalances([rebalance_task])
            except Exception as e:
                self.log.warning(
                    f"Rebalance did not complete cleanly ({e}) — "
                    f"continuing to the cleanup assertions, which a clean "
                    f"failure still has to satisfy")
        finally:
            self._restore_migration_rate_limit()

        CapellaAPI.wait_until_done(
            self.pod, self.tenant, self.cluster.id, timeout=1800)

        with self._stage(
                "DDD3b: monitor activeGuestVolumes after resuming migration"):
            monitor_duration = int(self.input.param(
                "active_guest_volume_monitor_duration", 1800))
            monitor_interval = int(self.input.param(
                "active_guest_volume_monitor_interval", 20))
            self._monitor_active_guest_volumes(
                monitor_duration, monitor_interval,
                "after deleting files from an accelerator's download")

        with self._stage(
                "DDD4: migration/read outcome and no orphaned resources "
                "(findings)"):
            state = CapellaAPI.get_cluster_state(
                self.pod, self.tenant, self.cluster.id)
            if state.lower() == "healthy":
                failures = self._sum_migration_stat(
                    "ep_fusion_migration_failures")
                self._record_issue(
                    "DDD4: migration failures after resolving healthy "
                    "(informational)",
                    f"ep_fusion_migration_failures={failures}",
                    severity="INFO" if failures == 0 else "WARNING")
                self._run_read_workload(
                    "after deleting files from an accelerator's download")
            else:
                self._record_issue(
                    "DDD4: skipping the read workload (informational)",
                    f"cluster state={state} — DDD3 already recorded this "
                    f"as a clean-failure outcome", severity="INFO")
            self._assert_no_orphan_accelerator_resources(
                label="the delete-during-download test")

        self._assert_no_stage_issues()

    # ------------------------------------------------------------------
    # Crash memcached during S3 upload (steady state, no rebalance)
    # ------------------------------------------------------------------

    def test_crash_memcached_during_s3_upload(self):
        """
        Load a large backlog of data with fusion still DISABLED, then enable
        fusion and repeatedly kill memcached (cluster-wide) while the CP is in
        the "enabling" transition — the initial full backfill of that backlog
        to S3 — before letting fusion settle to "enabled" and triggering an
        ordinary fusion rebalance on top.

        COVERAGE.md §10 "Crash Recovery — Server" / "Crash memcached during S3
        upload". This is a full redesign of an earlier version of this test
        that instead tried to land a SINGLE-node kill during fusion's
        steady-state periodic upload cycle, timed only by replaying _wait_for_
        log_store_sync's window-start math against the clock. That is a
        estimate, not a guarantee — there is no direct "is uploading right
        now" signal for the steady-state cycle (checked: FusionRestAPI.
        get_fusion_status()'s snapshotPendingBytes is a backlog SIZE, not an
        active/idle flag), so the kill could land early or late depending on
        how good the estimate was for that particular run.

        WHY DISABLE-THEN-ENABLE IS THE RELIABLE VERSION. Enabling fusion on a
        cluster that already has a large amount of unsynced local data forces
        a real, large, and OBSERVABLE initial backfill: fusion_test_base.py's
        state machine exposes an explicit "enabling" status (GET .../fusion/
        status, or the CP-level CapellaUtils.get_fusion_status) distinct from
        "enabled", and pytests/aGoodDoctor/fusion/fusion_enable_disable_test.py
        already has direct, confirmed prior art for exactly this shape
        (test_kill_memcached_during_enable_cp_retries, test_ns_server_restart_
        during_enable, test_disable_fusion_during_enabling_state) — polling
        get_fusion_status() for state == "enabling" right after calling
        CapellaUtils.enable_fusion() and injecting the fault there, instead of
        guessing at a steady-state cycle's timing. This test borrows that
        pattern directly (see CMU2 below) rather than re-deriving it, and
        extends it two ways: (1) kills memcached REPEATEDLY while still
        "enabling" (enable_kill_count times, not once), matching the "kill it
        a few times" retry-resilience shape test_kill_cp_job_during_scaling
        and the on-prem fusion_sync.py::test_crash_during_large_file_sync
        already use elsewhere in this codebase; (2) once fusion reaches
        "enabled", goes on to trigger a normal fusion-eligible rebalance on
        top, since that combination (crashed-and-recovered enable, then a
        real accelerated rebalance on the same cluster) has not been
        exercised by the sibling tests in fusion_enable_disable_test.py,
        which stop at "enabled". test_kill_memcached_during_enable_cp_retries
        cannot itself trigger that rebalance — FusionEnableDisableTests
        extends _FusionTestBase directly, without _trigger_rebalance or the
        rest of FusionAcceleratorLifecycleTest's rebalance machinery, which
        this file's FusionCPResiliencyTest has — hence this stays a separate
        test here rather than an extension of that one.

        Like every other test in this codebase that reads a CP/ns_server
        state field, landing inside "enabling" is not guaranteed on every
        run: a small-enough backlog could enable faster than this test can
        observe it. CMU2 fails loudly (not silently) if that happens, same
        posture as test_kill_memcached_during_enable_cp_retries's own
        `self.fail("Fusion enabled too fast to inject...")` — raise
        create_end or lower enabling_state_search_timeout's polling interval
        if this becomes a recurring problem for a given cluster/data size.

        Sequence:
          1. ensure fusion DISABLED, then load create_end docs while it stays
             disabled — nothing has synced to S3 yet, so enabling afterward
             has a genuine, large backlog to push
          2. call CapellaUtils.enable_fusion(); poll get_fusion_status() for
             state == "enabling" (bounded by enabling_state_search_timeout)
          3. while still "enabling": kill memcached on every cluster node
             (SSM, concurrent), confirm ns_server restarts it everywhere,
             wait a short settle, repeat up to enable_kill_count times or
             until state leaves "enabling" (whichever first)
          4. wait for fusion to reach "enabled" (_wait_for_fusion_state, the
             same CP-retry-tolerant wait test_kill_memcached_during_enable_
             cp_retries relies on), then for the backlog to actually finish
             syncing: pending bytes -> 0 and S3 objects present (hard gate)
          5. apply the fusion support config/sync threshold (deferred until
             now, since a disabled cluster has no fusion config to apply to)
             and trigger an ordinary fusion rebalance; wait for it healthy
          6. read workload for data integrity; no orphaned accelerator/
             guest-volume resources
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)

        create_end = int(self.input.param("create_end", 50_000_000))
        enabling_state_search_timeout = int(
            self.input.param("enabling_state_search_timeout", 120))
        enable_kill_count = int(self.input.param("enable_kill_count", 3))
        kill_settle_secs = int(self.input.param("enable_kill_settle_secs", 20))
        memcached_restart_timeout = int(
            self.input.param("memcached_restart_timeout", 300))
        sync_wait_timeout = int(self.input.param("sync_wait_timeout", 1800))

        with self._stage("CMU1: load data while fusion is disabled"):
            self._ensure_fusion_state(self.tenant, self.cluster, "disabled")
            self._load_data(self.cluster, create_start=0, create_end=create_end)

        with self._stage(
                "CMU2: enable fusion, kill memcached repeatedly while "
                "'enabling'"):
            resp = CapellaAPI.enable_fusion(
                self.pod, self.tenant, self.cluster.id)
            self.assertEqual(
                resp.status_code, 200,
                f"enable_fusion returned {resp.status_code}: {resp.content}")

            search_deadline = time.time() + enabling_state_search_timeout
            reached_enabling = False
            while time.time() < search_deadline:
                status = CapellaAPI.get_fusion_status(
                    self.pod, self.tenant, self.cluster.id)
                state = status.get("state", "")
                self.log.info(f"[enable-kill] fusion state: {state}")
                if state == "enabling":
                    reached_enabling = True
                    break
                if state == "enabled":
                    break
                time.sleep(2)
            self.assertTrue(
                reached_enabling,
                "Fusion never showed 'enabling' within "
                f"{enabling_state_search_timeout}s of calling enable_fusion "
                "(it may have gone straight to 'enabled' too fast to catch, "
                "or the backlog was too small to trigger a visible backfill "
                "at all) — raise create_end so there is more data to enable "
                "over, or retry.")

            for attempt in range(1, enable_kill_count + 1):
                status = CapellaAPI.get_fusion_status(
                    self.pod, self.tenant, self.cluster.id)
                state = status.get("state", "")
                if state != "enabling":
                    self.log.info(
                        f"[enable-kill] fusion left 'enabling' (now "
                        f"'{state}') before kill attempt {attempt}/"
                        f"{enable_kill_count} — stopping the kill loop")
                    break

                instances = [
                    i.get("InstanceId") for i in
                    self.fusion_aws_util.list_instances(
                        self.fusion_aws_util._cluster_filter(self.cluster.id))]
                self.assertTrue(
                    instances,
                    "No cluster instances found to kill memcached on")

                def _kill(instance_id):
                    self._run_on_cluster_node(
                        instance_id,
                        "sudo pkill -9 memcached || pkill -9 memcached")

                kill_threads = [threading.Thread(target=_kill, args=(i,))
                                for i in instances]
                for t in kill_threads:
                    t.start()
                for t in kill_threads:
                    t.join(timeout=30)
                self.log.info(
                    f"[enable-kill] attempt {attempt}/{enable_kill_count}: "
                    f"killed memcached on {len(instances)} node(s) "
                    f"{sorted(instances)}")

                back_deadline = time.time() + memcached_restart_timeout
                pending = set(instances)
                while pending and time.time() < back_deadline:
                    for instance_id in list(pending):
                        check = self._run_on_cluster_node(
                            instance_id, "pgrep -c memcached || true")
                        if (check.get("stdout") or "").strip() not in (
                                "", "0"):
                            pending.discard(instance_id)
                    if pending:
                        time.sleep(10)
                self.assertFalse(
                    pending,
                    f"memcached did not come back on {sorted(pending)} "
                    f"within {memcached_restart_timeout}s after kill "
                    f"attempt {attempt} — ns_server did not restart it "
                    f"there")
                self.log.info(
                    f"[enable-kill] attempt {attempt}/{enable_kill_count}: "
                    f"memcached is running again on all {len(instances)} "
                    f"node(s)")

                self.sleep(
                    kill_settle_secs,
                    f"Settle after kill attempt {attempt}/{enable_kill_count} "
                    f"before checking whether to kill again")

        with self._stage(
                "CMU3: fusion reaches 'enabled' and the backlog finishes "
                "syncing"):
            self._wait_for_fusion_state(self.tenant, self.cluster, "enabled")
            s3_uri = self.fusion_monitor.get_fusion_s3_uri(self.cluster)
            self.assertIsNotNone(
                s3_uri,
                "S3 URI not found after fusion reached 'enabled' — the "
                "backfill did not complete despite the CP reporting "
                "'enabled'")
            self._wait_for_pending_bytes_zero(
                self.cluster, timeout=sync_wait_timeout)
            self._wait_for_s3_data_synced(
                self.cluster, timeout=sync_wait_timeout)

        with self._stage("CMU4: fusion rebalance completes healthy"):
            self._apply_fusion_config_from_params()
            self._apply_fusion_sync_threshold()
            rebalance_task = self._trigger_rebalance()
            self.wait_for_rebalances([rebalance_task])
            CapellaAPI.wait_until_done(
                self.pod, self.tenant, self.cluster.id, timeout=1800)
            state = CapellaAPI.get_cluster_state(
                self.pod, self.tenant, self.cluster.id)
            self.assertEqual(
                state.lower(), "healthy",
                f"Cluster is not healthy after the post-enable rebalance: "
                f"{state}")

        with self._stage("CMU5: data readable, no orphaned resources"):
            self._run_read_workload(
                "after crashing memcached during fusion enable + rebalance")
            self._assert_no_orphan_accelerator_resources(
                label="the crash-during-enable test")

        self._assert_no_stage_issues()
