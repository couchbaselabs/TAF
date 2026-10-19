"""
KV checks used by the KV upgrade tests (KVUpgradeTests).

Built on the framework helpers wherever one exists: SDKClient result dicts
and the SDK client pool, the doc loader, the Cbstats factory, RebalanceUtil
/ ClusterUtils rebalance status, CollectionUtils and the bucket REST APIs.
Only what has no framework equivalent is implemented here: the versioned
availability probe, the stall watchdog's warmup STAT timing and the raw
getAndLock / unlock calls (SDKClient has no wrapper for them).
"""

import math
import threading
import time
from collections import Counter
from datetime import datetime

from bucket_utils.bucket_ready_functions import DocLoaderUtils
from BucketLib.bucket import Bucket
from BucketLib.BucketOperations import BucketHelper
from cb_constants import CbServer, DocLoading
from cb_server_rest_util.buckets.buckets_api import BucketRestApi
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from cb_tools.cbstats import Cbstats
from couchbase.exceptions import CouchbaseException
from couchbase_helper.documentgenerator import doc_generator
from mc_bin_client import MemcachedClient
from rebalance_utils.rebalance_util import RebalanceUtil
from sdk_client3 import SDKClient
from sdk_exceptions import SDKException
from sdk_utils.sdk_options import SDKOptions

from constants.sdk_constants.java_client import SDKConstants

# KV returns this CAS for a plain GET of a locked document
LOCKED_CAS = (-1, 0xFFFFFFFFFFFFFFFF)


def outcome_name(result):
    """'success', or the short exception name of an SDKClient result dict"""
    return "success" if result["status"] else DocLoaderUtils.get_exception_name(result["error"])


class KVAvailabilityProbe(threading.Thread):
    """
    Continuous write / durable-write / read workload over a fixed key pool.

    Each write carries a per-key version. A write counts as acked only when
    the SDK returns success, and the ledger keeps the highest acked version
    per key. So verify_no_data_loss() can prove every acked write is still
    readable (a higher version is fine: an ambiguous write that landed).

    Outcomes are counted per phase (set_phase), so steady states (expected
    to be error free) can be judged apart from rebalance windows.

    Not a ContinuousDocOpsTask: that task takes one durability level, rewrites
    the same values (so no version ledger) and collects .fail before its
    sub-tasks run. It holds its own SDKClient rather than a pool client, as
    a long-lived pool client would block other pool users for the whole test.
    """

    WRITE = "write"
    READ = "read"
    STALE_READ = "StaleRead"

    def __init__(self, cluster, bucket, scope, collection, num_keys, durability_levels, op_timeout, log):
        super().__init__(name="kv_availability_probe", daemon=True)
        self.client = SDKClient(cluster, bucket, scope=scope, collection=collection)
        self.keys = [f"probe-{index:06d}" for index in range(num_keys)]
        self.durability_levels = durability_levels
        self.op_timeout = op_timeout
        self.log = log
        self.phase = "initial"
        # phase -> Counter of "<op>:<durability>:<outcome>"
        self.outcomes = {}
        # phase -> {failure key: [first, last] local timestamps}, to line a
        # failure up with ns_server / memcached logs of that topology change
        self.failure_times = {}
        self.acked = {}
        self._versions = {}
        self._lock = threading.Lock()
        self._stop_event = threading.Event()

    def set_phase(self, phase):
        with self._lock:
            self.log.info(f"Availability probe phase: {self.phase} -> {phase}")
            self.phase = phase

    def _record(self, op, durability, outcome):
        name = f"{op}:{durability}:{outcome}"
        with self._lock:
            self.outcomes.setdefault(self.phase, Counter())[name] += 1
            if outcome != "success":
                now = datetime.now().isoformat(sep=" ", timespec="milliseconds")
                times = self.failure_times.setdefault(self.phase, {}).setdefault(name, [now, now])
                times[1] = now

    def run(self):
        try:
            self._run_ops()
        except Exception as e:
            # Recorded, so the phase assertions fail instead of the probe
            # silently doing nothing for the rest of the upgrade
            self.log.critical(f"Availability probe crashed: {e}")
            self._record("probe", "-", "ProbeCrashed")

    def _run_ops(self):
        index = 0
        while not self._stop_event.is_set():
            key = self.keys[index % len(self.keys)]
            level = self.durability_levels[index % len(self.durability_levels)]
            index += 1

            version = self._versions.get(key, 0) + 1
            self._versions[key] = version
            durability = "" if level == SDKConstants.DurabilityLevel.NONE else level
            result = self.client.upsert(
                key, {"key": key, "ver": version}, durability=durability, timeout=self.op_timeout
            )
            self._record(self.WRITE, level, outcome_name(result))
            if result["status"]:
                with self._lock:
                    self.acked[key] = max(version, self.acked.get(key, 0))

            if key not in self.acked:
                continue
            result = self.client.read(key, timeout=self.op_timeout)
            outcome = outcome_name(result)
            if result["status"] and result["value"]["ver"] < self.acked[key]:
                outcome = self.STALE_READ
            self._record(self.READ, "-", outcome)

    def stop(self):
        self._stop_event.set()
        self.join(timeout=120)

    def summary(self, phase):
        """
        :return: (total_ops, failed_ops, Counter of failure keys,
                  {failure key: [first, last] timestamp})
        """
        with self._lock:
            counter = Counter(self.outcomes.get(phase, Counter()))
            times = {k: list(v) for k, v in self.failure_times.get(phase, {}).items()}
        failures = Counter({k: v for k, v in counter.items() if not k.endswith(":success")})
        return sum(counter.values()), sum(failures.values()), failures, times

    def verify_no_data_loss(self, retries=3):
        """
        Read back every acked key. Must be called after stop()
        :return: List of (key, acked_version, found_version_or_error)
        """
        lost = []
        for key, acked_version in sorted(self.acked.items()):
            found = None
            for _ in range(retries):
                result = self.client.read(key, timeout=10)
                if result["status"]:
                    found = result["value"]["ver"]
                    break
                found = outcome_name(result)
                time.sleep(1)
            if not isinstance(found, int) or found < acked_version:
                lost.append((key, acked_version, found))
        return lost

    def existing_key_count(self):
        """
        Keys of the pool present in the collection. Failed / ambiguous
        writes may still have created a key, so the expected doc count of
        the probe collection is read back instead of derived from the ledger
        """
        success, _ = self.client.get_multi(self.keys, timeout=10)
        return len(success)

    def close(self):
        self.client.close()


class RebalanceStallMonitor(threading.Thread):
    """
    Watches a running rebalance through ClusterUtils'
    get_rebalance_status_and_progress().

    The upgrade's own waits do not cover this: online_swap waits on
    RebalanceTask, whose check() doubles its retry threshold on every poll
    in a mixed-version cluster (lib/Jython_tasks/task.py), and
    RebalanceUtil.monitor_rebalance() resets its stuck counter while progress
    is 0 and never stops the rebalance.

    Signals recorded:
    - progress not moving for stall_secs -> rebalance stall (CBSE-22926)
    - a memcached 'warmup' STAT call on a watched node not returning within
      stat_timeout -> warmup STAT blocked (CBSE-22272)
    On a stall the rebalance is stopped (RebalanceUtil.stop_rebalance), so the
    test fails with evidence instead of waiting on a hung job.
    """

    def __init__(
        self,
        cluster,
        cluster_util,
        bucket_name,
        watch_nodes,
        log,
        stall_secs=600,
        poll_secs=15,
        stat_timeout=30,
        stop_on_stall=True,
    ):
        super().__init__(name="rebalance_stall_monitor", daemon=True)
        self.cluster = cluster
        self.cluster_util = cluster_util
        self.bucket_name = bucket_name
        self.watch_nodes = watch_nodes
        self.log = log
        self.stall_secs = stall_secs
        self.poll_secs = poll_secs
        self.stat_timeout = stat_timeout
        self.stop_on_stall = stop_on_stall
        self.stall_reason = None
        self.max_stat_latency = {}
        self.progress_samples = []
        self._stop_event = threading.Event()

    def _rebalance_progress(self):
        """Progress %, or None when no rebalance is running"""
        status, progress = self.cluster_util.get_rebalance_status_and_progress(self.cluster)
        return progress if status == "running" else None

    def _warmup_stat_latency(self, node):
        """
        Raw memcached connection on purpose: Cbstats / MemcachedClientHelper
        wait up to 60s for the vBucket map first, which would read as a
        blocked STAT on a node that does not host the bucket yet
        """
        start = time.time()
        client = None
        try:
            client = MemcachedClient(node.ip, node.memcached_port, timeout=self.stat_timeout)
            client.sasl_auth_plain(node.rest_username, node.rest_password)
            client.bucket_select(self.bucket_name)
            client.stats("warmup")
        except Exception as e:
            # Bucket not yet on an incoming node is expected. Only a call
            # that sat for the whole socket timeout means a blocked STAT
            self.log.debug(f"warmup STAT on {node.ip}: {e}")
        finally:
            if client is not None:
                client.close()
        return time.time() - start

    def run(self):
        last_progress, last_change = None, time.time()
        while not self._stop_event.wait(self.poll_secs):
            try:
                progress = self._rebalance_progress()
            except Exception as e:
                self.log.warning(f"Rebalance progress poll failed: {e}")
                continue
            self.progress_samples.append((round(time.time()), progress))
            # Nothing to judge before the rebalance starts (eg: while the
            # spare node is still being installed) or after it ends
            if progress is None:
                continue
            for node in self.watch_nodes:
                latency = self._warmup_stat_latency(node)
                self.max_stat_latency[node.ip] = max(latency, self.max_stat_latency.get(node.ip, 0))
                if latency >= self.stat_timeout * 0.9:
                    self._stalled(f"warmup STAT on {node.ip} took {latency:.0f}s")
                    return

            if progress != last_progress:
                last_progress, last_change = progress, time.time()
            elif time.time() - last_change >= self.stall_secs:
                self._stalled(f"rebalance progress stuck at {progress}% for {self.stall_secs}s")
                return

    def _stalled(self, reason):
        self.stall_reason = reason
        self.log.critical(f"Rebalance stall detected: {reason}. Recent progress samples: {self.progress_samples[-10:]}")
        if self.stop_on_stall:
            stopped = RebalanceUtil(self.cluster).stop_rebalance()
            self.log.critical(f"Rebalance stopped: {stopped}")

    def stop(self):
        self._stop_event.set()
        self.join(timeout=self.stat_timeout + self.poll_secs + 30)


class KVUpgradeChecks:
    """
    Mixin for upgrade tests. Expects self.cluster, self.cluster_util,
    self.bucket_util, self.task, self.task_manager, self.input, self.log,
    self.sleep and self.log_failure from the TAF base test classes
    """

    CHECK_COLLECTION = "kv_upgrade_checks"

    def init_kv_upgrade_check_params(self):
        param = self.input.param
        # Availability probe
        self.probe_num_keys = param("probe_num_keys", 1000)
        self.probe_durability_levels = param("probe_durability_levels", "NONE;MAJORITY;PERSIST_TO_MAJORITY").split(";")
        self.probe_op_timeout = param("probe_op_timeout", 10)
        self.probe_baseline_secs = param("probe_baseline_secs", 60)
        self.max_error_pct_in_rebalance = float(param("max_error_pct_in_rebalance", 1))
        # Engine defaults / behaviour
        self.engine_watch_stats = param(
            "engine_watch_stats",
            "ep_workload:num_readers;ep_workload:num_writers;"
            "ep_workload:num_auxio;ep_workload:num_nonio;"
            "ep_exp_pager_enabled;ep_exp_pager_stime;"
            "ep_getl_default_timeout;ep_getl_max_timeout;"
            "ep_mem_low_wat_percent;ep_mem_high_wat_percent",
        ).split(";")
        self.allowed_engine_changes = [s for s in param("allowed_engine_changes", "").split(";") if s]
        # "stat=value;stat=value" checked on every KV node post-upgrade
        self.expected_engine_values = dict(
            kv.split("=", 1) for kv in param("expected_engine_values", "").split(";") if kv
        )
        self.locked_key_docs = param("locked_key_docs", 16)
        self.allowed_locked_key_changes = [s for s in param("allowed_locked_key_changes", "").split(";") if s]
        # Tombstone purge
        self.validate_tombstone_purge = param("validate_tombstone_purge", True)
        # Days, as the REST purgeInterval. ns_server's minimum for
        # couchbase buckets is ~1 hour (0.04); ephemeral allows less
        self.tombstone_purge_interval = float(param("tombstone_purge_interval", 0.04))
        self.tombstone_docs = param("tombstone_docs", 200)
        self.tombstone_ttl = param("tombstone_ttl", 10)
        self.tombstone_purge_timeout = param("tombstone_purge_timeout", 1800)
        self.tombstone_phases = param("tombstone_phases", "pre;post").split(";")

    # ---------------- Check collection ----------------

    def kv_check_collection(self, bucket):
        """
        (scope, collection) holding every doc these checks write: probe,
        locked-key, TTL and seqno-bump docs. Created once per bucket through
        CollectionUtils, so it is part of the bucket object like any other
        collection. Its check docs are added to num_items only by
        apply_kv_check_doc_counts(), right before doc-count validation:
        spec loads size their deletes from num_items, so counting probe docs
        earlier would make them delete keys that do not exist
        """
        created = self.__dict__.setdefault("_kv_check_collections", {})
        if bucket.name not in created:
            self.bucket_util.create_collection(
                self.cluster.master, bucket, CbServer.default_scope, {"name": self.CHECK_COLLECTION}
            )
            created[bucket.name] = (CbServer.default_scope, self.CHECK_COLLECTION)
        return created[bucket.name]

    def _count_check_docs(self, bucket, num_docs):
        pending = self.__dict__.setdefault("_kv_check_docs", Counter())
        pending[bucket.name] += num_docs

    def apply_kv_check_doc_counts(self):
        """Adds the live check docs to the check collections' num_items"""
        pending = self.__dict__.get("_kv_check_docs", Counter())
        for bucket in self.cluster.buckets:
            if pending.get(bucket.name):
                scope, collection = self.kv_check_collection(bucket)
                bucket.scopes[scope].collections[collection].num_items += pending.pop(bucket.name)

    def _get_check_client(self, bucket):
        scope, collection = self.kv_check_collection(bucket)
        if self.cluster.sdk_client_pool:
            return self.cluster.sdk_client_pool.get_client_for_bucket(bucket, scope, collection)
        return SDKClient(self.cluster, bucket, scope=scope, collection=collection)

    def _release_check_client(self, client):
        if self.cluster.sdk_client_pool:
            self.cluster.sdk_client_pool.release_client(client)
        else:
            client.close()

    # ---------------- Availability probe ----------------

    def start_availability_probe(self, bucket, phase="pre_upgrade"):
        scope, collection = self.kv_check_collection(bucket)
        probe = KVAvailabilityProbe(
            self.cluster,
            bucket,
            scope,
            collection,
            self.probe_num_keys,
            self.probe_durability_levels,
            self.probe_op_timeout,
            self.log,
        )
        probe.probe_bucket = bucket
        probe.set_phase(phase)
        probe.start()
        return probe

    def assert_probe_phase(self, probe, phase, allowed_error_pct=0):
        """
        Steady phases must be error free; rebalance windows may lose up to
        allowed_error_pct % of ops to transient errors. Never acceptable:
        DurabilityImpossible (mixed mode must keep durability possible)
        and stale reads (an acked write that went missing)
        """
        total, failed, failures, times = probe.summary(phase)
        self.log.info(f"Probe phase '{phase}': {total} ops, {failed} failed: {dict(failures)}")
        if times:
            self.log.info(f"Probe phase '{phase}' failure times [first, last] (test host local time): {times}")
        if total == 0:
            self.log_failure(f"Probe phase '{phase}' ran no ops")
            return
        hard = {
            k: v
            for k, v in failures.items()
            if SDKException.check_if_exception_exists(SDKException.DurabilityImpossibleException, k)
            or KVAvailabilityProbe.STALE_READ in k
        }
        if hard:
            hard_times = {k: times.get(k) for k in hard}
            self.log_failure(f"Probe phase '{phase}': durability/data errors {hard} at [first, last] {hard_times}")
        error_pct = failed * 100.0 / total
        if error_pct > allowed_error_pct:
            self.log_failure(
                f"Probe phase '{phase}': {error_pct:.2f}% ops failed (allowed {allowed_error_pct}%): {dict(failures)}"
            )

    def finish_availability_probe(self, probe, phase="post_upgrade"):
        """
        Stops the probe and asserts the final phase and no data loss
        """
        probe.set_phase(phase)
        self.sleep(self.probe_baseline_secs, "Post-upgrade probe baseline")
        probe.stop()
        self.assert_probe_phase(probe, phase)

        lost = probe.verify_no_data_loss()
        self.log.info(f"Probe acked {len(probe.acked)} keys, lost {len(lost)}")
        if lost:
            self.log_failure(
                f"Data loss: {len(lost)} acked writes not readable at their acked version. "
                f"Sample (key, acked, found): {lost[:10]}"
            )

        self._count_check_docs(probe.probe_bucket, probe.existing_key_count())
        probe.close()

    # ---------------- DCP ----------------

    def wait_for_dcp_drain(self, bucket, timeout):
        """
        Polls ep_dcp_items_remaining through the Cbstats factory. Not
        bucket_util._wait_for_stats_all_buckets(check_ep_items_remaining=True):
        its StatsWaitTask timeouts are swallowed by get_task_result(), so a
        DCP stall would pass
        :return: {node_ip: ep_dcp_items_remaining} for KV nodes that did not
                 drain within timeout (empty when every node drained)
        """
        end_time = time.time() + timeout
        while True:
            pending = {}
            for node in self.cluster_util.get_kv_nodes(self.cluster):
                cbstats = Cbstats(node)
                try:
                    remaining = cbstats.dcp_stats(bucket.name).get("ep_dcp_items_remaining")
                except Exception as e:
                    remaining = DocLoaderUtils.get_exception_name(e)
                finally:
                    cbstats.disconnect()
                if str(remaining) != "0":
                    pending[node.ip] = remaining
            if not pending or time.time() > end_time:
                return pending
            self.sleep(10, f"DCP items remaining on {bucket.name}: {pending}")

    # ---------------- Engine defaults ----------------

    @staticmethod
    def _engine_stats(node, bucket):
        """'cbstats all' (ep_* config) + 'cbstats workload' (executor pools)"""
        cbstats = Cbstats(node)
        try:
            stats = dict(cbstats.all_stats(bucket.name))
            stats.update(cbstats.workload_stats(bucket.name))
        finally:
            cbstats.disconnect()
        return stats

    def snapshot_engine_settings(self, bucket):
        """
        :return: {stat: sorted distinct values across KV nodes}, plus the
                 configured thread counts from the memcached global settings.
                 Pool sizes follow the core count, so nodes are assumed to
                 be of the same shape before and after the upgrade
        """
        snapshot = {}
        for node in self.cluster_util.get_kv_nodes(self.cluster):
            try:
                stats = self._engine_stats(node, bucket)
            except Exception as e:
                self.log_failure(f"Engine stats from {node.ip} failed: {e}")
                continue
            for name in self.engine_watch_stats:
                snapshot.setdefault(name, set()).add(str(stats.get(name, "<absent>")))
        status, content = ClusterRestAPI(self.cluster.master).manage_global_memcached_setting()
        if status:
            for name, value in content.items():
                if "thread" in name:
                    snapshot[f"global:{name}"] = {str(value)}
        else:
            self.log.warning(f"memcached global settings GET failed: {content}")
        snapshot = {k: sorted(v) for k, v in snapshot.items()}
        self.log.info(f"Engine settings snapshot: {snapshot}")
        return snapshot

    @staticmethod
    def same_engine_values(old, new):
        """
        Values match as strings or as numbers within 1e-6 relative. Older
        releases print float config at float precision
        (0.8499999977648258 vs 0.85 for ep_mem_high_wat_percent)
        """
        if old == new:
            return True
        if old is None or new is None or len(old) != len(new):
            return False
        try:
            return all(
                math.isclose(float(a), float(b), rel_tol=1e-6)
                for a, b in zip(sorted(old, key=float), sorted(new, key=float), strict=True)
            )
        except ValueError:
            return False

    def compare_engine_settings(self, before, after):
        """
        Flags every watched value that changed across the upgrade, unless
        listed in allowed_engine_changes, and checks expected_engine_values
        """
        for name in sorted(set(before) | set(after)):
            old, new = before.get(name), after.get(name)
            if self.same_engine_values(old, new):
                continue
            if old is None or old == ["<absent>"]:
                self.log.info(f"Engine setting new in target: {name}={new}")
            elif name in self.allowed_engine_changes:
                self.log.info(f"Allowed engine change {name}: {old} -> {new}")
            else:
                self.log_failure(f"Engine setting changed across upgrade: {name} {old} -> {new}")
        for name, expected in self.expected_engine_values.items():
            if not self.same_engine_values(after.get(name), [expected]):
                self.log_failure(f"Engine setting {name}: expected {expected} on every KV node, got {after.get(name)}")

    # ---------------- Locked key / CAS ----------------

    @staticmethod
    def _locked_key_outcomes(client, key):
        """
        Observed getAndLock / CAS behaviour for one key. Plain ops go through
        SDKClient result dicts; getAndLock / unlock have no SDKClient wrapper,
        so they use client.collection as epengine's test_unlock_key does
        """
        lock_time = SDKOptions.get_duration(15, SDKConstants.TimeUnit.SECONDS)

        def lock_op(func):
            try:
                return "success", func()
            except CouchbaseException as e:
                return DocLoaderUtils.get_exception_name(e), None

        result = client.upsert(key, {"k": key})
        if not result["status"]:
            return {"setup": outcome_name(result)}
        original_cas = result["cas"]
        status, locked = lock_op(lambda: client.collection.get_and_lock(key, lock_time))
        if locked is None:
            return {"setup": status}
        lock_cas = locked.cas

        outcome = {"lock_cas": "differs_from_original" if lock_cas != original_cas else "same_as_original"}
        get_cas = client.read(key)["cas"]
        if get_cas in LOCKED_CAS:
            outcome["get_cas_while_locked"] = "locked_cas"
        elif get_cas == lock_cas:
            outcome["get_cas_while_locked"] = "lock_cas"
        elif get_cas == original_cas:
            outcome["get_cas_while_locked"] = "original_cas"
        else:
            outcome["get_cas_while_locked"] = "other"
        outcome["upsert_while_locked"] = outcome_name(client.upsert(key, {"k": key, "v": 1}, timeout=3))
        outcome["relock_while_locked"] = lock_op(lambda: client.collection.get_and_lock(key, lock_time))[0]
        outcome["replace_with_lock_cas"] = outcome_name(client.replace(key, {"k": key, "v": 2}, cas=lock_cas))
        outcome["upsert_after_cas_unlock"] = outcome_name(client.upsert(key, {"k": key, "v": 3}))

        pre_lock_cas = client.read(key)["cas"]
        status, locked = lock_op(lambda: client.collection.get_and_lock(key, lock_time))
        if locked is None:
            outcome["unlock_with_lock_cas"] = status
            return outcome
        outcome["unlock_with_lock_cas"] = lock_op(lambda: client.collection.unlock(key, locked.cas))[0]
        outcome["cas_after_unlock"] = "restored" if client.read(key)["cas"] == pre_lock_cas else "changed"
        return outcome

    def locked_key_signature(self, bucket, phase):
        """
        :return: {check: sorted distinct outcomes over locked_key_docs keys}.
                 Keys spread over vBuckets, so in mixed mode they land on
                 both old and new nodes
        """
        client = self._get_check_client(bucket)
        signature = {}
        try:
            for index in range(self.locked_key_docs):
                outcomes = self._locked_key_outcomes(client, f"locked-{phase}-{index:03d}")
                for check, value in outcomes.items():
                    signature.setdefault(check, set()).add(value)
        finally:
            self._release_check_client(client)
        self._count_check_docs(bucket, self.locked_key_docs)
        signature = {k: sorted(v) for k, v in signature.items()}
        self.log.info(f"Locked-key signature ({phase}): {signature}")
        return signature

    def compare_locked_key_signatures(self, before, during, after):
        """
        - Behaviour before and after the upgrade must match, unless the
          check is listed in allowed_locked_key_changes
        - Mixed mode may only show outcomes seen before or after
        - Invariants that must hold post-upgrade
        """
        for check in sorted(set(before) | set(after)):
            if before.get(check) != after.get(check) and check not in self.allowed_locked_key_changes:
                self.log_failure(
                    f"Locked-key behaviour '{check}' changed across upgrade: {before.get(check)} -> {after.get(check)}"
                )
        if during is not None:
            for check, values in during.items():
                known = set(before.get(check, [])) | set(after.get(check, []))
                unknown = set(values) - known
                if unknown:
                    self.log_failure(
                        f"Locked-key '{check}' in mixed mode shows {sorted(unknown)}, seen neither before nor after"
                    )
        if "setup" in after:
            self.log_failure(f"Locked-key checks could not run post-upgrade: {after['setup']}")
            return
        if "success" in after["upsert_while_locked"]:
            self.log_failure("Upsert without CAS succeeded on a locked key post-upgrade")
        for check in ("replace_with_lock_cas", "upsert_after_cas_unlock", "unlock_with_lock_cas"):
            if after.get(check) != ["success"]:
                self.log_failure(f"Locked-key '{check}' post-upgrade: expected success, got {after.get(check)}")

    # ---------------- Tombstone purge ----------------

    @staticmethod
    def _tombstone_states(client, keys):
        """
        :return: {key: 'tombstone' | 'live' | 'purged' | <error name>}, from
                 a $document.deleted lookup that can read deleted docs
        """
        success, fail = client.sub_doc_read_multi(
            {key: [("$document.deleted", "")] for key in keys}, timeout=10, xattr=True, access_deleted=True
        )
        states = {}
        for key in keys:
            if key in success:
                states[key] = "tombstone" if success[key]["value"]["$document.deleted"] else "live"
                continue
            error = DocLoaderUtils.get_exception_name(fail[key]["error"])
            purged = SDKException.check_if_exception_exists(SDKException.DocumentNotFoundException, error)
            states[key] = "purged" if purged else error
        return states

    def set_bucket_purge_interval(self, bucket, purge_interval):
        rest = BucketRestApi(self.cluster.master)
        if bucket.bucketType == Bucket.Type.EPHEMERAL:
            # Ephemeral purgeInterval is a plain bucket setting
            status, content = rest.edit_bucket(bucket.name, {"purgeInterval": purge_interval})
        else:
            status, content = rest.set_auto_compaction(bucket.name, purge_interval=purge_interval)
        if not status:
            self.log_failure(f"Setting purgeInterval={purge_interval} on {bucket.name} failed: {content}")
        return status

    def bump_vbucket_high_seqnos(self, bucket):
        """
        Writes one doc into every vBucket. KV never purges the item holding
        a vBucket's highest seqno (it carries the vBucket's high seqno over a
        warmup), so without a later write the newest tombstone of each
        vBucket stays forever and the purge check would fail on that, not on
        purgeInterval handling. The vBucket count comes from the server's map:
        the bucket object's numVBuckets can differ for buckets created on an
        older release (eg: 7.6.x magma)
        """
        vb_map, _, _ = BucketHelper(self.cluster.master).get_vbucket_map_and_server_list(bucket.name)
        num_vbuckets = len(vb_map)
        keys = {}
        index = 0
        while len(keys) < num_vbuckets:
            key = f"seqno-bump-{index:06d}"
            keys.setdefault(self.bucket_util.get_vbucket_num_for_key(key, num_vbuckets), key)
            index += 1
        client = self._get_check_client(bucket)
        try:
            _, fail = client.upsert_multi({key: {"k": key} for key in keys.values()}, timeout=30)
        finally:
            self._release_check_client(client)
        if fail:
            self.log_failure(f"Seqno bump writes failed for {len(fail)} keys: {list(fail.items())[:5]}")
        self._count_check_docs(bucket, num_vbuckets - len(fail))
        self.log.info(f"Wrote one doc into each of {num_vbuckets} vBuckets of {bucket.name}")

    def create_ttl_tombstones(self, bucket, phase):
        """
        Loads TTL docs, waits for them to expire and reads them, which makes
        KV expire each one into a tombstone. Not bucket_util._expiry_pager():
        it changes ep_exp_pager_stime, which the engine-defaults check watches
        :return: (keys, time the tombstones were created)
        """
        scope, collection = self.kv_check_collection(bucket)
        key_prefix = f"ttl-{phase}"
        gen = doc_generator(key_prefix, 0, self.tombstone_docs)
        keys = []
        while gen.has_next():
            keys.append(gen.next()[0])

        task = self.task.async_load_gen_docs(
            self.cluster,
            bucket,
            doc_generator(key_prefix, 0, self.tombstone_docs),
            DocLoading.Bucket.DocOps.CREATE,
            exp=self.tombstone_ttl,
            batch_size=50,
            process_concurrency=1,
            timeout_secs=10,
            scope=scope,
            collection=collection,
        )
        self.task_manager.get_task_result(task)
        if task.fail:
            self.log_failure(f"TTL doc ({phase}) writes failed for {len(task.fail)} keys")

        self.sleep(self.tombstone_ttl + 5, "Wait for TTL docs to expire")
        client = self._get_check_client(bucket)
        try:
            client.get_multi(keys, timeout=30)
            states = Counter(self._tombstone_states(client, keys).values())
        finally:
            self._release_check_client(client)
        self.log.info(f"TTL docs ({phase}) after expiry: {dict(states)}")
        if states.get("tombstone", 0) != len(keys):
            self.log_failure(f"Expired TTL docs ({phase}) did not all become tombstones: {dict(states)}")
        return keys, time.time()

    def verify_tombstones_purged(self, bucket, keys, created_at, phase):
        """
        Once purgeInterval has elapsed since the tombstones were created,
        compaction (or the ephemeral purger) must remove every one of them.
        Call bump_vbucket_high_seqnos() after the last tombstone is created
        """
        wait = created_at + self.tombstone_purge_interval * 86400 - time.time()
        if wait > 0:
            self.sleep(wait + 60, f"Wait for purgeInterval to elapse for the {phase} tombstones")
        end_time = time.time() + self.tombstone_purge_timeout
        while True:
            if bucket.bucketType == Bucket.Type.MEMBASE:
                task = self.task.async_compact_bucket(self.cluster.master, bucket)
                self.task_manager.get_task_result(task)
                if not task.result:
                    self.log_failure(f"Compaction of {bucket.name} failed: {task.exception}")
            client = self._get_check_client(bucket)
            try:
                states = self._tombstone_states(client, keys)
            finally:
                self._release_check_client(client)
            counts = Counter(states.values())
            self.log.info(f"Tombstones ({phase}) after purge window: {dict(counts)}")
            if counts.get("purged", 0) == len(keys):
                return
            if time.time() > end_time:
                self.log_failure(
                    f"Tombstones ({phase}) not purged {self.tombstone_purge_timeout}s after purgeInterval "
                    f"elapsed: {dict(counts)}"
                )
                self.log_tombstone_placement(bucket, states)
                return
            self.sleep(60, "Retry tombstone purge check")

    def log_tombstone_placement(self, bucket, states):
        """
        Purged vs remaining tombstones per active node and vBucket, plus the
        purge age each node's engine uses, so a partial purge can be told
        apart: one node / some vBuckets (engine side) vs all keys (config)
        """
        vb_map, server_list, _ = BucketHelper(self.cluster.master).get_vbucket_map_and_server_list(bucket.name)
        for node in self.cluster_util.get_kv_nodes(self.cluster):
            cbstats = Cbstats(node)
            try:
                purge_age = cbstats.all_stats(bucket.name).get("ep_persistent_metadata_purge_age", "<absent>")
            except Exception as e:
                purge_age = DocLoaderUtils.get_exception_name(e)
            finally:
                cbstats.disconnect()
            self.log.critical(f"{node.ip}: ep_persistent_metadata_purge_age={purge_age}")
        per_node = {}
        remaining_vbs = set()
        for key, state in states.items():
            vb = self.bucket_util.get_vbucket_num_for_key(key, len(vb_map))
            per_node.setdefault(server_list[vb_map[vb][0]], Counter())[state] += 1
            if state != "purged":
                remaining_vbs.add(vb)
        self.log.critical(f"Tombstone states per active node: { {n: dict(c) for n, c in per_node.items()} }")
        self.log.critical(f"vBuckets still holding tombstones: {sorted(remaining_vbs)}")

    # ---------------- Pre / post upgrade sanity ----------------

    def kv_sanity_pre_upgrade(self, bucket):
        """
        Captures the pre-upgrade baselines. Tombstones are created here
        (on the source version) so the post-upgrade check proves tombstones
        that predate the upgrade still get purged (CBSE-22954)
        """
        state = {
            "engine": self.snapshot_engine_settings(bucket),
            "locked_key": self.locked_key_signature(bucket, "pre"),
            "locked_key_mixed": None,
            "tombstones": {},
        }
        if self.validate_tombstone_purge:
            self.set_bucket_purge_interval(bucket, self.tombstone_purge_interval)
            if "pre" in self.tombstone_phases:
                state["tombstones"]["pre"] = self.create_ttl_tombstones(bucket, "pre")
        return state

    def kv_sanity_post_upgrade(self, bucket, state):
        self.compare_engine_settings(state["engine"], self.snapshot_engine_settings(bucket))
        self.compare_locked_key_signatures(
            state["locked_key"], state["locked_key_mixed"], self.locked_key_signature(bucket, "post")
        )
        if self.validate_tombstone_purge:
            if "post" in self.tombstone_phases:
                state["tombstones"]["post"] = self.create_ttl_tombstones(bucket, "post")
            if state["tombstones"]:
                self.bump_vbucket_high_seqnos(bucket)
            for phase, (keys, created_at) in state["tombstones"].items():
                self.verify_tombstones_purged(bucket, keys, created_at, phase)
