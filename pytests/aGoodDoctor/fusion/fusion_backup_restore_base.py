"""
Base class for Fusion Backup/Restore tests.

Provides cluster provisioning, bucket management, document loading,
rebalance triggering, cloud snapshot backup, and restore helpers
for all Fusion backup/restore test classes.

All helpers use Capella v4 REST API (APIBase) and SiriusCouchbaseLoader.
"""
import json
import os
import threading
import time
import types
import uuid

import requests

from global_vars import logger
from TestInput import TestInputSingleton
from pytests.Capella.RestAPIv4.api_base import APIBase
from pytests.aGoodDoctor.fusion.fusion_aws_util import (
    FusionAWSUtil, resolve_fusion_aws_credentials)
from Jython_tasks.java_loader_tasks import SiriusCouchbaseLoader
from capellaAPI.capella.dedicated.CapellaAPI import CapellaAPI as CapellaAPIv2
from sirius_client_framework.sirius_setup import SiriusSetup


class FusionBackupRestoreBase(APIBase):
    """
    Reusable base for Fusion backup/restore test classes.

    Parameters (pass via .ini [capella] section or -p):
      aws_access_key     : AWS IAM access key (EC2/S3 describe permissions)
      aws_secret_key     : AWS IAM secret key
      aws_region         : AWS region (default: us-east-1)
      num_docs           : Documents per bucket (default: 100000)
      num_buckets        : Buckets to create (default: 1)
      bucket_id          : Reuse existing source bucket; skip creation + load
      source_cluster_id  : Reuse existing source cluster (skips provisioning)
      target_cluster_id  : Reuse existing target cluster (skips provisioning)
      source_num_nodes   : KV node count for source cluster (default: 3)
      target_num_nodes   : KV node count for target cluster (default: 3)
      preserve_clusters  : Keep clusters alive after test (default: True)
      reuse_clusters     : Reuse clusters across tests in one suite run via a
                           class-level pool (default: True). Only active when
                           preserve_clusters is also True; with
                           preserve_clusters False each test provisions and
                           destroys its own clusters (no pooling).
      deploy_timeout     : Cluster deploy poll timeout in seconds (default: 1800)
      backup_timeout     : Backup poll timeout in seconds (default: 3600)
      restore_timeout    : Restore poll timeout in seconds (default: 3600)
      rebalance_timeout  : Rebalance poll timeout in seconds (default: 7200)
    """

    CLUSTER_HEALTHY = "healthy"

    # Class-level cluster pool shared across test methods in a single suite
    # run. Maps (fusion_enabled: bool, num_nodes: int) -> list of entries:
    #   {"id": str, "project_id": str, "in_use": bool}
    # Populated lazily by acquire_cluster(); reused (not re-provisioned) by
    # later tests. Only used when pooling is active (reuse + preserve).
    _cluster_pool = {}

    # Class-level registry of EVERY cluster this suite ever provisioned, id ->
    # project_id. The reuse pool can evict a failed test's cluster (so it isn't
    # reused), which would otherwise drop it from end-of-matrix deletion; this
    # registry guarantees the matrix's last test can destroy every cluster it
    # created regardless of pool state, so a full run leaves nothing behind.
    _all_cluster_ids = {}

    def setUp(self):
        # The [capella] ini section is mapped to self.input.capella, not
        # test_params; _p() below reads conf/CLI first then falls back to ini.
        capella = TestInputSingleton.input.capella

        preset_project_id = (capella.get("project_id")
                             or capella.get("project"))
        if preset_project_id and not capella.get("project"):
            capella["project"] = preset_project_id

        # Skip APIBase's wrapper-cluster deploy by pre-filling the placeholder.
        if not capella.get("clusters"):
            capella["clusters"] = {
                "cluster_id": None, "vpc_id": None, "app_id": None}

        APIBase.setUp(self)

        # APIBase occasionally creates a fresh project despite the preset hint;
        # detect, delete, and override so the rest of the test uses the right id.
        if preset_project_id and self.project_id != preset_project_id:
            stray = self.project_id
            self.log.info(
                "APIBase created project {} — overriding with preset {}; "
                "deleting stray project".format(stray, preset_project_id))
            try:
                resp = self.capellaAPI.org_ops_apis.delete_project(
                    self.organisation_id, stray)
                if resp.status_code not in [200, 202, 204]:
                    self.log.warning(
                        "Could not delete stray project {}: {} {}".format(
                            stray, resp.status_code, resp.content))
            except Exception as exc:
                self.log.warning(
                    "Exception deleting stray project {}: {}".format(
                        stray, exc))
            self.project_id = preset_project_id
            self.capella["project"] = preset_project_id

        self.cluster_id = None
        self._preset_project_id = preset_project_id

        def _p(key, default=None):
            v = self.input.param(key, None)
            if v is None:
                v = capella.get(key, default)
            return v

        # DocLoader URL. In CI, testrunner.py --launch_java_doc_loader starts
        # the Java DocLoader and passes sirius_url; locally pass sirius_url
        # directly (optionally over an SSH tunnel). Falls back to the
        # SiriusSetup default when unset.
        sirius_url = _p("sirius_url")
        if sirius_url:
            SiriusSetup.sirius_url = sirius_url
            self.log.info("DocLoader URL set to {}".format(sirius_url))
        else:
            self.log.info(
                "No sirius_url provided — using default DocLoader URL: "
                "{}".format(SiriusSetup.sirius_url))

        self.aws_access_key = _p("aws_access_key")
        self.aws_secret_key = _p("aws_secret_key")
        # aws_use_iam_role=true: don't use static keys — build the boto3 clients
        # with NO explicit credentials so boto3 resolves them from its default
        # chain (env vars, shared config, or the Jenkins agent's IAM
        # instance-profile / assumed role). Requires the agent to have an IAM
        # role with EC2/S3 read perms in the CLUSTER's AWS account. This is how
        # the EC2/S3 (guest-volume / fusion-S3) checks run without static keys.
        self.aws_use_iam_role = str(
            _p("aws_use_iam_role", False)).lower() == "true"
        self.aws_region = _p("aws_region", "us-east-1")
        self.num_docs = int(_p("num_docs", 100000))
        # Docs to preload into the target before restore (the non-empty-target
        # check). Default ~20 GB (20M x 1 KB) — far cheaper than the 100 GB
        # source, but comfortably ABOVE the ~10 GB Fusion threshold (fusion_
        # threshold_gib) so a Fusion rebalance forms guest volumes on an ENABLED
        # target. Those pre-existing guest volumes are what the restore must tear
        # down — the real customer scenario — so the preload must exceed ~10 GB;
        # 1 GB would leave 0 guest volumes. A DISABLED target only needs to be
        # non-empty, so for disabled-target runs set target_preload_docs=1000000
        # (1 GB) to go faster. If an enabled target still shows 0 guest volumes,
        # raise this.
        self.target_preload_docs = int(_p("target_preload_docs", 20000000))
        # Document body size in bytes. Drives the on-disk dataset volume that
        # determines whether a rebalance uses Fusion acceleration (and thus
        # populates guest volumes) vs DCP. e.g. 5,242,880 docs x 10240 B ~= 50GB.
        # NOTE: the dataset only needs to exceed the Fusion threshold
        # (fusion_threshold_gib, ~10GB) — ~12-15GB is plenty and far less
        # OOM-prone to ingest than 50GB.
        self.doc_size = int(_p("doc_size", 1024))
        # Bucket RAM quota (MB per node). 1024 is too small to ingest tens of
        # GB without ServerOutOfMemory back-pressure; raise for large loads.
        self.bucket_ram_quota = int(_p("bucket_ram_quota", 1024))
        # DocLoader ingest tuning. Lower these to ease KV memory pressure on
        # large loads (trades throughput for stability).
        self.load_concurrency = int(_p("load_concurrency", 20))
        self.load_clients = int(_p("load_clients", 5))
        self.num_buckets = int(_p("num_buckets", 1))
        # backup_id reuses an existing snapshot (Capella retains 168h) so the
        # test can skip Step 5's POST when iterating on later steps.
        self.preset_backup_id = _p("backup_id")
        # source_bucket_id reuses an already-loaded source bucket (local fast
        # iteration) — skips create + 100 GB load, and is preserved in tearDown.
        # Only safe for tests that do NOT re-rebalance the source.
        self.preset_source_bucket_id = _p("source_bucket_id")
        # Optional dp-agent version hash to pin on every freshly provisioned
        # cluster (source/target/self) before any Fusion op runs — used to
        # validate a specific data-plane agent build (e.g. a fix candidate) or
        # to reproduce an agent-version-specific bug. None = leave the sandbox
        # default agent in place.
        self.dp_agent_hash = _p("dp_agent_hash", None)
        self.deploy_timeout = int(_p("deploy_timeout", 1800))      # 30 min
        self.backup_timeout = int(_p("backup_timeout", 3600))      # 1 hour
        self.restore_timeout = int(_p("restore_timeout", 3600))    # 1 hour
        # Scaling/rebalance must not exceed 30 min.
        self.rebalance_timeout = int(_p("rebalance_timeout", 1800))  # 30 min
        # Poll cadence for the resilient CP wait loops, and how many CONSECUTIVE
        # transient control-plane errors (wrapper sys.exit / 5xx / network) to
        # tolerate before declaring the CP unreachable. At 15s x 40 that's ~10
        # min of continuous failure — long enough to ride out a flaky-sandbox
        # blip, short enough to fail fast (with a clear message) on a real CP
        # outage instead of silently burning the whole convergence timeout.
        self._cp_poll_interval = int(_p("cp_poll_interval", 15))
        self._cp_error_limit = int(_p("cp_error_limit", 40))
        self.preserve_clusters = _p("preserve_clusters", True)
        self.reuse_clusters = _p("reuse_clusters", True)
        # Cluster pooling (reuse across tests) is only safe when clusters are
        # preserved between tests — otherwise per-test teardown would destroy a
        # cluster a later test wants to reuse. So gate pooling on both flags.
        self._pooling = bool(self.reuse_clusters) and bool(self.preserve_clusters)
        self._acquired_pool_entries = []
        # Set True at the end of each test. tearDown uses it to decide whether
        # to run destructive cleanup (bucket delete + scale-down) and return
        # clusters to the pool — on failure it preserves everything (incl. the
        # source bucket) for debugging and evicts the clusters from the pool.
        self._test_succeeded = False
        self.source_num_nodes = int(_p("source_num_nodes", 3))
        self.target_num_nodes = int(_p("target_num_nodes", 3))
        # Per-node hardware for provisioned clusters. Defaults match the
        # original small node (4 vCPU / 16 GB, 100 GB gp3 @ 3000 IOPS). Raise
        # these for large loads (e.g. 100 GB): kv_cpu/kv_ram must be a valid
        # Capella AWS combo (m5 = 1:4, c5 = 1:2, r5 = 1:8 vCPU:GB), and gp3
        # IOPS can go up to 16000. e.g. kv_cpu=16,kv_ram=64,kv_disk=500,
        # kv_iops=8000.
        self.kv_cpu = int(_p("kv_cpu", 4))
        self.kv_ram = int(_p("kv_ram", 16))
        self.kv_disk = int(_p("kv_disk", 100))
        self.kv_iops = int(_p("kv_iops", 3000))

        # source_cluster_id: conf param > ini source_cluster_id > ini cluster_id
        self.source_cluster_id = (
            self.input.param("source_cluster_id", None)
            or capella.get("source_cluster_id")
            or capella.get("cluster_id"))
        self.source_project_id = (
            self.input.param("source_project_id", None)
            or capella.get("source_project_id")
            or self.project_id)
        self.source_bucket_ids = []
        self.source_bucket_names = []

        self.target_cluster_id = (
            self.input.param("target_cluster_id", None)
            or capella.get("target_cluster_id"))
        self.target_project_id = (
            self.input.param("target_project_id", None)
            or capella.get("target_project_id")
            or self.project_id)

        self._db_users_to_cleanup = {}
        self._clusters_created = []
        # Clusters provisioned fresh for THIS test only (never pooled/reused):
        # every target, plus the source of a guest-volume test (its per-test
        # fusion rebalance mutates the source, so it can't be reused). Deleted
        # at the end of this test in tearDown so nothing accumulates.
        self._ephemeral_clusters = []
        self._target_bucket_ids = []
        self._source_original_nodes = None
        self._target_original_nodes = None
        # Guest-volume count on the source at backup time (folds the
        # count-mismatch / node-mapping validations into the matrix runner).
        self._source_guest_vol_count = None
        # Name of the target's own pre-load bucket (its non-empty-target data),
        # preserved across runs for cluster reuse.
        self._target_preload_bucket = None
        # Track the backup created by this test (if any) so teardown can delete
        # it. preset_backup_id is NOT deleted — it belongs to another run.
        self._last_backup_id = None

        # Feature flags: the test does not hardcode any. It only applies flags
        # explicitly passed by the pipeline via ff_to_update (no-op if unset).
        self._apply_feature_flags_from_param()

        # AWS access for the deep fusion checks (guest-volume presence/count,
        # S3 log-store residue, EBS snapshot tags/counts, memcached CRITICAL
        # scan). Resolve creds the same way the lifecycle suite does: explicit
        # aws_access_key/aws_secret_key if given, else assume the jenkins-cp-cli
        # role via STS (account_id from the [capella] ini + the cp-cli
        # external_id). An auto-refreshing boto3 session keeps the clients alive
        # after the assumed creds expire mid-test.
        self.fusion_aws_util = None
        self.aws_iam = None
        self.aws_session_token = None
        self.aws_boto3_session = None
        try:
            (self.aws_access_key, self.aws_secret_key,
             self.aws_session_token, self.aws_iam) = \
                resolve_fusion_aws_credentials(
                    self.input, region=self.aws_region)
            self.aws_boto3_session = (
                self.aws_iam.get_boto3_session(region=self.aws_region)
                if self.aws_iam else None)
            self.fusion_aws_util = FusionAWSUtil(
                self.aws_access_key, self.aws_secret_key,
                session_token=self.aws_session_token, region=self.aws_region,
                boto3_session=self.aws_boto3_session)
            self.log.info(
                "AWS access enabled for fusion deep checks (guest volumes, "
                "S3 residue, snapshots, memcached CRITICAL scan).")
        except Exception as e:
            # Only reached when no creds are resolvable (e.g. a local run with
            # no keys and no account_id). Log LOUDLY — this is an explicit, not
            # silent, skip of the EC2/S3/snapshot/memcached checks.
            self.log.warning(
                "AWS deep checks DISABLED — could not resolve fusion AWS "
                "credentials ({}). Guest-volume / S3-residue / snapshot / "
                "memcached-CRITICAL checks are SKIPPED; restore validated by "
                "DATA INTEGRITY only. Provide aws_access_key/aws_secret_key, or "
                "account_id in the [capella] ini (to assume jenkins-cp-cli), to "
                "enable them.".format(e))
        # AWS utils keyed by region (for cross-region restore, the target's
        # guest-volume / S3 checks must run in the target's region). Default
        # target util = source util so same-region tests are unaffected.
        self._region_aws_utils = {self.aws_region: self.fusion_aws_util}
        self._target_aws_util = self.fusion_aws_util
        self._target_region = self.aws_region

    def tearDown(self):
        failures = []

        # Bail early if setUp raised before initializing internal state.
        if not hasattr(self, "_clusters_created"):
            APIBase.tearDown(self)
            return

        # Debug flag: preserve EVERYTHING (clusters, buckets, backup, project)
        # so a dev can inspect a failed cluster to replicate the issue. Skips
        # all teardown deletion and the project/API-key cleanup.
        if self.input.param("keep_clusters", False):
            self.log.warning(
                "keep_clusters=True — preserving clusters/buckets/backup/"
                "project for debugging; nothing deleted. source_cluster={} "
                "source_project={} target_cluster={} target_project={} "
                "backup={}".format(
                    self.source_cluster_id, self.source_project_id,
                    self.target_cluster_id, self.target_project_id,
                    self._last_backup_id))
            return

        will_delete = (set(self._clusters_created)
                       if not self.preserve_clusters else set())

        passed = getattr(self, "_test_succeeded", False)

        # Always delete the backup this test created — on success AND failure —
        # so backups never leak across runs. A preset backup_id belongs to
        # another run and is left alone (handled inside the helper).
        self._delete_last_backup()

        # Delete THIS test's fresh cluster(s) now (pass or fail): every target,
        # plus a guest-volume source. They're never reused, so there's no reason
        # to keep them until end-of-matrix — deleting per-test keeps the live
        # cluster count minimal (only the one reused pooled source persists).
        self._delete_ephemeral_clusters()

        # End of the WHOLE matrix run: tear down ALL clusters (pass or fail).
        # testrunner sets case_number (1-based) and no_of_test_identified; when
        # they're equal this is the final test, so we delete every pooled/
        # created cluster instead of returning them to the pool.
        case_no = int(self.input.param("case_number", 0) or 0)
        total = int(self.input.param("no_of_test_identified", 0) or 0)
        if total and case_no >= total:
            self.log.info(
                "Last test of the matrix (case {}/{}) — tearing down ALL "
                "clusters".format(case_no, total))
            self._delete_all_pooled_clusters()
            if self._preset_project_id:
                self.project_id = None
            APIBase.tearDown(self)
            return

        # Delete any unhealthy source/target cluster so the next test deploys a
        # fresh one — there's no point running a test on a wedged cluster.
        # Healthy clusters are kept and reused. Runs on success AND failure.
        will_delete |= self._delete_unhealthy_clusters()

        if not passed:
            # Test failed/errored: keep buckets + the (healthy) clusters for
            # debugging and reuse — any unhealthy cluster was already deleted
            # above, so the remaining healthy ones are safe to reuse.
            self.log.warning(
                "tearDown: test did not succeed — the fresh target was already "
                "deleted above; preserving the reused source cluster {} and its "
                "buckets {} for reuse. (Use keep_clusters=True to preserve the "
                "target too for debugging.)".format(
                    self.source_cluster_id, self.source_bucket_ids))
            self._scale_pooled_clusters_to_baseline(will_delete)
            if self._pooling:
                self._release_pooled_clusters(reusable=True)
            if self._preset_project_id:
                self.project_id = None
            APIBase.tearDown(self)
            return

        # Buckets are kept for reuse; only the backup is always removed.
        self._scale_pooled_clusters_to_baseline(will_delete)

        for cluster_id, user_id in self._db_users_to_cleanup.items():
            # Best-effort: the CapellaAPI wrapper sys.exit()s on a persistent
            # API error, so a teardown 500 here must not crash the test (turning
            # a passed test into an ERROR).
            try:
                resp = self.capellaAPI.cluster_ops_apis.delete_database_user(
                    self.organisation_id, self.project_id, cluster_id, user_id)
                if getattr(resp, "status_code", 200) not in [
                        200, 202, 204, 404]:
                    failures.append(
                        "Could not delete DB user {} on cluster {}".format(
                            user_id, cluster_id))
            except BaseException as exc:
                self.log.warning(
                    "tearDown: delete DB user {} on {} raised {}".format(
                        user_id, cluster_id, exc))

        if not self.preserve_clusters:
            for cluster_id in self._clusters_created:
                resp = self.capellaAPI.cluster_ops_apis.delete_cluster(
                    self.organisation_id, self.project_id, cluster_id)
                if resp.status_code not in [202, 204]:
                    failures.append(
                        "Could not delete cluster {}".format(cluster_id))

        if failures:
            self.log.error("tearDown failures: {}".format(failures))

        # Return this test's acquired clusters to the pool (buckets already
        # cleaned and clusters scaled to baseline above) so the next test in
        # the run reuses them instead of re-provisioning.
        if self._pooling:
            self._release_pooled_clusters()

        # Preset project belongs to the user, not this test run; opt out
        # of APIBase's project deletion via the None-guard it now honors.
        if self._preset_project_id:
            self.project_id = None

        APIBase.tearDown(self)

    @classmethod
    def tearDownClass(cls):
        """Do NOT reset the in-memory pool here.

        TAF runs each conf test as its own suite, so this classmethod fires
        after EVERY test — wiping the pool here would destroy it between tests
        and defeat reuse. The pool must persist across the whole matrix run so
        later tests reuse earlier clusters. End-of-matrix teardown of all
        clusters is handled in tearDown() on the final test (case_number ==
        no_of_test_identified). We only log any leftover here as a safety net
        (e.g. if the run was aborted before the last test).
        """
        pooled = [e["id"]
                  for entries in cls._cluster_pool.values()
                  for e in entries]
        if pooled:
            logger["test"].info(
                "Pooled cluster(s) still alive (deleted on the matrix's last "
                "test, or manually if the run was aborted): {}".format(pooled))
        parent = super(FusionBackupRestoreBase, cls)
        if hasattr(parent, "tearDownClass"):
            parent.tearDownClass()

    def _delete_cluster_best_effort(self, cid, project_id):
        """Delete one cluster, tolerating the CapellaAPI wrapper's sys.exit() on
        a persistent API error (BaseException). Returns True on a delete that
        took (or a 404); on success, drops the cluster from the class registry
        and this test's created-list so end-of-matrix won't retry it."""
        try:
            resp = self.capellaAPI.cluster_ops_apis.delete_cluster(
                self.organisation_id, project_id or self.project_id, cid)
            if getattr(resp, "status_code", 202) in (200, 202, 204, 404):
                type(self)._all_cluster_ids.pop(cid, None)
                if cid in self._clusters_created:
                    self._clusters_created.remove(cid)
                return True
            self.log.warning(
                "delete cluster {} returned {} — will be swept at "
                "end-of-matrix".format(cid, resp.status_code))
        except BaseException as exc:
            self.log.warning(
                "delete cluster {} raised {} — will be swept at "
                "end-of-matrix".format(cid, exc))
        return False

    def _delete_ephemeral_clusters(self):
        """Delete the fresh (never-reused) cluster(s) this test created — every
        target, plus a guest-volume source. Runs every test (pass or fail) so
        they don't accumulate; anything that fails to delete is swept at
        end-of-matrix, so it's never leaked."""
        for cid, proj in self._ephemeral_clusters:
            if self._delete_cluster_best_effort(cid, proj):
                self.log.info(
                    "tearDown: deleted ephemeral cluster {}".format(cid))
            # Forget the id so the later unhealthy-sweep doesn't re-check this
            # now-deleting cluster and raise a false "LEAKED" alarm.
            if cid == self.source_cluster_id:
                self.source_cluster_id = None
            if cid == self.target_cluster_id:
                self.target_cluster_id = None
        self._ephemeral_clusters = []

    def _delete_pooled_sources(self, except_key=None):
        """Delete pooled SOURCE clusters so at most one source stays alive.

        Called when a fresh source is about to be stood up, or when a new source
        spec is provisioned (except_key = the spec we're keeping). Targets are
        never pooled, so this only ever touches sources."""
        for pkey in list(type(self)._cluster_pool.keys()):
            if except_key is not None and pkey == except_key:
                continue
            if self._is_target_prefix(pkey[2]):
                continue
            for entry in list(type(self)._cluster_pool[pkey]):
                if self._delete_cluster_best_effort(
                        entry["id"], entry.get("project_id")):
                    self.log.info(
                        "acquire_cluster: evicted stale pooled source {} to "
                        "keep one source alive".format(entry["id"]))
                type(self)._cluster_pool[pkey].remove(entry)
                if entry in self._acquired_pool_entries:
                    self._acquired_pool_entries.remove(entry)

    def _delete_all_pooled_clusters(self):
        """End of the whole matrix run: delete every cluster we pooled or
        created across the suite, then clear the in-memory pool. Runs on the
        last test regardless of pass/fail."""
        seen = set()
        targets = []
        for entries in type(self)._cluster_pool.values():
            for e in entries:
                if e["id"] not in seen:
                    seen.add(e["id"])
                    targets.append((e["id"], e.get("project_id")))
        for cid in self._clusters_created:
            if cid not in seen:
                seen.add(cid)
                targets.append((cid, self.project_id))
        # Sweep the full registry too: a fresh target from an earlier test that
        # failed was evicted from the reuse pool and won't be in this last
        # test's _clusters_created, so without this it would survive the run.
        for cid, proj in type(self)._all_cluster_ids.items():
            if cid not in seen:
                seen.add(cid)
                targets.append((cid, proj))
        leaked = []
        for cid, proj in targets:
            try:
                resp = self.capellaAPI.cluster_ops_apis.delete_cluster(
                    self.organisation_id, proj or self.project_id, cid)
                if getattr(resp, "status_code", 202) in (200, 202, 204, 404):
                    self.log.info(
                        "End-of-matrix: deleted cluster {}".format(cid))
                else:
                    leaked.append(cid)
                    self.log.warning(
                        "End-of-matrix: delete cluster {} returned {}: "
                        "{}".format(cid, resp.status_code, resp.content))
            except BaseException as exc:
                leaked.append(cid)
                self.log.warning(
                    "End-of-matrix: could not delete cluster {}: {}".format(
                        cid, exc))
        if leaked:
            self.log.critical(
                "LEAKED clusters — delete failed, NOT lost (recorded in the "
                "ledger). Remove via cleanup_fusion_clusters.py --delete or the "
                "Capella UI: {}".format(leaked))
        type(self)._cluster_pool = {}
        type(self)._all_cluster_ids = {}
        self._acquired_pool_entries = []

    def _scale_pooled_clusters_to_baseline(self, will_delete):
        """Scale source/target back to their provisioned node count so a reused
        cluster starts at baseline. Runs on success AND failure. Skips deleted
        or unhealthy clusters and no-ops when already at baseline."""
        for cl_id, proj_id, orig_nodes, label in (
                (self.source_cluster_id,
                 self.source_project_id or self.project_id,
                 self._source_original_nodes, "source"),
                (self.target_cluster_id,
                 self.target_project_id or self.project_id,
                 self._target_original_nodes, "target")):
            if not cl_id or not orig_nodes or cl_id in will_delete:
                continue
            try:
                state = None
                try:
                    info = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
                        self.organisation_id, proj_id, cl_id)
                    if info.status_code == 200:
                        state = (info.json().get("currentState") or "").lower()
                except BaseException:
                    pass
                if state != self.CLUSTER_HEALTHY:
                    self.log.warning(
                        "tearDown: {} cluster {} not healthy (state={!r}) — "
                        "skipping scale-down".format(label, cl_id, state))
                    continue
                current_nodes = self.get_cluster_node_count(cl_id, proj_id)
                if current_nodes is not None and current_nodes <= orig_nodes:
                    continue
                self.log.info(
                    "tearDown: scaling {} cluster {} back to {} nodes "
                    "(from {})".format(
                        label, cl_id, orig_nodes, current_nodes))
                self.trigger_fusion_rebalance(
                    cl_id, project_id=proj_id, target_nodes=orig_nodes)
                if not self.wait_for_rebalance_complete(
                        cl_id, project_id=proj_id):
                    self.log.warning(
                        "tearDown: scale-down of {} cluster {} did not "
                        "complete cleanly".format(label, cl_id))
            except BaseException as exc:
                self.log.warning(
                    "tearDown: could not scale down {} cluster {}: {}".format(
                        label, cl_id, exc))

    @staticmethod
    def _next_cidr(cidr):
        """Return the next /20 CIDR by bumping the second octet, e.g.
        '10.0.0.0/20' -> '10.1.0.0/20'. Wraps within 1..255."""
        base = cidr.split("/")[0]
        octets = base.split(".")
        octets[1] = str((int(octets[1]) + 1) % 256)
        octets[2] = "0"
        octets[3] = "0"
        return ".".join(octets) + "/20"

    def _alternate_region(self, region):
        """Pick a different AWS region for the cross-region target, derived
        from the chosen source region so the user only sets one region."""
        if region == "us-east-1":
            return "us-west-2"
        if region == "us-west-2":
            return "us-east-1"
        return "us-east-1"

    def _record_cluster_id(self, cid, project_id, region):
        """Append every provisioned cluster id to a persistent ledger file so no
        id is ever lost — even if the process dies or a later delete fails, the
        ledger (and cleanup_fusion_clusters.py) can reap it. Path is overridable
        via the cluster_ledger param."""
        path = self.input.param(
            "cluster_ledger", "/tmp/taf_fusion_cluster_ledger.txt")
        try:
            with open(path, "a") as f:
                f.write("{},{},{}\n".format(
                    cid, project_id or self.project_id, region))
        except Exception as e:
            self.log.warning(
                "Could not write cluster ledger {}: {}".format(path, e))

    def _validate_aws_access(self):
        """Fail fast if AWS isn't actually reachable, so the EC2/S3 checks RUN
        (no silent skip). Used with aws_use_iam_role=true (no static keys): a
        minimal EC2 describe confirms boto3 resolved a working role from the
        environment (agent instance-profile / assumed role) with EC2 access to
        the cluster's account. If it can't, the run fails here with a clear
        message rather than passing while skipping the guest-volume / S3 libs."""
        try:
            self.fusion_aws_util.ec2.ec2_client.describe_availability_zones()
            self.log.info(
                "AWS access via IAM role confirmed (EC2 describe succeeded) — "
                "guest-volume / S3 checks will run.")
        except BaseException as exc:
            self.fail(
                "aws_use_iam_role=true but AWS could not be reached "
                "(EC2 describe failed: {}). No static keys and no usable IAM "
                "role in the environment — ensure the Jenkins agent has an IAM "
                "role (instance-profile / assumed) with EC2+S3 access to the "
                "cluster's AWS account. Refusing to run with the EC2/S3 checks "
                "silently disabled.".format(exc))

    def _aws_util_for_region(self, region):
        """Return a FusionAWSUtil bound to `region` (cached). None if AWS access
        isn't configured. Used so the target's guest-volume/S3 checks run in the
        target's region during a cross-region restore. Reuses the same resolved
        creds (explicit keys or assumed-role) with a region-specific
        auto-refreshing session."""
        if self.fusion_aws_util is None:
            return None
        if region not in self._region_aws_utils:
            boto3_session = (
                self.aws_iam.get_boto3_session(region=region)
                if self.aws_iam else None)
            self._region_aws_utils[region] = FusionAWSUtil(
                self.aws_access_key, self.aws_secret_key,
                session_token=self.aws_session_token, region=region,
                boto3_session=boto3_session)
        return self._region_aws_utils[region]

    def _scan_memcached_logs_after_restore(self, cluster_id, aws_util):
        """Scan a restored cluster's memcached logs (and crash dir) for
        CRITICAL errors / core dumps and FAIL the test if any are found.

        Uses AWS SSM into the nodes, so it needs AWS access; when
        fusion_aws_util is unset the scan is skipped with a loud warning (same
        policy as the other EC2/S3 checks — no silent skip)."""
        if aws_util is None:
            self.log.warning(
                "Post-restore memcached log scan SKIPPED for cluster {} — no "
                "AWS access (fusion_aws_util unset); CRITICAL errors in "
                "memcached logs are NOT checked.".format(cluster_id))
            return
        self.log.info(
            "=== Post-restore: scanning memcached logs for CRITICAL errors on "
            "cluster {} ===".format(cluster_id))
        if aws_util.scan_logs_for_errors_on_cluster_instances(cluster_id):
            self.fail(
                "CRITICAL errors or core dumps found in memcached logs on "
                "restored cluster {} — see the CRITICAL lines above.".format(
                    cluster_id))
        self.log.info(
            "No CRITICAL memcached errors on cluster {} after restore".format(
                cluster_id))

    def provision_fusion_cluster(self, fusion_enabled=True, num_nodes=3,
                                 name_prefix="TAF_Fusion", region=None):
        """Deploy a Capella Dedicated cluster (data-service only) and return its
        id. region defaults to self.aws_region; pass a different region for the
        cross-region restore target."""
        region = region or self.aws_region
        cluster_name = "{}_{}".format(
            name_prefix,
            uuid.uuid4().hex[:5])

        self.log.info(
            "Provisioning cluster '{}' ({} nodes)".format(
                cluster_name, num_nodes))

        # Dev builds (e.g. 8.1) can't deploy via the public v4 API (released
        # versions only). When the pipeline provides a custom image, deploy via
        # the internal customAMI endpoint; else fall back to the v4 version path.
        image = (self.input.capella.get("image", None)
                 or self.input.capella.get("cb_image", None))
        if image:
            server_ver = (self.input.param("server_version", None)
                          or self.input.capella.get("server_version", None)
                          or os.environ.get("cbs_version"))
            compute_type = self.input.param("kv_compute", "c7g.4xlarge")
            config = {
                "cidr": "10.0.0.0/20",
                "name": cluster_name,
                "description": "",
                "overRide": {
                    "token": self._internal_support_token(),
                    "image": image,
                    "server": server_ver,
                },
                "projectId": self.project_id,
                "provider": "hostedAWS",
                "region": region,
                "singleAZ": self.input.param("singleAZ", False),
                "server": None,
                "specs": [{
                    "count": num_nodes,
                    "services": [{"type": "kv"}],
                    "compute": {"type": compute_type, "cpu": 0,
                                "memoryInGb": 0},
                    "disk": {"type": "gp3", "sizeInGb": self.kv_disk,
                             "iops": self.kv_iops},
                    "diskAutoScaling": {
                        "enabled": self.input.param("diskAutoScaling", True)},
                }],
                "package": "enterprise",
            }
            self.log.info(
                "Deploy via customAMI: image={} server={} token_present={} "
                "compute={}".format(
                    image, server_ver,
                    bool(config["overRide"]["token"]), compute_type))
            resp = self.capellaAPI.create_cluster_customAMI(
                self.organisation_id, config)
            cidr_attempts = 0
            while (resp.status_code == 422 and cidr_attempts < 40
                   and "cidr" in (resp.content or b"").decode(
                       "utf-8", "ignore").lower()):
                cidr_attempts += 1
                config["cidr"] = self._next_cidr(config["cidr"])
                self.log.info(
                    "CIDR overlap — retrying with {}".format(config["cidr"]))
                resp = self.capellaAPI.create_cluster_customAMI(
                    self.organisation_id, config)
        else:
            cloud_provider = {
                "type": "aws",
                "region": region,
                "cidr": "10.0.0.0/20"
            }
            couchbase_server = {
                "version": str(self.input.param("server_version", "8.1"))
            }
            service_groups = [
                {
                    "node": {
                        "compute": {"cpu": self.kv_cpu, "ram": self.kv_ram},
                        "disk": {"storage": self.kv_disk, "type": "gp3",
                                 "iops": self.kv_iops}
                    },
                    "numOfNodes": num_nodes,
                    "services": ["data"]
                }
            ]
            availability = {"type": "multi"}
            support = {"plan": "enterprise", "timezone": "GMT"}

            # select_CIDR retries only on one specific 422 wording; some tenants
            # return "... overlaps with existing resource with CIDR ..."
            # instead, which it doesn't recognize. Walk the CIDR ourselves on
            # any CIDR-related 422 until one is free.
            resp = self.select_CIDR(
                self.organisation_id, self.project_id, cluster_name,
                cloud_provider, service_groups, availability, support,
                couchbase_server)
            cidr_attempts = 0
            while (resp.status_code == 422 and cidr_attempts < 40
                   and "cidr" in (resp.content or b"").decode(
                       "utf-8", "ignore").lower()):
                cidr_attempts += 1
                cloud_provider["cidr"] = self._next_cidr(
                    cloud_provider["cidr"])
                self.log.info(
                    "CIDR overlap — retrying with {}".format(
                        cloud_provider["cidr"]))
                resp = self.select_CIDR(
                    self.organisation_id, self.project_id, cluster_name,
                    cloud_provider, service_groups, availability, support,
                    couchbase_server)
        if resp.status_code != 202:
            self.fail(
                "Cluster deployment failed: {} {}".format(
                    resp.status_code, resp.content))

        cluster_id = resp.json()["id"]
        self._clusters_created.append(cluster_id)
        type(self)._all_cluster_ids[cluster_id] = self.project_id
        self._record_cluster_id(cluster_id, self.project_id, region)
        self.log.info(
            "Cluster {} created, waiting for healthy state".format(cluster_id))

        if not self.wait_for_deployment(self.project_id, cluster_id):
            self.fail(
                "Cluster {} did not reach healthy state within {}s".format(
                    cluster_id, self.deploy_timeout))

        # Pin the requested dp-agent build BEFORE any Fusion op runs, so the
        # fusion enable, backup, restore and rebalance all execute under it.
        self._pin_dp_agent_hash(cluster_id, self.project_id)

        # A cluster is Fusion-enabled only if express-scaling enable is called
        # on it; a disabled cluster simply never gets that call — there is
        # nothing to "disable". Source of truth is the cp-db dataplane.clusters
        # doc (config.enableFusion / fusionState), observable via the internal
        # fusion/status endpoint — NOT the v4 `expressScaling` field (which can
        # read 'enabled' as a capability even when Fusion is off).
        if fusion_enabled:
            self._enable_fusion_and_wait(cluster_id, self.project_id)

        self.log.info("Cluster {} ready (fusion_enabled={})".format(
            cluster_id, fusion_enabled))
        return cluster_id

    def _pin_dp_agent_hash(self, cluster_id, project_id):
        """Activate self.dp_agent_hash as the dp-agent build on every node of
        the cluster, then wait for the rollout to converge. No-op when
        dp_agent_hash is unset.

        Uses the internal support endpoint
        POST /internal/support/clusters/{id}/agent-versions/activate with
        {"hash": <hash>}. Fails the test if activation is rejected — running the
        scenario under the wrong agent build would make the result meaningless.
        Convergence is best-effort verified via the agent-versions GET; if that
        endpoint is unavailable we fall back to a fixed settle + health wait.
        """
        if not self.dp_agent_hash:
            return
        v2 = self._v2_api()
        self.log.info("Pinning dp-agent hash {} on cluster {}".format(
            self.dp_agent_hash, cluster_id))
        resp = v2.upgrade_dp_agent(cluster_id, self.dp_agent_hash)
        if resp.status_code not in [200, 201, 202]:
            self.fail(
                "dp-agent activate ({}) failed on cluster {}: {} {}".format(
                    self.dp_agent_hash, cluster_id,
                    resp.status_code, resp.content))

        # Poll the agent-versions status until every node reports the desired
        # hash, or fall back to a fixed settle if the GET endpoint is absent.
        deadline = time.time() + self.rebalance_timeout
        status_url = "{}/internal/support/clusters/{}/agent-versions".format(
            v2.internal_url, cluster_id)
        verified = False
        get_supported = True
        while time.time() < deadline:
            if not get_supported:
                break
            try:
                s = v2._urllib_request(
                    status_url, method="GET",
                    headers=v2.cbc_api_request_headers)
            except Exception as e:
                self.log.warning(
                    "dp-agent status GET errored ({}); will settle-wait".format(
                        e))
                get_supported = False
                break
            if s.status_code == 404:
                get_supported = False
                break
            if s.status_code != 200:
                time.sleep(15)
                continue
            try:
                body = json.loads(s.content)
            except Exception:
                time.sleep(15)
                continue
            desired = body.get("desiredHashes", {}).get("dp-agent")
            nodes = body.get("nodes", []) or []
            if (desired == self.dp_agent_hash and nodes and all(
                    n.get("agentHashes", {}).get("dp-agent")
                    == self.dp_agent_hash for n in nodes)):
                verified = True
                break
            time.sleep(15)

        if verified:
            self.log.info(
                "dp-agent hash {} active on all nodes of cluster {}".format(
                    self.dp_agent_hash, cluster_id))
        else:
            # Endpoint not available (or didn't converge in time) — give the
            # rollout a fixed window and re-confirm cluster health.
            self.log.warning(
                "Could not verify dp-agent rollout via API for cluster {}; "
                "settling for 180s then re-checking health".format(cluster_id))
            time.sleep(180)
        self._wait_for_cluster_healthy(
            cluster_id, project_id, timeout=self.rebalance_timeout)

    @staticmethod
    def _is_target_prefix(name_prefix):
        """A cluster acquired as a restore target (name_prefix carries 'Tgt').
        Targets are provisioned fresh per test (never reused) because a restore
        leaves fusion state on them that breaks a later restore."""
        return "Tgt" in (name_prefix or "")

    def acquire_cluster(self, fusion_enabled, num_nodes, name_prefix,
                        region=None, fresh=False):
        """Return (cluster_id, project_id) for a cluster matching
        (fusion_enabled, num_nodes).

        fresh=True (or a target) provisions a brand-new cluster for THIS test
        that is deleted in tearDown and never reused — used for targets (a
        restore wedges them) and for guest-volume sources (their per-test fusion
        rebalance mutates the source, so a reused GV source drifts and later
        restores fail to match fusion nodes).

        Otherwise, when pooling is active, reuse a free cluster of the same spec
        from the class-level pool (provisioning one only if none is free) and
        release it back in tearDown so later tests reuse it. To cap the run at
        ~2 live clusters (one source + one target), only a single source spec is
        kept alive: acquiring a fresh source or a differently-specced pooled
        source first deletes any other pooled source.

        When pooling is inactive, provision a fresh cluster (old behavior); the
        per-test tearDown then destroys it if preserve_clusters is False.
        """
        fusion_enabled = bool(fusion_enabled)
        num_nodes = int(num_nodes)
        region = region or self.aws_region
        is_target = self._is_target_prefix(name_prefix)

        if not self._pooling:
            cid = self.provision_fusion_cluster(
                fusion_enabled=fusion_enabled, num_nodes=num_nodes,
                name_prefix=name_prefix, region=region)
            return cid, self.project_id

        # Targets are restored INTO: a restore leaves fusion node records and
        # guest-volume/log-store state that wedges a subsequent restore (e.g.
        # "failed to match fusion node", stale S3 residue) — so a target is
        # never reused. Guest-volume sources are mutated by their per-test
        # fusion rebalance and drift the same way, so they're not reused either
        # (fresh=True). Both are provisioned fresh and deleted at the end of
        # THIS test (see _delete_ephemeral_clusters), so nothing piles up.
        if is_target or fresh:
            # Keep at most one source alive: before standing up a fresh source,
            # drop any pooled source left over from an earlier (non-GV) group.
            if fresh and not is_target:
                self._delete_pooled_sources()
            cid = self.provision_fusion_cluster(
                fusion_enabled=fusion_enabled, num_nodes=num_nodes,
                name_prefix=name_prefix, region=region)
            self._ephemeral_clusters.append((cid, self.project_id))
            return cid, self.project_id

        # Key the pool by ROLE (name_prefix) and REGION as well as
        # (fusion_enabled, num_nodes). Reuse happens freely within a source
        # spec; a new source spec evicts the previous pooled source so only one
        # source is ever alive.
        key = (fusion_enabled, num_nodes, name_prefix, region)
        pool = type(self)._cluster_pool.setdefault(key, [])

        for entry in pool:
            if entry["in_use"]:
                continue
            if not self.wait_for_deployment(
                    entry["project_id"], entry["id"]):
                self.log.warning(
                    "acquire_cluster: pooled cluster {} not healthy — "
                    "skipping".format(entry["id"]))
                continue
            # Re-assert Fusion-enabled on reuse: a prior test may have left an
            # ON cluster disabled (e.g. the disabled->enabled convergence).
            # express-scaling enable is idempotent — cheap when already on,
            # self-heals a drifted cluster otherwise. A disabled cluster needs
            # nothing (it's disabled precisely because we don't enable it).
            if fusion_enabled:
                self._enable_fusion_and_wait(
                    entry["id"], entry["project_id"])
            else:
                # A pooled cluster keyed 'disabled' may have drifted to enabled
                # (e.g. it served as a target that a restore turned on). Force it
                # back to Fusion-disabled before reusing it as a disabled cluster.
                self._ensure_fusion_disabled(
                    entry["id"], entry["project_id"], label="pooled cluster")
            entry["in_use"] = True
            self._acquired_pool_entries.append(entry)
            self.log.info(
                "acquire_cluster: reusing pooled cluster {} "
                "(fusion_enabled={}, nodes={})".format(
                    entry["id"], fusion_enabled, num_nodes))
            return entry["id"], entry["project_id"]

        # No free cluster of this spec — provision and register a new one.
        # First evict any pooled source of a DIFFERENT spec so we keep only one
        # source alive (e.g. moving from the disabled-source group to the
        # enabled-no-guest-volume group deletes the disabled source).
        self._delete_pooled_sources(except_key=key)
        self.log.info(
            "acquire_cluster: no free pooled cluster for "
            "(fusion_enabled={}, nodes={}); provisioning new".format(
                fusion_enabled, num_nodes))
        cid = self.provision_fusion_cluster(
            fusion_enabled=fusion_enabled, num_nodes=num_nodes,
            name_prefix=name_prefix, region=region)
        entry = {"id": cid, "project_id": self.project_id, "in_use": True}
        pool.append(entry)
        self._acquired_pool_entries.append(entry)
        return cid, self.project_id

    def _release_pooled_clusters(self, reusable=True):
        """Return this test's acquired pool clusters to the pool.

        reusable=True (test passed): mark them free so the next test reuses
        them. reusable=False (test failed): evict them from the pool so a later
        test won't reuse a possibly-wedged cluster (the cluster itself is still
        preserved on Capella for debugging — just not reused). Never destroys.
        """
        for entry in self._acquired_pool_entries:
            if reusable:
                entry["in_use"] = False
                self.log.info(
                    "tearDown: released pooled cluster {} back to the "
                    "pool".format(entry["id"]))
            else:
                for entries in type(self)._cluster_pool.values():
                    if entry in entries:
                        entries.remove(entry)
                self.log.info(
                    "tearDown: evicted pooled cluster {} from the pool (test "
                    "failed — not reusing; cluster preserved)".format(
                        entry["id"]))
        self._acquired_pool_entries = []

    def _internal_support_token(self):
        """Internal-support token: ini/param override_token, else the per-pod
        env token (sbx/dev/stage) the pipeline injects. Needed for internal /v2
        endpoints (custom-image deploy, express-scaling)."""
        tok = self.input.capella.get("override_token", None) \
            or self.input.param("override_token", None)
        if tok:
            return tok
        url = self.url or ""
        if "qe-" in url or "sbx-" in url:
            return os.environ.get("sbx_token_for_internal_support")
        if "dev" in url:
            return os.environ.get("dev_token_for_internal_support")
        if "stage" in url:
            return os.environ.get("stage_token_for_internal_support")
        return None

    def _v2_api(self):
        """Return a v2 CapellaAPI instance for internal endpoints (feature flags etc.)."""
        return CapellaAPIv2(
            "https://" + self.url, "", "",
            self.user, self.passwd,
            self._internal_support_token() or "")

    def _apply_tenant_feature_flag(self, v2, ff, value):
        """Create-or-update a single tenant feature flag to `value`."""
        resp = v2.create_tenant_feature_flag(
            self.organisation_id, ff, {"value": value})
        if resp.status_code in [200, 201, 204]:
            self.log.info("Feature flag {} set to {}".format(ff, value))
            return
        try:
            err_type = json.loads(resp.content).get("errorType", "")
        except Exception:
            err_type = ""
        if err_type == "FeatureFlagAlreadyExists":
            resp = v2.update_tenant_feature_flag(
                self.organisation_id, ff, {"value": value})
        if resp.status_code not in [200, 201, 204]:
            self.log.warning("Feature flag {}={} returned {}: {}".format(
                ff, value, resp.status_code, resp.content))

    def _apply_feature_flags_from_param(self):
        """Apply tenant feature flags passed by the pipeline via ``ff_to_update``
        (a.k.a. ``feature_flags``), e.g.
        ff_to_update=fusion-rebalances=true;fusion-fallback-replace=true;enable-eight-one-zero=true
        Pass it as ONE token (';' or ',' separators, '=' or ':' delimiters). No
        flag is set unless passed — the test hardcodes none."""
        raw = (self.input.param("ff_to_update", None)
               or self.input.param("feature_flags", None))
        if not raw:
            return
        v2 = self._v2_api()
        for token in str(raw).replace(";", ",").split(","):
            token = token.strip()
            if not token:
                continue
            if "=" in token:
                name, val = token.split("=", 1)
            elif ":" in token:
                name, val = token.split(":", 1)
            else:
                name, val = token, "true"
            value = str(val).strip().lower() in ("true", "1", "yes", "on")
            self._apply_tenant_feature_flag(v2, name.strip(), value)

    def enable_fusion_on_cluster(self, cluster_id, project_id):
        """Enable Fusion on the cluster via express-scaling enable — this is the
        one and only switch that makes a cluster Fusion-enabled.

        Fusion-readiness is signalled by the cluster reaching `healthy`
        state after this POST — callers should follow up with
        `_wait_for_cluster_healthy` (provision_fusion_cluster already does).
        """
        url = (
            "{}/v2/organizations/{}/projects/{}/clusters/{}"
            "/express-scaling/enable".format(
                self.capellaAPI.internal_url,
                self.organisation_id, project_id, cluster_id))
        resp = self.capellaAPI.do_internal_request(
            url, method="POST", params=json.dumps({}))
        if resp.status_code not in [200, 202]:
            self.log.warning(
                "express-scaling/enable returned {}: {}".format(
                    resp.status_code, resp.content))
        else:
            self.log.info(
                "Fusion enable triggered on cluster {}".format(cluster_id))

    def _enable_fusion_and_wait(self, cluster_id, project_id):
        """Enable Fusion and wait for the resulting rebalance to finish. enable
        is async — the cluster stays 'healthy' briefly before entering
        'rebalancing', so we wait for the transition to START (bounded) then for
        it to complete. Otherwise callers hit the cluster mid-rebalance (422
        'Temporarily unavailable while the Cluster is in the Rebalancing
        state')."""
        self.enable_fusion_on_cluster(cluster_id, project_id)
        deadline = time.time() + 180
        while time.time() < deadline:
            try:
                info = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
                    self.organisation_id, project_id, cluster_id)
                if info.status_code == 200:
                    st = (info.json().get("currentState") or "").lower()
                    if st and st != self.CLUSTER_HEALTHY:
                        break
            except Exception:
                pass
            time.sleep(10)
        self._wait_for_cluster_healthy(
            cluster_id, project_id, timeout=self.rebalance_timeout)

    def disable_fusion_on_cluster(self, cluster_id, project_id):
        """Disable Fusion on the cluster via express-scaling disable.

        Fusion infrastructure teardown (guest volume removal, S3 log-store
        bucket deletion) is triggered by this POST. The cluster transitions
        through 'disabling' → 'disabled'; callers should poll
        get_fusion_state() or assert_fusion_free_after_restore() for
        convergence.
        """
        url = (
            "{}/v2/organizations/{}/projects/{}/clusters/{}"
            "/express-scaling/disable".format(
                self.capellaAPI.internal_url,
                self.organisation_id, project_id, cluster_id))
        resp = self.capellaAPI.do_internal_request(
            url, method="POST", params=json.dumps({}))
        if resp.status_code not in [200, 202]:
            self.log.warning(
                "express-scaling/disable returned {}: {}".format(
                    resp.status_code, resp.content))
        else:
            self.log.info(
                "Fusion disable triggered on cluster {}".format(cluster_id))

    def _ensure_fusion_disabled(self, cluster_id, project_id,
                                label="cluster"):
        """Ensure a reused cluster is Fusion-disabled before a test that
        requires it. If the cluster is currently fusion-enabled, explicitly
        disable it and wait for convergence (guest volumes removed, S3
        bucket deleted). If already disabled/None, this is a no-op.

        This is for the cluster-reuse path — fresh deployments are provisioned
        with the correct fusion state by acquire_cluster.
        """
        state = self.get_fusion_state(cluster_id)
        if state in ("disabled", None):
            self.log.info(
                "{} {} already fusion-disabled (state={!r}) — no action "
                "needed".format(label, cluster_id, state))
            return
        self.log.info(
            "{} {} is fusion state={!r} — explicitly disabling".format(
                label, cluster_id, state))
        self.disable_fusion_on_cluster(cluster_id, project_id)
        self.assert_fusion_free_after_restore(
            cluster_id, project_id=project_id,
            timeout=int(self.input.param("fusion_free_timeout", 1800)))
        self.log.info(
            "{} {} now fusion-free after explicit disable".format(
                label, cluster_id))

    def get_fusion_state(self, cluster_id):
        """Return the runtime Fusion lifecycle state (fusionState) from the
        internal fusion/status endpoint — the observable proxy for the cp-db
        dataplane.clusters config.enableFusion flag (true only when Fusion is
        enabled). Returns the state string (e.g. 'enabled', 'enabling',
        'disabled') or None when the endpoint reports nothing, as for a cluster
        on which express-scaling enable was never called.
        """
        url = "{}/internal/support/clusters/{}/fusion/status".format(
            self.capellaAPI.internal_url, cluster_id)
        # Issue a single bounded request with a freshly-minted JWT. We avoid
        # the SDK's do_internal_request because it recurses on every 401 to
        # re-auth, and if the internal session token has aged out it recurses
        # ~1000 deep (~45 min) before raising RecursionError. Forcing jwt=None
        # makes get_authorization_internal POST /sessions for a fresh token.
        try:
            self.capellaAPI.jwt = None
            headers = self.capellaAPI.get_authorization_internal()
            resp = requests.get(url, headers=headers, verify=False, timeout=30)
        except Exception as exc:
            self.log.warning(
                "fusion/status read failed for {}: {}".format(cluster_id, exc))
            return None
        if resp.status_code != 200:
            self.log.info(
                "fusion/status returned {} for {} — state unknown".format(
                    resp.status_code, cluster_id))
            return None
        try:
            body = resp.json()
        except Exception:
            return None
        return (body.get("state") or body.get("status") or "").lower() or None

    def _wait_for_fusion_disabled(self, cluster_id, project_id=None,
                                  timeout=None):
        """Wait until Fusion is fully disabled after a disable_fusion_on_cluster
        call, before any enable_fusion_on_cluster on the same cluster.

        Confirms 'disabled' via an explicit fusion/status state OR, when that
        endpoint is indefinite, via the authoritative AWS signals (guest volumes
        removed AND Fusion S3 bucket empty/absent). A 401/None from fusion/status
        is NEVER treated as 'disabled' — enabling on a still-disabling cluster
        deadlocks the fusion state machine.
        """
        # Disable teardown (guest volumes + S3) legitimately runs longer than a
        # rebalance, so give it its own (longer) timeout.
        timeout = timeout or int(
            self.input.param("fusion_disable_timeout", 3600))
        project_id = project_id or self.project_id
        deadline = time.time() + timeout
        while time.time() < deadline:
            state = self.get_fusion_state(cluster_id)
            if state == "disabled":
                self.log.info(
                    "Fusion disable complete on {} (state='disabled')".format(
                        cluster_id))
                return
            # fusion/status is auth-flaky — it intermittently returns 401, which
            # get_fusion_state collapses to None. A 401/None must NOT be read as
            # 'disabled': enabling on a still-disabling cluster deadlocks the
            # fusion state machine (disablefusion: "fusion is enabled, cannot
            # disable" vs enablefusion: "still disabling, cannot enable"). When
            # the status is indefinite, confirm via authoritative AWS signals.
            if state is None and self.fusion_aws_util:
                try:
                    guest = self.fusion_aws_util.get_guest_volumes_for_cluster(
                        cluster_id)
                    guest_total = sum(len(v) for v in guest.values())
                except NotImplementedError:
                    guest_total = 0
                s3 = self.fusion_aws_util.find_fusion_s3_bucket(cluster_id)
                s3_objs = (self.fusion_aws_util.s3.count_objects(s3)
                           if s3 else -1)
                if guest_total == 0 and s3_objs <= 0:
                    self.log.info(
                        "Fusion disable confirmed on {} via AWS (0 guest "
                        "volumes, S3 empty/absent); fusion/status={!r}".format(
                            cluster_id, state))
                    return
                self.log.info(
                    "Fusion state on {} = {!r}; {} guest volume(s), S3 "
                    "objects={} — still disabling".format(
                        cluster_id, state, guest_total, s3_objs))
            else:
                self.log.info(
                    "Fusion state on {} = {!r} (waiting for 'disabled')".format(
                        cluster_id, state))
            time.sleep(15)
        self.fail(
            "Cluster {} did not reach fusion-disabled state within {}s "
            "(last state={!r})".format(
                cluster_id, timeout,
                self.get_fusion_state(cluster_id)))

    def _wait_for_fusion_enabled(self, cluster_id, project_id=None,
                                 timeout=None):
        """Poll fusion/status until state is 'enabled'.

        For callers that need to confirm the enable POST has been fully
        processed before proceeding (e.g. before triggering a backup on a
        freshly-enabled cluster, or before disabling immediately after).
        In most cases _wait_for_cluster_healthy (which watches the v4 cluster
        state) is sufficient and more robust; use this when you specifically
        need the fusion lifecycle state.
        """
        timeout = timeout or int(getattr(self, 'rebalance_timeout', 1800))
        project_id = project_id or self.project_id
        deadline = time.time() + timeout
        while time.time() < deadline:
            state = self.get_fusion_state(cluster_id)
            self.log.info(
                "Fusion state on {} = {!r} (waiting for 'enabled')".format(
                    cluster_id, state))
            if state == "enabled":
                self.log.info(
                    "Fusion enable complete on {} (state={!r})".format(
                        cluster_id, state))
                return
            time.sleep(15)
        self.fail(
            "Cluster {} did not reach fusion-enabled state within {}s "
            "(last state={!r})".format(
                cluster_id, timeout,
                self.get_fusion_state(cluster_id)))

    def assert_fusion_enabled_after_restore(self, cluster_id, project_id=None,
                                            timeout=1800):
        """Verify the server enabled Fusion on a target after restoring a
        Fusion-enabled backup.

        The target converges to match the source's Fusion state. All checks
        are hard-fail after the polling deadline — the server MUST stand up
        Fusion infrastructure on the destination.

        Authoritative AWS signals (JWT-independent):
          1. Fusion S3 log-store bucket created         -> hard fail if not
          2. Data sync starts (S3 object count > 0)     -> hard fail if not
          3. Guest volumes appear (from backup snaps)    -> hard fail if not
          4. fusion/status                               -> best-effort log
        """
        project_id = project_id or self.project_id

        if not self.fusion_aws_util:
            # No AWS creds — fall back to the (best-effort) internal status.
            state = self.get_fusion_state(cluster_id)
            self.log.warning(
                "AWS creds not set — verifying Fusion-enabled via fusion/status "
                "only (state={!r}); set aws creds for the authoritative S3 / "
                "guest-volume checks.".format(state))
            if state not in ("enabled", "enabling"):
                self.fail(
                    "Server did not enable Fusion on target {} after restore "
                    "(fusion_status={!r})".format(cluster_id, state))
            return

        # 1) PRIMARY: the server must create a Fusion S3 log-store bucket for
        #    the target. Reliable and JWT-independent.
        deadline = time.time() + timeout
        s3_bucket = None
        while time.time() < deadline:
            s3_bucket = self.fusion_aws_util.find_fusion_s3_bucket(cluster_id)
            if s3_bucket:
                break
            self.log.info(
                "Waiting for server to create the Fusion S3 bucket on target "
                "{}...".format(cluster_id))
            time.sleep(30)
        if not s3_bucket:
            self.fail(
                "No Fusion S3 log-store bucket created for target {} after "
                "restore — server did not enable Fusion on the "
                "destination".format(cluster_id))
        self.log.info(
            "Fusion S3 bucket '{}' created for target {} — server enabled "
            "Fusion on the destination".format(s3_bucket, cluster_id))

        # 2) Data sync must start — objects appear in the log-store bucket.
        deadline = time.time() + timeout
        obj_count = 0
        while time.time() < deadline:
            obj_count = self.fusion_aws_util.s3.count_objects(s3_bucket)
            if obj_count and obj_count > 0:
                break
            self.log.info(
                "Waiting for data sync to start — Fusion S3 bucket '{}' object "
                "count = {}".format(s3_bucket, obj_count))
            time.sleep(30)
        if obj_count and obj_count > 0:
            self.log.info(
                "Data sync started — Fusion S3 bucket '{}' has {} "
                "objects".format(s3_bucket, obj_count))
        else:
            self.fail(
                "Fusion S3 bucket '{}' has 0 objects after {}s — data sync / "
                "migration did not start on target {}".format(
                    s3_bucket, timeout, cluster_id))

        # 3) Guest volumes (from the backup's guest-vol snapshots) must be
        #    applied to the target as Fusion comes up. Poll because there is a
        #    delay between restore-complete and guest volumes appearing.
        try:
            deadline = time.time() + timeout
            guest_total = 0
            while time.time() < deadline:
                guest = self.fusion_aws_util.get_guest_volumes_for_cluster(
                    cluster_id)
                guest_total = sum(len(v) for v in guest.values())
                if guest_total and guest_total > 0:
                    break
                self.log.info(
                    "Waiting for guest volumes to appear on target {} "
                    "({} so far)...".format(cluster_id, guest_total))
                time.sleep(30)
        except NotImplementedError as e:
            self.log.warning("Guest-volume lookup unsupported: {}".format(e))
            guest_total = None
        if guest_total:
            self.log.info(
                "Target {} has {} Fusion guest volume(s) after restore".format(
                    cluster_id, guest_total))
        else:
            self.fail(
                "Target {} has 0 guest volumes after {}s — guest-volume "
                "snapshots from the backup were not applied".format(
                    cluster_id, timeout))

        # 4) fusion/status — best-effort (the internal endpoint + its session
        #    token can be flaky on long runs, so this is informational only).
        state = self.get_fusion_state(cluster_id)
        self.log.info(
            "Target {} fusion/status = {!r} (informational)".format(
                cluster_id, state))

    def assert_fusion_free_after_restore(self, cluster_id, project_id=None,
                                         timeout=1800, aws_util=None):
        """Verify a Fusion-enabled target converges to Fusion-FREE after
        restoring a Fusion-DISABLED backup. aws_util defaults to the source
        region's util; pass the target region's util for a cross-region restore.

        The target matches the source's Fusion state. The server MUST
        tear down Fusion infrastructure: state transitions through
        'disabling' (S3 file deletion starts, migration stops) to
        'disabled' (CP removes guest volumes, deletes the S3 bucket).

        Checks (hard-fail on timeout):
          0. fusion/status transitions: enabled -> disabling -> disabled
          1. Guest volumes removed by the CP
          2. Fusion S3 log-store bucket deleted
        """
        project_id = project_id or self.project_id
        aws_util = aws_util or self.fusion_aws_util

        # 0) Verify the fusion/status lifecycle transition. The server must
        #    move from enabled through disabling to disabled. Poll so we catch
        #    the intermediate state, but the hard requirement is reaching
        #    'disabled' (or the endpoint returning None, meaning Fusion was
        #    fully torn down).
        state = self.get_fusion_state(cluster_id)
        self.log.info(
            "Target {} initial fusion/status = {!r}".format(cluster_id, state))
        if state not in ("disabling", "disabled"):
            deadline = time.time() + timeout
            saw_disabling = False
            while time.time() < deadline:
                state = self.get_fusion_state(cluster_id)
                self.log.info(
                    "Target {} fusion/status = {!r} (expect disabling -> "
                    "disabled)".format(cluster_id, state))
                if state in ("disabled", None):
                    break
                if state == "disabling":
                    saw_disabling = True
                    self.log.info(
                        "Target {} transitioned to 'disabling' — server is "
                        "deleting S3 files, migration stopping".format(
                            cluster_id))
                time.sleep(30)
            if state not in ("disabled", None):
                if saw_disabling:
                    self.fail(
                        "Target {} is stuck at 'disabling' after {}s — did "
                        "not reach 'disabled'".format(cluster_id, timeout))
                else:
                    self.fail(
                        "Target {} fusion/status = {!r} after {}s — never "
                        "transitioned to 'disabling'/'disabled'".format(
                            cluster_id, state, timeout))
        else:
            self.log.info(
                "Target {} fusion/status = {!r} — disable already in "
                "progress".format(cluster_id, state))

        # AWS-cred-less path: the internal status endpoint is the only signal.
        if not aws_util:
            if state not in ("disabled", None, "disabling"):
                self.fail(
                    "Target {} still reports Fusion '{}' after restoring a "
                    "non-Fusion backup — expected it to become "
                    "Fusion-free".format(cluster_id, state))
            self.log.warning(
                "AWS creds not set — verified Fusion-free via fusion/status "
                "only (state={!r}); set aws creds for the authoritative "
                "guest-volume / S3 checks.".format(state))
            return

        # 1) Guest volumes must be removed by the CP — poll until zero.
        deadline = time.time() + timeout
        guest_total = None
        while time.time() < deadline:
            try:
                guest = aws_util.get_guest_volumes_for_cluster(
                    cluster_id)
            except NotImplementedError as e:
                self.log.warning("Guest-volume lookup unsupported: {}".format(e))
                guest_total = 0
                break
            guest_total = sum(len(v) for v in guest.values())
            if guest_total == 0:
                break
            self.log.info(
                "Waiting for CP to remove guest volumes on {} ({} "
                "remaining)".format(cluster_id, guest_total))
            time.sleep(30)
        if guest_total:
            self.fail(
                "Target {} still has {} guest volume(s) after {}s — did not "
                "converge Fusion-free".format(cluster_id, guest_total, timeout))
        self.log.info("Target {} has 0 guest volumes".format(cluster_id))

        # 2) The Fusion S3 log-store bucket must be emptied of its fusion log
        #    data. The bucket itself is pre-provisioned on every cluster at
        #    deploy time and is not necessarily deleted when fusion is off, so
        #    "Fusion-free" means the bucket is absent OR empty — not absent.
        deadline = time.time() + timeout
        s3_bucket = aws_util.find_fusion_s3_bucket(cluster_id)
        s3_objs = (aws_util.s3.count_objects(s3_bucket)
                   if s3_bucket else -1)
        while s3_bucket and s3_objs > 0 and time.time() < deadline:
            self.log.info(
                "Waiting for CP to drain the Fusion S3 bucket '{}' on {} "
                "({} objects)...".format(s3_bucket, cluster_id, s3_objs))
            time.sleep(30)
            s3_bucket = aws_util.find_fusion_s3_bucket(cluster_id)
            s3_objs = (aws_util.s3.count_objects(s3_bucket)
                       if s3_bucket else -1)
        if s3_bucket and s3_objs > 0:
            self.fail(
                "Target {} Fusion S3 bucket '{}' still has {} objects after "
                "{}s — did not converge Fusion-free".format(
                    cluster_id, s3_bucket, s3_objs, timeout))
        self.log.info(
            "Target {} Fusion S3 bucket {} — Fusion-free".format(
                cluster_id,
                "absent" if not s3_bucket else "'{}' empty".format(s3_bucket)))

    TARGET_PRELOAD_PREFIX = "tgt-pre-"

    def preload_target(self, rebalance, aws_util=None):
        """Give the target its OWN data before the restore, so we test
        restoring into a NON-EMPTY cluster (a common customer situation).

        Always creates + loads a fresh '<TARGET_PRELOAD_PREFIX>*' bucket
        (buckets are never reused — only clusters are). When `rebalance` is True
        (Fusion-enabled target) it also Fusion-rebalances so the target has its
        own guest volumes.

        Records the preload bucket name in self._target_preload_bucket and
        returns (preload_bucket_name, guest_volume_ids).
        """
        cid, proj = self.target_cluster_id, self.target_project_id
        ops = self.capellaAPI.cluster_ops_apis

        # Delete any stale preload bucket first, then create a fresh one (the
        # restore wipes/replaces it, so it can't be reused). delete-old-create-
        # new keeps buckets from accumulating / exhausting the KV-RAM quota.
        self._delete_buckets_with_prefix(cid, proj, self.TARGET_PRELOAD_PREFIX)
        name = "{}{}".format(
            self.TARGET_PRELOAD_PREFIX,
            uuid.uuid4().hex[:5])
        self.log.info(
            "Pre-loading target {} with its own bucket '{}'".format(cid, name))
        bkt_id = self.create_fusion_bucket(cid, name, project_id=proj)
        deadline = time.time() + 120
        while time.time() < deadline:
            if ops.fetch_bucket_info(
                    self.organisation_id, proj, cid,
                    bkt_id).status_code == 200:
                break
            time.sleep(10)
        time.sleep(120)  # Magma KV storage init
        self.load_documents(
            cid, name, self.target_preload_docs, project_id=proj)
        count = self._wait_for_item_count(
            cid, proj, bkt_id, self.target_preload_docs)
        if count == 0:
            self.fail(
                "Target preload bucket '{}' has 0 docs after load".format(name))
        self.log.info(
            "Target preload bucket '{}' loaded — {} docs".format(name, count))

        self._target_preload_bucket = name

        vol_ids = set()
        if rebalance:
            # Fusion rebalance so the target has its own guest volumes. Records
            # the baseline node count so tearDown scales it back.
            self._target_original_nodes, new_nodes = (
                self.trigger_fusion_rebalance(cid, project_id=proj))
            if not self.wait_for_rebalance_complete(cid, project_id=proj):
                self.fail(
                    "Target Fusion rebalance did not complete on {}".format(
                        cid))
            self.log.info(
                "Target Fusion rebalance complete ({} -> {} nodes)".format(
                    self._target_original_nodes, new_nodes))
            aws_util = aws_util or self.fusion_aws_util
            if aws_util:
                guest = aws_util.get_guest_volumes_for_cluster(cid)
                for vols in guest.values():
                    vol_ids.update(vols)
                self.log.info(
                    "Target {} has {} guest volume(s) before restore: "
                    "{}".format(cid, len(vol_ids), sorted(vol_ids)))
                if not vol_ids:
                    self.log.warning(
                        "Target {} has 0 guest volumes after preload rebalance "
                        "— data may be below the Fusion threshold".format(cid))
        return name, vol_ids

    def check_preload_bucket_after_restore(self, preload_bucket):
        """Informational: did the restore preserve or remove the target's own
        pre-existing bucket? Reveals whether restore is additive or
        wipe-and-replace — key for deciding cluster-reuse cleanup."""
        if not preload_bucket:
            return
        resp = self.capellaAPI.cluster_ops_apis.list_buckets(
            self.organisation_id, self.target_project_id,
            self.target_cluster_id)
        present = ([b["name"] for b in resp.json().get("data", [])]
                   if resp.status_code == 200 else [])
        if preload_bucket in present:
            self.log.info(
                "Restore PRESERVED the target's pre-existing bucket '{}' "
                "(restore is additive). Target buckets now: {}".format(
                    preload_bucket, present))
        else:
            self.log.info(
                "Restore REMOVED the target's pre-existing bucket '{}' "
                "(restore is wipe-and-replace). Target buckets now: {}".format(
                    preload_bucket, present))

    def assert_guest_volumes_deleted(self, cluster_id, pre_vol_ids,
                                     timeout=1800, aws_util=None):
        """Verify the restore deleted the target's pre-existing guest volumes:
        none of pre_vol_ids may remain on the cluster. Hard fail otherwise.
        aws_util defaults to the source util; pass the target region's util for
        a cross-region restore.
        """
        aws_util = aws_util or self.fusion_aws_util
        if not pre_vol_ids:
            self.log.warning(
                "No pre-restore guest volumes captured — skipping deletion "
                "check on {}.".format(cluster_id))
            return
        if not aws_util:
            self.log.warning(
                "AWS creds not set — skipping guest-volume deletion check.")
            return
        deadline = time.time() + timeout
        remaining = set(pre_vol_ids)
        while remaining and time.time() < deadline:
            guest = aws_util.get_guest_volumes_for_cluster(
                cluster_id)
            current = set()
            for vols in guest.values():
                current.update(vols)
            remaining = set(pre_vol_ids) & current
            if not remaining:
                break
            self.log.info(
                "Waiting for {} pre-restore guest volume(s) to be deleted on "
                "{}: {}".format(len(remaining), cluster_id, sorted(remaining)))
            time.sleep(30)
        if remaining:
            self.fail(
                "Pre-restore guest volumes still present on {} after {}s — "
                "restore did not delete them: {}".format(
                    cluster_id, timeout, sorted(remaining)))
        self.log.info(
            "All {} pre-restore guest volumes deleted on {} after "
            "restore".format(len(pre_vol_ids), cluster_id))

    def _cp_poll(self, fn, *args, **kwargs):
        """Call a CapellaAPI method for a poll loop and classify the outcome:
          ('ok', resp)        -- HTTP 200
          ('rate', resp)      -- HTTP 429 (rate limited)
          ('notfound', reason)-- HTTP 404: the resource genuinely does not
                                 exist (e.g. a restore never created the
                                 bucket). This is a PRODUCT/restore signal, not
                                 infra — callers must not conflate it with a CP
                                 outage.
          ('transient', reason)-- the wrapper's sys.exit() (BaseException) on a
                                 connection error, a 5xx, or any other non-200:
                                 the control plane is momentarily unusable.
        Callers treat 'transient'/'notfound' as "retry WITHOUT spending the
        convergence budget" and give up only after _cp_error_limit consecutive
        such results, with a message that names which of the two it was."""
        try:
            resp = fn(*args, **kwargs)
        except BaseException as exc:
            return "transient", "exception: {}".format(exc)
        code = getattr(resp, "status_code", None)
        if code == 200:
            return "ok", resp
        if code == 429:
            return "rate", resp
        reason = "HTTP {}: {}".format(
            code, (resp.content[:200] if getattr(resp, "content", None)
                   else ""))
        if code == 404:
            return "notfound", reason
        return "transient", reason

    def _cp_giveup_msg(self, outcome, resource, count):
        """Message when a poll loop gives up after `count` consecutive non-OK
        results, worded by outcome so infra (5xx/network) is never confused with
        a genuine not-found (restore did not create the resource)."""
        secs = count * self._cp_poll_interval
        if outcome == "notfound":
            return ("{} not found after ~{}s ({} consecutive 404s) — the "
                    "restore did not create it".format(resource, secs, count))
        return ("Control plane unreachable for {} — {} consecutive errors "
                "(~{}s)".format(resource, count, secs))

    def _wait_for_cluster_healthy(self, cluster_id, project_id, timeout=1800):
        """Poll cluster state until healthy or timeout.

        Resilient to a flaky control plane: transient CP errors (wrapper
        sys.exit / 5xx / network) do NOT count against the convergence timeout,
        and a sustained outage (>= _cp_error_limit consecutive) fails fast with
        a clear "control plane unreachable" message rather than silently eating
        the whole timeout and reporting a misleading "did not reach healthy".
        """
        deadline = time.time() + timeout
        last_state_logged = None
        last_full_log = 0
        cp_errors = 0
        while time.time() < deadline:
            outcome, payload = self._cp_poll(
                self.capellaAPI.cluster_ops_apis.fetch_cluster_info,
                self.organisation_id, project_id, cluster_id)
            if outcome in ("transient", "notfound"):
                cp_errors += 1
                if cp_errors >= self._cp_error_limit:
                    self.fail(self._cp_giveup_msg(
                        outcome, "cluster {}".format(cluster_id), cp_errors)
                        + "; last: {}".format(payload))
                # Don't spend the convergence budget on a CP blip / not-yet.
                deadline += self._cp_poll_interval
                time.sleep(self._cp_poll_interval)
                continue
            cp_errors = 0
            if outcome == "rate":
                self.handle_rate_limit(int(payload.headers["Retry-After"]))
                continue
            resp = payload
            if resp.status_code == 200:
                body = resp.json()
                state = body.get("currentState", "")
                now = time.time()
                if state != last_state_logged:
                    self.log.info(
                        "Cluster {} state: '{}'".format(cluster_id, state))
                    last_state_logged = state
                # When state is empty/unknown, dump the full body once per
                # minute so we can see what field actually carries status.
                if (not state or state not in (
                        "healthy", "deploying", "scaling", "rebalancing")):
                    if now - last_full_log > 60:
                        self.log.info(
                            "Cluster {} full info (keys={}): {}".format(
                                cluster_id, list(body.keys()), body))
                        last_full_log = now
                if state == self.CLUSTER_HEALTHY:
                    return True
                if state in ["deploymentFailed", "deletionFailed",
                             "restoreFailed", "rebalanceFailed", "scaleFailed"]:
                    self.fail(
                        "Cluster {} reached terminal error state: {}".format(
                            cluster_id, state))
            # 200 but still converging — this DOES count against the timeout.
            time.sleep(self._cp_poll_interval)
        self.fail(
            "Cluster {} did not reach healthy within {}s".format(
                cluster_id, timeout))

    SOURCE_BUCKET_PREFIX = "fusion-bkt-"

    def _delete_last_backup(self):
        """Delete the backup this test created (called on success AND failure).
        A preset backup_id belongs to another run and is never deleted. No-op if
        no backup was created."""
        if (not self._last_backup_id
                or self._last_backup_id == self.preset_backup_id
                or not self.source_project_id
                or not self.source_cluster_id):
            return
        try:
            resp = self.capellaAPI.cluster_ops_apis.delete_backup(
                self.organisation_id, self.source_project_id,
                self.source_cluster_id, self._last_backup_id)
            if resp.status_code not in (200, 202, 204, 404):
                self.log.warning(
                    "tearDown: delete backup {} returned {}: {}".format(
                        self._last_backup_id, resp.status_code, resp.content))
            else:
                self.log.info(
                    "tearDown: deleted backup {}".format(self._last_backup_id))
        except BaseException as exc:
            self.log.warning(
                "tearDown: exception deleting backup {}: {}".format(
                    self._last_backup_id, exc))

    def _delete_buckets_with_prefix(self, cluster_id, project_id, prefix,
                                    keep_ids=()):
        """Delete every bucket whose name starts with `prefix` on a cluster,
        except any id in keep_ids. Called before creating a fresh bucket so a
        stale same-prefix bucket is cleared first — buckets never accumulate
        (and the cluster's KV-RAM quota isn't exhausted by leftovers)."""
        ops = self.capellaAPI.cluster_ops_apis
        resp = ops.list_buckets(self.organisation_id, project_id, cluster_id)
        if resp.status_code != 200:
            return
        for b in resp.json().get("data", []):
            name = b.get("name", "")
            if not name.startswith(prefix) or b.get("id") in keep_ids:
                continue
            try:
                dr = ops.delete_bucket(
                    self.organisation_id, project_id, cluster_id, b["id"])
                if dr.status_code in (200, 202, 204, 404):
                    self.log.info(
                        "Deleted stale bucket '{}' on {} before creating a "
                        "fresh one".format(name, cluster_id))
                else:
                    self.log.warning(
                        "Delete stale bucket '{}' on {} returned {}: {}".format(
                            name, cluster_id, dr.status_code, dr.content))
            except Exception as exc:
                self.log.warning(
                    "Exception deleting stale bucket '{}' on {}: {}".format(
                        name, cluster_id, exc))

    def populate_source_buckets(self):
        """Populate the source cluster's bucket(s) with self.num_docs docs each.

        Reuse is the default so a 100M-doc load isn't repeated every test:
          1. source_bucket_id set -> reuse that specific bucket (top up if short)
          2. else, an existing fusion-bkt-* bucket on the (pooled) cluster is
             reused as-is, topping up only if short of num_docs
          3. else (fresh cluster) -> create + load
        Buckets are preserved (never deleted here). Records bucket ids/names in
        self.source_bucket_ids/source_bucket_names.
        """
        cid, proj = self.source_cluster_id, self.source_project_id
        ops = self.capellaAPI.cluster_ops_apis

        # Local fast-iteration override: reuse a specific existing source bucket
        # by id (source_bucket_id in the .ini). Skips create + load (tops up only
        # if short of num_docs); preserved in tearDown. NOTE: only safe for tests
        # that do NOT re-rebalance the source — reusing a bucket that already has
        # guest volumes corrupts Step 3/4/8 of the Fusion-enabled-source tests.
        if self.preset_source_bucket_id:
            info = ops.fetch_bucket_info(
                self.organisation_id, proj, cid, self.preset_source_bucket_id)
            if info.status_code != 200:
                self.fail(
                    "source_bucket_id {} not found on cluster {}: {} {}".format(
                        self.preset_source_bucket_id, cid,
                        info.status_code, info.content))
            b = info.json()
            name = b.get("name")
            count = b.get("stats", {}).get("itemCount", 0)
            self.source_bucket_ids.append(self.preset_source_bucket_id)
            self.source_bucket_names.append(name)
            self.log.info(
                "Reusing preset source bucket '{}' ({} docs) — skipping "
                "load".format(name, count))
            if count < self.num_docs:
                self.load_documents(
                    cid, name, self.num_docs, project_id=proj,
                    create_start_index=count, create_end_index=self.num_docs)
            return

        # Reuse usable source bucket(s) on the pooled cluster; top up if short.
        # A stale/malformed/missing bucket falls through to create-fresh below.
        existing = []
        lb = ops.list_buckets(self.organisation_id, proj, cid)
        if lb.status_code == 200:
            for b in lb.json().get("data", []):
                if (b.get("name", "").startswith(self.SOURCE_BUCKET_PREFIX)
                        and b.get("id")
                        and b.get("stats", {}).get("itemCount") is not None):
                    existing.append(b)
        if len(existing) >= self.num_buckets:
            for b in existing[:self.num_buckets]:
                name, bid = b["name"], b["id"]
                count = b.get("stats", {}).get("itemCount", 0)
                self.source_bucket_ids.append(bid)
                self.source_bucket_names.append(name)
                if count < self.num_docs:
                    self.log.info(
                        "Reusing source bucket '{}' ({} docs) — topping up to "
                        "{}".format(name, count, self.num_docs))
                    self.load_documents(
                        cid, name, self.num_docs, project_id=proj,
                        create_start_index=count,
                        create_end_index=self.num_docs)
                else:
                    self.log.info(
                        "Reusing source bucket '{}' ({} docs >= {}) — no "
                        "reload".format(name, count, self.num_docs))
            return

        # No reusable bucket on this (fresh) cluster — create + load. Clear any
        # partial leftovers first so the KV-RAM quota isn't exhausted.
        self._delete_buckets_with_prefix(cid, proj, self.SOURCE_BUCKET_PREFIX)
        # Use uuid, NOT generate_random_string: the doc generator calls
        # random.seed(0) on the global RNG during loading, which makes
        # generate_random_string deterministic — producing IDENTICAL bucket
        # names across separate runs and cross-contaminating clusters that
        # share a DocLoader. uuid4 is independent of the RNG seed.
        run_id = uuid.uuid4().hex[:6]
        for i in range(self.num_buckets):
            name = "{}{}-{}".format(self.SOURCE_BUCKET_PREFIX, run_id, i)
            self.source_bucket_ids.append(
                self.create_fusion_bucket(cid, name, project_id=proj))
            self.source_bucket_names.append(name)

        for bkt_id in self.source_bucket_ids:
            deadline = time.time() + 120
            while time.time() < deadline:
                if ops.fetch_bucket_info(
                        self.organisation_id, proj, cid,
                        bkt_id).status_code == 200:
                    break
                time.sleep(10)
            else:
                self.fail(
                    "Bucket {} did not become ready within 120s".format(bkt_id))
        time.sleep(120)  # Magma KV storage init

        for name in self.source_bucket_names:
            self.load_documents(cid, name, self.num_docs, project_id=proj)

        for bkt_id in self.source_bucket_ids:
            count = self._wait_for_item_count(cid, proj, bkt_id, self.num_docs)
            if count == 0:
                self.fail(
                    "Bucket {} has 0 docs after load — DocLoader failed "
                    "silently".format(bkt_id))
            self.log.info("Bucket {} loaded — {} docs".format(bkt_id, count))

    def _wait_for_item_count(self, cluster_id, project_id, bucket_id,
                             target, timeout=300):
        """Poll a bucket's itemCount until it reaches `target` or times out;
        return the last observed count."""
        count = 0
        deadline = time.time() + timeout
        cp_errors = 0
        while time.time() < deadline:
            outcome, payload = self._cp_poll(
                self.capellaAPI.cluster_ops_apis.fetch_bucket_info,
                self.organisation_id, project_id, cluster_id, bucket_id)
            if outcome in ("transient", "notfound"):
                cp_errors += 1
                if cp_errors >= self._cp_error_limit:
                    self.fail(self._cp_giveup_msg(
                        outcome, "bucket {}".format(bucket_id), cp_errors)
                        + "; last: {}".format(payload))
                deadline += self._cp_poll_interval
                time.sleep(self._cp_poll_interval)
                continue
            cp_errors = 0
            if outcome == "rate":
                self.handle_rate_limit(int(payload.headers["Retry-After"]))
                continue
            count = payload.json().get("stats", {}).get("itemCount", 0)
            if count >= target:
                break
            time.sleep(self._cp_poll_interval)
        return count

    def verify_data_integrity(self):
        """Verify each source bucket's doc count matches on the target after
        restore. Records restored bucket ids in self._target_bucket_ids so
        tearDown can clean them up before a rerun reuses the target.
        """
        ops = self.capellaAPI.cluster_ops_apis
        target_buckets = []
        deadline = time.time() + 120
        while time.time() < deadline:
            resp = ops.list_buckets(
                self.organisation_id, self.target_project_id,
                self.target_cluster_id)
            if resp.status_code == 429:
                self.handle_rate_limit(int(resp.headers["Retry-After"]))
                continue
            if resp.status_code == 200:
                target_buckets = resp.json().get("data", [])
                if len(target_buckets) >= self.num_buckets:
                    break
            time.sleep(15)
        if not target_buckets:
            self.fail("No buckets found on target cluster after restore")

        # tearDown deletes these. Exclude the target's own preload bucket so a
        # reused target keeps its 100 GB dataset (no reload next run).
        self._target_bucket_ids = [
            b["id"] for b in target_buckets
            if b.get("name") != self._target_preload_bucket]
        tgt_id_by_name = {b["name"]: b["id"] for b in target_buckets}

        for src_bkt_id, name in zip(
                self.source_bucket_ids, self.source_bucket_names):
            src = ops.fetch_bucket_info(
                self.organisation_id, self.source_project_id,
                self.source_cluster_id, src_bkt_id)
            if src.status_code != 200:
                self.fail("Could not fetch source bucket '{}': {} {}".format(
                    name, src.status_code, src.content))
            source_count = src.json().get("stats", {}).get("itemCount", 0)

            tgt_bkt_id = tgt_id_by_name.get(name)
            if not tgt_bkt_id:
                self.fail(
                    "Bucket '{}' not found on target after restore".format(
                        name))

            target_count = self._wait_for_item_count(
                self.target_cluster_id, self.target_project_id, tgt_bkt_id,
                source_count, timeout=self.restore_timeout)
            if target_count != source_count:
                self.fail(
                    "Doc count mismatch for bucket '{}': source={}, "
                    "target={}".format(name, source_count, target_count))
            self.log.info(
                "Data integrity OK: bucket '{}' — source={}, target={}".format(
                    name, source_count, target_count))

            # Basic correctness beyond count: the restored bucket must be Magma
            # (fusion requires Magma), and its settings are logged for
            # post-run visibility. The Magma assertion only fires when the
            # field is present and genuinely non-Magma, so it can't false-fail.
            tgt = ops.fetch_bucket_info(
                self.organisation_id, self.target_project_id,
                self.target_cluster_id, tgt_bkt_id)
            if tgt.status_code == 200:
                tcfg = tgt.json()
                scfg = src.json()
                storage = tcfg.get("storageBackend")
                if storage and str(storage).lower() != "magma":
                    self.fail(
                        "Restored bucket '{}' is not Magma (storageBackend={})"
                        " — fusion requires Magma".format(name, storage))
                self.log.info(
                    "Bucket '{}' after restore: storageBackend={}, replicas={},"
                    " durability={}, ttl={}, ramMB={} | source: replicas={}, "
                    "durability={}, ttl={}".format(
                        name, storage, tcfg.get("replicas"),
                        tcfg.get("durabilityLevel"),
                        tcfg.get("timeToLiveInSeconds"),
                        tcfg.get("memoryAllocationInMb"),
                        scfg.get("replicas"), scfg.get("durabilityLevel"),
                        scfg.get("timeToLiveInSeconds")))

    def create_fusion_bucket(self, cluster_id, bucket_name, project_id=None):
        """Create a Magma bucket on the cluster. Returns bucket_id.

        RAM quota (MB/node) is self.bucket_ram_quota — raise it (e.g. 4096) for
        large data loads; 1024 is too small to ingest tens of GB without the KV
        write buffer back-pressuring into ServerOutOfMemory (ENOMEM).
        """
        project_id = project_id or self.project_id
        ram = self.bucket_ram_quota
        deadline = time.time() + self.rebalance_timeout
        while True:
            resp = self.capellaAPI.cluster_ops_apis.create_bucket(
                self.organisation_id, project_id, cluster_id,
                bucket_name, "couchbase", "magma",
                ram, "seqno", "none", 1, False, 0)
            if resp.status_code == 429:
                self.handle_rate_limit(int(resp.headers["Retry-After"]))
                continue
            if resp.status_code == 201:
                break
            body = (resp.content or b"").decode("utf-8", "ignore").lower()
            if (("rebalancing" in body or "temporarily unavailable" in body)
                    and time.time() < deadline):
                self.log.info(
                    "Bucket create on {} deferred (cluster busy) — waiting for "
                    "healthy, then retrying".format(cluster_id))
                self._wait_for_cluster_healthy(
                    cluster_id, project_id, timeout=self.rebalance_timeout)
                continue
            self.fail(
                "Bucket creation failed on cluster {}: {} {}".format(
                    cluster_id, resp.status_code, resp.content))
        bucket_id = resp.json()["id"]
        self.log.info(
            "Created bucket '{}' ({}) on cluster {}".format(
                bucket_name, bucket_id, cluster_id))
        return bucket_id

    def _setup_cluster_access(self, cluster_id, project_id):
        """Allow 0.0.0.0/0 and create a DB user for DocLoader.

        Returns (connection_string, username, password).
        User ID is stored in _db_users_to_cleanup for tearDown.
        """
        project_id = project_id or self.project_id

        self.capellaAPI.cluster_ops_apis.add_CIDR_to_allowed_CIDRs_list(
            self.organisation_id, project_id, cluster_id,
            "0.0.0.0/0", comment="TAF fusion test")

        username = "taf_loader_{}".format(
            uuid.uuid4().hex[:6])
        password = "{}P@ss1!".format(
            self.generate_random_string(8, special_characters=False))
        access = [{"privileges": ["data_reader", "data_writer"]}]

        resp = self.capellaAPI.cluster_ops_apis.create_database_user(
            self.organisation_id, project_id, cluster_id,
            username, access, password=password)
        if resp.status_code == 429:
            self.handle_rate_limit(int(resp.headers["Retry-After"]))
            resp = self.capellaAPI.cluster_ops_apis.create_database_user(
                self.organisation_id, project_id, cluster_id,
                username, access, password=password)
        if resp.status_code not in [200, 201]:
            self.fail(
                "Failed to create DB user on cluster {}: {} {}".format(
                    cluster_id, resp.status_code, resp.content))

        user_id = resp.json().get("id") or username
        self._db_users_to_cleanup[cluster_id] = user_id

        conn_resp = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
            self.organisation_id, project_id, cluster_id)
        if conn_resp.status_code != 200:
            self.fail(
                "Could not fetch cluster info for {}: {} {}".format(
                    cluster_id, conn_resp.status_code, conn_resp.content))
        conn_str = conn_resp.json().get("connectionString", "")
        if not conn_str:
            self.fail(
                "Empty connectionString for cluster {}".format(cluster_id))

        # New DB users propagate to data nodes asynchronously; without this
        # sleep the next SDK auth attempt races and hits
        # AuthenticationFailureException.
        self.log.info(
            "Waiting 60s for DB user '{}' to propagate".format(username))
        time.sleep(60)

        self.log.info(
            "Cluster {} access: conn_str={}, user={}".format(
                cluster_id, conn_str, username))
        return conn_str, username, password

    def load_documents(self, cluster_id, bucket_name, num_docs=None,
                       project_id=None,
                       create_start_index=None, create_end_index=None):
        """Load CREATE operations into bucket_name via DocLoader.

        If create_start_index/end are given, only that key range is loaded
        (useful for topping up an existing bucket).  Otherwise we load
        [0, num_docs).  Final bucket itemCount is expected to be
        >= create_end_index (or num_docs when not specified).
        """
        project_id = project_id or self.project_id
        num_docs = num_docs or self.num_docs
        start_idx = 0 if create_start_index is None else create_start_index
        end_idx = num_docs if create_end_index is None else create_end_index
        target_total = end_idx

        conn_str, username, password = self._setup_cluster_access(
            cluster_id, project_id)

        self.log.info(
            "Loading docs into bucket '{}' on cluster {} via DocLoader "
            "(range [{}, {}) → {} docs)".format(
                bucket_name, cluster_id, start_idx, end_idx,
                end_idx - start_idx))

        # TaskManager must be initialised before doc_load — otherwise
        # submit_task/get_task_result no-op silently (Java swallows the NPE
        # and returns 200 with status:false). HTTP 406 = already initialised.
        # Generous timeout: a busy loader can take a while to answer.
        init_resp = requests.post(
            "{}/init_task_manager".format(SiriusSetup.sirius_url),
            data=json.dumps({"num_workers": 100}),
            headers={"Content-Type": "application/json"},
            timeout=120)
        if init_resp.status_code not in (200, 406):
            self.fail(
                "DocLoader /init_task_manager failed: http={} body={}".format(
                    init_resp.status_code, init_resp.text))
        self.log.info(
            "DocLoader TaskManager ready (http={})".format(init_resp.status_code))

        # Register cluster master + creds in the SDK client pool before
        # doc_load, else doc_load fires with Server=null. The Java DocLoader
        # /create_clients always returns HTTP 400 (known bug — file in
        # DocLoader repo), so we read body["status"] instead of response.ok.
        create_clients_payload = {
            "server_ip": conn_str,
            "server_port": "11207",
            "username": username,
            "password": password,
            "bucket_name": bucket_name,
            "req_clients": self.load_clients,
        }
        cc_url = "{}/create_clients".format(SiriusSetup.sirius_url)
        cc_resp = requests.post(
            cc_url,
            data=json.dumps(create_clients_payload),
            headers={"Content-Type": "application/json"},
            timeout=120)
        try:
            cc_body = cc_resp.json()
        except Exception:
            cc_body = {"raw": cc_resp.text}
        if not cc_body.get("status", False):
            self.fail(
                "DocLoader /create_clients failed for bucket '{}': "
                "http={} body={}".format(
                    bucket_name, cc_resp.status_code, cc_body))
        self.log.info(
            "DocLoader client pool created for bucket '{}'".format(bucket_name))

        loader = SiriusCouchbaseLoader(
            server_ip=conn_str,
            server_port=11207,
            username=username,
            password=password,
            bucket=types.SimpleNamespace(name=bucket_name),
            create_percent=100,
            create_start_index=start_idx,
            create_end_index=end_idx,
            key_prefix="fd-",
            key_size=32,
            doc_size=self.doc_size,
            process_concurrency=self.load_concurrency)

        ok, resp = loader.create_doc_load_task()
        if not ok:
            self.fail(
                "DocLoader create_doc_load_task failed for bucket '{}': "
                "{}".format(bucket_name, resp))

        if not loader.start_task():
            self.fail(
                "DocLoader start_task failed for bucket '{}'".format(
                    bucket_name))

        loader_ok = loader.get_task_result()
        self.log.info(
            "DocLoader get_task_result: ok={}, fail_count={}".format(
                loader_ok, loader.fail_count))
        if loader.fail_count > 0:
            self.fail(
                "DocLoader reported {} failures loading bucket '{}'".format(
                    loader.fail_count, bucket_name))

        # DocLoader can return ok=true with 0 failures even when nothing was
        # written (silent SDK auth/connect failures swallowed by workers).
        actual = 0
        deadline = time.time() + 120
        while time.time() < deadline:
            list_resp = self.capellaAPI.cluster_ops_apis.list_buckets(
                self.organisation_id, project_id, cluster_id)
            if list_resp.status_code == 200:
                for b in list_resp.json().get("data", []):
                    if b.get("name") == bucket_name:
                        actual = b.get("stats", {}).get("itemCount", 0)
                        break
                if actual >= target_total:
                    break
            time.sleep(5)
        self.log.info(
            "Bucket '{}' itemCount after load: {} (expected >= {})".format(
                bucket_name, actual, target_total))
        if actual == 0:
            self.fail(
                "DocLoader claimed success but bucket '{}' has 0 items — "
                "likely SDK auth/connect failure (check the slave's "
                "/tmp/docloader.log)".format(bucket_name))
        if actual < target_total:
            self.fail(
                "Bucket '{}' has only {}/{} items after load — refusing "
                "to proceed to subsequent steps with a partial dataset. "
                "Check /tmp/docloader.log on the DocLoader slave for "
                "individual mutation failures.".format(
                    bucket_name, actual, target_total))

        self.log.info(
            "DocLoader complete: {} docs loaded into '{}'".format(
                num_docs, bucket_name))

    def trigger_fusion_rebalance(self, cluster_id, project_id=None,
                                 target_nodes=None):
        """Scale cluster to trigger a Fusion express-scaling rebalance:
        GET /specs (v2 internal), bump the data-service count, POST it back
        wrapped as {"specs": [...]}. Returns (original_nodes, new_nodes).
        """
        project_id = project_id or self.project_id
        base_url = "{}/v2/organizations/{}/projects/{}/clusters/{}/specs".format(
            self.capellaAPI.internal_url,
            self.organisation_id, project_id, cluster_id)

        resp = self.capellaAPI.do_internal_request(base_url, method="GET")
        if resp.status_code != 200:
            self.fail(
                "GET /specs failed for cluster {}: {} {}".format(
                    cluster_id, resp.status_code, resp.content))

        specs_body = resp.json()
        self.log.info("GET /specs returned (truncated): {}".format(
            json.dumps(specs_body)[:500]))

        # The Capella v2 endpoint returns {"data": {"specs": [...]}} on this
        # pod; older pods returned {"specs": [...]} or a bare list.  Handle
        # all three shapes.
        raw_specs = specs_body
        if isinstance(raw_specs, dict) and "data" in raw_specs:
            raw_specs = raw_specs["data"]
        if isinstance(raw_specs, dict) and "specs" in raw_specs:
            raw_specs = raw_specs["specs"]
        if isinstance(raw_specs, dict):
            raw_specs = [raw_specs]
        if not isinstance(raw_specs, list) or not raw_specs:
            self.fail("Unexpected /specs response: {}".format(specs_body))

        # Strip the read-only metadata Capella inflates the response with —
        # POST /specs rejects unknown fields like serviceOptions, computeOptions,
        # diskOptions, modificationLimited, customizable, default.
        def _clean(spec):
            services = spec.get("services", [])
            if services and isinstance(services[0], dict):
                services = [s.get("type") or s.get("key") for s in services]
            disk = spec.get("disk", {}) or {}
            das = spec.get("diskAutoScaling", {}) or {}
            return {
                "count": spec["count"],
                "services": services,
                "compute": spec.get("compute"),
                "disk": {
                    "type": disk.get("type"),
                    "sizeInGb": disk.get("sizeInGb"),
                    "iops": disk.get("iops"),
                },
                "diskAutoScaling": {"enabled": das.get("enabled", False)},
            }

        clean_specs = [_clean(s) for s in raw_specs]
        original_nodes = clean_specs[0]["count"]
        new_nodes = (
            target_nodes if target_nodes is not None else original_nodes + 1)

        # Bump the data-service (kv) group count.
        for spec in clean_specs:
            if "kv" in spec["services"] or "data" in spec["services"] \
                    or not spec["services"]:
                spec["count"] = new_nodes
                break
        else:
            clean_specs[0]["count"] = new_nodes

        # POST /specs requires nested-object format even though GET returns
        # plain strings: services -> [{"type": "kv"}], compute -> {"type": "m7g.xlarge"}.
        for spec in clean_specs:
            spec["services"] = [{"type": s} for s in spec["services"]]
            if isinstance(spec.get("compute"), str):
                spec["compute"] = {"type": spec["compute"]}

        payload = {"specs": clean_specs}
        self.log.info(
            "POST /specs ({} → {} nodes): {}".format(
                original_nodes, new_nodes, json.dumps(payload)))

        for _ in range(3):
            resp = self.capellaAPI.do_internal_request(
                base_url, method="POST", params=json.dumps(payload))
            if resp.status_code == 429:
                self.handle_rate_limit(int(resp.headers["Retry-After"]))
                continue
            if resp.status_code not in [200, 202]:
                try:
                    err_type = resp.json().get("errorType", "")
                except Exception:
                    err_type = ""
                if err_type in ("ClusterModifySpecsInvalidState",
                                "EntityNotWritable", "EntityStateInvalid"):
                    self.log.warning(
                        "Cluster not ready for scaling ({}); waiting…".format(
                            err_type))
                    time.sleep(30)
                    continue
                self.fail(
                    "Cluster scale to {} nodes failed on {}: {} {}".format(
                        new_nodes, cluster_id,
                        resp.status_code, resp.content))
            break

        return original_nodes, new_nodes

    def get_cluster_node_count(self, cluster_id, project_id=None):
        """Return the data-service (KV) node count for a cluster via GET /specs,
        or None if it can't be determined. Used by tearDown to decide whether a
        cluster actually needs scaling back down to its baseline.
        """
        project_id = project_id or self.project_id
        url = "{}/v2/organizations/{}/projects/{}/clusters/{}/specs".format(
            self.capellaAPI.internal_url,
            self.organisation_id, project_id, cluster_id)
        try:
            resp = self.capellaAPI.do_internal_request(url, method="GET")
        except Exception as exc:
            self.log.warning(
                "get_cluster_node_count GET /specs raised: {}".format(exc))
            return None
        if resp.status_code != 200:
            self.log.warning(
                "get_cluster_node_count GET /specs non-200 for {}: {}".format(
                    cluster_id, resp.status_code))
            return None
        # Same response-shape handling as trigger_fusion_rebalance:
        # {"data": {"specs": [...]}} | {"specs": [...]} | [...] | {...}
        raw = resp.json()
        if isinstance(raw, dict) and "data" in raw:
            raw = raw["data"]
        if isinstance(raw, dict) and "specs" in raw:
            raw = raw["specs"]
        if isinstance(raw, dict):
            raw = [raw]
        if not isinstance(raw, list) or not raw:
            return None
        for spec in raw:
            services = spec.get("services", [])
            if services and isinstance(services[0], dict):
                services = [s.get("type") or s.get("key") for s in services]
            if "kv" in services or "data" in services or not services:
                return spec.get("count")
        return raw[0].get("count")

    def wait_for_rebalance_complete(self, cluster_id, project_id=None):
        """Poll until cluster returns to healthy after rebalance."""
        project_id = project_id or self.project_id
        deadline = time.time() + self.rebalance_timeout

        self.log.info(
            "Waiting for rebalance to complete on cluster {}".format(cluster_id))
        time.sleep(30)

        cp_errors = 0
        while time.time() < deadline:
            # Resilient to a flaky CP: transient errors (wrapper sys.exit / 5xx /
            # network) don't burn the rebalance budget; a sustained outage gives
            # up (returns False) rather than eating the whole timeout.
            outcome, payload = self._cp_poll(
                self.capellaAPI.cluster_ops_apis.fetch_cluster_info,
                self.organisation_id, project_id, cluster_id)
            if outcome in ("transient", "notfound"):
                cp_errors += 1
                if cp_errors >= self._cp_error_limit:
                    self.log.error(self._cp_giveup_msg(
                        outcome, "cluster {}".format(cluster_id), cp_errors)
                        + "; last: {}".format(payload))
                    return False
                deadline += self._cp_poll_interval
                time.sleep(self._cp_poll_interval)
                continue
            cp_errors = 0
            if outcome == "rate":
                self.handle_rate_limit(int(payload.headers["Retry-After"]))
                continue
            state = payload.json().get("currentState", "")
            self.log.info(
                "Cluster {} state: {}".format(cluster_id, state))
            if state == self.CLUSTER_HEALTHY:
                return True
            if state in ["rebalanceFailed", "deploymentFailed", "scaleFailed"]:
                self.log.error(
                    "Rebalance/scale failed on cluster {}: {} — failing fast "
                    "(terminal state)".format(cluster_id, state))
                return False
            time.sleep(self._cp_poll_interval)

        self.log.error(
            "Rebalance did not complete within {}s".format(
                self.rebalance_timeout))
        return False

    def regenerate_guest_volumes_via_rebalance(self, cluster_id,
                                               project_id=None, aws_util=None):
        """Restore brings back KV primary data but does NOT re-attach guest
        volumes from their snapshots — guest volumes regenerate on the next
        Fusion rebalance. Trigger that rebalance, wait for it, and return the
        per-node guest-volume map ({node_id: [vol_ids]}; empty if AWS creds
        aren't set). aws_util defaults to the source util; pass the target
        region's util for a cross-region restore."""
        project_id = project_id or self.project_id
        aws_util = aws_util or self.fusion_aws_util
        self.log.info(
            "Post-restore: triggering a Fusion rebalance on {} to regenerate "
            "guest volumes (restore restores primary data only)".format(
                cluster_id))
        self.trigger_fusion_rebalance(cluster_id, project_id=project_id)
        if not self.wait_for_rebalance_complete(cluster_id,
                                                project_id=project_id):
            self.fail(
                "Post-restore Fusion rebalance did not complete on {}".format(
                    cluster_id))
        if not aws_util:
            return {}
        return self._settle_guest_volumes(cluster_id, aws_util)

    def _settle_guest_volumes(self, cluster_id, aws_util, timeout=None):
        """Return the per-node guest-volume map, tolerating attach/tag lag.

        Guest volumes attach and get tagged a little AFTER a Fusion rebalance
        reports the cluster healthy, so a single immediate EC2 read races them
        and yields a spurious 0 (or a partial count that varies run-to-run,
        e.g. 0/1/3). Poll until the detected count is nonzero AND stable across
        two consecutive reads, or until timeout. A genuine 0 (no volumes were
        created) is still returned after the timeout, so real failures are not
        masked — just confirmed. 'unattached' is excluded from the map."""
        timeout = timeout or int(
            self.input.param("guest_vol_settle_timeout", 300))
        deadline = time.time() + timeout
        last_total = -1
        guest = {}
        while time.time() < deadline:
            try:
                guest = aws_util.get_guest_volumes_for_cluster(cluster_id)
            except NotImplementedError:
                return {}
            except BaseException as exc:
                self.log.warning(
                    "guest-volume read on {} errored: {} — retrying".format(
                        cluster_id, exc))
                time.sleep(15)
                continue
            attached = {n: v for n, v in guest.items() if n != "unattached"}
            total = sum(len(v) for v in attached.values())
            if total > 0 and total == last_total:
                break
            if total != last_total:
                self.log.info(
                    "guest-volume count on {} = {} (waiting for it to settle "
                    "nonzero)".format(cluster_id, total))
            last_total = total
            time.sleep(15)
        return {n: sorted(v) for n, v in guest.items() if n != "unattached"}

    def _delete_unhealthy_clusters(self):
        """At tearDown, delete EVERY known cluster that is NOT healthy — this
        test's source/target AND every cluster still in the reuse pool — so
        unhealthy clusters never accumulate across the run. Health is read with
        a couple of retries so a single transient API blip (500/network)
        doesn't trigger a false delete; if it still does not read back healthy,
        the cluster is torn down. Healthy clusters are left for reuse. Returns
        the set of deleted cluster ids (also evicted from the reuse pool)."""
        deleted = set()
        seen = set()
        # Candidates = this test's source/target + every pooled cluster (from
        # earlier tests) + anything this test created. Sweeping the whole pool
        # each teardown means an unhealthy cluster is reaped in the very next
        # teardown, not left until the end of the matrix.
        candidates = [(self.source_cluster_id, self.source_project_id),
                      (self.target_cluster_id, self.target_project_id)]
        for entries in type(self)._cluster_pool.values():
            for e in entries:
                candidates.append((e.get("id"), e.get("project_id")))
        for cid in list(self._clusters_created):
            candidates.append((cid, self.project_id))
        for cid, proj in candidates:
            proj = proj or self.project_id
            if not cid or cid in seen:
                continue
            seen.add(cid)
            state = None
            for attempt in range(3):
                try:
                    info = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
                        self.organisation_id, proj, cid)
                    if info.status_code == 200:
                        state = (info.json().get("currentState") or "").lower()
                        break
                except BaseException:
                    pass
                if attempt < 2:
                    time.sleep(10)
            if state == self.CLUSTER_HEALTHY:
                continue
            self.log.warning(
                "tearDown: cluster {} not healthy (state={!r}) — deleting so "
                "the next test deploys a fresh one".format(cid, state))
            ok = False
            try:
                resp = self.capellaAPI.cluster_ops_apis.delete_cluster(
                    self.organisation_id, proj, cid)
                ok = getattr(resp, "status_code", 202) in (200, 202, 204, 404)
                if not ok:
                    self.log.warning(
                        "tearDown: delete cluster {} returned {}: {}".format(
                            cid, resp.status_code, resp.content))
            except BaseException as exc:
                self.log.warning(
                    "tearDown: delete unhealthy cluster {} raised {}".format(
                        cid, exc))
            if not ok:
                # Delete failed (e.g. mid-rebalance / API blip). Do NOT forget
                # it — keep it tracked so the next teardown retries, and flag it
                # loudly (it's also in the ledger) so the id is never lost.
                self.log.critical(
                    "LEAKED cluster {} — delete failed, kept tracked for retry "
                    "(also in the ledger; cleanup_fusion_clusters.py can reap "
                    "it)".format(cid))
                continue
            deleted.add(cid)
            if cid in self._clusters_created:
                self._clusters_created.remove(cid)
            for entries in type(self)._cluster_pool.values():
                entries[:] = [e for e in entries if e.get("id") != cid]
            self._acquired_pool_entries = [
                e for e in self._acquired_pool_entries if e.get("id") != cid]
        return deleted

    def run_backup_restore_case(self, *, source_fusion_enabled,
                                source_has_guest_volumes,
                                target_fusion_enabled,
                                target_has_guest_volumes=False,
                                target_transition=None,
                                same_cluster=False,
                                expect_target_enabled,
                                check_pre_existing_gv_deleted=None,
                                cross_region=False,
                                cross_region_backup=False):
        """Run one backup/restore matrix case end-to-end and validate it.

        cross_region_backup: at snapshot time, copy the backup to the alternate
            region (DR / "enable cross-region backups"), verify the copy landed
            there, then provision the target in that region and restore from the
            copied backup — mirroring the cross-region restore flow but with the
            backup explicitly pre-positioned in the other region. Implies the
            target is in the alternate region.

        source_fusion_enabled / target_fusion_enabled: provision each cluster in
            that fusion state.
        source_has_guest_volumes: rebalance the source so it has guest volumes
            (and shard data in the fusion S3 bucket) before the backup.
        target_has_guest_volumes: preload + rebalance the target so it has its
            OWN guest volumes before restore; those are captured and asserted
            deleted by the restore.
        target_transition: None | "enabling" | "disabling" — fire enable/disable
            on the target and DO NOT wait, then restore while the target is still
            mid-transition (tests the operation against an in-flight state).
        same_cluster: restore into the source cluster itself (self-cluster).
        expect_target_enabled: expected target fusion state after restore.
        check_pre_existing_gv_deleted: defaults to target_has_guest_volumes.
        """
        if check_pre_existing_gv_deleted is None:
            check_pre_existing_gv_deleted = target_has_guest_volumes
        free_to = int(self.input.param("fusion_free_timeout", 1800))

        # cross_region (restore) OR cross_region_backup: keep the source in the
        # chosen aws_region (reused from the pool) and put the target in the
        # alternate region. The target's guest-volume/S3 checks then run against
        # the target region's AWS util. Same-region (default) leaves target util
        # == source util.
        remote_target = cross_region or cross_region_backup
        self._target_region = (self._alternate_region(self.aws_region)
                               if remote_target else self.aws_region)
        self._target_aws_util = self._aws_util_for_region(self._target_region)
        # cross_region_backup copies the snapshot to the alternate region at
        # backup time, then restores from that copy there.
        backup_copy_regions = [self._target_region] if cross_region_backup \
            else None
        if remote_target:
            self.log.info(
                "{}: source region {}, target region {}".format(
                    "Cross-region backup" if cross_region_backup
                    else "Cross-region restore",
                    self.aws_region, self._target_region))

        # --- Source ---
        self.log.info("=== Step 1: Provisioning source cluster (fusion={}) "
                      "===".format(source_fusion_enabled))
        if not self.source_cluster_id:
            self.source_cluster_id, self.source_project_id = (
                self.acquire_cluster(
                    fusion_enabled=source_fusion_enabled,
                    num_nodes=self.source_num_nodes,
                    name_prefix="TAF_FusionSrc",
                    # A guest-volume source is mutated by its per-test fusion
                    # rebalance (Step 3), so it can't be reused — provision it
                    # fresh and delete it after this test.
                    fresh=source_has_guest_volumes))
        else:
            if not self.wait_for_deployment(
                    self.source_project_id, self.source_cluster_id):
                self.fail("Source cluster not healthy at start")
            if source_fusion_enabled:
                self._enable_fusion_and_wait(
                    self.source_cluster_id, self.source_project_id)

        self.log.info("=== Step 2: Populating source data ===")
        self.populate_source_buckets()

        if source_has_guest_volumes:
            self.log.info("=== Step 3: Source Fusion rebalance (guest "
                          "volumes) ===")
            self._source_original_nodes, _ = self.trigger_fusion_rebalance(
                self.source_cluster_id, project_id=self.source_project_id)
            if not self.wait_for_rebalance_complete(
                    self.source_cluster_id, project_id=self.source_project_id):
                self.fail("Source Fusion rebalance did not complete")
            # Record the source guest-volume count at backup time, mapped by
            # node — this is what the count-mismatch and node-mapping
            # snapshot_verification tests asserted; logged here so every
            # source-with-guest-volumes matrix case carries the same evidence.
            if self.fusion_aws_util:
                # Settle-poll: guest volumes attach/tag a little after the
                # rebalance reports healthy, so a single read can race them.
                sg = self._settle_guest_volumes(
                    self.source_cluster_id, self.fusion_aws_util)
                by_node = {n: len(v) for n, v in sg.items()
                           if n != "unattached"}
                self._source_guest_vol_count = sum(by_node.values())
                self.log.info(
                    "Source guest volumes at backup: {} total, by node: "
                    "{}".format(self._source_guest_vol_count, by_node))

        self.log.info("=== Step 4: Creating snapshot backup ===")
        backup_id = self.trigger_snapshot_backup(
            self.source_cluster_id, project_id=self.source_project_id,
            copy_regions=backup_copy_regions)
        self._last_backup_id = backup_id
        backup_record = self.wait_for_backup_complete(
            backup_id, self.source_cluster_id,
            project_id=self.source_project_id)
        if backup_copy_regions:
            self.assert_backup_copied_to_regions(
                backup_record, backup_copy_regions)

        # --- Target ---
        if same_cluster:
            self.target_cluster_id = self.source_cluster_id
            self.target_project_id = self.source_project_id
            self.log.info("=== Step 5: Self-cluster restore — target = "
                          "source {} ===".format(self.source_cluster_id))
        else:
            self.log.info("=== Step 5: Provisioning target cluster (fusion={}) "
                          "===".format(target_fusion_enabled))
            if not self.target_cluster_id:
                self.target_cluster_id, self.target_project_id = (
                    self.acquire_cluster(
                        fusion_enabled=target_fusion_enabled,
                        num_nodes=self.target_num_nodes,
                        name_prefix="TAF_FusionTgt",
                        region=self._target_region))
            else:
                if not self.wait_for_deployment(
                        self.target_project_id, self.target_cluster_id):
                    self.fail("Target cluster not healthy at start")
                if target_fusion_enabled:
                    self._enable_fusion_and_wait(
                        self.target_cluster_id, self.target_project_id)
            self._target_original_nodes = self.get_cluster_node_count(
                self.target_cluster_id, self.target_project_id)

        # --- Pre-existing guest volumes on the target ---
        pre_guest_vols = set()
        if same_cluster and source_has_guest_volumes and self.fusion_aws_util:
            g = self._settle_guest_volumes(
                self.source_cluster_id, self.fusion_aws_util)
            for k, v in g.items():
                if k != "unattached":
                    pre_guest_vols.update(v)
        elif target_has_guest_volumes and not same_cluster:
            self.log.info("=== Step 6: Preload target + rebalance (target "
                          "guest volumes) ===")
            _pre_bucket, pre_guest_vols = self.preload_target(
                rebalance=True, aws_util=self._target_aws_util)
        self.log.info("Target has {} pre-existing guest volume(s) before "
                      "restore".format(len(pre_guest_vols)))

        # --- Drive the target into a transitional state (fire, do NOT wait) ---
        if target_transition == "enabling":
            self.log.info("=== Step 7: Trigger ENABLE on target and restore "
                          "while still enabling (no wait) ===")
            self.enable_fusion_on_cluster(
                self.target_cluster_id, self.target_project_id)
        elif target_transition == "disabling":
            self.log.info("=== Step 7: Trigger DISABLE on target and restore "
                          "while still disabling (no wait) ===")
            self.disable_fusion_on_cluster(
                self.target_cluster_id, self.target_project_id)

        # --- Restore ---
        self.log.info("=== Step 8: Restoring backup into target ===")
        self.trigger_restore(
            backup_id=backup_id, target_cluster_id=self.target_cluster_id,
            project_id=self.target_project_id)
        self.wait_for_restore_complete(
            self.target_cluster_id, project_id=self.target_project_id,
            expected_bucket_names=self.source_bucket_names)

        # --- Validations ---
        self.log.info("=== Step 9: Verifying data integrity ===")
        self.verify_data_integrity()

        # Every restore must leave a clean target: scan the restored cluster's
        # memcached logs (and crash dir) for CRITICAL errors / core dumps and
        # fail if any are found.
        self._scan_memcached_logs_after_restore(
            self.target_cluster_id, self._target_aws_util)

        if check_pre_existing_gv_deleted and pre_guest_vols:
            self.log.info("=== Step 10: Verifying pre-existing target guest "
                          "volumes deleted by restore ===")
            self.assert_guest_volumes_deleted(
                self.target_cluster_id, pre_guest_vols, timeout=free_to,
                aws_util=self._target_aws_util)

        if expect_target_enabled and (source_has_guest_volumes or remote_target):
            # Restoring a GUEST-VOLUME backup is a "mid-migration" restore: per
            # the CP team, in ANY region it does NOT create fusion accelerator
            # nodes — the restore attaches guest volumes to the active nodes,
            # completes the restore, then a queued teardown job destroys them
            # once the background migration finishes. So a target restored from
            # a guest-volume backup legitimately ends with 0 persistent guest
            # volumes (they exist only transiently mid-migration). We therefore
            # validate the restore by DATA INTEGRITY (Step 9), not by
            # guest-volume presence. (A non-guest-volume source is a normal
            # backup — that path below still regenerates + asserts guest
            # volumes, since the target builds its own.)
            self.log.info(
                "=== Step 11: guest-volume (mid-migration) restore — target "
                "guest volumes are torn down post-migration by design; "
                "validated via data integrity above, skipping guest-volume "
                "presence check ===")
        elif expect_target_enabled:
            self.log.info("=== Step 11: Verifying target is Fusion-enabled "
                          "(guest volumes regenerate on rebalance) ===")
            if self._target_aws_util:
                node_vols = self.regenerate_guest_volumes_via_rebalance(
                    self.target_cluster_id, self.target_project_id,
                    aws_util=self._target_aws_util)
                target_total = sum(len(v) for v in node_vols.values())
                # Node-mapping + count-mismatch evidence (folded from the
                # snapshot_verification node_mapping / count_mismatch tests):
                # log the per-node guest-volume distribution and compare the
                # target's regenerated count against the source's at backup.
                self.log.info(
                    "Target guest volumes after post-restore rebalance: {} "
                    "total, by node: {}".format(
                        target_total,
                        {n: len(v) for n, v in node_vols.items()}))
                if self._source_guest_vol_count is not None:
                    self.log.info(
                        "Guest-volume counts — source@backup={}, target "
                        "pre-existing={}, target regenerated={}".format(
                            self._source_guest_vol_count,
                            len(pre_guest_vols), target_total))
                if target_total == 0:
                    self.fail(
                        "Target expected Fusion-enabled but 0 guest volumes "
                        "after a post-restore rebalance")
        else:
            self.log.info("=== Step 11: Verifying target is Fusion-free (no "
                          "guest volumes, S3 empty/absent) ===")
            self.assert_fusion_free_after_restore(
                self.target_cluster_id, project_id=self.target_project_id,
                timeout=free_to, aws_util=self._target_aws_util)

        self._test_succeeded = True

    def trigger_snapshot_backup(self, cluster_id, project_id=None,
                                copy_regions=None):
        """POST cloudsnapshotbackups and return the backup_id.

        copy_regions: when set, request the snapshot be copied to those
        additional region(s) at backup time (cross-region backup / DR). The
        request body field carrying the region list is configurable via the
        ``backup_copy_regions_field`` param (default ``copyToRegions``) so the
        exact API contract can be corrected without a code change; the copy is
        then verified against the backup record (see run_backup_restore_case),
        so a wrong field name fails loudly rather than silently no-op'ing."""
        project_id = project_id or self.project_id

        # The backup RECORD is the same object on both APIs, so we always
        # list/wait via the public v4 endpoint (below and in
        # wait_for_backup_complete).
        v4_endpoint = (
            "/v4/organizations/{}/projects/{}/clusters/{}"
            "/cloudsnapshotbackups".format(
                self.organisation_id, project_id, cluster_id))

        body = {}
        if copy_regions:
            # Confirmed contract: {"copyToRegions": [...], "retention": N}.
            field = self.input.param(
                "backup_copy_regions_field", "copyToRegions")
            body[field] = list(copy_regions)
            body["retention"] = int(self.input.param("backup_retention", 30))

        if copy_regions:
            # copyToRegions is an INTERNAL v2 feature: the public v4 schema
            # rejects it (400 "unknown key copyToRegions") and the public host
            # has no /v2 route (404 nginx). Go through the internal API — the
            # same transport trigger_fusion_rebalance uses for /specs.
            endpoint = (
                "{}/v2/organizations/{}/projects/{}/clusters/{}"
                "/cloudsnapshotbackups".format(
                    self.capellaAPI.internal_url,
                    self.organisation_id, project_id, cluster_id))
            self.log.info(
                "Triggering cross-region cloud snapshot backup for cluster {} "
                "— copy to {} via internal API (body={})".format(
                    cluster_id, copy_regions, body))
            resp = self.capellaAPI.do_internal_request(
                endpoint, method="POST", params=json.dumps(body))
        else:
            self.log.info(
                "Triggering cloud snapshot backup for cluster {}".format(
                    cluster_id))
            resp = self.capellaAPI.cluster_ops_apis.api_post(v4_endpoint, body)
            if resp.status_code == 429:
                self.handle_rate_limit(int(resp.headers["Retry-After"]))
                resp = self.capellaAPI.cluster_ops_apis.api_post(
                    v4_endpoint, body)

        if resp.status_code not in [200, 201, 202]:
            self.fail(
                "Cloud snapshot backup request failed for cluster {}: "
                "{} {}".format(cluster_id, resp.status_code, resp.content))

        try:
            backup_id = resp.json().get("id") or resp.json().get("backupID")
        except (ValueError, AttributeError):
            backup_id = None
        if not backup_id:
            deadline = time.time() + 120
            while time.time() < deadline:
                list_resp = self.capellaAPI.cluster_ops_apis.api_get(
                    v4_endpoint)
                if list_resp.status_code == 200:
                    records = list_resp.json().get("data", [])
                    if records:
                        records.sort(
                            key=lambda r: r.get("createdAt", ""),
                            reverse=True)
                        backup_id = records[0]["id"]
                        break
                time.sleep(10)
            if not backup_id:
                self.fail(
                    "Could not retrieve backup ID for cluster {}".format(
                        cluster_id))

        self.log.info(
            "Cloud snapshot backup {} created for cluster {}".format(
                backup_id, cluster_id))
        return backup_id

    def wait_for_backup_complete(self, backup_id, cluster_id, project_id=None):
        """Poll cloudsnapshotbackups until the backup completes (GET-by-id
        returns 405 on this API, so we list and match by id).
        """
        project_id = project_id or self.project_id
        deadline = time.time() + self.backup_timeout
        terminal_failure_states = (
            "failed", "error", "cancelled", "canceled", "aborted")

        # The backup record is visible on the public v4 list regardless of
        # whether it was created via v4 or the internal v2 (cross-region).
        list_all = (
            "/v4/organizations/{}/projects/{}/clusters/{}"
            "/cloudsnapshotbackups".format(
                self.organisation_id, project_id, cluster_id))

        self.log.info(
            "Waiting for backup {} progress to reach 100".format(backup_id))

        last_progress_logged = -1
        last_full_log = 0

        while time.time() < deadline:
            resp = self.capellaAPI.cluster_ops_apis.api_get(list_all)
            if resp.status_code == 429:
                self.handle_rate_limit(int(resp.headers["Retry-After"]))
                continue
            if resp.status_code != 200:
                self.log.warning(
                    "Backup poll non-200: {} {}".format(
                        resp.status_code,
                        resp.content[:300] if resp.content else ""))
                time.sleep(15)
                continue

            record = None
            for r in resp.json().get("data", []):
                if r.get("id") == backup_id:
                    record = r
                    break
            if not record:
                time.sleep(15)
                continue

            raw_progress = record.get("progress", 0)
            # progress may be an int (0-100) or a status dict {status, time}.
            if isinstance(raw_progress, dict):
                progress_status = raw_progress.get("status", "")
                progress_pct = 100 if progress_status == "complete" else 0
            else:
                try:
                    progress_pct = int(raw_progress)
                except (TypeError, ValueError):
                    progress_pct = 0
                progress_status = (
                    "complete" if progress_pct >= 100 else "processing")

            now = time.time()
            if progress_pct != last_progress_logged or progress_status != getattr(
                    self, "_last_backup_status", None):
                self.log.info(
                    "Backup {} progress: {}% (status={})".format(
                        backup_id, progress_pct, progress_status))
                last_progress_logged = progress_pct
                self._last_backup_status = progress_status
            if now - last_full_log > 60:
                self.log.info(
                    "Backup {} full record: {}".format(backup_id, record))
                last_full_log = now

            # Fail fast on terminal failure states so the test doesn't burn
            # the full backup_timeout waiting on a backup that will never
            # progress. Check status object, top-level status field, and
            # the record's currentState as fallbacks.
            record_state = (
                record.get("currentState")
                or record.get("status")
                or progress_status
                or "").lower()
            if record_state in terminal_failure_states:
                self.fail(
                    "Backup {} entered terminal failure state '{}' — "
                    "record: {}".format(backup_id, record_state, record))

            if progress_pct >= 100 or progress_status == "complete":
                self.log.info(
                    "Backup {} complete — record: {}".format(
                        backup_id, record))
                return record
            time.sleep(20)

        self.fail(
            "Backup {} did not reach 100% within {}s".format(
                backup_id, self.backup_timeout))

    def assert_backup_copied_to_regions(self, record, regions):
        """Verify a cross-region backup was actually copied to each requested
        region by inspecting the completed backup record. The exact schema for
        the copied-region list isn't fixed, so we scan the known candidate
        fields AND fall back to a substring match on the whole record — either
        way, a region we asked to copy to that does NOT appear means the copy
        did not happen (e.g. a wrong request field), and we fail loudly with the
        full record so the real contract is visible."""
        blob = json.dumps(record).lower()
        candidates = ("copyregions", "regions", "copiedregions",
                      "crossregioncopies", "replicaregions", "copies")
        present = []
        for k in record.keys():
            if k.lower() in candidates:
                present.append((k, record[k]))
        self.log.info(
            "Cross-region backup {}: region fields on record = {}".format(
                record.get("id"), present or "none found by name"))
        missing = [r for r in regions if r.lower() not in blob]
        if missing:
            self.fail(
                "Cross-region backup copy not reflected for region(s) {} — the "
                "backup was not copied there (check the request field / API "
                "contract). Full backup record: {}".format(missing, record))
        self.log.info(
            "Cross-region backup {} confirmed copied to region(s) {}".format(
                record.get("id"), regions))

    def trigger_restore(self, backup_id, target_cluster_id, project_id=None):
        """Restore a snapshot backup into target_cluster_id via the v4
        /clusters/{TARGET}/cloudsnapshotbackups/{id}/restore endpoint
        (source cluster is already linked to the backup record).
        Returns the restore job id, or None if the response omits it.
        """
        project_id = project_id or self.project_id

        if not target_cluster_id:
            self.fail(
                "trigger_restore needs target_cluster_id — the v4 /restore "
                "endpoint requires a pre-existing target cluster in the URL.")

        endpoint = (
            "/v4/organizations/{}/projects/{}/clusters/{}"
            "/cloudsnapshotbackups/{}/restore".format(
                self.organisation_id, project_id,
                target_cluster_id, backup_id))
        body = {}

        self.log.info(
            "Restoring backup {} into target cluster {} "
            "(POST {} body={})".format(
                backup_id, target_cluster_id, endpoint, body))

        resp = self.capellaAPI.cluster_ops_apis.api_post(endpoint, body)
        if resp.status_code == 429:
            self.handle_rate_limit(int(resp.headers["Retry-After"]))
            resp = self.capellaAPI.cluster_ops_apis.api_post(endpoint, body)
        if resp.status_code not in [200, 201, 202]:
            self.fail(
                "cloudsnapshotbackups restore failed: {} {} (body sent: {})".format(
                    resp.status_code, resp.content, body))

        try:
            body_json = resp.json() or {}
        except Exception:
            body_json = {}
        restore_id = (body_json.get("restoreId")
                      or body_json.get("id")
                      or body_json.get("jobId"))
        self.log.info(
            "Restore job started on target {}: restoreId={}".format(
                target_cluster_id, restore_id))
        return restore_id

    def wait_for_restore_complete(self, target_cluster_id, project_id=None,
                                  expected_bucket_names=None):
        """Wait until target cluster is healthy after restore, then verify buckets."""
        project_id = project_id or self.project_id
        self.log.info(
            "Waiting for restore to complete on cluster {}".format(
                target_cluster_id))

        time.sleep(30)
        self._wait_for_cluster_healthy(
            target_cluster_id, project_id, timeout=self.restore_timeout)

        if expected_bucket_names:
            # Use restore_timeout, not a short fixed wait: a cross-region
            # restore does an async cross-region data transfer that can take
            # several minutes before the buckets appear (the cluster reaches
            # 'healthy' before the transfer completes). Same-region buckets
            # still appear in seconds, so this returns immediately there.
            deadline = time.time() + self.restore_timeout
            cp_errors = 0
            while time.time() < deadline:
                # Resilient to a flaky CP: transient errors don't burn the
                # (long, cross-region) wait budget, and a sustained outage fails
                # fast with a clear message instead of a misleading "buckets not
                # found".
                outcome, payload = self._cp_poll(
                    self.capellaAPI.cluster_ops_apis.list_buckets,
                    self.organisation_id, project_id, target_cluster_id)
                if outcome in ("transient", "notfound"):
                    cp_errors += 1
                    if cp_errors >= self._cp_error_limit:
                        self.fail(self._cp_giveup_msg(
                            outcome, "target {}".format(target_cluster_id),
                            cp_errors) + "; last: {}".format(payload))
                    deadline += self._cp_poll_interval
                    time.sleep(self._cp_poll_interval)
                    continue
                cp_errors = 0
                if outcome == "rate":
                    self.handle_rate_limit(int(payload.headers["Retry-After"]))
                    continue
                present = {
                    b["name"]
                    for b in payload.json().get("data", [])}
                if all(n in present for n in expected_bucket_names):
                    self.log.info(
                        "All expected buckets present on target {}".format(
                            target_cluster_id))
                    return
                self.log.info(
                    "Buckets present: {} (waiting for: {})".format(
                        present, expected_bucket_names))
                time.sleep(self._cp_poll_interval)
            self.fail(
                "Expected buckets {} not found on target {} after restore".format(
                    expected_bucket_names, target_cluster_id))

    def start_s3_cleanup_monitor(self, bucket_name, poll_interval=30):
        """Poll S3 object count in a background thread while restore runs.

        Returns (thread, stop_event, counts) where counts is a list of
        (timestamp, object_count) tuples appended by the thread.
        count == -1 means the bucket did not exist yet at that sample.
        """
        stop_event = threading.Event()
        counts = []

        def _poll():
            # Swallow per-iteration errors so a transient S3 / network blip
            # doesn't kill the thread and truncate the time-series Step 11
            # analyses.
            while not stop_event.is_set():
                try:
                    count = self.fusion_aws_util.s3.count_objects(bucket_name)
                    counts.append((time.time(), count))
                    if count == -1:
                        self.log.info(
                            "S3 monitor: bucket '{}' not found yet".format(
                                bucket_name))
                    else:
                        self.log.info(
                            "S3 monitor: bucket '{}' object count = {}".format(
                                bucket_name, count))
                except Exception as exc:
                    self.log.warning(
                        "S3 monitor: s3.count_objects raised {}: {}".format(
                            type(exc).__name__, exc))
                stop_event.wait(poll_interval)

        t = threading.Thread(
            target=_poll, daemon=True, name="s3-cleanup-monitor")
        t.start()
        return t, stop_event, counts

    def stop_s3_cleanup_monitor(self, thread, stop_event, timeout=60):
        """Signal the S3 monitor thread to stop and wait for it to exit."""
        stop_event.set()
        thread.join(timeout=timeout)
        if thread.is_alive():
            self.log.warning(
                "S3 monitor thread did not exit within {}s".format(timeout))
