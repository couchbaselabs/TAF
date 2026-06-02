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

import requests

from global_vars import logger
from TestInput import TestInputSingleton
from pytests.Capella.RestAPIv4.api_base import APIBase
from pytests.aGoodDoctor.fusion.fusion_aws_util import FusionAWSUtil
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
        self.deploy_timeout = int(_p("deploy_timeout", 1800))      # 30 min
        self.backup_timeout = int(_p("backup_timeout", 3600))      # 1 hour
        self.restore_timeout = int(_p("restore_timeout", 3600))    # 1 hour
        # Scaling/rebalance must not exceed 30 min.
        self.rebalance_timeout = int(_p("rebalance_timeout", 1800))  # 30 min
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
        self._target_bucket_ids = []
        self._source_original_nodes = None
        self._target_original_nodes = None
        # Name of the target's own pre-load bucket (its non-empty-target data),
        # preserved across runs for cluster reuse.
        self._target_preload_bucket = None
        # Track the backup created by this test (if any) so teardown can delete
        # it. preset_backup_id is NOT deleted — it belongs to another run.
        self._last_backup_id = None

        # Make Fusion available in the tenant (8.1.0 + the FUSION_* flags).
        # Per-cluster Fusion-on/off is decided by express-scaling enable, not
        # these flags, so we set them once here and never toggle per cluster.
        self._set_tenant_feature_flags()

        self.fusion_aws_util = None
        if self.aws_access_key and self.aws_secret_key:
            self.fusion_aws_util = FusionAWSUtil(
                self.aws_access_key, self.aws_secret_key,
                region=self.aws_region)

    def tearDown(self):
        failures = []

        # Bail early if setUp raised before initializing internal state.
        if not hasattr(self, "_clusters_created"):
            APIBase.tearDown(self)
            return

        will_delete = (set(self._clusters_created)
                       if not self.preserve_clusters else set())

        passed = getattr(self, "_test_succeeded", False)

        # Always delete the backup this test created — on success AND failure —
        # so backups never leak across runs. A preset backup_id belongs to
        # another run and is left alone (handled inside the helper).
        self._delete_last_backup()

        if not passed:
            # The test failed/errored. Skip the remaining cleanup so buckets,
            # loaded data and (possibly wedged) clusters are left intact for
            # debugging. Evict acquired clusters from the pool so a later test
            # doesn't reuse a half-broken cluster, then defer to APIBase.tearDown.
            self.log.warning(
                "tearDown: test did not succeed — preserving buckets and "
                "clusters for debugging (no scale-down). Source bucket(s): {} "
                "on {}; target bucket(s): {} on {}.".format(
                    self.source_bucket_ids, self.source_cluster_id,
                    self._target_bucket_ids, self.target_cluster_id))
            if self._pooling:
                self._release_pooled_clusters(reusable=False)
            if self._preset_project_id:
                self.project_id = None
            APIBase.tearDown(self)
            return

        # Buckets are NOT deleted here — they are kept for reuse on the next run.
        # When a fresh bucket is needed, the stale same-prefix bucket is deleted
        # first at create time (see _delete_buckets_with_prefix), so buckets
        # never accumulate. Only the backup (above) is always removed.

        scale_targets = [
            (self.source_cluster_id,
             self.source_project_id or self.project_id,
             self._source_original_nodes,
             "source"),
            (self.target_cluster_id,
             self.target_project_id or self.project_id,
             self._target_original_nodes,
             "target"),
        ]
        for cl_id, proj_id, orig_nodes, label in scale_targets:
            if not cl_id or not orig_nodes or cl_id in will_delete:
                continue
            try:
                # Don't try to scale a cluster that isn't healthy — a wedged or
                # mid-operation cluster (e.g. a stalled restore) would otherwise
                # make wait_for_rebalance_complete spin until rebalance_timeout.
                state = None
                try:
                    info = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
                        self.organisation_id, proj_id, cl_id)
                    if info.status_code == 200:
                        state = (info.json().get("currentState") or "").lower()
                except Exception:
                    pass
                if state != self.CLUSTER_HEALTHY:
                    self.log.warning(
                        "tearDown: {} cluster {} not healthy (state={!r}) — "
                        "skipping scale-down".format(label, cl_id, state))
                    continue
                # Only scale back down if the cluster is currently larger than
                # its recorded baseline — covers the target being grown by the
                # restore as well as the source's explicit scale-up, and skips
                # a wasteful no-op rebalance when nothing changed.
                current_nodes = self.get_cluster_node_count(cl_id, proj_id)
                if current_nodes is not None and current_nodes <= orig_nodes:
                    self.log.info(
                        "tearDown: {} cluster {} already at {} nodes "
                        "(baseline {}) — no scale-down needed".format(
                            label, cl_id, current_nodes, orig_nodes))
                    continue
                self.log.info(
                    "tearDown: Scaling {} cluster {} back to {} nodes "
                    "(from {})".format(
                        label, cl_id, orig_nodes, current_nodes))
                self.trigger_fusion_rebalance(
                    cl_id, project_id=proj_id, target_nodes=orig_nodes)
                if not self.wait_for_rebalance_complete(
                        cl_id, project_id=proj_id):
                    self.log.warning(
                        "tearDown: scale-down of {} cluster {} did not "
                        "complete cleanly".format(label, cl_id))
            except Exception as exc:
                self.log.warning(
                    "tearDown: could not scale down {} cluster {}: {}".format(
                        label, cl_id, exc))

        for cluster_id, user_id in self._db_users_to_cleanup.items():
            resp = self.capellaAPI.cluster_ops_apis.delete_database_user(
                self.organisation_id, self.project_id, cluster_id, user_id)
            if resp.status_code not in [200, 202, 204, 404]:
                failures.append(
                    "Could not delete DB user {} on cluster {}".format(
                        user_id, cluster_id))

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
        """Pooled clusters are preserved across the suite run (pooling only
        runs with preserve_clusters True). Log them so they can be reused on a
        subsequent run or cleaned up manually, then reset the in-memory pool.
        """
        pooled = [e["id"]
                  for entries in cls._cluster_pool.values()
                  for e in entries]
        if pooled:
            logger["test"].info(
                "Preserving {} pooled cluster(s) after suite run (reuse via "
                "*_cluster_id, or delete manually): {}".format(
                    len(pooled), pooled))
        cls._cluster_pool = {}
        parent = super(FusionBackupRestoreBase, cls)
        if hasattr(parent, "tearDownClass"):
            parent.tearDownClass()

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

    def provision_fusion_cluster(self, fusion_enabled=True, num_nodes=3,
                                 name_prefix="TAF_Fusion"):
        """Deploy a Capella Dedicated cluster (data-service only) and return its id."""
        cluster_name = "{}_{}".format(
            name_prefix,
            self.generate_random_string(5, special_characters=False))

        cloud_provider = {
            "type": "aws",
            "region": self.aws_region,
            "cidr": "10.0.0.0/20"
        }
        couchbase_server = {
            "version": str(self.input.param("server_version", "8.1"))
        }
        service_groups = [
            {
                "node": {
                    "compute": {"cpu": 4, "ram": 16},
                    "disk": {"storage": 100, "type": "gp3", "iops": 3000}
                },
                "numOfNodes": num_nodes,
                "services": ["data"]
            }
        ]
        availability = {"type": "multi"}
        support = {"plan": "enterprise", "timezone": "GMT"}

        self.log.info(
            "Provisioning cluster '{}' ({} nodes)".format(
                cluster_name, num_nodes))

        # select_CIDR retries only on one specific 422 wording; some tenants
        # return "... overlaps with existing resource with CIDR ..." instead,
        # which it doesn't recognize. Walk the CIDR ourselves on any CIDR-
        # related 422 until one is free.
        resp = self.select_CIDR(
            self.organisation_id, self.project_id, cluster_name,
            cloud_provider, service_groups, availability, support,
            couchbase_server)
        cidr_attempts = 0
        while (resp.status_code == 422 and cidr_attempts < 40
               and "cidr" in (resp.content or b"").decode(
                   "utf-8", "ignore").lower()):
            cidr_attempts += 1
            cloud_provider["cidr"] = self._next_cidr(cloud_provider["cidr"])
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
        self.log.info(
            "Cluster {} created, waiting for healthy state".format(cluster_id))

        if not self.wait_for_deployment(self.project_id, cluster_id):
            self.fail(
                "Cluster {} did not reach healthy state within {}s".format(
                    cluster_id, self.deploy_timeout))

        # A cluster is Fusion-enabled only if express-scaling enable is called
        # on it; a disabled cluster simply never gets that call — there is
        # nothing to "disable". Source of truth is the cp-db dataplane.clusters
        # doc (config.enableFusion / fusionState), observable via the internal
        # fusion/status endpoint — NOT the v4 `expressScaling` field (which can
        # read 'enabled' as a capability even when Fusion is off).
        if fusion_enabled:
            self.enable_fusion_on_cluster(cluster_id, self.project_id)
            self._wait_for_cluster_healthy(
                cluster_id, self.project_id, timeout=self.rebalance_timeout)

        self.log.info("Cluster {} ready (fusion_enabled={})".format(
            cluster_id, fusion_enabled))
        return cluster_id

    def acquire_cluster(self, fusion_enabled, num_nodes, name_prefix):
        """Return (cluster_id, project_id) for a cluster matching
        (fusion_enabled, num_nodes).

        When pooling is active (reuse_clusters and preserve_clusters), reuse a
        free cluster of the same spec from the class-level pool, provisioning a
        new one only if none is free; the cluster is marked in-use for this
        test and released back to the pool in tearDown so later tests in the
        same suite run reuse it instead of re-provisioning.

        When pooling is inactive, provision a fresh cluster (old behavior); the
        per-test tearDown then destroys it if preserve_clusters is False.
        """
        fusion_enabled = bool(fusion_enabled)
        num_nodes = int(num_nodes)

        if not self._pooling:
            cid = self.provision_fusion_cluster(
                fusion_enabled=fusion_enabled, num_nodes=num_nodes,
                name_prefix=name_prefix)
            return cid, self.project_id

        key = (fusion_enabled, num_nodes)
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
                self.enable_fusion_on_cluster(entry["id"], entry["project_id"])
                self._wait_for_cluster_healthy(
                    entry["id"], entry["project_id"],
                    timeout=self.rebalance_timeout)
            entry["in_use"] = True
            self._acquired_pool_entries.append(entry)
            self.log.info(
                "acquire_cluster: reusing pooled cluster {} "
                "(fusion_enabled={}, nodes={})".format(
                    entry["id"], fusion_enabled, num_nodes))
            return entry["id"], entry["project_id"]

        # No free cluster of this spec — provision and register a new one.
        self.log.info(
            "acquire_cluster: no free pooled cluster for "
            "(fusion_enabled={}, nodes={}); provisioning new".format(
                fusion_enabled, num_nodes))
        cid = self.provision_fusion_cluster(
            fusion_enabled=fusion_enabled, num_nodes=num_nodes,
            name_prefix=name_prefix)
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

    def _v2_api(self):
        """Return a v2 CapellaAPI instance for internal endpoints (feature flags etc.)."""
        return CapellaAPIv2(
            "https://" + self.url, "", "",
            self.user, self.passwd,
            self.input.capella.get("override_token", ""))

    # Tenant-scoped feature flags that make Fusion *available* in the tenant
    # (enable-eight-one-zero validates 8.1.0; the two FUSION_* flags gate
    # Fusion). They are set on once per run. They do NOT make any individual
    # cluster Fusion-enabled — that is decided purely by whether express-scaling
    # enable is called on the cluster (see enable_fusion_on_cluster). A
    # Fusion-disabled cluster is simply one we never enable express-scaling on.
    SERVER_810_FEATURE_FLAG = "enable-eight-one-zero"
    FUSION_FEATURE_FLAGS = [
        "fusion-rebalances",
        "fusion-fallback-replace",
    ]

    def _apply_tenant_feature_flag(self, v2, ff, value):
        """Create-or-update a single tenant feature flag to `value`."""
        try:
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
                if resp.status_code not in [200, 204]:
                    self.log.warning(
                        "Feature flag {}={} update returned {}: {}".format(
                            ff, value, resp.status_code, resp.content))
                else:
                    self.log.info(
                        "Feature flag {} set to {} (updated)".format(ff, value))
            else:
                self.log.warning(
                    "Feature flag {}={} returned {}: {}".format(
                        ff, value, resp.status_code, resp.content))
        except Exception as e:
            self.log.warning(
                "Could not set feature flag {}={}: {}".format(ff, value, e))

    def _set_tenant_feature_flags(self):
        """Enable the tenant feature flags that make Fusion available (8.1.0 +
        the two FUSION_* flags). Idempotent; called once in setUp. Per-cluster
        Fusion state is controlled by express-scaling enable, not these flags.
        """
        v2 = self._v2_api()
        self._apply_tenant_feature_flag(v2, self.SERVER_810_FEATURE_FLAG, True)
        for ff in self.FUSION_FEATURE_FLAGS:
            self._apply_tenant_feature_flag(v2, ff, True)

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
                s3_objs = (self.fusion_aws_util.count_s3_objects(s3)
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
            obj_count = self.fusion_aws_util.count_s3_objects(s3_bucket)
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
                                         timeout=1800):
        """Verify a Fusion-enabled target converges to Fusion-FREE after
        restoring a Fusion-DISABLED backup.

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
        if not self.fusion_aws_util:
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
                guest = self.fusion_aws_util.get_guest_volumes_for_cluster(
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
        s3_bucket = self.fusion_aws_util.find_fusion_s3_bucket(cluster_id)
        s3_objs = (self.fusion_aws_util.count_s3_objects(s3_bucket)
                   if s3_bucket else -1)
        while s3_bucket and s3_objs > 0 and time.time() < deadline:
            self.log.info(
                "Waiting for CP to drain the Fusion S3 bucket '{}' on {} "
                "({} objects)...".format(s3_bucket, cluster_id, s3_objs))
            time.sleep(30)
            s3_bucket = self.fusion_aws_util.find_fusion_s3_bucket(cluster_id)
            s3_objs = (self.fusion_aws_util.count_s3_objects(s3_bucket)
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

    def preload_target(self, rebalance):
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
            self.generate_random_string(5, special_characters=False))
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
            if self.fusion_aws_util:
                guest = self.fusion_aws_util.get_guest_volumes_for_cluster(cid)
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
                                     timeout=1800):
        """Verify the restore deleted the target's pre-existing guest volumes:
        none of pre_vol_ids may remain on the cluster. Hard fail otherwise.
        """
        if not pre_vol_ids:
            self.log.warning(
                "No pre-restore guest volumes captured — skipping deletion "
                "check on {}.".format(cluster_id))
            return
        if not self.fusion_aws_util:
            self.log.warning(
                "AWS creds not set — skipping guest-volume deletion check.")
            return
        deadline = time.time() + timeout
        remaining = set(pre_vol_ids)
        while remaining and time.time() < deadline:
            guest = self.fusion_aws_util.get_guest_volumes_for_cluster(
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

    def _wait_for_cluster_healthy(self, cluster_id, project_id, timeout=1800):
        """Poll cluster state until healthy or timeout.

        Logs the full response body once per minute when the `currentState`
        field is empty/missing — useful for catching cases where the v4
        API returns a body with a renamed field during certain ops.
        """
        deadline = time.time() + timeout
        last_state_logged = None
        last_full_log = 0
        while time.time() < deadline:
            try:
                resp = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
                    self.organisation_id, project_id, cluster_id)
            except BaseException as exc:
                self.log.warning(
                    "fetch_cluster_info transient error on {}: {} — "
                    "retrying after 15s".format(cluster_id, exc))
                time.sleep(15)
                continue
            if resp.status_code == 429:
                self.handle_rate_limit(int(resp.headers["Retry-After"]))
                continue
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
                             "restoreFailed", "rebalanceFailed"]:
                    self.fail(
                        "Cluster {} reached terminal error state: {}".format(
                            cluster_id, state))
            else:
                self.log.warning(
                    "fetch_cluster_info non-200: {} {}".format(
                        resp.status_code,
                        resp.content[:300] if resp.content else ""))
            time.sleep(15)
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
        except Exception as exc:
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

        Reuse path: with source_bucket_id set, reuse that already-loaded bucket
        (no reload). Otherwise delete any stale source bucket first, then create
        + load fresh ones (delete-old-create-new — buckets never accumulate).
        Records bucket ids/names in self.source_bucket_ids/source_bucket_names.
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

        # Delete any stale source bucket(s) first so a fresh one can be created
        # without accumulating buckets / exhausting the cluster's KV-RAM quota.
        self._delete_buckets_with_prefix(cid, proj, self.SOURCE_BUCKET_PREFIX)
        run_id = self.generate_random_string(6, special_characters=False)
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
        while time.time() < deadline:
            resp = self.capellaAPI.cluster_ops_apis.fetch_bucket_info(
                self.organisation_id, project_id, cluster_id, bucket_id)
            if resp.status_code == 429:
                self.handle_rate_limit(int(resp.headers["Retry-After"]))
                continue
            if resp.status_code == 200:
                count = resp.json().get("stats", {}).get("itemCount", 0)
                if count >= target:
                    break
            time.sleep(15)
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

    def create_fusion_bucket(self, cluster_id, bucket_name, project_id=None):
        """Create a Magma bucket on the cluster. Returns bucket_id.

        RAM quota (MB/node) is self.bucket_ram_quota — raise it (e.g. 4096) for
        large data loads; 1024 is too small to ingest tens of GB without the KV
        write buffer back-pressuring into ServerOutOfMemory (ENOMEM).
        """
        project_id = project_id or self.project_id
        ram = self.bucket_ram_quota
        resp = self.capellaAPI.cluster_ops_apis.create_bucket(
            self.organisation_id, project_id, cluster_id,
            bucket_name, "couchbase", "magma",
            ram, "seqno", "none", 1, False, 0)
        if resp.status_code == 429:
            self.handle_rate_limit(int(resp.headers["Retry-After"]))
            resp = self.capellaAPI.cluster_ops_apis.create_bucket(
                self.organisation_id, project_id, cluster_id,
                bucket_name, "couchbase", "magma",
                ram, "seqno", "none", 1, False, 0)
        if resp.status_code != 201:
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
            self.generate_random_string(6, special_characters=False))
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

        while time.time() < deadline:
            # capellaAPI's CbcAPIError raises SystemExit (a BaseException, not
            # Exception) on transient connection drops like RemoteDisconnected
            # or "Connection aborted." — catch BaseException so a single
            # network blip doesn't kill the entire polling loop.
            try:
                resp = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
                    self.organisation_id, project_id, cluster_id)
            except BaseException as exc:
                self.log.warning(
                    "fetch_cluster_info transient error on {}: {} — "
                    "retrying after 20s".format(cluster_id, exc))
                time.sleep(20)
                continue
            if resp.status_code == 429:
                self.handle_rate_limit(int(resp.headers["Retry-After"]))
                continue
            if resp.status_code == 200:
                state = resp.json().get("currentState", "")
                self.log.info(
                    "Cluster {} state: {}".format(cluster_id, state))
                if state == self.CLUSTER_HEALTHY:
                    return True
                if state in ["rebalanceFailed", "deploymentFailed"]:
                    self.log.error(
                        "Rebalance failed on cluster {}: {}".format(
                            cluster_id, state))
                    return False
            time.sleep(20)

        self.log.error(
            "Rebalance did not complete within {}s".format(
                self.rebalance_timeout))
        return False

    def regenerate_guest_volumes_via_rebalance(self, cluster_id,
                                               project_id=None):
        """Restore brings back KV primary data but does NOT re-attach guest
        volumes from their snapshots — guest volumes regenerate on the next
        Fusion rebalance. Trigger that rebalance, wait for it, and return the
        per-node guest-volume map ({node_id: [vol_ids]}; empty if AWS creds
        aren't set)."""
        project_id = project_id or self.project_id
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
        if not self.fusion_aws_util:
            return {}
        try:
            guest = self.fusion_aws_util.get_guest_volumes_for_cluster(
                cluster_id)
        except NotImplementedError:
            return {}
        return {n: sorted(v) for n, v in guest.items() if n != "unattached"}

    def trigger_snapshot_backup(self, cluster_id, project_id=None):
        """POST cloudsnapshotbackups and return the backup_id."""
        project_id = project_id or self.project_id

        endpoint = (
            "/v4/organizations/{}/projects/{}/clusters/{}"
            "/cloudsnapshotbackups".format(
                self.organisation_id, project_id, cluster_id))

        self.log.info(
            "Triggering cloud snapshot backup for cluster {}".format(cluster_id))

        resp = self.capellaAPI.cluster_ops_apis.api_post(endpoint, {})
        if resp.status_code == 429:
            self.handle_rate_limit(int(resp.headers["Retry-After"]))
            resp = self.capellaAPI.cluster_ops_apis.api_post(endpoint, {})
        if resp.status_code not in [200, 201, 202]:
            self.fail(
                "Cloud snapshot backup request failed for cluster {}: "
                "{} {}".format(cluster_id, resp.status_code, resp.content))

        backup_id = resp.json().get("id") or resp.json().get("backupID")
        if not backup_id:
            deadline = time.time() + 120
            while time.time() < deadline:
                list_resp = self.capellaAPI.cluster_ops_apis.api_get(endpoint)
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
            deadline = time.time() + 300
            while time.time() < deadline:
                resp = self.capellaAPI.cluster_ops_apis.list_buckets(
                    self.organisation_id, project_id, target_cluster_id)
                if resp.status_code == 429:
                    self.handle_rate_limit(int(resp.headers["Retry-After"]))
                    continue
                if resp.status_code == 200:
                    present = {
                        b["name"]
                        for b in resp.json().get("data", [])}
                    if all(n in present for n in expected_bucket_names):
                        self.log.info(
                            "All expected buckets present on target {}".format(
                                target_cluster_id))
                        return
                    self.log.info(
                        "Buckets present: {} (waiting for: {})".format(
                            present, expected_bucket_names))
                time.sleep(15)
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
                    count = self.fusion_aws_util.count_s3_objects(bucket_name)
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
                        "S3 monitor: count_s3_objects raised {}: {}".format(
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
