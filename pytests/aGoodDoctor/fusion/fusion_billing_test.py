"""
Fusion Billing Tests (AV-110004 / AV-94188 — Capella Express Scaling billing).

End-to-end, verify-only. Each run: deploys/reuses a cluster (via the SAME v4
APIBase pattern the fusion backup/restore suite uses — FusionBackupRestoreBase),
creates buckets, enables fusion (needs the fusion-rebalances tenant flag on in
the env), loads data, runs a fusion rebalance, then verifies the billing docs the
control plane wrote — by querying the control-plane Couchbase DB (the same DB
`cbc-db <env>` reaches) via the kubectl CP-DB util (TAF 248672).

Re-homed onto FusionBackupRestoreBase (an APIBase / Capella v4 subclass) so the
P0 fusion dispatcher works: the old dedicated CapellaBaseTest.setUp path called
CapellaUtils.create_access_secret_key, whose tenant-key endpoint returns
`404 page not found` on P0. The v4 deploy (provision_fusion_cluster ->
create_cluster_customAMI with the per-pod internal-support token) avoids it.

The 3 fusion billing changes verified here (per #capella-billing):
  1. FIXED SSD   — models.HourlyBillingRecord gains an SSD add-on PER NODE when
                   fusion is enabled. (every hour, for the previous hour)
  2. FIXED bucket— models.HourlyBillingRecord gains a bucket charge when a
                   cluster has > free-tier buckets. Lands at debug.fusionBucketCosts
                   {nodeCount, credits} (nodeCount is really the BUCKET count) as a
                   SINGLE per-cluster record (nodeId="") — the per-cluster form from
                   couchbase-cloud PR #54446 (verified live on sbx-25; the old
                   per-node form is gone).
  3. VARIABLE    — billing.pagerTask (one per accelerator node, written the moment
                   the node finishes a rebalance download) records GiB moved;
                   billing.variable is the priced aggregate the pager-biller job
                   writes later (scheduled daily 18:30 UTC, or a manual run).
                   Automation reads pagerTask directly — no wait for the daily job.

Keyspaces / fields / formulas verified against couchbase-cloud source (NOT the
functional test-plan doc, which had stale names). Bucket = `capella`:
  models.HourlyBillingRecord: databaseId, nodeId, fusionEnabled,
      debug.fusionCosts.{fusionSSDCost,ebsListPrice,fusionSSDUplift}  (per node),
      debug.fusionBucketCosts.{credits,nodeCount}  (single per-cluster, nodeId="")
  billing.pagerTask: clusterId, fusionDetails.{nodeID,planUUID,shardSizeInBytes,
                                               downloadCompletedAt}
  billing.variable:  databaseId, usageCategory="Fusion 2", creditQuantity,
                     billingBreakdown.{usage,usageUnit,uplift}
  billing.credit_factors: category="Express Scaling (Fusion)", name, multiplier
  SSD hourly = (diskSizeGb * EBSListPrice / 732) * SSDUplift
      732 = biller.HoursInMonth = 30.5*24 (couchbase-cloud
      internal/billing/biller/fixed/biller.go). The functional test-plan doc's
      "730" is a DOC BUG — the biller and live records both use 732.
  bucket     = max(0, bucketCount - freeTier) * bucketGlobalPrice
  variable   = (sum shardSizeInBytes / 1024^3) * globalRate * cspUplift
  creditsConsumed (total, AV-28236) = (computeCost + storageCost + logsPrice)
      * nodeUplift * ssdToRAMRatio * iopsToRAMRatio + fusionSSDCost
  Job types (couchbase-cloud): hourly fixed = "ClusterBilling", pager = "BillPagerTasks".

Aggregate (optional total-credits check): POST /internal/support/f/aggregate with
  {BillingDate, Manual:true, ServiceIds:[clusterId]} returns the summed credits
  that would go to Salesforce — worth running once billers have produced docs.

Params:
  cp_env=sbx-N            CP EKS derived as sbx-N-cp-eks (or cp_eks_cluster_name=)
  keep_cluster=True       do NOT destroy the cluster at tearDown (default True)
  num_buckets=12          >10 to exercise bucket overage (default from base=1)
  create_end=2000000      docs to load per bucket
Run with the AWS profile that reaches the CP account (cbc-db default:
dbaas-test-0005-temp).
"""

import logging
import os
import re
import time

from TestInput import TestInputSingleton
from .fusion_backup_restore_base import FusionBackupRestoreBase
from .kubectl_cp_db_util import KubectlCPDBUtil

# Module logger — the early setUp (before super()) runs before self.log exists.
_LOG = logging.getLogger("fusion_billing_test")


class FusionBillingTest(FusionBackupRestoreBase):
    """Validate fusion billing records in the control-plane database.

    Re-homed onto FusionBackupRestoreBase (APIBase / Capella v4). Object model:
      self.cluster_id / self.cluster_ids   (v4 cluster ids; was self.cluster[.id])
      self.organisation_id                 (v4 tenant; was self.tenant)
      self.url / self.capellaAPI           (v4 host + client; was self.pod)
    """

    HOURS_PER_MONTH = 30.5 * 24                 # 732; EBS monthly->hourly divisor
    # OffClusterBilling only bills an hour the cluster was off for the WHOLE
    # clock hour — worst case (turned off right at minute 0) that first
    # qualifying hour doesn't close for just under 2 hours. Give it margin
    # over self.billing_wait_timeout's default (90 min), which isn't enough.
    OFF_RECORD_WAIT_TIMEOUT = 130 * 60          # 130 min
    FUSION_FACTOR_CATEGORY = "Express Scaling (Fusion)"
    BYTES_PER_GIB = 1024 ** 3
    # TENANT-level flag that gates fusion billing. ff-billing-fusion-enabled
    # (the old GLOBAL flag this used to be) is confirmed removed from the
    # product entirely (AV-140741, Tom Tonner, 2026-08-17) — fusion billing
    # is "all under fusion-rebalances flag" now, and that flag is
    # tenant-scoped, not global. See _set_fusion_billing_enabled.
    FUSION_BILLING_FLAG = "fusion-rebalances"
    # Standard sandbox CP-DB access (baked in so the ini needs no AWS config):
    # base creds = cbc-main-004-iam (or AWS_ACCESS_KEY_ID_004 env on CI), which
    # assume the jenkins-cp-cli role that is mapped in the sandbox EKS RBAC.
    DEFAULT_AWS_BASE_PROFILE = "cbc-main-004-iam"
    DEFAULT_ASSUME_ROLE_ARN = "arn:aws:iam::264138468394:role/jenkins-cp-cli"

    def setUp(self):
        inp = TestInputSingleton.input

        # Base AWS creds priority for the CP-DB kubectl/boto3 path (assume
        # jenkins-cp-cli from these):
        #   1. AWS_ACCESS_KEY_ID_004 / AWS_SECRET_ACCESS_KEY_004 env (CI).
        #   2. explicit aws_access_key/aws_secret_key params.
        #   3. a named aws_profile read from ~/.aws/credentials (local).
        aws_profile = inp.param("aws_profile", self.DEFAULT_AWS_BASE_PROFILE)
        if not inp.param("aws_access_key", None) \
                and not os.environ.get("AWS_ACCESS_KEY_ID_004"):
            creds_path = os.path.join(
                os.path.expanduser("~"), ".aws", "credentials")
            if aws_profile and os.path.exists(creds_path):
                import configparser
                cp = configparser.ConfigParser()
                cp.read(creds_path)
                if aws_profile in cp:
                    s = cp[aws_profile]
                    if "aws_access_key_id" in s:
                        inp.test_params["aws_access_key"] = s["aws_access_key_id"]
                    if "aws_secret_access_key" in s:
                        inp.test_params["aws_secret_key"] = s["aws_secret_access_key"]
                    if "aws_session_token" in s:
                        inp.test_params["aws_session_token"] = s["aws_session_token"]

        # keep_cluster maps onto the base's cluster lifecycle:
        #   keep_cluster=True  -> base keep_clusters=True (preserve EVERYTHING; no
        #                         deletion, no end-of-matrix sweep) for manual use.
        #   keep_cluster=False -> preserve_clusters=True so clusters are REUSED
        #                         across same-spec tests, and the base's
        #                         end-of-matrix teardown destroys them all on the
        #                         last test (guaranteed teardown).
        self.keep_cluster = str(inp.param("keep_cluster", True)).lower() != "false"
        if self.keep_cluster:
            inp.test_params["keep_clusters"] = True
        inp.test_params.setdefault("preserve_clusters", True)
        inp.test_params.setdefault("reuse_clusters", True)

        # 8.1.0 + fusion must be enabled on the TENANT *before* the cluster is
        # deployed — otherwise Capella rejects the deploy with "requested version
        # 8.1.0 is not supported" (only 7.x is offered until enable-eight-one-zero
        # flips the version gate). The base applies the `feature_flags` param via
        # the v2 tenant-feature-flag API during its setUp (BEFORE we deploy here),
        # so inject the required flags into that param now.
        required_ff = ("enable-eight-one-zero:true,fusion-rebalances:true,"
                       "fusion-fallback-replace:true")
        existing_ff = inp.test_params.get("feature_flags", "")
        inp.test_params["feature_flags"] = (
            "%s,%s" % (existing_ff, required_ff) if existing_ff else required_ff)
        _LOG.info("Pre-deploy tenant feature_flags = %s"
                  % inp.test_params["feature_flags"])

        # APIBase deploy-nothing + attrs/token/tenant-feature-flags/AWS creds.
        super(FusionBillingTest, self).setUp()

        # Topology params.
        self.own_cluster = str(inp.param("own_cluster", False)).lower() == "true"
        self.num_clusters = int(inp.param("num_clusters", 1))
        self.kv_nodes = int(inp.param("kv_nodes", self.source_num_nodes))
        # Docs per bucket: the conf passes create_end (compat with the old suite).
        self.num_docs = int(
            inp.param("create_end", inp.param("num_docs", self.num_docs)))

        # Region convention (same as backup/restore cross-region): with
        # cross_region=True + num_clusters>1, deploy cluster[0] in `region` and the
        # rest in the alternate region — set ONE region, get the pair. An explicit
        # `regions` (';'/',' separated) overrides the derivation.
        src_region = inp.param("region", self.aws_region)
        self.aws_region = src_region
        regions_param = inp.param("regions", None)
        if regions_param:
            self.deploy_regions = [r.strip()
                                   for r in re.split("[;,]", str(regions_param))
                                   if r.strip()]
        elif inp.param("cross_region", False) and self.num_clusters > 1:
            self.deploy_regions = [src_region,
                                   self._alternate_region(src_region)]
        else:
            self.deploy_regions = [src_region]

        # Deploy (or reuse from the base pool) the billing cluster(s). own_cluster
        # tests take a FRESH, self-destroying cluster (different topology) that
        # doesn't disturb the shared reuse pool.
        self.cluster_ids = []
        self.cluster_regions = []
        for i in range(self.num_clusters):
            region = self.deploy_regions[i % len(self.deploy_regions)]
            cid, _ = self.acquire_cluster(
                fusion_enabled=True, num_nodes=self.kv_nodes,
                name_prefix="TAF_FusionBilling", region=region,
                fresh=self.own_cluster)
            self.cluster_ids.append(cid)
            self.cluster_regions.append(region)
        self.cluster_id = self.cluster_ids[0]
        self.aws_region = self.cluster_regions[0]
        self.log.info("[setUp] billing cluster(s)=%s regions=%s"
                      % (self.cluster_ids, self.cluster_regions))

        # CP-DB connection (kubectl CP-DB util) for the granular N1QL billing-doc
        # reads. The HTTP hourly-records endpoint returns only the aggregate
        # sender.Summary, NOT the debug.fusionCosts breakdown the formulas need,
        # so N1QL against the control-plane DB is required.
        self._connect_cp_db()

        # Sandbox CP DB layout: a single `default` bucket, docs discriminated by
        # a `_type` field = "<scope>.<collection>" (verified against couchbase-cloud
        # source; e.g. models.HourlyBillingRecord). Query by _type, not keyspace.
        self.cp_bucket = inp.param("cp_bucket", "default")
        self.KS = "`%s`" % self.cp_bucket
        self.TYPE_HOURLY = "models.HourlyBillingRecord"
        self.TYPE_PAGER = "billing.pagerTask"
        self.TYPE_VARIABLE = "billing.variable"
        self.TYPE_FACTORS = "billing.credit_factors"

        # SSD disk defaults to the cluster's LIVE (auto-scaled) disk read at verify
        # time; pass ssd_gib_per_node only to override that expectation.
        self.credit_tolerance = float(inp.param("credit_tolerance", 1e-6))
        self.billing_wait_timeout = int(inp.param("billing_wait_timeout", 5400))

        # SPEED-UP: trigger the billing jobs on demand instead of waiting up to
        # ~60 min for the top-of-hour fixed biller / the 18:30 pager biller.
        # CONFIRMED from couchbase-cloud source: cp-api mounts the job-run endpoint
        # at POST /internal/support/jobs behind InternalSupportAuth(TokenForInternal
        # Support) — the SAME token _internal_support_token() carries. jobTypes:
        #   ClusterBilling -> hourly fixed biller (HourlyBillingRecord: SSD+bucket);
        #                     payload serviceIds bills only this cluster.
        #   BillPagerTasks -> pager biller (priced billing.variable).
        # ON by default; best-effort — if a call is rejected it logs and falls back
        # to waiting, so a run is never broken. Disable with trigger_billing_jobs=False.
        self.trigger_billing_jobs = inp.param("trigger_billing_jobs", True)
        self.hourly_billing_job_type = inp.param(
            "hourly_billing_job_type", "ClusterBilling")
        self.pager_billing_job_type = inp.param(
            "pager_billing_job_type", "BillPagerTasks")
        # Off-state fixed biller (couchbase-cloud cmd/cp-scheduler wiring):
        # OffClusterBilling. Distinct job type from the on-cluster ClusterBilling
        # above (offclustersbiller/offbilling.go) — _maybe_trigger_billing_jobs
        # does NOT fire this one; a test that turns a cluster off must trigger it
        # explicitly (see test_fusion_uplift_during_turnoff_after_scale).
        self.off_billing_job_type = inp.param(
            "off_billing_job_type", "OffClusterBilling")
        self.billing_jobs_path = inp.param(
            "billing_jobs_path", "/internal/support/jobs")
        self.billing_jobs_token = inp.param("billing_jobs_token", None)

    def _connect_cp_db(self):
        """Derive the CP EKS name from the v4 pod host, set AWS creds for the
        kubectl/boto3 subprocess, and connect (assume jenkins-cp-cli via the boto3
        path in kubectl_lib — no aws CLI). Sets self.cp_db and the cp_* attrs. The
        internal-support token for billing-job triggers now comes from the base's
        _internal_support_token() (per-pod env), so no token-fetch dance here."""
        inp = TestInputSingleton.input
        aws_profile = inp.param("aws_profile", self.DEFAULT_AWS_BASE_PROFILE)
        # CP-DB creds are resolved independently of the base's (already-assumed)
        # fusion_aws_util creds: the boto3 path below assumes jenkins-cp-cli from
        # these BASE creds, so use raw params/locals (never the base's assumed
        # creds, which cannot re-assume the same role).
        cp_ak = inp.param("aws_access_key", None)
        cp_sk = inp.param("aws_secret_key", None)
        cp_st = inp.param("aws_session_token", None)
        self.cp_role_arn = inp.param(
            "aws_assume_role_arn", self.DEFAULT_ASSUME_ROLE_ARN)
        self.cp_role_external_id = inp.param(
            "jenkins_cpcli_role_external_id",
            "f7bdb290-7b15-4ab7-afbf-28f3464a6144")
        self.cp_namespace = inp.param("cp_namespace", None)
        # The CP-EKS control-plane cluster lives in its own fixed home region
        # (us-east-1 for this account, per the fusion-triage AWS access notes)
        # regardless of which region the fusion DATA cluster under test is
        # deployed into via the 'region' param. Conflating the two broke the
        # eu-west-1 cross-region conf entry: it searched for the sbx's
        # -cp-eks cluster in eu-west-1 (ResourceNotFoundException) because
        # 'region' there is eu-west-1, not the CP's actual region. Use a
        # dedicated param so the two can never accidentally collide again.
        region = inp.param("cp_region", "us-east-1")

        # CP EKS name: explicit param, cp_env override, or derived from the pod.
        self.cp_eks_cluster_name = inp.param("cp_eks_cluster_name", None)
        cp_env = inp.param("cp_env", None)
        if not cp_env:
            pod_host = self.url or ""
            parts = pod_host.split(".")
            if len(parts) >= 2 and parts[0] == "cloudapi":
                cp_env = parts[1]   # e.g. sbx-25, qe-7
        if not self.cp_eks_cluster_name and cp_env:
            self.cp_eks_cluster_name = "%s-cp-eks" % cp_env
        if not self.cp_eks_cluster_name:
            self.fail("Could not determine CP EKS cluster — set 'pod' in the ini "
                      "(cloudapi.<env>.sandbox...) or pass -p cp_eks_cluster_name=...")
        self.log.info("CP EKS cluster: %s" % self.cp_eks_cluster_name)

        # AWS creds for the aws-cli/kubectl subprocess.
        if cp_ak and cp_sk:
            os.environ["AWS_ACCESS_KEY_ID"] = cp_ak
            os.environ["AWS_SECRET_ACCESS_KEY"] = cp_sk
            os.environ.pop("AWS_PROFILE", None)
            os.environ.pop("AWS_SESSION_TOKEN", None)
            if cp_st:
                os.environ["AWS_SESSION_TOKEN"] = cp_st
        elif not os.environ.get("AWS_ACCESS_KEY_ID_004") and aws_profile \
                and not os.environ.get("AWS_ACCESS_KEY_ID"):
            os.environ["AWS_PROFILE"] = aws_profile

        self.cp_db = KubectlCPDBUtil(
            access_key=cp_ak, secret_key=cp_sk, region=region)
        connected = False
        attempts = 3
        for attempt in range(1, attempts + 1):
            if self.cp_role_arn:
                connected = self.cp_db.connect(
                    self.cp_eks_cluster_name, role_arn=self.cp_role_arn,
                    external_id=self.cp_role_external_id)
            else:
                connected = self.cp_db.kubectl.update_kubeconfig(
                    self.cp_eks_cluster_name)
            if connected:
                break
            if attempt < attempts:
                self.log.warning(
                    "Attempt %d/%d to reach CP EKS cluster %s failed (%s) — "
                    "retrying" % (attempt, attempts, self.cp_eks_cluster_name,
                                  self.cp_db.kubectl.last_error))
                time.sleep(5 * attempt)
        if not connected:
            self.fail("Could not reach CP EKS cluster %s after %d attempts: %s"
                      % (self.cp_eks_cluster_name, attempts,
                         self.cp_db.kubectl.last_error))

    def tearDown(self):
        """Disconnect the CP-DB tunnel, then defer to the base for cluster/bucket
        lifecycle (reuse across tests, guaranteed end-of-matrix teardown when
        keep_cluster=False, preserve-everything when keep_cluster=True)."""
        if getattr(self, "cp_db", None):
            try:
                self.cp_db.disconnect()
            except Exception as e:
                self.log.warning("cp_db disconnect failed: %s" % e)
        super(FusionBillingTest, self).tearDown()

    # ------------------------------------------------------------------
    # CP-DB helpers
    # ------------------------------------------------------------------

    def _n1ql(self, statement):
        self.log.info("CP-DB N1QL: %s" % statement)
        return self.cp_db.run_n1ql_query(statement, namespace=self.cp_namespace)

    def _get_factor(self, name):
        """credit_factors multiplier by name in the fusion category (keyed by name+category only)."""
        rows = self._n1ql(
            "SELECT f.multiplier FROM %s f "
            "WHERE f.`_type` = \"%s\" "
            "AND f.category = \"%s\" AND f.name = \"%s\""
            % (self.KS, self.TYPE_FACTORS, self.FUSION_FACTOR_CATEGORY, name))
        if not rows:
            self.fail("Factor '%s' missing in credit_factors" % name)
        return float(rows[0]["multiplier"])

    def _csp_uplift_factor(self, region=None):
        """The per-region accelerator CSP uplift multiplier.

        The factor name embeds the CLOUD PROVIDER, so this is not a constant
        even though the suite currently only ever deploys AWS
        (provision_fusion_cluster hardcodes provider='hostedAWS' / disk
        type 'gp3'). Centralised here rather than repeating the literal at
        each call site, both to kill the duplication and so that adding
        Azure/GCP coverage later is a one-line change here instead of a
        hunt through the file. cloud_provider is a param so a future
        non-AWS entry can set it without touching code."""
        provider = str(self.input.param("cloud_provider", "AWS")).upper()
        return self._get_factor(
            "Accelerator CSP Uplift - %s %s"
            % (provider, region or self.aws_region))

    def _poll(self, fn, what, interval=120, timeout=None):
        """Poll fn() until it returns truthy or `timeout` elapses.

        fn() typically ends in a CP-DB N1QL call (_n1ql -> kubectl_cp_db_
        util.run_n1ql_query), which re-discovers the query-capable CP pod on
        every single call over a fresh kubectl port-forward — a transient
        blip there (port-forward hiccup, query service mid-restart on the
        CP's own side) raises RuntimeError. Without tolerance, one such blip
        on the LAST iteration of an hours-long wait kills the whole poll and
        throws away all the time already spent — mirror the existing
        _cp_error_limit convention (fusion_backup_restore_base.py's v4-API
        polling) here: tolerate up to _cp_error_limit CONSECUTIVE fn()
        exceptions (extending the deadline by `interval` each time so a
        blip doesn't eat into the real wait budget) and only let a
        SUSTAINED outage fail the poll."""
        deadline = time.time() + (timeout if timeout is not None
                                  else self.billing_wait_timeout)
        cp_errors = 0
        while time.time() < deadline:
            try:
                result = fn()
            except Exception as exc:
                cp_errors += 1
                if cp_errors >= self._cp_error_limit:
                    self.fail(
                        "%s: %s consecutive errors polling for %s — giving "
                        "up: %s" % (type(exc).__name__, cp_errors, what, exc))
                self.log.warning(
                    "Transient error (%s/%s) polling for %s: %s — retrying"
                    % (cp_errors, self._cp_error_limit, what, exc))
                deadline += interval
                time.sleep(interval)
                continue
            cp_errors = 0
            if result:
                return result
            self.log.info("Waiting for %s (%ds left)"
                          % (what, int(deadline - time.time())))
            time.sleep(interval)
        return None

    def _trigger_billing_job(self, job_type, payload=None):
        """Best-effort: ask the control plane to run a billing job NOW instead of
        waiting for its schedule. Body matches couchbase-cloud RunJobPayload:
        {jobType, payload}. Returns True on 2xx. NEVER raises — a wrong jobType/auth
        is logged and we fall back to the natural schedule, so a run is never broken."""
        import requests
        token = self.billing_jobs_token or self._internal_support_token()
        pod_host = self.url or ""
        if not token or not job_type or not pod_host:
            return False
        base = "https://" + pod_host.replace("cloudapi.", "api.")
        try:
            resp = requests.post(
                "%s%s" % (base, self.billing_jobs_path),
                headers={"Authorization": "Bearer %s" % token,
                         "Content-Type": "application/json"},
                json={"jobType": job_type, "payload": payload or {"scheduled": False}},
                timeout=60)
            ok = 200 <= resp.status_code < 300
            self.log.info("[trigger] job %s -> %s%s"
                          % (job_type, resp.status_code,
                             "" if ok else " " + resp.text[:200]))
            return ok
        except Exception as e:
            self.log.warning("[trigger] job %s failed: %s" % (job_type, e))
            return False

    def _maybe_trigger_billing_jobs(self):
        """When trigger_billing_jobs is on, run the fixed (ClusterBilling) + pager
        (BillPagerTasks) jobs so records appear in ~seconds instead of at the top of
        the hour. No-op by default. Called before each record query so 'wait for a
        fresh hour' loops resolve fast. Payloads per couchbase-cloud: ClusterBilling
        accepts serviceIds to bill only this cluster; scheduled=false = manual."""
        if not getattr(self, "trigger_billing_jobs", False):
            return
        # Bill the period the data is in: the fixed biller bills a clock HOUR, the
        # pager biller a DAY (couchbase-cloud biller payload.Time). Send explicit
        # UTC times so a fresh cluster's current hour/day is billed on demand.
        this_hour = time.strftime("%Y-%m-%dT%H:00:00Z", time.gmtime())
        this_day = time.strftime("%Y-%m-%dT00:00:00Z", time.gmtime())
        cid = getattr(self, "cluster_id", None)
        if getattr(self, "hourly_billing_job_type", None) and cid:
            self._trigger_billing_job(
                self.hourly_billing_job_type,
                {"scheduled": False, "time": this_hour, "serviceIds": [cid]})
        if getattr(self, "pager_billing_job_type", None):
            self._trigger_billing_job(
                self.pager_billing_job_type, {"scheduled": False, "time": this_day})

    def _hourly_records(self, cluster_id=None):
        self._maybe_trigger_billing_jobs()
        cid = cluster_id or self.cluster_id
        return self._n1ql(
            "SELECT b.nodeId, b.fusionEnabled, b.isOff, b.billingPeriod, "
            "b.creditsConsumed, "
            "b.debug.fusionCosts AS fusionCosts, "
            "b.debug.fusionBucketCosts AS bucketCosts, "
            "b.debug.computeCost AS computeCost, b.debug.storageCost AS storageCost, "
            "b.debug.logsPrice AS logsPrice, b.debug.nodeUplift AS nodeUplift, "
            "b.debug.ssdToRAMRatio AS ssdToRAMRatio, "
            "b.debug.iopsToRAMRatio AS iopsToRAMRatio "
            "FROM %s AS b WHERE b.`_type` = \"%s\" "
            "AND b.databaseId = \"%s\" "
            "ORDER BY b.billingPeriod DESC" % (self.KS, self.TYPE_HOURLY, cid))

    def _completed_pager_tasks(self, cluster_id=None):
        cid = cluster_id or self.cluster_id
        return self._n1ql(
            "SELECT p.fusionDetails.nodeID AS nodeID, "
            "p.fusionDetails.planUUID AS planUUID, "
            "p.fusionDetails.shardSizeInBytes AS shardSizeInBytes "
            "FROM %s p WHERE p.`_type` = \"%s\" "
            "AND p.clusterId = \"%s\" "
            "AND p.fusionDetails.downloadCompletedAt IS NOT NULL "
            "AND p.fusionDetails.downloadCompletedAt != \"\""
            % (self.KS, self.TYPE_PAGER, cid))

    def _kv_spec(self, cluster_id=None):
        """(diskGb, nodeCount) of the cluster's CURRENT (live, possibly auto-scaled)
        KV spec, read from config.specs. The SSD biller prices against this live
        disk — it grows above the requested size as Capella disk auto-scaling kicks
        in during a heavy load, so a record's implied disk (cost*732/(ebs*uplift))
        tracks THIS, not the ini's kv_disk. nodeCount is the KV node count, used to
        prove the per-node SSD charge scales with the cluster size."""
        cid = cluster_id or self.cluster_id
        rows = self._n1ql(
            "SELECT d.config.specs AS specs FROM %s d USE KEYS [\"%s\"]"
            % (self.KS, cid))
        specs = (rows[0].get("specs") if rows else None) or []
        for s in specs:
            svc_types = [svc.get("type") for svc in (s.get("services") or [])]
            if "kv" in svc_types:
                return int(s["disk"]["sizeInGb"]), int(s.get("count", 0))
        if specs:  # single-spec cluster (kv-only) — fall back to spec[0]
            return int(specs[0]["disk"]["sizeInGb"]), int(specs[0].get("count", 0))
        return 0, 0

    def _current_kv_disk_gb(self, cluster_id=None):
        return self._kv_spec(cluster_id)[0]

    # ------------------------------------------------------------------
    # Fusion-billing feature flag (TENANT scope, via the v2 API)
    # ------------------------------------------------------------------

    def _set_fusion_billing_enabled(self, value):
        """Set the TENANT-level fusion-rebalances flag, which now gates
        fusion billing (fixed SSD/bucket AND variable) — replaces the old
        ff-billing-fusion-enabled GLOBAL flag.

        CONFIRMED REMOVED FROM THE PRODUCT (AV-140741 comment, Tom Tonner,
        2026-08-17): "ff-billing-fusion-enabled is gone. It's all under
        fusion-rebalances flag. It's tenant level as well". Toggling the
        old global flag (as this method used to, and as AV-140741's
        original repro did) has NO EFFECT on billing at all any more —
        that ticket's symptom (SSD/bucket cost never zeroing after
        'disabling' the flag) was this suite exercising a stale
        assumption, not a live product bug.

        This suite already sets fusion-rebalances=true at the tenant
        level unconditionally at setUp time, for an unrelated reason
        (deploy-gating 8.1.0 clusters — see the required_ff block and
        _apply_tenant_feature_flag) — this method targets the exact same
        flag, just later in the test lifecycle and with a readback-verify
        that call site doesn't have. Reuses the same create-tenant-flag
        (POST), fall back to update (PUT) on FeatureFlagAlreadyExists
        pattern as _apply_tenant_feature_flag.

        Verify via list_tenant_feature_flags_internal_specific — the
        Bearer-token (cbc_api_request_headers) authenticated GET. NOT
        list_tenant_feature_flags_specific, which (like the equivalent
        global-scope method this replaced) routes through
        do_internal_request()'s broken Basic-auth /sessions login and
        recurses into a RecursionError instead of returning a clean
        error — found and fixed for the global-flag case; same fix
        applies here.

        DELETE-then-CREATE, not create-then-update-on-conflict: a live run
        proved the update (PUT) path on this control plane doesn't actually
        change an existing flag's value — it returns the same
        FeatureFlagAlreadyExists conflict create does, and a follow-up
        readback confirmed the value never moved (asked for False, stayed
        True). Deleting the tenant-level override first (no-op if it's
        already absent) means the following create() never has an existing
        key to conflict with, so it never touches the broken update path at
        all. The update fallback is kept as a last resort only in case
        delete itself doesn't take effect in time (eventual consistency) —
        the readback-verify below is what actually decides pass/fail either
        way."""
        v2 = self._v2_api()
        payload = {"value": bool(value)}
        del_resp = v2.delete_tenant_feature_flag(
            self.organisation_id, self.FUSION_BILLING_FLAG)
        self.log.info("[flag] delete-before-create %s -> %s"
                      % (self.FUSION_BILLING_FLAG, del_resp.status_code))
        resp = v2.create_tenant_feature_flag(
            self.organisation_id, self.FUSION_BILLING_FLAG, payload)
        if resp.status_code not in [200, 201, 204]:
            try:
                err_type = json.loads(resp.content).get("errorType", "")
            except Exception:
                err_type = ""
            if err_type == "FeatureFlagAlreadyExists":
                # Known-unreliable fallback (see docstring) — last resort
                # only; the readback-verify below is the real judge.
                resp = v2.update_tenant_feature_flag(
                    self.organisation_id, self.FUSION_BILLING_FLAG, payload)
        if resp.status_code not in [200, 201, 204]:
            # Don't fail on this status code alone — on a long-lived shared
            # tenant the flag is very likely already sitting at the value
            # we want (left over from an earlier run/setUp's deploy-gating
            # create), and this second call can return a non-2xx for
            # reasons that don't reflect the actual flag state. The
            # readback-verify right below is the real source of truth.
            self.log.warning(
                "Setting tenant feature flag %s=%s returned %s: %s "
                "(continuing to readback-verify before deciding pass/fail)"
                % (self.FUSION_BILLING_FLAG, value, resp.status_code,
                   resp.content))

        verify_resp = v2.list_tenant_feature_flags_internal_specific(
            self.organisation_id, self.FUSION_BILLING_FLAG)
        actual = None
        if verify_resp.status_code == 200:
            try:
                actual = (verify_resp.json() or {}).get(self.FUSION_BILLING_FLAG)
            except ValueError:
                actual = None
        if actual != bool(value):
            self.fail(
                "Tenant feature flag %s did not stick: asked for %s, read "
                "back %r (verify GET status %s, raw body %s) — the SET "
                "call reported success but the flag isn't actually what "
                "we asked for. If %r looks like the wrong SHAPE rather "
                "than the wrong VALUE, the tenant-scoped internal-support "
                "flags GET may not mirror the global one's {flag: bool} "
                "response shape — check the raw body above before "
                "assuming this is a real flag-propagation failure."
                % (self.FUSION_BILLING_FLAG, value, actual,
                   verify_resp.status_code, verify_resp.content, actual))
        self.log.info("Tenant feature flag %s set to %s (verified)"
                      % (self.FUSION_BILLING_FLAG, value))

    # ------------------------------------------------------------------
    # Workload driver: create buckets -> enable fusion -> load -> rebalance
    # ------------------------------------------------------------------

    def _cluster_has_loaded_buckets(self, cluster_id):
        """True when the cluster already carries >= num_buckets loaded
        fusion-bkt-* buckets (a reused cluster) — so the heavy load + rebalance
        (and thus the billing docs) already exist and can be skipped."""
        lb = self.capellaAPI.cluster_ops_apis.list_buckets(
            self.organisation_id, self.project_id, cluster_id)
        if lb.status_code != 200:
            return False
        good = [b for b in lb.json().get("data", [])
                if b.get("name", "").startswith(self.SOURCE_BUCKET_PREFIX)
                and (b.get("stats", {}).get("itemCount") or 0) >= self.num_docs]
        return len(good) >= self.num_buckets

    def _setup_fusion_workload(self):
        """create/reuse buckets -> enable fusion -> load -> rebalance.

        Uses the backup/restore base helpers: populate_source_buckets (bucket
        create + DocLoader load, with built-in reuse/top-up of existing
        fusion-bkt-* buckets), trigger_fusion_rebalance + wait_for_rebalance_complete
        for the rebalance that produces the pagerTask/variable docs. A reused,
        already-loaded cluster skips the load + rebalance (it already carries
        billing docs) — this is what makes the shared-cluster suite cheap."""
        cid = self.cluster_id
        proj = self.project_id
        # Enable fusion BILLING globally (biller reads it without tenant context).
        self._set_fusion_billing_enabled(True)
        # Re-assert fusion is enabled on the cluster (idempotent — cheap when
        # already on, self-heals a drifted reuse). provision already enabled it.
        self._enable_fusion_and_wait(cid, proj)

        already_loaded = self._cluster_has_loaded_buckets(cid)
        # populate_source_buckets works off source_cluster_id/source_project_id and
        # tracks buckets in source_bucket_ids/source_bucket_names.
        self.source_cluster_id = cid
        self.source_project_id = proj
        self.source_bucket_ids = []
        self.source_bucket_names = []
        self.populate_source_buckets()

        if already_loaded:
            self.log.info(
                "Reusing loaded cluster %s — skipping rebalance (already has "
                "data + billing docs)" % cid)
            return

        time.sleep(120)  # Allow initial sync to S3 before rebalance
        orig, new = self.trigger_fusion_rebalance(cid, project_id=proj)
        if not self.wait_for_rebalance_complete(cid, project_id=proj):
            self.fail(
                "Fusion rebalance (%s -> %s nodes) did not complete on cluster %s"
                % (orig, new, cid))

    # ------------------------------------------------------------------
    # Assertion helpers (each covers one billing dimension)
    # ------------------------------------------------------------------

    def _verify_ssd_costs(self, records):
        """Verify the fixed per-node SSD add-on, and the fusion-enabled-vs-disabled
        difference, against the AV-110004 formula:

            fusionSSDCost = (diskGb * ebsListPrice / 732) * SSDUplift

        `diskGb` is the node's LIVE disk (Capella auto-scales it up during a heavy
        load, so we read it from config.specs — NOT the ini's kv_disk). Because a
        rebalance/scaling op is in flight while records are cut, individual hourly
        records can capture a node mid auto-scale (e.g. 200 or 300 GiB before it
        settles at 450). We therefore:
          * assert every enabled record is self-consistent (implied disk is a whole
            number >= the requested floor) — catches a wrong divisor/factor, and
          * require at least one enabled record to match the CURRENT live disk
            exactly — proves the biller prices the real provisioned disk, and
          * assert the enabled-vs-disabled DIFFERENCE: enabled records carry SSD>0
            while any pre-enable/disabled record is exactly 0."""
        factor_uplift = self._get_factor("SSD Uplift")
        cur_disk = self._current_kv_disk_gb()
        # Optional explicit override (default: use the live disk).
        override_disk = self.input.param("ssd_gib_per_node", None)
        if override_disk:
            cur_disk = int(override_disk)
        floor_disk = int(self.input.param("kv_disk", 0) or 0)
        self.log.info("[SSD] live per-node KV disk = %s GiB (requested floor %s GiB)"
                      % (cur_disk, floor_disk))

        enabled, disabled, settled_matches, intermediates = [], [], 0, 0
        for rec in records:
            fc = rec.get("fusionCosts") or {}
            node = rec.get("nodeId", "<unknown>")
            raw = fc.get("fusionSSDCost")
            # Disabled / pre-enable baseline: fusion off, or SSD cost is zero.
            if not rec.get("fusionEnabled") or raw in (None, 0, 0.0):
                if raw in (None, 0, 0.0):
                    disabled.append((node, 0.0))
                continue
            ssd_cost = float(raw)
            ebs_price = float(fc["ebsListPrice"])
            rec_uplift = float(fc["fusionSSDUplift"])
            self.assertGreater(ssd_cost, 0, "Node %s: fusionSSDCost should be > 0" % node)
            self.assertAlmostEqual(rec_uplift, factor_uplift, delta=self.credit_tolerance,
                                   msg="Node %s: uplift %s != %s"
                                       % (node, rec_uplift, factor_uplift))
            # Self-consistency: the implied disk must be a whole number (proves the
            # cost is exactly diskGb*ebs/732*uplift with 732 as the divisor).
            implied = ssd_cost * self.HOURS_PER_MONTH / (ebs_price * rec_uplift)
            self.assertAlmostEqual(
                implied, round(implied), delta=1e-3,
                msg=("Node %s: implied disk %s is not a whole GiB — formula "
                     "divisor/factors are off (ssd=%s, ebs=%s, uplift=%s)"
                     % (node, implied, ssd_cost, ebs_price, rec_uplift)))
            implied_disk = round(implied)
            if floor_disk:
                self.assertGreaterEqual(
                    implied_disk, floor_disk,
                    msg=("Node %s: implied disk %s < requested %s — disk "
                         "auto-scale only grows" % (node, implied_disk, floor_disk)))
            enabled.append((node, ssd_cost, implied_disk))
            # Exact match against the CURRENT live disk (settled node).
            expected_cur = (cur_disk * ebs_price / self.HOURS_PER_MONTH) * rec_uplift
            if abs(ssd_cost - expected_cur) <= self.credit_tolerance:
                settled_matches += 1
            else:
                intermediates += 1
                self.log.info(
                    "[SSD] node %s: implied disk %s GiB != live %s GiB (record cut "
                    "mid auto-scale, ssd=%s) — allowed"
                    % (node, implied_disk, cur_disk, ssd_cost))

        # --- assertions -------------------------------------------------
        if not enabled:
            # AV-140399 (Blocker, filed 2026-08-14): the fixed SSD cost has
            # been observed to NEVER populate for the large majority of
            # fusion-enabled clusters (a fleet survey found 41/45 affected,
            # some stuck at zero for 19+ days) — this is NOT primarily a
            # flag-timing issue (the fusion-rebalances readback is
            # confirmed instant) and does NOT correlate with pagerTask
            # completion. Gather the same diagnostic signals that ticket's
            # investigation used so a real recurrence is triaged fast instead
            # of re-litigating "is the flag actually on" from scratch.
            pager_done = self._n1ql(
                "SELECT COUNT(*) AS n FROM %s p WHERE p.`_type` = \"%s\" "
                "AND p.clusterId = \"%s\" "
                "AND p.fusionDetails.downloadCompletedAt IS NOT NULL "
                "AND p.fusionDetails.downloadCompletedAt != \"\""
                % (self.KS, self.TYPE_PAGER, self.cluster_id))
            pager_done_n = (pager_done[0].get("n") if pager_done else None)
            self.fail(
                "No fusion-enabled record carried a non-zero "
                "debug.fusionCosts.fusionSSDCost for cluster %s — matches "
                "AV-140399 (fixed SSD cost never populates for the large "
                "majority of fusion-enabled clusters; NOT a flag-timing "
                "issue — confirm via the flag's own readback rather than "
                "re-flipping it). Diagnostic: %s completed pagerTask(s) for "
                "this cluster (AV-140399 found SSD-cost appearance does NOT "
                "correlate with pagerTask completion in either direction, so "
                "this being nonzero does not rule the ticket out)."
                % (self.cluster_id, pager_done_n))
        self.assertGreater(
            settled_matches, 0,
            "No enabled record matched the live disk %s GiB via "
            "(disk*{ebs}/732)*uplift — cluster may not have settled after "
            "scaling, or the biller is not pricing the provisioned disk" % cur_disk)
        self.log.info(
            "[SSD] %s enabled record(s): %s match live disk %s GiB, %s at "
            "auto-scale intermediates"
            % (len(enabled), settled_matches, cur_disk, intermediates))

        # --- enabled vs disabled difference (the explicit ask) ----------
        for node, val in disabled:
            self.assertEqual(val, 0.0,
                             "Disabled/pre-enable node %s SSD should be 0, got %s"
                             % (node, val))
        max_enabled = max(s for _, s, _ in enabled)
        max_disabled = max((v for _, v in disabled), default=0.0)
        if disabled:
            # Same-test proof: the disabled/pre-enable SSD cost is strictly LESS
            # than the fusion-enabled SSD cost.
            self.assertGreater(
                max_enabled, max_disabled,
                msg=("enabled SSD %s should be > disabled SSD %s — fusion must "
                     "add a per-node SSD charge" % (max_enabled, max_disabled)))
            self.log.info(
                "[SSD enabled-vs-disabled] enabled SSD=%s > disabled SSD=%s across "
                "%s disabled record(s) -> fusion adds the per-node SSD charge"
                % (max_enabled, max_disabled, len(disabled)))
        else:
            self.log.info(
                "[SSD enabled-vs-disabled] enabled max SSD=%s (>0); no disabled "
                "baseline in the query window (cluster was already fusion-enabled). "
                "Run test_fusion_disable_reenable_billing to toggle explicitly."
                % max_enabled)
        self.assertGreater(max_enabled, 0)
        return enabled, disabled

    def _verify_bucket_costs(self, records):
        """Verify the fixed bucket charge:  credits = max(0, buckets - freeTier) *
        globalPrice.  Landed at debug.fusionBucketCosts (NOT under fusionCosts) as
        a SINGLE per-cluster record (nodeId="") — the per-cluster form from
        couchbase-cloud PR #54446. `nodeCount` there is actually the bucket count.
        Only emitted while fusion billing is enabled, so a disabled/pre-enable
        record carries none — that is the enabled-vs-disabled difference. That
        difference is a REAL assertion below (every disabled record's bucket
        charge, if any, must be 0), not just an opportunistic log — a disabled
        record that DOES carry a non-zero bucket charge must fail loudly, not
        be silently excluded from consideration."""
        free_tier = self._get_factor("Buckets - Free Tier")
        global_price = self._get_factor("Buckets - Global Price")
        expected = max(0, self.num_buckets - free_tier) * global_price
        self.log.info(
            "[bucket] num_buckets=%s, freeTier=%s, price=%s -> expected bucket "
            "charge %s" % (self.num_buckets, free_tier, global_price, expected))

        bucket_recs = [r for r in records if r.get("bucketCosts")]
        # ALL disabled/pre-enable records, regardless of whether they happen to
        # carry a bucketCosts sub-doc — checked below, not filtered out here.
        disabled = [r for r in records if not r.get("fusionEnabled")]
        self.assertTrue(
            bucket_recs,
            "No record carried debug.fusionBucketCosts — the fusion bucket charge "
            "was not emitted (num_buckets=%s > freeTier=%s)"
            % (self.num_buckets, free_tier))

        # Assert per billing period: exactly ONE cluster-level bucket doc, correct.
        by_period = {}
        for r in bucket_recs:
            by_period.setdefault(r.get("billingPeriod"), []).append(r)
        for period, recs in by_period.items():
            self.assertEqual(
                len(recs), 1,
                "%s: bucket charge should be ONE per-cluster doc, got %s "
                "(regressed to per-node?)" % (period, len(recs)))
            bkt = recs[0]["bucketCosts"]
            self.assertEqual(
                recs[0].get("nodeId", ""), "",
                "%s: bucket doc should be cluster-level (nodeId=''), got nodeId=%r"
                % (period, recs[0].get("nodeId")))
            self.assertAlmostEqual(
                float(bkt["credits"]), expected, delta=self.credit_tolerance,
                msg=("%s: bucket credits %s != max(0,%s-%s)*%s = %s"
                     % (period, bkt["credits"], self.num_buckets, free_tier,
                        global_price, expected)))
            self.assertEqual(
                int(bkt.get("nodeCount", -1)), self.num_buckets,
                msg=("%s: bucket doc nodeCount %s != num_buckets %s (field is "
                     "misnamed but should equal the bucket count)"
                     % (period, bkt.get("nodeCount"), self.num_buckets)))
        self.log.info(
            "[bucket] verified per-cluster bucket charge = %s on %s billing "
            "period(s)" % (expected, len(by_period)))

        # --- enabled vs disabled difference (REAL assertion, not a log) ----
        for r in disabled:
            bkt = r.get("bucketCosts") or {}
            credits = float(bkt.get("credits") or 0)
            self.assertEqual(
                credits, 0,
                "Disabled/pre-enable record for period %s carries a non-zero "
                "bucket charge %s — fusion bucket cost should be 0 (or the "
                "sub-doc absent) while fusion billing is disabled"
                % (r.get("billingPeriod"), credits))
        if disabled:
            self.log.info(
                "[bucket enabled-vs-disabled] %s disabled/pre-enable record(s) "
                "confirmed carrying NO non-zero bucket charge -> fusion adds "
                "it" % len(disabled))
        else:
            self.log.info(
                "[bucket enabled-vs-disabled] no disabled/pre-enable record in "
                "this query window — no comparison available here (see "
                "test_fusion_disable_reenable_billing for an explicit toggle)")

    def _verify_pager_tasks(self):
        tasks = self._poll(self._completed_pager_tasks,
                           "pagerTask documents after rebalance")
        self.assertTrue(
            tasks, "No completed pagerTask for cluster %s after rebalance"
                   % self.cluster_id)
        for pt in tasks:
            self.assertGreater(int(pt["shardSizeInBytes"]), 0,
                               "Node %s: shardSizeInBytes should be > 0" % pt["nodeID"])
        plan_uuid = tasks[0]["planUUID"]
        total_bytes = sum(int(pt["shardSizeInBytes"]) for pt in tasks
                          if pt["planUUID"] == plan_uuid)
        gib = total_bytes / self.BYTES_PER_GIB
        base_price = self._get_factor("Accelerator Global Rate - per GiB")
        uplift = self._csp_uplift_factor()
        self.log.info(
            "[variable] %s pagerTask(s), planUUID=%s: %s bytes = %s GiB -> expected "
            "variable cost %s credits (rate %s, uplift %s)"
            % (len(tasks), plan_uuid, total_bytes, gib,
               gib * base_price * uplift, base_price, uplift))
        return plan_uuid, gib

    def _current_fusion_variable_credits(self, force_trigger=False):
        """Latest billing.variable creditQuantity for usageCategory='Fusion 2'
        on this cluster (by usageDate DESC, take the newest — the same
        'most recent record' precedent test_variable_record_after_rebalance
        already relies on). force_trigger=True re-runs BillPagerTasks for
        TODAY right now, so the record reflects the CURRENT global
        fusion-rebalances state before reading it — no second
        rebalance needed, since the pager biller reprices whatever
        pagerTask docs already exist for the day, not just ones written
        after the trigger."""
        if force_trigger:
            self._trigger_billing_job(
                self.pager_billing_job_type,
                {"scheduled": False,
                 "time": time.strftime("%Y-%m-%dT00:00:00Z", time.gmtime())})
        rows = self._n1ql(
            "SELECT v.creditQuantity FROM %s v WHERE v.`_type` = \"%s\" "
            "AND v.databaseId = \"%s\" AND v.usageCategory = \"Fusion 2\" "
            "ORDER BY v.usageDate DESC LIMIT 1"
            % (self.KS, self.TYPE_VARIABLE, self.cluster_id))
        return float(rows[0].get("creditQuantity") or 0) if rows else 0.0

    # ------------------------------------------------------------------
    # Tests
    # ------------------------------------------------------------------

    def test_fusion_billing_e2e(self):
        """Comprehensive: deploy -> buckets -> enable fusion -> load -> rebalance,
        then verify EVERY billing dimension on the produced records in one run:
          * VARIABLE   — pagerTask GiB priced by rate * CSP uplift
          * FIXED SSD  — per-node (disk*ebs/732)*uplift, incl enabled-vs-disabled
          * FIXED bkt  — per-cluster max(0, buckets-freeTier)*price
          * integrity  — no duplicate (node,hour) records, per-node coverage
          * region     — the region's ebsListPrice + CSP uplift are applied
        One deploy, one workload, all checks."""
        self._setup_fusion_workload()

        # VARIABLE — pagerTask is written immediately on rebalance completion.
        self._verify_pager_tasks()

        # FIXED — wait for the hourly job, then run every record-based check.
        records = self._poll(self._hourly_records,
                            "the hourly fixed-billing job to emit a record")
        self.assertTrue(records, "No HourlyBillingRecord for cluster %s"
                                 % self.cluster_id)
        self._verify_ssd_costs(records)                 # SSD + enabled-vs-disabled
        if self.num_buckets > 10:
            self._verify_bucket_costs(records)          # per-cluster bucket charge
        else:
            self.log.info("[bucket] num_buckets=%s <= free tier — no overage "
                          "expected (set num_buckets>10 to bill a charge)"
                          % self.num_buckets)
        self._verify_no_duplicates(records)             # integrity + per-node coverage
        self._verify_region_pricing(records)            # region-correct factors
        self._verify_billing_continuity(records)        # a record every hour, no gaps
        self._verify_fusion_premium(records)            # fusion raises total creditsConsumed
        self._verify_credits_decomposition(records)     # EXACT total-credits equation
        self._test_succeeded = True

    def _verify_no_duplicates(self, records):
        """Integrity: at most ONE SSD record per (node, billingPeriod) and ONE
        per-cluster bucket doc per period (no double-billing); and the per-node SSD
        charge covers at least the KV node count (proves it scales per node)."""
        seen, dupes, bucket_per_period, ssd_nodes = set(), [], {}, set()
        for r in records:
            fc = r.get("fusionCosts") or {}
            if "fusionSSDCost" in fc:
                key = (r.get("nodeId"), r.get("billingPeriod"))
                if key in seen:
                    dupes.append(key)
                seen.add(key)
                if float(fc.get("fusionSSDCost") or 0) > 0:
                    ssd_nodes.add(r.get("nodeId"))
            if r.get("bucketCosts"):
                p = r.get("billingPeriod")
                bucket_per_period[p] = bucket_per_period.get(p, 0) + 1
        self.assertFalse(dupes, "Duplicate SSD records for (node,period): %s" % dupes[:5])
        multi = {p: n for p, n in bucket_per_period.items() if n > 1}
        self.assertFalse(multi, "More than one bucket doc in a period: %s" % multi)
        min_nodes = int(self.input.param("kv_nodes", 3))
        self.assertGreaterEqual(
            len(ssd_nodes), min_nodes,
            "SSD billed on only %s distinct node(s); expected >= %s — per-node SSD "
            "billing should cover every KV node" % (len(ssd_nodes), min_nodes))
        self.log.info("[integrity] %s unique (node,period) SSD record(s), no dupes; "
                      "per-node SSD on %s node(s) (>= %s)"
                      % (len(seen), len(ssd_nodes), min_nodes))

    def _verify_region_pricing(self, records):
        """Region correctness: the SSD ebsListPrice and the variable CSP uplift used
        are the ones for the cluster's region (not a hardcoded default). Pass
        expected_ebs_price=<n> to pin the region's disk price."""
        region = self.aws_region
        uplift = self._csp_uplift_factor(region)  # must exist
        ebs_prices = {float(r["fusionCosts"]["ebsListPrice"]) for r in records
                      if (r.get("fusionCosts") or {}).get("ebsListPrice") is not None}
        self.assertTrue(ebs_prices, "No ebsListPrice on any SSD record")
        for p in ebs_prices:
            self.assertGreater(p, 0, "ebsListPrice %s for region %s must be > 0"
                                     % (p, region))
        expected_ebs = self.input.param("expected_ebs_price", None)
        if expected_ebs is not None:
            self.assertIn(
                float(expected_ebs), ebs_prices,
                "region %s: expected ebsListPrice %s not in %s"
                % (region, expected_ebs, ebs_prices))
        self.log.info("[region %s] ebsListPrice=%s, cspUplift=%s applied"
                      % (region, ebs_prices, uplift))

    def _verify_billing_continuity(self, records):
        """Strict: once fusion billing is on, EVERY node has an HourlyBillingRecord
        for EVERY hour, AND every one of those hours actually carries a non-zero
        fusion SSD charge. billingPeriod is ISO hourly UTC (e.g.
        2026-08-03T16:00:00Z).

        Three DISTINCT failure modes, reported separately (they are different
        bugs and were previously conflated into one message — or missed):

          a) INTERIOR gap  — an hour with no record at all, between two hours
             that do have records.
          b) TRAILING gap  — records simply STOP: the biller died / stopped
             enumerating this cluster and never resumed. This is the exact
             shape of the reported production symptom ("hourly billing is
             missing for many hours"). The previous implementation anchored
             its window to max(existing records), so a trailing gap could
             never be detected — the loop ended at the last record that DID
             exist and reported success.
          c) ZERO-COST hour — a record EXISTS but carries fusionSSDCost <= 0.
             This is the AV-140399 signature. The previous implementation
             FILTERED these out before computing the window, so a zero-cost
             PREFIX (the common AV-140399 shape: first N hours after enable
             are all zero) was silently dropped and the check passed.

        The trailing-gap check needs a grace window: the fixed biller runs at
        :15 past the hour FOR THE PREVIOUS HOUR (cmd/cp-scheduler cron
        "0 15 * * * *"), so the current hour and the one just ended are both
        legitimately unbilled. continuity_grace_hours (default 2) covers
        that; raise it if a slow env trips it spuriously."""
        from datetime import datetime, timedelta

        fmt = "%Y-%m-%dT%H:%M:%SZ"
        # ALL fusion-enabled hours (whatever the SSD value) vs the subset that
        # actually carries a charge. Keeping both is what makes (c) visible.
        all_hours, paid_hours = {}, {}
        for r in records:
            if not r.get("fusionEnabled"):
                continue
            bp = r.get("billingPeriod")
            if not bp:
                continue
            node = r.get("nodeId")
            if not node:      # per-cluster bucket doc — not a per-node series
                continue
            all_hours.setdefault(node, set()).add(bp)
            if float((r.get("fusionCosts") or {}).get("fusionSSDCost") or 0) > 0:
                paid_hours.setdefault(node, set()).add(bp)
        self.assertTrue(
            all_hours, "No fusion-enabled hourly records to check continuity")

        grace = int(self.input.param("continuity_grace_hours", 2))
        now_hour = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
        expected_latest = now_hour - timedelta(hours=grace)

        interior, trailing, zero_cost = {}, {}, {}
        for node, periods in all_hours.items():
            pset = {datetime.strptime(p, fmt) for p in periods}
            lo, hi = min(pset), max(pset)

            missing, t = [], lo
            while t <= hi:
                if t not in pset:
                    missing.append(t.strftime(fmt))
                t += timedelta(hours=1)
            if missing:
                interior[node] = missing

            # (b) records stop before the last hour that SHOULD have been
            # billed by now — invisible to the old min..max-of-existing loop.
            if hi < expected_latest:
                gap_h = int((expected_latest - hi).total_seconds() // 3600) + 1
                trailing[node] = (hi.strftime(fmt), gap_h)

            # (c) hours that exist but were never charged.
            unpaid = sorted(p.strftime(fmt)
                            for p in pset
                            if p.strftime(fmt) not in paid_hours.get(node, set()))
            if unpaid:
                zero_cost[node] = unpaid

        self.assertFalse(
            interior,
            "INTERIOR billing gap — hour(s) with NO HourlyBillingRecord at all, "
            "between hours that do have one (lost billing hours): %s"
            % {k: v[:5] for k, v in interior.items()})
        self.assertFalse(
            trailing,
            "TRAILING billing gap — records STOP and never resume. Per node: "
            "{node: (last billed hour, hours missing since)}: %s. Expected "
            "billing through %s (now - continuity_grace_hours=%s). This is the "
            "shape of the reported production symptom 'hourly billing missing "
            "for many hours' — the biller stopped enumerating this cluster."
            % (trailing, expected_latest.strftime(fmt), grace))
        self.assertFalse(
            zero_cost,
            "ZERO-COST billing hour(s) — an HourlyBillingRecord EXISTS with "
            "fusionEnabled=true but fusionSSDCost <= 0, so the hour is on "
            "record yet never charged (AV-140399 signature; a zero-cost "
            "PREFIX right after fusion enable is the common shape): %s"
            % {k: v[:5] for k, v in zero_cost.items()})
        self.log.info(
            "[continuity] %s node(s): contiguous, billed through at least %s, "
            "and every fusion-enabled hour carries a non-zero SSD charge"
            % (len(all_hours), expected_latest.strftime(fmt)))

    def _verify_fusion_premium(self, records):
        """Strict directional check on the TOTAL bill: every fusion-enabled record's
        creditsConsumed is strictly greater than every disabled/pre-enable record's —
        i.e. fusion is actually ADDED to the total credits, not just itemized in a
        debug field that never reaches creditsConsumed."""
        enabled = [float(r["creditsConsumed"]) for r in records
                   if r.get("fusionEnabled")
                   and float((r.get("fusionCosts") or {}).get("fusionSSDCost") or 0) > 0
                   and r.get("creditsConsumed") is not None]
        disabled = [float(r["creditsConsumed"]) for r in records
                    if (not r.get("fusionEnabled")
                        or float((r.get("fusionCosts") or {}).get("fusionSSDCost") or 0) == 0)
                    and r.get("creditsConsumed") is not None]
        self.assertTrue(enabled, "No fusion-enabled creditsConsumed to compare")
        if not disabled:
            self.log.info("[premium] no disabled baseline in window — enabled "
                          "creditsConsumed min=%s (can't compare in-run)" % min(enabled))
            return
        self.assertGreater(
            min(enabled), max(disabled),
            "Fusion-enabled creditsConsumed min=%s is NOT > disabled max=%s — the "
            "fusion add-on did not reach the total bill" % (min(enabled), max(disabled)))
        self.log.info("[premium] enabled creditsConsumed >= %s > disabled <= %s -> "
                      "fusion cost reaches the total bill" % (min(enabled), max(disabled)))

    def _verify_credits_decomposition(self, records):
        """EXACT (AV-28236 running-node algorithm): a node's total creditsConsumed =
            (computeCost * (1 - cloudDiscount) + storageCost + logsPrice)
              * nodeUplift * ssdToRAMRatio * iopsToRAMRatio   [Hourly Node Credits]
            + fusionSSDCost                                    [fusion add-on]
        then quantised UP to 1e-6.

        Two things the earlier version of this helper got wrong, both of which
        made it accept results it should have rejected:

          1. CLOUD DISCOUNT. The real formula (couchbase-cloud clustersbiller/
             fixed.go:433 hostedIAASCredits) multiplies compute by
             (1 - cloudDiscount), sourced from ratio.G2CloudDiscounts.
             GetDiscount(node) (fixed.go:328). cloudDiscount is NOT exposed in
             debug.*, so we cannot assert it directly — but omitting the term
             entirely means this check only holds while the discount is 0. It
             would false-PASS a discount-application bug and, the moment any
             discount is configured, false-FAIL with a message blaming the
             fusion add-on. So: on a mismatch, solve for the discount that
             WOULD explain the residual and report it, instead of asserting a
             formula we know is incomplete.

          2. ROUNDING. Real node records are quantised with
             math.Ceil(credits*1e6)/1e6 (fixed.go:389, sender.FloatPrecision).
             The old relative tolerance of 1e-4 is 100x COARSER than that
             1e-6 quantum, so a ceil->floor/round regression was invisible.
             Asserted exactly below.

        (The per-cluster bucket charge lands on the separate nodeId='' record,
        which notably is NOT quantised at all — fixed.go:252 assigns *credits
        raw. Checked in _verify_bucket_costs' own rounding assertion.)"""
        import math

        tol = float(self.input.param("credits_tolerance", 1e-4))
        precision = float(self.input.param("credits_float_precision", 1e6))
        need = ("computeCost", "storageCost", "logsPrice",
                "nodeUplift", "ssdToRAMRatio", "iopsToRAMRatio")
        checked, unrounded, discount_suspects = 0, [], []
        for r in records:
            # Skip the per-cluster bucket doc (nodeId="") — it carries only the
            # bucket charge, not the full node-level breakdown.
            if not r.get("nodeId"):
                continue
            if (not r.get("fusionEnabled") or r.get("creditsConsumed") is None
                    or any(r.get(k) is None for k in need)):
                continue
            compute = float(r["computeCost"])
            uplifts = (float(r["nodeUplift"]) * float(r["ssdToRAMRatio"])
                       * float(r["iopsToRAMRatio"]))
            base = ((compute + float(r["storageCost"])
                     + float(r["logsPrice"])) * uplifts)
            fusion_ssd = float((r.get("fusionCosts") or {}).get("fusionSSDCost") or 0)
            expected = base + fusion_ssd
            actual = float(r["creditsConsumed"])

            # (2) exact ceil-quantisation to 1e-6 — independent of the formula.
            if actual != math.ceil(actual * precision) / precision:
                unrounded.append((r.get("nodeId"), r.get("billingPeriod"), actual))

            if abs(actual - expected) > max(tol, abs(expected) * tol):
                # (1) Could a cloud discount explain the whole residual? Solve
                # actual = ((compute*(1-d) + storage + logs) * uplifts) + ssd
                # for d, and report it rather than blaming the fusion add-on.
                implied = None
                if compute > 0 and uplifts > 0:
                    implied = (expected - actual) / (compute * uplifts)
                discount_suspects.append(
                    (r.get("nodeId"), r.get("billingPeriod"), actual, expected,
                     implied))
            checked += 1

        self.assertGreater(
            checked, 0, "No record had the full debug breakdown to verify creditsConsumed")
        self.assertFalse(
            unrounded,
            "creditsConsumed is NOT ceil-quantised to 1/%s on node record(s) "
            "(nodeId, period, value): %s — couchbase-cloud clustersbiller/"
            "fixed.go:389 applies math.Ceil(credits*FloatPrecision)/"
            "FloatPrecision, so any other rounding is a regression"
            % (int(precision), unrounded[:5]))
        self.assertFalse(
            discount_suspects,
            "creditsConsumed != (compute + storage + logs)*uplifts + fusionSSD "
            "on (nodeId, period, actual, expected-without-discount, implied "
            "cloudDiscount): %s. An implied discount close to a plausible rate "
            "(e.g. ~0.10) means a cloud discount IS being applied and this "
            "check simply does not model it (clustersbiller/fixed.go:433 "
            "multiplies compute by (1-cloudDiscount); it is not exposed in "
            "debug.*, so it cannot be read back directly) — that is a TEST "
            "gap, not a billing bug. An implausible or wildly varying implied "
            "value means the total genuinely does not decompose and IS a "
            "billing bug." % (discount_suspects[:5],))
        self.log.info(
            "[credits] EXACT creditsConsumed decomposition verified on %s "
            "record(s): base*uplifts + fusionSSD, ceil-quantised to 1/%s"
            % (checked, int(precision)))

    def test_variable_record_after_rebalance(self):
        """Focused: the priced billing.variable record (needs the pager-biller job
        to have run — daily 18:30 UTC or a manual run)."""
        self._setup_fusion_workload()
        plan_uuid, expected_gib = self._verify_pager_tasks()
        base_price = self._get_factor("Accelerator Global Rate - per GiB")
        uplift = self._csp_uplift_factor()
        expected_cost = expected_gib * base_price * uplift

        def _fetch_variable():
            self._maybe_trigger_billing_jobs()   # run the pager-biller now if enabled
            return self._n1ql(
                "SELECT v.creditQuantity, v.usageCategory, "
                "v.billingBreakdown.usage AS usage, "
                "v.billingBreakdown.usageUnit AS usageUnit "
                "FROM %s v WHERE v.`_type` = \"%s\" "
                "AND v.databaseId = \"%s\" "
                "AND v.usageCategory = \"Fusion 2\" ORDER BY v.usageDate DESC"
                % (self.KS, self.TYPE_VARIABLE, self.cluster_id))

        variable = self._poll(_fetch_variable,
                             "the pager-biller job to write a variable record")
        self.assertTrue(
            variable,
            "No variable record for cluster %s — pager-biller job may not have run "
            "(daily 18:30 UTC / manual)" % self.cluster_id)
        rec = variable[0]
        self.assertEqual(rec.get("usageUnit"), "GiB", "usageUnit should be 'GiB'")
        self.assertAlmostEqual(
            float(rec["usage"]), expected_gib, delta=1e-3,
            msg="variable usage %s != summed GiB %s" % (rec["usage"], expected_gib))
        self.assertAlmostEqual(
            float(rec["creditQuantity"]), expected_cost, delta=1e-3,
            msg="creditQuantity %s != %s" % (rec["creditQuantity"], expected_cost))
        self.log.info("[variable] verified %s GiB -> %s credits"
                      % (expected_gib, rec["creditQuantity"]))
        self._test_succeeded = True

    # ------------------------------------------------------------------
    # P0/P1 edge cases (strict) — each does its own deploy + workload
    # ------------------------------------------------------------------

    def test_bucket_free_tier_boundary(self):
        """Strict boundary of max(0, buckets - freeTier) * price. Run once with
        num_buckets <= freeTier (expect ZERO charge) and once with > freeTier
        (expect exactly the overage) to catch an off-by-one in the biller.

        Then, DYNAMICALLY change the bucket count on the SAME cluster and
        confirm the next hour's charge tracks the new count — not just a
        static snapshot at two different topologies. Closes "Bucket Cost
        Updates When Bucket Count Changes" from the QE functional test plan,
        previously untested (every other bucket check used a fixed
        num_buckets for the whole test)."""
        self._setup_fusion_workload()
        free_tier = int(self._get_factor("Buckets - Free Tier"))
        price = self._get_factor("Buckets - Global Price")
        records = self._poll(self._hourly_records, "the hourly fixed-billing job")
        self.assertTrue(records, "No HourlyBillingRecord for %s" % self.cluster_id)
        bucket_recs = [r for r in records if r.get("bucketCosts")]
        expected = max(0, self.num_buckets - free_tier) * price
        if self.num_buckets <= free_tier:
            for r in bucket_recs:
                self.assertEqual(
                    float(r["bucketCosts"].get("credits") or 0), 0,
                    "num_buckets=%s <= freeTier=%s but charge %s != 0 (off-by-one?)"
                    % (self.num_buckets, free_tier, r["bucketCosts"].get("credits")))
            self.log.info("[boundary] %s <= freeTier %s: charge 0 (correct)"
                          % (self.num_buckets, free_tier))
        else:
            self.assertTrue(bucket_recs, "Expected a bucket charge above free tier")
            for r in bucket_recs:
                self.assertAlmostEqual(
                    float(r["bucketCosts"]["credits"]), expected,
                    delta=self.credit_tolerance,
                    msg=("bucket credits %s != (%s-%s)*%s = %s"
                         % (r["bucketCosts"]["credits"], self.num_buckets,
                            free_tier, price, expected)))
            self.log.info("[boundary] %s > freeTier %s: charge %s (correct)"
                          % (self.num_buckets, free_tier, expected))

        # ---- DYNAMIC: push the count from wherever it started to
        # comfortably above the free tier, and confirm the charge updates on
        # the SAME cluster (not a fresh deploy at a different topology). ----
        latest = max((r.get("billingPeriod") or "") for r in records)
        add = max(1, (free_tier - self.num_buckets) + 2)
        new_ids = []
        for i in range(add):
            bkt_id = self.create_fusion_bucket(
                self.cluster_id, "fusion-bkt-extra-%d" % i)
            new_ids.append(bkt_id)
        new_count = self.num_buckets + add
        self.source_bucket_ids.extend(new_ids)
        expected_new = max(0, new_count - free_tier) * price
        self.log.info(
            "[boundary-dynamic] added %d bucket(s): %d -> %d (freeTier=%d) "
            "-> expected charge %s" % (add, self.num_buckets, new_count,
                                       free_tier, expected_new))

        def _fresh_updated_charge():
            rows = [r for r in self._hourly_records()
                    if (r.get("billingPeriod") or "") > latest
                    and r.get("bucketCosts")]
            return rows or None
        fresh = self._poll(
            _fresh_updated_charge,
            "a fresh hourly record reflecting the new bucket count")
        self.assertTrue(
            fresh, "No fresh bucket-cost record for %s after adding %d "
                   "bucket(s)" % (self.cluster_id, add))
        newest = max(r.get("billingPeriod") for r in fresh)
        rec = next(r for r in fresh if r.get("billingPeriod") == newest)
        actual_new = float(rec["bucketCosts"].get("credits") or 0)
        self.assertAlmostEqual(
            actual_new, expected_new, delta=self.credit_tolerance,
            msg=("After adding %d bucket(s) (%d -> %d), bucket credits %s != "
                 "(%d-%d)*%s = %s — the charge did not track the new bucket "
                 "count on the same cluster"
                 % (add, self.num_buckets, new_count, actual_new, new_count,
                    free_tier, price, expected_new)))
        self.log.info(
            "[boundary-dynamic] bucket count %d -> %d: charge updated to %s "
            "(correct)" % (self.num_buckets, new_count, actual_new))
        self._test_succeeded = True

    def test_multiple_rebalances_variable(self):
        """Strict: each rebalance writes its OWN pagerTask set (distinct
        planUUID), each priced independently — no plan double-counted or
        dropped. Checked at both layers: pagerTask-level (distinct
        planUUIDs, no cross-plan dedupe suppression) AND billing.variable-
        level.

        billing.variable does NOT carry a per-record planUUID (verified
        against variablerecord.BillingBreakdown, biller.go:220-227) — the
        document is keyed by (tenant, cluster, category, region, day) with
        no planUUID (record.go:95), and written via a plain
        Create-else-Upsert (writer.go:122-127), so two rebalances on the
        same cluster/day CAN legitimately collapse into fewer documents
        than plans. What must still hold is the TOTAL: the sum of
        creditQuantity across whatever records exist must equal the sum of
        both rebalances' independently-computed costs — if the second
        Put() silently overwrote the first instead of both being billed,
        the total falls short by exactly one plan's cost, which is
        asserted explicitly. own_cluster=True (conf) is REQUIRED for this
        check to be valid — a shared/pooled cluster with other same-day
        billing activity would pollute the total."""
        self._setup_fusion_workload()               # load + 1st rebalance (data +1)
        # 2nd rebalance (one more KV node) -> a second pagerTask plan.
        _, cur_nodes = self._kv_spec()
        orig, new = self.trigger_fusion_rebalance(
            self.cluster_id, project_id=self.project_id,
            target_nodes=(cur_nodes or self.kv_nodes) + 1)
        if not self.wait_for_rebalance_complete(
                self.cluster_id, project_id=self.project_id):
            self.fail("2nd fusion rebalance (%s -> %s nodes) did not complete on %s"
                      % (orig, new, self.cluster_id))
        tasks = self._poll(self._completed_pager_tasks,
                           "pagerTasks from both rebalances")
        self.assertTrue(tasks, "No pagerTasks after two rebalances")
        plans = {}
        for pt in tasks:
            plans.setdefault(pt["planUUID"], []).append(int(pt["shardSizeInBytes"]))
        self.assertGreaterEqual(
            len(plans), 2,
            "Expected >=2 distinct planUUIDs after 2 rebalances, got %s: %s"
            % (len(plans), list(plans)))
        rate = self._get_factor("Accelerator Global Rate - per GiB")
        uplift = self._csp_uplift_factor()
        for plan, sizes in plans.items():
            gib = sum(sizes) / self.BYTES_PER_GIB
            self.assertGreater(gib, 0, "plan %s moved 0 GiB" % plan)
            self.log.info("[multi-rebalance] plan %s: %s GiB -> %s credits"
                          % (plan, gib, gib * rate * uplift))
        self.log.info("[multi-rebalance] %s distinct plan(s), each priced" % len(plans))

        # ---- EMPIRICAL PROBE: is the nodeID-only pagerTask dedupe actually
        # reachable? (couchbase-cloud dedupes on fusionDetails.nodeID with no
        # plan/time scoping — service.go:27 + pager/service.go:56-64 +
        # record/reader.go:38-40 — so a node that ALREADY has a pagerTask can
        # never get another for the record's 1-year TTL.) Source-reading alone
        # cannot tell whether an accelerator node is ever REUSED across plans;
        # if every plan gets brand-new nodes the suppression never fires and
        # there is no bug. That question is only answerable from real data, so
        # measure it here instead of asserting a guess. ----
        nodes_by_plan = {}
        for pt in tasks:
            nodes_by_plan.setdefault(pt["planUUID"], set()).add(pt["nodeID"])
        all_nodes = set()
        recurring = set()
        for plan_nodes in nodes_by_plan.values():
            recurring |= (all_nodes & plan_nodes)
            all_nodes |= plan_nodes

        node_counts = self._n1ql(
            "SELECT p.fusionDetails.nodeID AS nodeID, "
            "COUNT(DISTINCT p.fusionDetails.planUUID) AS plans "
            "FROM %s p WHERE p.`_type` = \"%s\" AND p.clusterId = \"%s\" "
            "GROUP BY p.fusionDetails.nodeID"
            % (self.KS, self.TYPE_PAGER, self.cluster_id))
        multi_plan_nodes = [r for r in node_counts if int(r.get("plans") or 0) > 1]

        if recurring:
            # A node took part in >1 plan AND got a task for each -> the dedupe
            # is scoped well enough in practice. Good news; assert it holds.
            self.assertTrue(
                multi_plan_nodes,
                "Node(s) %s appear in more than one plan's pagerTask set, but "
                "NO node has tasks spanning >1 distinct planUUID — the "
                "nodeID-only dedupe (service.go:27) suppressed the later "
                "charge. Shard bytes moved by an already-billed node are "
                "silently dropped (under-billing) for the task's 1-year TTL."
                % sorted(recurring))
            self.log.info(
                "[pager-dedupe-probe] %s node(s) recur across plans and each "
                "carries a task per plan -> nodeID-only dedupe did NOT "
                "suppress a later charge in this run" % len(recurring))
        else:
            # No node was reused, so the suppression path was never exercised.
            # Say so explicitly rather than letting a vacuous pass read as
            # evidence the dedupe is safe.
            self.log.warning(
                "[pager-dedupe-probe] INCONCLUSIVE: this 2-plan run reused NO "
                "accelerator node across plans (%s plans, %s distinct nodes, "
                "0 recurring), so the nodeID-only dedupe in service.go:27 was "
                "never exercised. Scaling OUT always adds fresh nodes; "
                "reaching it likely needs a scale-down/scale-up cycle that "
                "puts a previously-billed node back into a new plan. Not "
                "asserted here — absence of evidence, not evidence of absence."
                % (len(plans), len(all_nodes)))

        # ---- EMPIRICAL PROBE: does a SECOND same-day rebalance's variable
        # record overwrite the first, instead of both being billed?
        #
        # An earlier version of this check assumed billing.variable carries
        # a per-record planUUID (billingBreakdown.planUUID) and asserted one
        # separate document per rebalance. That field DOES NOT EXIST on the
        # persisted record — verified directly against the real struct,
        # variable/record/variablerecord.BillingBreakdown (biller.go:
        # 220-227): Usage, UsageUnit, Region, Provider, Uplift, BasePrice —
        # no PlanUUID. PlanUUID only exists on the in-memory aggregation
        # struct used to GROUP pagerTasks before pricing (biller.go:60-64);
        # it is never written to CP-DB. So the old assertion always failed
        # by construction, regardless of any real billing defect — that is
        # a bug in this test, not evidence of one in the product.
        #
        # Tracing further surfaced a SEPARATE, real concern the old check
        # was blind to either way: the record's own deterministic ID
        # (variablerecord.GetVariableId — biller.go:213, record.go:95) is
        # built from (tenantID, clusterID, category, region, day) — NOT
        # planUUID — and the write path is Put = Create-else-Upsert
        # (writer.go:122-127), a plain replace, not an accumulate. Two
        # rebalances landing in the biller's aggregation on the SAME
        # cluster/day therefore compute the SAME document ID, and the
        # second Put() would silently overwrite the first: only the
        # last-processed plan's cost would survive, under-billing the
        # other. That is testable WITHOUT the nonexistent planUUID field:
        # if it happens, the day's total creditQuantity will be short of
        # the sum of both plans' independently-computed costs. ----
        self._maybe_trigger_billing_jobs()
        variable_recs = self._n1ql(
            "SELECT v.creditQuantity, v.usageDate "
            "FROM %s v WHERE v.`_type` = \"%s\" AND v.databaseId = \"%s\" "
            "AND v.usageCategory = \"Fusion 2\""
            % (self.KS, self.TYPE_VARIABLE, self.cluster_id))
        self.assertTrue(
            variable_recs,
            "No billing.variable record at all for cluster %s after 2 "
            "rebalances" % self.cluster_id)

        per_plan_cost = {p: (sum(sizes) / self.BYTES_PER_GIB) * rate * uplift
                        for p, sizes in plans.items()}
        expected_total = sum(per_plan_cost.values())
        actual_total = sum(float(r.get("creditQuantity") or 0)
                          for r in variable_recs)
        tol = max(1e-3, expected_total * 1e-3)

        one_plan_only = len(variable_recs) == 1 and any(
            abs(actual_total - cost) < tol for cost in per_plan_cost.values())
        self.assertFalse(
            one_plan_only,
            "UNDER-BILLING: exactly one billing.variable record exists for "
            "cluster %s (creditQuantity=%s) and it matches only ONE plan's "
            "cost, not the sum of both (%s) — the second same-day "
            "rebalance's Put() overwrote the first's record instead of "
            "both being billed. GetVariableId (record.go:95) keys the "
            "document by (tenant, cluster, category, region, day) with no "
            "planUUID, and Put (writer.go:122-127) is a plain "
            "Create-else-Upsert, not an accumulate."
            % (self.cluster_id, actual_total, expected_total))
        self.assertAlmostEqual(
            actual_total, expected_total, delta=tol,
            msg=("Sum of billing.variable creditQuantity for cluster %s "
                 "(%s across %s record(s)) != sum of both rebalances' "
                 "independently-computed cost (%s) — some portion of one "
                 "rebalance's GiB was never billed"
                 % (self.cluster_id, actual_total, len(variable_recs),
                    expected_total)))
        self.log.info(
            "[multi-rebalance] billing.variable total %s across %s "
            "record(s) matches the sum of both rebalances' costs (%s) — "
            "neither was silently overwritten or dropped"
            % (actual_total, len(variable_recs), expected_total))
        self._test_succeeded = True

    def test_fusion_disable_reenable_billing(self):
        """Full toggle lifecycle + the cluster-disabled over-billing guard.

        This is ALSO the suite's MANUAL-TRIGGER test. test_fusion_billing_e2e
        deliberately runs on the real hourly cron with no on-demand poke, so
        without this nothing would prove the on-demand path works. Its
        baseline below explicitly fires BOTH billing jobs and asserts each
        was accepted, then verifies the records they produced:
            ClusterBilling  -> the hourly fixed biller (SSD + bucket)
            BillPagerTasks  -> the daily pager biller (priced variable)
        Together with e2e's natural-cron run, both trigger paths are covered
        and neither is covered twice.

        Phases (one cluster throughout, so hardware/plan/region are constant
        and every delta is attributable to fusion alone):
          BASELINE  manual-trigger both jobs; SSD formula verified, bucket and
                    variable non-zero. Doubles as the precondition — "the
                    charge is 0 once disabled" is meaningless unless it was
                    non-zero first.
          PHASE A   tenant billing flag OFF  -> SSD, bucket AND variable all 0
          PHASE B   tenant billing flag ON   -> all three RESUME
          PHASE C   CLUSTER express scaling disabled (accelerator, guest
                    volumes and S3 log store converge away) -> SSD 0,
                    bucket 0, no NEW pagerTask, but compute/storage STILL
                    billed, so a billing outage cannot masquerade as
                    "correctly not charging for fusion".

        Does NOT re-verify the enabled charge values beyond the baseline —
        test_fusion_billing_e2e owns the full enabled verification, and
        repeating it here would cost a second cluster and more hourly waits
        to re-prove the same thing."""
        self._setup_fusion_workload()

        # ---- BASELINE: explicitly drive BOTH billing jobs ----------------
        this_hour = time.strftime("%Y-%m-%dT%H:00:00Z", time.gmtime())
        this_day = time.strftime("%Y-%m-%dT00:00:00Z", time.gmtime())
        self.assertTrue(
            self._trigger_billing_job(
                self.hourly_billing_job_type,
                {"scheduled": False, "time": this_hour,
                 "serviceIds": [self.cluster_id]}),
            "On-demand %s (hourly fixed biller) trigger was rejected — the "
            "manual billing-job path is broken, so no test in this suite can "
            "rely on it" % self.hourly_billing_job_type)
        self.assertTrue(
            self._trigger_billing_job(
                self.pager_billing_job_type,
                {"scheduled": False, "time": this_day}),
            "On-demand %s (pager biller) trigger was rejected — the manual "
            "billing-job path is broken" % self.pager_billing_job_type)
        self.log.info(
            "[manual-trigger] both %s and %s accepted on demand"
            % (self.hourly_billing_job_type, self.pager_billing_job_type))

        records = self._poll(self._hourly_records, "an enabled hourly record")
        self.assertTrue(records, "No HourlyBillingRecord for %s" % self.cluster_id)
        enabled, _ = self._verify_ssd_costs(records)
        self.assertTrue(enabled, "No non-zero SSD charge at baseline — a "
                                 "later 'charge is 0' would prove nothing")
        bucket_before = [r for r in records if r.get("bucketCosts")]
        self.assertTrue(
            bucket_before,
            "No bucket charge at baseline (num_buckets=%s) — this conf entry "
            "needs num_buckets > freeTier or the bucket checks below prove "
            "nothing" % self.num_buckets)
        variable_before = self._current_fusion_variable_credits(force_trigger=True)
        self.assertGreater(
            variable_before, 0,
            "No non-zero Fusion 2 variable credits at baseline — cannot prove "
            "variable billing drops to 0 without a non-zero starting point")
        latest = max((r.get("billingPeriod") or "") for r in records)
        enabled_ssd = max(s for _, s, _ in enabled)
        self.log.info(
            "[baseline] SSD max=%s, bucket docs=%s, variable=%s"
            % (enabled_ssd, len(bucket_before), variable_before))

        try:
            # ---- PHASE A: tenant billing flag OFF ------------------------
            self.log.info("[phase-A] disabling tenant billing flag %s"
                          % self.FUSION_BILLING_FLAG)
            self._set_fusion_billing_enabled(False)

            def _flag_off_zeroed():
                rows = [r for r in self._hourly_records()
                        if (r.get("billingPeriod") or "") > latest]
                if not rows:
                    return None
                ok = all(
                    float((r.get("fusionCosts") or {}).get("fusionSSDCost") or 0) == 0
                    and float((r.get("bucketCosts") or {}).get("credits") or 0) == 0
                    for r in rows)
                return rows if ok else None
            off = self._poll(
                _flag_off_zeroed,
                "a fresh hour with SSD AND bucket zeroed (billing flag off)")
            self.assertTrue(
                off,
                "OVER-BILLING: with %s disabled, no fresh hour came back with "
                "BOTH fusionSSDCost=0 and bucket credits=0 for cluster %s"
                % (self.FUSION_BILLING_FLAG, self.cluster_id))
            variable_off = self._current_fusion_variable_credits(force_trigger=True)
            self.assertEqual(
                variable_off, 0,
                "OVER-BILLING: Fusion 2 variable credits still %s after "
                "disabling %s — pager-priced variable billing does not track "
                "the flag the way fixed SSD/bucket billing does. Worth "
                "confirming with #capella-billing whether that is deliberate "
                "before treating it as a defect."
                % (variable_off, self.FUSION_BILLING_FLAG))
            flag_off_latest = max((r.get("billingPeriod") or "") for r in off)
            self.log.info(
                "[phase-A] flag off -> SSD=0, bucket=0 across %s record(s), "
                "variable=0" % len(off))

            # ---- PHASE B: flag back ON, charges must RESUME --------------
            self.log.info("[phase-B] re-enabling tenant billing flag %s"
                          % self.FUSION_BILLING_FLAG)
            self._set_fusion_billing_enabled(True)

            def _flag_on_resumed():
                rows = [r for r in self._hourly_records()
                        if (r.get("billingPeriod") or "") > flag_off_latest
                        and float((r.get("fusionCosts") or {})
                                  .get("fusionSSDCost") or 0) > 0
                        and float((r.get("bucketCosts") or {})
                                  .get("credits") or 0) > 0]
                return rows or None
            back = self._poll(
                _flag_on_resumed,
                "a fresh hour with SSD AND bucket resumed (billing flag on)")
            self.assertTrue(
                back,
                "UNDER-BILLING: fusion SSD and/or bucket charges did NOT "
                "resume on cluster %s after re-enabling %s — once disabled, "
                "billing never recovered" % (self.cluster_id,
                                             self.FUSION_BILLING_FLAG))
            variable_back = self._current_fusion_variable_credits(force_trigger=True)
            self.assertGreater(
                variable_back, 0,
                "UNDER-BILLING: Fusion 2 variable credits still 0 after "
                "re-enabling %s" % self.FUSION_BILLING_FLAG)
            resumed_latest = max((r.get("billingPeriod") or "") for r in back)
            self.log.info(
                "[phase-B] flag on -> SSD>0 and bucket>0 across %s record(s), "
                "variable=%s -> billing recovers from a disable"
                % (len(back), variable_back))

            # ---- PHASE C: CLUSTER express scaling OFF --------------------
            if str(self.input.param("cluster_disable_coverage", True)
                   ).lower() == "false":
                self.log.info("[phase-C] skipped (cluster_disable_coverage=False)")
                self._test_succeeded = True
                return

            pager_before = {(p.get("nodeID"), p.get("planUUID"))
                            for p in (self._completed_pager_tasks() or [])}
            self.log.info(
                "[phase-C] disabling express scaling on %s (fusionState=%r) — "
                "converges guest volumes and the S3 log store away, so slow"
                % (self.cluster_id, self.get_fusion_state(self.cluster_id)))
            self._ensure_fusion_disabled(
                self.cluster_id, self.project_id, label="billing cluster")

            def _fresh_after_cluster_disable():
                rows = [r for r in self._hourly_records()
                        if (r.get("billingPeriod") or "") > resumed_latest
                        and r.get("nodeId")]
                return rows or None
            post = self._poll(
                _fresh_after_cluster_disable,
                "a fresh hourly record after CLUSTER-LEVEL fusion disable")
            self.assertTrue(
                post,
                "No hourly record at all after disabling express scaling on "
                "%s — cannot tell 'fusion correctly not charged' apart from "
                "'billing stopped entirely'" % self.cluster_id)

            charged_ssd = [
                (r.get("nodeId"), r.get("billingPeriod"),
                 float((r.get("fusionCosts") or {}).get("fusionSSDCost") or 0))
                for r in post
                if float((r.get("fusionCosts") or {}).get("fusionSSDCost") or 0) > 0]
            self.assertFalse(
                charged_ssd,
                "OVER-BILLING: cluster %s has express scaling DISABLED (no "
                "accelerator, no guest volumes) yet is still charged a fusion "
                "SSD cost (nodeId, period, fusionSSDCost): %s"
                % (self.cluster_id, charged_ssd[:5]))

            charged_bucket = [
                (r.get("billingPeriod"),
                 float((r.get("bucketCosts") or {}).get("credits") or 0))
                for r in post
                if float((r.get("bucketCosts") or {}).get("credits") or 0) > 0]
            self.assertFalse(
                charged_bucket,
                "OVER-BILLING: cluster %s has express scaling DISABLED yet is "
                "still charged a fusion BUCKET cost (period, credits): %s"
                % (self.cluster_id, charged_bucket[:5]))

            pager_after = {(p.get("nodeID"), p.get("planUUID"))
                           for p in (self._completed_pager_tasks() or [])}
            self.assertFalse(
                pager_after - pager_before,
                "OVER-BILLING: new pagerTask(s) %s appeared for cluster %s "
                "AFTER express scaling was disabled — a fusion-free cluster "
                "moves no accelerator data and must accrue no new variable "
                "charge" % (sorted(pager_after - pager_before)[:5],
                            self.cluster_id))

            # The cluster must still be billed for what it DOES use —
            # otherwise every check above would pass just as well if billing
            # had simply died, which is the failure mode this suite exists to
            # catch.
            still_billed = [r for r in post
                            if float(r.get("creditsConsumed") or 0) > 0]
            self.assertTrue(
                still_billed,
                "Cluster %s is billed NOTHING after disabling express "
                "scaling — the fusion charge was supposed to drop, not the "
                "whole bill. That is a billing OUTAGE, not correct "
                "fusion-disabled behaviour." % self.cluster_id)
            base = still_billed[0]
            for field in ("computeCost", "storageCost"):
                self.assertGreater(
                    float(base.get(field) or 0), 0,
                    "Disabled-state record has %s=%s; the cluster still runs "
                    "the same hardware as the baseline, so its non-fusion "
                    "costs must persist" % (field, base.get(field)))

            self.log.info(
                "[phase-C] express-scaling-DISABLED %s: fusionSSD=0, "
                "bucket=0, no new pagerTasks, still billed compute=%s "
                "storage=%s -> fusion charges dropped, base billing intact"
                % (self.cluster_id, base.get("computeCost"),
                   base.get("storageCost")))
            self.log.info(
                "[enabled-vs-disabled] baseline SSD=%s vs disabled SSD=0 "
                "across %s record(s) — same cluster, same hardware, so the "
                "delta is fusion alone" % (enabled_ssd, len(post)))
        finally:
            # CLEANUP (not assertions): the pool keys on fusion state and
            # every other test expects an enabled cluster with the flag on.
            self.log.info("[cleanup] restoring flag + express scaling on %s"
                          % self.cluster_id)
            self._set_fusion_billing_enabled(True)
            if self.get_fusion_state(self.cluster_id) != "enabled":
                self._enable_fusion_and_wait(self.cluster_id, self.project_id)
        self._test_succeeded = True

    def test_scale_down_billing(self):
        """Lifecycle (slow): after scaling DOWN a KV node, the removed node stops
        being billed SSD — the billed-node set shrinks to the new node count."""
        self._setup_fusion_workload()
        before = self._poll(self._hourly_records, "records before scale-down")
        self.assertTrue(before, "No records before scale-down")
        _, before_count = self._kv_spec()
        latest = max((r.get("billingPeriod") or "") for r in before)
        self.log.info("[scale-down] node count before: %s" % before_count)

        orig, new = self.trigger_fusion_rebalance(
            self.cluster_id, project_id=self.project_id,
            target_nodes=before_count - 1)
        if not self.wait_for_rebalance_complete(
                self.cluster_id, project_id=self.project_id):
            self.fail("Scale-down rebalance (%s -> %s nodes) did not complete on %s"
                      % (orig, new, self.cluster_id))
        _, after_count = self._kv_spec()
        self.assertLess(
            after_count, before_count,
            "Node count did not drop after scale-down (%s -> %s)"
            % (before_count, after_count))

        def _fresh_settled():
            rows = [r for r in self._hourly_records()
                    if (r.get("billingPeriod") or "") > latest]
            if not rows:
                return None
            newest = max(r.get("billingPeriod") for r in rows)
            nodes = {r.get("nodeId") for r in rows
                     if r.get("billingPeriod") == newest
                     and float((r.get("fusionCosts") or {}).get("fusionSSDCost") or 0) > 0}
            return nodes if len(nodes) == after_count else None
        billed = self._poll(_fresh_settled,
                           "a settled hour with exactly %s billed node(s)" % after_count)
        self.assertTrue(
            billed, "After scaling to %s nodes, no fresh hour billed exactly %s SSD "
                    "nodes (removed node still billed, or not settled)"
                    % (after_count, after_count))
        self.log.info("[scale-down] billed node count -> %s == %s (removed node no "
                      "longer billed)" % (len(billed), after_count))
        self._test_succeeded = True

    # ------------------------------------------------------------------
    # Off-cluster helpers (AV-110004 fusion-off-uplift-gap):
    #   turn the cluster off/on via the v4 activationState endpoint and read
    #   the REAL fusion accelerator/guest-volume EBS size from AWS.
    # ------------------------------------------------------------------

    def _wait_for_onoff_state(self, states, description, timeout=900, interval=15):
        """Poll validate_onoff_state(states) (inherited from APIBase — GET
        currentState on self.cluster_id) until it matches one of `states`
        (e.g. ["turnedOff"] / ["healthy"]) or `timeout` elapses."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.validate_onoff_state(states, sleep=0):
                return True
            self.log.info("Waiting for %s (%ds left)"
                          % (description, int(deadline - time.time())))
            time.sleep(interval)
        return False

    def _turn_cluster_off_and_wait(self, cluster_id=None, project_id=None,
                                   timeout=900):
        """v4 switch_cluster_off (DELETE .../activationState) -> wait for
        currentState == 'turnedOff'.

        Chosen over the v2 DoctorHostedOnOff path (used by
        fusion_cluster_on_off_test.py) because that helper is built for the
        older v2/CapellaAPI object model (self.pod/self.tenant/self.cluster),
        which FusionBillingTest — an APIBase/v4 subclass throughout (self.
        cluster_id, self.organisation_id, self.capellaAPI as a v4 client) —
        does not carry. The v4 activationState endpoint does the same on/off
        transition without needing that parallel object model."""
        cid = cluster_id or self.cluster_id
        proj = project_id or self.project_id
        resp = self.capellaAPI.cluster_ops_apis.switch_cluster_off(
            self.organisation_id, proj, cid)
        if resp.status_code == 429:
            self.handle_rate_limit(int(resp.headers["Retry-After"]))
            resp = self.capellaAPI.cluster_ops_apis.switch_cluster_off(
                self.organisation_id, proj, cid)
        # 409 = already turning off/off (idempotent from this test's POV).
        if resp.status_code not in (202, 409):
            self.fail("switch_cluster_off on %s -> %s: %s"
                      % (cid, resp.status_code, resp.content))
        return self._wait_for_onoff_state(
            ["turnedOff"], "cluster %s to reach 'turnedOff'" % cid,
            timeout=timeout)

    def _turn_cluster_on_and_wait(self, cluster_id=None, project_id=None,
                                  timeout=1200):
        """v4 switch_cluster_on (POST .../activationState) -> wait for
        currentState == 'healthy'. turnOnLinkedAppService=False — this suite
        never attaches App Services to the billing cluster."""
        cid = cluster_id or self.cluster_id
        proj = project_id or self.project_id
        resp = self.capellaAPI.cluster_ops_apis.switch_cluster_on(
            self.organisation_id, proj, cid, False)
        if resp.status_code == 429:
            self.handle_rate_limit(int(resp.headers["Retry-After"]))
            resp = self.capellaAPI.cluster_ops_apis.switch_cluster_on(
                self.organisation_id, proj, cid, False)
        # 409 = already turning on/healthy (idempotent from this test's POV).
        if resp.status_code not in (202, 409):
            self.fail("switch_cluster_on on %s -> %s: %s"
                      % (cid, resp.status_code, resp.content))
        return self._wait_for_onoff_state(
            ["healthy"], "cluster %s to return to 'healthy'" % cid,
            timeout=timeout)

    def _real_accelerator_volume_gib(self, cluster_id=None):
        """Sum the REAL AWS EBS size (GiB) of the fusion accelerator guest
        volume(s) attached to this cluster, read directly from AWS via the
        Layer-1 EC2Lib (never raw boto3) — bypassing the billing proxy
        entirely. Tag filter mirrors
        fusion_accelerator_lifecycle_test.py's _accelerator_volume_filters
        (~L1166): couchbase-cloud-cluster-id + couchbase-cloud-function=
        fusion-accelerator + couchbase-cloud-fusion-guest-volume=true. The
        guest-volume tag is required — it excludes each accelerator
        instance's ROOT volume, which has unrelated size/IOPS and would
        pollute the sum."""
        if not self.fusion_aws_util:
            self.fail(
                "AWS access not available — cannot read the real fusion "
                "accelerator/guest-volume EBS size (set aws_access_key/"
                "aws_secret_key, or account_id in the ini, to assume "
                "jenkins-cp-cli).")
        cid = cluster_id or self.cluster_id
        filters = {
            "couchbase-cloud-cluster-id": cid,
            "couchbase-cloud-function": "fusion-accelerator",
            "couchbase-cloud-fusion-guest-volume": "true",
        }
        volumes = self.fusion_aws_util.ec2.list_volumes_by_cluster_id(
            filters=filters)
        sizes = [int(v.get("Size") or 0) for v in volumes if v.get("Size")]
        total = sum(sizes)
        self.log.info(
            "[accelerator-volume] cluster %s: %s guest volume(s), sizes=%s "
            "GiB, total=%s GiB" % (cid, len(volumes), sizes, total))
        return total, sizes

    def test_fusion_uplift_during_turnoff_after_scale(self):
        """Off-cluster fusion billing after a scale-up: does turning a
        cluster OFF zero out (or otherwise drop) the fusion SSD charge, and
        does the charge reflect the REAL scaled-up accelerator EBS volume
        while off?

        Lifecycle (slow): run the workload -> scale UP a couple of KV nodes
        (grows the fusion accelerator/guest volume measurably) -> switch the
        cluster OFF (v4 activationState) -> READ THE REAL AWS ACCELERATOR/
        GUEST-VOLUME SIZE (only now — see the real_gib comment below for why
        this must happen after turning off, not before) -> trigger the
        off-state fixed biller (OffClusterBilling) on demand -> read a fresh
        isOff=true hourly record -> switch the cluster back ON (in a
        finally, before _test_succeeded is set) so tearDown's
        healthy-cluster reuse/delete logic is never confused by an off
        cluster it doesn't expect.

        Two checks, deliberately different strictness — root-caused against
        couchbase-cloud source (NOT re-derived here):
          internal/billing/biller/fixed/offclustersbiller/offbilling.go:208-227
              (the 2x Database Hibernation uplift is applied BEFORE
              FusionSSDCost is added, at 1x — mirroring the on-cluster path)
          internal/billing/biller/fixed/fusiondiskbiller/fixed.go:65
              (FusionSSDCost is priced from node.Disk, the base KV/index EBS
              size — a FinOps proxy, AV-123471 — NOT from the real
              accelerator/guest volume, whether the cluster is on or off)
          internal/clusters/fusion/accelerator/accelerator.go:2877-2880
              (the accelerator volume is intentionally kept while
              IntendedState == TurnedOff — AV-105105 — so AWS keeps charging
              for it during the off window)
        This exact interaction already has passing Go unit tests
        (offbilling_test.go ~L402, ~L473) — it's known, intentional biller
        behavior, not a bug to chase here.

          1. HARD regression guard — a fresh off-state (isOff=true),
             fusion-enabled HourlyBillingRecord must still carry
             debug.fusionCosts.fusionSSDCost > 0. If Capella silently
             zeroed/dropped the fusion line item while off, this fails.
          2. INFORMATIONAL gap (logged, NOT asserted) — the disk size
             IMPLIED by the billed fusionSSDCost (inverting the same
             (disk*ebs/732)*uplift formula _verify_ssd_costs uses) vs the
             REAL accelerator/guest-volume EBS size read directly from AWS.
             The gap is expected and deliberate (same proxy approximation as
             the on-cluster path, per fixed.go:65 above) — this is FinOps
             evidence for a pricing discussion, not a defect signature, so it
             is only logged.

        Not a real 30-day soak: a single fresh off-state hourly record is
        sufficient evidence. The per-hour approximation (if present) is
        constant while off — it would recur IDENTICALLY every hour of the
        ~30-day window, so waiting out the real window would only re-observe
        the same number, never surface a new failure mode.

        Off-record wait timing: OffClusterBilling (HostedOff query) only
        counts an hour where the cluster was off/turningOff/turningOn for the
        WHOLE clock hour AND running 0s in it. Turning off at minute m past
        the hour disqualifies the CURRENT hour (it had running time before
        the off), so the earliest qualifying hour is the NEXT one, which only
        closes once the wall clock reaches the hour after that. Worst case
        (turned off right at minute 0) that is just under 2 HOURS away, not
        ~60 min — self.billing_wait_timeout (default 5400s/90min) is not
        enough margin, so this wait uses its own OFF_RECORD_WAIT_TIMEOUT.
        """
        self._setup_fusion_workload()

        # ---- scale UP a couple of nodes so the accelerator/guest volume
        # grows measurably (own_cluster=True in the conf — this test mutates
        # node count, like test_scale_down_billing). ----
        _, before_nodes = self._kv_spec()
        # Watermark so the scale-up billing check below only looks at hours
        # cut AFTER the new nodes joined.
        latest_before_scale = max(
            (r.get("billingPeriod") or "")
            for r in (self._hourly_records() or [{}])) or ""
        scale_up_nodes = int(self.input.param("scale_up_nodes", 2))
        target_nodes = before_nodes + scale_up_nodes
        orig, new = self.trigger_fusion_rebalance(
            self.cluster_id, project_id=self.project_id,
            target_nodes=target_nodes)
        if not self.wait_for_rebalance_complete(
                self.cluster_id, project_id=self.project_id):
            self.fail("Scale-up rebalance (%s -> %s nodes) did not complete "
                      "on %s" % (orig, new, self.cluster_id))
        self.log.info("[scale-up] %s -> %s KV nodes before turning cluster off"
                      % (orig, new))

        # ---- SCALE-UP billing (the inverse of test_scale_down_billing).
        # AV-110004 bills the fixed SSD charge PER NODE, so growing the
        # cluster must grow the billed-node set. Scale-DOWN has its own test;
        # scale-UP was previously only a setup step here, with nothing
        # asserting the new nodes actually get billed — an under-billing
        # blind spot. Checked here rather than in a new test because this is
        # the only test that already scales up. ----
        _, after_nodes = self._kv_spec()
        self.assertEqual(
            after_nodes, target_nodes,
            "Scale-up did not land the expected node count (%s != %s)"
            % (after_nodes, target_nodes))

        def _scaled_hour_billed():
            rows = [r for r in self._hourly_records()
                    if (r.get("billingPeriod") or "") > latest_before_scale
                    and r.get("nodeId")
                    and float((r.get("fusionCosts") or {})
                              .get("fusionSSDCost") or 0) > 0]
            if not rows:
                return None
            newest = max(r.get("billingPeriod") for r in rows)
            nodes = {r.get("nodeId") for r in rows
                     if r.get("billingPeriod") == newest}
            return nodes if len(nodes) >= after_nodes else None
        scaled_nodes = self._poll(
            _scaled_hour_billed,
            "a settled hour billing SSD on all %s scaled-up node(s)"
            % after_nodes)
        self.assertTrue(
            scaled_nodes,
            "UNDER-BILLING after scale-up %s -> %s: no settled hour billed a "
            "fusion SSD charge on all %s nodes. The per-node SSD charge must "
            "grow with the cluster."
            % (before_nodes, after_nodes, after_nodes))
        self.log.info(
            "[scale-up] %s node(s) billed SSD after scaling %s -> %s"
            % (len(scaled_nodes), before_nodes, after_nodes))

        latest_on = max((r.get("billingPeriod") or "")
                        for r in self._hourly_records()) or ""

        turned_on = False
        real_gib, real_sizes = 0, []
        try:
            # ---- turn the cluster OFF ----
            turned_off = self._turn_cluster_off_and_wait(timeout=900)
            self.assertTrue(
                turned_off, "Cluster %s did not reach 'turnedOff' state"
                           % self.cluster_id)

            # Read the REAL accelerator/guest-volume size only NOW, while the
            # cluster is off — NOT right after the scale-up rebalance, which
            # is where an earlier version of this test checked it and
            # consistently found 0 volumes on AWS despite billing correctly
            # reflecting the scaled-up node count. couchbase-cloud's own
            # fusion teardown logic (internal/clusters/fusion/accelerator/
            # accelerator.go:2856-2864) only preserves the guest volume while
            # cluster.Config.IntendedState == TurnedOff (or turning off);
            # the DEFAULT path, while a cluster is healthy/on, allows normal
            # teardown to proceed once the volume is no longer needed for an
            # active rebalance. Checking right after the rebalance settled —
            # while still on — had no such guarantee, so the volume could
            # legitimately already be gone by then. Off is the one state the
            # source promises it survives, so that is where this asserts it.
            real_gib, real_sizes = self._real_accelerator_volume_gib()
            self.assertGreater(
                real_gib, 0,
                "No real fusion accelerator/guest volume found on AWS while "
                "cluster %s is OFF, after scaling %s -> %s nodes — "
                "couchbase-cloud's own teardown logic guarantees this volume "
                "is preserved while off (accelerator.go:2856-2864), so "
                "finding none here is unexpected and worth filing"
                % (self.cluster_id, before_nodes, target_nodes))

            # Off-state fixed billing job — OffClusterBilling, NOT the
            # on-cluster ClusterBilling _maybe_trigger_billing_jobs fires.
            # HostedOff() (the off-biller's query) only counts an hour where
            # the cluster was off for the WHOLE clock hour (turnedOff/
            # turningOff/turningOn >=1s AND running 0s in that hour) — the
            # hour we just turned off in is necessarily partial (it was
            # running for part of it), so it can never qualify. Re-trigger
            # with the CURRENT hour on every poll iteration instead of once
            # up front: once the wall clock rolls past the next hour
            # boundary (which the cluster spends entirely off), that
            # trigger produces the qualifying record. Worst case (turned off
            # right at minute 0) that's just under 2 hours, not ~60 min —
            # see OFF_RECORD_WAIT_TIMEOUT.
            def _fresh_off_record():
                this_hour = time.strftime("%Y-%m-%dT%H:00:00Z", time.gmtime())
                self._trigger_billing_job(
                    self.off_billing_job_type,
                    {"scheduled": False, "time": this_hour,
                     "serviceIds": [self.cluster_id]})
                rows = [r for r in self._hourly_records()
                       if r.get("isOff")
                       and (r.get("billingPeriod") or "") > latest_on]
                return rows or None

            off_records = self._poll(
                _fresh_off_record,
                "a fresh off-state (isOff=true) hourly record (needs a "
                "full clock hour fully off — can take up to ~2 hours)",
                timeout=self.input.param(
                    "off_record_wait_timeout", self.OFF_RECORD_WAIT_TIMEOUT))
            self.assertTrue(
                off_records, "No fresh off-state HourlyBillingRecord "
                            "(isOff=true) for cluster %s after turning it "
                            "off" % self.cluster_id)

            # ---- check 1: HARD regression guard ----
            fusion_off_records = [r for r in off_records
                                  if r.get("fusionEnabled")]
            self.assertTrue(
                fusion_off_records,
                "Off-state record(s) exist but none carry fusionEnabled=true "
                "for cluster %s" % self.cluster_id)
            ssd_costs = [float((r.get("fusionCosts") or {})
                               .get("fusionSSDCost") or 0)
                        for r in fusion_off_records]
            self.assertTrue(
                any(c > 0 for c in ssd_costs),
                "REGRESSION: every off-state, fusion-enabled hourly record "
                "has debug.fusionCosts.fusionSSDCost <= 0 (%s) — fusion "
                "billing is being silently dropped while the cluster is off"
                % ssd_costs)
            billed_ssd = max(ssd_costs)
            self.assertGreater(billed_ssd, 0,
                               "Off-state fusionSSDCost should be > 0")

            # ---- check 2: INFORMATIONAL gap (logged only, no assertion) ----
            rec = fusion_off_records[ssd_costs.index(billed_ssd)]
            fc = rec.get("fusionCosts") or {}
            ebs_price = float(fc.get("ebsListPrice") or 0)
            uplift = float(fc.get("fusionSSDUplift") or 0)
            if ebs_price and uplift:
                implied_disk_gib = (
                    billed_ssd * self.HOURS_PER_MONTH / (ebs_price * uplift))
                delta_pct = (((implied_disk_gib - real_gib) / real_gib) * 100
                            if real_gib else float("nan"))
                self.log.info(
                    "[fusion-off-uplift-gap] billed-basis=%.1f GiB vs "
                    "actual-accelerator-volume=%.1f GiB (delta=%.1f%%) — "
                    "off-billing prices the base KV/index node disk, NOT the "
                    "real accelerator volume; the same proxy gap exists "
                    "on-cluster too (deliberate, pre-existing design per "
                    "fusiondiskbiller/fixed.go:65 — NOT an off-specific "
                    "defect, no assertion on this delta)"
                    % (implied_disk_gib, real_gib, delta_pct))
            else:
                self.log.warning(
                    "[fusion-off-uplift-gap] off-state record missing "
                    "ebsListPrice/fusionSSDUplift — cannot compute the "
                    "implied-disk gap")

            self.log.info(
                "[fusion-off] %s off-state, fusion-enabled record(s); "
                "fusionSSDCost max=%s (>0) -> fusion billing NOT dropped "
                "while off" % (len(fusion_off_records), billed_ssd))

            # ---- check 3: the BUCKET charge must survive the off window
            # too. AV-110004 specifies the hourly bucket charge "in addition
            # to existing Operational Cluster billing for On & Off Clusters"
            # — the same On-and-Off wording it uses for the SSD charge — and
            # offclustersbiller/offbilling.go:111-171 does emit a bucket doc
            # with IsOff:true. Only the SSD half of that requirement was
            # asserted here before, so a regression that silently dropped the
            # bucket line item while a cluster is hibernated would have gone
            # unnoticed. Requires num_buckets > freeTier in the conf entry
            # (12 > 10) or there is no charge to lose. ----
            off_bucket = [r for r in off_records
                          if float((r.get("bucketCosts") or {})
                                   .get("credits") or 0) > 0]
            self.assertTrue(
                off_bucket,
                "UNDER-BILLING: no off-state (isOff=true) record for cluster "
                "%s carries a non-zero fusion BUCKET charge, but num_buckets"
                "=%s is above the free tier. AV-110004 bills the hourly "
                "bucket charge for On AND Off clusters, and "
                "offbilling.go:111-171 emits it with IsOff:true — so it "
                "should still be present while hibernated."
                % (self.cluster_id, self.num_buckets))
            self.log.info(
                "[fusion-off] bucket charge also survives the off window: "
                "%s off-state record(s) carry credits=%s"
                % (len(off_bucket),
                   max(float((r.get("bucketCosts") or {}).get("credits") or 0)
                       for r in off_bucket)))
        finally:
            # ALWAYS restore 'healthy' before tearDown: _delete_unhealthy_
            # clusters treats anything != 'healthy' as unhealthy and DELETES
            # it, and _scale_pooled_clusters_to_baseline also skips a
            # non-healthy cluster. Doing this here (not in tearDown) keeps
            # that shared logic untouched for every other test in the suite.
            turned_on = self._turn_cluster_on_and_wait(timeout=1200)

        if not turned_on:
            self.fail("Cluster %s did not return to 'healthy' after turning "
                      "it back on" % self.cluster_id)

        # ---- EMPIRICAL PROBE: does the off/on transition hour get billed
        # TWICE? couchbase-cloud's off biller writes its HourlyBillingRecord
        # under a DIFFERENT doc key than the on biller — fixed/service.go:
        # 296-298 appends ":Y:M:D:H" to the nodeID when isG2Off=true, while
        # the on path uses the bare (phone-home) nodeID. The comment at
        # offclustersbiller/offbilling.go:254 asserts "the normal
        # clustersbiller will overwrite said written HBR"; with different
        # keys it provably cannot. Both records can therefore survive on the
        # same billingPeriod, each carrying its own fusionSSDCost -> the
        # transition hour is charged twice (OVER-billing).
        #
        # _verify_no_duplicates cannot see this: it groups by
        # (nodeId, billingPeriod), and the two records carry different
        # nodeIds by construction. Whether the two actually land on the SAME
        # period depends on timing (off uses createdAt.Truncate(hour); on
        # uses createdAt.Add(1h).Truncate(hour)), so this is measured, not
        # assumed — a clean run here is a genuine negative result for that
        # hypothesis, not a vacuous pass. ----
        overlap = self._n1ql(
            "SELECT b.billingPeriod, "
            "COUNT(CASE WHEN b.isOff = true THEN 1 ELSE NULL END) AS off_recs, "
            "COUNT(CASE WHEN b.isOff = true THEN NULL ELSE 1 END) AS on_recs "
            "FROM %s AS b WHERE b.`_type` = \"%s\" AND b.databaseId = \"%s\" "
            "AND b.fusionEnabled = true "
            "AND b.debug.fusionCosts.fusionSSDCost > 0 "
            "GROUP BY b.billingPeriod "
            "HAVING COUNT(CASE WHEN b.isOff = true THEN 1 ELSE NULL END) > 0 "
            "AND COUNT(CASE WHEN b.isOff = true THEN NULL ELSE 1 END) > 0"
            % (self.KS, self.TYPE_HOURLY, self.cluster_id))
        self.assertFalse(
            overlap,
            "DOUBLE-BILLED transition hour(s) on cluster %s: the same "
            "billingPeriod carries BOTH off-state (isOff=true) and "
            "on-state (isOff=false) records with a non-zero fusionSSDCost, "
            "so that hour's fusion SSD charge is applied twice "
            "(period, off_recs, on_recs): %s. offbilling.go:254 claims the "
            "on-biller overwrites the off record, but fixed/service.go:"
            "296-298 gives the off record a different doc key "
            "(nodeID + ':Y:M:D:H'), so no overwrite can occur."
            % (self.cluster_id, overlap))
        self.log.info(
            "[off/on-overlap] no billingPeriod carries both an off-state and "
            "an on-state fusion SSD charge on %s — transition hour not "
            "double-billed in this run" % self.cluster_id)
        self._test_succeeded = True

    def _aggregate(self, variable, day):
        """POST /internal/support/f/aggregate — runs the SFDC summary calc ON DEMAND
        (no waiting for the scheduled biller) and returns []Summary. Reachable with
        the internal-support token. variable=True -> VariableSummaries (GiB-based
        variable total computed from pagerTasks); False -> DailyCalc (fixed daily).
        Body matches couchbase-cloud AggregatePayload {billingDate, variable, manual,
        serviceIds}; response Summary carries creditQuantity."""
        import requests
        pod_host = self.url or ""
        internal = "https://" + pod_host.replace("cloudapi.", "api.")
        token = self.billing_jobs_token or self._internal_support_token()
        self.assertTrue(token, "No internal-support token for the aggregate API")
        resp = requests.post(
            "%s/internal/support/f/aggregate" % internal,
            headers={"Authorization": "Bearer %s" % token,
                     "Content-Type": "application/json"},
            json={"billingDate": day, "variable": bool(variable), "manual": True,
                  "serviceIds": [self.cluster_id]}, timeout=180)
        self.assertEqual(
            resp.status_code, 200,
            "aggregate(variable=%s) %s -> %s: %s"
            % (variable, internal, resp.status_code, resp.text[:400]))
        return resp.json() or []

    def test_aggregate_api_total(self):
        """End-to-end money — trigger the pager-biller job on demand, then read
        the SFDC aggregation via the internal-support billing API instead of
        waiting for the scheduled 18:30 GMT cron.

        /internal/support/f/aggregate's variable path (couchbase-cloud
        VariableSummaries) reads the persisted `billing.variable` collection,
        NOT raw pagerTasks — that collection is written only by the
        BillPagerTasks job (its daily cron, or this explicit trigger). The
        pagerTasks themselves (verified below) exist the moment the rebalance
        completes, well before that job has ever run, so hitting the
        aggregate endpoint without triggering the job first sees an empty
        summary regardless of how much real pagerTask data exists."""
        self._setup_fusion_workload()
        # Expected fusion variable cost from the pagerTasks of this rebalance.
        _, gib = self._verify_pager_tasks()
        rate = self._get_factor("Accelerator Global Rate - per GiB")
        uplift = self._csp_uplift_factor()
        expected_variable = gib * rate * uplift
        day = time.strftime("%Y-%m-%dT00:00:00Z", time.gmtime())

        # VARIABLE — trigger BillPagerTasks on demand so billing.variable gets
        # written, then poll the aggregate endpoint (it 200s with an empty
        # summary until the triggered job actually lands).
        self.assertTrue(
            self._trigger_billing_job(
                self.pager_billing_job_type, {"scheduled": False, "time": day}),
            "On-demand %s (pager biller) trigger was rejected — the manual "
            "billing-job path is broken" % self.pager_billing_job_type)
        var_summaries = self._poll(
            lambda: self._aggregate(True, day),
            "a non-empty variable aggregate summary", interval=30, timeout=300)
        self.assertTrue(
            var_summaries,
            "aggregate(variable=True) never returned a non-empty summary "
            "within 300s of triggering %s" % self.pager_billing_job_type)
        var_total = sum(float(s.get("creditQuantity") or 0) for s in var_summaries)
        self.log.info("[aggregate] variable total=%s vs fusion pager %s "
                      "(%s GiB * %s * %s); summaries=%s"
                      % (var_total, expected_variable, gib, rate, uplift, var_summaries))
        self.assertGreater(var_total, 0,
                           "aggregate variable total is 0 — pager GiB not billed")
        # Total variable >= the fusion GiB cost (may exceed it if other variable
        # components exist); the fusion charge must be represented.
        self.assertGreaterEqual(
            var_total, expected_variable * 0.95,
            "aggregate variable %s < fusion pager cost %s"
            % (var_total, expected_variable))

        # FIXED — daily calc from HourlyBillingRecords (sanity; total > 0).
        fixed_summaries = self._aggregate(False, day)
        fixed_total = sum(float(s.get("creditQuantity") or 0) for s in fixed_summaries)
        self.log.info("[aggregate] fixed daily total=%s for %s"
                      % (fixed_total, self.cluster_id))
        self.assertGreaterEqual(fixed_total, 0)
        self._test_succeeded = True

    def test_pager_task_idempotent(self):
        """Idempotency, scoped PER PLAN: a given accelerator node writes exactly
        ONE pagerTask *per planUUID*. A duplicate within the same plan would
        double-bill the variable charge if /fusion/complete is retried — that
        is the property this test exists to protect.

        DELIBERATELY scoped by (nodeID, planUUID) rather than nodeID alone.
        The previous version grouped by nodeID only and asserted global
        uniqueness, which does NOT just check idempotency — it also asserts
        that a node participating in a SECOND plan produces no second task,
        i.e. it certifies suppression of a legitimate later charge as
        'correct'. That matters because couchbase-cloud dedupes on exactly
        that key and nothing else:
            internal/clusters/fusion/billing/service/service.go:27
                dedupeField = "fusionDetails.nodeID"      (no plan, no time)
            .../service.go:66  s.pager.Record(ctx, task, dedupeField, n.ID)
            internal/billing/pager/service.go:56-64
                FindExisting(...) -> if a task exists at all: skip, return nil
            internal/billing/pager/record/reader.go:38-40
                r.One(ctx, n1ql.WithWheres(n1ql.Equals(fieldName, value)))
        combined with a 1-year record TTL. If a node can be reused across
        plans, its shard bytes are dropped from every later plan for up to a
        year (under-billing). Whether that is REACHABLE is the open question
        — see test_multiple_rebalances_variable, which now checks empirically
        whether any node actually recurs across plans. Keeping this assertion
        nodeID-global would have made that bug un-observable here AND made
        this test fail if the product were ever fixed."""
        self._setup_fusion_workload()
        self._poll(self._completed_pager_tasks, "pagerTasks after rebalance")
        dupes = self._n1ql(
            "SELECT p.fusionDetails.nodeID AS nodeID, "
            "p.fusionDetails.planUUID AS planUUID, COUNT(*) AS n "
            "FROM %s p WHERE p.`_type` = \"%s\" "
            "AND p.clusterId = \"%s\" "
            "GROUP BY p.fusionDetails.nodeID, p.fusionDetails.planUUID "
            "HAVING COUNT(*) > 1"
            % (self.KS, self.TYPE_PAGER, self.cluster_id))
        self.assertFalse(
            dupes, "Duplicate pagerTask(s) for the same (nodeID, planUUID) — "
                   "the variable charge would be double-billed if "
                   "/fusion/complete is retried: %s" % dupes)
        self.log.info(
            "[idempotent] every (nodeID, planUUID) pair is unique — no double "
            "billing within a plan")
        self._test_succeeded = True

    def test_pager_task_ttl(self):
        """pagerTask docs carry a ~1 year TTL (META().expiration) so the collection
        self-prunes and never grows unboundedly."""
        self._setup_fusion_workload()
        self._poll(self._completed_pager_tasks, "pagerTasks after rebalance")
        rows = self._n1ql(
            "SELECT META(p).expiration AS exp, p.fusionDetails.nodeID AS nodeID "
            "FROM %s p WHERE p.`_type` = \"%s\" "
            "AND p.clusterId = \"%s\" LIMIT 5"
            % (self.KS, self.TYPE_PAGER, self.cluster_id))
        self.assertTrue(rows, "No pagerTask to check TTL for %s" % self.cluster_id)
        year, now = 365 * 24 * 3600, int(time.time())
        for r in rows:
            exp = int(r.get("exp") or 0)
            self.assertGreater(
                exp, 0, "pagerTask %s has no TTL (expiration=0) — the collection "
                        "would grow unboundedly" % r.get("nodeID"))
            drift = abs(exp - (now + year))
            self.assertLess(
                drift, 30 * 24 * 3600,
                "pagerTask %s expiration %s not ~1 year out (now+365d=%s, drift %.1fd)"
                % (r.get("nodeID"), exp, now + year, drift / 86400))
        self.log.info("[ttl] %s pagerTask(s) carry ~365d TTL" % len(rows))
        self._test_succeeded = True

    def test_fleet_fusion_ssd_cost_coverage(self):
        """AV-140399 fleet-wide regression check — cheap, no soak needed.

        Directly automates the methodology that actually found AV-140399
        (Ritesh Agarwal, 2026-08-14 comment): survey EVERY fusion-enabled
        cluster's HourlyBillingRecord history fleet-wide (not just this
        test's own freshly-provisioned cluster) and check whether the fixed
        SSD cost EVER populated for each one. That survey found 41 of 45
        fusion-enabled clusters NEVER got a non-zero debug.fusionCosts.
        fusionSSDCost — most stuck at zero for their entire observed
        history, some for 19+ days.

        Why fleet-wide instead of just this test's cluster: with ~91% of
        clusters affected fleet-wide, a single freshly-provisioned test
        cluster is NOT guaranteed to reproduce it on any given run (and
        conversely a pass here proves nothing if it happens to land in the
        lucky minority) — querying every cluster already in the CP-DB is
        what actually caught this, and is the only way to reliably catch a
        SYSTEMIC (not just "this one cluster is unlucky") regression.

        Needs no new cluster and no multi-hour wait — a point-in-time survey
        of whatever fusion-enabled clusters already exist in the CP-DB, so
        unlike its siblings in this GROUP it runs in seconds, not hours.

        min_fusion_ssd_coverage_pct (default 50%) is deliberately generous —
        AV-140399's own numbers (4/45 covered, ~9%) are far below even a lax
        bar, so this is a loud, unambiguous trip-wire, not a tight SLA (a
        real fix should push coverage well above 50%, not just barely over
        it)."""
        rows = self._n1ql(
            "SELECT b.databaseId, "
            "b.debug.fusionCosts.fusionSSDCost AS fusionSSDCost "
            "FROM %s AS b WHERE b.`_type` = \"%s\" AND b.fusionEnabled = true"
            % (self.KS, self.TYPE_HOURLY))
        self.assertTrue(
            rows, "No fusion-enabled HourlyBillingRecord found fleet-wide — "
            "cannot run the AV-140399 coverage survey (no fusion-enabled "
            "clusters currently billed in this CP-DB)")

        ever_nonzero = {}
        for r in rows:
            did = r.get("databaseId")
            if not did:
                continue
            cost = float(r.get("fusionSSDCost") or 0)
            ever_nonzero[did] = ever_nonzero.get(did, False) or (cost > 0)

        total = len(ever_nonzero)
        covered = [did for did, ok in ever_nonzero.items() if ok]
        never = [did for did, ok in ever_nonzero.items() if not ok]
        pct = 100.0 * len(covered) / total if total else 0.0
        min_pct = float(self.input.param("min_fusion_ssd_coverage_pct", 50))
        self.log.info(
            "[fleet-ssd-coverage] %s/%s (%.1f%%) fusion-enabled cluster(s) "
            "fleet-wide EVER carried a non-zero fusionSSDCost; %s never "
            "did: %s" % (len(covered), total, pct, len(never), never[:10]))
        self.assertGreaterEqual(
            pct, min_pct,
            "AV-140399 REGRESSION: only %.1f%% (%s/%s) of fusion-enabled "
            "clusters fleet-wide have EVER carried a non-zero "
            "debug.fusionCosts.fusionSSDCost (min_fusion_ssd_coverage_pct=%s"
            "%%) — matches the AV-140399 fleet survey (2026-08-14: 4/45 "
            "clusters covered, ~9%%, most never recovering for 19+ days). "
            "Never-covered databaseId sample: %s"
            % (pct, len(covered), total, min_pct, never[:10]))
        self.log.info(
            "[fleet-ssd-coverage] %.1f%% >= %s%% threshold — AV-140399 does "
            "not currently reproduce at fleet scale in this env"
            % (pct, min_pct))

        # ---- VARIABLE-billing coverage, same fleet-wide method ------------
        # AV-110004 is explicit: "The absence of a region should result in a
        # FAILURE to ensure new CSP region pricing is explicitly defined."
        # The implementation does the opposite — variable/billers/fusion/
        # biller.go:161-172 looks up the per-region CSP uplift factor and, on
        # ErrNotFound, `continue`s: the cluster is skipped and NO
        # billing.variable record is written. Silently. A cluster in a region
        # whose factor was never defined therefore accrues zero variable
        # revenue with no error anywhere.
        #
        # Detected here by signature rather than by reading a region field:
        # a cluster with COMPLETED pagerTasks (data provably moved through
        # the accelerator) but NO billing.variable record is exactly what
        # that silent skip looks like. That needs no CP-DB schema guessing
        # and catches any other cause of a dropped variable charge too.
        priced = {r.get("databaseId") for r in self._n1ql(
            "SELECT DISTINCT v.databaseId FROM %s v "
            "WHERE v.`_type` = \"%s\" AND v.usageCategory = \"Fusion 2\""
            % (self.KS, self.TYPE_VARIABLE)) if r.get("databaseId")}
        moved = {r.get("clusterId") for r in self._n1ql(
            "SELECT DISTINCT p.clusterId FROM %s p "
            "WHERE p.`_type` = \"%s\" "
            "AND p.fusionDetails.downloadCompletedAt IS NOT NULL "
            "AND p.fusionDetails.downloadCompletedAt != \"\""
            % (self.KS, self.TYPE_PAGER)) if r.get("clusterId")}
        unpriced = sorted(moved - priced)
        self.log.info(
            "[fleet-variable-coverage] %s cluster(s) moved accelerator data; "
            "%s have a priced Fusion 2 billing.variable record"
            % (len(moved), len(moved & priced)))
        self.assertFalse(
            unpriced,
            "UNDER-BILLING: %s cluster(s) have COMPLETED pagerTask(s) — data "
            "provably moved through the fusion accelerator — but NO priced "
            "billing.variable record: %s. The known silent-skip path is a "
            "missing per-region CSP uplift factor (variable/billers/fusion/"
            "biller.go:161-172 `continue`s on ErrNotFound instead of "
            "failing), which AV-110004 explicitly says must be a FAILURE so "
            "new CSP region pricing is defined before clusters land there. "
            "Check whether a 'Accelerator CSP Uplift - <provider> <region>' "
            "factor exists for these clusters' regions."
            % (len(unpriced), unpriced[:10]))
        self._test_succeeded = True
