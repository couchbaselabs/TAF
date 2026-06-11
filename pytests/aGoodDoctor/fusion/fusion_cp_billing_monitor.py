"""
Fusion CP Billing Monitor — queries CP Couchbase billing collections.

Layer 2 utility: uses KubectlCPDBUtil (kubectl_cp_db_util.py) to run N1QL
queries against the CP Couchbase database and validates billing records
written after fusion rebalances.

All billing documents live flat in the `default` bucket (no scopes/collections
are used in the CP database) and are discriminated by a `_type` field:
  - billing.pagerTask       : one document per completed accelerator node
  - billing.credit_factors  : billing price/uplift factors
  - billing.variable        : cost aggregated per cluster PER DAY (not per
                               rebalance — see query_variable_records())
  - models.HourlyBillingRecord : fixed SSD/bucket cost written hourly, keyed
                               by databaseId (NOT clusterId)

The control plane Couchbase cluster runs as pods on a private CP EKS
cluster. KubectlCPDBUtil reaches it via an assumed IAM role (jenkins-cp-cli)
plus a kubectl port-forward to the query service's REST endpoint, and reads
the readonly DB credentials from the cluster-wide cp-couchbase-auth Secret
(user cbc-cp-readonly) — no credentials are ever passed on a command line
or written to logs.

billing.variable and models.HourlyBillingRecord are normally only written by
their respective daily/hourly cron jobs (BillPagerTasks at 18:30 UTC,
ClusterBilling at :15 past every hour). This module also provides on-demand
triggers for both via the internal-support API
(trigger_bill_pager_tasks_job/trigger_cluster_billing_job, and the combined
trigger_and_verify_* helpers) — these fire the jobs immediately instead of
waiting for their cron schedule, so callers no longer need to defer these
checks to a separate, later test invocation.

CAUTION re: trigger_and_verify_hourly_billing_records() specifically —
confirmed live against a real CP database that calling ClusterBilling
on demand immediately after every single rebalance does NOT give an
accurate picture of an hour's billing, and can be actively misleading.
models.HourlyBillingRecord.billingPeriod is computed PER NODE from that
node's own last-observed timestamp at query time
(node.CreatedAt.Add(1h).Truncate(1h) — see
internal/billing/biller/fixed/clustersbiller/fixed.go in couchbase-cloud),
not from a fixed historical registration time. Triggering repeatedly and
early causes the SAME node to be billed under different, shifting
billingPeriod buckets across successive calls, and each call only ever
captures whichever nodes have phoned home by that exact instant — never
an hour's true peak node count. Production's own :15-past-the-hour cron
already waits for phone-home data to settle before billing the prior
hour; HourlyBillingWindowTracker (bottom of this module) does the same
(a slightly larger :20 buffer by default) instead of triggering
immediately after each rebalance.
"""

import json
import math
import threading
import time
from datetime import datetime, timedelta, timezone
from prettytable import PrettyTable

from .kubectl_cp_db_util import KubectlCPDBUtil


class FusionCPBillingMonitor:
    """
    Utility for verifying fusion billing records in the CP Couchbase database.

    All methods return bool so that test classes (Layer 3) perform assertions.
    Monitoring logic stays here; assertions belong in the test class.
    """

    # -------------------------------------------------------------------------
    # Couchbase document constants — flat `default` bucket, discriminated by
    # `_type` (confirmed against a live CP database; there are no scopes or
    # collections in this cluster).
    # -------------------------------------------------------------------------
    DEFAULT_BUCKET = "default"
    PAGER_TASK_TYPE = "billing.pagerTask"
    CREDIT_FACTORS_TYPE = "billing.credit_factors"
    VARIABLE_RECORD_TYPE = "billing.variable"
    HOURLY_BILLING_RECORD_TYPE = "models.HourlyBillingRecord"

    # -------------------------------------------------------------------------
    # Billing domain constants
    # -------------------------------------------------------------------------
    BYTES_PER_GIB = 1024 * 1024 * 1024
    FUSION_USAGE_CATEGORY = "Fusion 2"
    FUSION_FACTOR_CATEGORY = "Express Scaling (Fusion)"
    REQUIRED_FACTOR_NAMES = [
        "SSD Uplift",
        "Buckets - Free Tier",
        "Buckets - Global Price",
        "Accelerator Global Rate - per GiB",
    ]
    # Go zero time serialised to JSON
    GO_ZERO_TIME = "0001-01-01T00:00:00Z"

    # -------------------------------------------------------------------------
    # Variable-record cost-formula constants (internal/billing/biller/variable/
    # billers/fusion/biller.go): creditQuantity = GiB * FUSION_VARIABLE_PRICE_FACTOR
    # * "Accelerator CSP Uplift - {display provider} {region}". Provider.Name()
    # (internal/clusters/providers/providers.go) maps both "AWS" and the
    # billingBreakdown.provider value "hostedAWS" to the display name "AWS".
    # -------------------------------------------------------------------------
    FUSION_VARIABLE_PRICE_FACTOR = "Accelerator Global Rate - per GiB"
    FUSION_CSP_UPLIFT_FACTOR_PREFIX = "Accelerator CSP Uplift"
    CSP_PROVIDER_DISPLAY_NAMES = {
        "hostedAWS": "AWS",
        "AWS": "AWS",
    }
    # Relative tolerance for float comparisons against creditQuantity (accounts
    # for float64 accumulation error across many summed PagerTasks, not a sign
    # of an intentionally loose check)
    CREDIT_REL_TOLERANCE = 1e-6

    # -------------------------------------------------------------------------
    # Timing constants
    # -------------------------------------------------------------------------
    # Max seconds to wait for PagerTasks to appear after rebalance
    PAGER_TASK_WAIT_TIMEOUT = 120

    # -------------------------------------------------------------------------
    # On-demand internal-support job trigger constants. The internal-support
    # API (cmd/cp-api/v2/internaljobs/internaljobs.go) exposes
    # POST /internal/support/jobs {"jobType": ..., "payload": {...}} to queue
    # a job to run immediately instead of waiting for its normal cron
    # schedule — this is what removes the "must wait for tomorrow's/next
    # hour's cron" limitation documented on verify_variable_record_after_rebalance()
    # and query_hourly_billing_record() below.
    # -------------------------------------------------------------------------
    INTERNAL_JOBS_PATH = "/internal/support/jobs"
    INTERNAL_JOB_STATUS_PATH = "/internal/support/jobs/{job_id}/status"
    # Produces billing.variable (internal/billing/pager/jobs/biller.go)
    BILL_PAGER_TASKS_JOB_TYPE = "BillPagerTasks"
    # Produces models.HourlyBillingRecord for a cluster
    # (internal/billing/biller/fixed/jobs/clusters.go)
    CLUSTER_BILLING_JOB_TYPE = "ClusterBilling"
    JOB_POLL_INTERVAL_SECS = 5
    JOB_TRIGGER_TIMEOUT_SECS = 120
    JOB_TERMINAL_STATUSES = {"complete", "failed"}

    def __init__(self, logger, kubectl_cp_db_util: KubectlCPDBUtil, namespace: str = None):
        """
        Initialize FusionCPBillingMonitor.

        :param logger: Logger instance
        :param kubectl_cp_db_util: KubectlCPDBUtil instance already connect()'ed
            to the CP EKS cluster
        :param namespace: k8s namespace where the CP Couchbase pods run
            (optional — defaults to whatever KubectlCPDBUtil/kubectl's current
            context considers default if not given)
        """
        self.log = logger
        self.cp_db_util = kubectl_cp_db_util
        self.namespace = namespace

    # -------------------------------------------------------------------------
    # Internal helpers
    # -------------------------------------------------------------------------

    @staticmethod
    def _n1ql_escape(value: str) -> str:
        """
        Escape a value for safe interpolation inside a single-quoted N1QL
        string literal. cluster_id/plan_uuid/node_id are always
        system-generated UUIDs so this never actually triggers — it's a
        defensive guard against building a query string via f-string
        interpolation (KubectlCPDBUtil.run_n1ql_query() takes only a raw
        statement, no parameterized args).
        """
        return str(value).replace("'", "''")

    def _run_n1ql(self, query: str):
        """
        Run a N1QL query against the CP Couchbase database via KubectlCPDBUtil.

        :param query: N1QL query string
        :return: List of result dicts, or None on error
        """
        try:
            return self.cp_db_util.run_n1ql_query(query, namespace=self.namespace)
        except RuntimeError as e:
            self.log.error(f"N1QL query failed: {e}")
            return None

    def _query_factor_multiplier(self, factor_name: str):
        """
        Look up a single credit factor's multiplier under the Express Scaling
        (Fusion) category by name.

        :return: multiplier (float), or None if not found / query error
        """
        factor_name = self._n1ql_escape(factor_name)
        query = (
            f"SELECT RAW f.multiplier FROM `{self.DEFAULT_BUCKET}` f "
            f"WHERE f.`_type` = '{self.CREDIT_FACTORS_TYPE}' "
            f"AND f.category = '{self.FUSION_FACTOR_CATEGORY}' "
            f"AND f.name = '{factor_name}' LIMIT 1;"
        )
        results = self._run_n1ql(query)
        if not results:
            self.log.error(f"Credit factor not found: '{factor_name}'")
            return None
        return results[0]

    # -------------------------------------------------------------------------
    # Query methods (return raw result lists)
    # -------------------------------------------------------------------------

    def query_pager_tasks_for_rebalance(self, cluster_id: str, plan_uuid: str):
        """
        Query all PagerTask documents for a specific rebalance (cluster + planUUID).

        planUUID is the same value as the EC2 tag couchbase-cloud-fusion-rebalance
        captured in self.fusion_rebalances by monitor_cluster_accelerator_instances().

        :return: List of result dicts, or None on query error
        """
        cluster_id = self._n1ql_escape(cluster_id)
        plan_uuid = self._n1ql_escape(plan_uuid)
        query = (
            f"SELECT p.id, p.tenantId, p.clusterId, p.region, p.usageCategory, "
            f"p.fusionDetails.planUUID AS planUUID, "
            f"p.fusionDetails.nodeID AS nodeID, "
            f"p.fusionDetails.shardSizeInBytes AS shardSizeInBytes, "
            f"p.fusionDetails.registeredAt AS registeredAt, "
            f"p.fusionDetails.downloadCompletedAt AS downloadCompletedAt, "
            f"p.billingBreakdown.provider AS provider, "
            f"META(p).expiration AS expiryEpochSecs "
            f"FROM `{self.DEFAULT_BUCKET}` p "
            f"WHERE p.`_type` = '{self.PAGER_TASK_TYPE}' "
            f"AND p.clusterId = '{cluster_id}' "
            f"AND p.fusionDetails.planUUID = '{plan_uuid}';"
        )
        return self._run_n1ql(query)

    def query_pager_task_count_by_node(self, node_id: str) -> int:
        """Return the count of PagerTask documents for a specific nodeID (dedup check)."""
        node_id = self._n1ql_escape(node_id)
        query = (
            f"SELECT COUNT(*) AS taskCount "
            f"FROM `{self.DEFAULT_BUCKET}` "
            f"WHERE `_type` = '{self.PAGER_TASK_TYPE}' "
            f"AND fusionDetails.nodeID = '{node_id}';"
        )
        results = self._run_n1ql(query)
        if results:
            return results[0].get("taskCount", 0)
        return 0

    def query_credit_factors(self):
        """Query all billing credit factors for the Express Scaling (Fusion) category."""
        query = (
            f"SELECT name, multiplier FROM `{self.DEFAULT_BUCKET}` "
            f"WHERE `_type` = '{self.CREDIT_FACTORS_TYPE}' "
            f"AND category = '{self.FUSION_FACTOR_CATEGORY}';"
        )
        return self._run_n1ql(query)

    def query_variable_records(self, cluster_id: str, plan_uuid: str):
        """
        Query the variable billing record for one rebalance (cluster + planUUID).

        billing.variable is written by the daily BillPagerTasks cron
        (18:30 UTC, always for the *previous* UTC day — see
        verify_variable_record_after_rebalance()'s docstring for the
        practical implication). There is no top-level planUUID field on this
        document — real field names are creditQuantity, activationId,
        usageCategory, usageDate, databaseId, and billingBreakdown.{usage,
        usageUnit,basePrice,uplift,region,provider}. Per-rebalance
        correlation is still possible though: confirmed against a live CP
        database that the biller groups PagerTasks by clusterID|planUUID
        (internal/billing/biller/variable/billers/fusion/biller.go) and the
        resulting doc's `id` embeds the planUUID verbatim as its suffix,
        e.g. "<tenantId>:<clusterId>:fusion-2:<region>:<YYYY-MM-DD>-<planUUID>".

        :return: List of result dicts, or None on query error
        """
        cluster_id = self._n1ql_escape(cluster_id)
        plan_uuid = self._n1ql_escape(plan_uuid)
        query = (
            f"SELECT META(v).id AS docId, v.databaseName, v.creditQuantity, "
            f"v.activationId, v.usageCategory, v.usageDate, v.manual, "
            f"v.createdByUserID, v.createdAt, "
            f"v.billingBreakdown.usage AS usage, "
            f"v.billingBreakdown.usageUnit AS usageUnit, "
            f"v.billingBreakdown.basePrice AS basePrice, "
            f"v.billingBreakdown.uplift AS uplift, "
            f"v.billingBreakdown.region AS region, "
            f"v.billingBreakdown.provider AS provider "
            f"FROM `{self.DEFAULT_BUCKET}` v "
            f"WHERE v.`_type` = '{self.VARIABLE_RECORD_TYPE}' "
            f"AND v.databaseId = '{cluster_id}' "
            f"AND v.id LIKE '%-{plan_uuid}';"
        )
        return self._run_n1ql(query)

    def query_hourly_billing_record(self, cluster_id: str):
        """Query the most recent HourlyBillingRecord docs for a cluster (fixed billing)."""
        cluster_id = self._n1ql_escape(cluster_id)
        query = (
            f"SELECT META(b).id AS docId, b.fusionEnabled, b.nodeId, b.billingPeriod, "
            f"b.debug.fusionCosts AS fusionCosts, "
            f"b.debug.fusionBucketCosts AS fusionBucketCosts "
            f"FROM `{self.DEFAULT_BUCKET}` b "
            f"WHERE b.`_type` = '{self.HOURLY_BILLING_RECORD_TYPE}' "
            f"AND b.databaseId = '{cluster_id}' "
            f"ORDER BY b.billingPeriod DESC LIMIT 20;"
        )
        return self._run_n1ql(query)

    def query_hourly_billing_records_for_period(self, cluster_id: str, billing_period: str):
        """
        Query all HourlyBillingRecord documents for a cluster for one specific
        billing hour.

        There are two distinct document shapes for the same cluster/hour, NOT
        one doc with both sets of fields (confirmed against a live CP database
        — an earlier version of this query wrongly assumed
        `debug.fusionCosts.fusionBucketCost` existed, which it never did):
          - Per-node compute/storage docs: non-empty `nodeId`,
            `debug.fusionCosts.{fusionSSDCost,fusionSSDUplift,ebsListPrice}`
          - One bucket-level doc: empty `nodeId`, id suffix `:fusion-bucket:...`,
            `debug.fusionBucketCosts.{credits,nodeCount}`

        :param billing_period: ISO-8601 UTC hour-start timestamp, e.g.
            "2026-08-10T18:00:00Z" — must match the doc's stored billingPeriod
            exactly (hour-truncated)
        :return: List of result dicts, or None on query error
        """
        cluster_id = self._n1ql_escape(cluster_id)
        billing_period = self._n1ql_escape(billing_period)
        query = (
            f"SELECT META(b).id AS docId, b.nodeId, b.fusionEnabled, "
            f"b.billingPeriod, b.creditsConsumed, "
            f"b.debug.computeCost AS computeCost, "
            f"b.debug.storageCost AS storageCost, "
            f"b.debug.fusionCosts AS fusionCosts, "
            f"b.debug.fusionBucketCosts AS fusionBucketCosts "
            f"FROM `{self.DEFAULT_BUCKET}` b "
            f"WHERE b.`_type` = '{self.HOURLY_BILLING_RECORD_TYPE}' "
            f"AND b.databaseId = '{cluster_id}' "
            f"AND b.billingPeriod = '{billing_period}';"
        )
        return self._run_n1ql(query)

    # -------------------------------------------------------------------------
    # Verification methods (return bool; test class does assertions)
    # -------------------------------------------------------------------------

    def verify_billing_factors_exist(self) -> bool:
        """
        Verify all required fusion billing factors are present in the CP database.

        Must be called before running billing tests — missing factors mean billing
        jobs will fail silently or abort.

        :return: True if all required factors exist, False otherwise
        """
        factors = self.query_credit_factors()
        if factors is None:
            self.log.error("Failed to query billing factors from CP database")
            return False
        if not factors:
            self.log.error(
                f"No billing factors found for category '{self.FUSION_FACTOR_CATEGORY}'"
            )
            return False

        factor_names = {f.get("name") for f in factors}
        missing = [n for n in self.REQUIRED_FACTOR_NAMES if n not in factor_names]

        table = PrettyTable()
        table.field_names = ["Factor Name", "Multiplier", "Status"]
        for factor in sorted(factors, key=lambda x: x.get("name", "")):
            name = factor.get("name", "N/A")
            status = "MISSING" if name in missing else "OK"
            table.add_row([name, factor.get("multiplier", "N/A"), status])
        self.log.info(f"Fusion billing factors in CP database:\n{table}")

        if missing:
            self.log.error(f"Missing required fusion billing factors: {missing}")
            return False
        return True

    def verify_pager_tasks_after_rebalance(
        self, cluster_id: str, plan_uuid: str, timeout: int = None
    ) -> bool:
        """
        Verify PagerTask documents were correctly written to CP DB after a fusion rebalance.

        The pager billing write is best-effort (fire-and-forget from the /complete handler)
        so this method polls until at least one task appears or the timeout expires.

        Checks per task:
          - shardSizeInBytes >= 0
          - downloadCompletedAt is non-zero and after registeredAt
          - usageCategory = "Fusion 2"
          - No duplicate nodeIDs within this rebalance (idempotency invariant)
          - TTL expiry is set (META.expiration != 0)

        :param cluster_id: Cluster ID string
        :param plan_uuid: Rebalance plan UUID (= EC2 tag couchbase-cloud-fusion-rebalance)
        :param timeout: Max seconds to poll for tasks (default: PAGER_TASK_WAIT_TIMEOUT)
        :return: True if all checks pass, False otherwise
        """
        if timeout is None:
            timeout = self.PAGER_TASK_WAIT_TIMEOUT

        self.log.info(
            f"Verifying PagerTasks in CP DB: cluster={cluster_id}, planUUID={plan_uuid}"
        )

        # Poll until tasks appear (billing write is async best-effort)
        tasks = None
        start = time.time()
        while time.time() - start < timeout:
            tasks = self.query_pager_tasks_for_rebalance(cluster_id, plan_uuid)
            if tasks:
                break
            elapsed = int(time.time() - start)
            self.log.info(
                f"No PagerTasks yet for planUUID={plan_uuid} ({elapsed}s elapsed), "
                f"retrying in 10s..."
            )
            time.sleep(10)

        if not tasks:
            self.log.error(
                f"No PagerTask documents found after {timeout}s "
                f"for cluster={cluster_id}, planUUID={plan_uuid}"
            )
            return False

        table = PrettyTable()
        table.field_names = [
            "NodeID", "ShardSizeBytes", "GiB",
            "DownloadCompletedAt", "UsageCategory", "Provider", "TTL Set"
        ]
        failures = []
        node_ids_seen = set()
        total_bytes = 0

        for task in tasks:
            node_id = task.get("nodeID", "")
            shard_bytes = task.get("shardSizeInBytes", -1)
            completed_at = task.get("downloadCompletedAt", "")
            registered_at = task.get("registeredAt", "")
            usage_cat = task.get("usageCategory", "")
            provider = task.get("provider", "")
            expiry = task.get("expiryEpochSecs", 0)

            gib = shard_bytes / self.BYTES_PER_GIB if shard_bytes >= 0 else 0.0
            total_bytes += max(0, shard_bytes)

            ttl_set = "YES" if expiry and expiry > 0 else "NO"
            display_node_id = f"{node_id[:20]}..." if len(node_id) > 23 else node_id

            table.add_row([
                display_node_id,
                shard_bytes,
                f"{gib:.4f}",
                completed_at or "ZERO",
                usage_cat,
                provider,
                ttl_set,
            ])

            # Idempotency: no duplicate nodeIDs in one rebalance
            if node_id in node_ids_seen:
                failures.append(f"Duplicate PagerTask for nodeID={node_id}")
            node_ids_seen.add(node_id)

            # shardSizeInBytes must be non-negative (0 is valid for empty shards)
            if shard_bytes < 0:
                failures.append(f"shardSizeInBytes < 0 for nodeID={node_id}")

            # downloadCompletedAt must be present and non-zero
            if not completed_at or completed_at == self.GO_ZERO_TIME:
                failures.append(
                    f"downloadCompletedAt is zero/missing for nodeID={node_id}"
                )

            # usageCategory must be "Fusion 2"
            if usage_cat != self.FUSION_USAGE_CATEGORY:
                failures.append(
                    f"Expected usageCategory='{self.FUSION_USAGE_CATEGORY}', "
                    f"got '{usage_cat}' for nodeID={node_id}"
                )

            # Time ordering: registeredAt must be <= downloadCompletedAt
            if (registered_at and completed_at
                    and completed_at != self.GO_ZERO_TIME
                    and completed_at < registered_at):
                failures.append(
                    f"downloadCompletedAt ({completed_at}) precedes "
                    f"registeredAt ({registered_at}) for nodeID={node_id}"
                )

            # TTL must be set (1-year expiry; a zero expiry means the document
            # will grow the collection unboundedly)
            if not expiry or expiry == 0:
                failures.append(f"TTL (META.expiration) not set for nodeID={node_id}")

        total_gib = total_bytes / self.BYTES_PER_GIB
        self.log.info(
            f"PagerTasks for cluster={cluster_id}, planUUID={plan_uuid} "
            f"({len(tasks)} task(s), total={total_bytes} bytes = {total_gib:.4f} GiB):\n{table}"
        )

        if failures:
            for failure in failures:
                self.log.error(f"PagerTask validation FAILED: {failure}")
            return False

        self.log.info(
            f"All {len(tasks)} PagerTask(s) verified successfully "
            f"for cluster={cluster_id}, planUUID={plan_uuid}"
        )
        return True

    def verify_no_duplicate_pager_tasks(self, cluster_id: str, plan_uuid: str) -> bool:
        """
        Explicit idempotency check: each nodeID must appear exactly once per rebalance.

        The dedup key used by the pager infrastructure is fusionDetails.nodeID.
        A count > 1 means a retry of /fusion/complete created a double-billing record.

        :return: True if no duplicates found (or no tasks exist), False on duplicates
        """
        tasks = self.query_pager_tasks_for_rebalance(cluster_id, plan_uuid)
        if tasks is None:
            self.log.error(f"Query failed for cluster={cluster_id}, planUUID={plan_uuid}")
            return False
        if not tasks:
            self.log.info(
                f"No PagerTasks found for cluster={cluster_id}, planUUID={plan_uuid} "
                f"— skipping dedup check"
            )
            return True

        node_ids = [t.get("nodeID") for t in tasks]
        duplicates = [nid for nid in set(node_ids) if node_ids.count(nid) > 1]

        if duplicates:
            self.log.error(
                f"Duplicate PagerTask nodeIDs for cluster={cluster_id}, "
                f"planUUID={plan_uuid}: {duplicates}"
            )
            return False

        self.log.info(
            f"No duplicate PagerTasks for cluster={cluster_id}, planUUID={plan_uuid} "
            f"({len(tasks)} unique accelerator node(s))"
        )
        return True

    def verify_pager_task_count(
        self, cluster_id: str, plan_uuid: str, expected_count: int
    ) -> bool:
        """
        Verify that the number of PagerTasks matches the expected accelerator node count.

        The expected count is the number of accelerator instances observed during
        the rebalance (i.e. the number of nodes that called /fusion/complete).

        :param expected_count: Number of accelerator nodes that completed the rebalance
        :return: True if count matches, False otherwise
        """
        tasks = self.query_pager_tasks_for_rebalance(cluster_id, plan_uuid)
        if tasks is None:
            return False
        actual = len(tasks)
        if actual != expected_count:
            self.log.error(
                f"PagerTask count mismatch for cluster={cluster_id}, "
                f"planUUID={plan_uuid}: expected={expected_count}, actual={actual}"
            )
            return False
        self.log.info(
            f"PagerTask count verified: {actual} task(s) for "
            f"cluster={cluster_id}, planUUID={plan_uuid}"
        )
        return True

    def verify_variable_record_after_rebalance(
        self, cluster_id: str, plan_uuid: str, expected_gib: float = None
    ) -> bool:
        """
        Verify the billing.variable record for one rebalance was computed correctly.

        billing.variable is normally only written by the daily BillPagerTasks
        cron (18:30 UTC, always for the *previous* UTC day — see
        query_variable_records() docstring). Callers running this inline
        during test_billing_volume() must first trigger that cron job
        on-demand for the rebalance's UTC day — see
        trigger_and_verify_variable_record(), which does exactly that before
        calling this method. This method itself makes no assumption about
        how/when the record got there, so it also still works unchanged for
        checking a genuinely cron-written record from a past day.

        Cross-checks two things, both confirmed to match a live CP database
        to full float precision:
          1. Internal consistency: creditQuantity == usage * basePrice * uplift
             (the record's own stored fields)
          2. No factor drift: creditQuantity == usage * (live
             FUSION_VARIABLE_PRICE_FACTOR) * (live per-provider/region CSP
             uplift factor) — catches the biller having used stale/wrong
             factor values at write time.

        :param expected_gib: If given, also asserts the record's billed usage
            (GiB) matches this value (e.g. summed from query_pager_tasks_for_rebalance()
            for the same plan_uuid, independently of billing.variable)
        :return: True if all checks pass, False otherwise
        """
        records = self.query_variable_records(cluster_id, plan_uuid)
        if records is None:
            self.log.error(
                f"Query failed for billing.variable, cluster={cluster_id}, "
                f"planUUID={plan_uuid}"
            )
            return False
        if not records:
            self.log.error(
                f"No billing.variable record found for cluster={cluster_id}, "
                f"planUUID={plan_uuid} — either the daily BillPagerTasks cron "
                f"(18:30 UTC) has not yet run for this rebalance's day, or "
                f"billing is broken. This check must run at least one full "
                f"cron cycle after the rebalance."
            )
            return False

        record = records[0]
        gib = record.get("usage")
        base_price = record.get("basePrice")
        uplift = record.get("uplift")
        credit_quantity = record.get("creditQuantity")
        provider = record.get("provider")
        region = record.get("region")

        table = PrettyTable()
        table.field_names = ["Field", "Value"]
        table.add_row(["id", record.get("docId")])
        table.add_row(["databaseName", record.get("databaseName")])
        table.add_row(["usageCategory", record.get("usageCategory")])
        table.add_row(["usageDate", record.get("usageDate")])
        table.add_row(["usage (GiB)", gib])
        table.add_row(["usageUnit", record.get("usageUnit")])
        table.add_row(["basePrice", base_price])
        table.add_row(["uplift", uplift])
        table.add_row(["provider / region", f"{provider} / {region}"])
        table.add_row(["creditQuantity", credit_quantity])
        table.add_row(["manual", record.get("manual")])
        table.add_row(["createdByUserID", record.get("createdByUserID")])
        table.add_row(["createdAt", record.get("createdAt")])
        self.log.info(
            f"Variable cost (billing.variable) for cluster={cluster_id}, "
            f"planUUID={plan_uuid}:\n{table}"
        )

        failures = []

        internal_expected = gib * base_price * uplift
        if not math.isclose(internal_expected, credit_quantity, rel_tol=self.CREDIT_REL_TOLERANCE):
            failures.append(
                f"creditQuantity ({credit_quantity}) != usage*basePrice*uplift "
                f"({gib}*{base_price}*{uplift}={internal_expected}) — record is "
                f"internally inconsistent"
            )

        variable_price = self._query_factor_multiplier(self.FUSION_VARIABLE_PRICE_FACTOR)
        display_provider = self.CSP_PROVIDER_DISPLAY_NAMES.get(provider, provider)
        csp_uplift_name = f"{self.FUSION_CSP_UPLIFT_FACTOR_PREFIX} - {display_provider} {region}"
        csp_uplift = self._query_factor_multiplier(csp_uplift_name)
        if variable_price is None or csp_uplift is None:
            failures.append(
                f"Could not fetch live credit factors "
                f"('{self.FUSION_VARIABLE_PRICE_FACTOR}'={variable_price}, "
                f"'{csp_uplift_name}'={csp_uplift})"
            )
        else:
            live_expected = gib * variable_price * csp_uplift
            if not math.isclose(live_expected, credit_quantity, rel_tol=self.CREDIT_REL_TOLERANCE):
                failures.append(
                    f"creditQuantity ({credit_quantity}) != usage*live_factors "
                    f"({gib}*{variable_price}*{csp_uplift}={live_expected}) — "
                    f"biller may have used stale/wrong credit factors"
                )

        if expected_gib is not None and not math.isclose(
                gib, expected_gib, rel_tol=self.CREDIT_REL_TOLERANCE):
            failures.append(
                f"billed usage ({gib} GiB) != expected ({expected_gib} GiB summed "
                f"from PagerTasks) for planUUID={plan_uuid}"
            )

        if failures:
            for failure in failures:
                self.log.error(f"Variable record validation FAILED: {failure}")
            return False

        self.log.info(
            f"Variable billing record verified for cluster={cluster_id}, "
            f"planUUID={plan_uuid}: {gib:.4f} GiB billed at {credit_quantity:.6f} credits"
        )
        return True

    def verify_hourly_billing_record_for_period(
        self, cluster_id: str, billing_period: str, expected_node_count: int = None
    ) -> bool:
        """
        Verify HourlyBillingRecord documents exist and are structurally
        correct for one cluster/hour.

        Checks:
          - At least one per-node record exists (has fusionCosts)
          - Exactly one bucket-level record exists (has fusionBucketCosts)
          - fusionEnabled == True on every record (this cluster has fusion enabled)
          - Per-node record count matches expected_node_count, if given

        Does NOT assert on the fusionCosts/fusionBucketCosts VALUES —
        whether those should be nonzero for accelerator-download activity is
        not yet confirmed (observed all-zero across a live rebalance despite
        1500+ GiB of accelerator downloads that hour; billing.variable
        correctly captured the real cost via the separate pagerTask/variable
        pipeline). Only logs them so a human can track the trend across runs
        rather than silently asserting an unconfirmed expectation.

        :return: True if all structural checks pass, False otherwise
        """
        records = self.query_hourly_billing_records_for_period(cluster_id, billing_period)
        if records is None:
            self.log.error(
                f"Query failed for HourlyBillingRecord, cluster={cluster_id}, "
                f"billingPeriod={billing_period}"
            )
            return False
        if not records:
            self.log.error(
                f"No HourlyBillingRecord documents found for cluster={cluster_id}, "
                f"billingPeriod={billing_period}"
            )
            return False

        node_records = [r for r in records if r.get("fusionCosts") is not None]
        bucket_records = [r for r in records if r.get("fusionBucketCosts") is not None]

        table = PrettyTable()
        table.field_names = [
            "NodeId", "Type", "FusionEnabled", "Credits",
            "ComputeCost", "StorageCost", "Fusion/BucketCosts",
        ]
        for r in records:
            costs = r.get("fusionCosts") or r.get("fusionBucketCosts")
            is_bucket = r.get("fusionBucketCosts") is not None
            node_id = r.get("nodeId", "")
            table.add_row([
                f"{node_id[:20]}..." if len(node_id) > 23 else (node_id or "(bucket)"),
                "fusion-bucket" if is_bucket else "node",
                r.get("fusionEnabled"),
                r.get("creditsConsumed"),
                r.get("computeCost"),
                r.get("storageCost"),
                costs,
            ])
        self.log.info(
            f"HourlyBillingRecord for cluster={cluster_id}, "
            f"billingPeriod={billing_period}:\n{table}"
        )

        failures = []
        not_fusion_enabled = [r for r in records if not r.get("fusionEnabled")]
        if not_fusion_enabled:
            failures.append(
                f"{len(not_fusion_enabled)} record(s) have fusionEnabled=False "
                f"for a fusion-enabled cluster"
            )
        if not node_records:
            failures.append("No per-node HourlyBillingRecord (fusionCosts) found")
        if len(bucket_records) != 1:
            failures.append(
                f"Expected exactly 1 fusion-bucket HourlyBillingRecord "
                f"(fusionBucketCosts), found {len(bucket_records)}"
            )
        if expected_node_count is not None and len(node_records) != expected_node_count:
            failures.append(
                f"Per-node HourlyBillingRecord count ({len(node_records)}) != "
                f"expected KV node count ({expected_node_count})"
            )

        if failures:
            for failure in failures:
                self.log.error(f"HourlyBillingRecord validation FAILED: {failure}")
            return False

        self.log.info(
            f"HourlyBillingRecord verified for cluster={cluster_id}, "
            f"billingPeriod={billing_period}: {len(node_records)} node record(s), "
            f"1 bucket record"
        )
        return True

    # -------------------------------------------------------------------------
    # On-demand job triggering (internal-support API)
    # -------------------------------------------------------------------------

    def trigger_internal_job(self, capella_api, job_type: str, payload: dict):
        """
        POST a job to the internal-support API to run immediately instead of
        waiting for its normal cron schedule.

        :param capella_api: an already-constructed CapellaAPI instance (see
            capellaAPI.capella.dedicated.CapellaAPI.CapellaAPI) carrying
            internal_url and TOKEN_FOR_INTERNAL_SUPPORT (cbc_api_request_headers)
            — callers own constructing this, e.g. CapellaAPI(pod.url_public,
            tenant.api_secret_key, tenant.api_access_key, tenant.user,
            tenant.pwd, pod.TOKEN), matching the pattern already used by
            CapellaUtils.create_tenant_feature_flag() for the same internal-
            support API.
        :param job_type: e.g. BILL_PAGER_TASKS_JOB_TYPE or CLUSTER_BILLING_JOB_TYPE
        :param payload: job-specific payload dict (time/scheduled/serviceIds)
        :return: job id (str), or None on failure
        """
        url = f"{capella_api.internal_url}{self.INTERNAL_JOBS_PATH}"
        body = json.dumps({"jobType": job_type, "payload": payload})
        resp = capella_api._urllib_request(
            url, method="POST", headers=capella_api.cbc_api_request_headers, params=body
        )
        if resp is None or resp.status_code != 202:
            self.log.error(
                f"Failed to trigger internal job jobType={job_type}, payload={payload}: "
                f"status={getattr(resp, 'status_code', None)}, "
                f"body={getattr(resp, 'content', None)}"
            )
            return None
        job_id = json.loads(resp.content)
        self.log.info(f"Triggered internal job jobType={job_type}, payload={payload}: jobId={job_id}")
        return job_id

    def wait_for_internal_job(self, capella_api, job_id: str, timeout: int = None) -> bool:
        """
        Poll GET /internal/support/jobs/{job_id}/status until it reaches a
        terminal status ("complete"/"failed") or timeout expires.

        :return: True if the job reached "complete", False otherwise
        """
        if timeout is None:
            timeout = self.JOB_TRIGGER_TIMEOUT_SECS
        url = f"{capella_api.internal_url}{self.INTERNAL_JOB_STATUS_PATH.format(job_id=job_id)}"
        start = time.time()
        status = None
        while time.time() - start < timeout:
            resp = capella_api._urllib_request(
                url, method="GET", headers=capella_api.cbc_api_request_headers
            )
            if resp is not None and resp.status_code == 200:
                status = json.loads(resp.content)
                if status in self.JOB_TERMINAL_STATUSES:
                    break
            time.sleep(self.JOB_POLL_INTERVAL_SECS)

        if status != "complete":
            self.log.error(
                f"Internal job jobId={job_id} did not complete within {timeout}s "
                f"(last status={status})"
            )
            return False
        self.log.info(f"Internal job jobId={job_id} completed")
        return True

    def trigger_bill_pager_tasks_job(self, capella_api, usage_date: datetime) -> bool:
        """
        Trigger the BillPagerTasks job on demand for a specific UTC day,
        instead of waiting for its 18:30 UTC daily cron run. Produces
        billing.variable documents for every cluster/plan billed that day.
        Idempotent to call multiple times for the same day (the resulting
        doc id is deterministic per cluster/plan/day), so this is safe to
        call after every rebalance rather than once per day.

        :param usage_date: datetime for the UTC day to bill (time-of-day is
            ignored; truncated to midnight)
        :return: True if the job completed successfully
        """
        day_start = usage_date.replace(hour=0, minute=0, second=0, microsecond=0)
        payload = {
            "time": day_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "scheduled": False,
        }
        job_id = self.trigger_internal_job(capella_api, self.BILL_PAGER_TASKS_JOB_TYPE, payload)
        if not job_id:
            return False
        return self.wait_for_internal_job(capella_api, job_id)

    def trigger_cluster_billing_job(self, capella_api, cluster_id: str, hour: datetime) -> bool:
        """
        Trigger the ClusterBilling job on demand for a specific UTC hour and
        cluster (via serviceIds scoping), instead of waiting for its
        :15-past-the-hour cron run. Produces models.HourlyBillingRecord
        documents for that cluster/hour.

        :param hour: datetime for the UTC hour to bill (minutes/seconds ignored;
            truncated to the hour)
        :return: True if the job completed successfully
        """
        hour_start = hour.replace(minute=0, second=0, microsecond=0)
        payload = {
            "time": hour_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "scheduled": False,
            "serviceIds": [cluster_id],
        }
        job_id = self.trigger_internal_job(capella_api, self.CLUSTER_BILLING_JOB_TYPE, payload)
        if not job_id:
            return False
        return self.wait_for_internal_job(capella_api, job_id)

    # -------------------------------------------------------------------------
    # On-demand trigger + verify orchestration
    # -------------------------------------------------------------------------

    def trigger_and_verify_variable_record(
        self, capella_api, cluster_id: str, plan_uuid: str,
        usage_date: datetime = None, expected_gib: float = None
    ) -> bool:
        """
        Trigger BillPagerTasks on demand for usage_date (default: today, UTC)
        and verify the resulting billing.variable record for one rebalance.

        Removes the wait-for-tomorrow's-cron limitation documented on
        verify_variable_record_after_rebalance() — that method is unchanged
        and still does the actual verification here, just no longer gated on
        cron timing.

        :return: True if the job ran and the record verified correctly
        """
        if usage_date is None:
            usage_date = datetime.now(timezone.utc)

        if not self.trigger_bill_pager_tasks_job(capella_api, usage_date):
            self.log.error(
                f"BillPagerTasks job did not complete — cannot verify "
                f"billing.variable for cluster={cluster_id}, planUUID={plan_uuid}"
            )
            return False

        return self.verify_variable_record_after_rebalance(
            cluster_id, plan_uuid, expected_gib=expected_gib
        )

    def trigger_and_verify_hourly_billing_records(
        self, capella_api, cluster_id: str, plan_uuid: str, expected_node_count: int = None
    ) -> bool:
        """
        Trigger ClusterBilling on demand for every UTC hour touched by one
        rebalance's accelerator activity (derived from that plan's PagerTask
        registeredAt/downloadCompletedAt timestamps) and verify the
        resulting HourlyBillingRecord documents.

        :return: True if all hours' jobs ran and all records verified correctly
        """
        tasks = self.query_pager_tasks_for_rebalance(cluster_id, plan_uuid)
        if not tasks:
            self.log.error(
                f"No PagerTasks found for cluster={cluster_id}, planUUID={plan_uuid} "
                f"— cannot determine which billing hour(s) to check"
            )
            return False

        hours = set()
        for task in tasks:
            for ts_field in ("registeredAt", "downloadCompletedAt"):
                ts = task.get(ts_field)
                if ts and ts != self.GO_ZERO_TIME:
                    hours.add(self._parse_iso_hour(ts))

        if not hours:
            self.log.error(
                f"Could not derive any billing hour from PagerTasks for "
                f"cluster={cluster_id}, planUUID={plan_uuid}"
            )
            return False

        failures = []
        for hour in sorted(hours):
            if not self.trigger_cluster_billing_job(capella_api, cluster_id, hour):
                failures.append(f"ClusterBilling job did not complete for hour={hour.isoformat()}")
                continue
            billing_period = hour.strftime("%Y-%m-%dT%H:%M:%SZ")
            if not self.verify_hourly_billing_record_for_period(
                    cluster_id, billing_period, expected_node_count=expected_node_count):
                failures.append(f"HourlyBillingRecord verification failed for hour={billing_period}")

        if failures:
            for failure in failures:
                self.log.error(f"Hourly billing record check FAILED: {failure}")
            return False
        return True

    @staticmethod
    def _parse_iso_hour(ts: str) -> datetime:
        """Parse a Go RFC3339 timestamp (with/without fractional seconds) and truncate to the hour."""
        base = ts.split(".")[0].rstrip("Z")
        dt = datetime.strptime(base, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
        return dt.replace(minute=0, second=0, microsecond=0)

    def log_billing_summary(self, cluster_id: str, plan_uuid: str):
        """
        Log a diagnostic summary of PagerTask GiB totals for a completed rebalance.

        Intended for informational logging — does not assert anything.
        """
        tasks = self.query_pager_tasks_for_rebalance(cluster_id, plan_uuid)
        if not tasks:
            self.log.info(
                f"No PagerTasks to summarise for cluster={cluster_id}, planUUID={plan_uuid}"
            )
            return

        total_bytes = sum(max(0, t.get("shardSizeInBytes", 0)) for t in tasks)
        total_gib = total_bytes / self.BYTES_PER_GIB
        registered_ats = [t.get("registeredAt") for t in tasks if t.get("registeredAt")]
        completed_ats = [
            t.get("downloadCompletedAt") for t in tasks
            if t.get("downloadCompletedAt") and t.get("downloadCompletedAt") != self.GO_ZERO_TIME
        ]

        summary_table = PrettyTable()
        summary_table.field_names = ["Metric", "Value"]
        summary_table.add_row(["Cluster ID", cluster_id])
        summary_table.add_row(["Plan UUID", plan_uuid])
        summary_table.add_row(["Accelerator nodes billed", len(tasks)])
        summary_table.add_row(["Total shard bytes", total_bytes])
        summary_table.add_row(["Total GiB processed", f"{total_gib:.6f}"])
        summary_table.add_row(["First registeredAt", min(registered_ats) if registered_ats else "N/A"])
        summary_table.add_row(["Last downloadCompletedAt", max(completed_ats) if completed_ats else "N/A"])

        self.log.info(f"PagerTask summary (billing.pagerTask) for cluster={cluster_id}, "
                       f"planUUID={plan_uuid}:\n{summary_table}")

    def log_all_variable_records_summary(self, cluster_plan_pairs):
        """
        Log a fresh, uncached summary of the billing.variable record for every
        (cluster_id, plan_uuid) pair given.

        Each call re-queries the CP database for every pair rather than reusing
        any previously fetched result. Intended to be called after every
        rebalance with every (cluster_id, plan_uuid) pair seen so far in the
        run (not just the one that just completed) — query_variable_records()
        matches on the doc id's `-<planUUID>` suffix, so each rebalance has
        its own distinct billing.variable document, but the sequence of these
        summaries across a run's rebalances is what actually shows whether an
        earlier rebalance's record is still intact (unchanged creditQuantity,
        same docId) after a later rebalance's on-demand BillPagerTasks trigger
        runs, rather than having been silently overwritten by it.

        Informational only — does not assert anything.

        :param cluster_plan_pairs: List of (cluster_id, plan_uuid) tuples
        """
        if not cluster_plan_pairs:
            self.log.info("No prior rebalances to summarise variable cost for")
            return

        table = PrettyTable()
        table.field_names = [
            "Cluster ID", "Plan UUID", "Doc ID", "Usage Date",
            "Usage (GiB)", "Credit Quantity", "Created At"
        ]
        for cluster_id, plan_uuid in cluster_plan_pairs:
            records = self.query_variable_records(cluster_id, plan_uuid)
            if records is None:
                table.add_row(
                    [cluster_id, plan_uuid, "QUERY FAILED", "-", "-", "-", "-"]
                )
                continue
            if not records:
                table.add_row([cluster_id, plan_uuid, "NOT FOUND", "-", "-", "-", "-"])
                continue
            for record in records:
                table.add_row([
                    cluster_id, plan_uuid, record.get("docId"),
                    record.get("usageDate"), record.get("usage"),
                    record.get("creditQuantity"), record.get("createdAt"),
                ])

        self.log.info(
            f"Variable fusion cost (billing.variable) summary across all "
            f"{len(cluster_plan_pairs)} rebalance(s) so far, freshly "
            f"re-queried from CP DB:\n{table}"
        )


class HourlyBillingWindowTracker(threading.Thread):
    """
    Background thread that independently tracks, per wall-clock UTC hour,
    the true peak live node count and every scaling operation observed for
    one fusion cluster — then, once phone-home data has had time to settle,
    triggers ClusterBilling on demand exactly once and logs a comparison
    against what CP actually billed for that hour.

    See the CAUTION note in this module's docstring for why this exists
    instead of triggering ClusterBilling right after each rebalance.

    Usage:
      tracker = HourlyBillingWindowTracker(
          log, cluster.id,
          get_billing_monitor_fn=lambda: self.billing_monitor,
          get_capella_api_fn=lambda: self._build_capella_api(tenant),
          get_live_node_ids_fn=lambda: {n.get("hostname") for n in CapellaUtils.get_nodes(pod, tenant, cluster.id)},
      )
      tracker.start()
      ...
      tracker.record_scaling_operation("4.2 (UP)", "14->16", start_dt, end_dt)
      ...
      tracker.stop_and_flush(timeout=1800)  # call before teardown
    """

    DEFAULT_POLL_INTERVAL_SECS = 30
    DEFAULT_REPORT_DELAY_MINUTES = 20

    def __init__(self, log, cluster_id: str,
                 get_billing_monitor_fn, get_capella_api_fn, get_live_node_ids_fn,
                 poll_interval_secs: int = None, report_delay_minutes: int = None):
        """
        :param log: Logger instance
        :param cluster_id: Cluster ID to track/bill
        :param get_billing_monitor_fn: zero-arg callable returning the
            CURRENT FusionCPBillingMonitor instance to use. Callable rather
            than a frozen instance because callers like
            FusionBillingVolumeTest rebuild their CP DB connection (and
            therefore self.billing_monitor) at every rebalance-batch
            boundary to survive assumed-role TTL expiry on long runs — a
            tracker created once up front must always resolve the CURRENT
            object, not the one that existed when the tracker started.
        :param get_capella_api_fn: zero-arg callable returning a CapellaAPI
            instance carrying TOKEN_FOR_INTERNAL_SUPPORT, for the on-demand
            trigger. Callable for the same expiring-credentials reason.
        :param get_live_node_ids_fn: zero-arg callable returning the
            cluster's CURRENT live node IDs (a set/iterable of hostnames),
            not just a count. CP bills every distinct node that phoned home
            at any point during the hour, not the peak concurrent count --
            a node created and torn down between two poll ticks (e.g. a
            fallback replacement mid-hour) would be missing from a
            concurrent-count-based peak but still gets its own
            HourlyBillingRecord. Sampling IDs each poll and unioning them
            across the hour is what lets the tracker match that. Test-owned
            rather than built in here, since fetching it needs
            pod/tenant/cluster plumbing this Layer-2 class has no business
            holding.
        :param poll_interval_secs: How often to sample the live node IDs
            (default 30s)
        :param report_delay_minutes: Minutes past an hour's close to wait
            before triggering/checking that hour's billing record
            (default 20, matching production's own :15-past-hour cron plus
            a small extra safety margin)
        """
        super().__init__(name=f"hourly-billing-tracker-{cluster_id}", daemon=True)
        self.log = log
        self.cluster_id = cluster_id
        self._get_billing_monitor = get_billing_monitor_fn
        self._get_capella_api = get_capella_api_fn
        self._get_live_node_ids = get_live_node_ids_fn
        self.poll_interval_secs = poll_interval_secs or self.DEFAULT_POLL_INTERVAL_SECS
        self.report_delay_minutes = (
            report_delay_minutes if report_delay_minutes is not None
            else self.DEFAULT_REPORT_DELAY_MINUTES
        )

        self._lock = threading.Lock()
        # Separate from self._lock (which only guards _hourly_state/
        # _reported_hours dict mutations): serializes whole _report_due_hours()
        # calls end-to-end, since both run()'s final pass and a concurrent
        # stop_and_flush() loop (from the main thread) can otherwise call it
        # at nearly the same instant right at stop time and double-trigger
        # the same hour before either has recorded it as reported.
        self._report_lock = threading.Lock()
        self._stop_event = threading.Event()
        # {hour_start (datetime, UTC, truncated): {"max_nodes": int,
        #  "node_ids": set of every node ID observed live at any poll tick
        #  during the hour, "ops": [...]}}
        self._hourly_state = {}
        self._reported_hours = set()
        # Retry count per hour whose trigger/query attempt errored (e.g. the
        # narrow CP-DB-reconnect race documented on
        # FusionBillingVolumeTest._connect_cp_db) -- retried a bounded
        # number of times on later poll ticks before being given up on, so
        # a transient failure doesn't silently drop that hour's report
        # forever, and a persistent one doesn't retry indefinitely either.
        self._report_attempts = {}
        self.MAX_REPORT_ATTEMPTS = 3
        # Populated with a dict per hour whose CP-billed node count didn't
        # match the observed max — callers can assert on this list.
        self.mismatches = []

    def record_scaling_operation(self, step_label: str, node_change: str,
                                  start_time: datetime, end_time: datetime):
        """
        Record a completed rebalance step against every wall-clock hour it
        overlaps, so that hour's eventual report can list what scaling
        activity happened in it. Call this right after a rebalance
        completes — independent of the polling loop, so a step is recorded
        even if it starts and finishes between two poll ticks.

        :param step_label: e.g. "4.2 (UP)"
        :param node_change: e.g. "14->16"
        :param start_time: rebalance start (datetime, UTC)
        :param end_time: rebalance end (datetime, UTC)
        """
        hour = start_time.replace(minute=0, second=0, microsecond=0)
        last_hour = end_time.replace(minute=0, second=0, microsecond=0)
        with self._lock:
            while hour <= last_hour:
                self._state_for_hour_locked(hour)["ops"].append(
                    (step_label, node_change, start_time, end_time)
                )
                hour += timedelta(hours=1)

    def _state_for_hour_locked(self, hour_start: datetime) -> dict:
        """Caller must hold self._lock."""
        return self._hourly_state.setdefault(
            hour_start, {"max_nodes": 0, "node_ids": set(), "ops": []})

    def run(self):
        while not self._stop_event.is_set():
            try:
                self._poll_once()
            except Exception as e:
                self.log.warning(f"HourlyBillingWindowTracker poll failed: {e}")
            self._report_due_hours()
            self._stop_event.wait(self.poll_interval_secs)
        # One last pass in case an hour became due between the final poll
        # and stop() being called.
        self._report_due_hours()

    def _poll_once(self):
        now = datetime.now(timezone.utc)
        hour_start = now.replace(minute=0, second=0, microsecond=0)
        node_ids = set(self._get_live_node_ids())
        with self._lock:
            state = self._state_for_hour_locked(hour_start)
            state["max_nodes"] = max(state["max_nodes"], len(node_ids))
            state["node_ids"].update(node_ids)

    def _report_due_hours(self):
        with self._report_lock:
            now = datetime.now(timezone.utc)
            to_report = []
            with self._lock:
                for hour_start, state in self._hourly_state.items():
                    if hour_start in self._reported_hours:
                        continue
                    due_at = hour_start + timedelta(hours=1, minutes=self.report_delay_minutes)
                    if now >= due_at:
                        to_report.append((hour_start, dict(state)))
            for hour_start, state in to_report:
                succeeded = self._report_hour(hour_start, state)
                with self._lock:
                    if succeeded:
                        self._reported_hours.add(hour_start)
                        self._report_attempts.pop(hour_start, None)
                    else:
                        attempts = self._report_attempts.get(hour_start, 0) + 1
                        self._report_attempts[hour_start] = attempts
                        if attempts >= self.MAX_REPORT_ATTEMPTS:
                            self.log.error(
                                f"HourlyBillingWindowTracker: giving up on hour="
                                f"{hour_start.isoformat()} for cluster={self.cluster_id} "
                                f"after {attempts} failed trigger/query attempts -- "
                                f"no report was logged for this hour"
                            )
                            self._reported_hours.add(hour_start)
                        # else: leave it out of _reported_hours so the next
                        # _report_due_hours() call (next poll tick) retries it

    def _report_hour(self, hour_start: datetime, state: dict) -> bool:
        """
        Trigger ClusterBilling once for hour_start and log a comparison
        between the observed max node count/scaling ops for
        [hour_start, hour_start+1h) and what CP actually billed.

        billingPeriod for phone-homes occurring during
        [hour_start, hour_start+1h) is hour_start+1h — see this module's
        docstring re: node.CreatedAt.Add(1h).Truncate(1h) — NOT hour_start
        itself.

        :return: True if the trigger+query both succeeded and a comparison
            was logged (regardless of whether it matched); False if either
            step errored and this hour should be retried
        """
        hour_end = hour_start + timedelta(hours=1)
        billing_period_str = hour_end.strftime("%Y-%m-%dT%H:%M:%SZ")

        billing_monitor = self._get_billing_monitor()
        capella_api = self._get_capella_api()

        if not billing_monitor.trigger_cluster_billing_job(
                capella_api, self.cluster_id, hour_start):
            self.log.warning(
                f"HourlyBillingWindowTracker: ClusterBilling trigger failed "
                f"for hour={hour_start.isoformat()}, cluster={self.cluster_id} "
                f"-- will retry"
            )
            return False

        records = billing_monitor.query_hourly_billing_records_for_period(
            self.cluster_id, billing_period_str
        )
        if records is None:
            self.log.warning(
                f"HourlyBillingWindowTracker: HourlyBillingRecord query failed "
                f"for hour={hour_start.isoformat()}, cluster={self.cluster_id} "
                f"-- will retry"
            )
            return False

        node_records = [r for r in records if r.get("fusionCosts") is not None]
        billed_node_count = len({r.get("nodeId") for r in node_records})
        # CP bills every distinct node that phoned home at any point during
        # the hour, not the peak concurrent count -- a node created and torn
        # down between two poll ticks (e.g. mid-hour scale up followed by
        # scale down, or a fallback replacement) still gets its own
        # HourlyBillingRecord even though it never overlapped with the
        # hour's peak. state["max_nodes"] only tracks the peak concurrent
        # snapshot, so the correctness check compares against the union of
        # every node ID observed across all poll ticks this hour instead.
        observed_unique_node_count = len(state["node_ids"])

        ops = sorted(state["ops"], key=lambda op: op[2])
        table = PrettyTable()
        table.field_names = ["Field", "Value"]
        table.add_row([
            "Wall-clock hour",
            f"{hour_start.strftime('%Y-%m-%d %H:%M')}-{hour_end.strftime('%H:%M')} UTC",
        ])
        table.add_row(["Max live node count observed", state["max_nodes"]])
        table.add_row(["Unique nodes phoned home (observed)", observed_unique_node_count])
        table.add_row(["Scaling operations in this hour", len(ops)])
        for step_label, node_change, start, end in ops:
            table.add_row([
                f"  {step_label}",
                f"{node_change}  ({start.strftime('%H:%M:%S')}-{end.strftime('%H:%M:%S')})",
            ])
        table.add_row(["billingPeriod queried", billing_period_str])
        table.add_row(["Nodes billed per CP DB", billed_node_count])
        mismatch = billed_node_count != observed_unique_node_count
        table.add_row(["MATCH", "NO -- MISMATCH" if mismatch else "yes"])

        self.log.info(
            f"Hourly billing window report for cluster={self.cluster_id}:\n{table}"
        )
        if mismatch:
            detail = {
                "cluster_id": self.cluster_id,
                "hour": hour_start.isoformat(),
                "observed_max_nodes": state["max_nodes"],
                "observed_unique_nodes": observed_unique_node_count,
                "billed_nodes": billed_node_count,
                "billing_period": billing_period_str,
                "ops": [
                    {"step": s, "node_change": n, "start": st.isoformat(), "end": e.isoformat()}
                    for s, n, st, e in ops
                ],
            }
            self.mismatches.append(detail)
            self.log.error(
                f"Hourly billing MISMATCH for cluster={self.cluster_id}, "
                f"hour={hour_start.isoformat()}: observed unique nodes="
                f"{observed_unique_node_count} (max concurrent={state['max_nodes']}), "
                f"CP billed nodes={billed_node_count} "
                f"(billingPeriod={billing_period_str})"
            )
        return True

    def stop(self):
        """Signal the tracker thread to stop after its current iteration."""
        self._stop_event.set()

    def stop_and_flush(self, timeout: float = None):
        """
        Stop polling and block until every hour whose report window has
        already opened gets reported, waiting up to `timeout` seconds for
        any hour(s) still short of their report_delay_minutes buffer.

        Call this at the end of the test (before teardown tears down the
        cluster/CP DB connection) rather than a bare stop() — otherwise the
        most recent 1-2 hours of scaling activity never get their
        comparison report logged.

        :param timeout: Max seconds to wait for pending hours to become due
            and get reported. None waits indefinitely.
        """
        self._stop_event.set()
        deadline = time.time() + timeout if timeout is not None else None
        while True:
            self._report_due_hours()
            with self._lock:
                pending = [h for h in self._hourly_state if h not in self._reported_hours]
            if not pending:
                break
            if deadline is not None and time.time() >= deadline:
                self.log.warning(
                    f"HourlyBillingWindowTracker.stop_and_flush timed out for "
                    f"cluster={self.cluster_id} with {len(pending)} hour(s) still "
                    f"unreported: {[h.isoformat() for h in sorted(pending)]}"
                )
                break
            sleep_for = 30 if deadline is None else max(1, min(30, deadline - time.time()))
            time.sleep(sleep_for)
        if self.is_alive():
            self.join(timeout=10)
