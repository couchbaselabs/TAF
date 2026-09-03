"""
Created on 14-August-2026

@author: vaibhav.somasundaram@couchbase.com

Hash-based COUNT(DISTINCT) - Enterprise Analytics (on-prem / columnar) functional tests.
"""
import json
from concurrent.futures import ThreadPoolExecutor

from cb_server_rest_util.analytics.analytics_api import AnalyticsRestAPI
from TestInput import TestInputSingleton
from cbas_utils.cbas_utils_columnar import CbasUtil as ColumnarCbasUtil
from cbas_utils.cbas_utils_on_prem import CBASRebalanceUtil
from Columnar.onprem.columnar_onprem_base import ColumnarOnPremBase
from shell_util.remote_connection import RemoteMachineShellConnection

Q_SCALAR = "SELECT VALUE COUNT(DISTINCT x.name) FROM {0} x;"
Q_GROUPED_CITY = ("SELECT x.city, COUNT(DISTINCT x.name) AS d FROM {0} x "
                  "GROUP BY x.city;")
Q_GROUPED_TYPE = ("SELECT x.`type`, COUNT(DISTINCT x.name) AS d FROM {0} x "
                  "GROUP BY x.`type`;")
Q_COUNT_ALL = "SELECT VALUE COUNT(*) FROM {0} x;"
Q_BUSIEST_CITY = ("SELECT VALUE x.city FROM {0} x GROUP BY x.city "
                  "ORDER BY COUNT(*) DESC LIMIT 1;")


class AggregateDistinctHash(ColumnarOnPremBase):

    LOAD_BATCH = 2000        # docs per INSERT statement
    DOC_SIZE = 256           # `padding` size only; no queried field depends on it

    HASH_TOKEN = "agg-sql-count-distinct-hash"
    HASH_FLAG = "compiler.aggregate.distinct.hash"
    HASH_MEM = "compiler.aggregate.distinct.hash.memory"

    HOTEL_COLLECTION = "adh_hotel"
    MIXED_COLLECTION = "adh_mixed"
    JOIN_COLLECTION = "adh_join"
    SHARED_COLLECTIONS = (HOTEL_COLLECTION, MIXED_COLLECTION, JOIN_COLLECTION)

    LOAD_TIMEOUT = 3600            # bulk INSERT of the shared fixture
    INSERT_TIMEOUT = 600           # one mixed-doc INSERT batch
    QUERY_TIMEOUT = 600            # ordinary EXPLAIN / SELECT
    SPILL_QUERY_TIMEOUT = 1800     # spilling query: run generation + merge
    CONCURRENT_TIMEOUT = 1200      # per-query budget under the herd
    CANCEL_BUDGET = 120            # no-hang budget for the cancel/timeout case
    RESTART_RECOVER_TIMEOUT = 600  # enterprise-analytics bounce -> serving

    # Memory-budget boundaries. The hash aggregate reserves budget/FRAME_BYTES
    # frames, so a budget under one frame is the 0-frame edge case.
    FRAME_BYTES = 32 * 1024
    SUB_FRAME_MEMORY = "16KB"      # < one frame -> 0-frame boundary
    INERT_MEMORY = "1MB"           # must be ignored entirely when the flag is OFF
    UNIT_SUFFIXES = ("64MB", "128MB", "1GB")   # unit-parsing matrix
    # Above any `price` the doc-gen emits, so the selection is provably empty.
    IMPOSSIBLE_PRICE = 999999999

    # Operator names the spill path builds (ensureSpillStructures ->
    # ExternalSortRunGenerator -> FrameSorterMergeSort). Any one of these in a
    # `profile: timings` envelope proves the aggregate actually spilled rather
    # than completing in memory. Matched case-insensitively.
    SPILL_PROFILE_MARKERS = ("ExternalSortRunGenerator", "ExternalSortRunMerger",
                             "FrameSorterMergeSort", "external-sort", "sort-run")

    def _base_setup(self):
        """Run the inherited setUp, optionally without the analytics wipe.

        CBASBaseTest.setUp calls CbasUtil.cleanup_cbas() unconditionally, which
        drops this suite's collections. _provision is written to reuse a
        collection that is already present with the expected row count, so the
        wipe forces a needless re-create and re-load of no_of_docs documents in
        every test.

        skip_cbas_cleanup (default True, and set on every line of
        aggregate_distinct_hash.conf) suppresses it for the duration of the
        base setUp only; the original method is always restored. The final conf
        line's drop_adh_collections=True hands the cluster back clean.
        """
        if not TestInputSingleton.input.param("skip_cbas_cleanup", True):
            super(AggregateDistinctHash, self).setUp()
            return
        original = ColumnarCbasUtil.cleanup_cbas
        ColumnarCbasUtil.cleanup_cbas = lambda *args, **kwargs: True
        try:
            super(AggregateDistinctHash, self).setUp()
        finally:
            ColumnarCbasUtil.cleanup_cbas = original

    def setUp(self):
        self._base_setup()

        self.no_of_docs = self.input.param("no_of_docs", 2000)
        self.hetero_docs = self.input.param("hetero_docs", 2000)
        self.storage_format = self.input.param("storage", "column")

        self.spill_memory = self.input.param("spill_memory", "64KB")
        self.large_memory = self.input.param("large_memory", "2GB")

        self.drop_adh_collections = self.input.param(
            "drop_adh_collections", False)
        self._hotel_ref = None
        self._mixed_ref = None
        self.hotel_join = None

        self.analytics_api = AnalyticsRestAPI(self.columnar_cluster.master)

        # topology helper for the rebalance/failover exactness cases
        self.rebalance_util = CBASRebalanceUtil(
            self.cluster_util, self.bucket_util, self.task, False, self.cbas_util)
        if not hasattr(self.columnar_cluster, "available_servers"):
            self.columnar_cluster.available_servers = []

        self.setup_collections()
        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.setUp.__name__)

    def tearDown(self):
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)

        self._restore_topology_if_shrunk()
        if self.drop_adh_collections:
            self._drop_shared_collections()
        super(AggregateDistinctHash, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage="Teardown")

    def _restore_topology_if_shrunk(self):
        """Rebalance a cbas node back in if a topology test left the cluster
        short of the count it started with.
        """
        expected = getattr(self, "_expected_cbas_nodes", None)
        if expected is None:
            return
        try:
            actual = len(self.cluster_util.get_nodes_from_services_map(
                self.columnar_cluster, service_type="cbas", get_all_nodes=True,
                servers=self.columnar_cluster.nodes_in_cluster))
            if actual >= expected:
                return
            self.log.warning(
                "cluster has {0} cbas node(s), started with {1} - rebalancing "
                "one back in".format(actual, expected))
            # Analytics-only add-back: the node returns with the cbas service
            # alone. If the cluster rejects a cbas-only rebalance-in, this is
            # the line to revisit.
            task, self.columnar_cluster.available_servers = \
                self.rebalance_util.rebalance(
                    cluster=self.columnar_cluster, cbas_nodes_in=1,
                    in_node_services="cbas",
                    available_servers=self.columnar_cluster.available_servers)
            if not self.rebalance_util.wait_for_rebalance_task_to_complete(
                    task, self.columnar_cluster, True, True):
                self.log.error(
                    "COULD NOT restore the cbas node - the cluster is left "
                    "DEGRADED and needs manual repair (restart "
                    "enterprise-analytics on the affected nodes, then "
                    "rebalance)")
        except Exception as e:                           # noqa: BLE001
            self.log.error("topology restore raised: {0}".format(e))

    def _drop_shared_collections(self):
        """Remove every collection this suite provisions, so a run with
        drop_adh_collections=True leaves the cluster as it found it.
        """
        for name in self.SHARED_COLLECTIONS:
            full_name = self._ref_for(name)["full_name"]
            try:
                if not self.cbas_util.drop_dataset(
                        self.columnar_cluster, full_name, if_exists=True,
                        timeout=self.QUERY_TIMEOUT,
                        analytics_timeout=self.QUERY_TIMEOUT):
                    self.log.warning("could not drop {0}".format(full_name))
            except Exception as e:                       # noqa: BLE001
                self.log.warning("dropping {0} raised: {1}".format(
                    full_name, e))

    # ------------------------------------------------------------- provisioning
    def _load(self, ref, no_of_docs, template, params=None):
        """ONE INSERT statement per `LOAD_BATCH` docs, issued SEQUENTIALLY."""
        ok = self.cbas_util.load_doc_to_standalone_collection(
            self.columnar_cluster, ref["name"], ref["dataverse_name"],
            ref["database_name"], no_of_docs=no_of_docs, doc_template=template,
            doc_template_params=params, document_size=self.DOC_SIZE,
            batch_size=self.LOAD_BATCH, max_concurrent_batches=1,
            analytics_timeout=self.LOAD_TIMEOUT, timeout=self.LOAD_TIMEOUT)
        if not ok:
            self.fail("Failed to load {0} docs (template={1}) into {2}".format(
                no_of_docs, template, ref["full_name"]))

    @staticmethod
    def _mixed_docs(n):
        """Controlled mixed-type / NULL / MISSING docs (no `id` field, so the
        AUTOGENERATED UUID primary key applies). The doc-gen `heterogeneous`
        (Person) template can't be used here because it emits a string `id`
        that collides with the UUID PK (23069) and its id counter is racy under
        the concurrent loader. `email` covers present/NULL/MISSING (3 distinct
        non-null values); `age` covers mixed int/string types."""
        emails = ["a@x.com", "b@y.com", "c@z.com"]
        docs = []
        for i in range(n):
            d = {}
            r = i % 6
            if r in (0, 4):
                d["email"] = emails[0]          # duplicate value
            elif r == 1:
                d["email"] = emails[1]
            elif r == 2:
                d["email"] = None               # JSON null -> skipped by step()
            elif r == 3:
                pass                            # MISSING field -> skipped
            else:
                d["email"] = emails[2]
            d["age"] = 30 if (i % 2 == 0) else "30"   # mixed int vs string
            docs.append(d)
        return docs

    def _load_mixed(self, ref, n, batch=None):
        """Populate the mixed collection via direct INSERT (no doc-gen template).
        Batched at LOAD_BATCH (not 500) so 2000 tiny {email, age} docs cost one
        statement (~30s) instead of four (~107s measured)."""
        batch = batch or self.LOAD_BATCH
        docs = self._mixed_docs(n)
        for i in range(0, len(docs), batch):
            if not self.cbas_util.insert_into_standalone_collection(
                    self.columnar_cluster, ref["name"], docs[i:i + batch],
                    ref["dataverse_name"], ref["database_name"],
                    analytics_timeout=self.INSERT_TIMEOUT,
                    timeout=self.INSERT_TIMEOUT):
                self.fail("Failed to INSERT mixed docs into {0}".format(
                    ref["full_name"]))

    def _ref_for(self, name):
        """Fixed-name ref in Default.Default (cleanup_cbas never drops the
        `Default` database or the `Default.Default` dataverse - only datasets -
        so a fixed home survives whatever the base setUp does)."""
        return {"name": name, "dataverse_name": "Default",
                "database_name": "Default",
                "full_name": "Default.Default.{0}".format(name)}

    def _row_count(self, full_name):
        """COUNT(*) for a collection, or None if it does not exist."""
        status, _, _, results, _, _ = self._run(Q_COUNT_ALL.format(full_name))
        if status != "success" or not results:
            return None
        return results[0]

    def _provision(self, name, expected_rows, loader, label):
        """Create + load `name` only if it is not already present with exactly
        `expected_rows` rows; otherwise reuse it as-is. Returns the ref.
        """
        ref = self._ref_for(name)
        rows = self._row_count(ref["full_name"])
        if rows == expected_rows:
            self.log.info("reusing persistent {0} collection {1} ({2} rows)"
                          .format(label, ref["full_name"], rows))
            return ref
        if rows is not None:
            self.log.info("{0} collection has {1} rows, expected {2} - "
                          "re-provisioning".format(label, rows, expected_rows))
            self.cbas_util.drop_dataset(
                self.columnar_cluster, ref["full_name"], if_exists=True,
                timeout=self.QUERY_TIMEOUT,
                analytics_timeout=self.QUERY_TIMEOUT)
        # Empty primary_key => PRIMARY KEY(id: UUID) AUTOGENERATED, so the
        # doc-gen / mixed docs (no "id" field) insert without a 23071/23069
        # rejection.
        if not self.cbas_util.create_standalone_collection(
                self.columnar_cluster, name,
                dataverse_name=ref["dataverse_name"],
                database_name=ref["database_name"], primary_key={},
                storage_format=self.storage_format):
            self.fail("Failed to create the {0} collection {1}".format(
                label, ref["full_name"]))
        loader(ref)
        return ref

    def setup_collections(self):
        self._hotel_ref = self._provision(
            self.HOTEL_COLLECTION, self.no_of_docs,
            lambda ref: self._load(ref, self.no_of_docs, "hotel"), "hotel")
        self.hotel = self._hotel_ref["full_name"]

        self._mixed_ref = None
        self.hotel_join = None

    @property
    def hetero(self):
        if not self._mixed_ref:
            self._mixed_ref = self._provision(
                self.MIXED_COLLECTION, self.hetero_docs,
                lambda ref: self._load_mixed(ref, self.hetero_docs), "mixed")
        return self._mixed_ref["full_name"]

    def _ensure_join(self):
        """Second hotel collection for the JOIN case, provisioned on FIRST READ
        and then PERSISTENT (only test_distinct_over_join needs it)."""
        if self.hotel_join:
            return self.hotel_join
        self.hotel_join = self._provision(
            self.JOIN_COLLECTION, self.no_of_docs,
            lambda ref: self._load(ref, self.no_of_docs, "hotel"),
            "JOIN")["full_name"]
        return self.hotel_join

    # ------------------------------------------------------------------ helpers
    def _opts(self, flag=None, memory=None):
        """Build the SET-clause prefix (dotted keys, backtick-quoted). Options
        travel as SET statements on the wire, NOT request params."""
        parts = []
        if flag is not None:
            parts.append('SET `{0}` "{1}";'.format(
                self.HASH_FLAG, "true" if flag else "false"))
        if memory is not None:
            parts.append('SET `{0}` "{1}";'.format(self.HASH_MEM, memory))
        return " ".join(parts)

    def _run(self, stmt, timeout=None, analytics_timeout=None):
        """execute_statement_on_cbas_util -> 6-tuple; returned verbatim so
        callers can branch on status / inspect errors."""
        timeout = timeout or self.QUERY_TIMEOUT
        return self.cbas_util.execute_statement_on_cbas_util(
            self.columnar_cluster, stmt, timeout=timeout,
            analytics_timeout=analytics_timeout or timeout)

    def _results(self, stmt, timeout=None, analytics_timeout=None):
        status, _, errors, results, _, _ = self._run(
            stmt, timeout, analytics_timeout)
        if status != "success":
            self.fail("query failed [{0}]: {1}".format(stmt, errors))
        return results

    def _explain_json(self, stmt, flag=None, memory=None):
        """EXPLAIN <stmt> (with any SET prefix); return the plan as a JSON
        string for substring assertions. The plan comes back in `results`, so
        no raw REST is needed (model: cbas_utils_on_prem.verify_index_used)."""
        full = "{0} EXPLAIN {1}".format(self._opts(flag, memory), stmt).strip()
        return json.dumps(self._results(full))

    def _assert_hash_token(self, stmt, flag, expected_present, memory=None):
        """Run EXPLAIN with the flag set and assert the hash token is
        present/absent. This is the routing DISCRIMINATOR: token present when
        the flag is ON, absent when OFF (OFF is the sort path)."""
        plan = self._explain_json(stmt, flag=flag, memory=memory)
        present = self.HASH_TOKEN in plan
        if present != expected_present:
            self.fail(
                "plan-token mismatch for flag={0}: expected token '{1}' "
                "present={2}, got present={3}.\nstmt=[{4}]\nplan={5}".format(
                    flag, self.HASH_TOKEN, expected_present, present, stmt,
                    plan[:4000]))

    def _assert_parity(self, stmt_body, memory=None):
        """Differential correctness: identical statement under flag OFF vs ON
        must return identical (order-insensitive) results. Runs OFF first so a
        green ON result cannot mask a wrong rewrite."""
        off = self._results(self._opts(False, memory) + " " + stmt_body)
        on = self._results(self._opts(True, memory) + " " + stmt_body)
        if self._norm(off) != self._norm(on):
            self.fail(
                "hash/sort parity broken for [{0}] (memory={1}):\n off={2}\n "
                "on={3}".format(stmt_body, memory, self._norm(off)[:50],
                                self._norm(on)[:50]))
        return off, on

    def _assert_spill_exact(self, stmt_body, expected_count, memory=None):
        """Force the flag ON with a small budget and assert the EXACT count.

        KNOWN ISSUE (MB-72573): on EA 2.3.0 b1126 the spill path throws an NPE
        surfaced as code 25000, so this FAILS on the buggy build - by design,
        the test catches the defect. When the spill path is fixed it must
        return the exact count.
        """
        memory = memory or self.spill_memory
        stmt = "{0} {1}".format(self._opts(True, memory), stmt_body)
        status, _, errors, results, _, _ = self._run(
            stmt, timeout=self.SPILL_QUERY_TIMEOUT)
        if status != "success":
            self.fail(self._spill_defect_msg(
                "hash COUNT(DISTINCT) SPILL path failed with {0} (expected "
                "exact {1}). stmt=[{2}]".format(errors, expected_count, stmt)))
        if results[0] != expected_count:
            self.fail("spill COUNT(DISTINCT) wrong for [{0}]: expected {1}, "
                      "got {2}".format(stmt_body, expected_count, results[0]))

    @staticmethod
    def _spill_defect_msg(detail):
        """One wording for the MB-72573 spill failure, so every spill case
        reports the same defect signature instead of paraphrasing it."""
        return ("KNOWN ISSUE MB-72573: {0}. Spill NPE (null "
                "keyNormalizerFactories in ensureSpillStructures -> "
                "ExternalSort) masked as code 25000 on EA 2.3.0 b1126."
                .format(detail))

    def _assert_spill_parity(self, stmt_body, label, memory=None):
        """Spill-path differential correctness for result sets whose exact
        per-group values are data-dependent: run the SAME small budget under
        flag OFF (sort path, the reference) and flag ON (hash path, spilling)
        and require identical order-insensitive results.

        Used instead of _assert_spill_exact wherever a single expected scalar
        cannot be stated up front.
        """
        memory = memory or self.spill_memory
        off = self._results(self._opts(False, memory) + " " + stmt_body)
        status, _, errors, on, _, _ = self._run(
            self._opts(True, memory) + " " + stmt_body,
            timeout=self.SPILL_QUERY_TIMEOUT)
        if status != "success":
            self.fail(self._spill_defect_msg(
                "{0} failed with {1}".format(label, errors)))
        if self._norm(off) != self._norm(on):
            self.fail("{0}: spill parity broken:\n off={1}\n on={2}".format(
                label, self._norm(off)[:20], self._norm(on)[:20]))
        return off, on

    def _run_concurrently(self, stmt, n, timeout):
        """Fire `stmt` from `n` threads and collect every outcome.

        Returns a list of (status, first_result_or_None, errors) - one entry
        per thread, in submission order. A thread that raises is reported as
        status None with the exception text in `errors` rather than escaping,
        so a single transport failure cannot hide the other n-1 verdicts.
        """
        def one():
            try:
                status, _, errors, results, _, _ = self._run(
                    stmt, timeout=timeout)
                return status, (results[0] if results else None), errors
            except Exception as e:                       # noqa: BLE001
                return None, None, str(e)

        with ThreadPoolExecutor(max_workers=n) as ex:
            return [f.result() for f in [ex.submit(one) for _ in range(n)]]

    # ===================================================== plan routing / gating
    def test_default_is_disabled(self):
        """Default-is-disabled(+explicit-false): no SET, and explicit false,
        both route to the SORT path (hash token absent)."""
        stmt = Q_SCALAR.format(self.hotel)
        # no flag at all -> default false
        plan = self._explain_json(stmt)
        if self.HASH_TOKEN in plan:
            self.fail("default (no SET) must NOT use the hash operator; plan={0}"
                      .format(plan[:2000]))
        # explicit false
        self._assert_hash_token(stmt, flag=False, expected_present=False)

    def test_flag_routes_to_hash_operator(self):
        """Flag-routes-to-hash-operator: SET true -> hash token present."""
        stmt = Q_SCALAR.format(self.hotel)
        self._assert_hash_token(stmt, flag=True, expected_present=True)

    def test_exact_option_string(self):
        stmt = Q_SCALAR.format(self.hotel)
        bad = 'SET `compiler.aggregatedistincthash` "true"; ' + stmt
        status, _, errors, _, _, _ = self._run(bad)
        if status == "success":
            self.fail("no-inner-dot option name must be REJECTED by SET, but "
                      "the statement succeeded")
        # The columnar CbasUtil has no validate_error_in_response; inline the
        # code check. Compare as strings so int/str 24022 both match.
        codes = [str(e.get("code")) for e in (errors or [])]
        if "24022" not in codes:
            self.fail("expected code 24022 for the misspelled option name, got "
                      "{0}".format(errors))

    def test_plan_cache_keyed_by_the_flag(self):
        """Plan-cache-keyed-by-the-flag: the flag (carried as a SET clause) is
        part of the plan-cache key, so ON and OFF never share a cached plan.
        Sequence [SET-true, SET-true, SET-false] must be [miss, hit, miss]."""
        base = Q_SCALAR.format(self.hotel)
        on = self._opts(True) + " " + base
        off = self._opts(False) + " " + base
        c1, _, s1 = self._run_with_cache_flag(on)
        c2, _, s2 = self._run_with_cache_flag(on)
        c3, _, s3 = self._run_with_cache_flag(off)
        if not (s1 == s2 == s3 == "success"):
            self.fail("plan-cache runs did not all succeed: {0}".format(
                [s1, s2, s3]))
        if [c1, c2, c3] != [False, True, False]:
            self.fail("expected cachedPlan [miss, hit, miss] across flag ON/ON/"
                      "OFF, got {0}".format([c1, c2, c3]))

    def test_window_distinct_also_rewritten(self):
        """Window-COUNT(DISTINCT)-also-rewritten: the rule descends into WINDOW
        nested plans, so the hash token appears inside the window subplan."""
        stmt = ("SELECT x.city, COUNT(DISTINCT x.name) OVER "
                "(PARTITION BY x.city) AS d FROM {0} x LIMIT 5;".format(self.hotel))
        self._assert_hash_token(stmt, flag=True, expected_present=True)
        self._assert_hash_token(stmt, flag=False, expected_present=False)

    # ============================================================ setting mechanism
    def test_per_request_scope(self):
        """Per-request-scope: a SET on one request must not leak into the next
        request (the option is request-scoped)."""
        stmt = Q_SCALAR.format(self.hotel)
        # request 1 sets it ON
        self._assert_hash_token(stmt, flag=True, expected_present=True)
        # request 2 with NO SET must be back to the default (sort path)
        plan = self._explain_json(stmt)
        if self.HASH_TOKEN in plan:
            self.fail("the flag leaked across requests - a subsequent request "
                      "with no SET used the hash operator; plan={0}".format(
                          plan[:2000]))

    def test_config_api_live_update(self):
        """Config-API-live-update: the flag is runtime-mutable
        (OptionClassificationUtil), so a cluster-wide PUT of
        `compiler.aggregate.distinct.hash` through the analytics service-config
        API must take effect on the NEXT request with no service restart - and
        an explicit per-request SET must still override it in both directions.

        Restores the original cluster-wide value before returning, so a failure
        cannot leave the flag globally ON for every later test.
        """
        stmt = Q_SCALAR.format(self.hotel)
        status, before, _ = self.analytics_api.get_service_config()
        if not status:
            self.fail("could not read the analytics service config: {0}"
                      .format(before))
        original = before.get(self.HASH_FLAG) if isinstance(before, dict) else None

        status, content, _ = self.analytics_api.update_service_config(
            {self.HASH_FLAG: True})
        if not status:
            self.fail("PUT {0}=true to the service-config API was rejected: "
                      "{1}".format(self.HASH_FLAG, content))
        try:
            # No restart, no SET clause: the cluster-wide value alone must
            # route the very next request to the hash operator.
            plan = self._explain_json(stmt)
            if self.HASH_TOKEN not in plan:
                self.fail(
                    "cluster-wide {0}=true did not take effect on the next "
                    "request (no restart): the plan still uses the sort path. "
                    "plan={1}".format(self.HASH_FLAG, plan[:2000]))
            # A per-request SET must still win over the cluster-wide value.
            self._assert_hash_token(stmt, flag=False, expected_present=False)
        finally:
            restore = False if original is None else original
            status, content, _ = self.analytics_api.update_service_config(
                {self.HASH_FLAG: restore})
            if not status:
                self.log.error(
                    "COULD NOT restore cluster-wide {0} to {1}: {2}. Every "
                    "later test now runs with the flag forced - reset it "
                    "before trusting further results.".format(
                        self.HASH_FLAG, restore, content))
        # Back at the restored value, the default routing must be the sort path.
        plan = self._explain_json(stmt)
        if self.HASH_TOKEN in plan:
            self.fail("cluster-wide {0} was not restored - the default request "
                      "still routes to the hash operator".format(self.HASH_FLAG))

    # ==================================================================== scope
    def test_scope_is_count_distinct_only(self):
        """Scope-is-COUNT(DISTINCT)-only: with the flag ON, COUNT(DISTINCT) is
        rewritten but SUM/AVG(DISTINCT) and COUNTN(DISTINCT) are NOT (they keep
        their plain sql-agg functions)."""
        stmt = ("SELECT COUNT(DISTINCT x.name) AS cd, "
                "SUM(DISTINCT x.price) AS sd, AVG(DISTINCT x.price) AS ad, "
                "COUNTN(DISTINCT x.price) AS cn FROM {0} x;".format(self.hotel))
        plan = self._explain_json(stmt, flag=True)
        if self.HASH_TOKEN not in plan:
            self.fail("COUNT(DISTINCT) was not rewritten to the hash operator "
                      "with the flag ON; plan={0}".format(plan[:3000]))
        for other in ("agg-sql-sum-distinct", "agg-sql-avg-distinct",
                      "agg-sql-countn-distinct"):
            if other in plan and other.replace("-distinct", "-distinct-hash") in plan:
                self.fail("{0} was unexpectedly rewritten to a hash variant; "
                          "scope must be COUNT(DISTINCT) only".format(other))
        # no *-distinct-hash token other than count may appear
        for token in ("agg-sql-sum-distinct-hash", "agg-sql-avg-distinct-hash",
                      "agg-sql-countn-distinct-hash"):
            if token in plan:
                self.fail("unexpected hash rewrite of a non-COUNT aggregate: "
                          "{0}".format(token))

    # ============================================================== correctness
    def test_hash_vs_sort_parity(self):
        """Hash-vs-sort-parity: exact COUNT(DISTINCT) must match between flag
        OFF and ON, and equal the loaded doc count (hotel.name is unique)."""
        body = Q_SCALAR.format(self.hotel)
        _, on = self._assert_parity(body)
        if on[0] != self.no_of_docs:
            self.fail("expected exact distinct count {0} (unique hotel.name), "
                      "got {1}".format(self.no_of_docs, on[0]))

    def test_group_by_per_group(self):
        """GROUP-BY-per-group: grouped COUNT(DISTINCT) is identical OFF vs ON."""
        self._assert_parity(Q_GROUPED_CITY.format(self.hotel))

    def test_all_duplicates_collapse(self):
        """All-duplicates-collapse: many rows carrying ONE repeated value must
        collapse to exactly 1.

        The city is resolved at runtime (busiest city) rather than hardcoded: a
        literal that the doc-gen no longer emits would select 0 rows, and a
        count of 0 would satisfy a "0 or 1" guard while exercising nothing.
        """
        city = self._results(Q_BUSIEST_CITY.format(self.hotel))
        if not city or not city[0]:
            self.fail("could not resolve a city to test against - the {0} "
                      "collection has no grouped `city` values".format(
                          self.hotel))
        body = ("SELECT VALUE COUNT(DISTINCT x.city) FROM {0} x "
                "WHERE x.city = '{1}';".format(self.hotel, city[0]))
        _, on = self._assert_parity(body)
        if on != [1]:
            self.fail("all-duplicates COUNT(DISTINCT city) over city='{0}' must "
                      "be exactly 1, got {1}".format(city[0], on))

    def test_mixed_json_types(self):
        """Mixed-JSON-types: heterogeneous `age` (int|string) - hash and sort
        must agree on distinct count across mixed types."""
        body = "SELECT VALUE COUNT(DISTINCT p.age) FROM {0} p;".format(self.hetero)
        self._assert_parity(body)

    def test_null_missing(self):
        """NULL/MISSING: step() skips NULL/MISSING/SYSTEM_NULL; heterogeneous
        `email` (string|null|MISSING) must give identical counts OFF vs ON."""
        body = "SELECT VALUE COUNT(DISTINCT p.email) FROM {0} p;".format(self.hetero)
        self._assert_parity(body)

    def test_empty_input(self):
        """Empty-input: COUNT(DISTINCT) over an empty selection is exactly [0].

        Asserted unconditionally: an empty result list - the aggregate emitting
        no row at all instead of a zero - is the failure this case exists to
        catch, so it must not be waved through as "nothing to check".
        """
        body = ("SELECT VALUE COUNT(DISTINCT x.name) FROM {0} x "
                "WHERE x.price > {1};".format(self.hotel, self.IMPOSSIBLE_PRICE))
        _, on = self._assert_parity(body)
        if on != [0]:
            self.fail("empty-input COUNT(DISTINCT) must be exactly [0], got "
                      "{0}".format(on))

    # ============================================================ disk-backed spill
    # KNOWN ISSUE MB-72573: the spill path NPEs (code 25000) on EA 2.3.0 b1126.
    # These assert the correct exact result, so they FAIL until the spill is fixed.
    def test_tiny_budget_forces_spill(self):
        """Tiny-budget-forces-spill: a budget far below the distinct-set size
        (`spill_memory`, default 64KB = 2 frames, against `no_of_docs` unique
        names) must spill and still return the exact count."""
        self._assert_spill_exact(Q_SCALAR.format(self.hotel), self.no_of_docs)

    def test_spill_with_group_by(self):
        """Spill-with-GROUP-BY: spilling under a GROUP BY must stay exact.

        Per-group counts are data-dependent, so exactness is asserted as parity
        against the flag-OFF sort path under the SAME small budget.
        """
        self._assert_spill_parity(Q_GROUPED_CITY.format(self.hotel),
                                  "spill-with-GROUP-BY")

    def test_spill_path_is_actually_exercised(self):
        """Spill-path-is-actually-exercised"""
        body = Q_SCALAR.format(self.hotel)
        stmt = "{0} {1}".format(self._opts(True, self.spill_memory), body)
        content = self._analytics_request(
            stmt, extra_params={"profile": "timings"},
            timeout=self.SPILL_QUERY_TIMEOUT)
        if content.get("status") != "success":
            self.fail(self._spill_defect_msg(
                "profiled spill query failed with {0}".format(
                    content.get("errors"))))
        results = content.get("results") or []
        if not results or results[0] != self.no_of_docs:
            self.fail("profiled spill query returned {0}, expected the exact "
                      "count {1}".format(results, self.no_of_docs))
        profile = json.dumps(content.get("profile") or {})
        if not profile or profile == "{}":
            self.fail("the server returned no `profile` for a "
                      "profile=timings request, so a spill cannot be "
                      "confirmed; envelope keys={0}".format(
                          sorted(content.keys())))
        spill_markers = [m for m in self.SPILL_PROFILE_MARKERS
                         if m.lower() in profile.lower()]
        if not spill_markers:
            self.fail(
                "no spill signal in the job profile: none of {0} appear, so "
                "the {1} budget did NOT force a spill and this case is not "
                "exercising the spill path.\nprofile={2}".format(
                    list(self.SPILL_PROFILE_MARKERS), self.spill_memory,
                    profile[:4000]))
        self.log.info("spill confirmed via profile markers: {0}".format(
            spill_markers))

    def test_spill_over_string_composite_key(self):
        """Spill-over-string/composite-key: spilling with an object/composite
        DISTINCT key must stay exact (the composite key path is what NPEs)."""
        body = ("SELECT VALUE COUNT(DISTINCT {{\"n\": x.name, \"c\": x.city}}) "
                "FROM {0} x;".format(self.hotel))
        self._assert_spill_exact(body, self.no_of_docs)

    def test_mixed_in_memory_and_spilling_groups(self):
        """Mixed-in-memory-and-spilling-groups: grouped by the low-cardinality
        `type`, some groups fit the budget and some overflow it; every group
        must stay exact against the sort path."""
        self._assert_spill_parity(Q_GROUPED_TYPE.format(self.hotel),
                                  "mixed in-memory/spill groups")

    # ================================================================= multi-node
    def test_distributed_exactness(self):
        """Distributed-exactness: on a >=2-node analytics cluster the
        non-combinable hash aggregate must still produce the exact count (by
        co-location, not partial-merge), equal to the sort path.
        """
        body = Q_SCALAR.format(self.hotel)
        _, on = self._assert_parity(body)
        if on[0] != self.no_of_docs:
            self.fail("distributed exact distinct count wrong: expected {0}, "
                      "got {1}".format(self.no_of_docs, on[0]))

    # =========================================================== topology exactness
    # The hash aggregate is NON-COMBINABLE (finishPartial=finish), so multi-node
    # exactness holds by CO-LOCATION - the rows of a group must land on one node -
    # rather than by merging partials. A rebalance/failover REDISTRIBUTES partitions,
    # which is precisely the thing that can break that invariant, and it would break
    # it SILENTLY: no error, just a wrong count. Hence these two.
    def _assert_exact_and_parity(self, when):
        """The hash count must equal the sort count AND the known row total."""
        body = Q_SCALAR.format(self.hotel)
        _, on = self._assert_parity(body)
        if on[0] != self.no_of_docs:
            self.fail("{0}: hash COUNT(DISTINCT) wrong - expected {1}, got {2} "
                      "(non-combinable aggregate lost/duplicated rows across the "
                      "topology change)".format(when, self.no_of_docs, on[0]))

    def test_exactness_survives_rebalance(self):
        """Rebalance-exactness: COUNT(DISTINCT) stays exact across a cbas node
        going out and coming back in.

        The add-back is part of the ASSERTION, not cleanup. Recovery from a
        mid-test failure is tearDown's job (_restore_topology_if_shrunk), which
        is why there is no try/finally here.
        """
        self._expected_cbas_nodes = len(
            self.cluster_util.get_nodes_from_services_map(
                self.columnar_cluster, service_type="cbas", get_all_nodes=True,
                servers=self.columnar_cluster.nodes_in_cluster))
        self._assert_exact_and_parity("before rebalance")
        task, self.columnar_cluster.available_servers = \
            self.rebalance_util.rebalance(
                cluster=self.columnar_cluster, cbas_nodes_out=1,
                exclude_nodes=[self.columnar_cluster.master],
                available_servers=self.columnar_cluster.available_servers)
        if not self.rebalance_util.wait_for_rebalance_task_to_complete(
                task, self.columnar_cluster, True, True):
            self.fail("rebalance-out of a cbas node failed")
        self._assert_exact_and_parity("after rebalance-out")
        # Add the node back with the cbas service only. If the cluster rejects
        # a cbas-only rebalance-in, this is the line to revisit.
        task, self.columnar_cluster.available_servers = \
            self.rebalance_util.rebalance(
                cluster=self.columnar_cluster, cbas_nodes_in=1,
                in_node_services="cbas",
                available_servers=self.columnar_cluster.available_servers)
        if not self.rebalance_util.wait_for_rebalance_task_to_complete(
                task, self.columnar_cluster, True, True):
            self.fail("rebalance-in of a cbas node failed")
        self._assert_exact_and_parity("after rebalance-in")

    def test_exactness_survives_failover(self):
        """Failover-exactness: COUNT(DISTINCT) stays exact across a hard failover
        with FullRecovery.

        DO NOT assume FullRecovery restores the node: on 2026-08-23 the
        framework's own recovery step raised "Rebalance failed while doing
        recovery after failover" and left the node `inactiveAdded` in a state no
        REST call could clear. tearDown attempts a rebalance-in
        (_restore_topology_if_shrunk), but that cannot help when the rebalance
        path itself is what is broken - a failure here may still need manual
        repair.
        """
        self._expected_cbas_nodes = len(
            self.cluster_util.get_nodes_from_services_map(
                self.columnar_cluster, service_type="cbas", get_all_nodes=True,
                servers=self.columnar_cluster.nodes_in_cluster))
        self._assert_exact_and_parity("before failover")
        self.columnar_cluster.available_servers, _, _ = \
            self.rebalance_util.failover(
                cluster=self.columnar_cluster, cbas_nodes=1,
                failover_type=self.input.param("failover_type", "Hard"),
                action="FullRecovery",
                exclude_nodes=[self.columnar_cluster.master],
                available_servers=self.columnar_cluster.available_servers)
        self._assert_exact_and_parity("after failover + FullRecovery")

    # =============================================================== memory config
    def test_default_32mb_and_inert_when_off(self):
        """Default-32MB+inert-when-off: with the flag OFF the memory budget is
        inert (no hash operator regardless of a set budget)."""
        stmt = Q_SCALAR.format(self.hotel)
        self._assert_hash_token(stmt, flag=False, expected_present=False,
                                memory=self.INERT_MEMORY)
        # flag ON with the default budget (no .memory) still routes to hash
        self._assert_hash_token(stmt, flag=True, expected_present=True)

    def test_size_string_unit_parsing(self):
        """Size-string/unit-parsing: unit suffixes (KB/MB/GB) are accepted and
        the query runs correctly under each."""
        body = "SELECT VALUE COUNT(DISTINCT x.city) FROM {0} x;".format(self.hotel)
        # The flag-OFF baseline carries no budget, so it is identical for every
        # unit - computing it once saves two EA statements (~60s).
        off = self._results(self._opts(False) + " " + body)
        for unit in self.UNIT_SUFFIXES:
            on = self._results(self._opts(True, unit) + " " + body)
            if self._norm(off) != self._norm(on):
                self.fail("unit '{0}' changed the result: off={1} on={2}".format(
                    unit, off, on))

    def test_budget_below_one_frame(self):
        """Budget-below-one-frame(0-frames): a budget under one 32KB frame is a
        boundary - it must be handled cleanly (accepted and spilling, or a clean
        error), never a hang or a wrong count.

        KNOWN ISSUE MB-72573: at this budget the query spills and hits the 25000
        NPE, so it fails until the spill path is fixed.
        """
        self._assert_spill_exact(Q_SCALAR.format(self.hotel), self.no_of_docs,
                                 memory=self.SUB_FRAME_MEMORY)

    def test_invalid_memory_value(self):
        """Invalid-memory-value: a non-parseable budget must be rejected with a
        clean error, not silently ignored."""
        body = Q_SCALAR.format(self.hotel)
        stmt = self._opts(True, "not-a-size") + " " + body
        status, _, errors, _, _, _ = self._run(stmt)
        if status == "success":
            self.fail("an invalid memory budget 'not-a-size' was accepted; it "
                      "must be rejected with an error")
        self.log.info("invalid-memory-value rejected as expected: {0}".format(
            errors))

    # ===================================================================== complex
    # Each complex case verifies the plan uses the hash operator
    # BEFORE asserting differential correctness.
    def test_distinct_over_join(self):
        """Distinct-over-JOIN: COUNT(DISTINCT) over a JOIN of two collections."""
        join = self._ensure_join()   # lazily provision the second collection
        body = ("SELECT VALUE COUNT(DISTINCT x.name) FROM {0} x JOIN {1} y "
                "ON x.city = y.city;".format(self.hotel, join))
        self._assert_hash_token(body, flag=True, expected_present=True)
        self._assert_parity(body)

    def test_composite_distinct(self):
        """Composite: COUNT(DISTINCT <object>) over a composite key."""
        body = ("SELECT VALUE COUNT(DISTINCT {{\"c\": x.city, \"t\": x.`type`}}) "
                "FROM {0} x;".format(self.hotel))
        self._assert_hash_token(body, flag=True, expected_present=True)
        self._assert_parity(body)

    def test_computed_nested_distinct(self):
        """Computed/nested: COUNT(DISTINCT <expr over a nested field>)."""
        body = ("SELECT VALUE COUNT(DISTINCT LOWER(x.name)) FROM {0} x;".format(
            self.hotel))
        self._assert_hash_token(body, flag=True, expected_present=True)
        self._assert_parity(body)

    def test_multiple_distinct_per_group(self):
        """Multiple-distinct-per-group: two COUNT(DISTINCT)s in one grouped
        SELECT - both must be rewritten to the hash operator."""
        body = ("SELECT x.city, COUNT(DISTINCT x.name) AS dn, "
                "COUNT(DISTINCT x.`type`) AS dt FROM {0} x "
                "GROUP BY x.city;".format(self.hotel))
        plan = self._explain_json(body, flag=True)
        if plan.count(self.HASH_TOKEN) < 2:
            self.fail("expected >=2 hash-operator occurrences for two "
                      "COUNT(DISTINCT)s, got {0}.\nplan={1}".format(
                          plan.count(self.HASH_TOKEN), plan[:3000]))
        self._assert_parity(body)

    def test_having_subquery_distinct(self):
        """HAVING/subquery: the rule descends into the nested (HAVING/subquery)
        plan, so the hash token appears inside the nested subplan."""
        body = (Q_GROUPED_CITY.format(self.hotel).rstrip(";")
                + " HAVING COUNT(DISTINCT x.name) > 1;")
        self._assert_hash_token(body, flag=True, expected_present=True)
        self._assert_parity(body)

    def test_unnest_distinct(self):
        """UNNEST: COUNT(DISTINCT) over an UNNESTed array field (public_likes)."""
        body = ("SELECT VALUE COUNT(DISTINCT pl) FROM {0} x UNNEST "
                "x.public_likes AS pl;".format(self.hotel))
        self._assert_hash_token(body, flag=True, expected_present=True)
        self._assert_parity(body)

    # ============================================================== concurrency/stress
    def test_parallel_hash_distinct_under_memory_pressure(self):
        """Parallel-hash-distinct-under-memory-pressure: N concurrent hash
        COUNT(DISTINCT) queries (in-memory budget) all succeed with the exact
        count - no cross-query corruption of the shared hash machinery."""
        n = self.input.param("herd_size", 10)
        # default (in-memory) budget - this case is about cross-query
        # interference in the shared hash machinery, not about spilling.
        stmt = self._opts(True) + " " + Q_SCALAR.format(self.hotel)
        out = self._run_concurrently(stmt, n, self.CONCURRENT_TIMEOUT)
        bad = [o for o in out if o[0] != "success"]
        if bad:
            self.fail("{0}/{1} concurrent hash-distinct queries failed; first "
                      "few: {2}".format(len(bad), n, bad[:3]))
        wrong = [o[1] for o in out if o[1] != self.no_of_docs]
        if wrong:
            self.fail("concurrent queries returned wrong distinct counts "
                      "(expected {0}): {1}".format(self.no_of_docs, wrong[:5]))

    def test_spill_disk_exhaustion(self):
        """Spill-disk-exhaustion: many SIMULTANEOUS spilling queries multiply
        the demand on the shared spill area, which is where disk exhaustion
        actually happens - a single spilling query cannot exhaust it.

        Distinct from test_tiny_budget_forces_spill (one query, exact count):
        here `spill_herd_size` queries each spill at the tiny budget at the same
        time. Every one must end in a definite state - the exact count, or a
        CLEAN server error - and never a wrong count or an unhandled exception.
        A wrong count under spill-area contention is the corruption this case
        exists to catch.

        KNOWN ISSUE MB-72573: on EA 2.3.0 b1126 every query hits the 25000 spill
        NPE, so this fails until the spill path is fixed.
        """
        n = self.input.param("spill_herd_size", 5)
        stmt = self._opts(True, self.spill_memory) + " " + Q_SCALAR.format(
            self.hotel)
        out = self._run_concurrently(stmt, n, self.SPILL_QUERY_TIMEOUT)

        crashed = [o for o in out if o[0] is None]
        if crashed:
            self.fail("{0}/{1} concurrent spilling queries raised an unhandled "
                      "exception instead of returning a clean error: {2}".format(
                          len(crashed), n, crashed[:3]))
        wrong = [o[1] for o in out
                 if o[0] == "success" and o[1] != self.no_of_docs]
        if wrong:
            self.fail("concurrent spilling queries returned WRONG counts under "
                      "spill-area contention (expected {0}): {1}".format(
                          self.no_of_docs, wrong[:5]))
        failed = [o for o in out if o[0] != "success"]
        if failed:
            self.fail(self._spill_defect_msg(
                "{0}/{1} concurrent spilling queries failed; first few: "
                "{2}".format(len(failed), n, failed[:3])))
        self.log.info("all {0} concurrent spilling queries returned the exact "
                      "count {1}".format(n, self.no_of_docs))

    def test_heap_pressure_from_large_budget(self):
        """Heap-pressure-from-large-budget/concurrency: an over-large budget
        (reserves budget/32KB frames per instance) under concurrency must be
        handled cleanly - correct results or a clean error, not an unhandled
        OOM. A large budget alone (staying in memory) must still be exact."""
        n = self.input.param("heap_herd_size", 5)
        stmt = self._opts(True, self.large_memory) + " " + Q_SCALAR.format(
            self.hotel)
        out = self._run_concurrently(stmt, n, self.SPILL_QUERY_TIMEOUT)
        # A clean out-of-budget/rejected error is acceptable; a wrong count or
        # an unhandled exception is not.
        for status, count, errors in out:
            if status == "success":
                if count != self.no_of_docs:
                    self.fail("large-budget query returned wrong count {0} "
                              "(expected {1})".format(count, self.no_of_docs))
            elif status is None:
                self.fail("large-budget concurrency raised an unhandled "
                          "exception: {0}".format(errors))
            else:
                self.log.info("large-budget query returned a clean error "
                              "(acceptable): {0}".format(errors))

    def test_cancel_timeout_during_spill(self):
        """Cancel/timeout-during-spill: a spilling query must reach a TERMINAL
        state inside a bounded client budget - it must not hang, and it must not
        leave the service unable to serve the next request.

        Three things are asserted, all unconditionally:
          1. the request returns a definite status within CANCEL_BUDGET;
          2. that status is either success with the exact count, or a clean
             server error - never a wrong count;
          3. the service still answers a trivial query afterwards, so a
             cancelled/aborted spill did not wedge the query pipeline.

        KNOWN ISSUE MB-72573: on EA 2.3.0 b1126 the spill path errors (25000)
        quickly, which satisfies (1) and (3); (2) records it as a clean error.
        """
        stmt = self._opts(True, self.spill_memory) + " " + Q_SCALAR.format(
            self.hotel)
        try:
            status, _, errors, results, _, _ = self._run(
                stmt, timeout=self.CANCEL_BUDGET)
        except Exception as e:                           # noqa: BLE001
            self.fail("spilling query did not reach a terminal state within "
                      "the {0}s client budget - it hung or the transport blew "
                      "up: {1}".format(self.CANCEL_BUDGET, e))
        if status is None:
            self.fail("spilling query returned no status within the {0}s "
                      "budget (treated as a hang)".format(self.CANCEL_BUDGET))
        if status == "success":
            if not results or results[0] != self.no_of_docs:
                self.fail("spilling query completed inside the budget but "
                          "returned {0}, expected the exact count {1}".format(
                              results, self.no_of_docs))
        else:
            self.log.info("spilling query returned a clean terminal error "
                          "(no hang): status={0} errors={1}".format(
                              status, errors))
        # The service must still be usable - a wedged pipeline would make every
        # later test fail for reasons unrelated to it.
        probe = self._results(Q_COUNT_ALL.format(self.hotel))
        if not probe or probe[0] != self.no_of_docs:
            self.fail("the analytics service did not serve a trivial COUNT(*) "
                      "after the terminated spilling query (got {0}, expected "
                      "{1}) - the query pipeline is wedged".format(
                          probe, self.no_of_docs))

    # ============================================================ durability / restart
    def _restart_enterprise_analytics(self):
        """Restart the `enterprise-analytics` systemd service on every analytics
        node, then block until cbas can serve queries again."""
        nodes = getattr(self.columnar_cluster, "cbas_nodes", None) \
            or [self.columnar_cluster.master]
        for node in nodes:
            shell = RemoteMachineShellConnection(node)
            try:
                shell.stop_enterprise_analytics()
                shell.start_enterprise_analytics()
            finally:
                shell.disconnect()
        if not self.cbas_util.wait_for_cbas_to_recover(
                self.columnar_cluster, timeout=self.RESTART_RECOVER_TIMEOUT):
            self.fail("cbas did not recover after restarting enterprise-analytics"
                      " on {0} node(s)".format(len(nodes)))

    def test_survives_service_restart(self):
        """Survives-service-restart: an `enterprise-analytics` restart must not
        change feature behaviour. Snapshot the baseline (data count, flag routing
        OFF->sort / ON->hash, scalar + grouped COUNT(DISTINCT) exactness), restart
        the service on every analytics node, wait for recovery, then assert every
        invariant is identical - the fixed ones (routing, scalar count) hold both
        times and the data-dependent grouped result set is byte-for-byte equal
        pre vs post. Automates the manual restart-durability run."""
        scalar = Q_SCALAR.format(self.hotel)
        grouped = Q_GROUPED_CITY.format(self.hotel)
        count_all = Q_COUNT_ALL.format(self.hotel)

        def snapshot(when):
            # data present (collection survived the restart)
            docs = self._results(count_all)
            if docs[0] != self.no_of_docs:
                self.fail("{0}-restart: expected {1} docs, got {2}".format(
                    when, self.no_of_docs, docs[0]))
            # flag routing: OFF -> sort (no token), ON -> hash (token)
            self._assert_hash_token(scalar, flag=False, expected_present=False)
            self._assert_hash_token(scalar, flag=True, expected_present=True)
            # scalar exactness on both paths == unique-name count
            _, s_on = self._assert_parity(scalar)
            if s_on[0] != self.no_of_docs:
                self.fail("{0}-restart: scalar COUNT(DISTINCT) wrong, expected "
                          "{1}, got {2}".format(when, self.no_of_docs, s_on[0]))
            # grouped exactness (data-dependent) captured for the pre/post compare
            _, g_on = self._assert_parity(grouped)
            return self._norm(g_on)

        before = snapshot("pre")
        self._restart_enterprise_analytics()
        after = snapshot("post")
        if before != after:
            self.fail("grouped COUNT(DISTINCT) changed across the restart:\n "
                      "pre={0}\n post={1}".format(before[:20], after[:20]))

    # ================================================================= regression
    def test_non_distinct_counts_unaffected(self):
        """Non-distinct-counts-unaffected: plain COUNT(*) / COUNT(field) are
        never touched by the flag (still plain agg-sql-count, same results)."""
        body = ("SELECT COUNT(*) AS c1, COUNT(x.name) AS c2 FROM {0} x;".format(
            self.hotel))
        plan = self._explain_json(body, flag=True)
        if self.HASH_TOKEN in plan:
            self.fail("plain COUNT was rewritten to the hash-distinct operator; "
                      "plan={0}".format(plan[:2000]))
        self._assert_parity(body)

    def test_existing_count_distinct_under_flag(self):
        """Existing-COUNT(DISTINCT)-under-flag: a pre-existing COUNT(DISTINCT)
        workload returns the same results with the flag ON as OFF (drop-in)."""
        body = ("SELECT x.country, COUNT(DISTINCT x.name) AS d FROM {0} x "
                "GROUP BY x.country;".format(self.hotel))
        self._assert_parity(body)
