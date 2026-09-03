"""

@author: vaibhav.somasundaram@couchbase.com

Query Plan Cache - Enterprise Analytics (on-prem / columnar) functional tests.

Feature: the cluster-shared, LRU-bounded cache of optimized plans for read (SELECT)
queries. Each query response carries a top-level "cachedPlan" boolean (true = hit,
false = miss). Tests drive the EA query service via
POST /api/v1/request directly (see _analytics_request) and assert on that
field - that framework wrapper returns the FULL envelope, unlike
CbasUtil.execute_statement_on_cbas_util which unpacks it and drops cachedPlan. The
plan-cache endpoints are likewise reached through the framework wrappers
(`clear_plan_cache`, `update_service_config`, `restart_analytics_service`); the request
helpers and the result comparator come from ColumnarOnPremBase. Topology changes are
driven directly through CBASRebalanceUtil here, and the cluster is restored by the conf
(nodes_init + no skip_setup_cleanup) plus tearDown's _restore_topology_if_shrunk.
Only the plan-cache-specific assertions live here.

Every plan-reuse case uses the collection workload query Q (travel-sample airport JOIN
route) - a non-trivial plan that exercises the optimizer and inter-node data exchange,
unlike a trivial SELECT 1. Distinct-plan cases use q_dist() variants. The
parameterized-value case (Q_PARAM) asserts on result values, so it queries the populated
airport collection alone (not the airport JOIN route workload, whose route side is empty
in some sample loads). `type` is a reserved word -> backticked.
"""
import time
from concurrent.futures import ThreadPoolExecutor

from Columnar.onprem.columnar_onprem_base import ColumnarOnPremBase
from cb_server_rest_util.analytics.analytics_api import AnalyticsRestAPI
from TestInput import TestInputSingleton
from cbas_utils.cbas_utils_columnar import CbasUtil as ColumnarCbasUtil
from cbas_utils.cbas_utils_columnar import RBAC_Util as ColumnarRBACUtil
from cbas_utils.cbas_utils_on_prem import CBASRebalanceUtil

# --- Canonical workload query Q (travel-sample) and variants ---
Q = ("SELECT a.city, COUNT(*) AS route_cnt, ROUND(AVG(r.distance),2) AS avg_dist "
     "FROM `travel-sample`.inventory.airport a, `travel-sample`.inventory.route r "
     "WHERE a.`type`='airport' AND r.`type`='route' AND r.sourceairport=a.faa "
     "AND a.country='United States' AND r.distance>500 "
     "GROUP BY a.city HAVING COUNT(*)>10 ORDER BY route_cnt DESC LIMIT 10;")
# Q' - same query, extra whitespace (different text, same plan).
Q_PRIME = Q.replace("SELECT a.city", "SELECT  a.city", 1)
# Q with a changed literal (different plan constant).
Q_LITERAL = Q.replace("a.country='United States'", "a.country='Canada'")
# Q($1) - parameterized-value case. Unlike Q (which JOINs inventory.route), this
# test asserts on result *values*, so it must hit populated data: some sample loads
# leave inventory.route empty, which would make every arg yield [] and mask the
# check. Parameterize over inventory.airport instead - US airports above altitude
# $1 give different, non-empty counts per arg (500 -> 767 rows, 1000 -> 414).
Q_PARAM = ("SELECT VALUE COUNT(*) "
           "FROM `travel-sample`.inventory.airport a "
           "WHERE a.country='United States' AND a.geo.alt > $1;")


def q_dist(n):
    """A distinct variant of Q: change the distance threshold -> distinct text/key."""
    return Q.replace("r.distance>500", "r.distance>%d" % n)


# --- Query matrix: distinct plan shapes over travel-sample (deterministic) ---
Q_AGG = ("SELECT r.airline, COUNT(*) AS cnt, ROUND(AVG(r.distance),2) AS avg_dist "
         "FROM `travel-sample`.inventory.route r WHERE r.`type`='route' AND r.distance>1000 "
         "GROUP BY r.airline HAVING COUNT(*)>50 ORDER BY cnt DESC LIMIT 10;")

Q_WINDOW = ("SELECT a.city, a.airportname, a.geo.alt AS alt, "
            "ROW_NUMBER() OVER (PARTITION BY a.country "
            "ORDER BY a.geo.alt DESC, a.airportname, a.faa) AS rnk "
            "FROM `travel-sample`.inventory.airport a "
            "WHERE a.`type`='airport' AND a.country='United States' "
            "ORDER BY a.airportname, a.city, a.faa LIMIT 20;")
Q_SUBQ = ("SELECT a.airportname, a.city FROM `travel-sample`.inventory.airport a "
          "WHERE a.`type`='airport' AND a.country='France' AND EXISTS "
          "(SELECT 1 FROM `travel-sample`.inventory.route r "
          "WHERE r.sourceairport=a.faa AND r.distance>2000) "
          "ORDER BY a.airportname, a.city, a.faa LIMIT 10;")
# Q_idx exercises an index-scan when a secondary index on airport(faa) exists.
Q_IDX = ("SELECT a.airportname, a.city FROM `travel-sample`.inventory.airport a "
         "WHERE a.`type`='airport' AND a.faa='SFO';")
# (alias, query) - reuse-correctness cases run once per entry.
MATRIX = [("Q", Q), ("Q_agg", Q_AGG), ("Q_window", Q_WINDOW),
          ("Q_subq", Q_SUBQ), ("Q_idx", Q_IDX)]


class QueryPlanCache(ColumnarOnPremBase):

    DEFAULT_CAPACITY = 1000
    USER_PWD = "password"
    AUTH_CACHE_WAIT = 6                       # analytics auth cache TTL (~5s)
    SAMPLE_INSTALL_TIMEOUT = 300              # POST /api/v1/samples HTTP timeout
    SAMPLE_LOAD_TIMEOUT = 600                 # wait for the sample to be queryable
    CBAS_RECOVER_TIMEOUT = 180                # service restart -> ready
    AIRPORT = "`travel-sample`.inventory.airport"
    ROUTE = "`travel-sample`.inventory.route"

    @property
    def plan_cache_api(self):
        """Full URL of the plan-cache admin endpoint on the CURRENT API target.

        Derived rather than stored so it follows self.analytics_api rather than
        caching a host that a topology change could invalidate.
        """
        return self.analytics_api.cbas_url + "/api/v1/plan_cache"

    def _base_setup(self):
        """Run the inherited setUp, optionally without the analytics wipe.

        CBASBaseTest.setUp calls CbasUtil.cleanup_cbas() unconditionally. That
        drops the `travel-sample` database, so _ensure_travel_sample has to
        re-install the sample in every test (~180s each). This suite creates no
        metadata outside self._cleanup_ddl, which tearDown drops, so the wipe
        buys nothing here.

        skip_cbas_cleanup (default True, and set on every line of
        query_plan_cache_core.conf) suppresses it for the duration of the base
        setUp only; the original method is always restored.
        """
        if not TestInputSingleton.input.param("skip_cbas_cleanup", True):
            super(QueryPlanCache, self).setUp()
            return
        original = ColumnarCbasUtil.cleanup_cbas
        ColumnarCbasUtil.cleanup_cbas = lambda *args, **kwargs: True
        try:
            super(QueryPlanCache, self).setUp()
        finally:
            ColumnarCbasUtil.cleanup_cbas = original

    def setUp(self):
        self._base_setup()
        self.analytics_api = AnalyticsRestAPI(self.columnar_cluster.master)
        self._capacity_modified = False
        self._cleanup_ddl = []
        self.rbac_util = ColumnarRBACUtil(self.task, self.use_sdk_for_cbas)
        self._users_created = set()
        # topology helper (rebalance/failover) for the topology-invalidation cases
        self.rebalance_util = CBASRebalanceUtil(
            self.cluster_util, self.bucket_util, self.task, False, self.cbas_util)
        if not hasattr(self.columnar_cluster, "available_servers"):
            self.columnar_cluster.available_servers = []
        self._ensure_travel_sample()
        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.setUp.__name__)

    def tearDown(self):
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)
        for stmt in reversed(getattr(self, "_cleanup_ddl", [])):
            try:
                self._analytics_request(stmt)
            except Exception as e:
                self.log.warning("cleanup DDL failed ({0}): {1}".format(stmt, e))
        for username in sorted(getattr(self, "_users_created", set())):
            try:
                self.rbac_util.delete_user(self.columnar_cluster, username)
            except Exception as e:
                self.log.warning("cleanup of user {0} failed: {1}"
                                 .format(username, e))
        if getattr(self, "_capacity_modified", False):
            self.log.info("Restoring queryPlanCacheCapacity to default {0}"
                          .format(self.DEFAULT_CAPACITY))
            self._set_capacity(self.DEFAULT_CAPACITY)
        self._restore_topology_if_shrunk()
        super(QueryPlanCache, self).tearDown()
        self.log_setup_status(self.__class__.__name__,
                              "Finished", stage="Teardown")

    def _cbas_node_count(self):
        """cbas nodes the cluster currently reports (MEMBERSHIP, not liveness -
        a node listed here is a cbas node, not proof cbas answers on it)."""
        return len(self.cluster_util.get_nodes_from_services_map(
            self.columnar_cluster, service_type="cbas", get_all_nodes=True,
            servers=self.columnar_cluster.nodes_in_cluster))

    def _restore_topology_if_shrunk(self):
        """Rebalance a cbas node back in if a topology test left the cluster
        short of the count it started with.

        Only the tests that change topology set `_expected_cbas_nodes`, so this
        is a no-op for every other test. NEVER raises: a teardown that throws
        would mask the test's own verdict, so a restore that cannot succeed is
        logged at ERROR naming the manual repair instead.

        The conf's nodes_init (with no skip_setup_cleanup) also rebuilds the
        cluster in the NEXT test's setUp, but that does nothing for the last
        test of a run - hence this.
        """
        expected = getattr(self, "_expected_cbas_nodes", None)
        if expected is None:
            return
        try:
            actual = self._cbas_node_count()
            if actual >= expected:
                return
            self.log.warning(
                "cluster has %s cbas node(s), started with %s - rebalancing "
                "one back in" % (actual, expected))
            # Analytics-only add-back: the node returns with the cbas service
            # alone. If the cluster rejects a cbas-only rebalance-in, this is
            # the line to revisit.
            task, self.columnar_cluster.available_servers = self.rebalance_util.rebalance(
                cluster=self.columnar_cluster, cbas_nodes_in=1,
                in_node_services="cbas",
                available_servers=self.columnar_cluster.available_servers)
            if not self.rebalance_util.wait_for_rebalance_task_to_complete(
                    task, self.columnar_cluster, True, True):
                self.log.error(
                    "COULD NOT restore the cbas node - the cluster is left "
                    "DEGRADED and needs manual repair (rejoin_nodes.sh, or "
                    "restart enterprise-analytics then rebalance)")
        except Exception as e:                           # noqa: BLE001
            self.log.error("topology restore raised: %s" % e)

    # ------------------------------------------------------------------ helpers
    def _ensure_travel_sample(self):
        """Ensure travel-sample (Q's data) is loaded; install it if absent so the
        suite needs no manual pre-load.
        """
        airport = "SELECT VALUE COUNT(*) FROM %s;" % self.AIRPORT
        _, res, st = self._run_with_cache_flag(airport)
        if st == "success" and res and res[0] and res[0] >= 1:
            return
        self.log.info("travel-sample not present - installing via the "
                      "Enterprise Analytics samples API")
        try:
            status, content, _ = self.analytics_api.load_analytics_sample(
                "travel-sample", timeout=self.SAMPLE_INSTALL_TIMEOUT)
            if not status:
                self.log.warning("POST /api/v1/samples returned a failure "
                                 "(%s) - waiting on the data anyway" % content)
        except Exception as e:
            self.log.warning("POST /api/v1/samples raised %s - waiting on the "
                             "data anyway" % e)
        self.assertTrue(
            self.cbas_util.wait_for_sample_to_be_queryable(
                self.columnar_cluster, airport,
                timeout=self.SAMPLE_LOAD_TIMEOUT),
            "travel-sample did not become queryable within %ss of "
            "POST /api/v1/samples" % self.SAMPLE_LOAD_TIMEOUT)

    def _clear_cache(self, username=None, password=None):
        """DELETE /api/v1/plan_cache via the framework wrapper; returns
        (status, content, response). Pass username/password to exercise the
        non-admin-rejected path."""
        return self.analytics_api.clear_plan_cache(username, password)

    def _set_capacity(self, capacity):
        """PUT queryPlanCacheCapacity, restart the service, wait for recovery."""
        status, content, _ = self.analytics_api.update_service_config(
            {"queryPlanCacheCapacity": capacity})
        self.assertTrue(status, "Failed to PUT queryPlanCacheCapacity=%s: %s" % (capacity, content))
        status, content, _ = self.analytics_api.restart_analytics_service()
        self.assertTrue(status, "Failed to restart analytics service: %s" % content)
        self.assertTrue(
            self.cbas_util.wait_for_cbas_to_recover(
                self.columnar_cluster, self.CBAS_RECOVER_TIMEOUT),
            "Analytics service did not recover after capacity=%s" % capacity)

    def _set_capacity_no_restart(self, capacity):
        status, content, _ = self.analytics_api.update_service_config(
            {"queryPlanCacheCapacity": capacity})
        self.assertTrue(status, "Failed to PUT queryPlanCacheCapacity=%s: %s" % (capacity, content))

    def _ddl(self, statement, ignore_errors=False):
        content = self._analytics_request(statement)
        if not ignore_errors:
            self.assertEqual(content.get("status"), "success",
                             "statement failed: %s -> %s"
                             % (statement, content.get("errors") or content))
        return content

    def _create_collection(self, name):
        self._ddl("CREATE COLLECTION `%s` IF NOT EXISTS PRIMARY KEY(`id` : UUID) "
                  "AUTOGENERATED;" % name)
        self._cleanup_ddl.append("DROP COLLECTION IF EXISTS `%s`;" % name)

    def _warm(self, statement, **kw):
        """Run a query twice; assert the 2nd is a HIT (cache primed)."""
        self._run_with_cache_flag(statement, **kw)
        cp, _, _ = self._run_with_cache_flag(statement, **kw)
        self.assertTrue(cp, "expected the query to be cached (hit) before invalidation")

    def _assert_miss(self, statement, **kw):
        """Assert a cacheable SELECT reports an explicit cache MISS.

        assertIs(False) rather than assertFalse: _cached_plan returns None when
        the server omits `cachedPlan` altogether, and a missing field must fail
        loudly instead of passing as a miss.
        """
        cp, _, _ = self._run_with_cache_flag(statement, **kw)
        self.assertIs(cp, False,
                      "expected an explicit MISS (cache invalidated); "
                      "cachedPlan=%s" % cp)

    def _ensure_user(self, username, role):
        if username not in self._users_created:
            ok = self.rbac_util.create_user(self.columnar_cluster, username, username,
                                            self.USER_PWD, roles=role)
            self.assertTrue(ok, "failed to create user %s (role=%s)" % (username, role))
            self._users_created.add(username)
            time.sleep(self.AUTH_CACHE_WAIT)

    def _set_user_role(self, username, role):
        """REPLACE an existing user's role set (a real privilege revocation,
        not an append), via the RBAC util, then wait out the analytics auth
        cache TTL so the change takes effect on the next request.
        """
        self.assertTrue(
            self.rbac_util.set_user_roles(self.columnar_cluster, username,
                                          self.USER_PWD, role),
            "failed to set role %s on %s" % (role, username))
        time.sleep(self.AUTH_CACHE_WAIT)

    def _ensure_faa_index(self):
        """Create a secondary index on airport(faa) so Q_idx exercises an index-scan."""
        self._cleanup_ddl.append("DROP INDEX %s.pc_faa_idx;" % self.AIRPORT)
        self._ddl("CREATE INDEX pc_faa_idx IF NOT EXISTS ON %s(faa: string);" % self.AIRPORT,
                  ignore_errors=True)

    # ============================ hit/miss & controls ============================
    def test_hit_miss(self):
        """First run misses, identical repeat hits (also proves cache is ON by default)."""
        self._clear_cache()
        cp1, res1, st1 = self._run_with_cache_flag(Q)
        cp2, res2, st2 = self._run_with_cache_flag(Q)
        self.assertEqual(st1, "success", "run1 not successful: %s" % st1)
        self.assertEqual(st2, "success", "run2 not successful: %s" % st2)
        self.assertIs(cp1, False, "run1 must be a MISS, got %s" % cp1)
        self.assertTrue(cp2, "run2 must be a HIT (no opt-in -> default on), got %s" % cp2)
        self.assertEqual(res1, res2, "results differ between miss and hit")

    def test_query_text_difference(self):
        """Whitespace/literal differences in the query text cause a miss."""
        self._clear_cache()
        cp1, _, _ = self._run_with_cache_flag(Q)          # miss (store)
        cp2, _, _ = self._run_with_cache_flag(Q)          # hit
        cp3, _, _ = self._run_with_cache_flag(Q_PRIME)    # extra whitespace -> new key
        cp4, _, _ = self._run_with_cache_flag(Q_LITERAL)  # changed literal -> new key
        self.assertEqual([cp1, cp2, cp3, cp4], [False, True, False, False],
                         "expected [miss, hit, miss, miss], got %s" % [cp1, cp2, cp3, cp4])

    def test_query_context_difference(self):
        """A different default namespace (query_context) causes a miss."""
        self._clear_cache()
        c1, _, _ = self._run_with_cache_flag(Q)
        c2, _, _ = self._run_with_cache_flag(Q)
        c3, _, _ = self._run_with_cache_flag(Q, query_context="default:Metadata")
        self.assertEqual([c1, c2, c3], [False, True, False],
                         "expected [miss, hit, miss], got %s" % [c1, c2, c3])

    def test_plan_affecting_setting_difference(self):
        """A plan-affecting compiler setting (compiler.cbo) re-keys the plan.

        The setting must travel as a SET clause on the statement: the dotted
        JSON request param 'compiler.cbo' is silently ignored by /api/v1/request
        (garbage values still return 200), so it never reaches the planner.
        """
        self._clear_cache()
        cbo_true = 'SET `compiler.cbo` "true";  ' + Q
        cbo_false = 'SET `compiler.cbo` "false"; ' + Q
        c1, _, _ = self._run_with_cache_flag(cbo_true)
        c2, _, _ = self._run_with_cache_flag(cbo_true)
        c3, _, _ = self._run_with_cache_flag(cbo_false)
        self.assertEqual([c1, c2, c3], [False, True, False],
                         "expected [miss, hit, miss], got %s" % [c1, c2, c3])

    def test_parameterized_values(self):
        """Parameterized keying: same values hit, different values miss, hit reflects its own args."""
        self._clear_cache()
        c1, r1, _ = self._run_with_cache_flag(Q_PARAM, args=[500])
        c2, r2, _ = self._run_with_cache_flag(Q_PARAM, args=[1000])
        c3, r3, _ = self._run_with_cache_flag(Q_PARAM, args=[500])
        self.assertEqual([c1, c2, c3], [False, False, True],
                         "expected cachedPlan [miss, miss, hit], got %s" % [c1, c2, c3])
        self.assertEqual(r1, r3, "args=[500] hit must return the value-500 result")
        self.assertNotEqual(r1, r2, "different args must yield different results (500 vs 1000)")

    def test_skip_plan_cache(self):
        """skip-plan-cache=true neither stores nor looks up a plan."""
        self._clear_cache()
        s1, _, _ = self._run_with_cache_flag(Q, skip_plan_cache=True)
        s2, _, _ = self._run_with_cache_flag(Q, skip_plan_cache=True)
        s3, _, _ = self._run_with_cache_flag(Q)     # normal: still nothing stored
        s4, _, _ = self._run_with_cache_flag(Q)     # normal: now a hit
        self.assertEqual([s1, s2, s3, s4], [False, False, False, True],
                         "expected [skipF, skipF, normalF, normalT], got %s (if s2 is True "
                         "the wire param name is not 'skip-plan-cache')" % [s1, s2, s3, s4])

    def test_skip_does_not_evict(self):
        """A skip request does not evict or serve an existing entry."""
        self._clear_cache()
        self._run_with_cache_flag(Q)
        warm, _, _ = self._run_with_cache_flag(Q)                       # hit
        skip, _, _ = self._run_with_cache_flag(Q, skip_plan_cache=True)
        after, _, _ = self._run_with_cache_flag(Q)
        self.assertTrue(warm, "warm-up run should be a hit")
        self.assertIs(skip, False, "skip run must be a forced miss")
        self.assertTrue(after, "existing entry must survive a skip request")

    def test_set_disable_plan_cache(self):
        """SET compiler.query.plan.cache=false disables caching for that request.

        Carried as a SET clause on the statement (the dotted JSON request param
        is silently ignored). A disabled request neither stores nor looks up a
        plan; once run without the SET, the default-on path caches as normal.
        """
        self._clear_cache()
        disabled = 'SET `compiler.query.plan.cache` "false"; ' + Q
        d1, _, _ = self._run_with_cache_flag(disabled)
        d2, _, _ = self._run_with_cache_flag(disabled)
        n1, _, _ = self._run_with_cache_flag(Q)
        n2, _, _ = self._run_with_cache_flag(Q)
        self.assertEqual([d1, d2, n1, n2], [False, False, False, True],
                         "disabled runs must never hit; default-on must then hit. Got %s"
                         % [d1, d2, n1, n2])

    def test_non_plan_affecting_params(self):
        """pretty / client_context_id must NOT change the cache key (a miss = finding)."""
        self._clear_cache()
        c1, _, _ = self._run_with_cache_flag(Q)
        c2, _, _ = self._run_with_cache_flag(Q, pretty=True)
        c3, _, _ = self._run_with_cache_flag(Q, client_context_id="qpc-ctx")
        self.assertEqual([c1, c2, c3], [False, True, True],
                         "expected [miss, hit, hit]; a miss means the key wrongly includes a "
                         "non-plan-affecting param. Got %s" % [c1, c2, c3])

    # ============================ admin clear API ============================
    def test_admin_clear_cache(self):
        """Admin DELETE /api/v1/plan_cache clears all and reports a count."""
        self._run_with_cache_flag(Q)
        self._run_with_cache_flag(Q)
        ok, content, response = self._clear_cache()
        self.assertTrue(ok and response.status_code == 200, "DELETE plan_cache failed: %s" % content)
        msg = content.get("status", "") if isinstance(content, dict) else str(content)
        self.assertIn("cleared", msg.lower(), "clear-all did not report a cleared count: %s" % content)
        cp, _, _ = self._run_with_cache_flag(Q)
        self.assertIs(cp, False, "after clear the next run must be a MISS, got %s" % cp)

    def test_non_admin_clear_rejected(self):
        """A non-admin DELETE /api/v1/plan_cache is rejected (403)."""
        self._ensure_user("pc_reader", "analytics_access")
        ok, content, response = self._clear_cache(username="pc_reader", password=self.USER_PWD)
        self.assertEqual(response.status_code, 403,
                         "non-admin clear should be 403, got %s (%s)" % (response.status_code, content))

    def test_non_delete_verb_405(self):
        """Non-DELETE verbs on /api/v1/plan_cache return 405."""
        headers = self.analytics_api.get_headers_for_content_type_json()
        for verb in ("GET", "POST", "PUT"):
            _, content, response = self.analytics_api.request(self.plan_cache_api, verb, headers=headers)
            self.assertEqual(response.status_code, 405,
                             "%s on plan_cache should be 405, got %s (%s)"
                             % (verb, response.status_code, content))

    # ============================ capacity ============================
    def test_negative_capacity_rejected(self):
        """A negative capacity is rejected."""
        status, content, response = self.analytics_api.update_service_config(
            {"queryPlanCacheCapacity": -1})
        rejected = (not status) or (response is not None and response.status_code == 400)
        self.assertTrue(rejected, "negative capacity should be rejected, got status=%s content=%s"
                        % (status, content))

    def test_capacity_zero_disables(self):
        """queryPlanCacheCapacity=0 disables caching (always miss)."""
        self._capacity_modified = True
        self._set_capacity(0)
        self._clear_cache()
        cp1, _, _ = self._run_with_cache_flag(Q)
        cp2, _, _ = self._run_with_cache_flag(Q)
        self.assertEqual([cp1, cp2], [False, False],
                         "capacity=0 must disable caching (both misses), got %s" % [cp1, cp2])

    def test_capacity_lru_eviction(self):
        """Least-recently-used plan is evicted when capacity is exceeded."""
        self._capacity_modified = True
        self._set_capacity(2)
        self._clear_cache()
        q1, q2, q3 = q_dist(501), q_dist(502), q_dist(503)
        self.assertIs(self._run_with_cache_flag(q1)[0], False, "q1 store must MISS")
        self.assertIs(self._run_with_cache_flag(q2)[0], False, "q2 store must MISS")
        self.assertIs(self._run_with_cache_flag(q3)[0], False,
                      "q3 store must MISS (and evict LRU q1)")
        self.assertIs(self._run_with_cache_flag(q1)[0], False, "q1 should have been evicted as LRU")
        self.assertTrue(self._run_with_cache_flag(q3)[0], "q3 should still be cached")

    def test_lru_recency_on_hit(self):
        """A hit refreshes recency, so the OTHER entry is evicted first."""
        self._capacity_modified = True
        self._set_capacity(2)
        self._clear_cache()
        q1, q2, q3 = q_dist(501), q_dist(502), q_dist(503)
        self.assertIs(self._run_with_cache_flag(q1)[0], False, "q1 store must MISS")
        self.assertIs(self._run_with_cache_flag(q2)[0], False, "q2 store must MISS")
        self.assertTrue(self._run_with_cache_flag(q1)[0],
                        "q1 must HIT, refreshing its recency so q2 becomes LRU")
        self.assertIs(self._run_with_cache_flag(q3)[0], False,
                      "q3 store must MISS (and evict q2)")
        # Check the SURVIVOR first: at capacity 2, probing q2 (a miss) would itself
        # store q2 and evict q1, so q1 must be verified before q2 is touched.
        self.assertTrue(self._run_with_cache_flag(q1)[0], "q1 should survive (recently used)")
        self.assertIs(self._run_with_cache_flag(q2)[0], False,
                      "q2 should be evicted (became LRU after the q1 hit)")

    def test_live_resize_evicts(self):
        """Reducing capacity evicts entries live, without a restart."""
        self._capacity_modified = True
        self._set_capacity_no_restart(1000)
        self._clear_cache()
        queries = [q_dist(501 + i) for i in range(5)]
        for q in queries:
            self._run_with_cache_flag(q)
        self.assertTrue(self._run_with_cache_flag(queries[0])[0], "an entry should be cached before the resize")
        self._set_capacity_no_restart(2)
        hits = sum(1 for q in queries if self._run_with_cache_flag(q)[0])
        self.assertLessEqual(hits, 2, "after live resize to 2, at most 2 of 5 may hit; got %d" % hits)

    # ============================ non-cacheable ============================
    def test_ddl_never_cached(self):
        """DDL (CREATE/DROP) is never cached."""
        self._clear_cache()
        stmt = "CREATE COLLECTION `pc_ddl` IF NOT EXISTS PRIMARY KEY(`id` : UUID) AUTOGENERATED;"
        c1 = self._analytics_request(stmt)
        c2 = self._analytics_request(stmt)
        self._cleanup_ddl.append("DROP COLLECTION IF EXISTS `pc_ddl`;")
        # EA may omit `cachedPlan` entirely on a DDL response; both "absent"
        # and "false" are correct here, "true" is the failure.
        for run, content in (("run1", c1), ("run2", c2)):
            self.assertIn(self._cached_plan(content), (False, None),
                          "DDL must not be a cache hit (%s cachedPlan=%s)"
                          % (run, self._cached_plan(content)))

    def test_dml_never_cached(self):
        """DML (INSERT/UPSERT/DELETE) is never cached."""
        self._create_collection("pc_dml")
        self._clear_cache()
        c1 = self._analytics_request('INSERT INTO `pc_dml` ([{"x": 1}]);')
        c2 = self._analytics_request('INSERT INTO `pc_dml` ([{"x": 1}]);')
        # As with DDL, an absent `cachedPlan` is an acceptable "not cached".
        for run, content in (("run1", c1), ("run2", c2)):
            self.assertIn(self._cached_plan(content), (False, None),
                          "DML must not be a cache hit (%s cachedPlan=%s)"
                          % (run, self._cached_plan(content)))

    # ============================ automatic invalidation ============================
    def test_create_index_invalidates(self):
        """CREATE INDEX clears the whole cache."""
        self._clear_cache()
        self._warm(Q)
        self._cleanup_ddl.append("DROP INDEX %s.pc_ci_idx;" % self.ROUTE)
        self._ddl("CREATE INDEX pc_ci_idx ON %s(sourceairport: string);" % self.ROUTE)
        self._assert_miss(Q)

    def test_drop_collection_invalidates(self):
        """DROP collection clears the cache (coarse - even the unrelated Q misses)."""
        self._create_collection("pc_dc")
        self._clear_cache()
        self._warm(Q)
        self._ddl("DROP COLLECTION IF EXISTS `pc_dc`;")
        self._assert_miss(Q)

    def test_drop_database_invalidates(self):
        """DROP DATABASE clears the cache."""
        self._ddl("CREATE DATABASE IF NOT EXISTS `pc_db`;")
        self._cleanup_ddl.append("DROP DATABASE IF EXISTS `pc_db`;")
        self._clear_cache()
        self._warm(Q)
        self._ddl("DROP DATABASE IF EXISTS `pc_db`;")
        self._assert_miss(Q)

    def test_drop_view_invalidates(self):
        """DROP view clears the cache (function/library/synonym are analogous)."""
        self._ddl("CREATE OR REPLACE ANALYTICS VIEW `pc_v` AS SELECT VALUE 1;")
        self._cleanup_ddl.append("DROP ANALYTICS VIEW IF EXISTS `pc_v`;")
        self._clear_cache()
        self._warm(Q)
        self._ddl("DROP ANALYTICS VIEW IF EXISTS `pc_v`;")
        self._assert_miss(Q)

    ANALYZE_OPS = ["analyze", "drop_statistics"]

    def test_analyze_reoptimizes_correctly(self):
        """ANALYZE (stats change) invalidates the cache and re-optimizes; results stay correct.

        Scenarios, selected by the `analyze_op` conf param:
          analyze         - ANALYZE ANALYTICS COLLECTION (also checks results
                            survive the re-optimize)
          drop_statistics - ANALYZE ... DROP STATISTICS
          all (default)   - both, as one sequence: ANALYZE creates the very
                            statistics DROP STATISTICS then drops, so the two
                            share a setUp with no loss of coverage. Each op is
                            still warmed and asserted on its own.
        """
        analyze_op = self.input.param("analyze_op", "all")
        if analyze_op == "all":
            ops = list(self.ANALYZE_OPS)
        elif analyze_op in self.ANALYZE_OPS:
            ops = [analyze_op]
            if analyze_op == "drop_statistics":
                # There must be statistics to drop for the drop to be the trigger.
                # In `all` mode the preceding `analyze` op has already created them.
                self._ddl("ANALYZE ANALYTICS COLLECTION %s;" % self.ROUTE)
        else:
            self.fail("unknown analyze_op=%s (expected all|%s)"
                      % (analyze_op, "|".join(self.ANALYZE_OPS)))
        self._clear_cache()
        for op in ops:
            stmt = "ANALYZE ANALYTICS COLLECTION %s%s;" % (
                self.ROUTE, " DROP STATISTICS" if op == "drop_statistics" else "")
            self._run_with_cache_flag(Q)
            _, warm_res, _ = self._run_with_cache_flag(Q)          # hit; reference results
            self._ddl(stmt)
            cp, res, _ = self._run_with_cache_flag(Q)
            self.assertIs(cp, False,
                          "%s must invalidate -> re-optimized (miss); cachedPlan=%s"
                          % (op, cp))
            self.assertEqual(res, warm_res,
                             "results must stay correct after re-optimize (%s)" % op)

    def test_global_invalidation_clears_unrelated(self):
        """Invalidation is COARSE: one event clears the ENTIRE cache, including unrelated plans.

        Warms two distinct, unrelated queries (airport and airline), then a single CREATE INDEX
        on airport must evict BOTH - proving the whole-cache clear, not just the affected plan.
        """
        q_air = "SELECT a.city FROM %s a WHERE a.country='United States' LIMIT 3;" % self.AIRPORT
        q_aln = "SELECT al.name FROM `travel-sample`.inventory.airline al LIMIT 3;"
        self._clear_cache()
        self._run_with_cache_flag(q_air)
        h_air = self._run_with_cache_flag(q_air)[0]
        self._run_with_cache_flag(q_aln)
        h_aln = self._run_with_cache_flag(q_aln)[0]
        self.assertTrue(h_air and h_aln, "both distinct queries must be cached before the event")
        self._cleanup_ddl.append("DROP INDEX %s.pc_gi_idx;" % self.AIRPORT)
        self._ddl("CREATE INDEX pc_gi_idx ON %s(country: string);" % self.AIRPORT)
        self.assertIs(self._run_with_cache_flag(q_air)[0], False, "airport plan must be evicted")
        self.assertIs(self._run_with_cache_flag(q_aln)[0], False,
                      "the UNRELATED airline plan must ALSO be evicted "
                      "(coarse whole-cache clear)")

    # ============================ CBO correctness + query matrix ============================
    def test_cbo_correctness(self):
        """Each matrix query returns identical results under CBO and RBO, after ANALYZE.

        Matrix case: runs once per plan shape (Q, Q_agg, Q_window, Q_subq, Q_idx).
        """
        self._ddl("ANALYZE ANALYTICS COLLECTION %s;" % self.AIRPORT, ignore_errors=True)
        self._ddl("ANALYZE ANALYTICS COLLECTION %s;" % self.ROUTE, ignore_errors=True)
        self._ensure_faa_index()
        for name, q in MATRIX:
            # compiler.cbo is NOT a request-body param (silently ignored by
            # /api/v1/request); it only takes effect as a SET clause on the
            # statement. Prepend SET to genuinely force each optimizer.
            rbo = self._analytics_request('SET `compiler.cbo` "false"; ' + q)
            cbo = self._analytics_request('SET `compiler.cbo` "true";  ' + q)
            self.assertEqual(rbo.get("status"), "success",
                             "%s RBO run failed: %s" % (name, rbo.get("errors")))
            self.assertEqual(cbo.get("status"), "success",
                             "%s CBO run failed: %s" % (name, cbo.get("errors")))
            self.assertEqual(self._norm(rbo.get("results")), self._norm(cbo.get("results")),
                             "%s: CBO and RBO results differ (correctness bug under CBO)" % name)

    def test_matrix_hit_miss(self):
        """Every plan shape in the matrix caches and reuses correctly (miss -> hit, same results).

        Matrix case: exercises join, aggregate, window, subquery and index-scan plans, so a
        wrong-plan-reuse bug specific to one shape is caught (which SELECT 1 never would).
        """
        self._ensure_faa_index()
        for name, q in MATRIX:
            self._clear_cache()
            cp1, r1, s1 = self._run_with_cache_flag(q)
            cp2, r2, s2 = self._run_with_cache_flag(q)
            self.assertEqual(s1, "success", "%s run1 failed: %s" % (name, s1))
            self.assertEqual(s2, "success", "%s run2 failed" % name)
            self.assertIs(cp1, False, "%s run1 should MISS, got %s" % (name, cp1))
            self.assertTrue(cp2, "%s run2 should HIT" % name)
            self.assertEqual(r1, r2, "%s results differ between miss and hit" % name)

    def test_plan_cached_on_compile_despite_runtime_failure(self):
        """A plan is stored at successful compile, before execution."""
        self._create_collection("pc_rt")
        self._ddl('INSERT INTO `pc_rt` ([{"a": 1, "b": 0}]);')
        self._clear_cache()
        q = "SELECT VALUE t.a / t.b FROM `pc_rt` t;"
        c1 = self._analytics_request(q)
        c2 = self._analytics_request(q)
        self.log.info("run1 status=%s errors=%s" % (c1.get("status"), c1.get("errors")))
        self.assertTrue(self._cached_plan(c2),
                        "plan must be cached at compile even if the query fails at runtime; "
                        "run2 cachedPlan=%s" % self._cached_plan(c2))

    def test_fresh_results_after_data_change(self):
        """A cache hit still returns fresh results after a data change (no DDL)."""
        self._create_collection("pc_fresh")
        self._ddl('INSERT INTO `pc_fresh` ([{"a": 1}, {"a": 2}]);')
        self._clear_cache()
        q = "SELECT VALUE COUNT(*) FROM `pc_fresh`;"
        cp1, _, _ = self._run_with_cache_flag(q)
        cp2, _, _ = self._run_with_cache_flag(q)
        self._ddl('INSERT INTO `pc_fresh` ([{"a": 3}, {"a": 4}, {"a": 5}]);')
        cp3, r3, _ = self._run_with_cache_flag(q)
        self.assertIs(cp1, False, "run1 should miss, got %s" % cp1)
        self.assertTrue(cp2, "run2 should hit")
        self.assertTrue(cp3, "data change (no DDL) must NOT invalidate; expected a hit")
        self.assertEqual(r3, [5], "results must be fresh on a hit; expected [5], got %s" % r3)

    def test_compile_error_not_cached(self):
        """A statement that fails to COMPILE stores no plan (only successful compiles cache).

        Inverse of test_plan_cached_on_compile_despite_runtime_failure: referencing a
        nonexistent collection fails at compile time, so a repeat must never report a hit
        (nothing was stored).
        """
        self._clear_cache()
        bad = "SELECT * FROM `travel-sample`.inventory.pc_no_such_coll_xyz;"
        c1 = self._analytics_request(bad)
        c2 = self._analytics_request(bad)
        self.assertNotEqual(c1.get("status"), "success",
                            "the query should fail to compile (unknown collection)")
        # An error envelope carries no `cachedPlan`; absent and false both mean
        # "nothing was stored", which is what this asserts.
        self.assertIn(self._cached_plan(c2), (False, None),
                      "a compilation error must not create a cache entry; run2 cachedPlan=%s"
                      % self._cached_plan(c2))

    def test_authz_enforced_after_revoke(self):
        """A cached plan is subject to CURRENT authorization - a revoked user is denied on a hit.

        Warms a read as an admin-capable user (plan cached under that userId), then demotes the
        user to analytics_reader (cannot read data) and re-runs the identical query. The request
        must be DENIED and return no rows - authorization is enforced per-request, not baked into
        the cached plan (no stale-auth leak).
        """
        self._create_collection("pc_authz")
        self._ddl('INSERT INTO `pc_authz` ([{"secret": 42}]);')
        self._ensure_user("pc_revoke", "analytics_admin")
        prot = "SELECT VALUE t.secret FROM `pc_authz` t;"
        self._clear_cache()
        self._warm(prot, username="pc_revoke", password=self.USER_PWD)   # plan cached; returns [42]
        self._set_user_role("pc_revoke", "analytics_reader")             # revoke data-read
        _, res, status = self._run_with_cache_flag(prot, username="pc_revoke", password=self.USER_PWD)
        self.assertNotEqual(status, "success",
                            "a revoked user must be DENIED even with a warm cached plan")
        self.assertNotEqual(res, [42],
                            "the cached plan must NOT leak protected data to a revoked user")

    # Operations NOT in the invalidation set. Only drops, CREATE INDEX, ANALYZE
    # and link/topology events invalidate; silent over-invalidation would defeat
    # the cache. One per run, selected by the `non_trigger` conf param.
    NON_TRIGGERS = {
        "create_collection":
            "CREATE COLLECTION `pc_nt2` IF NOT EXISTS PRIMARY KEY(`id`:UUID) AUTOGENERATED;",
        "create_synonym": "CREATE SYNONYM `pc_nt_syn` FOR `pc_nt`;",
        "create_function": "CREATE FUNCTION `pc_nt_fn`() { 1 };",
        "insert": 'INSERT INTO `pc_nt` ([{"z": 1}]);',
        "set": 'SET `compiler.cbo` "true";',
    }

    def test_non_triggers_do_not_invalidate(self):
        """Non-invalidating operations must NOT clear a warm plan cache.

        `non_trigger` selects the scope: a single key runs just that operation,
        while the default `all` runs every NON_TRIGGERS entry against ONE warm
        cache. `all` loses no coverage - each operation is asserted separately,
        and a sub-case that DOES over-invalidate is recorded and the cache
        re-warmed, so it cannot mask the operations after it. Every failing
        operation is reported, not just the first.
        """
        key = self.input.param("non_trigger", "all")
        if key == "all":
            keys = list(self.NON_TRIGGERS)
        elif key in self.NON_TRIGGERS:
            keys = [key]
        else:
            self.fail("unknown non_trigger=%s (expected all|%s)"
                      % (key, "|".join(sorted(self.NON_TRIGGERS))))
        self._create_collection("pc_nt")
        self._cleanup_ddl.append("DROP SYNONYM IF EXISTS `pc_nt_syn`;")
        self._cleanup_ddl.append("DROP FUNCTION IF EXISTS `pc_nt_fn`();")
        self._cleanup_ddl.append("DROP COLLECTION IF EXISTS `pc_nt2`;")
        self._clear_cache()
        self._warm(Q)
        over_invalidated = []
        for k in keys:
            self._ddl(self.NON_TRIGGERS[k], ignore_errors=True)
            cp, _, _ = self._run_with_cache_flag(Q)
            if not cp:
                over_invalidated.append((k, self.NON_TRIGGERS[k]))
                self._warm(Q)      # re-arm so the remaining sub-cases stay meaningful
        self.assertEqual(over_invalidated, [],
                         "these operations must NOT invalidate the cache "
                         "(over-invalidation): %s" % over_invalidated)

    def test_explain_not_cached(self):
        """EXPLAIN is never plan-cached and keys separately from the equivalent SELECT.

        EXPLAIN returns a freshly-produced plan by design, so it must not report a hit even when
        the identical SELECT is warm (the plan-cache-key `optimize` flag keys it apart).
        """
        self._clear_cache()
        self._warm(Q)                                   # SELECT Q is now cached
        e1, _, s1 = self._run_with_cache_flag("EXPLAIN " + Q)
        e2, _, s2 = self._run_with_cache_flag("EXPLAIN " + Q)
        self.assertEqual(s1, "success", "EXPLAIN should succeed")
        # EXPLAIN is non-cacheable, so - as with DDL/DML - EA may omit
        # `cachedPlan` rather than emit false. Both are correct; "true" is not.
        self.assertIn(e1, (False, None),
                      "EXPLAIN must not reuse the SELECT's cached plan "
                      "(separate key); cachedPlan=%s" % e1)
        self.assertIn(e2, (False, None),
                      "EXPLAIN must never report a cache hit (not "
                      "plan-cached); cachedPlan=%s" % e2)

    def test_parameterized_type_difference(self):
        """Parameterized keying is type-sensitive: the same numeric text as a different type misses.

        args=[500] (int) and args=["500"] (string) are different parameter values, so the second
        must MISS - plans are bound to exact parameter values/types (the doc's safety guarantee
        for RBO and CBO). Uses an airport-only query (route-independent).
        """
        pq = ("SELECT a.city FROM %s a WHERE a.geo.alt > $1 AND a.country='United States' "
              "LIMIT 3;" % self.AIRPORT)
        self._clear_cache()
        c1, _, _ = self._run_with_cache_flag(pq, args=[500])
        c2, _, _ = self._run_with_cache_flag(pq, args=[500])
        c3, _, _ = self._run_with_cache_flag(pq, args=["500"])
        self.assertEqual([c1, c2, c3], [False, True, False],
                         "expected [miss, hit, miss] for int/int/string args, got %s"
                         % [c1, c2, c3])

    def test_warnings_not_stale_on_hit(self):
        """A hit returns this request's own warnings, not a stale/empty set."""
        self._create_collection("pc_w")
        self._ddl('INSERT INTO `pc_w` ([{"a": "x"}]);')
        self._clear_cache()
        q = "SELECT VALUE t.a + 1 FROM `pc_w` t;"
        c1 = self._analytics_request(q)
        c2 = self._analytics_request(q)
        w1, w2 = c1.get("warnings"), c2.get("warnings")
        self.log.info("warnings miss=%s hit=%s" % (w1, w2))
        self.assertTrue(self._cached_plan(c2), "run2 should be a hit")
        self.assertEqual(w2, w1, "a hit must reproduce this request's warnings, not a stale set "
                                 "(miss=%s hit=%s)" % (w1, w2))

    # ============================ security (RBAC) ============================
    def test_no_cross_user_leakage(self):
        """Per-user keying: a plan compiled for one user is never served to another.

        Both principals must be able to READ the data (userId is part of the cache
        key, so the second user gets its own miss rather than reusing the first's
        plan). On this build only analytics_admin can read travel-sample -
        analytics_access/reader/select cannot - so we use two DISTINCT admin users;
        distinct userIds are what the per-user keying is being tested on.
        """
        self._ensure_user("pc_admin", "analytics_admin")
        self._ensure_user("pc_admin2", "analytics_admin")
        self._clear_cache()
        self._run_with_cache_flag(Q, username="pc_admin", password=self.USER_PWD)
        w2, _, _ = self._run_with_cache_flag(Q, username="pc_admin", password=self.USER_PWD)
        b1, _, bs = self._run_with_cache_flag(Q, username="pc_admin2", password=self.USER_PWD)
        self.assertTrue(w2, "first user's warm-up run should be a hit")
        self.assertIs(b1, False,
                      "the other user must key separately (miss), not reuse the first plan")
        self.assertEqual(bs, "success", "the second distinct user should still be able to run Q")

    def test_rbac_enforced_on_hit(self):
        """A cached plan never lets an under-privileged user read a protected collection.
        """
        self._create_collection("pc_protected")
        self._ddl('INSERT INTO `pc_protected` ([{"secret": 1}]);')
        # analytics_reader authenticates to analytics but has no data-read privilege
        self._ensure_user("pc_limited", "analytics_reader")
        self._clear_cache()
        prot_q = "SELECT VALUE t.secret FROM `pc_protected` t;"
        # admin warms + caches the plan for the protected collection
        self._warm(prot_q)
        # the under-privileged user must be DENIED even though a plan is cached
        _, r_prot, s_prot = self._run_with_cache_flag(prot_q, username="pc_limited",
                                             password=self.USER_PWD)
        self.assertNotEqual(s_prot, "success",
                            "limited user must be DENIED on the protected collection")
        self.assertNotEqual(r_prot, [1],
                            "limited user must NOT receive the protected data on a cache hit")

    # ============================ concurrency ============================
    # Non-deterministic + no cache-introspection API -> assert observable invariants.
    def test_thundering_herd_cold_miss(self):
        """Many concurrent identical queries on a cold cache all succeed."""
        self._clear_cache()
        n = self.input.param("herd_size", 15)

        def one():
            try:
                c = self._analytics_request(Q)
                return c.get("status"), c.get("results"), None
            except Exception as e:
                return None, None, str(e)

        with ThreadPoolExecutor(max_workers=n) as ex:
            results = [f.result() for f in [ex.submit(one) for _ in range(n)]]
        errors = [r[2] for r in results if r[2]]
        self.assertEqual(errors, [], "concurrent queries raised exceptions: %s" % errors)
        self.assertTrue(all(r[0] == "success" for r in results),
                        "all %d concurrent cold-miss queries must succeed; got %s"
                        % (n, [r[0] for r in results]))
        # Note: row-count is NOT asserted here. Q is an airport-route join and the
        # route collection may be unloaded on the lab dataset (0 docs), so Q can
        # legitimately return []. The thundering-herd property under test is that
        # concurrent cold-miss compiles all SUCCEED and return IDENTICAL results,
        # then collapse to a single cached plan - which holds regardless of rows.
        ref = results[0][1]
        self.assertTrue(all(r[1] == ref for r in results),
                        "all concurrent queries must return identical results")
        final, _, _ = self._run_with_cache_flag(Q)
        self.assertTrue(final, "after the herd, an identical query must be a HIT")

    def test_invalidation_races_lookup(self):
        """Invalidation racing concurrent lookups never yields wrong results or hangs."""
        self._clear_cache()
        self._cleanup_ddl.append("DROP INDEX %s.pc_race_idx;" % self.ROUTE)
        self._warm(Q)
        per_thread = self.input.param("race_queries", 20)
        query_threads = self.input.param("race_query_threads", 3)
        rounds = self.input.param("race_invalidations", 8)
        ref = self._analytics_request(Q).get("results")

        def querier():
            out = []
            for _ in range(per_thread):
                try:
                    c = self._analytics_request(Q)
                    out.append((c.get("status"), c.get("results")))
                except Exception as e:
                    out.append(("EXC", str(e)))
            return out

        def invalidator():
            for _ in range(rounds):
                self._analytics_request("CREATE INDEX pc_race_idx IF NOT EXISTS ON %s(sourceairport: string);" % self.ROUTE)
                self._analytics_request("DROP INDEX %s.pc_race_idx;" % self.ROUTE)
            return "done"

        with ThreadPoolExecutor(max_workers=query_threads + 1) as ex:
            qfuts = [ex.submit(querier) for _ in range(query_threads)]
            ifut = ex.submit(invalidator)
            query_results = []
            for f in qfuts:
                query_results.extend(f.result())
            ifut.result()
        bad = [r for r in query_results if r[0] != "success"]
        self.assertEqual(bad, [], "every concurrent query must succeed during invalidation; "
                                  "first few bad = %s" % bad[:5])
        self.assertTrue(all(r[1] == ref for r in query_results),
                        "every query must return the correct results during the race")

    # ============================ scale ============================
    def test_scale_many_distinct_plans(self):
        """Cache stays correct and bounded under many distinct plans.

        Inserts `total` (> capacity) distinct Q variants sequentially, so insertion order
        equals LRU order: oldest evicted, most-recent retained, all succeed. Q is heavy, so
        defaults are modest; raise scale_capacity/scale_queries toward the plan's 1000/1500.
        """
        capacity = self.input.param("scale_capacity", 100)
        total = self.input.param("scale_queries", 150)
        self.assertGreater(total, capacity, "scale needs total (%d) > capacity (%d)" % (total, capacity))
        self._capacity_modified = True
        self._set_capacity(capacity)
        self._clear_cache()
        base = 1000
        failures = []
        for i in range(total):
            _, _, st = self._run_with_cache_flag(q_dist(base + i))
            if st != "success":
                failures.append((i, st))
        self.assertEqual(failures, [], "%d/%d scale inserts failed; first few: %s"
                         % (len(failures), total, failures[:5]))
        recent = [base + total - 1, base + total - 2, base + total - 3]
        recent_misses = [n for n in recent if not self._run_with_cache_flag(q_dist(n))[0]]
        self.assertEqual(recent_misses, [], "most-recent plans must still be cached (hit); missed: %s"
                         % recent_misses)
        oldest = [base + 0, base + 1, base + 2]
        old_hits = [n for n in oldest if self._run_with_cache_flag(q_dist(n))[0]]
        self.assertEqual(old_hits, [], "oldest plans must be evicted (miss); unexpectedly cached: %s"
                         % old_hits)

    # ============================ links / KV (need infra) ============================
    def _require_remote(self):
        remote = getattr(self, "remote_cluster", None)
        if not remote:
            self.fail("requires a remote cluster (2-cluster ini: analytics + remote)")
        return remote

    def _create_remote_link(self, name):
        remote = self._require_remote()
        status, content = remote.rest.security.get_trusted_root_certificates()
        self.assertTrue(status, "failed to fetch remote cluster certificate")
        props = {"name": name, "type": "couchbase", "hostname": remote.master.ip,
                 "username": remote.master.rest_username, "password": remote.master.rest_password,
                 "encryption": "full", "certificate": content[0]["pem"]}
        self.assertTrue(self.cbas_util.create_link(self.columnar_cluster, props),
                        "failed to create remote link %s" % name)
        self._cleanup_ddl.append("DROP LINK %s;" % name)

    # S3 link DDL, placeholder credentials - the link is never connected, so no
    # real credentials are needed or used.
    CREATE_S3_LINK = ('CREATE LINK pc_s3link TYPE S3 WITH '
                      '{"accessKeyId": "dummy", "secretAccessKey": "dummy", '
                      '"region": "us-east-1"};')
    LINK_DDL = {
        "create": CREATE_S3_LINK,
        "alter": 'ALTER LINK pc_s3link SET WITH {"region": "us-west-2"};',
        "drop": "DROP LINK pc_s3link;",
    }

    LINK_DDL_SEQUENCE = ["create", "alter", "drop"]

    def test_link_ddl_invalidates(self):
        """Link DDL (CREATE / ALTER / DROP LINK) clears the plan cache.
        """
        form = self.input.param("link_ddl", "all")
        if form == "all":
            forms = list(self.LINK_DDL_SEQUENCE)
        elif form in self.LINK_DDL:
            forms = [form]
        else:
            self.fail("unknown link_ddl=%s (expected all|%s)"
                      % (form, "|".join(sorted(self.LINK_DDL))))
        if forms[0] != "create":
            # ALTER/DROP need the link to exist first; that setup must not be
            # the event under test, so it happens before the cache is warmed.
            self._ddl(self.CREATE_S3_LINK)
        self._cleanup_ddl.append("DROP LINK IF EXISTS pc_s3link;")
        self._clear_cache()
        for f in forms:
            self._warm(Q)
            self._ddl(self.LINK_DDL[f])
            cp, _, _ = self._run_with_cache_flag(Q)
            self.assertIs(cp, False,
                          "%s LINK must invalidate the plan cache (expected an "
                          "explicit MISS); cachedPlan=%s" % (f.upper(), cp))

    def test_connect_disconnect_does_not_invalidate(self):
        """CONNECT / DISCONNECT LINK must NOT clear the cache (ingestion-only)."""
        self._require_remote()
        self._create_remote_link("pc_rlink")
        self._clear_cache()
        self._warm(Q)
        self._ddl("DISCONNECT LINK pc_rlink;", ignore_errors=True)
        self._ddl("CONNECT LINK pc_rlink;", ignore_errors=True)
        cp, _, _ = self._run_with_cache_flag(Q)
        self.assertTrue(cp, "connect/disconnect affect ingestion only and must NOT invalidate")

    # ============================ topology invalidation ============================
    # A rebalance/failover clears the whole cache via a non-statement path. Cached plans
    # encode inter-node data exchange, so reuse across a changed topology would be wrong -
    # this needs a >=2-node cluster + a collection query (Q), not SELECT 1. Each topology
    # change is slow, so we warm all matrix shapes and do ONE change, then assert all miss.
    def _run_via(self, node, statement):
        """POST a statement to a SPECIFIC cbas node's /api/v1/request.

        Cross-node cases must bypass self.analytics_api (which is pinned to one
        node), so they build a client for the target node and go through the
        same framework executor everything else uses.
        """
        return self._analytics_request(
            statement, client=AnalyticsRestAPI(node))

    def _warm_matrix(self):
        self._ensure_faa_index()
        self._clear_cache()
        for name, q in MATRIX:
            self._run_with_cache_flag(q)
            cp, _, _ = self._run_with_cache_flag(q)
            self.assertTrue(cp, "%s should be cached before the topology change" % name)

    def _assert_matrix_all_miss(self, phase):
        for name, q in MATRIX:
            cp, _, _ = self._run_with_cache_flag(q)
            self.assertIs(cp, False,
                          "%s must MISS after %s (topology invalidation); "
                          "cachedPlan=%s" % (name, phase, cp))

    def test_rebalance_invalidates(self):
        """A rebalance (cbas node out, then back in) clears the plan cache.

        The add-back is part of the ASSERTION, not cleanup - the cache must miss
        after the node returns too. Recovery from a mid-test failure is
        tearDown's job (_restore_topology_if_shrunk), which is why there is no
        try/finally here.
        """
        self._expected_cbas_nodes = self._cbas_node_count()
        self._warm_matrix()
        # remove a cbas node
        task, self.columnar_cluster.available_servers = self.rebalance_util.rebalance(
            cluster=self.columnar_cluster, cbas_nodes_out=1,
            exclude_nodes=[self.columnar_cluster.master],
            available_servers=self.columnar_cluster.available_servers)
        self.assertTrue(
            self.rebalance_util.wait_for_rebalance_task_to_complete(
                task, self.columnar_cluster, True, True),
            "rebalance-out of a cbas node failed")
        self._assert_matrix_all_miss("rebalance-out")
        # Add the node back with the cbas service only. If the cluster rejects
        # a cbas-only rebalance-in, this is the line to revisit.
        task, self.columnar_cluster.available_servers = self.rebalance_util.rebalance(
            cluster=self.columnar_cluster, cbas_nodes_in=1,
            in_node_services="cbas",
            available_servers=self.columnar_cluster.available_servers)
        self.assertTrue(
            self.rebalance_util.wait_for_rebalance_task_to_complete(
                task, self.columnar_cluster, True, True),
            "rebalance-in of a cbas node failed")
        self._assert_miss(Q)

    def test_failover_invalidates(self):
        """A cbas node failover (with recovery) clears the plan cache.
        """
        self._expected_cbas_nodes = self._cbas_node_count()
        self._warm_matrix()
        self.columnar_cluster.available_servers, _, _ = self.rebalance_util.failover(
            cluster=self.columnar_cluster, cbas_nodes=1,
            failover_type=self.input.param("failover_type", "Hard"),
            action="FullRecovery",
            exclude_nodes=[self.columnar_cluster.master],
            available_servers=self.columnar_cluster.available_servers)
        self._assert_matrix_all_miss("failover")

    def test_cross_node_shared_hit(self):
        """The cache is cluster-shared: a plan compiled via one cbas node is a HIT on another.
        """
        nodes = self.cluster_util.get_nodes_from_services_map(
            self.columnar_cluster, service_type="cbas", get_all_nodes=True,
            servers=self.columnar_cluster.nodes_in_cluster)
        if len(nodes) < 2:
            self.fail("need >= 2 cbas nodes for a cross-node hit, got %s"
                      % len(nodes))
        node_a, node_b = nodes[0], nodes[1]
        self._clear_cache()
        self._run_via(node_a, Q)
        self.assertTrue(self._cached_plan(self._run_via(node_a, Q)),
                        "Q should be cached after warming on node A")
        cross = self._run_via(node_b, Q)
        self.assertTrue(self._cached_plan(cross),
                        "a plan cached via node A must be served as a HIT on node B "
                        "(cluster-shared cache); got cachedPlan=%s" % self._cached_plan(cross))
