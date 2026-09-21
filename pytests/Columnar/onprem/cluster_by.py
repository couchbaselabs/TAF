"""
CLUSTER BY / CLUSTER AS - query-time distributed K-Means clustering of vector
embeddings on Couchbase Enterprise Analytics (onprem-columnar).
"""

import json
import os
import random
import struct
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from cb_server_rest_util.analytics.analytics_api import AnalyticsRestAPI
from shell_util.remote_connection import RemoteMachineShellConnection
from Columnar.onprem.columnar_onprem_base import ColumnarOnPremBase
from TestInput import TestInputSingleton
from cbas_utils.cbas_utils_columnar import CbasUtil as ColumnarCbasUtil
from cbas_utils.cbas_utils_columnar import RBAC_Util as ColumnarRBACUtil
from cbas_utils.cbas_utils_on_prem import CBASRebalanceUtil
from cluster_utils.cluster_ready_functions import CBCluster
from BucketLib.bucket import Bucket
from bucket_utils.bucket_ready_functions import JavaDocLoaderUtils
from Jython_tasks.java_loader_tasks import SiriusCouchbaseLoader
from security_utils.x509main import x509main


class ClusterBy(ColumnarOnPremBase):

    _sift_link = None

    # --------------------------------------------------------------- knobs
    def _base_setup(self):
        if not TestInputSingleton.input.param("skip_cbas_cleanup", True):
            super(ClusterBy, self).setUp()
            return
        original = ColumnarCbasUtil.cleanup_cbas
        ColumnarCbasUtil.cleanup_cbas = lambda *args, **kwargs: True
        try:
            super(ClusterBy, self).setUp()
        finally:
            ColumnarCbasUtil.cleanup_cbas = original

    def setUp(self):
        self._aux_collections = []
        self._cleanup_ddl = []
        self._rbac_users = []

        self._base_setup()

        self.no_of_docs = self.input.param("no_of_docs", 500)
        self.dim = self.input.param("dim", 4)
        self.centers = self.input.param("num_centers", 5)
        self.num_clusters = self.input.param("num_clusters", 5)
        self.seed = self.input.param("seed", 42)
        self.rbac_password = self.input.param("rbac_password", "password")

        # Link-fed / SIFT knobs (only used by the remote-link cases)
        self.sift_bucket = self.input.param("sift_bucket", "sift")
        self.sift_dim = self.input.param("sift_dim", 128)
        self.docloader_create_end = self.input.param(
            "docloader_create_end", 100000)
        self.base_vectors_file_path = self.input.param(
            "base_vectors_file_path", "/mnt/nfsdata/bigann")
        self.load_sift = self.input.param("load_sift", False)
        self.drop_sift_fixture = self.input.param("drop_sift_fixture", False)
        self.sift_vectors_file = os.path.join(
            self.base_vectors_file_path, "bigann_base.bvecs")

        self._check_fixture_params()

        self.analytics_api = AnalyticsRestAPI(self.columnar_cluster.master)

        self.rbac_util = ColumnarRBACUtil(self.task, self.use_sdk_for_cbas)
        if not hasattr(self.columnar_cluster, "available_servers"):
            self.columnar_cluster.available_servers = []
        self.rebalance_util = CBASRebalanceUtil(
            self.cluster_util, self.bucket_util, self.task, False,
            self.cbas_util)

        self._ensure_pts()
        self.log_setup_status(
            self.__class__.__name__, "Finished", stage=self.setUp.__name__)

    def _check_fixture_params(self):
        """Fail on parameter combinations the shared blob fixture cannot
        satisfy, so a mis-parameterised run reports the parameters rather than
        a downstream assertion. _mkvec assigns blob c = i % centers, so the
        blobs are equally sized only when no_of_docs divides by centers, and
        several cases assert equal per-blob counts."""
        bad = []
        for name, value in (("no_of_docs", self.no_of_docs),
                            ("dim", self.dim),
                            ("num_centers", self.centers),
                            ("num_clusters", self.num_clusters)):
            if not isinstance(value, int) or value < 1:
                bad.append("{0}={1!r} must be a positive integer".format(
                    name, value))
        if not bad and self.no_of_docs % self.centers:
            bad.append(
                "no_of_docs={0} is not divisible by num_centers={1}, so the "
                "blobs are unequal and the per-blob count assertions cannot "
                "hold".format(self.no_of_docs, self.centers))
        if bad:
            self.fail("Unusable fixture parameters: " + "; ".join(bad))

    def tearDown(self):
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)

        leaked = []
        for stmt in reversed(getattr(self, "_cleanup_ddl", [])):
            try:
                self._analytics_request(stmt)
            except Exception as err:
                leaked.append("cleanup DDL failed ({0}): {1}".format(
                    stmt, err))
        for coll in getattr(self, "_aux_collections", []):
            status, _, errors, _, _, _ = self._run(
                "DROP COLLECTION {0} IF EXISTS;".format(coll),
                fail_on_error=False)
            if status != "success":
                leaked.append("failed to drop collection {0}: {1}".format(
                    coll, errors))
        for user in getattr(self, "_rbac_users", []):
            try:
                if not self.rbac_util.delete_user(self.columnar_cluster, user):
                    leaked.append("failed to drop user {0}".format(user))
            except Exception as err:
                leaked.append("failed to drop user {0}: {1}".format(user, err))
        if getattr(self, "drop_sift_fixture", False):
            self._drop_sift_fixture()
        super(ClusterBy, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage="Teardown")
        if leaked:
            self.fail("Teardown left artifacts behind:\n  " +
                      "\n  ".join(leaked))

    def _drop_sift_fixture(self):
        """Drop the persistent link-fed fixture (shadow dataset + remote link).
        Opt-in via drop_sift_fixture=True: the fixture is deliberately shared
        across test processes, so dropping it mid-suite forces a full re-ingest."""
        for stmt in ("DROP DATASET Default.Default.sift_ds IF EXISTS;",
                     "DROP LINK sift_remote_link IF EXISTS;"):
            try:
                self._run(stmt, fail_on_error=False)
            except Exception as err:
                self.log.warning("SIFT fixture cleanup failed ({0}): {1}".format(
                    stmt, err))

    # ----------------------------------------------------------- provisioning
    def _ensure_pts(self):
        """Provision + load the shared well-separated blob collection and bind
        it to self.pts. testrunner re-execs a fresh process per test, so the
        collection is given a deterministic name keyed on its config and
        whatever a previous process left behind is reused (create + load
        skipped). Name encodes the config so a config change never reuses stale
        data; the exact-count check means a partial load is never reused."""
        name = "cb_pts_{0}_{1}_{2}_{3}".format(
            self.no_of_docs, self.dim, self.centers, self.seed)
        full = "Default.Default.{0}".format(name)
        if self._count_of(full, fail_on_error=False) == self.no_of_docs:
            self.pts = full
            return
        self._run("DROP COLLECTION {0} IF EXISTS;".format(full),
                  fail_on_error=False)
        self._run("CREATE COLLECTION {0} PRIMARY KEY (pk: bigint);".format(name),
                  msg="Failed to create shared collection {0}".format(name))
        docs = self._mkvec(self.no_of_docs, self.dim, self.centers,
                           seed=self.seed)
        self._load_docs(name, docs)
        self.pts = full

    # ---------------------------------------------------------- data builders
    @staticmethod
    def _make_centers(centers, dim, sep):
        out = []
        for k in range(centers):
            v = [0.0] * dim
            v[k % dim] = sep * (k + 1)
            out.append(v)
        return out

    @staticmethod
    def _round(x):
        return round(x, 4)

    def _mkuniform(self, n, dim, seed=42, lo=0.0, hi=100.0):
        """n docs whose vectors are uniform over [lo, hi] in every dimension,
        carrying pk (bigint PK) and vec. The cloud has no cluster structure, so
        k-means keeps refining for many iterations instead of settling on the
        first pass."""
        rng = random.Random(seed)
        return [{"pk": i,
                 "vec": [self._round(rng.uniform(lo, hi)) for _ in range(dim)]}
                for i in range(n)]

    def _mkvec(self, n, dim, centers, seed=42, sep=10.0, sigma=0.5):
        """n balanced Gaussian-blob docs. Each doc carries pk (bigint PK),
        cid_true (true blob 0..centers-1), vec (dim doubles), plus scalar
        helper fields used by specific cases: a/b (comma-list & LET cases),
        year (non-key WHERE filter, ~half < 2020), grp (deterministic control
        for the non-det-input case)."""
        rng = random.Random(seed)
        centroids = self._make_centers(centers, dim, sep)
        docs = []
        for i in range(n):
            c = i % centers
            vec = [self._round(centroids[c][d] + rng.gauss(0, sigma))
                   for d in range(dim)]
            docs.append({
                "pk": i, "cid_true": c, "vec": vec,
                "a": vec[0], "b": vec[1 % dim],
                "year": 2015 if (i % 2 == 0) else 2025,
                "grp": c % 2,
            })
        return docs

    def _mkvec_dupes(self, n_identical, n_other, dim, seed=42, sep=10.0,
                     sigma=0.5):
        """n_identical byte-identical vectors + n_other normal points around a
        2nd centre. Two distinct-enough blobs so num_clusters=2 is legal."""
        centroids = self._make_centers(2, dim, sep)
        fixed = [self._round(x) for x in centroids[0]]
        docs = [{"pk": i, "cid_true": 0, "vec": fixed[:]}
                for i in range(n_identical)]
        rng = random.Random(seed)
        for k in range(n_other):
            i = n_identical + k
            vec = [self._round(centroids[1][d] + rng.gauss(0, sigma))
                   for d in range(dim)]
            docs.append({"pk": i, "cid_true": 1, "vec": vec})
        return docs

    def _mk_exact_dupes(self, m, per_blob=40):
        """m exact-duplicate blobs, per_blob identical rows each."""
        return [{"pk": c * per_blob + i, "cid_true": c, "vec": [c * 10, 0, 0, 0]}
                for c in range(m) for i in range(per_blob)]

    @staticmethod
    def _mk_movies_reviews(n_movies=5, reviews_per_movie=3, dim=4, seed=7):
        """JOIN fixture: Movies(movie_id, movie_year) + flat
        Reviews(review_id, movie_id, vec), review vectors blobbed per movie."""
        rng = random.Random(seed)
        movies, reviews, rid = [], [], 0
        for mid in range(n_movies):
            movies.append({"movie_id": mid,
                           "movie_year": 2015 if mid % 2 == 0 else 2025})
            centre = [0.0] * dim
            centre[mid % dim] = 10.0 * (mid + 1)
            for _ in range(reviews_per_movie):
                vec = [round(centre[d] + rng.gauss(0, 0.4), 4)
                       for d in range(dim)]
                reviews.append({"review_id": rid, "movie_id": mid, "vec": vec})
                rid += 1
        return movies, reviews

    @staticmethod
    def _mk_movies_nested_reviews(n_movies=10, reviews_per_movie=3, dim=4,
                                  seed=11):
        """UNNEST fixture: movies with reviews:[{review, vec}, ...]."""
        rng = random.Random(seed)
        movies = []
        for mid in range(n_movies):
            centre = [0.0] * dim
            centre[mid % dim] = 10.0 * (mid + 1)
            revs = [{"review": r,
                     "vec": [round(centre[d] + rng.gauss(0, 0.4), 4)
                             for d in range(dim)]}
                    for r in range(reviews_per_movie)]
            movies.append({"movie_id": mid, "reviews": revs})
        return movies

    # ------------------------------------------------------- collection utils
    def _load_docs(self, collection_name, docs, batch=500):
        """UPSERT docs into a standalone collection in batches."""
        for i in range(0, len(docs), batch):
            if not self.cbas_util.upsert_into_standalone_collection(
                    self.columnar_cluster, collection_name, docs[i:i + batch],
                    timeout=1800, analytics_timeout=1800):
                self.fail("Failed to load {0} docs into {1}".format(
                    len(docs), collection_name))

    def _create_aux_collection(self, name, docs=None, primary_key="pk: bigint"):
        """CREATE a standalone collection with a known name, optionally load
        docs, and register it for teardown."""
        self._run("DROP COLLECTION {0} IF EXISTS;".format(name),
                  fail_on_error=False)
        self._run("CREATE COLLECTION {0} PRIMARY KEY ({1});".format(
            name, primary_key),
            msg="Failed to create collection {0}".format(name))
        self._aux_collections.append(name)
        if docs:
            self._load_docs(name, docs)
        return name

    # ------------------------------------------------------------- query utils
    def _with(self, num_clusters=None, dimension=None, algorithm="K-Means",
              similarity="euclidean", init_mode=None, seed=None,
              cross_pollination=None, num_iterations=None, extra=None,
              drop=None):
        """Build the CLUSTER BY `WITH { ... }` block. clustering_algorithm
        and dimension (a one-element array) are required, so both are included
        by default. `drop` removes a key to build missing-option cases; `extra`
        injects raw key:value pairs for unknown-option and bad-type cases."""
        opts = {}
        if algorithm is not None:
            opts["clustering_algorithm"] = json.dumps(algorithm)
        if dimension is None:
            dimension = [self.dim]
        opts["dimension"] = json.dumps(dimension)
        nc = self.num_clusters if num_clusters is None else num_clusters
        if nc is not None:
            opts["num_clusters"] = json.dumps(nc)
        if similarity is not None:
            opts["similarity"] = json.dumps(similarity)
        if init_mode is not None:
            opts["init_mode"] = json.dumps(init_mode)
        if seed is not None:
            opts["seed"] = json.dumps(seed)
        if cross_pollination is not None:
            opts["cross_pollination"] = json.dumps(cross_pollination)
        if num_iterations is not None:
            opts["num_iterations"] = json.dumps(num_iterations)
        for key in (drop or []):
            opts.pop(key, None)
        pairs = ["\"{0}\": {1}".format(k, v) for k, v in opts.items()]
        for raw_k, raw_v in (extra or {}).items():
            pairs.append("\"{0}\": {1}".format(raw_k, raw_v))
        return "WITH{ " + ", ".join(pairs) + " }"

    def _run(self, statement, timeout=300, analytics_timeout=300,
             username=None, password=None, fail_on_error=True, msg=None):
        """Run a statement through the columnar util; return the 6-tuple
        (status, metrics, errors, results, handle, warnings). Fails the test on
        a non-success status unless fail_on_error is False; `msg` labels that
        failure."""
        result = self.cbas_util.execute_statement_on_cbas_util(
            self.columnar_cluster, statement, timeout=timeout,
            analytics_timeout=analytics_timeout, username=username,
            password=password)
        status = result[0]
        if fail_on_error and status != "success":
            self.fail("{0}: {1}\n  errors={2}".format(
                msg or "Statement failed unexpectedly", statement, result[2]))
        return result

    def _count_of(self, source, fail_on_error=True):
        """Row count for `source` - a collection name, optionally with an alias
        and a WHERE clause, or a parenthesised subquery. Returns the integer, or
        None when the statement failed and fail_on_error is False."""
        status, _, _, rows, _, _ = self._run(
            "SELECT VALUE count(*) FROM {0};".format(source),
            fail_on_error=fail_on_error)
        if status != "success" or not rows:
            return None
        return rows[0]

    def _run_raw(self, statement, profile=None, max_warnings=None,
                 scan_consistency=None, timeout=None, username=None,
                 password=None, http_timeout=None):
        """Run through /api/v1/request and return the full envelope dict so
        profile / warnings can be read - the 6-tuple util drops them.

        `timeout` is the server-side query timeout carried in the payload;
        `http_timeout` is the socket timeout. submit_service_request defaults
        the socket to 300s, which several cases here exceed, so the default
        comes from the http_timeout parameter instead."""
        extra = {}
        if profile is not None:
            extra["profile"] = profile
        if scan_consistency is not None:
            extra["scan_consistency"] = scan_consistency
        kwargs = {"http_timeout": (
            self.input.param("http_timeout", 1800)
            if http_timeout is None else http_timeout)}
        if extra:
            kwargs["extra_params"] = extra
        if max_warnings is not None:
            kwargs["max_warnings"] = max_warnings
        if timeout is not None:
            kwargs["timeout"] = timeout
        if username is not None:
            kwargs["username"] = username
            kwargs["password"] = password
        return self._analytics_request(statement, **kwargs)

    def _explain(self, statement):
        """Return the EXPLAIN plan text for a statement."""
        _, _, _, results, _, _ = self._run("EXPLAIN " + statement)
        return json.dumps(results)

    # ------------------------------------------------------- profile walkers
    @staticmethod
    def _iter_named_ops(node):
        """Yield every dict in a profile envelope that carries an operator
        `name`."""
        if isinstance(node, dict):
            if "name" in node:
                yield node
            for v in node.values():
                for op in ClusterBy._iter_named_ops(v):
                    yield op
        elif isinstance(node, list):
            for v in node:
                for op in ClusterBy._iter_named_ops(v):
                    yield op

    def _operator_runtimes(self, profile):
        """Map operator-name -> max run-time seen. Operators that report no
        run-time are omitted, so an empty result means the profile carried no
        timings rather than zero-cost operators."""
        runtimes = {}
        for op in self._iter_named_ops(profile):
            name = str(op.get("name", ""))
            rt = op.get("run-time", op.get("runtime"))
            if name and isinstance(rt, (int, float)) and not isinstance(rt, bool):
                runtimes[name] = max(runtimes.get(name, rt), rt)
        return runtimes

    @staticmethod
    def _scan_totals(profile):
        """Sum cardinality-out per Index Search scan runtime-id."""
        totals = {}
        for op in ClusterBy._iter_named_ops(profile):
            if "Index Search" in str(op.get("name", "")) and "runtime-id" in op:
                rid = op["runtime-id"]
                totals[rid] = totals.get(rid, 0) + op.get("cardinality-out", 0)
        return totals

    # -------------------------------------------------------- correctness
    def _cluster_rows(self, statement, **kwargs):
        """Run a CLUSTER BY statement and return its result rows, failing on a
        non-success status."""
        status, _, errors, results, _, _ = self._run(statement, **kwargs)
        if status != "success":
            self.fail("CLUSTER BY failed: {0}\n  errors={1}".format(
                statement, errors))
        return results or []

    def _members_stmt(self, coll_full=None, vec_expr="p.vec", projection=None,
                      suffix="", from_clause=None, having=None, **with_kw):
        """Build a CLUSTER BY ... CLUSTER AS members statement.

        `from_clause` is the text following FROM; when omitted it is
        `<coll_full> AS p`, the binding the default vec_expr `p.vec` reads.
        Pass it directly for subquery, join and UNNEST sources. `having` is the
        predicate for a HAVING clause, which SQL++ places before SELECT."""
        if from_clause is None:
            from_clause = "{0} AS p".format(coll_full)
        return ("FROM {0} CLUSTER BY {1} AS sc CLUSTER AS members {2} "
                "{5}SELECT {3}{4};").format(
                    from_clause, vec_expr, self._with(**with_kw),
                    projection or "array_count(members) AS cnt",
                    (" " + suffix) if suffix else "",
                    "HAVING {0} ".format(having) if having else "")

    def _sum_members(self, coll_full, vec_expr="p.vec", raw=False,
                     profile=None, max_warnings=None, prefix="", **with_kw):
        """Total members and cluster count for a bare CLUSTER BY over
        coll_full - the disjoint-assignment invariant (total == input rows).
        `raw` routes through /api/v1/request so profile / warnings are
        observable; it then returns (total, n_clusters, envelope)."""
        stmt = prefix + self._members_stmt(coll_full, vec_expr=vec_expr,
                                           **with_kw)
        if raw:
            content = self._run_raw(stmt, profile=profile,
                                    max_warnings=max_warnings)
            rows = content.get("results") or []
            return sum(r["cnt"] for r in rows), len(rows), content
        rows = self._cluster_rows(stmt)
        return sum(r["cnt"] for r in rows), len(rows)

    def _cluster_counts(self, coll_full, suffix="ORDER BY cid", **with_kw):
        """Rows of {cid, cnt} for a bare CLUSTER BY over coll_full."""
        return self._cluster_rows(self._members_stmt(
            coll_full,
            projection="sc.cluster_id AS cid, array_count(members) AS cnt",
            suffix=suffix, **with_kw))

    def _assert_member_bindings(self, rows, expected, label):
        """Assert every cluster's members carry exactly the FROM bindings in
        `expected` (the field names a member record exposes)."""
        if not rows:
            self.fail("{0}: no clusters returned, member bindings "
                      "unverified".format(label))
        for row in rows:
            if sorted(row["f"]) != sorted(expected):
                self.fail("{0}: members should retain bindings {1}, got "
                          "{2}".format(label, sorted(expected), row["f"]))

    def _wait_for_analytics_ready(self, timeout=180, interval=5, probe=None):
        """Poll until analytics answers a query that reads the shared
        collection, so the wait covers the restarted node's partitions rather
        than only the coordinator being reachable. `probe` overrides the
        statement."""
        probe = probe or "SELECT VALUE count(*) FROM {0};".format(self.pts)
        deadline = time.time() + timeout
        last = None
        while time.time() < deadline:
            status, _, errors, _, _, _ = self._run(probe, fail_on_error=False)
            if status == "success":
                return True
            last = errors
            time.sleep(interval)
        self.fail("Analytics did not become ready within {0}s (probe={1}, "
                  "last errors: {2})".format(timeout, probe, last))

    @staticmethod
    def _result_drift(rows_a, rows_b, tol=1e-9):
        """Compare two result sets that must describe the SAME clustering.
        Everything except floats is compared exactly (cluster_id, member pks,
        counts, shape); floats are compared to a relative tolerance. Returns
        (ok, worst_drift, detail).

        Compare centroids to a tolerance, never bit-exactly: they are
        partition-reduced means. Reserve exactness for integers and partitions.
        """
        worst = 0.0

        def cmp(a, b, path):
            nonlocal worst
            if isinstance(a, bool) or isinstance(b, bool):
                return None if a == b else "%s: %r != %r" % (path, a, b)
            if isinstance(a, float) or isinstance(b, float):
                try:
                    delta = abs(float(a) - float(b))
                except (TypeError, ValueError):
                    return "%s: %r != %r" % (path, a, b)
                worst = max(worst, delta)
                if delta > tol * max(1.0, abs(float(a))):
                    return "%s: %r vs %r (|diff|=%g)" % (path, a, b, delta)
                return None
            if isinstance(a, dict) and isinstance(b, dict):
                if set(a) != set(b):
                    return "%s: keys %s != %s" % (path, sorted(a), sorted(b))
                for key in sorted(a):
                    bad = cmp(a[key], b[key], "%s.%s" % (path, key))
                    if bad:
                        return bad
                return None
            if isinstance(a, list) and isinstance(b, list):
                if len(a) != len(b):
                    return "%s: length %d != %d" % (path, len(a), len(b))
                for idx, (x, y) in enumerate(zip(a, b)):
                    bad = cmp(x, y, "%s[%d]" % (path, idx))
                    if bad:
                        return bad
                return None
            return None if a == b else "%s: %r != %r" % (path, a, b)

        rows_a, rows_b = rows_a or [], rows_b or []
        if len(rows_a) != len(rows_b):
            return False, worst, "row count %d != %d" % (len(rows_a),
                                                         len(rows_b))
        detail = cmp(rows_a, rows_b, "rows")
        return detail is None, worst, detail

    @staticmethod
    def _argmin_centroid(vec, centroids):
        """Index of the nearest centroid by squared euclidean distance."""
        best, best_d = None, None
        for cid, cen in centroids.items():
            d = sum((a - b) ** 2 for a, b in zip(vec, cen))
            if best_d is None or d < best_d:
                best, best_d = cid, d
        return best

    def _assert_error(self, statement, expected_code=None, expected_msg=None,
                      username=None, password=None):
        """Run a statement expected to FAIL and assert its code / message.
        Routed through /api/v1/request (JSON body) so set-ops are not mangled by
        the legacy form-encoding transport. Exact code match, substring match on
        the message with backticks stripped."""
        content = self._run_raw(statement, username=username, password=password)
        if not isinstance(content, dict):
            self.fail("Expected an error envelope but got {0!r} for: {1}".format(
                content, statement))
        status = content.get("status")
        if status == "success":
            self.fail("Expected failure but statement succeeded: {0}\n"
                      "  content={1}".format(statement, content))
        errors = content.get("errors") or []
        err = errors[0] if errors else {}
        code, msg = err.get("code"), str(err.get("msg", ""))
        if expected_code is not None and code != expected_code:
            self.fail("Expected error code {0}, got {1} (msg={2}) for: {3}"
                      .format(expected_code, code, msg, statement))
        if (expected_msg is not None
                and expected_msg.replace("`", "") not in msg.replace("`", "")):
            self.fail("Expected error msg containing '{0}', got '{1}' for: {2}"
                      .format(expected_msg, msg, statement))
        return code, msg

    def _register_ddl_cleanup(self, drop_statement):
        self._cleanup_ddl.append(drop_statement)

    def _profile_of(self, statement):
        """Run a statement with profile=timings and return the `profile`
        sub-envelope (or fail if the request itself failed)."""
        content = self._run_raw(statement, profile="timings")
        if content.get("status") != "success":
            self.fail("Profiled statement failed: {0}\n  errors={1}".format(
                statement, content.get("errors")))
        return content.get("profile", {})

    def _has_operator(self, profile, substring):
        """True if any operator name in the profile contains `substring`
        (names carry an @hashcode suffix, so this is a substring match)."""
        return any(substring in str(op.get("name", ""))
                   for op in self._iter_named_ops(profile))

    # ======================================================================
    #  Category: Syntax & parsing
    # ======================================================================
    def test_as_descriptor_optional(self):
        """All three CLUSTER BY forms parse and execute: with `AS sc`, with no
        descriptor at all, and with `CLUSTER AS members` only. The
        descriptor-less members form still assigns every input row."""
        w = self._with()
        s1 = "FROM {0} AS p CLUSTER BY p.vec AS sc {1} SELECT sc.cluster_id " \
             "AS cid;".format(self.pts, w)
        s2 = "FROM {0} AS p CLUSTER BY p.vec {1} SELECT 1 AS c;".format(
            self.pts, w)
        s3 = "FROM {0} AS p CLUSTER BY p.vec CLUSTER AS members {1} " \
             "SELECT array_count(members) AS cnt;".format(self.pts, w)
        rows = []
        for label, stmt in (("AS sc", s1), ("no descriptor", s2),
                            ("CLUSTER AS only", s3)):
            _, _, _, results, _, _ = self._run(
                stmt,
                msg="Form '{0}' should parse and execute".format(label))
            if label == "CLUSTER AS only":
                rows = results or []
        total = sum(r["cnt"] for r in rows)
        if total != self.no_of_docs:
            self.fail("Descriptor-less members form summed to {0}, expected {1}"
                      .format(total, self.no_of_docs))

    def test_syntax_rejections(self):
        """The grammar-level and rewrite-level exclusions and the working
        escape for each: CLUSTER BY cannot co-occur with GROUP BY (24000), and
        set operations are rejected (INTERSECT 24001, parenthesised UNION ALL
        24000) while a subquery wrapper works. LET is accepted, both as the
        source of the clustering expression and alongside it, and clusters the
        same rows as the equivalent statement without it."""
        w = self._with()
        # CLUSTER BY + GROUP BY in one SELECT block
        self._assert_error(
            ("FROM {0} AS p CLUSTER BY p.vec AS sc {1} GROUP BY p.pk "
             "SELECT sc.cluster_id;").format(self.pts, w),
            expected_code=24000)
        # set operations
        self._assert_error(
            "FROM {0} AS p CLUSTER BY p.vec AS sc {1} SELECT sc.cluster_id AS c "
            "INTERSECT SELECT 1 AS c;".format(self.pts, w),
            expected_code=24001)
        self._assert_error(
            "(FROM {0} AS p CLUSTER BY p.vec AS sc {1} SELECT sc.cluster_id AS c)"
            " UNION ALL (SELECT 1 AS c);".format(self.pts, w),
            expected_code=24000)
        self._run(
            ("SELECT c.cid AS c FROM (FROM {0} AS p CLUSTER BY p.vec AS sc {1} "
             "SELECT sc.cluster_id AS cid) AS c UNION ALL SELECT 1 AS c;"
             ).format(self.pts, w),
            msg="Subquery UNION ALL workaround should succeed")
        # LET, compared against the same clustering without it
        seeded = self._with(seed=42)
        members = ("CLUSTER AS m {0} SELECT sc.cluster_id AS cid, "
                   "array_count(m) AS n ORDER BY cid;").format(seeded)
        baseline = self._cluster_rows(
            "FROM {0} AS p CLUSTER BY p.vec AS sc {1}".format(self.pts, members))

        if not baseline:
            self.fail("The LET baseline clustering returned no rows, so the "
                      "comparisons below cannot distinguish a working LET from "
                      "a broken one")
        for label, stmt in (
                ("LET bound to the clustering expression",
                 "FROM {0} AS p LET x = p.vec CLUSTER BY x AS sc {1}".format(
                     self.pts, members)),
                ("LET alongside the clustering expression",
                 "FROM {0} AS p LET z = p.a + 1 CLUSTER BY p.vec AS sc "
                 "{1}".format(self.pts, members)),
                ("LET inside a subquery",
                 "FROM (FROM {0} AS p LET z = p.a + 1 SELECT VALUE p) AS q "
                 "CLUSTER BY q.vec AS sc {1}".format(self.pts, members))):
            ok, _, detail = self._result_drift(
                self._cluster_rows(stmt), baseline)
            if not ok:
                self.fail("{0} clustered differently from the same statement "
                          "without LET: {1}".format(label, detail))

    def test_select_distinct_collapses(self):
        """DISTINCT collapses cluster rows. Every blob holds the same number of
        docs, so all clusters report an identical array_count(members) and
        DISTINCT reduces the per-cluster rows to one."""

        if self.num_clusters != self.centers:
            self.fail(
                "This case needs num_clusters ({0}) == num_centers ({1}); with "
                "unequal values the clusters hold different counts and DISTINCT "
                "cannot collapse to one row".format(
                    self.num_clusters, self.centers))
        def counts(qualifier):
            return self._cluster_rows(self._members_stmt(
                self.pts, projection="{0} array_count(members) AS cnt".format(
                    qualifier)))

        plain = counts("")
        distinct = counts("DISTINCT")
        if len(plain) != self.num_clusters:
            self.fail("Expected {0} plain rows, got {1}".format(
                self.num_clusters, len(plain)))
        if len(distinct) != 1:
            self.fail("Expected DISTINCT to collapse to 1 row, got {0}: {1}"
                      .format(len(distinct), distinct))

    def test_clustering_expr_single_not_list(self):
        """The clustering expression is a single expression: a comma list does
        not parse (24000), while `(a, b)` is an array constructor and `(vec)` is
        a parenthesised expression - both execute."""
        self._assert_error(
            "FROM {0} AS p CLUSTER BY p.a, p.b AS sc {1} SELECT sc.cluster_id;"
            .format(self.pts, self._with()), expected_code=24000)
        self._run(
            ("FROM {0} AS p CLUSTER BY (p.a, p.b) AS sc {1} "
             "SELECT sc.cluster_id;").format(
                 self.pts, self._with(dimension=[2])),
            msg="(a,b) array-constructor form should succeed")
        self._run(
            ("FROM {0} AS p CLUSTER BY (p.vec) AS sc {1} "
             "SELECT sc.cluster_id;").format(self.pts, self._with()),
            msg="(vec) parenthesised form should succeed")

    def test_cluster_by_in_view_and_cte(self):
        """CLUSTER BY works inside a VIEW body and inside a CTE, and both return
        the same clustering. The CTE is the parser-disambiguation case: SQL++
        `WITH name AS (...)` and CLUSTER BY's `WITH {...}` in one statement."""
        body = ("FROM {0} AS p CLUSTER BY p.vec AS sc CLUSTER AS members {1} "
                "SELECT sc.cluster_id AS cid, array_count(members) AS cnt"
                ).format(self.pts, self._with())
        view = "cb_cluster_view"
        self._run("DROP VIEW {0} IF EXISTS;".format(view), fail_on_error=False)
        self._run("CREATE VIEW {0} AS {1};".format(view, body),
                  msg="CREATE VIEW with a CLUSTER BY body should succeed")
        self._register_ddl_cleanup("DROP VIEW {0} IF EXISTS;".format(view))
        view_rows = self._cluster_rows(
            "SELECT c.* FROM {0} AS c ORDER BY c.cid;".format(view))
        cte_rows = self._cluster_rows(
            "WITH c AS ({0}) SELECT c.* FROM c ORDER BY c.cid;".format(body))
        for label, rows in (("view", view_rows), ("cte", cte_rows)):
            total = sum(r["cnt"] for r in rows)
            if total != self.no_of_docs:
                self.fail("{0} form summed to {1}, expected {2}".format(
                    label, total, self.no_of_docs))
        if self._norm(view_rows) != self._norm(cte_rows):
            self.fail("VIEW and CTE clustering rows differ: {0} vs {1}".format(
                view_rows, cte_rows))

    # ======================================================================
    #  Category: WITH options
    # ======================================================================
    def _select_cid(self, with_block, coll=None):
        return "FROM {0} AS p CLUSTER BY p.vec AS sc {1} SELECT sc.cluster_id;" \
            .format(coll or self.pts, with_block)

    def test_with_option_validation(self):
        """Validation of every CLUSTER BY WITH option. Each case names itself in
        the failure because _assert_error echoes the rejected statement.

        num_clusters   - required, positive integer, at most 65536
        dimension      - required, a one-element array of positive integers
        clustering_algorithm - required, K-Means only
        similarity     - non-euclidean values rejected
        unknown key    - rejected
        cross_pollination - only false accepted; the removed
                         cross_pollination_distance_ratio is an unknown option
        """
        # --- num_clusters
        self._assert_error(
            self._select_cid(self._with(drop=["num_clusters"])),
            expected_code=24001, expected_msg="requires the 'num_clusters'")
        for bad in (0, -3):
            self._assert_error(
                self._select_cid(self._with(num_clusters=bad)),
                expected_code=24001,
                expected_msg="'num_clusters' must be a positive integer")
        self._assert_error(
            self._select_cid(self._with(num_clusters="five")),
            expected_code=24001)

        self._run(self._select_cid(self._with(num_clusters=65536)),
                  msg="num_clusters at the 65536 cap should succeed",
                  timeout=600, analytics_timeout=600)
        for over in (65537, 2147483647):
            self._assert_error(
                self._select_cid(self._with(num_clusters=over)),
                expected_code=24001,
                expected_msg="'num_clusters' must be at most 65536")
        self._assert_error(
            self._select_cid(self._with(num_clusters=9999999999999)),
            expected_code=24001,
            expected_msg="'num_clusters' must be a positive integer")

        # --- dimension
        self._assert_error(
            self._select_cid(self._with(drop=["dimension"])),
            expected_code=24001, expected_msg="requires the 'dimension' option")
        for dim, msg in (
                ([0], "'dimension' must be a positive integer, but was: 0"),
                ([-4], "'dimension' must be a positive integer, but was: -4"),
                ([4, 4], "'dimension' must hold exactly one element"),
                (4, "'dimension' must be an array of positive integers")):
            self._assert_error(
                self._select_cid(self._with(dimension=dim)),
                expected_code=24001, expected_msg=msg)

        # --- clustering_algorithm
        self._run(self._select_cid(self._with(algorithm="K-Means")),
                  msg="clustering_algorithm K-Means should succeed")
        self._assert_error(
            self._select_cid(self._with(algorithm=None)),
            expected_code=24001,
            expected_msg="requires the 'clustering_algorithm'")
        self._assert_error(
            self._select_cid(self._with(algorithm="DBSCAN")),
            expected_code=24001, expected_msg="Supported: K-Means")

        # --- similarity (negative half; the accepted family has its own case)
        for sim in ("cosine", "dot", "manhattan", "euclidian"):
            self._assert_error(
                self._select_cid(self._with(similarity=sim)),
                expected_code=24001,
                expected_msg="is not supported. Supported: EUCLIDEAN, "
                             "EUCLIDEAN_SQUARED.")
        self._assert_error(
            self._select_cid(self._with(extra={"Foo": "1"})),
            expected_code=24001, expected_msg="Unknown CLUSTER BY option")

        # --- cross_pollination
        self._assert_error(
            self._select_cid(self._with(cross_pollination=True)),
            expected_code=24001, expected_msg="not enabled")
        self._assert_error(
            self._select_cid(
                self._with(extra={"cross_pollination_distance_ratio": "1.5"})),
            expected_code=24001)
        for bad in ('"yes"', "1"):
            self._assert_error(
                self._select_cid(self._with(extra={"cross_pollination": bad})),
                expected_code=24001, expected_msg="must be true or false")
        total, _ = self._sum_members(self.pts, cross_pollination=False)
        if total != self.no_of_docs:
            self.fail("Explicit cross_pollination:false should cluster "
                      "disjointly: sum(members)={0}, expected {1}".format(
                          total, self.no_of_docs))

    def test_num_iterations(self):
        """The num_iterations WITH option: rejection of non-positive and
        non-integer values, a default of 3 when absent, an observable effect on
        the clustering for values up to 20, and a silent clamp to 20 above that.

        Uses a uniform cloud with random init so the refinement has not
        converged by iteration 3.
        """
        coll = self._create_aux_collection(
            "cb_iters", self._mkuniform(2000, self.dim, seed=7))
        k = 12

        def cluster(num_iterations=None):
            return self._cluster_rows(
                self._members_stmt(
                    coll,
                    projection="sc.cluster_id AS cid, sc.centroid AS centroid, "
                               "array_count(members) AS cnt",
                    suffix="ORDER BY cid", num_clusters=k, seed=42,
                    init_mode="random", num_iterations=num_iterations),
                timeout=600, analytics_timeout=600)

        def same(rows_a, rows_b):
            ok, _, detail = self._result_drift(rows_a, rows_b)
            return ok, detail

        for bad in (0, -1, 2.5, "three", True):
            self._assert_error(
                self._select_cid(self._with(num_iterations=bad), coll=coll),
                expected_code=24001,
                expected_msg="'num_iterations' must be a positive integer")

        one, three, nineteen, twenty = (cluster(1), cluster(3),
                                        cluster(19), cluster(20))

        effect, _ = same(one, twenty)
        if effect:
            self.fail("num_iterations made no difference between 1 and 20 - "
                      "the fixture converged, so this case cannot distinguish "
                      "a working option from an ignored one.")

        default_ok, detail = same(cluster(), three)
        if not default_ok:
            self.fail("Absent num_iterations did not match an explicit 3: "
                      "{0}".format(detail))

        near_cap, _ = same(nineteen, twenty)
        if near_cap:
            self.fail("num_iterations 19 and 20 produced the same clustering, "
                      "so the count stops taking effect below the cap")

        for above in (21, 100, 2147483647):
            clamped, detail = same(cluster(above), twenty)
            if not clamped:
                self.fail("num_iterations {0} was not clamped to 20: {1}"
                          .format(above, detail))

        self.log.info("num_iterations: default 3 confirmed, effect observed "
                      "through 20, values 21/100/2147483647 clamped to 20")

    def test_similarity_euclidean_family(self):
        """The accepted similarity values - euclidean, l2, euclidean_squared,
        l2_squared - all execute."""
        for sim in ("euclidean", "l2", "euclidean_squared", "l2_squared"):
            self._run(self._select_cid(self._with(similarity=sim)),
                      msg="similarity '{0}' should succeed".format(sim))

    def test_seed_determinism(self):
        """Determinism of the clustering, over both seed paths.

        EXPLICIT seed - the same seed over the same data and topology yields the
        same clustering across two runs, a different seed still assigns every
        row, and a non-integer or >32-bit seed is rejected (24001).

        DEFAULT seed (no seed option) - three runs on a fixed topology produce
        an identical PARTITION, so the default is a fixed constant rather than
        clock- or random-derived.

        Compare the partition exactly and the centroids to a tolerance. Assert
        only on a fixed topology: a rebalance legitimately re-partitions.
        """
        tol = float(self.input.param("centroid_tol", 1e-9))

        q = ("FROM {0} AS p CLUSTER BY p.vec AS sc {1} "
             "SELECT sc.cluster_id AS cid, sc.centroid AS centroid "
             "ORDER BY cid;").format(self.pts, self._with(seed=12345))
        run_a, run_b = self._cluster_rows(q), self._cluster_rows(q)
        if not run_a:
            self.fail("Explicit seed: the clustering returned no rows, so the "
                      "determinism comparison would hold vacuously")
        ok, worst, detail = self._result_drift(run_a, run_b, tol=tol)
        if not ok:
            self.fail("Explicit seed: two runs must give the same clustering: "
                      "{0}".format(detail))
        self.log.info("explicit seed: 2 runs identical, worst centroid drift "
                      "{0:g}".format(worst))
        total, _ = self._sum_members(self.pts, seed=999)
        if total != self.no_of_docs:
            self.fail("Explicit seed 999 broke sum(members)=={0}, got {1}"
                      .format(self.no_of_docs, total))
        for bad in ("abc", 99999999999):
            self._assert_error(
                self._select_cid(self._with(seed=bad)),
                expected_code=24001,
                expected_msg="'seed' must be a 32-bit integer")

        stmt = self._members_stmt(
            self.pts,
            projection="sc.cluster_id AS cid, sc.centroid AS centroid, "
                       "(SELECT VALUE m.p.pk FROM members AS m) AS pks",
            suffix="ORDER BY cid")
        runs = [self._cluster_rows(stmt) for _ in range(3)]
        base = runs[0]
        if not base:
            self.fail("Default-seed run returned no clusters")
        base_part = {r["cid"]: sorted(r["pks"]) for r in base}
        worst = 0.0
        for idx, rows in enumerate(runs[1:], start=2):
            if len(rows) != len(base):
                self.fail("Default seed: run {0} returned {1} clusters, run 1 "
                          "returned {2}".format(idx, len(rows), len(base)))
            part = {r["cid"]: sorted(r["pks"]) for r in rows}
            if part != base_part:
                moved = [cid for cid in base_part
                         if base_part.get(cid) != part.get(cid)]
                self.fail(
                    "Default seed: clustering is NOT reproducible on a fixed "
                    "topology - run 1 vs run {0} assign different members. "
                    "Clusters differing: {1}; sizes run1={2} run{0}={3}".format(
                        idx, moved,
                        {c: len(v) for c, v in base_part.items()},
                        {c: len(v) for c, v in part.items()}))
            for b_row, r_row in zip(base, rows):
                for j, (a, b) in enumerate(zip(b_row["centroid"],
                                               r_row["centroid"])):
                    delta = abs(a - b)
                    worst = max(worst, delta)
                    if delta > tol * max(1.0, abs(a)):
                        self.fail(
                            "Default seed: centroid drifted beyond float noise "
                            "- cluster {0} component {1} = {2!r} (run 1) vs "
                            "{3!r} (run {4}), |diff|={5:g} > tol".format(
                                b_row["cid"], j, a, b, idx, delta))
        total = sum(len(r["pks"]) for r in base)
        if total != self.no_of_docs:
            self.fail("Default seed: run lost rows, sum(members)={0} != {1}"
                      .format(total, self.no_of_docs))
        self.log.info(
            "default seed: 3 runs, partition identical, worst centroid drift "
            "{0:g} (tolerance {1:g})".format(worst, tol))

    def test_init_mode_values_and_effect(self):
        """init_mode kmeans_parallel (default), random and kmeanspp are all
        accepted and each assigns every input row; an unknown mode is rejected
        (24001). In the profile the recluster operator is present under the
        default and absent under random, while KMeansLloyd runs under both."""
        for mode in (None, "random", "kmeanspp"):
            total, _ = self._sum_members(self.pts, init_mode=mode)
            if total != self.no_of_docs:
                self.fail("init_mode {0} broke sum(members)=={1}, got {2}"
                          .format(mode, self.no_of_docs, total))
        self._assert_error(
            self._select_cid(self._with(init_mode="bogus")),
            expected_code=24001)
        default_prof = self._profile_of(self._select_cid(self._with()))
        random_prof = self._profile_of(
            self._select_cid(self._with(init_mode="random")))
        if not self._has_operator(default_prof, "KMeansLloyd"):
            self.fail("KMeansLloyd operator missing under default init")
        if not self._has_operator(random_prof, "KMeansLloyd"):
            self.fail("KMeansLloyd operator missing under init_mode:random")
        if not self._has_operator(default_prof, "KMeansRecluster"):
            self.fail("KMeansRecluster expected under default (kmeans_parallel)")
        if self._has_operator(random_prof, "KMeansRecluster"):
            self.fail("KMeansRecluster should be ABSENT under init_mode:random")

    # ======================================================================
    #  Category: CLUSTER BY semantics
    # ======================================================================
    def test_disjoint_assignment_array_count(self):
        """Each embedding lands in exactly one cluster: array_count(members)
        equals an independent per-cluster count, and the sum over clusters
        equals the input row count."""
        stmt = self._members_stmt(
            self.pts,
            projection="sc.cluster_id AS cid, array_count(members) AS cnt, "
                       "(SELECT VALUE count(*) FROM members AS m)[0] AS cnt2",
            suffix="ORDER BY cid")
        rows = self._cluster_rows(stmt)
        if not rows:
            self.fail("CLUSTER BY returned no clusters")
        total = 0
        for r in rows:
            if r["cnt"] != r["cnt2"]:
                self.fail("array_count(members)={0} != independent count {1} "
                          "for cluster {2}".format(r["cnt"], r["cnt2"], r["cid"]))
            total += r["cnt"]
        if total != self.no_of_docs:
            self.fail("sum(members)={0}, expected {1} (disjoint assignment "
                      "invariant)".format(total, self.no_of_docs))

    def test_k_clusters_purity(self):
        """K well-separated blobs with num_clusters=K produce K pure clusters:
        each cluster's members share one true blob id and the clusters map 1:1
        onto the blobs."""
        stmt = self._members_stmt(
            self.pts,
            projection="sc.cluster_id AS cid, "
                       "(SELECT VALUE m.p.cid_true FROM members AS m) AS trues",
            suffix="ORDER BY cid", num_clusters=self.centers)
        rows = self._cluster_rows(stmt)
        if len(rows) != self.centers:
            self.fail("Expected {0} clusters for separable data, got {1}".format(
                self.centers, len(rows)))
        dominant = set()
        for r in rows:
            trues = set(r["trues"])
            if len(trues) != 1:
                self.fail("Cluster {0} is impure (mixed true blobs): {1}".format(
                    r["cid"], trues))
            dominant |= trues
        if len(dominant) != self.centers:
            self.fail("Clusters did not map 1:1 to the {0} true blobs: {1}"
                      .format(self.centers, dominant))

    def test_assignment_correct(self):
        """Every member's nearest RETURNED centroid, recomputed independently in
        Python as a squared-euclidean argmin, equals the cluster_id the engine
        assigned it. euclidean and euclidean_squared then produce an identical
        partition under the same seed, since argmin is invariant under
        squaring."""
        def cluster_partition(similarity):
            rows = self._cluster_rows(self._members_stmt(
                self.pts,
                projection="sc.cluster_id AS cid, sc.centroid AS centroid, "
                           "(SELECT m.p.pk AS pk, m.p.vec AS vec FROM members "
                           "AS m) AS mem",
                suffix="ORDER BY cid", similarity=similarity, seed=42))
            if not rows:
                self.fail("similarity={0}: no clusters returned".format(
                    similarity))
            centroids = {r["cid"]: r["centroid"] for r in rows}
            for r in rows:
                for m in r["mem"]:
                    nearest = self._argmin_centroid(m["vec"], centroids)
                    if nearest != r["cid"]:
                        self.fail(
                            "similarity={0}: member pk={1} is in cluster {2} but "
                            "its nearest returned centroid is {3} (engine "
                            "assignment disagrees with recomputed argmin)".format(
                                similarity, m["pk"], r["cid"], nearest))
            return frozenset(
                frozenset(m["pk"] for m in r["mem"]) for r in rows)

        p_euclid = cluster_partition("euclidean")
        p_squared = cluster_partition("euclidean_squared")
        if p_euclid != p_squared:
            self.fail(
                "euclidean and euclidean_squared produced DIFFERENT partitions "
                "with the same seed (expected identical - argmin is invariant "
                "under squaring): euclidean={0} clusters, euclidean_squared={1} "
                "clusters; symmetric-diff size={2}".format(
                    len(p_euclid), len(p_squared),
                    len(p_euclid ^ p_squared)))

    def test_centroid_equals_member_mean(self):
        """The returned centroid is the component-wise arithmetic mean of its
        own members, and every component is a finite number. A centroid that is
        stale, mis-scaled, un-normalised or NaN is caught here.

        The fixture is well-separated blobs, so the assignment is stable from
        the first iteration and a converged k-means must return the member
        mean."""
        tol_rel = float(self.input.param("centroid_mean_tol", 1e-6))
        rows = self._cluster_rows(self._members_stmt(
            self.pts,
            projection="sc.cluster_id AS cid, sc.centroid AS centroid, "
                       "(SELECT VALUE m.p.vec FROM members AS m) AS vecs",
            suffix="ORDER BY cid", seed=42))
        if not rows:
            self.fail("CLUSTER BY returned no clusters - nothing to validate")
        worst = 0.0
        for r in rows:
            cid, centroid, vecs = r["cid"], r["centroid"], r["vecs"]
            if not vecs:
                self.fail("Cluster {0} has an empty member list but was still "
                          "returned as a descriptor".format(cid))
            if len(centroid) != self.dim:
                self.fail("Cluster {0}: centroid length {1} != dimension {2}"
                          .format(cid, len(centroid), self.dim))
            for j, comp in enumerate(centroid):
                if isinstance(comp, bool) or not isinstance(comp, (int, float)):
                    self.fail("Cluster {0}: centroid[{1}]={2!r} is not a number"
                              .format(cid, j, comp))
                if comp != comp or comp in (float("inf"), float("-inf")):
                    self.fail("Cluster {0}: centroid[{1}]={2!r} is not finite"
                              .format(cid, j, comp))
            expected = [sum(v[j] for v in vecs) / float(len(vecs))
                        for j in range(self.dim)]
            for j, (got, want) in enumerate(zip(centroid, expected)):
                tol = tol_rel * max(1.0, abs(want))
                worst = max(worst, abs(got - want))
                if abs(got - want) > tol:
                    self.fail(
                        "Cluster {0} (n={1}): centroid[{2}]={3!r} != member mean "
                        "{4!r} (|diff|={5:g} > tol={6:g}) - the centroid is not "
                        "the mean of its members".format(
                            cid, len(vecs), j, got, want, abs(got - want), tol))
        self.log.info("centroid == member mean: {0} clusters, worst |diff|="
                      "{1:g} (relative tolerance {2:g})".format(
                          len(rows), worst, tol_rel))

    def test_num_clusters_boundaries(self):
        """num_clusters=1 returns one all-inclusive cluster. num_clusters above
        the input row count succeeds with at least one and at most #rows
        non-empty clusters, rather than erroring or degenerating."""
        rows = self._cluster_rows(
            self._members_stmt(self.pts, num_clusters=1))
        if len(rows) != 1 or rows[0]["cnt"] != self.no_of_docs:
            self.fail("K=1 should give one cluster with cnt=={0}, got {1}"
                      .format(self.no_of_docs, rows))
        n_tiny = self.input.param("tiny_docs", 3)
        tiny = self._create_aux_collection(
            "cb_tiny", self._mkvec(n_tiny, self.dim, 1))
        rows = self._cluster_rows(
            "FROM {0} AS p CLUSTER BY p.vec AS sc {1} SELECT sc.cluster_id "
            "AS cid;".format(tiny, self._with(num_clusters=n_tiny + 2)))
        if not 1 <= len(rows) <= n_tiny:
            self.fail("K>rows returned {0} clusters over {1} rows, expected "
                      "between 1 and {1}".format(len(rows), n_tiny))

    def test_duplicate_points(self):
        """Many byte-identical vectors alongside a second blob cluster without
        divide-by-zero or NaN: every row is assigned and all centroid
        components are finite."""
        n_identical = self.input.param("dupe_identical", 150)
        n_other = self.input.param("dupe_other", 50)
        docs = self._mkvec_dupes(n_identical, n_other, self.dim)
        dupes = self._create_aux_collection("cb_dupes", docs)
        total, _ = self._sum_members(dupes, num_clusters=2)
        if total != len(docs):
            self.fail("Duplicate-points sum(members)={0}, expected {1}".format(
                total, len(docs)))
        rows = self._cluster_rows(
            "FROM {0} AS p CLUSTER BY p.vec AS sc {1} SELECT sc.centroid AS c;"
            .format(dupes, self._with(num_clusters=2)))
        if not rows:
            self.fail("Duplicate-points clustering returned no clusters")
        for r in rows:
            for v in r["c"]:
                if not isinstance(v, (int, float)) or v != v:
                    self.fail("Non-finite centroid element on duplicate data: "
                              "{0}".format(r["c"]))

    def test_empty_input(self):
        """A predicate matching no rows returns [], not an error. Filters on
        p.year, a non-key field, so the case stays independent of the separate
        primary-key predicate path covered by
        test_pk_where_filters_correctly."""
        rows = self._cluster_rows(
            "FROM {0} AS p WHERE p.year < 0 CLUSTER BY p.vec AS sc {1} "
            "SELECT sc.cluster_id;".format(self.pts, self._with()))
        if rows:
            self.fail("Empty input should return [], got {0}".format(rows))

    def test_subquery_bounds_input(self):
        """Whatever the input expression yields is what gets clustered:
        sum(members) equals the subquery's own row count for a plain derived
        table, a subquery WHERE, an ORDER BY..LIMIT and a CTE."""
        n_filtered = self._count_of(
            "{0} AS p WHERE p.year < 2020".format(self.pts))
        shapes = {
            "derived": ("FROM (FROM {0} AS p SELECT VALUE p) AS q", self.no_of_docs),
            "sub_where": ("FROM (FROM {0} AS p WHERE p.year < 2020 SELECT VALUE p)"
                          " AS q", n_filtered),
            "limit": ("FROM (FROM {0} AS p SELECT VALUE p LIMIT 100) AS q", 100),
            "cte": ("WITH src AS (FROM {0} AS p WHERE p.year < 2020 SELECT VALUE "
                    "p) FROM src AS q", n_filtered),
        }
        for label, (prefix, expected) in shapes.items():
            stmt = (prefix + " CLUSTER BY q.vec AS sc CLUSTER AS members {1} "
                    "SELECT array_count(members) AS cnt;").format(
                        self.pts, self._with())
            rows = self._cluster_rows(stmt)
            total = sum(r["cnt"] for r in rows)
            if total != expected:
                self.fail("{0}: sum(members)={1}, expected subquery row count {2}"
                          .format(label, total, expected))

    # ======================================================================
    #  Category: CLUSTER AS retention and cluster output shape
    # ======================================================================
    def test_members_retain_bindings(self):
        """CLUSTER AS keeps one field per FROM binding for each member, so a
        bare single-collection query exposes exactly ["p"]."""
        rows = self._cluster_rows(self._members_stmt(
            self.pts,
            projection="sc.cluster_id AS cid, (SELECT VALUE object_names(m) "
                       "FROM members AS m LIMIT 1)[0] AS f",
            suffix="ORDER BY cid"))
        self._assert_member_bindings(rows, ["p"], "single-collection CLUSTER AS")

    def test_cluster_as_optional_unprojectable(self):
        """CLUSTER AS may be omitted - the descriptor-only query runs and
        returns one row per cluster - but referencing a members variable that
        was never bound is an error naming that identifier."""
        rows = self._cluster_rows(
            "FROM {0} AS p CLUSTER BY p.vec AS sc {1} "
            "SELECT sc.cluster_id AS cid, sc.centroid AS centroid;".format(
                self.pts, self._with()))
        if len(rows) != self.num_clusters:
            self.fail("Descriptor-only query should return one row per cluster,"
                      " got {0} rows for num_clusters={1}".format(
                          len(rows), self.num_clusters))
        self._assert_error(
            "FROM {0} AS p CLUSTER BY p.vec AS sc {1} "
            "SELECT array_count(members);".format(self.pts, self._with()),
            expected_msg="members")

    def test_descriptor_contract(self):
        """The descriptor exposes exactly cluster_id and centroid and nothing
        else: the whole record projects to those two names, both fields project
        individually, an unknown sub-field resolves to MISSING rather than
        erroring, cluster_id is unique per cluster, and centroid holds
        `dimension` elements."""
        w = self._with()
        name_rows = self._cluster_rows(
            "FROM {0} AS p CLUSTER BY p.vec AS sc {1} "
            "SELECT VALUE object_names(sc);".format(self.pts, w))
        if not name_rows:
            self.fail("Descriptor projection returned no rows")
        for names in name_rows:
            if sorted(names) != ["centroid", "cluster_id"]:
                self.fail("Descriptor must expose exactly cluster_id+centroid, "
                          "got {0}".format(names))
        for proj in ("foo", "cluster_radius"):
            rows = self._cluster_rows(
                "FROM {0} AS p CLUSTER BY p.vec AS sc {1} "
                "SELECT sc.{2} AS v;".format(self.pts, w, proj))
            if not rows:
                self.fail("Unknown-field projection sc.{0} returned no rows"
                          .format(proj))
            if any("v" in row for row in rows):
                self.fail("Unknown descriptor field sc.{0} should be MISSING, "
                          "got {1}".format(proj, rows))
        rows = self._cluster_rows(
            "FROM {0} AS p CLUSTER BY p.vec AS sc {1} "
            "SELECT sc.cluster_id AS cid, array_count(sc.centroid) AS dim "
            "ORDER BY cid;".format(self.pts, w))
        cids = [r["cid"] for r in rows]
        if not cids:
            self.fail("Descriptor projection returned no clusters")
        if len(cids) != len(set(cids)):
            self.fail("cluster_id values are not unique: {0}".format(cids))
        for r in rows:
            if r["dim"] != self.dim:
                self.fail("centroid length {0} != dimension {1}".format(
                    r["dim"], self.dim))

    # ======================================================================
    #  Category: Confirmed defects (assertions flip when the fix lands)
    # ======================================================================
    def _count_clusters(self, coll, num_clusters, seed=42, init_mode=None):
        rows = self._cluster_rows(
            "FROM {0} AS p CLUSTER BY p.vec AS sc {1} SELECT sc.cluster_id AS "
            "cid ORDER BY cid;".format(
                coll, self._with(num_clusters=num_clusters, seed=seed,
                                 init_mode=init_mode)))
        return len(rows)

    def test_default_init_under_clusters_duplicates(self):
        """With exact-duplicate vectors both initialisations return fewer
        clusters than num_clusters, and init_mode:random returns at least as
        many as the default across a sweep of seeds.

        KNOWN BUG - this asserts the current buggy behaviour. Flip the
        assertion when the fix lands: both modes should then return exactly m.

        Compare distributions across seeds, never a single draw: random init
        samples m rows from data holding only m distinct vectors, so it often
        draws the same vector twice and the cluster count varies by seed.
        """
        seeds = [1, 7, 13, 42, 99, 123, 777, 2024]
        for m in (2, 3, 6):
            coll = self._create_aux_collection(
                "cb_uc{0}".format(m), self._mk_exact_dupes(m))
            default_n = [self._count_clusters(coll, m, seed=s) for s in seeds]
            random_n = [self._count_clusters(coll, m, seed=s,
                                             init_mode="random")
                        for s in seeds]
            self.log.info("under-clustering m={0}: default={1} random={2}"
                          .format(m, default_n, random_n))

            over = [n for n in default_n + random_n if n > m]
            if over:
                self.fail("m={0}: a run returned MORE than {0} clusters ({1}) "
                          "- num_clusters is an upper bound".format(m, over))
            if all(n == m for n in default_n):
                self.fail("m={0}: default init returned exactly {0} clusters "
                          "for every seed ({1}) - the under-clustering bug no "
                          "longer reproduces, flip this assertion".format(
                              m, default_n))
            if sum(random_n) < sum(default_n):
                self.fail("m={0}: init_mode:random ({1}) clustered worse than "
                          "the default ({2})".format(m, random_n, default_n))

    def test_nondeterministic_input_single_scan(self):
        """A non-deterministic clustering input (random()) in a block WHERE is
        read by exactly ONE source scan, so training and assignment observe the
        same rows within a query. The subquery-fed form is likewise a single
        scan, and a deterministic input under a fixed seed is reproducible run
        to run.

        REGRESSION GUARD: two scans here means the double-read has returned."""
        w = self._with(seed=777)
        for label, stmt in (
                ("block WHERE random()",
                 self._members_stmt(
                     from_clause="{0} AS p WHERE random() < 0.5".format(
                         self.pts), seed=777)),
                ("subquery-fed random()",
                 "FROM (FROM {0} AS p WHERE random() < 0.5 SELECT VALUE p) AS q "
                 "CLUSTER BY q.vec AS sc {1} SELECT sc.cluster_id;".format(
                     self.pts, w))):
            scans = self._scan_totals(self._profile_of(stmt))
            if len(scans) != 1:
                self.fail("{0}: expected exactly ONE source scan, got {1} ({2}). "
                          "A second scan means the non-deterministic input is "
                          "evaluated twice again.".format(
                              label, len(scans), scans))
            self.log.info("{0}: one scan {1}".format(label, scans))
        det = ("FROM {0} AS p WHERE p.grp < 2 CLUSTER BY p.vec AS sc {1} "
               "SELECT sc.cluster_id AS cid, sc.centroid AS centroid "
               "ORDER BY cid;").format(self.pts, w)
        ok, worst, detail = self._result_drift(self._cluster_rows(det),
                                               self._cluster_rows(det))
        if not ok:
            self.fail("Deterministic input with a fixed seed must be "
                      "reproducible run-to-run: {0}".format(detail))
        self.log.info("deterministic control: 2 runs identical, worst centroid "
                      "drift {0:g}".format(worst))

    # ======================================================================
    #  Category: Database designs + PK-WHERE defect
    # ======================================================================
    def test_single_collection_where_nonkey(self):
        """A plain FROM with a non-key WHERE reduces the input before centroids
        are trained: only qualifying rows are clustered and their count equals
        sum(members). Filters on p.year, a non-key field; the primary-key
        predicate path is covered separately by
        test_pk_where_filters_correctly."""
        expected = self._count_of(
            "{0} AS p WHERE p.year < 2020".format(self.pts))
        rows = self._cluster_rows(self._members_stmt(
            from_clause="{0} AS p WHERE p.year < 2020".format(self.pts),
            projection="sc.cluster_id AS cid, array_count(members) AS cnt"))
        total = sum(r["cnt"] for r in rows)
        if total != expected:
            self.fail("WHERE-filtered sum(members)={0}, expected qualifying "
                      "count {1}".format(total, expected))

    def test_pk_where_filters_correctly(self):
        """A block WHERE on the PRIMARY KEY filters correctly: the query
        succeeds and sum(members) equals an independent count of the qualifying
        rows, for range, equality and IN predicates. The subquery form yields
        the same total.

        REGRESSION GUARD: a non-success status here means the pk-predicate
        25000 Internal error has returned."""
        for pred in ("p.pk < 100", "p.pk >= 400", "p.pk = 7"):
            expected = self._count_of("{0} AS p WHERE {1}".format(
                self.pts, pred))
            rows = self._cluster_rows(self._members_stmt(
                from_clause="{0} AS p WHERE {1}".format(self.pts, pred)))
            total = sum(r["cnt"] for r in rows)
            if total != expected:
                self.fail("WHERE {0}: sum(members)={1}, expected qualifying "
                          "count {2}".format(pred, total, expected))
            self.log.info("pk predicate '{0}': {1} rows clustered".format(
                pred, total))
        sub = self._cluster_rows(self._members_stmt(
            from_clause="(FROM {0} AS p WHERE p.pk < 100 SELECT VALUE p) AS "
                        "q".format(self.pts),
            vec_expr="q.vec"))
        sub_total = sum(r["cnt"] for r in sub)
        if sub_total != 100:
            self.fail("Subquery pk-filter clustered {0} rows, expected 100"
                      .format(sub_total))

    def test_join_unnest_designs(self):
        """The three multi-binding FROM shapes, over one shared fixture set.

        JOIN          - two collections joined on movie_id with a predicate
                        spanning both; only qualifying joined rows are
                        clustered and members retain both bindings.
        UNNEST        - a nested review array unnested and clustered per
                        review; members retain both bindings.
        OUTER         - LEFT OUTER JOIN and LEFT OUTER UNNEST are rejected,
                        since either can leave the clustering expression
                        MISSING.
        COMBINE       - CLUSTER BY in a subquery composes with an outer
                        GROUP BY.

        Each assertion names its shape, so a failure identifies the case.
        """
        n_movies = self.input.param("join_movies", 5)
        reviews_per = self.input.param("join_reviews_per_movie", 3)
        n_nested = self.input.param("unnest_movies", 10)

        movies, reviews = self._mk_movies_reviews(n_movies, reviews_per)
        m_coll = self._create_aux_collection(
            "cb_movies", movies, primary_key="movie_id: bigint")
        r_coll = self._create_aux_collection(
            "cb_reviews", reviews, primary_key="review_id: bigint")
        nested = self._mk_movies_nested_reviews(n_nested, reviews_per)
        n_coll = self._create_aux_collection(
            "cb_nested_movies", nested, primary_key="movie_id: bigint")

        # --- JOIN: movies with an even movie_id carry movie_year 2015 (<2020)
        expected_join = len([m for m in movies
                             if m["movie_year"] < 2020]) * reviews_per
        rows = self._cluster_rows(self._members_stmt(
            from_clause="{0} m, {1} r WHERE r.movie_id = m.movie_id AND "
                        "m.movie_year < 2020".format(m_coll, r_coll),
            vec_expr="r.vec",
            projection="sc.cluster_id AS cid, array_count(members) AS cnt, "
                       "(SELECT VALUE object_names(x) FROM members AS x "
                       "LIMIT 1)[0] AS f"))
        total = sum(r["cnt"] for r in rows)
        if total != expected_join:
            self.fail("JOIN: sum(members)={0}, expected {1}".format(
                total, expected_join))
        self._assert_member_bindings(rows, ["m", "r"], "JOIN")

        # --- UNNEST
        expected_unnest = n_nested * reviews_per
        rows = self._cluster_rows(self._members_stmt(
            from_clause="{0} m, m.reviews r".format(n_coll),
            vec_expr="r.vec",
            projection="sc.cluster_id AS cid, array_count(members) AS cnt, "
                       "(SELECT VALUE object_names(x) FROM members AS x "
                       "LIMIT 1)[0] AS f"))
        total = sum(r["cnt"] for r in rows)
        if total != expected_unnest:
            self.fail("UNNEST: sum(members)={0}, expected {1}".format(
                total, expected_unnest))
        self._assert_member_bindings(rows, ["m", "r"], "UNNEST")

        # --- OUTER join / outer UNNEST are rejected
        self._assert_error(
            "FROM {0} m LEFT OUTER JOIN {1} r ON r.movie_id = m.movie_id "
            "CLUSTER BY r.vec AS sc {2} SELECT sc.cluster_id;".format(
                m_coll, r_coll, self._with()),
            expected_msg="inner join")
        self._assert_error(
            "FROM {0} m LEFT OUTER UNNEST m.reviews r CLUSTER BY r.vec AS sc "
            "{1} SELECT sc.cluster_id;".format(n_coll, self._with()),
            expected_msg="inner UNNEST")

        # --- COMBINE: CLUSTER BY subquery + outer GROUP BY
        _, _, _, results, _, _ = self._run(
            ("FROM (FROM {0} m, {1} r WHERE r.movie_id = m.movie_id "
             "CLUSTER BY r.vec AS sc CLUSTER AS members {2} "
             "SELECT sc.cluster_id AS cid, members) AS mc, mc.members AS mi "
             "GROUP BY mi.m.movie_year, mc.cid GROUP AS g "
             "SELECT mc.cid AS cid, mi.m.movie_year AS year, "
             "(SELECT DISTINCT VALUE g.mi.m.movie_id FROM g) AS ids;").format(
                 m_coll, r_coll, self._with()),
            msg="COMBINE: CLUSTER-BY subquery + outer GROUP BY should compose")
        if not results:
            self.fail("COMBINE: outer GROUP BY returned no rows, so the output "
                      "shape was never checked")
        for r in results:
            if "cid" not in r or "year" not in r or "ids" not in r:
                self.fail("COMBINE: output row missing cid/year/ids: {0}".format(
                    r))

    # ======================================================================
    #  Category: HAVING / ordering / slicing
    # ======================================================================
    def test_having_on_clusters(self):
        """HAVING on a CLUSTER BY block filters clusters by a predicate over
        the member array. Runs over deliberately unequal clusters so a
        mid-range threshold keeps some and drops others, and checks the
        boundaries: a threshold below the smallest cluster keeps every cluster
        and one above the largest keeps none. The equivalent outer WHERE over a
        subquery returns the same clusters.
        """

        sizes = [10, 25, 60]
        rng = random.Random(42)
        docs, pk = [], 0
        for cid, size in enumerate(sizes):
            for _ in range(size):
                docs.append({"pk": pk,
                             "vec": [self._round(cid * 50 + rng.gauss(0, 0.5))]
                                    + [self._round(rng.gauss(0, 0.5))
                                       for _ in range(self.dim - 1)]})
                pk += 1
        coll = self._create_aux_collection("cb_having", docs)
        w = self._with(num_clusters=len(sizes), seed=42)

        def having(threshold):
            return self._cluster_rows(self._members_stmt(
                coll,
                having="array_count(members) >= {0}".format(threshold),
                projection="sc.cluster_id AS cid, array_count(members) AS cnt",
                suffix="ORDER BY cid",
                num_clusters=len(sizes), seed=42))

        def subquery(threshold):
            return self._cluster_rows(
                "FROM (FROM {0} AS p CLUSTER BY p.vec AS sc CLUSTER AS members "
                "{1} SELECT sc.cluster_id AS cid, array_count(members) AS cnt) "
                "AS c WHERE c.cnt >= {2} SELECT c.cid AS cid, c.cnt AS cnt "
                "ORDER BY cid;".format(coll, w, threshold))

        baseline = self._cluster_counts(coll, num_clusters=len(sizes), seed=42)
        counts = sorted(r["cnt"] for r in baseline)
        if counts != sorted(sizes):
            self.fail("Fixture did not cluster into {0}; got {1}. A mid-range "
                      "threshold cannot split equal clusters, so the filter "
                      "would not be exercised.".format(sorted(sizes), counts))
        smallest, largest = counts[0], counts[-1]
        middle = counts[1]

        for threshold, expected in ((smallest, len(counts)),
                                    (middle, len([c for c in counts
                                                  if c >= middle])),
                                    (largest + 1, 0)):
            rows = having(threshold)
            if len(rows) != expected:
                self.fail("HAVING >= {0} kept {1} cluster(s), expected {2}: "
                          "{3}".format(threshold, len(rows), expected, rows))
            for row in rows:
                if row["cnt"] < threshold:
                    self.fail("HAVING >= {0} returned a cluster below the "
                              "threshold: {1}".format(threshold, row))
            sub_rows = subquery(threshold)
            if self._norm(rows) != self._norm(sub_rows):
                self.fail("HAVING and the subquery WHERE disagreed at "
                          "threshold {0}: {1} vs {2}".format(
                              threshold, rows, sub_rows))

    def test_cluster_ordering_and_slicing(self):
        """ORDER BY and LIMIT/OFFSET over the cluster result stream.

        ORDER BY cluster_id ASC is honoured, ORDER BY member count DESC is
        honoured, and LIMIT 3 OFFSET 1 over the cluster_id ordering returns
        exactly the three clusters following the first.

        Slice against the cluster_id ordering, not the count ordering: the
        fixture blobs are equally sized, so ordering by count is a tie.
        """
        counts_desc = self._cluster_counts(self.pts, suffix="ORDER BY cnt DESC")
        counts = [r["cnt"] for r in counts_desc]
        if counts != sorted(counts, reverse=True):
            self.fail("ORDER BY cnt DESC not honored: {0}".format(counts))

        by_cid = self._cluster_counts(self.pts, suffix="ORDER BY cid")
        cids = [r["cid"] for r in by_cid]
        if cids != sorted(cids):
            self.fail("ORDER BY cid ASC not honored: {0}".format(cids))
        if len(cids) < 4:
            self.fail("Need at least 4 clusters to exercise LIMIT 3 OFFSET 1, "
                      "got {0}".format(len(cids)))

        sliced = self._cluster_counts(
            self.pts, suffix="ORDER BY cid LIMIT 3 OFFSET 1")
        if len(sliced) != 3:
            self.fail("LIMIT 3 OFFSET 1 returned {0} rows, expected 3".format(
                len(sliced)))
        expected = cids[1:4]
        if [r["cid"] for r in sliced] != expected:
            self.fail("LIMIT 3 OFFSET 1 over ORDER BY cid returned {0}, "
                      "expected {1} (the 3 clusters after the first)".format(
                          [r["cid"] for r in sliced], expected))

    # ======================================================================
    #  Category: Materialization and integration
    # ======================================================================
    def test_materialize_insert_into_select(self):
        """CLUSTER BY composes as the source of INSERT INTO ... SELECT: the
        target collection ends up holding exactly one row per cluster the same
        query reports."""
        target = self._create_aux_collection(
            "cb_cluster_out", docs=None, primary_key="cid: bigint")
        source = ("FROM {0} AS p CLUSTER BY p.vec AS sc {1} "
                  "SELECT sc.cluster_id AS cid, sc.centroid AS centroid"
                  ).format(self.pts, self._with())
        expected = len(self._cluster_rows(source + ";"))
        if not expected:
            self.fail("Source CLUSTER BY returned no clusters to materialize")
        self._run("INSERT INTO {0} ({1});".format(target, source),
                  msg="INSERT INTO ... CLUSTER BY SELECT should succeed")
        materialized = self._count_of(target)
        if materialized != expected:
            self.fail("Materialized collection holds {0} rows, expected one per "
                      "cluster ({1})".format(materialized, expected))

    def test_composition(self):
        """CLUSTER BY output feeds further constructs: the centroids can
        themselves be clustered, cluster rows can be ordered by a centroid
        component, and an aggregate over clusters accounts for every input
        row."""
        inner = ("FROM {0} AS p CLUSTER BY p.vec AS sc {1} "
                 "SELECT VALUE sc.centroid").format(self.pts, self._with())
        nested = self._cluster_rows(
            "FROM ({0}) AS c CLUSTER BY c AS sc2 {1} SELECT sc2.cluster_id AS "
            "cid;".format(inner, self._with(num_clusters=2)))
        if not 1 <= len(nested) <= 2:
            self.fail("Nested clustering of centroids returned {0} clusters, "
                      "expected at most 2".format(len(nested)))
        ordered = self._cluster_rows(
            "FROM {0} AS p CLUSTER BY p.vec AS sc {1} "
            "SELECT sc.cluster_id AS cid, sc.centroid[0] AS c0 "
            "ORDER BY sc.centroid[0] DESC;".format(self.pts, self._with()))
        c0s = [r["c0"] for r in ordered]
        if c0s != sorted(c0s, reverse=True):
            self.fail("ORDER BY centroid[0] DESC not honored: {0}".format(c0s))
        agg = self._cluster_rows(
            "FROM (FROM {0} AS p CLUSTER BY p.vec AS sc CLUSTER AS members {1} "
            "SELECT sc.cluster_id AS cid, array_count(members) AS cnt) AS c "
            "SELECT count(*) AS n_clusters, sum(c.cnt) AS total;".format(
                self.pts, self._with()))
        if not agg or agg[0]["total"] != self.no_of_docs:
            self.fail("Aggregate over clusters total={0}, expected {1}".format(
                agg[0]["total"] if agg else None, self.no_of_docs))

    # ======================================================================
    #  Category: Query plan / EXPLAIN
    # ======================================================================
    def test_explain_and_profile_operators(self):
        """EXPLAIN exposes the desugared k-means plan tokens, and a profile
        shows the k-means operators present AND carrying a positive run-time -
        a plan can be right while the runtime silently degrades."""
        stmt = self._members_stmt(
            self.pts, projection="sc.cluster_id, array_count(members)")
        plan = self._explain(stmt).lower()
        for token in ("kmeans_oversample_loop", "kmeans_recluster",
                      "kmeans_lloyd_loop"):
            if token not in plan:
                self.fail("EXPLAIN plan missing desugar token '{0}'. If the "
                          "operator names changed, capture the new tokens from "
                          "a live EXPLAIN and update. Plan head: {1}".format(
                              token, plan[:400]))
        profile = self._profile_of(stmt)
        for op in ("KMeansLloydController", "KMeansCentroidMerge"):
            if not self._has_operator(profile, op):
                self.fail("Profile missing k-means operator '{0}' (did the "
                          "runtime degrade?)".format(op))
        runtimes = self._operator_runtimes(profile)
        kmeans_rts = {name: rt for name, rt in runtimes.items()
                      if "KMeans" in name}
        if not kmeans_rts:
            self.fail("No KMeans operator reported a run-time in the profile")
        if max(kmeans_rts.values()) <= 0:
            self.fail("Every KMeans operator reported a non-positive run-time "
                      "{0} - the operators are in the plan but did not run"
                      .format(kmeans_rts))
        self.log.info("k-means operator run-times: {0}".format(kmeans_rts))

    def test_source_read_count(self):
        """The rewrite reads the source collection ONCE: a single Index Search
        operator id and processedObjects == N, for a bare collection and for a
        CTE-fed input alike. The source double-read (2*N) was collapsed by the
        REPLICATE materialization.

        2*N here means the REPLICATE collapse was lost, which re-opens the
        no-shared-snapshot hazard - investigate before relaxing this case."""
        for label, stmt in (
                ("bare collection",
                 "FROM {0} AS p CLUSTER BY p.vec AS sc {1} SELECT sc.cluster_id;"
                 .format(self.pts, self._with())),
                ("CTE-fed input",
                 "WITH src AS (FROM {0} AS p SELECT VALUE p) FROM src AS q "
                 "CLUSTER BY q.vec AS sc {1} SELECT sc.cluster_id;".format(
                     self.pts, self._with()))):
            content = self._run_raw(stmt, profile="timings")
            if content.get("status") != "success":
                self.fail("{0}: query failed: {1}".format(
                    label, content.get("errors")))
            po = int(content.get("metrics", {}).get("processedObjects", 0))
            scans = self._scan_totals(content.get("profile", {}))
            if po != self.no_of_docs:
                self.fail("{0}: processedObjects={1}, expected N={2}. If it is "
                          "2*N the source double-read is back.".format(
                              label, po, self.no_of_docs))
            if len(scans) != 1:
                self.fail("{0}: expected exactly one Index Search scan, got {1} "
                          "({2}). A second scan re-opens the no-shared-snapshot "
                          "hazard.".format(label, len(scans), scans))
            self.log.info("{0}: one scan {1}, processedObjects={2}".format(
                label, scans, po))

    # ======================================================================
    #  Category: Security / RBAC
    # ======================================================================
    def _create_rbac_user(self, username, roles):
        """Create a builtin user with the given ns_server roles and register it
        for teardown. Analytics roles on EA are cluster-wide (params:null);
        `analytics_access` is the enabling read role."""
        password = self.rbac_password
        self.rbac_util.create_user(
            self.columnar_cluster, username, username, password, roles=roles)
        self._rbac_users.append(username)
        time.sleep(self.input.param("rbac_settle", 5))
        return username, password

    def test_read_privilege_enforced_like_select(self):
        """A user with no analytics read fails a plain SELECT and a CLUSTER BY
        with the same error - code 20001 - never a camouflaged empty success."""
        user, pwd = self._create_rbac_user("cb_noaccess", roles=[])
        for label, stmt in (
                ("plain SELECT",
                 "SELECT VALUE count(*) FROM {0};".format(self.pts)),
                ("CLUSTER BY",
                 "FROM {0} AS p CLUSTER BY p.vec AS sc {1} "
                 "SELECT sc.cluster_id;".format(self.pts, self._with()))):
            code, msg = self._assert_error(stmt, expected_code=20001,
                                           username=user, password=pwd)
            self.log.info("{0} denied with code {1}: {2}".format(
                label, code, msg))

    # ======================================================================
    #  Category: Regression / adjacent
    # ======================================================================
    def test_group_by_unchanged(self):
        """GROUP BY / GROUP AS / HAVING are unaffected. A plain GROUP BY over
        the shared blob data returns the known per-blob counts, and HAVING
        genuinely filters: a threshold at the per-blob count keeps every group
        and a threshold above it keeps none."""
        rows = self._cluster_rows(
            "FROM {0} AS p GROUP BY p.cid_true GROUP AS g "
            "SELECT p.cid_true AS cid, count(*) AS cnt ORDER BY cid;".format(
                self.pts))
        per_blob = self.no_of_docs // self.centers
        if len(rows) != self.centers:
            self.fail("GROUP BY returned {0} groups, expected {1}".format(
                len(rows), self.centers))
        for r in rows:
            if r["cnt"] != per_blob:
                self.fail("GROUP BY count {0} != expected {1} for blob {2}"
                          .format(r["cnt"], per_blob, r["cid"]))

        def having(threshold):
            return self._cluster_rows(
                "FROM {0} AS p GROUP BY p.cid_true HAVING count(*) >= {1} "
                "SELECT p.cid_true AS cid, count(*) AS cnt;".format(
                    self.pts, threshold))

        kept = having(per_blob)
        if len(kept) != self.centers:
            self.fail("GROUP BY + HAVING regressed: threshold {0} should keep "
                      "all {1} groups, kept {2}".format(
                          per_blob, self.centers, len(kept)))
        dropped = having(per_blob + 1)
        if dropped:
            self.fail("GROUP BY + HAVING did not filter: threshold {0} exceeds "
                      "every group's count ({1}) yet {2} group(s) came back: "
                      "{3}".format(per_blob + 1, per_blob, len(dropped),
                                   dropped))

    def test_neither_clause_unaffected(self):
        """Plain queries are unchanged and `cluster` is still usable as an
        identifier (soft keyword)."""
        total = self._count_of(self.pts)
        if total != self.no_of_docs:
            self.fail("Plain COUNT(*) returned {0}, expected {1}".format(
                total, self.no_of_docs))
        rows = self._cluster_rows(
            "FROM [1, 2, 3] AS cluster SELECT VALUE cluster ORDER BY cluster;")
        if rows != [1, 2, 3]:
            self.fail("`cluster` should be usable as an identifier, got {0}"
                      .format(rows))

    def test_vector_index_ann_unaffected(self):
        """A vector index and CLUSTER BY coexist on one collection:

          * an ann_distance top-K query uses the vector index
          * CLUSTER BY over the same collection returns the same clusters as it
            did before the index existed, and does not read through the index
          * ann_distance still uses the index after the clustering query

        Asserts the ann plan only - the top-K values are not compared against a
        non-indexed baseline. The index needs ANALYZE first and the VECTOR
        field annotation with TYPE VTREE.
        """
        coll = self._create_aux_collection(
            "cb_ann", self._mkvec(500, self.dim, self.centers))
        before = self._cluster_counts(coll, num_clusters=self.centers, seed=42)
        if sum(r["cnt"] for r in before) != 500:
            self.fail("Pre-index CLUSTER BY did not cover all 500 rows: "
                      "{0}".format(before))
        self._run("ANALYZE COLLECTION {0};".format(coll),
                  msg="ANALYZE is a precondition for creating a vector index")
        self._run(
            "CREATE INDEX cb_ann_vec_idx IF NOT EXISTS ON {0}(vec VECTOR) "
            "TYPE VTREE WITH {{ 'dimension': {1}, "
            "'similarity': 'L2_SQUARED' }};".format(coll, self.dim),
            msg="Failed to create the vector index",
            timeout=600, analytics_timeout=600)
        self._register_ddl_cleanup(
            "DROP INDEX {0}.cb_ann_vec_idx IF EXISTS;".format(coll))

        query = ("FROM {0} AS p SELECT p.pk ORDER BY ann_distance(p.vec, {1}, "
                 "'L2_SQUARED') LIMIT 5;").format(coll, [0.0] * self.dim)
        rows = self._cluster_rows(query)
        if len(rows) != 5:
            self.fail("ann_distance top-5 returned {0} row(s): {1}".format(
                len(rows), rows))
        plan = self._explain(query).lower()
        if "index-search" not in plan:
            self.fail("ann_distance query did not use the vector index; plan "
                      "head: {0}".format(plan[:400]))

        after = self._cluster_counts(coll, num_clusters=self.centers, seed=42)
        ok, _, detail = self._result_drift(before, after)
        if not ok:
            self.fail("CLUSTER BY returned different clusters once the "
                      "collection carried a vector index: {0}".format(detail))

        cb_plan = self._explain(self._members_stmt(
            coll, num_clusters=self.centers, seed=42)).lower()
        if "cb_ann_vec_idx" in cb_plan:
            self.fail("CLUSTER BY plan referenced the vector index; it must "
                      "scan the collection: {0}".format(cb_plan[:400]))

        replan = self._explain(query).lower()
        if "index-search" not in replan:
            self.fail("ann_distance stopped using the vector index after a "
                      "CLUSTER BY on the same collection; plan head: "
                      "{0}".format(replan[:400]))

    # ======================================================================
    #  Category: Error / edge handling and scale
    # ======================================================================
    @staticmethod
    def _warning_codes(content):
        return [w.get("code") for w in (content.get("warnings") or [])]

    @staticmethod
    def _bytes_from_size(text):
        """Bytes for a size string such as 128KB, 32MB or 1GB."""
        units = {"KB": 1024, "MB": 1024 ** 2, "GB": 1024 ** 3}
        text = str(text).strip().upper()
        for suffix, mult in units.items():
            if text.endswith(suffix):
                return int(float(text[:-len(suffix)]) * mult)
        return int(float(text))

    def test_large_volume_spill(self):
        """Clustering under a sort memory smaller than the data still assigns
        every row, and the run provably exercised the spill path: the plan's
        sort operators handle more bytes than the configured budget, so they
        cannot have completed wholly in memory.

        Covers the external sorts inside the CLUSTER BY plan. The k-means
        workspace files are a separate memory path and are not pressured here.
        """
        n = self.input.param("spill_docs", 8000)
        dim = self.input.param("spill_dim", 8)
        k = self.input.param("spill_k", 8)
        sort_memory = self.input.param("spill_sortmemory", "128KB")
        budget = self._bytes_from_size(sort_memory)
        coll = self._create_aux_collection(
            "cb_spill", self._mkvec(n, dim, k, sep=20.0))
        total, _, content = self._sum_members(
            coll, raw=True, profile="timings",
            prefix="SET `compiler.sortmemory` \"{0}\"; ".format(sort_memory),
            num_clusters=k, dimension=[dim])
        if content.get("status") != "success":
            self.fail("Large-volume clustering under memory pressure failed: {0}"
                      .format(content.get("errors")))
        if total != n:
            self.fail("Spill run sum(members)={0}, expected {1} (no truncation)"
                      .format(total, n))

        sorts = [op for op in self._iter_named_ops(content.get("profile", {}))
                 if "Sort" in str(op.get("name", ""))]
        if not sorts:
            self.fail("No sort operator in the CLUSTER BY profile - the plan "
                      "changed and this case no longer pressures a sort")
        widest = max(op.get("tuple-bytes", 0) for op in sorts)
        if widest <= budget:
            self.fail(
                "Sort operators handled at most {0} bytes against a {1} ({2} "
                "byte) budget, so nothing had to spill and this case proved "
                "only that the query ran. Raise spill_docs or lower "
                "spill_sortmemory.".format(widest, sort_memory, budget))
        self.log.info("spill: {0} sort operators, widest handled {1} bytes "
                      "against a {2} budget; all {3} rows assigned"
                      .format(len(sorts), widest, sort_memory, total))

    def test_invalid_embeddings_excluded(self):
        """Malformed embeddings - wrong dimension, null, missing, non-numeric
        element, and a magnitude that overflows the squared distance - are all
        excluded with a runtime warning (23088, sometimes with 23067). The valid
        rows still cluster, sum(members) equals the valid-row count, and every
        centroid component stays finite. max-warnings must be sent for the
        warning to be observable."""
        good = self._mkvec(40, self.dim, 2)
        bad = [
            {"pk": 1000, "vec": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]},
            {"pk": 1001, "vec": None},
            {"pk": 1002},
            {"pk": 1003, "vec": [1, "x", 3, 4]},
            {"pk": 1004, "vec": [1e300, 1e300, 1e300, 1e300]},
        ]
        coll = self._create_aux_collection("cb_invalid", good + bad)
        stmt = self._members_stmt(
            coll,
            projection="sc.cluster_id AS cid, array_count(members) AS cnt, "
                       "sc.centroid AS centroid",
            num_clusters=2)
        content = self._run_raw(stmt, max_warnings=50)
        if content.get("status") != "success":
            self.fail("Clustering with invalid rows should succeed: {0}".format(
                content.get("errors")))
        rows = content.get("results", [])
        if not rows:
            self.fail("Clustering with invalid rows returned no clusters")
        total = sum(r["cnt"] for r in rows)
        if total != len(good):
            self.fail("sum(members)={0}, expected valid-row count {1} (bad rows "
                      "must be excluded)".format(total, len(good)))
        codes = self._warning_codes(content)
        if 23088 not in codes and 23067 not in codes:
            self.fail("Expected an exclusion warning (23088/23067) with "
                      "max-warnings set; got warnings {0}".format(
                          content.get("warnings")))
        for r in rows:
            for v in r["centroid"]:
                if not isinstance(v, (int, float)) or v != v:
                    self.fail("Non-finite centroid after excluding bad rows: {0}"
                              .format(r["centroid"]))

    def test_high_dimensional(self):
        """Realistic high-dimensional embeddings cluster correctly at low row
        count: separable blobs map 1:1 onto clusters, the centroid holds
        `dimension` elements, every row is assigned and nothing overflows."""
        dim = self.input.param("highdim_dim", 1536)
        centers = self.input.param("highdim_centers", 4)
        n = self.input.param("highdim_docs", 200)
        query_timeout = self.input.param("highdim_timeout", 600)
        coll = self._create_aux_collection(
            "cb_highdim", self._mkvec(n, dim, centers, sep=200.0))
        rows = self._cluster_rows(
            self._members_stmt(
                coll,
                projection="sc.cluster_id AS cid, array_count(members) AS cnt, "
                           "array_count(sc.centroid) AS clen, "
                           "(SELECT VALUE m.p.cid_true FROM members AS m) AS "
                           "trues",
                suffix="ORDER BY cid", num_clusters=centers, dimension=[dim]),
            timeout=query_timeout, analytics_timeout=query_timeout)
        if len(rows) != centers:
            self.fail("High-dim: expected {0} clusters, got {1}".format(
                centers, len(rows)))
        total = 0
        for r in rows:
            if r["clen"] != dim:
                self.fail("High-dim centroid length {0} != {1}".format(
                    r["clen"], dim))
            if len(set(r["trues"])) != 1:
                self.fail("High-dim cluster {0} impure: {1}".format(
                    r["cid"], set(r["trues"])))
            total += r["cnt"]
        if total != n:
            self.fail("High-dim sum(members)={0}, expected {1}".format(total, n))

    def test_non_array_fails_silently(self):
        """A non-array clustering expression skips every row: the query
        succeeds, returns [], and surfaces warning 23088 only when max-warnings
        is sent. dimension:[1] does not rescue a bare scalar.

        Row-level exclusion of malformed vectors is covered by
        test_invalid_embeddings_excluded."""
        for dim in ([self.dim], [1]):
            content = self._run_raw(
                "FROM {0} AS p CLUSTER BY p.a AS sc {1} SELECT sc.cluster_id;"
                .format(self.pts, self._with(dimension=dim)),
                max_warnings=10)
            if content.get("status") != "success":
                self.fail("dimension={0}: bare-scalar CLUSTER BY should be a "
                          "silent success: {1}".format(
                              dim, content.get("errors")))
            if content.get("results"):
                self.fail("dimension={0}: bare-scalar CLUSTER BY should return "
                          "[], got {1}".format(dim, content.get("results")))
            if 23088 not in self._warning_codes(content):
                self.fail("dimension={0}: bare-scalar CLUSTER BY should surface "
                          "warning 23088 with max-warnings; got {1}".format(
                              dim, content.get("warnings")))

    # ======================================================================
    #  Category: Concurrency and query management
    # ======================================================================
    def test_timeout_aborts_cleanly(self):
        """A CLUSTER BY cut short by a request timeout aborts cleanly - status
        timeout, code 21002, never an internal error or a crash - and a
        subsequent CLUSTER BY returns the full correct result. Client-initiated
        cancel is not exercised here."""
        stmt = ("FROM {0} AS p CLUSTER BY p.vec AS sc {1} SELECT sc.cluster_id;"
                ).format(self.pts, self._with())
        content = self._run_raw(
            stmt, timeout=self.input.param("abort_timeout", "1ms"))
        status = content.get("status")
        if status != "timeout":
            self.fail("Tiny timeout should abort with status 'timeout', got "
                      "'{0}' (errors={1})".format(status, content.get("errors")))
        codes = [e.get("code") for e in (content.get("errors") or [])]
        if 21002 not in codes:
            self.fail("Timeout should carry code 21002, got {0}".format(codes))
        total, _ = self._sum_members(self.pts)
        if total != self.no_of_docs:
            self.fail("Post-timeout CLUSTER BY under-counted: {0}".format(total))

    def test_concurrent_cluster_by(self):
        """Multiple CLUSTER BY queries run concurrently without interference:
        all succeed, each assigns every input row, and all return the identical
        partition under a pinned seed."""
        n = self.input.param("concurrency", 8)
        q = self._members_stmt(
            self.pts,
            projection="sc.cluster_id AS cid, array_count(members) AS cnt",
            suffix="ORDER BY cid", seed=42)

        def one():
            try:
                c = self._run_raw(q)
                return c.get("status"), c.get("results"), None
            except Exception as e:
                return None, None, str(e)

        with ThreadPoolExecutor(max_workers=n) as ex:
            results = [f.result() for f in [ex.submit(one) for _ in range(n)]]
        errs = [r[2] for r in results if r[2]]
        if errs:
            self.fail("Concurrent CLUSTER BY raised: {0}".format(errs))
        if not all(r[0] == "success" for r in results):
            self.fail("Not all concurrent queries succeeded: {0}".format(
                [r[0] for r in results]))
        ref = results[0][1]
        if not ref:
            self.fail("Concurrent CLUSTER BY returned no rows")
        for _, rows, _ in results:
            if sum(r["cnt"] for r in rows) != self.no_of_docs:
                self.fail("A concurrent query under-counted: {0}".format(rows))
            if self._norm(rows) != self._norm(ref):
                self.fail("Concurrent queries returned different partitions "
                          "(cross-talk?): {0} vs {1}".format(rows, ref))

    def test_collection_dropped_mid_query(self):
        """DDL racing a running query: the SOURCE collection is dropped while a
        CLUSTER BY executes. The query must resolve cleanly - either the full
        result or a clear error - never a crash, a hang, a dropped connection,
        or a success carrying a silent under-count. cbas must stay healthy
        afterwards."""
        n = self.input.param("drop_docs", 20000)
        dim = self.input.param("drop_dim", 16)
        k = self.input.param("drop_k", 16)
        coll = self._create_aux_collection(
            "cb_dropmid", self._mkvec(n, dim, k, sep=40.0))
        stmt = self._members_stmt(coll, num_clusters=k, dimension=[dim])
        out = {}

        def run_query():
            try:
                out["content"] = self._run_raw(stmt, timeout="120s")
            except Exception as e:
                out["exc"] = str(e)

        qt = threading.Thread(target=run_query)
        qt.start()
        time.sleep(self.input.param("drop_mid_query_settle", 0.5))
        drop = self._run_raw("DROP COLLECTION {0} IF EXISTS;".format(coll))
        self.log.info("concurrent DROP status: {0}".format(drop.get("status")))
        qt.join(timeout=180)

        if qt.is_alive():
            self.fail("CLUSTER BY hung after the source collection was dropped "
                      "mid-query")
        if "exc" in out:
            self.fail("CLUSTER BY raised a transport error (dropped "
                      "connection/crash) on concurrent DROP: {0}".format(
                          out["exc"]))
        content = out.get("content", {})
        status = content.get("status")
        if status == "success":
            total = sum(r.get("cnt", 0) for r in content.get("results", []))
            if total != n:
                self.fail("SUCCESS but silent under-count after a concurrent "
                          "DROP: sum(members)={0}, expected {1}".format(total, n))
        elif status in ("fatal", "timeout"):
            if not content.get("errors"):
                self.fail("Failed status with no error detail on concurrent "
                          "DROP: {0}".format(content))
        else:
            self.fail("Unexpected/empty query state on concurrent DROP: {0}"
                      .format(content))

        total, _ = self._sum_members(self.pts)
        if total != self.no_of_docs:
            self.fail("cbas unhealthy after concurrent DROP: shared-collection "
                      "CLUSTER BY returned {0}, expected {1}".format(
                          total, self.no_of_docs))

    # ======================================================================
    #  Category: Consistency
    # ======================================================================
    def test_no_shared_snapshot_under_dml(self):
        """REGRESSION GUARD for the source double-read. Under concurrent DML on
        the source collection, every CLUSTER BY must show:

          1. exactly ONE Index Search runtime-id  - the REPLICATE collapse held
          2. sum(members) == that scan's cardinality-out - a coherent read

        Keep this query UNFILTERED: cardinality-out is measured at the Index
        Search, before any WHERE, so on a filtered query sum(members) <
        cardinality legitimately and oracle 2 is invalid."""
        iterations = self.input.param("probe_iterations", 20)
        n_docs = self.input.param("probe_docs", 400)
        churn_docs = self.input.param("probe_churn_docs", 300)
        coll = self._create_aux_collection(
            "cb_d1probe", self._mkvec(n_docs, 4, 4))
        churn = [{"pk": pk, "vec": [999.0, 999.0, 999.0, 999.0]}
                 for pk in range(100000, 100000 + churn_docs)]
        stop = threading.Event()
        cycles = {"n": 0}

        def churn_loop():
            while not stop.is_set():
                try:
                    self._load_docs(coll, churn, batch=churn_docs)
                    self._run("DELETE FROM {0} WHERE pk >= 100000;".format(coll),
                              fail_on_error=False)
                    cycles["n"] += 1
                except Exception:
                    pass

        q = self._members_stmt(
            coll, projection="sc.cluster_id AS cid, array_count(members) AS cnt",
            num_clusters=4, seed=42)
        t = threading.Thread(target=churn_loop, daemon=True)
        t.start()
        multi_scan, incoherent, failed, ran, widths = [], [], [], 0, set()
        overlapped = 0
        try:
            for i in range(iterations):
                cycles_before = cycles["n"]
                content = self._run_raw(q, profile="timings")
                if cycles["n"] > cycles_before:
                    overlapped += 1
                if content.get("status") != "success":
                    failed.append((i, content.get("status"),
                                   content.get("errors")))
                    continue
                ran += 1
                scans = self._scan_totals(content.get("profile", {}))
                members = sum(r.get("cnt", 0)
                              for r in content.get("results", []))
                card = sum(scans.values())
                widths.add(card)
                if len(scans) != 1:
                    multi_scan.append((i, scans))
                elif card != members:
                    incoherent.append((i, card, members))
        finally:
            stop.set()
            t.join(timeout=10)

        min_success = self.input.param("probe_min_success",
                                       max(1, int(0.8 * iterations)))
        if ran < min_success:
            self.fail(
                "Only {0} of {1} CLUSTER BY queries succeeded (need {2}) - too "
                "few observations for the guard to mean anything. Failures "
                "(iter, status, errors): {3}".format(
                    ran, iterations, min_success, failed[:3]))
        if cycles["n"] == 0:
            self.fail("The churn thread completed 0 upsert/delete cycles in {0} "
                      "queries, so no concurrent DML was actually exercised - "
                      "the probe proved nothing.".format(ran))
        if overlapped == 0:
            self.fail(
                "The churn thread completed {0} cycles but none of them "
                "finished while a CLUSTER BY was in flight, so no query "
                "actually observed concurrent DML - the probe proved nothing. "
                "Raise probe_churn_docs or probe_iterations.".format(
                    cycles["n"]))
        if multi_scan:
            self.fail(
                "REGRESSION: the CLUSTER BY source double-read is BACK - {0}/{1} "
                "queries showed more than one Index Search scan, e.g. {2}. The "
                "REPLICATE collapse has been lost, which re-opens the "
                "no-shared-snapshot defect.".format(
                    len(multi_scan), ran, multi_scan[0]))
        if incoherent:
            self.fail(
                "INCOHERENT READ: {0}/{1} queries had sum(members) != scan "
                "cardinality-out, e.g. (iter, scan, members)={2}. A single scan "
                "must feed training and assignment identically."
                .format(len(incoherent), ran, incoherent[:3]))
        self.log.info(
            "single-scan guard holds: {0}/{1} queries succeeded, all "
            "single-scan and coherent; {2} churn cycles, {3} queries overlapped "
            "a cycle; collection widths observed under churn: {4}"
            .format(ran, iterations, cycles["n"], overlapped, sorted(widths)))

    def test_request_plus_standalone_noop(self):
        """On a STANDALONE collection scan_consistency=request_plus is a silent
        no-op (it is gated on shadow datasets): the query succeeds and returns
        the same clustering with and without it."""
        q = ("FROM {0} AS p CLUSTER BY p.vec AS sc {1} "
             "SELECT sc.cluster_id AS cid, sc.centroid AS centroid "
             "ORDER BY cid;").format(self.pts, self._with(seed=42))
        plain = self._run_raw(q)
        rp = self._run_raw(q, scan_consistency="request_plus")
        for label, content in (("baseline", plain), ("request_plus", rp)):
            if content.get("status") != "success":
                self.fail("{0} run on a standalone collection should succeed "
                          "(request_plus is a no-op here): {1}".format(
                              label, content.get("errors")))
            if not content.get("results"):
                self.fail("{0} run returned no clusters, so the comparison "
                          "below would hold vacuously".format(label))

        def by_cid(rows):
            return sorted(rows or [], key=lambda r: r.get("cid"))

        ok, worst, detail = self._result_drift(by_cid(plain.get("results")),
                                               by_cid(rp.get("results")))
        if not ok:
            self.fail("request_plus changed the standalone result - it must be "
                      "a silent no-op on standalone collections: {0}".format(
                          detail))
        self.log.info("request_plus no-op: results match, worst centroid drift "
                      "{0:g}".format(worst))

    # ======================================================================
    #  Link-fed setup (Couchbase remote link -> SIFT shadow dataset)
    # ======================================================================
    def _resolve_kv_node(self):
        """Return the KV (remote) node, read straight from the ini.

        Do not use cb_clusters or self.remote_cluster here: the framework
        expects the remote cluster first and analytics second, so under this
        ini ordering no cb_clusters entry wraps the KV node and remote_cluster
        resolves to the EA master.
        """
        kv_node = next(
            (s for s in self.servers
             if "kv" in (getattr(s, "services", "") or "")
             and "cbas" not in (getattr(s, "services", "") or "")), None)
        if kv_node is None:
            self.fail("No KV node (services:kv, no cbas) in the ini - a "
                      "2-cluster ini ([cluster1]=EA, [cluster2]=KV) is "
                      "required for the link-fed cases.")
        return kv_node

    def _node_credentials(self, node):
        """Return (username, password) for a node as declared in the ini.
        Fails rather than falling back to a built-in default, so a misconfigured
        ini surfaces here instead of as an opaque auth error later."""
        username = getattr(node, "rest_username", None)
        password = getattr(node, "rest_password", None)
        if not username or not password:
            self.fail(
                "Node {0} carries no rest_username/rest_password; declare the "
                "credentials in the ini rather than relying on a default."
                .format(getattr(node, "ip", node)))
        return username, password

    def _load_sift_via_framework(self):
        """Load the KV `sift` bucket with real BigANN vectors through the
        framework loader (JavaDocLoaderUtils -> Sirius). Requires the Java doc
        loader to be running (--launch_java_doc_loader).

        The loader writes doc.id = the numeric record index and the vector into
        the `embedding` field, and record n of the source file lands on doc id
        n even at process_concurrency > 1, which is what keeps the
        id -> file-record oracle in test_sift_vector_fidelity valid.
        """
        src = self.sift_vectors_file
        if not os.path.exists(src):
            self.fail(
                "SIFT source {0} not found. The Java loader hardcodes this "
                "filename and will otherwise DOWNLOAD the full 1-billion-vector "
                "base set over FTP. Set base_vectors_file_path to a directory "
                "holding bigann_base.bvecs (current value: {1}).".format(
                    src, self.base_vectors_file_path))
        need = self.docloader_create_end * (self.sift_dim + 4)
        have = os.path.getsize(src)
        if have < need:
            self.fail(
                "SIFT source {0} holds ~{1} vectors but {2} were requested "
                "({3} bytes < {4}). Point base_vectors_file_path at a corpus "
                "holding at least docloader_create_end records."
                .format(src, have // (self.sift_dim + 4),
                        self.docloader_create_end, have, need))

        kv_node = self._resolve_kv_node()
        cluster = CBCluster(name="sift_kv", servers=[kv_node])
        cluster.kv_nodes = [kv_node]
        cluster.nodes_in_cluster = [kv_node]
        self.log.info("SIFT loader: KV cluster master = {0}, source = {1}"
                      .format(cluster.master.ip, src))

        buckets = self.bucket_util.get_all_buckets(cluster) or []
        bucket = next((b for b in buckets if b.name == self.sift_bucket), None)
        if bucket is None:
            self.log.info("Creating KV bucket `{0}` on {1}".format(
                self.sift_bucket, cluster.master.ip))
            bucket = Bucket({
                Bucket.name: self.sift_bucket,
                Bucket.ramQuotaMB: self.input.param("sift_bucket_ram", 1024),
                Bucket.replicaNumber: 0,
                Bucket.bucketType: Bucket.Type.MEMBASE,
                Bucket.flushEnabled: Bucket.FlushBucket.ENABLED})
            self.bucket_util.create_bucket(cluster, bucket)
            buckets = self.bucket_util.get_all_buckets(cluster) or []
            bucket = next(
                (b for b in buckets if b.name == self.sift_bucket), None)
            if bucket is None:
                self.fail("Failed to create KV bucket `{0}` on {1}".format(
                    self.sift_bucket, cluster.master.ip))

        JavaDocLoaderUtils(self.bucket_util, self.cluster_util)

        kv_user, kv_pwd = self._node_credentials(cluster.master)
        SiriusCouchbaseLoader.create_clients_in_pool(
            cluster.master, kv_user, kv_pwd,
            bucket.name, req_clients=self.input.param("sift_req_clients", 5))
        loader = SiriusCouchbaseLoader(
            server_ip=cluster.master.ip, server_port=cluster.master.port,
            username=kv_user, password=kv_pwd,
            bucket=bucket, scope_name="_default", collection_name="_default",
            key_prefix="test_docs-", key_size=32, key_type="SimpleKey",
            doc_size=1024, value_type="siftBigANN",
            create_percent=100, read_percent=0, update_percent=0,
            delete_percent=0, expiry_percent=0,
            create_start_index=0, create_end_index=self.docloader_create_end,
            read_start_index=0, read_end_index=0,
            update_start_index=0, update_end_index=0,
            delete_start_index=0, delete_end_index=0,
            touch_start_index=0, touch_end_index=0,
            replace_start_index=0, replace_end_index=0,
            expiry_start_index=0, expiry_end_index=0,
            process_concurrency=self.input.param("pc", 8),
            task_identifier="sift", ops=self.input.param("ops_rate", 20000),
            suppress_error_table=False, track_failures=True, mutate=0,
            elastic=False, model=None, mockVector=False, dim=self.sift_dim,
            base64=False,          # True would emit `embedding` as a STRING
            base_vectors_file_path=self.base_vectors_file_path,
            sift_url="ftp://ftp.irisa.fr/local/texmex/corpus/bigann_base.bvecs.gz",
            stall_timeout=-1)
        loader.create_doc_load_task()
        self.task_manager.add_new_task(loader)
        self.task_manager.get_task_result(loader)
        stall_limit = self.input.param("sift_load_stall_checks", 12)
        hard_cap = time.time() + self.input.param("sift_load_timeout", 1800)
        loaded, previous, stalled = 0, -1, 0
        while time.time() < hard_cap:
            loaded = self.bucket_util.get_buckets_item_count(
                cluster, bucket_name=bucket.name)
            if loaded >= self.docloader_create_end:
                break
            stalled = 0 if loaded > previous else stalled + 1
            if stalled >= stall_limit:
                self.fail("SIFT load stalled at {0} of {1} docs in `{2}` - no "
                          "progress for {3} consecutive checks".format(
                              loaded, self.docloader_create_end, self.sift_bucket,
                              stall_limit))
            previous = loaded
            time.sleep(5)
        if loaded < self.docloader_create_end:
            self.fail("SIFT load reached {0} of {1} docs in `{2}` before the "
                      "hard cap".format(loaded, self.docloader_create_end,
                                        self.sift_bucket))
        self.log.info("SIFT load complete: {0} docs in `{1}`".format(
            loaded, self.sift_bucket))

    def _setup_sift_link(self):
        """Provision the link-fed fixture once and return the shadow
        dataset's full name: ensure the SIFT KV bucket exists, optionally load
        SIFT into it, create the Couchbase remote link (EA -> KV) and the
        shadow dataset, and wait for ingestion. Reused across test processes,
        so an already-populated dataset skips creation and the ingestion wait."""
        link_dv, link_name = "Default", "sift_remote_link"
        link_full = link_name
        ds_name = "sift_ds"
        ds_full = "Default.Default.{0}".format(ds_name)

        kv_node = self._resolve_kv_node()
        self.log.info("link-fed: KV node = {0}, EA = {1}".format(
            kv_node.ip, self.columnar_cluster.master.ip))

        have = self._count_of(ds_full, fail_on_error=False) or 0
        if have == self.docloader_create_end:
            ClusterBy._sift_link = link_full
            return ds_full
        if have and not self.load_sift:
            self.fail(
                "{0} holds {1} rows but docloader_create_end={2}. Refusing to run "
                "against "
                "a differently-sized dataset. Re-load the `{3}` bucket, pass "
                "docloader_create_end={1}, or run with load_sift=True to rebuild it."
                .format(ds_full, have, self.docloader_create_end, self.sift_bucket))

        if self.load_sift:
            self._load_sift_via_framework()

        link_user, link_pwd = self._node_credentials(kv_node)
        status, cert, _ = x509main(kv_node)._get_cluster_ca_cert()
        cert_pem = json.loads(cert)["cert"]["pem"] if status else None
        link_props = {
            "name": link_name, "scope": link_dv, "type": "couchbase",
            "hostname": kv_node.ip,
            "username": link_user, "password": link_pwd,
            "encryption": "full", "certificate": cert_pem}
        if not self.cbas_util.create_remote_link(
                self.columnar_cluster, link_props, create_if_not_exists=True):
            self.fail("Failed to create remote link {0}".format(link_full))

        if not self.cbas_util.create_remote_dataset(
                self.columnar_cluster, dataset_name=ds_name,
                kv_entity=self.sift_bucket, link_name=link_full,
                dataverse_name="Default", database_name="Default",
                if_not_exists=True):
            self.fail("Failed to create shadow dataset {0}".format(ds_full))

        self.cbas_util.connect_link(self.columnar_cluster, link_name=link_full)
        if not self.cbas_util.wait_for_ingestion_complete(
                self.columnar_cluster, ds_full, self.docloader_create_end, timeout=3600):
            self.fail("Shadow dataset {0} did not finish ingesting {1} docs"
                      .format(ds_full, self.docloader_create_end))

        ClusterBy._sift_link = link_full
        return ds_full

    def _link_cluster(self, coll, num_clusters=10, scan_consistency=None,
                      projection="array_count(members) AS cnt"):
        """Cluster the link-fed dataset with a profile and return
        (envelope, scan_totals, sum_members, distinct_pk)."""
        stmt = self._members_stmt(
            coll, vec_expr="p.embedding", projection=projection,
            num_clusters=num_clusters, dimension=[self.sift_dim], seed=42)
        content = self._run_raw(stmt, profile="timings",
                                scan_consistency=scan_consistency)
        if content.get("status") != "success":
            self.fail("Link-fed CLUSTER BY failed (scan_consistency={0}): {1}"
                      .format(scan_consistency, content.get("errors")))
        total = sum(r["cnt"] for r in content.get("results", []))
        scans = self._scan_totals(content.get("profile", {}))
        distinct_pk = self._count_of(
            "(SELECT DISTINCT meta(p).id FROM {0} AS p) AS d".format(coll))
        return content, scans, total, distinct_pk

    def test_link_fed_coherence(self):
        """CLUSTER BY over a Couchbase-link-fed shadow dataset of real SIFT
        vectors, in three variants. Each assertion names its variant.

        DEFAULT        - every visible pk is in exactly one cluster
                         (sum(members) == distinct-pk count) and the source is
                         read by exactly one Index Search scan.
        REQUEST_PLUS   - scan_consistency=request_plus is a real DCP-seqno
                         freshness wait on a shadow dataset (unlike the
                         standalone no-op); the query succeeds and stays
                         coherent.
        SHAPE          - the centroid holds `sift_dim` elements. Purity is not
                         checked: real SIFT is not well separated.

        Assert exactly one scan: comparing two scans' totals degrades to a
        no-op on a build where the double-read is already collapsed."""
        coll = self._setup_sift_link()
        k = self.input.param("link_k", 10)

        default_content, scans, total, distinct_pk = self._link_cluster(
            coll, num_clusters=k,
            projection="array_count(members) AS cnt, "
                       "array_count(sc.centroid) AS clen")
        if total != distinct_pk:
            self.fail("DEFAULT: sum(members)={0} != distinct-pk {1} (torn or "
                      "duplicated assignment)".format(total, distinct_pk))
        if len(scans) != 1:
            self.fail("DEFAULT: expected exactly one Index Search scan over the "
                      "link-fed source, got {0}. A second scan re-opens the "
                      "no-shared-snapshot hazard.".format(scans))
        card = sum(scans.values())
        if card != total:
            self.fail("DEFAULT: scan cardinality-out {0} != sum(members) {1} - "
                      "the single scan did not feed training and assignment "
                      "identically.".format(card, total))

        _, _, rp_total, rp_distinct = self._link_cluster(
            coll, num_clusters=k, scan_consistency="request_plus")
        if rp_total != rp_distinct:
            self.fail("REQUEST_PLUS: result incoherent, sum(members)={0} != "
                      "distinct-pk {1}".format(rp_total, rp_distinct))

        rows = default_content.get("results", [])
        if not rows:
            self.fail("SHAPE: link-fed CLUSTER BY returned no clusters")
        for r in rows:
            if r["clen"] != self.sift_dim:
                self.fail("SHAPE: centroid length {0} != {1}".format(
                    r["clen"], self.sift_dim))
        self.log.info("link-fed coherence: {0} rows, {1} distinct pks, one scan "
                      "of cardinality {2}".format(total, distinct_pk, card))

    def test_link_reconnect_no_duplicate_pks(self):
        """After a disconnect and reconnect of the remote link forces
        re-ingestion, no pk appears in two clusters: sum(members) still equals
        the distinct-pk count."""
        coll = self._setup_sift_link()
        _, _, base_total, base_pk = self._link_cluster(coll)
        if base_total != base_pk:
            self.fail("Baseline link-fed sum(members)={0} != distinct-pk {1}"
                      .format(base_total, base_pk))
        self.cbas_util.disconnect_link(self.columnar_cluster,
                                       link_name=ClusterBy._sift_link)
        self.cbas_util.connect_link(self.columnar_cluster,
                                    link_name=ClusterBy._sift_link)
        if not self.cbas_util.wait_for_ingestion_complete(
                self.columnar_cluster, coll, self.docloader_create_end,
                timeout=self.input.param("reingest_timeout", 3600)):
            self.fail("Shadow dataset {0} did not finish re-ingesting {1} docs "
                      "after the link was reconnected".format(
                          coll, self.docloader_create_end))
        _, _, total, distinct_pk = self._link_cluster(coll)
        if total != distinct_pk:
            self.fail("After link reconnect sum(members)={0} != distinct-pk {1} "
                      "- re-ingestion duplicated pks (broke disjointness)".format(
                          total, distinct_pk))

    # ------------------------------------------------- SIFT ground truth
    def _read_bvecs(self, path, count, dim):
        """Read `count` vectors from a BigANN .bvecs file: each record is a
        4-byte little-endian dimension header followed by `dim` uint8 values.
        Returns a list of lists of float, index i == doc id i. This file is the
        ground truth the KV bucket was built from."""
        if not os.path.exists(path):
            self.fail(
                "SIFT ground-truth file {0} not found. Set "
                "base_vectors_file_path to a directory holding "
                "bigann_base.bvecs (current value: {1}).".format(
                    path, self.base_vectors_file_path))
        vectors = []
        with open(path, "rb") as handle:
            for i in range(count):
                header = handle.read(4)
                if len(header) < 4:
                    break
                rec_dim = struct.unpack("<i", header)[0]
                raw = handle.read(rec_dim)
                if len(raw) < rec_dim:
                    break
                if rec_dim != dim:
                    self.fail("{0}: record {1} has dimension {2}, expected {3}"
                              .format(path, i, rec_dim, dim))
                vectors.append([float(b) for b in raw])
        if len(vectors) < count:
            self.fail("{0} holds only {1} vectors, need {2}".format(
                path, len(vectors), count))
        return vectors

    def test_sift_vector_fidelity(self):
        """Every ingested vector is byte-identical to the source SIFT file.

        Walks the whole dataset in pages and compares element by element
        against the .bvecs file the KV bucket was loaded from, so a corrupted,
        rescaled, truncated or re-ordered embedding is caught - nothing else in
        the suite validates the vector VALUES arriving over the link. Exact
        equality, no epsilon: the values are uint8 0-255 carried as doubles, so
        any difference is corruption rather than float drift."""
        coll = self._setup_sift_link()
        n_docs = self._count_of(coll)
        truth = self._read_bvecs(self.sift_vectors_file, n_docs, self.sift_dim)

        page, checked, mismatches = 5000, 0, []
        for start in range(0, n_docs, page):
            rows = self._cluster_rows(
                "SELECT p.id AS id, p.embedding AS emb FROM {0} AS p "
                "WHERE p.id >= {1} AND p.id < {2} ORDER BY p.id;".format(
                    coll, start, start + page),
                timeout=1200, analytics_timeout=1200)
            if not rows:
                self.fail("Page [{0},{1}) returned no rows but the dataset "
                          "holds {2} docs".format(start, start + page, n_docs))
            for row in rows:
                idx, emb = row["id"], row["emb"]
                if not isinstance(idx, int) or not 0 <= idx < len(truth):
                    mismatches.append(
                        "id={0!r} is outside the ground-truth range "
                        "[0,{1})".format(idx, len(truth)))
                    checked += 1
                    if len(mismatches) >= 5:
                        self.fail("Ingested vectors do not match {0}:\n  "
                                  "{1}".format(self.sift_vectors_file,
                                               "\n  ".join(mismatches)))
                    continue
                expected = truth[idx]
                if len(emb) != self.sift_dim:
                    mismatches.append(
                        "id={0}: length {1} != {2}".format(
                            idx, len(emb), self.sift_dim))
                elif emb != expected:
                    bad = [(j, emb[j], expected[j])
                           for j in range(self.sift_dim)
                           if emb[j] != expected[j]]
                    mismatches.append(
                        "id={0}: {1} component(s) differ, first 3 "
                        "(index, got, want)={2}".format(idx, len(bad), bad[:3]))
                checked += 1
                if len(mismatches) >= 5:
                    self.fail("Ingested vectors do not match {0}:\n  {1}".format(
                        self.sift_vectors_file, "\n  ".join(mismatches)))
        if checked != n_docs:
            self.fail("Compared {0} vectors but the dataset holds {1} - the "
                      "`id` field is not dense over [0,{1})".format(
                          checked, n_docs))
        if mismatches:
            self.fail("Ingested vectors do not match {0}:\n  {1}".format(
                self.sift_vectors_file, "\n  ".join(mismatches)))
        self.log.info("vector fidelity: {0} vectors x {1} dims byte-identical "
                      "to {2}".format(checked, self.sift_dim,
                                      self.sift_vectors_file))

    def test_sift_cluster_quality(self):
        """Measure clustering quality on real SIFT data. Both numbers are taken
        over the full dataset and logged every run.

        LEAK RATE - the share of points not sitting with their nearest reported
        centroid. Guarded loosely: the engine can report a centroid one Lloyd
        step ahead of the one the assignment used, so a small non-zero rate is
        structural.

        INERTIA RATIO - engine inertia over an independent numpy Lloyd's
        reference, scored at two budgets: matched to the engine's own Lloyd
        iteration count (tight guard, an implementation regression shows here)
        and thorough (loose guard, the gap is expected).

        Vectors come from the local ground-truth file; only descriptors and
        member ids cross the wire."""
        try:
            import numpy as np
        except ImportError:
            self.fail("test_sift_cluster_quality needs numpy in the test venv "
                      "(pip install numpy)")
        coll = self._setup_sift_link()
        n_docs = self._count_of(coll)
        k = self.input.param("quality_k", 10)
        max_leak_pct = float(self.input.param("max_leak_pct", 0.5))
        max_inertia_ratio = float(self.input.param("max_inertia_ratio", 1.10))
        matched_iters = self.input.param("quality_matched_iters", 3)
        max_matched_ratio = float(
            self.input.param("max_matched_inertia_ratio", 1.05))

        rows = self._cluster_rows(
            self._members_stmt(
                coll, vec_expr="p.embedding",
                projection="sc.cluster_id AS cid, sc.centroid AS centroid, "
                           "(SELECT VALUE m.p.id FROM members AS m) AS ids",
                suffix="ORDER BY cid", num_clusters=k,
                dimension=[self.sift_dim], seed=42),
            timeout=1800, analytics_timeout=1800)
        if not rows:
            self.fail("CLUSTER BY returned no clusters")

        if len(rows) != k:
            self.fail(
                "Asked for {0} clusters over {1} all-distinct real vectors but "
                "got {2} (ids {3}) - under-clustering, not a quality issue"
                .format(k, n_docs, len(rows), [r["cid"] for r in rows]))

        vectors = np.asarray(
            self._read_bvecs(self.sift_vectors_file, n_docs, self.sift_dim),
            dtype=np.float64)
        centroids = np.asarray([r["centroid"] for r in rows], dtype=np.float64)
        assigned = np.full(n_docs, -1, dtype=np.int64)
        for slot, row in enumerate(rows):
            ids = np.asarray(row["ids"], dtype=np.int64)
            if ids.size and (ids.min() < 0 or ids.max() >= n_docs):
                self.fail("Cluster {0} reports member id outside [0,{1})"
                          .format(row["cid"], n_docs))
            assigned[ids] = slot
        if (assigned < 0).any():
            self.fail("{0} of {1} docs appear in no cluster".format(
                int((assigned < 0).sum()), n_docs))

        def inertia_of(vecs, cents, labels):
            diff = vecs - cents[labels]
            return float((diff * diff).sum())

        def sqdist(vecs, cents):
            """Squared distance from every vector to every centroid, one
            centroid at a time. Use per-centroid differences, not the
            |v|^2-2vc+|c|^2 identity - it loses precision through cancellation
            and trips spurious BLAS overflow flags."""
            out = np.empty((vecs.shape[0], cents.shape[0]), dtype=np.float64)
            for idx in range(cents.shape[0]):
                diff = vecs - cents[idx]
                out[:, idx] = (diff * diff).sum(axis=1)
            return out

        if not np.isfinite(vectors).all():
            self.fail("Ground-truth vectors from {0} contain non-finite values"
                      .format(self.sift_vectors_file))
        if not np.isfinite(centroids).all():
            self.fail("Engine returned non-finite centroid components: {0}"
                      .format(centroids[~np.isfinite(centroids)][:5]))
        nearest = sqdist(vectors, centroids).argmin(axis=1)
        leaked = int((nearest != assigned).sum())
        leak_pct = 100.0 * leaked / float(n_docs)
        engine_inertia = inertia_of(vectors, centroids, assigned)

        rng = np.random.default_rng(42)
        total_iters = self.input.param("quality_iters", 50)
        restarts = self.input.param("quality_restarts", 3)
        if restarts < 1 or total_iters < 1:
            self.fail("quality_restarts ({0}) and quality_iters ({1}) must both "
                      "be >= 1, otherwise no reference is computed".format(
                          restarts, total_iters))
        best_ref, best_matched = None, None
        for _ in range(restarts):
            cur = vectors[rng.choice(n_docs, size=k, replace=False)].copy()
            labels = None
            for it in range(total_iters):
                new_labels = sqdist(vectors, cur).argmin(axis=1)
                converged = labels is not None and np.array_equal(new_labels,
                                                                  labels)
                labels = new_labels
                if not converged:
                    for c in range(k):
                        mask = labels == c
                        if mask.any():
                            cur[c] = vectors[mask].mean(axis=0)

                if it + 1 == matched_iters or (converged and
                                               best_matched is None):
                    m_lab = sqdist(vectors, cur).argmin(axis=1)
                    m_in = inertia_of(vectors, cur, m_lab)
                    best_matched = (m_in if best_matched is None
                                    else min(best_matched, m_in))
                if converged:
                    break
            ref = inertia_of(vectors, cur, labels)
            best_ref = ref if best_ref is None else min(best_ref, ref)
        if best_ref is None or best_matched is None:
            self.fail("Reference k-means produced no inertia (best_ref={0}, "
                      "best_matched={1}) - cannot score quality".format(
                          best_ref, best_matched))

        ratio = engine_inertia / best_ref if best_ref > 0 else float("inf")
        matched_ratio = (engine_inertia / best_matched
                         if best_matched > 0 else float("inf"))
        self.log.info(
            "SIFT quality (n={0}, k={1}): leak={2} ({3:.3f}%), engine inertia="
            "{4:.6g} | matched({5} iters) ref={6:.6g} ratio={7:.4f} | thorough"
            "({8} iters) ref={9:.6g} ratio={10:.4f}".format(
                n_docs, k, leaked, leak_pct, engine_inertia, matched_iters,
                best_matched, matched_ratio, total_iters, best_ref, ratio))
        if leak_pct > max_leak_pct:
            self.fail("{0} of {1} points ({2:.3f}%) are not with their nearest "
                      "returned centroid - above the {3}% guard".format(
                          leaked, n_docs, leak_pct, max_leak_pct))
        if matched_ratio > max_matched_ratio:
            self.fail(
                "At the engine's OWN budget ({0} Lloyd iterations) its inertia "
                "{1:.6g} is {2:.4f}x an independent k-means {3:.6g} (guard "
                "{4}x). Like-for-like, so this is an implementation problem, "
                "not the fixed-3 design.".format(
                    matched_iters, engine_inertia, matched_ratio, best_matched,
                    max_matched_ratio))
        if ratio > max_inertia_ratio:
            self.fail("Engine inertia {0:.6g} is {1:.4f}x the numpy Lloyd's "
                      "reference {2:.6g} (guard {3}x) - the clustering is "
                      "materially worse than an independent k-means at k={4}"
                      .format(engine_inertia, ratio, best_ref,
                              max_inertia_ratio, k))

    # ======================================================================
    #  Category: Resiliency / topology
    # ======================================================================
    def _require_multinode(self):
        nodes = getattr(self.columnar_cluster, "cbas_nodes", []) or []
        if len(nodes) < 2:
            self.fail("Precondition unmet: this case needs >= 2 cbas nodes "
                      "(found {0}). Run with a multi-node ini.".format(
                          len(nodes)))
        return nodes

    def _non_cc_cbas_node(self):
        cc = self.columnar_cluster.cbas_cc_node
        for node in self.columnar_cluster.cbas_nodes:
            if node.ip != cc.ip:
                return node
        self.fail("No non-coordinator cbas node available for the victim")

    def test_rebalance_in_partition_sensitivity(self):
        """After a rebalance-in, every row is still assigned: sum(members)
        equals the input count. Exact cluster ids and centroids may legitimately
        differ, because the seed mixes in the partition id - do not assert
        per-cluster counts across a topology change."""
        self._require_multinode()
        base_total, _ = self._sum_members(self.pts, seed=42)
        if base_total != self.no_of_docs:
            self.fail("Baseline sum(members)={0}, expected {1}".format(
                base_total, self.no_of_docs))
        rebalance_task, self.columnar_cluster.available_servers = \
            self.rebalance_util.rebalance(
                cluster=self.columnar_cluster, cbas_nodes_in=1,
                available_servers=self.columnar_cluster.available_servers,
                in_node_services="kv,cbas")
        if not self.rebalance_util.wait_for_rebalance_task_to_complete(
                rebalance_task, self.columnar_cluster, True, True):
            self.fail("Rebalance-in of a kv,cbas node failed")
        total, _ = self._sum_members(self.pts, seed=42)
        if total != self.no_of_docs:
            self.fail("After rebalance-in sum(members)={0}, expected {1} "
                      "(sum must stay invariant across a topology change)"
                      .format(total, self.no_of_docs))

    def test_failover_then_cluster_by(self):
        """After a hard failover and FullRecovery, clustering is complete over
        the recovered topology: every qualifying row is accounted for."""
        self._require_multinode()
        self.columnar_cluster.available_servers, _, _ = \
            self.rebalance_util.failover(
                cluster=self.columnar_cluster, cbas_nodes=1,
                failover_type=self.input.param("failover_type", "Hard"),
                action="FullRecovery",
                available_servers=self.columnar_cluster.available_servers)
        total, _ = self._sum_members(self.pts)
        if total != self.no_of_docs:
            self.fail("After failover+recovery sum(members)={0}, expected {1}"
                      .format(total, self.no_of_docs))

    def test_concurrent_with_rebalance(self):
        """A CLUSTER BY submitted during an active rebalance either returns the
        correct full result or fails with a clear error - never a silent
        under-count."""
        self._require_multinode()
        rebalance_task, self.columnar_cluster.available_servers = \
            self.rebalance_util.rebalance(
                cluster=self.columnar_cluster, cbas_nodes_out=1,
                available_servers=self.columnar_cluster.available_servers)
        content = self._run_raw(self._members_stmt(self.pts))
        self.rebalance_util.wait_for_rebalance_task_to_complete(
            rebalance_task, self.columnar_cluster, True, True)
        status = content.get("status")
        if status == "success":
            total = sum(r["cnt"] for r in content.get("results", []))
            if total != self.no_of_docs:
                self.fail("Query during rebalance returned a silent under-count: "
                          "sum(members)={0}, expected {1}".format(
                              total, self.no_of_docs))
        elif status not in ("fatal", "timeout"):
            self.fail("Query during rebalance ended in an unexpected state: {0}"
                      .format(content))

    def test_node_killed_mid_query(self):
        """A node dying mid-query fails cleanly or returns the complete result -
        never success with an under-count - and after the node restarts a re-run
        returns the full correct clustering."""
        self._require_multinode()
        victim = self._non_cc_cbas_node()
        base_total, _ = self._sum_members(self.pts)
        out = {}

        def run_query():
            try:
                out["content"] = self._run_raw(
                    self._members_stmt(self.pts))
            except Exception as e:
                out["exc"] = str(e)

        qt = threading.Thread(target=run_query)
        qt.start()
        shell = RemoteMachineShellConnection(victim)
        try:
            shell.kill_process("java", "java", signum=9)
        finally:
            shell.disconnect()
        qt.join(timeout=self.input.param("kill_join_timeout", 120))

        shell = RemoteMachineShellConnection(victim)
        try:
            shell.start_couchbase()
        finally:
            shell.disconnect()
        self._wait_for_analytics_ready(
            timeout=self.input.param("restart_ready_timeout", 300))

        if qt.is_alive():
            self.fail("CLUSTER BY hung after the cbas node was killed mid-query")
        if "exc" in out:
            self.fail("CLUSTER BY raised a transport error on node kill: {0}"
                      .format(out["exc"]))
        if "content" not in out:
            self.fail("The in-flight query thread produced no result and no "
                      "exception - nothing was verified about the node kill")
        if out["content"].get("status") == "success":
            total = sum(r["cnt"] for r in out["content"].get("results", []))
            if total != base_total:
                self.fail("Node-kill produced a SILENT partial clustering: "
                          "sum(members)={0}, expected {1}".format(
                              total, base_total))
        total, _ = self._sum_members(self.pts)
        if total != base_total:
            self.fail("After recovery the re-run under-counted: {0} vs baseline "
                      "{1}".format(total, base_total))

    def test_service_restart_then_cluster_by(self):
        """After an Enterprise Analytics service restart on a non-coordinator
        node, clustering still works and returns the same cluster ids and member
        counts as the pre-restart baseline."""
        self._require_multinode()
        victim = self._non_cc_cbas_node()
        base = self._cluster_counts(self.pts, seed=42)
        if not base:
            self.fail("Pre-restart baseline returned no clusters")
        shell = RemoteMachineShellConnection(victim)
        try:
            shell.execute_command("systemctl restart enterprise-analytics")
        finally:
            shell.disconnect()
        self._wait_for_analytics_ready(
            timeout=self.input.param("restart_ready_timeout", 300))
        after = self._cluster_counts(self.pts, seed=42)
        if sum(r["cnt"] for r in after) != self.no_of_docs:
            self.fail("Post-restart clustering under-counted: {0}".format(after))
        if self._norm(base) != self._norm(after):
            self.fail("Post-restart clustering differs from the pre-restart "
                      "baseline (metadata corruption?):\n{0}\n{1}".format(
                          base, after))
