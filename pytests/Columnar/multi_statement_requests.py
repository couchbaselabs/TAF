"""
Created on 20-Sep-2026

Automation for MB-62708 (multi-statement requests: POST /api/v1/request
with "multi-statement": true returns a "statements" array, one entry per
statement, instead of the pre-feature flat envelope).

Exercised against /api/v1/request, the documented endpoint, rather than the
legacy /analytics/service: the multi-statement parameter and the wire status
codes these cases assert on are only reachable there.
"""
import json
import threading
import time
from urllib.parse import urljoin, urlparse, urlunparse
from uuid import uuid4

import requests
from cb_constants.CBServer import CbServer
from cb_server_rest_util.analytics.analytics_api import AnalyticsRestAPI
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from cbas_utils.cbas_utils_columnar import CbasUtil as ColumnarCbasUtil
from cbas_utils.cbas_utils_columnar import RBAC_Util
from cbas_utils.cbas_utils_on_prem import CBASRebalanceUtil
from Columnar.onprem.columnar_onprem_base import ColumnarOnPremBase
from membase.api.rest_client import RestConnection
from security_utils.audit_ready_functions import audit

from TestInput import TestInputSingleton

MS_FIXTURE_STATEMENT = (
    "drop scope ms if exists; "
    "create scope ms; "
    "create dataset ms.one primary key (id: int); "
    "create dataset ms.empty primary key (id: int); "
    "create dataset ms.big primary key (id: int); "
    "upsert into ms.one [{\"id\":1,\"v\":\"solo\"}]; "
    "upsert into ms.big (select value {\"id\": i, \"grp\": i % 10, "
    "\"pad\": \"abcdefghij\"} from range(1,1000) i);"
)

CBO_FIXTURE_STATEMENT = (
    "drop scope cbo if exists; "
    "create scope cbo; "
    "create collection cbo.big primary key (id: bigint); "
    "create collection cbo.small primary key (id: bigint); "
    "upsert into cbo.big (select value {\"id\": i, \"grp\": case when i <= "
    "99000 then 0 else i % 10 end, \"pad\": \"abcdefghijklmnopqrst\"} "
    "from range(1,100000) i); "
    "upsert into cbo.small (select value {\"id\": i, \"v\": \"g\" || "
    "string(i)} from range(0,9) i);"
)


PLAN_DEBUG_PARAMS = {"optimized-logical-plan": True, "skip-plan-cache": True}

STATEMENT_NUMBER_KEYS = ("statementId", "statement")

JOB_NUMBER_SQL = "ifmissing(j.statementId, j.statement)"

CBO_JOIN_QUERY = (
    "select b.id, s.v from cbo.big b join cbo.small s on b.grp = s.id "
    "where b.grp = 7;"
)


class MultiStatementRequests(ColumnarOnPremBase):

    MS_FIXTURE_SHAPE = (("ms.one", 1), ("ms.empty", 0), ("ms.big", 1000))


    DDL_ENTRY_KEYS = frozenset({"plans", "status", "metrics"})
    QUERY_ENTRY_KEYS = DDL_ENTRY_KEYS | {"signature", "results"}
    AUDIT_TESTS = (
        "test_one_audit_event_per_executed_statement",
        "test_failure_audit_per_statement_never_run_and_request_level",
        "test_per_event_enablement_and_sensitive_statements")

    TOPOLOGY_TESTS = (
        "test_rebalance_starting_mid_request",
        "test_analytics_service_killed_and_restarted_mid_request",
        "test_deferred_handles_across_node_restart",
        "test_hard_failover_of_node_executing_statement")

    def _base_setup(self):
        if not TestInputSingleton.input.param("skip_cbas_cleanup", True):
            super().setUp()
            return
        original = ColumnarCbasUtil.cleanup_cbas
        ColumnarCbasUtil.cleanup_cbas = lambda *args, **kwargs: True
        try:
            super().setUp()
        finally:
            ColumnarCbasUtil.cleanup_cbas = original

    def setUp(self):
        self._base_setup()
        self.analytics_api = AnalyticsRestAPI(self.columnar_cluster.master)
        self.rbac_util = RBAC_Util(self.task, self.use_sdk_for_cbas)

        self.rebalance_util = CBASRebalanceUtil(
            self.cluster_util, self.bucket_util, self.task, False,
            self.cbas_util)
        if not hasattr(self.columnar_cluster, "available_servers"):
            self.columnar_cluster.available_servers = []

        self._run_tag = uuid4().hex[:8]
        self._expected_cbas_nodes = None
        self._audit = None
        self._pre_test_audit_settings = (
            self._snapshot_audit_settings()
            if self._testMethodName in self.AUDIT_TESTS else None)
        self._rbac_users_created = []
        self._cbo_fixture_ready = False
        if self._testMethodName not in self.TOPOLOGY_TESTS:
            self._create_ms_fixture()
        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.setUp.__name__)

    def tearDown(self):
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)
        self._restore_topology_if_shrunk()
        for username in self._rbac_users_created:
            try:
                self.rbac_util.delete_user(self.columnar_cluster, username)
            except Exception as e:
                self.log.warning(
                    f"Failed to delete RBAC user {username}: {e}")
        try:
            self._restore_audit_settings(self._pre_test_audit_settings)
        except Exception as e:
            self.log.warning(f"Failed to restore audit settings: {e}")
        super().tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.tearDown.__name__)

    def _wait_for_user(self, username, password, timeout=60):
        """Poll until analytics authenticates a freshly granted RBAC user.

        A role change takes a moment to reach the analytics service, and a
        fixed sleep either wastes that time or races it. This waits for the
        user to stop being reported as unauthenticated (20000), whatever
        authorization verdict it then gets - so it serves a user that should
        be allowed and one that should be refused equally.
        """
        deadline = time.time() + timeout
        while True:
            response = self._multi_statement("select 1;", multi_statement=None,
                username=username, password=password)
            errors = response.get("errors") or []
            if not errors or errors[0].get("code") != 20000:
                return
            if time.time() >= deadline:
                self.fail(
                    f"analytics still reported {username} as unauthenticated "
                    f"{timeout}s after the grant: {response}")
            time.sleep(1)

    def _wait_for_active_request(self, ccid, min_jobs=1, timeout=120):
        """Poll until a request carrying this client_context_id is in flight.

        :param min_jobs: wait until the request has created at least this many
            jobs. The resilience cases need statement 1 to have COMPLETED
            before they disturb the cluster - their premise is that its write
            is durable - so they wait for job 2 (the sleep) to exist rather
            than firing as soon as the request appears. A fixed sleep used to
            provide that delay incidentally; waiting on the jobs array states
            it.
        """
        deadline = time.time() + timeout
        while True:
            rows = self._results(
                "select value r from active_requests() r where "
                f"r.clientContextID = \"{ccid}\";")
            if rows and len(rows[0].get("jobs") or []) >= min_jobs:
                return rows
            if time.time() >= deadline:
                self.fail(
                    f"no active request with >= {min_jobs} job(s) appeared for "
                    f"{ccid} within {timeout}s (rows={rows})")
            time.sleep(1)

    def _wait_for_analytics(self, timeout=180):
        """Poll until analytics serves a trivial query again after a restart."""
        deadline = time.time() + timeout
        while True:
            status, _, errors, _ = self._run_statement("select 1;")
            if status == "success":
                return
            if time.time() >= deadline:
                self.fail(
                    f"analytics did not come back within {timeout}s: {errors}")
            time.sleep(5)

    def _rbac_user(self, label):
        """A username and password unique to this test run.

        Fixed names collide when two jobs share a cluster, and the password
        was a literal in the source - the root AGENTS.md forbids hard-coded
        credentials.
        """
        return f"{label}_{self._run_tag}", f"Pw1!{uuid4().hex[:12]}"

    def _create_rbac_user(self, label, role, failure_message=None):
        """Create a run-unique user holding one role, ready to be used.

        Registering for teardown and waiting out the auth cache are part of
        making the user usable, not extras: set_user_roles returns before
        analytics honours the grant, so a caller that proceeds straight to a
        query races it.
        """
        username, password = self._rbac_user(label)
        self.assertTrue(
            self.rbac_util.set_user_roles(
                self.columnar_cluster, username, password, role),
            failure_message or f"Failed to create user with {role}")
        self._rbac_users_created.append(username)
        self._wait_for_user(username, password)
        return username, password

    def _ccid(self, label):
        """A client_context_id unique to this test run.

        completed_requests() accumulates a row per run, so a fixed id makes
        results[0] ambiguous from the second run onward.
        """
        return f"{label}-{self._run_tag}"

    def _cluster_rest(self):
        """A ClusterRestAPI bound to a node that is still in the cluster.
        """
        candidates = [self.columnar_cluster.master] + [
            server for server in self.columnar_cluster.servers
            if server.ip != self.columnar_cluster.master.ip]
        for server in candidates:
            try:
                rest = ClusterRestAPI(server)
                ok, pool = rest.cluster_details()
                if ok and (pool or {}).get("nodes"):
                    return rest, pool
            except Exception as e:
                self.log.info(f"cluster_details via {server.ip} failed: {e}")
        return None, None

    def _cbas_node_count(self):
        """Active cbas nodes the cluster reports, read live over REST.
        """
        _, pool = self._cluster_rest()
        return sum(
            1 for node in (pool or {}).get("nodes", [])
            if "cbas" in (node.get("services") or [])
            and node.get("clusterMembership") == "active")

    def _restore_topology_if_shrunk(self):
        """Put back any cbas node a topology test removed from the cluster.

        Only the tests that change topology set `_expected_cbas_nodes`, so this
        is a no-op for every other test. NEVER raises: a teardown that throws
        would mask the test's own verdict, so a restore that cannot succeed is
        logged at ERROR naming the manual repair instead.
        """
        expected = getattr(self, "_expected_cbas_nodes", None)
        if expected is None:
            return
        try:
            actual = self._cbas_node_count()
            if actual >= expected:
                return
            self.log.warning(
                f"cluster has {actual} cbas node(s), started with {expected} - "
                "re-adding the missing one")
            rest, pool = self._cluster_rest()
            if rest is None:
                self.log.error(
                    "no node in the ini reports a live cluster - cannot "
                    "restore automatically; needs manual repair")
                return
            in_cluster = {n["otpNode"].split("@")[-1]
                          for n in (pool or {}).get("nodes", [])}
            failed_over = [node["otpNode"]
                           for node in (pool or {}).get("nodes", [])
                           if node.get("clusterMembership") == "inactiveFailed"]
            missing = [server for server in self.columnar_cluster.servers
                       if server.ip not in in_cluster]
            if not failed_over and not missing:
                self.log.error(
                    "cluster is short a cbas node but none is failed over or "
                    "outside it - cannot restore automatically")
                return
            for otp_node in failed_over:
                recovered, content = rest.set_recovery_type(otp_node, "full")
                self.log.info(
                    f"set_recovery_type {otp_node}: {recovered} {content}")
            for server in missing:
                added, content = rest.add_node(
                    server.ip, username=server.rest_username,
                    password=server.rest_password, services="kv,cbas")
                self.log.info(f"add_node {server.ip}: {added} {content}")
            _, pool = rest.cluster_details()
            known = [node["otpNode"] for node in (pool or {}).get("nodes", [])]
            started, content = rest.rebalance(known_nodes=known)
            if not started:
                self.log.error(
                    "COULD NOT start the restore rebalance - the cluster is "
                    f"left DEGRADED and needs manual repair: {content}")
                return
            deadline = time.time() + 600
            while time.time() < deadline:
                _, progress = rest.rebalance_progress()
                if (progress or {}).get("status") == "none":
                    break
                time.sleep(5)

            deadline = time.time() + 300
            while time.time() < deadline:
                if self._cbas_node_count() >= expected:
                    self.log.info(
                        f"topology restored: {expected} cbas node(s) back")
                    return
                time.sleep(10)
            self.log.error(
                "COULD NOT restore the cbas node - the cluster is left "
                "DEGRADED and needs manual repair ")
        except Exception as e:
            self.log.error(f"topology restore raised: {e}")

    def _ms_fixture_is_intact(self):
        """True when ms.one/ms.empty/ms.big are all present with the row counts
        MS_FIXTURE_STATEMENT builds.

        Any mismatch - absent, or contaminated by a test that wrote to them -
        returns False so the caller rebuilds, which keeps reuse self-correcting
        rather than assuming the fixture stayed pristine.
        """
        for collection, expected in self.MS_FIXTURE_SHAPE:
            try:
                status, _, _, results = self._run_statement(
                    f"SELECT VALUE COUNT(*) FROM {collection};")
            except Exception as e:
                self.log.info(
                    f"ms fixture probe on {collection} raised: {e}")
                return False
            if status != "success" or not results or results[0] != expected:
                self.log.info(
                    f"ms fixture probe: {collection} status={status} "
                    f"results={results}, expected {expected} - rebuilding")
                return False
        return True

    def _create_ms_fixture(self):
        """Build the shared ms.one/ms.empty/ms.big fixture from the TSV via a
        single multi-statement request.

        With skip_cbas_cleanup (see _base_setup) the fixture survives between
        tests, so an intact one is probed and kept rather than rebuilt for
        every test that needs it.
        """
        if self._ms_fixture_is_intact():
            self.log.info("ms fixture already present and intact - reusing it")
            return
        timeout = int(self.input.param("ms_fixture_timeout", 600))
        response = self._multi_statement(
            MS_FIXTURE_STATEMENT, multi_statement=True,
            analytics_timeout=timeout, http_timeout=timeout + 60)
        if response.get("status") not in ("success",):
            self.fail(f"Failed to build the ms fixture: {response}")

    def _create_cbo_fixture(self):
        """Build the skewed 100k-row cbo.big/cbo.small fixture used by
        cases 33-35, called only by the cases that need it since it is
        expensive to build.
        """
        if self._cbo_fixture_ready:
            return
        timeout = int(self.input.param("cbo_fixture_timeout", 600))
        response = self._multi_statement(
            CBO_FIXTURE_STATEMENT, multi_statement=True,
            analytics_timeout=timeout, http_timeout=timeout + 60)
        if response.get("status") not in ("success",):
            self.fail(f"Failed to build the cbo fixture: {response}")
        self._cbo_fixture_ready = True

    # ------------------------------------------------------------------
    # Assertion helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _governing_http_for_status(status_field, error_code=None):
        """The HTTP status the TSV's governing rule predicts for a response.
        """
        if status_field != "fatal":
            return 200
        return {20000: 401, 20001: 403}.get(error_code, 400)

    def _assert_flat_envelope(self, response, expected_status,
                              expected_error_code=None, response_obj=None):
        """Assert a flat (non-array) request-level envelope.

        The HTTP status is derived from the TSV's governing rule rather than
        restated per call: the rule is total, so an explicit code could only
        ever agree with it. It is asserted against the wire when response_obj
        (a requests.Response, from _multi_statement called with
        with_response=True) is supplied.
        """
        expected_http = self._governing_http_for_status(
            expected_status, expected_error_code)
        self.assertNotIn(
            "statements", response,
            f"Expected a flat envelope, found a statements array: {response}")
        self.assertEqual(
            response.get("status"), expected_status,
            f"status mismatch, full response: {response}")
        if response_obj is not None:
            self.assertEqual(
                response_obj.status_code, expected_http,
                f"wire HTTP status mismatch, full response: {response}")
        if expected_error_code is not None:
            errors = response.get("errors") or []
            self.assertTrue(
                errors and errors[0].get("code") == expected_error_code,
                f"error code mismatch, expected {expected_error_code}, "
                f"full response: {response}")

    @staticmethod
    def _statement_number(entry):
        for key in STATEMENT_NUMBER_KEYS:
            if key in entry:
                return entry[key]
        return None

    def _assert_response_ok(self, response, context="request"):
        """Assert a request-level envelope reports success.

        A bare assertEqual on the status reports only 'fatal' != 'success',
        which does not say why; the envelope carries the errors, so it goes
        in the message.
        """
        self.assertEqual(
            response.get("status"), "success",
            f"{context} did not succeed: {response}")

    def _assert_statements_array(self, response, expected_entries=None,
                                 expected_count=None, response_obj=None):
        """Assert response["statements"] and hand the array back.

        The array only ever appears on HTTP 200, per the TSV's governing rule,
        and its entries are always numbered 1..N in submission order - that
        numbering is checked for every entry regardless of what else is asked
        for.

        :param expected_entries: per-entry expectations, checked positionally.
            Each is a dict; every key is optional, and an absent key skips
            that check for that entry:
              status        - entry["status"]
              results       - entry["results"]
              error_code    - entry["errors"][0]["code"]
              metrics       - dict of expected entry["metrics"] sub-values
              has_keys      - keys that must be present on the entry
              missing_keys  - keys that must be absent from the entry
        :param expected_count: array length, for callers that assert the
            length but carry their own per-entry assertions.
        :returns: response["statements"], so a caller with case-specific
            assertions continues from it rather than re-indexing the response.
        """
        self.assertIn(
            "statements", response,
            f"Expected a statements array, full response: {response}")
        statements = response["statements"]
        if expected_count is None and expected_entries is not None:
            expected_count = len(expected_entries)
        if expected_count is not None:
            self.assertEqual(
                len(statements), expected_count,
                f"statements array length mismatch, full response: {response}")
        for i, entry in enumerate(statements):
            self.assertEqual(
                self._statement_number(entry), i + 1,
                f"entry {i} statement number mismatch: {entry}")
        for i, expected in enumerate(expected_entries or []):
            entry = statements[i]
            if "status" in expected:
                self.assertEqual(
                    entry.get("status"), expected["status"],
                    f"entry {i} status mismatch: {entry}")
            if "results" in expected:
                self.assertEqual(
                    entry.get("results"), expected["results"],
                    f"entry {i} results mismatch: {entry}")
            if "error_code" in expected:
                errors = entry.get("errors") or []
                self.assertTrue(
                    errors and errors[0].get("code") == expected["error_code"],
                    f"entry {i} error code mismatch: {entry}")
            for key, value in (expected.get("metrics") or {}).items():
                self.assertEqual(
                    (entry.get("metrics") or {}).get(key), value,
                    f"entry {i} metrics.{key} mismatch: {entry}")
            for key in expected.get("has_keys", ()):
                self.assertIn(
                    key, entry, f"entry {i} is missing {key}: {entry}")
            for key in expected.get("missing_keys", ()):
                self.assertNotIn(
                    key, entry, f"entry {i} should not carry {key}: {entry}")
        if response_obj is not None:
            self.assertEqual(
                response_obj.status_code, 200,
                f"a statements array must come back on HTTP 200, got "
                f"{response_obj.status_code}: {response}")
        return statements

    def _assert_entry_key_set(self, entry, kind):
        """Assert a successful entry carries exactly the keys its kind should.

        :param kind: "ddl" for a DDL/DML entry, "query" for one that returns
            rows. This is the strictest assertion in the suite - an extra key
            the product starts returning fails it, which is what case 5 asks
            for ("pins the exact field set").
        """
        expected = set(self.DDL_ENTRY_KEYS if kind == "ddl"
                       else self.QUERY_ENTRY_KEYS)

        present = [key for key in STATEMENT_NUMBER_KEYS if key in entry]
        self.assertEqual(
            len(present), 1,
            f"{kind} entry must carry exactly one position field out of "
            f"{list(STATEMENT_NUMBER_KEYS)}, got {present}: {entry}")
        expected.add(present[0])
        self.assertEqual(
            set(entry.keys()), expected,
            f"{kind} entry key set mismatch - expected exactly "
            f"{sorted(expected)}, got {sorted(entry.keys())}: {entry}")

    def _assert_request_metrics_keys_only(self, response, expected_keys):
        metrics = response.get("metrics") or {}
        self.assertEqual(
            set(metrics.keys()), set(expected_keys),
            f"top-level metrics carries more than {expected_keys}: {metrics}")

    def _snapshot_audit_settings(self):
        return RestConnection(self.columnar_cluster.master).getAuditSettings()

    def _restore_audit_settings(self, settings):
        if not settings:
            return
        rest = RestConnection(self.columnar_cluster.master)
        disabled = settings.get("disabled") or []
        if isinstance(disabled, list):
            disabled = ",".join(str(d) for d in disabled)
        users = settings.get("disabledUsers") or ""
        if isinstance(users, list):
            users = ",".join(users)
        rest.setAuditSettings(
            enabled=str(settings.get("auditdEnabled", False)).lower(),
            rotateInterval=settings.get("rotateInterval", 86400),
            logPath=settings["logPath"],
            disabled=disabled, users=users,
            rotateSize=settings.get("rotateSize", 524288000))

    def _set_audit_events_enabled(self, enable_ids, disable_ids=None):
        """Enable the given event ids and (optionally) explicitly disable
        others, preserving every other event's current enablement.

        Every other field of the audit settings is passed back unchanged.
        setAuditSettings defaults rotateInterval/rotateSize/disabledUsers when
        they are omitted, so leaving them out silently reset log rotation and
        cleared the whitelist until teardown repaired it.
        """
        rest = RestConnection(self.columnar_cluster.master)
        current = rest.getAuditSettings()
        disabled_set = {int(d) for d in (current.get("disabled") or [])}
        disabled_set -= set(enable_ids)
        disabled_set |= set(disable_ids or [])
        users = current.get("disabledUsers") or ""
        if isinstance(users, list):
            users = ",".join(str(u) for u in users)
        rest.setAuditSettings(
            enabled="true",
            rotateInterval=current.get("rotateInterval", 86400),
            logPath=current["logPath"],
            disabled=",".join(str(d) for d in sorted(disabled_set)),
            users=users,
            rotateSize=current.get("rotateSize", 524288000))

    def _resolve_handle_url(self, handle_url):
        """The absolute URL to GET for a handle, whatever shape it came in.

        A handle from /api/v1/request is a ROOTED RELATIVE path
        (/api/v1/request/result/<uuid>/<job>-<n>), which a client resolves
        against the base it sent the request to - there is no host in it to
        connect to on its own.

        An absolute handle is followed as given except for its scheme, which
        is aligned with the one this cluster is addressed on: a handle
        carrying http:// against a TLS listener is unfetchable as returned,
        and the port is echoed correctly, so only the scheme is rewritten.

        Cases that assert the handle's own shape do not come through here -
        see test_deferred_handles_are_usable_as_returned, which follows the
        handle verbatim so that a wrong one fails rather than being repaired.
        """
        parsed = urlparse(handle_url)
        if not parsed.scheme and not parsed.netloc:
            return urljoin(self.analytics_api.cbas_url, handle_url)
        if not CbServer.use_https:
            return handle_url
        return urlunparse(parsed._replace(scheme="https"))

    def _fetch_handle(self, handle_url):
        """GET a deferred/async handle, resolving it first.

        AnalyticsServiceAPI has no wrapper that takes a handle as the server
        hands it back, so the GET is issued directly.
        """
        return requests.get(
            self._resolve_handle_url(handle_url),
            auth=(self.columnar_cluster.master.rest_username,
                  self.columnar_cluster.master.rest_password),
            verify=False)

    def _multi_statement(self, statement, multi_statement=True,
                         with_response=False, **kwargs):
        """POST a statement to /api/v1/request and return the response
        envelope - the statements array when the request opted in and at least
        one real statement ran, the flat request-level envelope otherwise.

        The body is parsed even on a non-2xx, so an error envelope comes back
        as a dict like any other.

        :param multi_statement: True/False set the "multi-statement" body key;
            None omits it entirely, which is the "absent parameter" transport
            the gating cases need.
        :param with_response: also return the requests response object, for
            the cases that assert the wire status code.
        :param kwargs: further REST parameters, passed through by the names
            submit_service_request declares - mode, client_context_id,
            query_context, readonly, username, password, http_timeout,
            max_warnings, and analytics_timeout/time_out_unit for the body's
            request budget. Keys that must reach the wire hyphenated have no
            such name and go in extra_params instead, e.g.
            extra_params={"optimized-logical-plan": True}.
        """
        result, response_obj = self._service_request(
            statement, multi_statement=multi_statement, **kwargs)
        return (result, response_obj) if with_response else result

    def _submit_in_background(self, statement, **kwargs):
        """Start a multi-statement request on a thread and return (thread,
        result_holder).

        The resilience cases disturb the cluster while a request is in
        flight, so the request must not block the test. result_holder gets
        "response" on return, or "exception" if the call raised - the caller
        joins with its own timeout and reads whichever landed.

        join() is used rather than the task framework's get_task_result
        because a join that times out leaves the request running and lets the
        test go on to assert on cluster state; get_task_result raises
        instead, which several of these cases are written not to expect.
        """
        result_holder = {}

        def _submit():
            try:
                result_holder["response"] = self._multi_statement(
                    statement, **kwargs)
            except Exception as e:
                result_holder["exception"] = str(e)

        thread = threading.Thread(target=_submit)
        thread.start()
        return thread, result_holder

    @staticmethod
    def _completed_request_query(ccid, projection="r"):
        """A completed_requests() probe for one request, by context id.

        Keeps the predicate and its quoting in one place; the projection
        varies per case, the lookup does not.
        """
        return (f"select value {projection} from completed_requests() r "
                f"where r.clientContextID = \"{ccid}\";")

    def _run_statement(self, statement, **kwargs):
        """Run one statement and return (status, metrics, errors, results).

        For the callers that must inspect a failure rather than fail on it -
        the fixture probe and the post-restart retry loop. Everything else
        should use _run_ok/_results/_single_result, which turn a failed setup
        query into a diagnostic instead of an IndexError further down.
        """
        status, metrics, errors, results, _, _ = (
            self.cbas_util.execute_statement_on_cbas_util(
                self.columnar_cluster, statement, **kwargs))
        return status, metrics, errors, results

    def _run_ok(self, statement, **kwargs):
        """Run one statement that must succeed and return (metrics, results)."""
        status, metrics, errors, results = self._run_statement(
            statement, **kwargs)
        if status != "success":
            self.fail(f"query failed [{statement}]: status={status} "
                      f"errors={errors}")
        return metrics, results

    def _results(self, statement, **kwargs):
        """Rows from a statement that must succeed."""
        return self._run_ok(statement, **kwargs)[1]

    def _single_result(self, statement, **kwargs):
        """The one row a statement that must succeed returns.

        Every completed_requests()/count(*) probe in this suite expects
        exactly one row; asserting that here reports the real shape instead
        of raising IndexError on an empty result.
        """
        results = self._results(statement, **kwargs)
        self.assertEqual(
            len(results or []), 1,
            f"expected exactly one row from [{statement}], got: {results}")
        return results[0]

    # ------------------------------------------------------------------
    # Cases 1-15: Parameter gating, response shape, error semantics,
    # delivery modes, request parameter scoping
    # ------------------------------------------------------------------

    def test_gating_rejected_without_optin(self):

        for multi_statement in (None, False):
            response, response_obj = self._multi_statement(
                "select 1; select 2;", multi_statement=multi_statement,
                with_response=True)
            self._assert_flat_envelope(
                response, "fatal", 21003, response_obj=response_obj)

        response = self._multi_statement(
            "select 1; select 2;", multi_statement=True)
        self._assert_statements_array(response, [
            {"status": "success", "results": [{"$1": 1}]},
            {"status": "success", "results": [{"$1": 2}]}])

    def test_optin_executes_and_reports_every_statement(self):
        setup = (
            "drop scope ms2 if exists; create scope ms2; "
            "create dataset ms2.ds primary key (id:string); "
            "upsert into ms2.ds ([{\"id\":\"1\",\"age\":10}]); "
            "select * from ms2.ds;")
        response = self._multi_statement(setup, http_timeout=600)
        self._assert_statements_array(response, [
            {"status": "success"}, {"status": "success"},
            {"status": "success"}, {"status": "success"},
            {"status": "success",
             "results": [{"ds": {"id": "1", "age": 10}}]}])

        fifty = "".join(
            f"select {i} as n;" for i in range(1, 51))
        response = self._multi_statement(fifty, http_timeout=600)
        self._assert_statements_array(
            response,
            [{"status": "success", "results": [{"n": i}]}
             for i in range(1, 51)])

        statements = [f"select {i} as n;" for i in range(1, 51)]
        statements[24] = "select * from ms.noSuchCollection;"
        response = self._multi_statement("".join(statements),
                                         http_timeout=600)
        expected = [{"status": "success", "results": [{"n": i}]}
                    for i in range(1, 25)]
        expected.append({"status": "fatal", "error_code": 24045})
        self._assert_statements_array(response, expected)

    def test_single_real_statement_keeps_flat_envelope(self):
        response = self._multi_statement("select 1;", multi_statement=None)
        self._assert_flat_envelope(response, "success")
        self.assertEqual(response.get("results"), [{"$1": 1}])

        response = self._multi_statement("use ms; select * from one;",
            multi_statement=True)
        self._assert_flat_envelope(response, "success")
        self.assertEqual(
            response.get("results"), [{"one": {"id": 1, "v": "solo"}}])

    def test_use_and_set_take_position_but_dont_count(self):
        response = self._multi_statement("use ms;", multi_statement=False)
        self._assert_flat_envelope(response, "success")

        response = self._multi_statement(
            "use ms; set `compiler.parallelism` \"1\";",
            multi_statement=False)
        self._assert_flat_envelope(response, "success")

        response = self._multi_statement("use ms; select * from one;",
            multi_statement=True)
        self._assert_flat_envelope(response, "success")

        response = self._multi_statement(
            "use ms; select * from one; select 1;", multi_statement=True)
        self._assert_statements_array(response, [
            {"status": "success", "missing_keys": ("results",)},
            {"status": "success"},
            {"status": "success"}])

        response = self._multi_statement(
            "set `compiler.parallelism` \"1\"; select 1; select 2;",
            multi_statement=True)
        self._assert_statements_array(response, [
            {"status": "success"},
            {"status": "success", "results": [{"$1": 1}]},
            {"status": "success", "results": [{"$1": 2}]}])

        response = self._multi_statement("declare function dbl(x) { x * 2 };",
            multi_statement=None)
        self._assert_flat_envelope(response, "fatal", 24004)

    def test_per_statement_entry_shape_and_request_metrics(self):
        statement = (
            "drop dataset ms.full1 if exists; "
            "create dataset ms.full1 primary key (id: int); "
            "select * from ms.full1; "
            "upsert into ms.full1 [{\"id\": 1}]; "
            "select * from ms.full1;")
        response = self._multi_statement(statement, multi_statement=True)

        ddl = {"status": "success", "missing_keys": ("signature", "results")}
        statements = self._assert_statements_array(response, [
            ddl,
            ddl,
            {"status": "success", "results": [], "has_keys": ("signature",)},
            ddl,
            {"status": "success", "results": [{"full1": {"id": 1}}],
             "has_keys": ("signature",)}])
        for index in (0, 1, 3):
            self._assert_entry_key_set(statements[index], "ddl")
        for index in (2, 4):
            self._assert_entry_key_set(statements[index], "query")

        statement = (
            "drop dataset ms.kinds if exists; "
            "create dataset ms.kinds primary key (id: int); "
            "upsert into ms.kinds [{\"id\":1,\"g\":1},{\"id\":2,\"g\":2}]; "
            "insert into ms.kinds [{\"id\":3,\"g\":3}]; "
            "update ms.kinds set g = 9 where id = 1; "
            "delete from ms.kinds where id = 2; "
            "create index ix_g on ms.kinds(g: int); "
            "select count(*) from ms.kinds;")
        response = self._multi_statement(statement, multi_statement=True)
        statements = self._assert_statements_array(response, expected_entries=[
            {"status": "success",
             "missing_keys": ("signature", "results")}
            for _ in range(7)
        ] + [{"status": "success", "has_keys": ("signature", "results")}])
        for entry in statements[:-1]:
            self._assert_entry_key_set(entry, "ddl")
        self._assert_entry_key_set(statements[-1], "query")
        self.assertEqual(statements[-1]["results"], [{"$1": 2}])

        response = self._multi_statement(
            "select 1; select 2;", multi_statement=True)
        self._assert_request_metrics_keys_only(
            response, {"elapsedTime", "executionTime"})

    def test_failing_statement_stops_request_at_every_position(self):
        response = self._multi_statement(
            "select id from ms.one; select * from ms.nosuch; select 1;",
            multi_statement=True)
        self._assert_statements_array(response, [
            {"status": "success", "results": [{"id": 1}]},
            {"status": "fatal", "error_code": 24045}])

        response = self._multi_statement("select * from ms.nosuch; select 1;",
            multi_statement=True)
        self._assert_flat_envelope(response, "fatal", 24045)

        response = self._multi_statement(
            "select 1; select 2; select * from ms.nosuch;",
            multi_statement=True)
        self._assert_statements_array(response, [
            {"status": "success", "results": [{"$1": 1}]},
            {"status": "success", "results": [{"$1": 2}]},
            {"status": "fatal", "error_code": 24045}])

        dup_statement = (
            "create dataset ms.dup primary key (id:string); "
            "upsert into ms.dup ([{\"id\":\"1\"}]); "
            "select * from ms.dup;")
        self._results("drop dataset ms.dup if exists;")
        first = self._multi_statement(dup_statement, multi_statement=True)
        self._assert_response_ok(first)
        second = self._multi_statement(dup_statement, multi_statement=True)
        self._assert_flat_envelope(second, "fatal")
        self.assertEqual(
            self._single_result("select count(*) from ms.dup;")["$1"], 1)

    def test_syntax_error_keeps_response_flat(self):
        for multi_statement in (True, False):
            response = self._multi_statement("select 1; selct 2; select 3;",
                multi_statement=multi_statement)
            self._assert_flat_envelope(response, "fatal", 24000)

        response = self._multi_statement("use ms; select * fm one;",
            multi_statement=False)
        self._assert_flat_envelope(response, "fatal", 24000)

    def test_async_rejected_for_multiple_statements_unchanged_for_one(
            self):
        response = self._multi_statement(
            "select 1; select 2;", multi_statement=True, mode="async")
        self._assert_flat_envelope(response, "fatal", 24320)

        response = self._multi_statement(
            "select 1;", multi_statement=True, mode="async")
        self.assertNotIn("statements", response)
        self.assertIn(response.get("status"), ("queued", "running"))
        self.assertIn("handle", response)
        errors = response.get("errors") or []
        self.assertFalse(
            errors and errors[0].get("code") == 24320,
            f"single-statement async request was wrongly rejected: {response}"
            )
        request_id = response["requestID"]
        handle = response["handle"].split("/")[-1]
        completed, _, _ = self.analytics_api.wait_for_request_completion(
            request_id, handle, timeout=60, poll_interval=1)
        self.assertTrue(
            completed,
            f"the async request never reached a terminal state: {response}")
        _, body, handle_response = self.analytics_api.get_request_result(
            request_id, handle)
        self.assertEqual(handle_response.status_code, 200)
        self.assertEqual(body.get("results"), [{"$1": 1}])

    def test_deferred_returns_handle_per_statement(self):
        response = self._multi_statement("select id from ms.one; select 1;",
            multi_statement=True, mode="deferred")
        statements = self._assert_statements_array(response, expected_count=2)
        handle1 = statements[0]["handle"]
        handle2 = statements[1]["handle"]
        self.assertNotEqual(handle1, handle2)

        second_first = self._fetch_handle(handle2)
        self.assertEqual(second_first.status_code, 200)
        self.assertEqual(second_first.json().get("results"), [{"$1": 1}])
        first_second = self._fetch_handle(handle1)
        self.assertEqual(first_second.status_code, 200)
        self.assertEqual(first_second.json().get("results"), [{"id": 1}])

        handles_a = [
            entry["handle"] for entry in self._assert_statements_array(
                self._multi_statement("select 10; select 11;",
                    multi_statement=True, mode="deferred"),
                expected_count=2)]
        handles_b = [
            entry["handle"] for entry in self._assert_statements_array(
                self._multi_statement("select 20; select 21;",
                    multi_statement=True, mode="deferred"),
                expected_count=2)]
        self.assertFalse(
            set(handles_a) & set(handles_b),
            f"two independent requests were handed the same handle: "
            f"{handles_a} / {handles_b}")

        for label, handles, expected in (
                ("B", handles_b, [[{"$1": 20}], [{"$1": 21}]]),
                ("A", handles_a, [[{"$1": 10}], [{"$1": 11}]])):
            for handle, rows in zip(handles, expected, strict=True):
                fetch = self._fetch_handle(handle)
                self.assertEqual(
                    fetch.status_code, 200,
                    f"request {label}'s own handle was refused: {fetch.text}")
                self.assertEqual(
                    fetch.json().get("results"), rows,
                    f"request {label}'s handle returned another request's "
                    f"rows: {fetch.text}")

    def test_explain_and_advise_report_per_statement(self):
        response = self._multi_statement("EXPLAIN select 1; EXPLAIN select 2;",
            multi_statement=True)
        statements = self._assert_statements_array(response, expected_entries=[
            {"status": "success", "missing_keys": ("handle",)},
            {"status": "success", "missing_keys": ("handle",)}])
        for entry in statements:
            self.assertIn("distribute-result", json.dumps(entry["results"]))

        response = self._multi_statement("ADVISE select 1; ADVISE select 2;",
            multi_statement=True)
        statements = self._assert_statements_array(response, expected_entries=[
            {"status": "success"}, {"status": "success"}])
        for entry in statements:
            payload = json.dumps(entry["results"])
            self.assertIn("Advise", payload)
            self.assertIn("recommended_indexes", payload)
        self._assert_request_metrics_keys_only(
            response, {"elapsedTime", "executionTime"})

    def test_max_warnings_is_a_budget_per_statement(self):
        """max-warnings caps each statement's warnings LIST; warningCount
        reports every occurrence the statement raised, independent of the cap.

        The fixture deliberately uses a query whose warnings REPEAT. This case
        used to assert the same property with `select 1="1", 2="2", 3="3"`,
        whose three warnings each fire exactly once - the one shape where
        "occurrences" and "distinct warnings" are the same number, so the
        assertion held for either definition. MB-74218 was a real divergence
        between the two that this case therefore could not see. Repeating
        warnings separate them: 3 warning sites over 1000 rows is 3 distinct
        warnings and 3000 occurrences, and only the occurrence count is
        correct. Same class of blind spot case 18 closed for resultCount.
        """
        statement = (
            "select 1 = \"1\"; select 1 = \"1\"; select 1 = \"1\";")
        response = self._multi_statement(
            statement, multi_statement=True, max_warnings=2)
        statements = self._assert_statements_array(
            response,
            expected_entries=[{"status": "success"} for _ in range(3)])
        total_warnings = sum(
            len(entry.get("warnings") or []) for entry in statements)
        self.assertEqual(total_warnings, 3)

        repeating = (
            "select value {\"a\": 1/(b-500), \"b\": 1/(b-501), "
            "\"c\": 1/(b-502)} from ms.big b;")
        statement2 = repeating + " select 4 = \"4\";"
        response = self._multi_statement(
            statement2, multi_statement=True, max_warnings=1)
        entry1, entry2 = self._assert_statements_array(
            response, expected_count=2)

        self.assertEqual(len(entry1.get("warnings") or []), 1)
        self.assertEqual(
            entry1["metrics"].get("warningCount"), 3000,
            "entry 1 raised 3 warnings on each of ms.big's 1000 rows; "
            "warningCount must report all 3000 occurrences regardless of the "
            "max-warnings=1 cap on the list")
        self.assertEqual(len(entry2.get("warnings") or []), 1)
        self.assertEqual(entry2["metrics"].get("warningCount"), 1)

        response = self._multi_statement(statement2, multi_statement=True)
        entry1, entry2 = self._assert_statements_array(
            response, expected_count=2)
        self.assertEqual(len(entry1.get("warnings") or []), 0)
        self.assertEqual(len(entry2.get("warnings") or []), 0)
        for position, (entry, expected) in enumerate(
                ((entry1, 3000), (entry2, 1)), start=1):
            self.assertEqual(
                entry["metrics"].get("warningCount"), expected,
                f"at the default max-warnings entry {position} listed no "
                "warnings, but metrics.warningCount must still report its "
                f"{expected} occurrences - got "
                f"{entry['metrics'].get('warningCount', 'ABSENT')}")

    def test_timeout_bounds_the_whole_request(self):
        statement = (
            "select id from ms.one; "
            "select value sleep(\"nope\", 60000);")
        start = time.time()
        response = self._multi_statement(statement, multi_statement=True,
            analytics_timeout=2, time_out_unit="s", http_timeout=30)
        elapsed = time.time() - start
        self._assert_flat_envelope(response, "timeout", 21002)
        self.assertTrue(
            response["errors"][0].get("retriable") is True,
            f"timeout error not marked retriable: {response}")

        self.assertLess(
            elapsed, 15,
            f"the request took {elapsed:.1f}s against a 2s request-level "
            "budget - the timeout did not bound the whole request")

    def test_readonly_rejects_whole_request_not_one_statement(self):
        response = self._multi_statement(
            "select 1; upsert into ms.one [{\"id\":99}]; select 2;",
            multi_statement=True, readonly=True)
        self._assert_flat_envelope(response, "fatal", 23029)

        self.assertEqual(
            self._single_result(
                "select value count(*) from ms.one where id = 99;"), 0)

        response = self._multi_statement(
            "select 1; select 2;", multi_statement=True, readonly=True)
        self._assert_statements_array(response, [
            {"status": "success", "results": [{"$1": 1}]},
            {"status": "success", "results": [{"$1": 2}]}])

    def test_query_context_is_implicit_use_ahead_of_every_statement(
            self):
        response = self._multi_statement(
            "select * from one; select id from big;", multi_statement=True,
            query_context="default:ms")
        self._assert_statements_array(response, expected_entries=[
            {"results": [{"one": {"id": 1, "v": "solo"}}]},
            {"metrics": {"resultCount": 1000}}])

        response = self._multi_statement(
            "select * from one; select id from big;", multi_statement=True,
            query_context="default:noSuchScope")
        self._assert_flat_envelope(response, "fatal", 24034)

        response = self._multi_statement(
            "select 1; use noSuchScope; select 2;", multi_statement=True)
        self._assert_flat_envelope(response, "fatal", 24034)

        response = self._multi_statement(
            "select 1; select 2;", multi_statement=True,
            query_context="bogus:scope")
        self.assertNotIn("statements", response)
        self.assertEqual(response.get("status"), "fatal")
        errors = response.get("errors") or []
        self.assertTrue(
            errors and "query_context" in errors[0].get("msg", ""),
            f"expected the query_context parameter named in the error: {response}"
            )

    def test_plans_and_the_plan_cache(self):
        two_queries = (
            "select b.id, o.v from ms.big b join ms.one o on "
            "b.grp = o.id; "
            "select grp, count(*) as c from ms.big group by grp "
            "order by grp;")
        response = self._multi_statement(
            two_queries, multi_statement=True,
            extra_params=PLAN_DEBUG_PARAMS)
        entry1, entry2 = self._assert_statements_array(
            response, expected_entries=[
                {"metrics": {"resultCount": 100}},
                {"metrics": {"resultCount": 10}}])
        self.assertNotEqual(entry1["plans"], entry2["plans"])
        self.assertIn("join", json.dumps(entry1["plans"]))
        self.assertIn("group-by", json.dumps(entry2["plans"]))

        two_queries_no_order = (
            "select b.id, o.v from ms.big b join ms.one o on "
            "b.grp = o.id; "
            "select grp, count(*) as c from ms.big group by grp;")
        cleared, content, _ = self.analytics_api.clear_plan_cache()
        self.assertTrue(
            cleared, f"failed to clear the plan cache before the cold-cache "
                     f"assertions: {content}")
        response = self._multi_statement(
            two_queries_no_order, multi_statement=True)
        statements = self._assert_statements_array(response, expected_count=2)
        self.assertNotIn("cachedPlan", statements[0])
        self.assertFalse(response.get("cachedPlan"))
        response = self._multi_statement(
            two_queries_no_order, multi_statement=True)
        self.assertTrue(response.get("cachedPlan"))
        self._assert_statements_array(response, expected_entries=[
            {"metrics": {"resultCount": 100}},
            {"metrics": {"resultCount": 10}}])

    # ------------------------------------------------------------------
    # Cases 16-17: Authorization
    # ------------------------------------------------------------------

    def test_authorization_is_per_statement_for_every_write_kind(
            self):
        setup = (
            "drop scope ms_rbac if exists; create scope ms_rbac; "
            "create dataset ms_rbac.ds primary key (id:string); "
            "upsert into ms_rbac.ds ([{\"id\":\"1\"}]);")
        response = self._multi_statement(setup, multi_statement=True)
        self._assert_response_ok(response)

        username, password = self._create_rbac_user(
            "ms16user", "analytics_access")

        write_statements = [
            "upsert into ms_rbac.ds ([{\"id\":\"2\",\"age\":10}])",
            "insert into ms_rbac.ds ([{\"id\":\"8\"}])",
            "delete from ms_rbac.ds where id = \"1\"",
            "truncate collection ms_rbac.ds",
            "create index ms16_ix_a on ms_rbac.ds(age: int)",
            "create dataset ms_rbac.ds2 primary key (id:string)"]
        for write_statement in write_statements:
            statement = f"select 1; {write_statement}; select 2;"
            response = self._multi_statement(statement, multi_statement=True,
                username=username, password=password)
            self._assert_statements_array(response, [
                {"status": "success", "results": [{"$1": 1}]},
                {"status": "fatal", "error_code": 20001}])

        self.assertEqual(
            self._single_result(
                "select value count(*) from ms_rbac.ds;"), 1)

        response = self._multi_statement(
            "select 1; upsert into ms_rbac.ds ([{\"id\":\"3\"}]); "
            "select 2;", multi_statement=True)
        self._assert_statements_array(response, [
            {"status": "success"}, {"status": "success"},
            {"status": "success"}])

    def test_request_level_check_still_refuses_whole_request(self):
        username, password = self._create_rbac_user(
            "ms17user", "ro_admin", "Failed to create user with ro_admin only")

        response, response_obj = self._multi_statement(
            "select 1; select 2;", multi_statement=True, username=username,
            password=password, with_response=True)
        self._assert_flat_envelope(
            response, "fatal", 20001, response_obj=response_obj)

        response, response_obj = self._multi_statement(
            "select 1; select 2;", multi_statement=True, username=username,
            password="wrong-password", with_response=True)
        self._assert_flat_envelope(
            response, "fatal", 20000, response_obj=response_obj)
        challenge = response_obj.headers.get("www-authenticate", "")
        self.log.info(f"case17 www-authenticate: {challenge}")
        self.assertIn(
            "Basic", challenge,
            f"a 401 must carry a Basic auth challenge: headers="
            f"{dict(response_obj.headers)}")

    # ------------------------------------------------------------------
    # Cases 18-19: Statement kinds and query complexity
    # ------------------------------------------------------------------

    def test_per_statement_metrics_are_the_statements_own(self):
        response = self._multi_statement(
            "select * from ms.empty; select * from ms.one; "
            "select id from ms.big;", multi_statement=True)
        entry1, entry2, entry3 = self._assert_statements_array(
            response, expected_entries=[
                {"metrics": {"resultCount": 0}},
                {"metrics": {"resultCount": 1}},
                {"metrics": {"resultCount": 1000}}])
        self.assertNotIn("bufferCacheHitRatio", entry1["metrics"])
        self.assertIn("bufferCacheHitRatio", entry2["metrics"])
        self.assertIn("bufferCacheHitRatio", entry3["metrics"])
        self._assert_request_metrics_keys_only(
            response, {"elapsedTime", "executionTime"})

        for standalone_statement, entry in (
                ("select * from ms.empty;", entry1),
                ("select * from ms.one;", entry2),
                ("select id from ms.big;", entry3)):
            metrics, _ = self._run_ok(standalone_statement)
            self.assertEqual(
                metrics.get("resultCount"), entry["metrics"]["resultCount"])

    def test_non_trivial_queries_joins_subqueries_and_aggregates(
            self):
        response = self._multi_statement(
            "drop analytics function ms.dbl(x) if exists; "
            "create analytics function ms.dbl(x) { x * 2 };",
            multi_statement=True)
        self._assert_response_ok(response)

        queries = [
            "select b.id, o.v from ms.big b join ms.one o on b.grp = o.id;",
            "select grp, count(*) as c from ms.big group by grp order by grp;",
            "select value b.id from ms.big b where b.grp in "
            "(select value o.id from ms.one o);",
            "select value u from ms.one o unnest [1,2,3] u;",
            "select value ms.dbl(b.id) from ms.big b where b.id <= 3;"]
        statement = " ".join(queries)
        response = self._multi_statement(statement, multi_statement=True)
        statements = self._assert_statements_array(response, expected_entries=[
            {"metrics": {"resultCount": 100}},
            {"metrics": {"resultCount": 10}},
            {"metrics": {"resultCount": 100}},
            {"results": [1, 2, 3]},
            {"results": [2, 4, 6]}])

        for query, entry in zip(queries, statements, strict=True):
            self.assertEqual(
                self._norm(entry.get("results")),
                self._norm(self._results(query)),
                f"entry rows diverged from the standalone run of {query}")

        response = self._multi_statement(
            statement, multi_statement=True,
            extra_params=PLAN_DEBUG_PARAMS)
        statements = self._assert_statements_array(response, expected_count=5)
        plans = [json.dumps(entry["plans"]) for entry in statements]
        self.assertEqual(
            len(set(plans)), len(plans),
            "two entries reported the same plan - plans are not per statement")
        for operator in ("join", "data-scan"):
            self.assertIn(operator, plans[0], f"JOIN plan lacks {operator}")
        for operator in ("group-by", "aggregate", "subplan"):
            self.assertIn(operator, plans[1], f"GROUP BY plan lacks {operator}")

    # ------------------------------------------------------------------
    # Cases 20-22: Auditing
    # ------------------------------------------------------------------

    @staticmethod
    def _normalise_statement(text):
        """Collapse whitespace and drop a trailing semicolon, so a submitted
        statement can be compared with the text the audit record carries."""
        return " ".join((text or "").split()).rstrip(";").strip()

    def _audit_session(self):
        """One `audit` object per test - building it opens an SSH session and
        makes a REST call, so it is built once and reused."""
        if self._audit is None:
            self._audit = audit(host=self.columnar_cluster.master)
        return self._audit

    def _audit_log(self):
        """The node's current audit log, as (raw_text, parsed_records).

        `audit.returnEvent()` returns only the LAST record of one event id
        (audit_ready_functions.py: `return data[len(data) - 1]`), which cannot
        express "exactly N records for this request" - what cases 20-22 are
        actually about - and cannot search the whole log for a leaked
        credential. This reads the same downloaded file and returns all of it.
        """
        auditing = self._audit_session()
        auditing.readFile(auditing.pathLogFile, audit.AUDITLOGFILENAME)
        with open(audit.DOWNLOADPATH + audit.AUDITLOGFILENAME) as log_file:
            raw = log_file.read()
        records = []
        for line in raw.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except ValueError:
                continue
        return raw, records

    def _audit_records_for(self, response, expected_count=None, timeout=60):
        """Every audit record one request wrote, in log order.

        Records are matched on the `requestId` the response carries.
        Auditing is asynchronous, so when expected_count is given this
        polls until that many have appeared rather than racing the writer.
        """
        request_id = response.get("requestID")
        self.assertTrue(
            request_id,
            f"response carries no requestID to audit against: {response}")
        deadline = time.time() + timeout
        while True:
            records = [r for r in self._audit_log()[1]
                       if r.get("requestId") == request_id]
            if expected_count is None or len(records) >= expected_count:
                break
            if time.time() >= deadline:
                break
            time.sleep(2)
        if expected_count is not None:
            self.assertEqual(
                len(records), expected_count,
                f"expected {expected_count} audit record(s) for request "
                f"{request_id}, found {len(records)}: "
                f"{[(r.get('id'), r.get('statement')) for r in records]}")
        return records

    def _assert_audit_statements(self, records, expected_statements):
        """Assert the records carry exactly these statement texts, in order."""
        self.assertEqual(
            [self._normalise_statement(r.get("statement")) for r in records],
            [self._normalise_statement(t) for t in expected_statements],
            f"audit record statement texts mismatch: "
            f"{[(r.get('id'), r.get('statement')) for r in records]}")

    def test_one_audit_event_per_executed_statement(self):
        self._set_audit_events_enabled(
            enable_ids=[36867, 36870, 36906, 36910, 36911])

        statement = (
            "drop dataset ms.audit_ds if exists; "
            "create dataset ms.audit_ds primary key (id: int); "
            "upsert into ms.audit_ds [{\"id\": 1}]; "
            "select value id from ms.audit_ds;")
        response = self._multi_statement(statement, multi_statement=True)
        self._assert_response_ok(response)

        records = self._audit_records_for(response, expected_count=3)
        self._assert_audit_statements(records, [
            "create dataset ms.audit_ds primary key (id: int)",
            "upsert into ms.audit_ds [{\"id\": 1}]",
            "select value id from ms.audit_ds"])
        self.assertEqual(
            [r.get("id") for r in records], [36870, 36906, 36867],
            f"each statement must be audited under its own event id: "
            f"{[(r.get('id'), r.get('name')) for r in records]}")

        self.assertNotIn(
            self._normalise_statement(statement),
            [self._normalise_statement(r.get("statement")) for r in records],
            "a record carried the full request text instead of one statement")

        response = self._multi_statement(
            "select 1; explain select 1; advise select 1;",
            multi_statement=True)
        self._assert_response_ok(response)

        records = self._audit_records_for(response, expected_count=3)
        self.assertEqual(
            [r.get("id") for r in records], [36867, 36910, 36911],
            f"EXPLAIN/ADVISE must be audited under their own ids, not as "
            f"SELECT: {[(r.get('id'), r.get('statement')) for r in records]}")
        self._assert_audit_statements(
            records, ["select 1", "explain select 1", "advise select 1"])

        response = self._multi_statement(
            "select value \"alone\";", multi_statement=True)
        self._assert_response_ok(response)
        records = self._audit_records_for(response, expected_count=1)
        self.assertEqual(records[0].get("id"), 36867)
        self.assertEqual(records[0].get("status"), "success")

    def test_failure_audit_per_statement_never_run_and_request_level(
            self):
        self._set_audit_events_enabled(enable_ids=[36867, 36879])

        response = self._multi_statement(
            "select 1; select * from ms.noSuchCollection; select 999;",
            multi_statement=True)
        self._assert_statements_array(response, expected_count=2)
        records = self._audit_records_for(response, expected_count=2)
        self._assert_audit_statements(
            records, ["select 1", "select * from ms.noSuchCollection"])
        self.assertEqual(records[0].get("status"), "success")
        self.assertEqual(
            records[1].get("status"), "errors",
            f"the failing statement must be audited as errors: {records[1]}")

        whole_text = "select * from ms.noSuchCollection; select 1;"
        response = self._multi_statement(whole_text, multi_statement=True)
        self._assert_flat_envelope(response, "fatal", 24045)
        records = self._audit_records_for(response, expected_count=1)
        self._assert_audit_statements(records, [whole_text])
        self.assertEqual(records[0].get("status"), "errors")

        whole_text = "select 1; select 2;"
        response = self._multi_statement(whole_text, multi_statement=None)
        self._assert_flat_envelope(response, "fatal", 21003)
        records = self._audit_records_for(response, expected_count=1)
        self._assert_audit_statements(records, [whole_text])
        self.assertEqual(
            records[0].get("id"), 36867,
            f"a refused request is audited under the first statement's kind, "
            f"not UNRECOGNIZED: {records[0]}")

        whole_text = "select 1; select * fom nothing;"
        response = self._multi_statement(whole_text, multi_statement=True)
        self._assert_flat_envelope(response, "fatal", 24000)
        records = self._audit_records_for(response, expected_count=1)
        self.assertEqual(
            records[0].get("id"), 36879,
            f"a parse failure must be audited as UNRECOGNIZED: {records[0]}")
        self._assert_audit_statements(records, [whole_text])

    def test_per_event_enablement_and_sensitive_statements(self):
        self._set_audit_events_enabled(
            enable_ids=[36867], disable_ids=[36870, 36906])

        response = self._multi_statement(
            "drop dataset ms.audit_ds2 if exists; "
            "create dataset ms.audit_ds2 primary key (id: int); "
            "upsert into ms.audit_ds2 [{\"id\": 1}]; "
            "select value id from ms.audit_ds2;", multi_statement=True)
        self._assert_response_ok(response)

        records = self._audit_records_for(response, expected_count=1)
        self.assertEqual(records[0].get("id"), 36867)
        self._assert_audit_statements(
            records, ["select value id from ms.audit_ds2"])

        response = self._multi_statement(
            "select 1; create link ms22cbl type couchbase with "
            "{\"password\": \"s3cret\"};", multi_statement=True)

        self._assert_statements_array(response, [
            {"status": "success", "results": [{"$1": 1}]},
            {"status": "fatal"}])

        records = self._audit_records_for(response, expected_count=1)
        self._assert_audit_statements(records, ["select 1"])

        raw_log, _ = self._audit_log()
        self.assertNotIn(
            "s3cret", raw_log,
            "the CREATE LINK password reached the audit log")

    # ------------------------------------------------------------------
    # Cases 23-27: Request listings / jobs
    # ------------------------------------------------------------------

    def test_one_job_per_statement_and_flat_job_fields_describe_request(
            self):
        ccid = self._ccid("ms-jobs-completed")
        response = self._multi_statement(
            "select 1; select 2;", multi_statement=True,
            client_context_id=ccid)
        self._assert_statements_array(response, expected_count=2)

        record = self._single_result(
                "select value {\"noJobIdOfItsOwn\": r.jobId is null, "
                "\"createdWithTheFirstJob\": r.jobCreateTime = "
                "r.jobs[0].jobCreateTime, \"endedWithTheLastJob\": "
                "r.jobEndTime = r.jobs[1].jobEndTime, "
                "\"statements\": (select value " + JOB_NUMBER_SQL +
                " from r.jobs j)} "
                "from completed_requests() r where r.clientContextID = "
                f"\"{ccid}\";")
        self.assertEqual(record, {
            "noJobIdOfItsOwn": True, "createdWithTheFirstJob": True,
            "endedWithTheLastJob": True, "statements": [1, 2]})

        record = self._single_result(
            self._completed_request_query(ccid))
        jobs = record.get("jobs") or []
        self.assertEqual(
            [self._statement_number(job) for job in jobs], [1, 2],
            f"jobs array must name one statement each: {jobs}")
        self.assertEqual(
            [job.get("jobStatus") for job in jobs],
            ["TERMINATED", "TERMINATED"], f"unexpected job statuses: {jobs}")
        self.assertEqual(
            len({job.get("jobId") for job in jobs}), 2,
            f"each statement must get its own job id: {jobs}")
        self.assertEqual(record.get("clientContextID"), ccid)
        self.assertEqual(
            self._normalise_statement(record.get("statement")),
            self._normalise_statement("select 1; select 2;"),
            f"the record must carry the full submitted text: {record}")
        self.assertEqual(record.get("scanConsistency"), "not_bounded")

        open_ccid = self._ccid("ms-jobs-open")
        response = self._multi_statement("select 1; select 2;",
            multi_statement=True, mode="deferred",
            client_context_id=open_ccid)
        self._assert_statements_array(response, expected_count=2)

        open_rows = self._results(
            "select value r from open_requests() r where "
            f"r.clientContextID = \"{open_ccid}\";")
        self.log.info(
            f"case23 open_requests() for an unfetched deferred request: "
            f"{open_rows}")
        completed_state = self._single_result(
            self._completed_request_query(open_ccid, "r.state"))
        self.assertEqual(
            completed_state, "completed",
            "a deferred request whose handles were never fetched should still "
            f"be accounted for somewhere; got state {completed_state}")

        peak_ccid = self._ccid("ms-jobs-peak")
        response = self._multi_statement(
            "select * from ms.empty; select * from ms.one; "
            "select id from ms.big;", multi_statement=True,
            client_context_id=peak_ccid)
        self._assert_statements_array(response, expected_count=3)
        record = self._single_result(
            self._completed_request_query(peak_ccid))
        jobs = record.get("jobs") or []
        self.assertEqual(len(jobs), 3, f"expected three jobs: {jobs}")
        for field in ("jobRequiredMemory", "jobRequiredCPUs"):
            per_job = [job.get(field) for job in jobs]
            self.log.info(
                f"case23 {field}: per-job {per_job}, top-level "
                f"{record.get(field)}")
            self.assertEqual(
                record.get(field), max(per_job),
                f"top-level {field} must be the peak across jobs, got "
                f"{record.get(field)} for per-job {per_job}")
            if sum(per_job) != max(per_job):
                self.assertNotEqual(
                    record.get(field), sum(per_job),
                    f"top-level {field} is the SUM across jobs, not the peak")

    def test_single_statement_request_reports_both_flat_and_array(
            self):
        ccid = self._ccid("ms-jobs-single")
        response = self._multi_statement("select 1;", multi_statement=None,
            client_context_id=ccid)
        self._assert_response_ok(response)

        record = self._single_result(
                "select value {\"jobIdIsNull\": r.jobId is null, "
                "\"nJobs\": array_length(r.jobs), \"statements\": "
                "(select value " + JOB_NUMBER_SQL + " from r.jobs j)} from "
                "completed_requests() r where r.clientContextID = "
                f"\"{ccid}\";")
        self.assertEqual(record, {
            "jobIdIsNull": False, "nJobs": 1, "statements": [1]})

    def test_plans_belong_to_their_statements(self):
        ccid = self._ccid("ms-jobs-plan")
        response = self._multi_statement(
            "select 1; select 2;", multi_statement=True,
            extra_params={"optimized-logical-plan": True},
            client_context_id=ccid)
        self._assert_statements_array(response, expected_count=2)

        record = self._single_result(
                "select value {\"noPlanOfItsOwn\": r.plan is missing, "
                "\"eachJobHasItsOwn\": r.jobs[0].plan != r.jobs[1].plan} "
                "from completed_requests() r where r.clientContextID = "
                f"\"{ccid}\";")
        self.assertEqual(record, {
            "noPlanOfItsOwn": True, "eachJobHasItsOwn": True})

    def test_statements_that_run_no_job_shorten_jobs_array(self):
        ccid = self._ccid("ms-jobs-set")
        response = self._multi_statement(
            "set `compiler.parallelism` \"1\"; select id from ms.one; "
            "select id from ms.big;", multi_statement=True,
            client_context_id=ccid)
        self._assert_statements_array(response, expected_count=3)

        record = self._single_result(
                "select value {\"nJobs\": array_length(r.jobs), "
                "\"statements\": (select value " + JOB_NUMBER_SQL +
                " from r.jobs j), "
                "\"jobIdIsNull\": r.jobId is null} from "
                "completed_requests() r where r.clientContextID = "
                f"\"{ccid}\";")
        self.assertEqual(record, {
            "nJobs": 2, "statements": [2, 3], "jobIdIsNull": True})

        explain_ccid = self._ccid("ms-jobs-explain")
        response = self._multi_statement("explain select 1; explain select 2;",
            multi_statement=True,
            client_context_id=explain_ccid)
        self._assert_statements_array(response, expected_count=2)
        record = self._single_result(
            "select value {\"nJobs\": array_length(r.jobs), "
            "\"nStatements\": 2} from completed_requests() r where "
            f"r.clientContextID = \"{explain_ccid}\";")
        self.assertIn(
            record["nJobs"], (0, None),
            f"EXPLAIN statements must create no job: {record}")

    def test_active_requests_and_cancellation(self):
        ccid = self._ccid("ms-active")
        thread, result_holder = self._submit_in_background(
            "select id from ms.one; "
            "select value sleep(\"nope\", 60000);",
            multi_statement=True, http_timeout=90,
            client_context_id=ccid)

        active = self._wait_for_active_request(ccid)
        self.log.info(f"active_requests() for {ccid}: {active}")
        self.assertEqual(len(active), 1, f"expected one active request: {active}")
        self.assertEqual(
            active[0].get("state"), "running",
            f"an in-flight request must be listed as running: {active[0]}")
        self.assertIsNone(
            active[0].get("jobId"),
            "top-level jobId must be null for a multi-statement request")
        self.assertTrue(
            active[0].get("jobs"),
            f"active_requests() must list the jobs created so far: "
            f"{active[0]}")

        cancel_status, cancel_content, _ = (
            self.analytics_api.cancel_request_by_context_id(ccid))
        self.assertTrue(
            cancel_status, f"cancel request failed: {cancel_content}")

        thread.join(timeout=90)
        response = result_holder.get("response") or {}
        self._assert_response_ok(response)
        self._assert_statements_array(response, expected_entries=[
            {"status": "success"},
            {"status": "fatal", "error_code": 23010}])

        record = self._single_result(
            self._completed_request_query(ccid))
        self.assertEqual(record.get("state"), "cancelled")
        jobs = record.get("jobs") or []
        self.assertEqual(
            [(self._statement_number(job), job.get("jobStatus"))
             for job in jobs],
            [(1, "TERMINATED"), (2, "FAILURE")],
            f"only the jobs actually created should be listed, with the "
            f"cancelled one marked FAILURE: {jobs}")

        self.assertEqual(
            record.get("jobStatus"), "FAILURE",
            f"top-level jobStatus must mirror the failed job, not the first "
            f"one: {record.get('jobStatus')}")

    # ------------------------------------------------------------------
    # Cases 28-31: Resilience / topology change (GROUP=destructive)
    # ------------------------------------------------------------------

    def _build_topology_fixture(self, statement):
        """Build a topology case's own fixture and assert it landed.

        setUp skips the shared ms fixture for these cases (they shrink the
        cluster, so each builds exactly what it needs). The build runs
        against a cluster that is about to be disturbed, hence the long
        budget - with the socket budget held above the request budget so the
        client cannot give up before the server does.
        """
        response = self._multi_statement(
            statement, multi_statement=True,
            analytics_timeout=600, http_timeout=660)
        self._assert_response_ok(response, "topology fixture build")

    def test_rebalance_starting_mid_request(self):
        self._expected_cbas_nodes = self._cbas_node_count()
        rebal_ccid = self._ccid("ms-rebal")
        setup = (
            "drop scope ms if exists; create scope ms; "
            "create dataset ms.one primary key (id: int); "
            "upsert into ms.one [{\"id\":1,\"v\":\"solo\"}];")
        self._build_topology_fixture(setup)

        thread, result_holder = self._submit_in_background(
            "upsert into ms.one [{\"id\":50,"
            "\"v\":\"pre-rebalance\"}]; "
            "select value sleep(\"hold\", 30000); "
            "create dataset ms.during_rebal primary key (id: int);",
            multi_statement=True, analytics_timeout=120,
            http_timeout=180,
            client_context_id=rebal_ccid)
        self._wait_for_active_request(rebal_ccid, min_jobs=2)

        rebalance_task, self.columnar_cluster.available_servers = (
            self.rebalance_util.rebalance(
                cluster=self.columnar_cluster, cbas_nodes_out=1,
                available_servers=self.columnar_cluster.available_servers,
                wait_for_complete=True))

        thread.join(timeout=180)
        response = result_holder.get("response")
        self.log.info(
            "case28 observed response (NOT YET MEASURED baseline - "
            f"capture and promote): {response}")

        self.assertEqual(
            self._single_result(
                "select value count(*) from ms.one where "
                "v = \"pre-rebalance\";"),
            1,
            "statement 1's write must be durable after the rebalance even "
            "though the request as a whole may report failure")

    def test_analytics_service_killed_and_restarted_mid_request(
            self):
        kill_ccid = self._ccid("ms-kill")
        setup = (
            "drop scope ms if exists; create scope ms; "
            "create dataset ms.one primary key (id: int); "
            "upsert into ms.one [{\"id\":1,\"v\":\"solo\"}];")
        self._build_topology_fixture(setup)

        thread, result_holder = self._submit_in_background(
            "upsert into ms.one [{\"id\":60,"
            "\"v\":\"pre-kill\"}]; "
            "select value sleep(\"hold\", 30000); "
            "select value count(*) from ms.one;",
            multi_statement=True, analytics_timeout=120,
            http_timeout=180,
            client_context_id=kill_ccid)
        self._wait_for_active_request(kill_ccid, min_jobs=2)

        self.analytics_api.restart_analytics_node()

        thread.join(timeout=180)
        self.log.info(
            "case29 observed response (NOT YET MEASURED baseline - "
            "capture and promote): response={} exception={}".format(
                result_holder.get("response"),
                result_holder.get("exception")))

        status, results = None, None
        for _ in range(30):
            status, _, errors, results = self._run_statement(
                "select value count(*) from ms.one where "
                "v = \"pre-kill\";")
            if status == "success":
                break
            time.sleep(5)
        self.assertEqual(
            status, "success",
            "analytics never came back after the restart - the durability "
            f"assertion below could not be evaluated: {errors}")
        self.assertEqual(
            results, [1],
            "the upsert that completed before the restart must be durable")

        completed = self._results(
            self._completed_request_query(kill_ccid))
        self.log.info(f"case29 completed_requests() record: {completed}")
        for record in completed:
            for job in record.get("jobs") or []:
                self.assertIn(
                    self._statement_number(job), (1, 2, 3),
                    f"the listing claims a job for a statement that was never "
                    f"submitted: {record.get('jobs')}")
        active = self._results(
            "select value r from active_requests() r where "
            f"r.clientContextID = \"{kill_ccid}\";")
        self.assertEqual(
            active, [],
            "a request killed mid-flight must not still be listed as active "
            f"once the service is back: {active}")

    def test_deferred_handles_across_node_restart(self):
        setup = (
            "drop scope ms if exists; create scope ms; "
            "create dataset ms.one primary key (id: int); "
            "create dataset ms.big primary key (id: int); "
            "upsert into ms.one [{\"id\":1,\"v\":\"solo\"}]; "
            "upsert into ms.big (select value {\"id\": i} from "
            "range(1,1000) i);")
        self._build_topology_fixture(setup)

        response = self._multi_statement(
            "select id from ms.one; select id from ms.big; " "select 1;",
            multi_statement=True, mode="deferred")
        statements = self._assert_statements_array(response, expected_count=3)
        handles = [entry["handle"] for entry in statements]

        first_fetch = self._fetch_handle(handles[0])
        self.assertEqual(first_fetch.status_code, 200)
        self.assertEqual(
            first_fetch.json().get("results"), [{"id": 1}],
            f"the first handle must return statement 1's rows before the "
            f"restart: {first_fetch.text}")

        self.analytics_api.restart_analytics_node()
        self._wait_for_analytics()

        truncated = []
        for handle in handles[1:]:
            try:
                fetch = self._fetch_handle(handle)
                body = fetch.text
            except (requests.exceptions.ChunkedEncodingError,
                    requests.exceptions.ConnectionError) as e:
                truncated.append(f"{handle}: {type(e).__name__}: {e}")
                continue
            self.log.info(
                f"case30 fetch after restart for handle {handle}: "
                f"status={fetch.status_code} body={body[:200]}")
            self.assertIn(
                fetch.status_code, (200, 404, 410),
                f"a handle that predates the restart must either serve its "
                f"rows or be refused cleanly with 404/410, not fail with "
                f"{fetch.status_code}: {body}")
            if body.strip():
                try:
                    fetch.json()
                except ValueError:
                    truncated.append(
                        f"{handle}: status={fetch.status_code} carried a "
                        f"non-JSON body: {body[:200]}")
        if truncated:
            self.fail(
                "a post-restart handle fetch returned a truncated response "
                "instead of its rows or a clean 404/410. The server commits "
                "to a status line and then abandons the body, so a client "
                "cannot tell success from failure. "
                + str(len(truncated)) + f" of {len(handles) - 1} fetches: "
                + "; ".join(truncated))

    def test_hard_failover_of_node_executing_statement(self):
        self._expected_cbas_nodes = self._cbas_node_count()
        setup = (
            "drop scope ms if exists; create scope ms; "
            "create dataset ms.big primary key (id: int); "
            "upsert into ms.big (select value {\"id\": i, "
            "\"grp\": i % 10} from range(1,1000) i);")
        self._build_topology_fixture(setup)

        cbas_nodes = self.cluster_util.get_nodes_from_services_map(
            self.columnar_cluster, service_type="cbas", get_all_nodes=True,
            servers=self.columnar_cluster.nodes_in_cluster)
        coordinator = self.columnar_cluster.master
        non_coordinator = next(
            (n for n in cbas_nodes if n.ip != coordinator.ip), None)
        if non_coordinator is None:
            self.fail(
                "Could not identify a non-coordinator cbas node for "
                "case 31's first sub-case")

        for label, target_node in (
                ("non-coordinator", non_coordinator),
                ("coordinator", coordinator)):
            failover_ccid = self._ccid(f"ms-failover-{label}")
            thread, result_holder = self._submit_in_background(
                "select grp, count(*) as c from ms.big group "
                "by grp; select value sleep(\"hold\", 30000); "
                "select id from ms.big;", multi_statement=True,
                analytics_timeout=120, http_timeout=180,
                client_context_id=failover_ccid)
            self._wait_for_active_request(failover_ccid, min_jobs=2)

            self.rebalance_util.failover(
                cluster=self.columnar_cluster, cbas_nodes=1, failover_type="Hard",
                cbas_failover_nodes=[target_node], action="FullRecovery",
                wait_for_complete=True)

            thread.join(timeout=180)
            self.log.info(
                "case31 ({} failover, NOT YET MEASURED baseline - "
                "capture and promote): response={} exception={}".format(
                    label, result_holder.get("response"),
                    result_holder.get("exception")))

            results = self._results(
                self._completed_request_query(failover_ccid))
            self.log.info(
                f"case31 ({label}) completed_requests() record: {results}")

            self._restore_topology_if_shrunk()
            self.assertTrue(
                results,
                f"the {label} failover left no completed_requests() record "
                f"for {failover_ccid} - the request is unaccounted for")
            record = results[0]
            jobs = record.get("jobs") or []
            statement_numbers = [self._statement_number(j) for j in jobs]
            self.assertEqual(
                len(statement_numbers), len(set(statement_numbers)),
                f"duplicate statement number in jobs array: {jobs}")
            for number in statement_numbers:
                self.assertIn(
                    number, (1, 2, 3),
                    f"jobs entry references a statement never submitted: "
                    f"{jobs}")
            self.assertIsNone(
                record.get("jobId"),
                f"top-level jobId must be null for a multi-statement "
                f"request: {record.get('jobId')}")



    # ------------------------------------------------------------------
    # Cases 33-35: CBO
    # ------------------------------------------------------------------

    PLAN_WITH_STATS_MARKERS = ("unnest-map", "BTREE_SEARCH",
                               "BROADCAST_EXCHANGE")
    PLAN_NO_STATS_MARKERS = ("HASH_PARTITION_EXCHANGE",)

    def _plan_uses_statistics(self, plans, label):
        """Classify a plan as statistics-present or statistics-absent.

        Returns True when it carries the with-statistics markers and none of
        the without-statistics ones, False for the reverse, and fails when it
        is neither - an unclassifiable plan means the markers no longer match
        what the optimizer emits, and silently guessing would turn this
        regression guard into noise.
        """
        payload = json.dumps(plans)
        with_stats = all(m in payload for m in self.PLAN_WITH_STATS_MARKERS)
        no_stats = any(m in payload for m in self.PLAN_NO_STATS_MARKERS)
        if with_stats and not no_stats:
            return True
        if no_stats and not with_stats:
            return False
        self.fail(
            f"{label}: plan matches neither the statistics-present shape "
            f"{self.PLAN_WITH_STATS_MARKERS} nor the statistics-absent shape "
            f"{self.PLAN_NO_STATS_MARKERS}. The markers the TSV records may "
            f"no longer describe what the optimizer emits - check them before "
            f"reading this as a product defect. Plan: {payload}")

    @staticmethod
    def _plan_cardinalities(plans):
        """Every optimizer-estimates cardinality in a plan."""
        found = []

        def walk(node):
            if isinstance(node, dict):
                estimates = node.get("optimizer-estimates")
                if isinstance(estimates, dict) and "cardinality" in estimates:
                    found.append(estimates["cardinality"])
                for value in node.values():
                    walk(value)
            elif isinstance(node, list):
                for value in node:
                    walk(value)

        walk(plans)
        return found

    def _cbo_service_config(self):
        status, content, _ = self.analytics_api.get_service_config()
        return content if isinstance(content, dict) else json.loads(content)

    def _require_cbo(self, case_number):
        """Fail unless the optimizer is on, then build the cbo fixture.

        Cases 33-35 are assertions about optimizer behaviour; with
        compilerCbo off they would pass while measuring nothing, so an
        unusable cluster is reported rather than quietly tolerated.
        """
        if not self._cbo_service_config().get("compilerCbo"):
            self.fail(f"compilerCbo is disabled on this cluster; case "
                      f"{case_number} is void without it")
        self._create_cbo_fixture()

    def test_metadata_changed_by_one_statement_is_reread_when_next_compiles(
            self):
        self._require_cbo(33)

        self._multi_statement(
            "analyze collection cbo.big drop statistics; "
            "analyze collection cbo.small drop statistics;",
            multi_statement=True, http_timeout=180)
        response = self._multi_statement(
            CBO_JOIN_QUERY, multi_statement=None,
            extra_params=PLAN_DEBUG_PARAMS)
        self.assertFalse(
            self._plan_uses_statistics(
                response.get("plans"), "no-statistics reference"),
            "the no-statistics reference plan was built WITH statistics - "
            "the DROP STATISTICS above did not take effect, and nothing "
            "below this point can be trusted")

        self._multi_statement(
            "analyze collection cbo.big; analyze collection cbo.small;",
            multi_statement=True, http_timeout=180)
        response = self._multi_statement(
            CBO_JOIN_QUERY, multi_statement=None,
            extra_params=PLAN_DEBUG_PARAMS)
        self.assertTrue(
            self._plan_uses_statistics(
                response.get("plans"), "statistics-present reference"),
            "the statistics-present reference plan was built WITHOUT "
            "statistics - the cbo fixture cannot distinguish the two, and "
            "nothing below this point can be trusted")

        self._multi_statement(
            "analyze collection cbo.big drop statistics; "
            "analyze collection cbo.small drop statistics;",
            multi_statement=True, http_timeout=180)

        statement = (
            "analyze collection cbo.big; analyze collection cbo.small; " +
            CBO_JOIN_QUERY)
        response = self._multi_statement(statement, multi_statement=True,
            analytics_timeout=600, http_timeout=660,
            extra_params=PLAN_DEBUG_PARAMS)
        entry3 = self._assert_statements_array(
            response, expected_entries=[
                {}, {}, {"metrics": {"resultCount": 100}}])[2]

        self.assertTrue(
            self._plan_uses_statistics(entry3["plans"], "entry 3"),
            "entry 3 was planned WITHOUT the statistics that statements 1 and "
            "2 had just produced. The statements were planned up front rather "
            "than one at a time; metadata written by an earlier statement is "
            "not being reread when a later one compiles.")

        cardinalities = self._plan_cardinalities(entry3["plans"])
        self.log.info(f"case33 entry 3 cardinalities: {cardinalities}")
        self.assertTrue(
            any(c > 0 for c in cardinalities),
            f"every optimizer estimate in entry 3 reads zero, which is the "
            f"no-statistics signature: {cardinalities}")

        results = self._results(
            "select value i.IndexName from Metadata.`Index` i where "
            "i.DataverseName = \"cbo\" and i.IndexStructure = "
            "\"SAMPLE\";")
        self.assertEqual(sorted(results), sorted(
            ["sample_idx_1_big", "sample_idx_1_small"]))

    def test_set_compiler_cbo_governs_statements_after_it(self):
        self._require_cbo(34)
        self._multi_statement(
            "analyze collection cbo.big; analyze collection cbo.small;",
            multi_statement=True, http_timeout=180)

        statement = (
            "set `compiler.cbo` \"false\"; " + CBO_JOIN_QUERY)
        response = self._multi_statement(
            statement, multi_statement=True,
            extra_params=PLAN_DEBUG_PARAMS)
        self._assert_flat_envelope(response, "success")
        self.assertEqual(response["metrics"].get("resultCount"), 100)
        plan_disabled = json.dumps(response.get("plans"))

        statement_two = (
            "set `compiler.cbo` \"false\"; " + CBO_JOIN_QUERY + " " +
            CBO_JOIN_QUERY)
        response = self._multi_statement(
            statement_two, multi_statement=True,
            extra_params=PLAN_DEBUG_PARAMS)
        statements = self._assert_statements_array(response, expected_count=3)
        entry2_plan = json.dumps(statements[1]["plans"])
        entry3_plan = json.dumps(statements[2]["plans"])
        self.assertEqual(entry2_plan, plan_disabled)
        self.assertEqual(entry3_plan, plan_disabled)

        response = self._multi_statement(
            CBO_JOIN_QUERY, multi_statement=None,
            extra_params=PLAN_DEBUG_PARAMS)
        plan_after = json.dumps(response.get("plans"))
        self.assertNotEqual(
            plan_after, plan_disabled,
            "SET compiler.cbo leaked into a later, unrelated request")

    def test_memory_budget_is_per_statement_not_divided(self):
        self._require_cbo(35)
        self._multi_statement(
            "analyze collection cbo.big; analyze collection cbo.small;",
            multi_statement=True, http_timeout=180)

        single_ccid = self._ccid("mem-single")
        multi_ccid = self._ccid("mem-multi")
        query = (
            "select b.grp, count(*) as c, max(b.pad) as m from cbo.big b "
            "join cbo.small s on b.grp = s.id group by b.grp order by "
            "c desc;")

        response = self._multi_statement(
            query, multi_statement=None, extra_params=PLAN_DEBUG_PARAMS,
            client_context_id=single_ccid)
        self._assert_response_ok(response)
        standalone_plan = json.dumps(response.get("plans"))

        standalone_mem = self._single_result(
            "select value {\"stmt\": " + JOB_NUMBER_SQL + ", \"mem\": "
            "j.jobRequiredMemory, \"cpu\": j.jobRequiredCPUs} from "
            "completed_requests() r unnest r.jobs j where "
            f"r.clientContextID = \"{single_ccid}\";")["mem"]

        five_queries = (query + " ") * 5
        response = self._multi_statement(five_queries, multi_statement=True,
            analytics_timeout=600, http_timeout=660,
            extra_params=PLAN_DEBUG_PARAMS,
            client_context_id=multi_ccid)
        statements = self._assert_statements_array(response, expected_count=5)
        for entry in statements:
            self.assertEqual(
                json.dumps(entry["plans"]), standalone_plan,
                "a late statement's plan diverged from the standalone "
                "reference plan")

        results = self._results(
            "select value {\"stmt\": " + JOB_NUMBER_SQL + ", \"mem\": "
            "j.jobRequiredMemory, \"cpu\": j.jobRequiredCPUs} from "
            "completed_requests() r unnest r.jobs j where "
            f"r.clientContextID = \"{multi_ccid}\" order by "
            + JOB_NUMBER_SQL + ";")
        self.assertEqual(len(results), 5)
        for row in results:
            self.assertEqual(
                row["mem"], standalone_mem,
                "jobRequiredMemory for statement {} diverged from the "
                "standalone estimate - the budget may be getting divided "
                "across the request: {}".format(row["stmt"], results))

        distinct_queries = [
            "select b.grp, count(*) as c from cbo.big b join cbo.small s "
            "on b.grp = s.id group by b.grp order by c desc;",
            "select b.grp, max(b.pad) as m from cbo.big b group by b.grp "
            "order by b.grp;",
            "select b.grp, count(distinct b.pad) as d from cbo.big b "
            "group by b.grp order by d desc;",
            "select b.id, s.v from cbo.big b join cbo.small s on "
            "b.grp = s.id order by b.id limit 50;",
            "select b.grp, sum(b.id) as t from cbo.big b group by b.grp "
            "order by t;"]
        by_position = {}
        for label, order in (("forward", distinct_queries),
                             ("reverse", list(reversed(distinct_queries)))):
            order_ccid = self._ccid(f"mem-{label}")
            response = self._multi_statement(
                " ".join(order), multi_statement=True, analytics_timeout=600,
                http_timeout=660,
                extra_params={"skip-plan-cache": True},
                client_context_id=order_ccid)
            self._assert_statements_array(response, expected_count=5)
            rows = self._results(
                "select value {\"stmt\": " + JOB_NUMBER_SQL + ", \"mem\": "
                "j.jobRequiredMemory} from completed_requests() r "
                f"unnest r.jobs j where r.clientContextID = \"{order_ccid}\" "
                "order by " + JOB_NUMBER_SQL + ";")
            self.assertEqual(len(rows), 5, f"{label}: {rows}")
            for row in rows:
                index = (row["stmt"] - 1 if label == "forward"
                         else 5 - row["stmt"])
                by_position.setdefault(index, {})[label] = row["mem"]
        self.log.info(f"case35 per-query memory by order: {by_position}")
        for index, seen in sorted(by_position.items()):
            self.assertEqual(
                seen.get("forward"), seen.get("reverse"),
                f"query {index + 1} was costed differently depending on where "
                f"it sat in the request ({seen}) - the budget is "
                f"position-dependent")

    def test_authorization_uses_current_roles_not_a_request_snapshot(self):
        """A statement is authorized against the user's roles as they stand
        when that statement runs, not as they stood when the request started.

        REGRESSION GUARD for MB-74150, fixed in 3.0.0-1056 by cbas-core
        d76820a "Recheck ns_server roles per statement". Authorization was
        performed per statement but against a permission set captured once at
        request start, so a write revoked mid-request still landed - and the
        caller chooses both the delay and the request timeout, so the caller
        chose the size of the escalation window.

        The control is load-bearing: a fresh request from the same user must
        be refused before the long-running write is judged. Without it, a
        refused write could just mean the revoke never reached analytics.
        """
        guard_ccid = self._ccid("h6-stale-auth")
        setup = (
            "drop scope h6 if exists; create scope h6; "
            "create collection h6.t primary key (id: bigint);")
        response = self._multi_statement(
            setup, multi_statement=True, http_timeout=60)
        self._assert_response_ok(response)

        username, password = self._create_rbac_user(
            "h6user", "analytics_admin")

        thread, result_holder = self._submit_in_background(
            "select value sleep(\"hold\", 25000); "
            "upsert into h6.t ([{\"id\":888}]); select 1;",
            multi_statement=True, analytics_timeout=600,
            http_timeout=60, username=username, password=password,
            client_context_id=guard_ccid)

        self._wait_for_active_request(guard_ccid)

        revoke_ok = self.rbac_util.set_user_roles(
            self.columnar_cluster, username, password, "analytics_access")
        self.assertTrue(
            revoke_ok,
            "the revoke call itself must succeed or this guard proves "
            "nothing")

        deadline = time.time() + 60
        while True:
            control_response = (
                self._multi_statement(
                    "upsert into h6.t ([{\"id\":889}]);",
                    multi_statement=None, http_timeout=30, username=username,
                    password=password))
            if control_response.get("status") == "fatal":
                break
            if time.time() >= deadline:
                break
            time.sleep(2)
        self.assertEqual(
            control_response.get("status"), "fatal",
            "control failed: a fresh request after the revoke must be "
            "refused, or this guard's own premise is broken")
        self.assertEqual(
            control_response.get("errors", [{}])[0].get("code"), 20001,
            "control failed: expected 20001 for the fresh post-revoke "
            "request")

        thread.join(timeout=60)
        long_running_response = result_holder.get("response") or {}
        self.log.info(
            f"stale-authorization guard: long-running response={long_running_response}")

        self.assertEqual(
            self._single_result(
                "select value count(*) from h6.t where id = 888;"),
            0,
            "MB-74150 has regressed: the long-running request's write used a "
            "stale permission snapshot and landed even after the write role "
            "was revoked mid-request. The control above confirmed the revoke "
            "had already taken effect for a fresh request, so this is the "
            "request-scoped snapshot, not a propagation delay.")

    def test_deferred_handles_are_usable_as_returned(self):
        """Every per-statement handle a deferred request returns is usable
        exactly as given, and returns that statement's own rows.

        A handle must be either a rooted relative path - resolved against the
        base the request was sent to, which is what /api/v1/request returns
        (`/api/v1/request/result/<uuid>/<job>-<n>`) - or an absolute URL
        carrying the scheme the request arrived on. Either way it is followed
        verbatim, with no scheme rewriting, and must return HTTP 200 and the
        rows belonging to that statement.

        An absolute handle with a hardcoded scheme is unusable over TLS: the
        GET lands on a TLS listener as plaintext and the connection is
        dropped. Asserting the scheme against the one the request arrived on
        is what catches that, so this case has teeth over TLS; the relative
        and fetchability assertions hold on a plaintext cluster too, so it
        runs on either.
        """
        base_url = self.analytics_api.cbas_url
        request_scheme = urlparse(base_url).scheme
        self.log.info(f"submitting to {base_url}/api/v1/request")

        content = self._multi_statement(
            "select id from ms.one; select 1;", mode="deferred")
        self.assertEqual(
            content.get("status"), "success",
            f"the deferred multi-statement request itself failed: {content}")
        statements = self._assert_statements_array(content, expected_count=2)

        expected_rows = ([{"id": 1}], [{"$1": 1}])
        for position, (entry, expected) in enumerate(
                zip(statements, expected_rows, strict=True), start=1):
            handle = entry.get("handle")
            self.assertTrue(
                handle,
                f"statement {position} carries no handle in deferred mode: "
                f"{entry}")
            self.log.info(f"stmt{position} handle as returned: {handle}")

            parsed = urlparse(handle)
            if parsed.scheme:

                self.assertEqual(
                    parsed.scheme, request_scheme,
                    f"statement {position}'s handle is absolute with scheme "
                    f"{parsed.scheme!r} but the request arrived over "
                    f"{request_scheme!r}: {handle}. Following it as given "
                    "would address the wrong listener.")
                url = handle
            else:
                self.assertTrue(
                    handle.startswith("/"),
                    f"statement {position}'s handle is neither absolute nor a "
                    f"rooted relative path: {handle}")
                url = base_url + handle

            fetch = requests.get(
                url, auth=(self.columnar_cluster.master.rest_username,
                           self.columnar_cluster.master.rest_password),
                verify=False)
            self.assertEqual(
                fetch.status_code, 200,
                f"statement {position}'s handle was not fetchable as "
                f"returned ({url}): {fetch.status_code} {fetch.text}")
            self.assertEqual(
                fetch.json().get("results"), expected,
                f"statement {position}'s handle returned the wrong rows: "
                f"{fetch.text}")
