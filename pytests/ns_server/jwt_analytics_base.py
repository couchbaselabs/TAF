"""Base class for the JWT-against-Analytics tests.

Holds the cluster/JWT fixture setup and every helper, so jwt_analytics.py
contains only test_ methods - the same split as jwt_oidc_base.py /
jwt_oidc_test.py and credential_store_base.py / credential_store_test.py.
"""
import json
import time
import uuid

import jwt as pyjwt

from BucketLib.bucket import Bucket
from basetestcase import ClusterSetup
from cb_constants import CbServer
from cb_server_rest_util.connection import CBRestConnection
from couchbase_utils.security_utils import jwt_utils
from membase.api.rest_client import RestConnection
from rbac_utils.Rbac_ready_functions import RbacUtils


class JwtAnalyticsBase(ClusterSetup):

    # The direct cbas port serves /analytics/service and 404s on
    # /analytics/query/service, while the ns_server proxy accepts both
    # spellings - they are aliases onto one servlet. Verified live; do not
    # "align" these two paths.
    ANALYTICS_PATH = "/analytics/service"
    PROXY_PATH = "/_p/cbas/analytics/service"

    BUCKET = "jwtanalytics"
    OTHER_BUCKET = "jwtanalyticsother"

    # Created once in setUp and never mutated, so the read-only matrix cells
    # need no per-attempt fixture reset.
    DATASET = "jwtads"
    # Created and dropped by the mutating cells.
    DDL_DATASET = "jwtddlds"
    DATAVERSE = "jwtdv"
    UDF = "jwtfn"

    # Verified empirically, and GET /settings/rbac/roles is misleading here: it
    # reports params=None for all four, but manager/select are bucket-scoped and
    # *require* [bucket] while admin/reader are global and *reject* one. Trust
    # the assignment attempt, not the listing.
    SCOPED_ROLES = ("analytics_manager", "analytics_select")
    MATRIX_ROLES = ["analytics_admin", "analytics_manager", "analytics_reader",
                    "analytics_select", "none"]
    USER_PASSWORD = "password"

    def setUp(self):
        super(JwtAnalyticsBase, self).setUp()

        if not self.cluster.cbas_nodes:
            self.fail("No node running the cbas service. Run with "
                      "services_init=kv:n1ql-cbas and do not pass "
                      "skip_cluster_reset=True.")
        self.cbas_node = self.cluster.cbas_nodes[0]

        # jwt_utils is written against the legacy membase RestConnection (it
        # uses .baseUrl / ._http_request), so use that rather than ClusterRestAPI.
        self.rest = RestConnection(self.cluster.master)
        self.jwt_utils = jwt_utils.JWTUtils(log=self.log)
        self.rbac_util = RbacUtils(self.cluster.master)
        # Bare connection: Analytics is not on the ns_server port, so every call
        # passes an absolute URL and explicit headers.
        self.conn = CBRestConnection()

        self.issuer_name = self.input.param("issuer_name",
                                            "analytics-jwt-issuer")
        self.audience = self.input.param("token_audience", "cb-analytics")
        self.algorithm = self.input.param("algorithm", "RS256")
        self.ttl = self.input.param("ttl", 600)
        self.roles_claim = "roles"
        self.groups_claim = "groups"

        self.private_key, self.pub_key = self.jwt_utils.generate_key_pair(
            self.algorithm)

        scheme = "https" if CbServer.use_https else "http"
        cbas_port = (CbServer.ssl_cbas_port if CbServer.use_https
                     else CbServer.cbas_port)
        self.direct_url = "%s://%s:%s%s" % (
            scheme, self.cbas_node.ip, cbas_port, self.ANALYTICS_PATH)
        self.proxy_url = "%s://%s:%s%s" % (
            scheme, self.cluster.master.ip, self.cluster.master.port,
            self.PROXY_PATH)

        self.created_groups = []
        self.created_users = []
        self.external_users = []

        self._create_buckets()
        self._wait_for_analytics()

        # One local user per role -- the Basic-auth oracle.
        for role in self.MATRIX_ROLES:
            self._create_local_user(role)

        self._admin_exec("CREATE DATASET IF NOT EXISTS %s ON `%s`;"
                         % (self.DATASET, self.BUCKET))

    def tearDown(self):
        try:
            self._cleanup_analytics_objects()
            self._admin_exec("DROP DATASET %s IF EXISTS;" % self.DATASET,
                             strict=False)
            self.jwt_utils.disable_jwt(self.rest)
            for user in self.created_users:
                try:
                    self.rbac_util._drop_user(user)
                except Exception as e:
                    self.log.warn("Could not drop user %s: %s", user, e)
            for user in self.external_users:
                self.jwt_utils.delete_external_user(self.rest, user)
            for group in self.created_groups:
                self.jwt_utils.delete_group(self.rest, group)
        except Exception as e:
            self.log.error("Cleanup hit an error (continuing): %s", e)
        super(JwtAnalyticsBase, self).tearDown()

    # ------------------------------------------------------------------
    # Fixtures
    # ------------------------------------------------------------------

    def _create_buckets(self):
        """Two buckets: the second exists only so a bucket-scoped role can be
        shown to be denied outside its own scope.

        The inherited ClusterSetup.create_bucket() wrapper is not used here: it
        passes ram_quota=self.bucket_size, which defaults to None and therefore
        hands the whole node quota to the first bucket, leaving nothing for the
        second. An explicit small quota is required.
        """
        for name in (self.BUCKET, self.OTHER_BUCKET):
            self.bucket_util.create_default_bucket(
                self.cluster, bucket_name=name, ram_quota=256, replica=0,
                storage=Bucket.StorageBackend.couchstore)

    def _wait_for_analytics(self, timeout=180):
        """The rebalance finishing does not mean cbas is ready to serve."""
        end = time.time() + timeout
        while time.time() < end:
            try:
                status, _ = self._analytics("SELECT VALUE 1;")
                if status == 200:
                    return
            except Exception as e:
                self.log.debug("Analytics not ready yet: %s", e)
            self.sleep(3, "Waiting for the Analytics service")
        self.fail("Analytics service did not become ready within %ss" % timeout)

    def _create_local_user(self, role):
        user = self._user_name(role)
        self.rbac_util._create_user_and_grant_role(
            user, self._role_spec(role), password=self.USER_PASSWORD)
        self.created_users.append(user)
        return user

    def _create_group(self, role):
        """Create the RBAC group used by the groupsMaps leg, tracked for
        teardown."""
        group = self._group_name(role)
        self.jwt_utils.create_group(self.rest, group, self._role_spec(role))
        if group not in self.created_groups:
            self.created_groups.append(group)
        return group

    def _cleanup_analytics_objects(self):
        """Drop whatever the mutating tests may have left, in dependency order,
        and restore ingestion. Best-effort by design."""
        for stmt in ["DROP ANALYTICS FUNCTION %s() IF EXISTS;" % self.UDF,
                     "DROP DATASET %s IF EXISTS;" % self.DDL_DATASET,
                     "DROP DATAVERSE %s IF EXISTS;" % self.DATAVERSE,
                     "CONNECT LINK Local;"]:
            self._admin_exec(stmt, strict=False)

    # ------------------------------------------------------------------
    # Naming / roles
    # ------------------------------------------------------------------

    @staticmethod
    def _user_name(role):
        return "jwtan_%s" % role

    @staticmethod
    def _group_name(role):
        return "jwtan_grp_%s" % role

    def _role_spec(self, role, bucket=None):
        """Role string for assignment: scoped roles carry a bucket parameter,
        global ones must not."""
        if role == "none":
            return ""
        if role in self.SCOPED_ROLES:
            return "%s[%s]" % (role, bucket or self.BUCKET)
        return role

    @staticmethod
    def _classify(status):
        """Collapse a response into an authorization verdict.

        The status code stays inside the verdict so a JWT 401 against a Basic
        403 surfaces as a mismatch instead of collapsing into a generic
        "denied" -- that is the error-path parity check, for free.
        """
        if status == 200:
            return "ALLOW"
        if status in (401, 403):
            return "DENY(%s)" % status
        return "ERR(%s)" % status

    # ------------------------------------------------------------------
    # Analytics plumbing
    # ------------------------------------------------------------------

    def _analytics(self, statement, token=None, username=None, password=None,
                   proxied=False, extra_headers=None):
        """Run one Analytics statement and return (status_code, body).

        Auth is whichever of token / username+password is supplied, defaulting
        to the cluster admin.
        """
        url = self.proxy_url if proxied else self.direct_url
        status, body = self.jwt_utils.request_with_bearer_url(
            self.conn, url, token=token,
            username=username or self.cluster.master.rest_username,
            password=password or self.cluster.master.rest_password,
            method="POST", body=json.dumps({"statement": statement}),
            extra_headers=extra_headers, timeout=120)

        # CBRestConnection logs at DEBUG, which is off by default -- without
        # this an all-green run leaves no evidence it issued any request at all.
        # Tokens are never logged, only which credential kind was used.
        if token is not None:
            who = "bearer"
        elif username:
            who = "basic:%s" % username
        else:
            who = "basic:admin"
        self.log.info("analytics [%s] %s auth=%s -> %s",
                      "proxy" if proxied else "direct",
                      statement[:60], who, status)
        return status, body[:400]

    def _admin_exec(self, statement, strict=True):
        """Fixture setup/teardown as Administrator.

        Every fixture statement is written to be idempotent (IF [NOT] EXISTS),
        so as Administrator there is no legitimate reason for one to fail. Any
        non-200 is therefore treated as fatal rather than logged: a fixture that
        fails silently leaves state dirty, which turns the next attempt's
        "already exists" into a permission verdict that is not real. That
        produced two false findings in the manual run.

        strict=False is only for best-effort cleanup paths, where the cluster
        may legitimately already be in the target state.
        """
        status, body = self._analytics(statement)
        if status != 200 and strict:
            self.fail("Fixture statement failed, results would be invalid: "
                      "%s -- status=%s %s" % (statement, status, body))
        return status, body

    def _attempt(self, setup, statement, teardown, **auth):
        """Run one matrix cell and return its verdict.

        Mutating operations reset their fixture around *every* attempt, not once
        per cell: a create that succeeded under Basic auth would otherwise leave
        the JWT attempt failing with "already exists" rather than a permission
        error, which reads as a mismatch that is not one.
        """
        if setup:
            self._admin_exec(setup)
        try:
            status, _ = self._analytics(statement, **auth)
            return self._classify(status)
        finally:
            if teardown:
                self._admin_exec(teardown)

    # ------------------------------------------------------------------
    # JWT plumbing
    # ------------------------------------------------------------------

    def _issuer(self, **extra):
        """Build a single-issuer JWT config.

        get_jwt_config/_build_jwt_issuer_entry model only groupsMaps, so the
        issuer dict is hand-built here to reach rolesClaim / rolesMaps /
        rolesMapsStopFirstMatch -- the same approach jwt_token_test.py takes.
        Unknown issuer keys are rejected with 400 (validator:unsupported), and
        the toggles are rolesMapsStopFirstMatch / groupsMapsStopFirstMatch, not
        stopFirstMatch.
        """
        issuer = {
            "name": self.issuer_name,
            "signingAlgorithm": self.algorithm,
            "publicKeySource": "pem",
            "publicKey": self.pub_key,
            "subClaim": "sub",
            "audClaim": "aud",
            "audienceHandling": "any",
            "audiences": [self.audience],
            # rolesMaps/groupsMaps are only consulted when JIT provisioning is on.
            "jitProvisioning": True,
        }
        issuer.update(extra)
        return {"enabled": True, "issuers": [issuer]}

    def _configure(self, **extra):
        """PUT the JWT config. PUT replaces the whole document, so each test
        calls this itself as its first step."""
        self.jwt_utils.setup_jwt_config(
            rest_connection=self.rest,
            config=self._issuer(**extra),
            create_groups_callback=None,
            sleep_callback=self.sleep)

    def _mint(self, sub, claims=None, ttl=None, audience=None, issuer=None):
        """Sign a token with arbitrary claims.

        jwt_utils.create_token only models a `groups` claim, so roles-claim
        tokens are signed here directly, as jwt_token_test.py does via
        _create_signed_token_from_payload.
        """
        now = int(time.time())
        payload = {
            "iss": issuer or self.issuer_name,
            "sub": sub,
            "aud": audience or self.audience,
            "iat": now,
            "nbf": now,
            "exp": now + (ttl if ttl is not None else self.ttl),
            "jti": str(uuid.uuid4()),
        }
        payload.update(claims or {})
        if sub not in self.external_users:
            self.external_users.append(sub)
        return pyjwt.encode(payload=payload, algorithm=self.algorithm,
                            key=self.private_key)

    def _role_token(self, role):
        """Token whose roles claim maps directly onto the given role."""
        values = [] if role == "none" else [self._role_spec(role)]
        return self._mint("jwtan-%s@example.com" % role,
                          {self.roles_claim: values})

    def _wait_for_jwt(self, token, timeout=60):
        """cbas resolves identities asynchronously through /_cbauth, so give a
        token that must work a chance to start working."""
        end = time.time() + timeout
        last = None
        while time.time() < end:
            status, last = self._analytics("SELECT VALUE 1;", token=token)
            if status == 200:
                return
            self.sleep(3, "Waiting for cbas to accept the JWT")
        self.fail("Token never accepted by Analytics within %ss. Last "
                  "response: %s" % (timeout, last))

    def _whoami_roles(self, token):
        """Roles ns_server resolved for a token, or None if it did not
        authenticate at all."""
        info = self.jwt_utils.get_user_info_from_whoami(self.rest, token)
        if info is None:
            return None
        return sorted(r.get("role") for r in info.get("roles", []))

    # ------------------------------------------------------------------
    # Operations under test
    # ------------------------------------------------------------------

    def _matrix_operations(self):
        """(label, setup, statement, teardown) for the operation surface.

        NOTE on syntax -- verified empirically, do not "tidy":
          * CREATE DATASET takes IF NOT EXISTS *before* the name while DROP
            DATASET takes IF EXISTS *after* it; the grammar is asymmetric.
          * DATAVERSE takes the modifier after the name in both directions.
          * Functions and synonyms need the ANALYTICS keyword, and on a function
            drop IF EXISTS goes after the parameter list:
            DROP ANALYTICS FUNCTION f() IF EXISTS.
        Every statement here is idempotent so that _admin_exec can treat any
        failure as fatal. Getting the syntax wrong makes the fixture fail,
        leaving state dirty and producing fake mismatches on the next attempt.

        Operations that gate identically to one already covered are omitted --
        drop vs create, index vs dataset, synonym vs UDF -- as shown by the
        manual 50-cell run. They would add runtime and no signal.
        """
        drop_ds = "DROP DATASET %s IF EXISTS;" % self.DDL_DATASET
        drop_dv = "DROP DATAVERSE %s IF EXISTS;" % self.DATAVERSE
        drop_fn = "DROP ANALYTICS FUNCTION %s() IF EXISTS;" % self.UDF
        return [
            ("query dataset", None,
             "SELECT VALUE 1 FROM %s LIMIT 1;" % self.DATASET, None),
            ("read metadata", None,
             "SELECT VALUE d.DatasetName FROM Metadata.`Dataset` d LIMIT 1;",
             None),
            ("create dataset", drop_ds,
             "CREATE DATASET %s ON `%s`;" % (self.DDL_DATASET, self.BUCKET),
             drop_ds),
            ("create dataverse", drop_dv,
             "CREATE DATAVERSE %s;" % self.DATAVERSE, drop_dv),
            ("create UDF", drop_fn,
             "CREATE ANALYTICS FUNCTION %s() { 1 };" % self.UDF, drop_fn),
        ]

    def _compare_auth(self, role, operations, token=None):
        """Run each operation as the Basic-auth oracle and as the equivalent JWT
        user; return the disagreements."""
        basic = {"username": self._user_name(role),
                 "password": self.USER_PASSWORD}
        bearer = {"token": token if token is not None
                  else self._role_token(role)}
        mismatches = []
        for label, setup, stmt, tdown in operations:
            basic_verdict = self._attempt(setup, stmt, tdown, **basic)
            jwt_verdict = self._attempt(setup, stmt, tdown, **bearer)
            self.log.info("  %-16s %-20s basic=%-10s jwt=%s",
                          label, role, basic_verdict, jwt_verdict)
            if basic_verdict != jwt_verdict:
                mismatches.append("%s / %s: basic=%s jwt=%s"
                                  % (label, role, basic_verdict, jwt_verdict))
        return mismatches

    def _verdicts(self, operations, token):
        """Verdict per operation for one bearer token."""
        return {label: self._attempt(setup, stmt, tdown, token=token)
                for label, setup, stmt, tdown in operations}

    def _bad_token_headers(self):
        """(name, headers) for every credential that must be refused.

        The exp/nbf offsets are an hour, comfortably outside the 15s default
        expiryLeewayS -- which applies to nbf as well -- so these cannot flake
        on a loaded box.
        """
        sub = "jwtan-negative@example.com"
        claims = {self.roles_claim: ["analytics_reader"]}
        valid = self._mint(sub, claims)
        now = int(time.time())

        expired = self._mint(sub, dict(claims, exp=now - 3600))
        future_nbf = self._mint(sub, dict(claims, nbf=now + 3600))
        wrong_issuer = self._mint(sub, claims, issuer="not-configured-issuer")
        wrong_aud = self._mint(sub, claims, audience="wrong-audience")
        tampered_sig = valid[:-3] + ("aaa" if not valid.endswith("aaa")
                                     else "bbb")
        tampered_payload = self.jwt_utils.build_tampered_payload_token(
            valid, {"sub": "someone-else@example.com"})

        tokens = [("expired token", expired),
                  ("future nbf token", future_nbf),
                  ("unknown issuer", wrong_issuer),
                  ("wrong audience", wrong_aud),
                  ("tampered signature", tampered_sig),
                  ("tampered payload", tampered_payload),
                  ("garbage token", "not.a.valid.jwt")]
        variants = [(name, {"Authorization": "Bearer %s" % tok})
                    for name, tok in tokens]
        variants.append(("missing bearer prefix", {"Authorization": valid}))
        variants.append(("empty bearer token", {"Authorization": "Bearer "}))
        return variants
