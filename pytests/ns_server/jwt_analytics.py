"""JWT bearer-token authentication against the Analytics (cbas) service.

Analytics does no JWT validation of its own -- AuthenticatedServlet forwards the
Authorization header to ns_server's /_cbauth and trusts whatever identity comes
back. So what is under test is not "does cbas parse a token" but "does swapping
the authentication mechanism change the authorization outcome". It must not.

Every check is therefore run twice -- once as a local Basic-auth user holding a
role, once as a JWT user mapped to the same role -- and the two outcomes must
agree. Basic auth is the oracle, so no per-cell expectation is hardcoded and the
suite stays correct if Analytics' own RBAC changes.

Automates the manual plan in cbas-core/jwt_analytics.md:
    test_jwt_analytics_bearer_auth              T1, T2, T3, negative matrix
    test_jwt_analytics_rbac_parity              T4, A1-A12, A17, scope isolation
    test_jwt_analytics_link_parity              A8
    test_jwt_analytics_role_mapping_equivalence A15, A16
    test_jwt_analytics_role_enforcement         authn vs authz (403, not 401)
    test_jwt_analytics_jit_provisioning         JIT on/off, record vs claim

Fixtures and helpers live in jwt_analytics_base.JwtAnalyticsBase.
"""
from pytests.ns_server.jwt_analytics_base import JwtAnalyticsBase


class JwtAnalytics(JwtAnalyticsBase):

    def test_jwt_analytics_bearer_auth(self):
        """A valid bearer token authenticates against Analytics and every
        invalid one is refused -- identically on the direct cbas port and
        through ns_server's /_p/cbas/ proxy.

        Any difference between the two paths is itself a bug: both are supposed
        to validate independently through /_cbauth.
        """
        self._configure(rolesClaim=self.roles_claim)
        token = self._role_token("analytics_reader")
        self._wait_for_jwt(token)

        query = "SELECT VALUE 1 FROM %s LIMIT 1;" % self.DATASET
        for proxied in (False, True):
            status, body = self._analytics(query, token=token, proxied=proxied)
            self.assertEqual(status, 200,
                             "Valid token should be accepted (proxied=%s). "
                             "status=%s body=%s" % (proxied, status, body))

        for name, headers in self._bad_token_headers():
            for proxied in (False, True):
                status, body = self._analytics(
                    "SELECT VALUE 1;", token="", proxied=proxied,
                    extra_headers=headers)
                self.assertEqual(
                    status, 401,
                    "%s should be rejected with 401 (proxied=%s). status=%s "
                    "body=%s" % (name, proxied, status, body))

        # Basic auth must keep working with JWT enabled cluster-wide.
        for proxied in (False, True):
            status, body = self._analytics("SELECT VALUE 1;", proxied=proxied)
            self.assertEqual(status, 200,
                             "Basic auth regressed with JWT enabled "
                             "(proxied=%s). status=%s body=%s"
                             % (proxied, status, body))

    def test_jwt_analytics_rbac_parity(self):
        """JWT is an authentication swap and must not change authorization.

        Each cell runs as a local Basic-auth user holding the role and as a JWT
        user mapped to the same role; the verdicts must agree. All mismatches
        are collected so one divergence does not hide the rest.
        """
        self._configure(rolesClaim=self.roles_claim)
        self._wait_for_jwt(self._role_token("analytics_reader"))

        operations = self._matrix_operations()
        mismatches = []
        for role in self.MATRIX_ROLES:
            mismatches += self._compare_auth(role, operations)

        # Bucket-scoped isolation: a scoped role may act on its own bucket and
        # must be refused on any other.
        drop_ds = "DROP DATASET %s IF EXISTS;" % self.DDL_DATASET
        scoped = self._mint(
            "jwtan-scoped@example.com",
            {self.roles_claim: ["analytics_manager[%s]" % self.BUCKET]})
        self._wait_for_jwt(scoped)
        own = self._attempt(
            drop_ds, "CREATE DATASET %s ON `%s`;" % (self.DDL_DATASET,
                                                     self.BUCKET),
            drop_ds, token=scoped)
        other = self._attempt(
            drop_ds, "CREATE DATASET %s ON `%s`;" % (self.DDL_DATASET,
                                                     self.OTHER_BUCKET),
            drop_ds, token=scoped)
        self.assertEqual(own, "ALLOW",
                         "analytics_manager[%s] should be allowed on its own "
                         "bucket, got %s" % (self.BUCKET, own))
        self.assertEqual(other, "DENY(403)",
                         "analytics_manager[%s] should be denied on %s, got %s"
                         % (self.BUCKET, self.OTHER_BUCKET, other))

        self.assertEqual(mismatches, [],
                         "JWT and Basic auth disagreed:\n  %s"
                         % "\n  ".join(mismatches))

    def test_jwt_analytics_link_parity(self):
        """Connect/disconnect of the Local link.

        Kept out of the main matrix because it mutates cluster-wide ingestion
        state: a failure here must not leave ingestion off for whatever runs
        next, so the link is restored in a finally.
        """
        self._configure(rolesClaim=self.roles_claim)
        self._wait_for_jwt(self._role_token("analytics_reader"))

        operations = [("disconnect local link", None,
                       "DISCONNECT LINK Local;", "CONNECT LINK Local;")]
        mismatches = []
        try:
            for role in self.MATRIX_ROLES:
                mismatches += self._compare_auth(role, operations)
        finally:
            self._admin_exec("CONNECT LINK Local;", strict=False)

        self.assertEqual(mismatches, [],
                         "JWT and Basic auth disagreed on link control:\n  %s"
                         % "\n  ".join(mismatches))

    def test_jwt_analytics_role_enforcement(self):
        """Authentication succeeding is not authorization succeeding.

        A token can be perfectly valid -- signature, issuer, audience, expiry
        all good, and ns_server resolves a real identity for it -- and still be
        refused by Analytics because the roles it carries do not grant Analytics
        access. That must come back as 403 (authenticated, not permitted), never
        401, or clients cannot tell a bad token from an under-privileged one.

        Also pins how ns_server treats role claims it cannot honour: they are
        dropped silently rather than rejected, so the user authenticates with an
        empty role set.
        """
        self._configure(rolesClaim=self.roles_claim)
        query = "SELECT VALUE 1 FROM %s LIMIT 1;" % self.DATASET

        # Positive control: a mapped Analytics role authenticates and is allowed.
        good = self._mint("jwtan-enforce-ok@example.com",
                          {self.roles_claim: ["analytics_reader"]})
        self._wait_for_jwt(good)
        self.assertEqual(self._whoami_roles(good), ["analytics_reader"],
                         "Mapped role should be visible on /whoami")
        status, body = self._analytics(query, token=good)
        self.assertEqual(status, 200,
                         "Mapped analytics_reader should be allowed. "
                         "status=%s body=%s" % (status, body))

        # A real, non-Analytics role: authentication succeeds and the role is
        # genuinely granted, but Analytics refuses it -- 403, not 401.
        other = self._mint("jwtan-enforce-ro@example.com",
                           {self.roles_claim: ["ro_admin"]})
        self.assertEqual(self._whoami_roles(other), ["ro_admin"],
                         "ro_admin should be granted -- this case is about "
                         "authorization, not a failed login")
        status, body = self._analytics(query, token=other)
        self.assertEqual(status, 403,
                         "A valid token holding only ro_admin must be refused "
                         "by Analytics with 403 (authenticated but not "
                         "permitted), got %s: %s" % (status, body))

        # Role claims ns_server cannot honour are dropped silently: the user
        # still authenticates, with no roles, and is then denied.
        for label, claims in [
                ("unknown role name",
                 {self.roles_claim: ["nonexistent_role_xyz"]}),
                ("roles claim absent", {}),
                # A bucket-scoped role given without its [bucket] parameter is
                # discarded rather than rejected -- worth pinning, since it
                # looks like a working config.
                ("scoped role missing its parameter",
                 {self.roles_claim: ["analytics_manager"]})]:
            token = self._mint("jwtan-enforce-%s@example.com"
                               % label.replace(" ", "-"), claims)
            roles = self._whoami_roles(token)
            self.assertEqual(roles, [],
                             "%s should leave the user with no roles, got %s"
                             % (label, roles))
            status, body = self._analytics(query, token=token)
            self.assertEqual(status, 403,
                             "%s should be denied by Analytics with 403, got "
                             "%s: %s" % (label, status, body))

    def test_jwt_analytics_jit_provisioning(self):
        """With JIT provisioning off, the token's role claim is ignored.

        Roles then come from the pre-created external user record instead, so a
        token asking for more than its record grants must not get it. This is
        the configuration an operator uses when they want roles managed in
        Couchbase rather than asserted by the IdP.
        """
        self._configure(jitProvisioning=False, rolesClaim=self.roles_claim)
        query = "SELECT VALUE 1 FROM %s LIMIT 1;" % self.DATASET
        ddl = "CREATE DATAVERSE %s;" % self.DATAVERSE

        # Warm-up: JWT config takes a moment to propagate to cbas. With JIT
        # off a bare role claim can never return 200, so an external user
        # record is needed here to confirm propagation like every other test.
        user = "jwtan-jit-known@example.com"
        self.jwt_utils.create_external_user(self.rest, user,
                                            roles="analytics_reader")
        if user not in self.external_users:
            self.external_users.append(user)
        self._wait_for_jwt(self._mint(user, {self.roles_claim: ["analytics_reader"]}))

        # No external user record: the token still authenticates, but no roles
        # are provisioned from the claim, so Analytics denies it.
        unknown = self._mint("jwtan-jit-unknown@example.com",
                             {self.roles_claim: ["analytics_reader"]})
        roles = self._whoami_roles(unknown)
        self.assertEqual(roles, [],
                         "With JIT off and no user record the role claim must "
                         "be ignored, got %s" % roles)
        status, body = self._analytics(query, token=unknown)
        self.assertEqual(status, 403,
                         "Unprovisioned user must be denied, got %s: %s"
                         % (status, body))

        # Pre-created record grants analytics_reader while the token claims
        # analytics_admin -- the record must win in both directions. Config
        # propagation was already confirmed by the warm-up above, so no
        # further _wait_for_jwt is needed for this same issuer.
        token = self._mint(user, {self.roles_claim: ["analytics_admin"]})
        self.assertEqual(self._whoami_roles(token), ["analytics_reader"],
                         "The external user record must win over the token's "
                         "role claim")

        status, body = self._analytics(query, token=token)
        self.assertEqual(status, 200,
                         "The record's analytics_reader should allow a query, "
                         "got %s: %s" % (status, body))

        verdict = self._attempt("DROP DATAVERSE %s IF EXISTS;" % self.DATAVERSE,
                                ddl,
                                "DROP DATAVERSE %s IF EXISTS;" % self.DATAVERSE,
                                token=token)
        self.assertEqual(verdict, "DENY(403)",
                         "The token claimed analytics_admin but the record "
                         "grants only analytics_reader, so DDL must be denied; "
                         "got %s" % verdict)

    def test_jwt_analytics_role_mapping_equivalence(self):
        """How a role was granted must not change what it permits.

        rolesMaps (claim value -> role) and groupsMaps (claim value -> Couchbase
        group -> that group's roles) are different code paths in ns_server and
        both are legitimate production configurations, so they must agree.
        """
        subset = [op for op in self._matrix_operations()
                  if op[0] in ("query dataset", "create dataset")]
        claim_value = "analytics-mapped"
        mismatches = []

        for role in ("analytics_reader", "analytics_manager"):
            group = self._create_group(role)

            self._configure(
                rolesClaim=self.roles_claim,
                rolesMaps=["^%s$ %s" % (claim_value, self._role_spec(role))])
            roles_token = self._mint("jwtan-rolesmap-%s@example.com" % role,
                                     {self.roles_claim: [claim_value]})
            self._wait_for_jwt(roles_token)
            via_roles = self._verdicts(subset, roles_token)

            self._configure(
                groupsClaim=self.groups_claim,
                groupsMaps=["^%s$ %s" % (claim_value, group)])
            groups_token = self._mint("jwtan-groupsmap-%s@example.com" % role,
                                      {self.groups_claim: [claim_value]})
            self._wait_for_jwt(groups_token)
            via_groups = self._verdicts(subset, groups_token)

            for label in via_roles:
                self.log.info("  %-16s %-20s rolesMaps=%-10s groupsMaps=%s",
                              label, role, via_roles[label], via_groups[label])
                if via_roles[label] != via_groups[label]:
                    mismatches.append(
                        "%s / %s: rolesMaps=%s groupsMaps=%s"
                        % (label, role, via_roles[label], via_groups[label]))

        self.assertEqual(mismatches, [],
                         "rolesMaps and groupsMaps disagreed:\n  %s"
                         % "\n  ".join(mismatches))

        # Collecting mode: two rules match the same claim value so the user gets
        # both roles. No single built-in role grants DDL *and* data read, so this
        # is how an operator actually gets a working Analytics admin over JWT.
        # The field is rolesMapsStopFirstMatch, not stopFirstMatch -- an unknown
        # issuer key is rejected with 400, not ignored.
        self._configure(
            rolesClaim=self.roles_claim,
            rolesMapsStopFirstMatch=False,
            rolesMaps=["^cbas-full$ analytics_admin",
                       "^cbas-full$ analytics_reader"])
        stacked = self._mint("jwtan-stacked@example.com",
                             {self.roles_claim: ["cbas-full"]})
        self._wait_for_jwt(stacked)
        verdicts = self._verdicts(subset, stacked)
        self.assertEqual(verdicts["create dataset"], "ALLOW",
                         "Stacked roles should grant DDL via analytics_admin, "
                         "got %s" % verdicts["create dataset"])
        self.assertEqual(verdicts["query dataset"], "ALLOW",
                         "Stacked roles should grant data read via "
                         "analytics_reader, got %s"
                         % verdicts["query dataset"])
