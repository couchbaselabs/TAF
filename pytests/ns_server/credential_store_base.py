import os

import urllib3
from membase.api.rest_client import RestConnection

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from couchbase_utils.security_utils.credential_store_utils import CredentialStoreUtils
from pytests.onPrem_basetestcase import ClusterSetup


class CredentialStoreBase(ClusterSetup):
    """
    Base class for Credential Store tests against Couchbase Server 8.1+ Enterprise.

    Provides setUp/tearDown, test-user provisioning, store-settings management,
    and shared helpers.  Test classes extend this and implement test_* methods.

    Credential store prerequisites:
    - Enterprise Edition 8.1+
    - Config encryption ON (default in 8.1)
    - n2n encryption ON, OR n2nEncryptionOverride=true

    By default setUp sets n2nEncryptionOverride=true so that tests work on
    clusters without n2n enabled.  S5 overrides this explicitly to test the
    prerequisite enforcement path.

    Consume-path tests (Pattern A / Pattern B) require a cbauth service password.
    Supply it via:
      -p cbq_engine_password=<value>   or   CBAUTH_CBQ_ENGINE_PASSWORD env var
    If neither is set, consume assertions are skipped with a clear log message.
    """

    # Local test user configuration
    SEC_ADMIN_USER = "cs_sec_admin"
    SEC_ADMIN_PASS = "Couchbase@1234"

    ALICE_USER = "cs_alice"
    ALICE_PASS = "Alice@1234"

    BOB_USER = "cs_bob"
    BOB_PASS = "Bob@1234"

    def setUp(self):
        super().setUp()

        self.cs_utils = CredentialStoreUtils(log=self.log)
        self.rest = RestConnection(self.cluster.master)

        # Cluster connectivity details for direct HTTP calls
        self.cluster_master_ip = (
            getattr(self.cluster.master, "ip", None)
            or getattr(self.cluster.master, "hostname", None)
        )
        self.cluster_port = getattr(self.cluster.master, "port", None) or 8091
        try:
            cluster_port = int(self.cluster_port)
        except (TypeError, ValueError):
            self.cluster_port = cluster_port = 8091
        self.cluster_use_https = self.input.param(
            "cluster_use_https", cluster_port == 18091
        )
        scheme = "https" if self.cluster_use_https else "http"
        self.cluster_base_url = f"{scheme}://{self.cluster_master_ip}:{self.cluster_port}"

        # cbauth service credential (needed for Pattern A / B consume tests)
        # Resolved automatically from ns_config via diag/eval; explicit param
        # or env var takes priority for local overrides.
        self.cbq_engine_password = self._resolve_cbq_engine_password()

        # Credential IDs created during a test — cleaned up in tearDown
        self._created_creds = []
        # Service names whose roles were modified — reset in tearDown
        self._modified_services = []

        # Provision test users and initialise store settings
        self._provision_test_users()
        self._enable_n2n_override()

    def tearDown(self):
        try:
            self._cleanup_created_credentials()
        except Exception as exc:
            self.log.warning(f"Credential cleanup error: {exc}")
        try:
            self._cleanup_service_roles()
        except Exception as exc:
            self.log.warning(f"Service role cleanup error: {exc}")
        try:
            self.cs_utils.delete_store_settings(self.rest)
            self.log.info("Store settings reset to defaults")
        except Exception as exc:
            self.log.warning(f"Store settings reset error: {exc}")
        try:
            self._deprovision_test_users()
        except Exception as exc:
            self.log.warning(f"Test user cleanup error: {exc}")
        finally:
            super().tearDown()

    # ── Secret resolution ─────────────────────────────────────────────────────

    def _secret(self, param_name, env_name, *, default=None, required=True):
        """
        Resolve a secret from TAF param or environment variable.

        Jenkins binds secrets as environment variables; local runs can use
        `export <ENV>=<value>` or pass `-p <param>=<value>`.
        """
        value = self.input.param(param_name, os.environ.get(env_name) or default)
        if required and not value:
            self.fail(
                f"Missing required secret '{param_name}': "
                f"set env var {env_name} or pass -p {param_name}=<value>"
            )
        return value

    # ── cbauth service password resolution ───────────────────────────────────

    def _resolve_cbq_engine_password(self):
        """
        Resolve the @cbq-engine cbauth service password.

        Resolution order:
          1. Test param  -p cbq_engine_password=<value>
          2. Environment variable  CBAUTH_CBQ_ENGINE_PASSWORD
          3. ns_config via diag/eval — reads the real-time rotating token
             directly from the cluster, no Jenkins pre-config required.

        Returns the password string or None if all sources are unavailable.
        """
        explicit = self._secret(
            "cbq_engine_password", "CBAUTH_CBQ_ENGINE_PASSWORD", required=False
        )
        if explicit:
            self.log.info("Using explicitly supplied cbq_engine_password")
            return explicit

        password = self._fetch_cbq_engine_password_via_diag_eval()
        if password:
            return password

        self.log.warning(
            "@cbq-engine cbauth password could not be resolved. "
            "Consume-path assertions (Pattern A / B) will be skipped. "
            "To force-enable: -p cbq_engine_password=<value> or "
            "export CBAUTH_CBQ_ENGINE_PASSWORD=<value>."
        )
        return None

    def _fetch_cbq_engine_password_via_diag_eval(self):
        """
        Read the @cbq-engine cbauth token from ns_config via POST /diag/eval.

        ns_server stores rotating service auth tokens in ns_config.
        We try multiple known key paths (varies by ns_server version) and
        return the first non-empty result.

        diag/eval is already enabled on all nodes by ClusterSetup.setUp().
        """
        # Each tuple: (description, Erlang expression)
        # Expressions return the password string or the atom 'undefined'.
        erlang_expressions = [
            (
                "ns_config_auth:get_password(special)",
                # The canonical call used in ns_server QA tests.
                # Returns the shared "special" cbauth password that all
                # internal services (including @cbq-engine) use to authenticate
                # to /_cbauth/ endpoints.
                "ns_config_auth:get_password(special).",
            ),
            (
                "cbauth service token (8.1+)",
                'case ns_config:search(ns_config:latest(), '
                '{cbauth_service_password, "@cbq-engine"}) of '
                '{value, P} when is_binary(P) -> binary_to_list(P); '
                '{value, P} when is_list(P) -> P; '
                '_ -> undefined end.',
            ),
            (
                "cbauth node prop",
                'ns_config:search_node_prop(node(), ns_config:latest(), '
                'cbauth, "@cbq-engine", undefined).',
            ),
            (
                "special_user_password",
                'case ns_config:search(ns_config:latest(), '
                '{special_user_password, "@cbq-engine"}) of '
                '{value, P} when is_binary(P) -> binary_to_list(P); '
                '{value, P} when is_list(P) -> P; '
                '_ -> undefined end.',
            ),
        ]

        for description, expr in erlang_expressions:
            try:
                ok, result = self.rest.diag_eval(expr)
                if not ok or not result:
                    continue
                password = result.strip().strip('"').strip("'")
                if password and password not in ("undefined", "false", "<<>>", ""):
                    self.log.info(
                        f"@cbq-engine password retrieved via diag/eval "
                        f"({description})"
                    )
                    return password
            except Exception as exc:
                self.log.debug(
                    f"diag/eval attempt '{description}' failed: {exc}"
                )
                continue

        self.log.debug(
            "All diag/eval expressions for @cbq-engine password returned "
            "no result — service may not be registered on this node."
        )
        return None

    # ── Store settings management ─────────────────────────────────────────────

    def _enable_n2n_override(self):
        """
        Enable n2nEncryptionOverride so credential CRUD works on clusters
        that do not have n2n encryption enabled.  configEncryptionOverride stays
        false (config encryption is ON by default in 8.1).
        """
        status, content = self.cs_utils.put_store_settings(
            self.rest,
            config_encryption_override=False,
            n2n_encryption_override=True,
        )
        if not self.cs_utils._is_success_status(status):
            self.log.warning(
                f"Could not enable n2nEncryptionOverride (status={status}). "
                "Credential CRUD may fail if n2n is not enabled on the cluster. "
                f"content={content}"
            )
        else:
            self.log.info("n2nEncryptionOverride=true: credential CRUD enabled")

    def _set_strict_store_settings(self):
        """Disable both overrides — used in S5 prerequisite tests."""
        return self.cs_utils.put_store_settings(
            self.rest,
            config_encryption_override=False,
            n2n_encryption_override=False,
        )

    # ── Test user provisioning ────────────────────────────────────────────────

    def _provision_test_users(self):
        """
        Create sec_admin, alice, and bob local users.

        Fails the test immediately if any user cannot be created — consume-path
        tests depend on alice and bob existing with known passwords.
        """
        for username, password, roles in (
            (self.SEC_ADMIN_USER, self.SEC_ADMIN_PASS, "security_admin"),
            (self.ALICE_USER, self.ALICE_PASS, ""),
            (self.BOB_USER, self.BOB_PASS, ""),
        ):
            status, content = self.cs_utils.create_local_user(
                self.rest, username, password, roles
            )
            if not self.cs_utils._is_success_status(status):
                self.fail(
                    f"Failed to provision test user '{username}': "
                    f"status={status} content={content}. "
                    "Tests cannot proceed without required users."
                )
            self.log.info(f"Test user provisioned: {username}")

    def _deprovision_test_users(self):
        """Delete sec_admin, alice, and bob local users."""
        for username in (self.SEC_ADMIN_USER, self.ALICE_USER, self.BOB_USER):
            self.cs_utils.delete_local_user(self.rest, username)

    # ── Credential lifecycle helpers ──────────────────────────────────────────

    def _create_tracked_credential(self, cred_id, payload, username=None, password=None):
        """
        Create a credential and register it for tearDown cleanup.

        Returns:
            tuple: (status_code, content)
        """
        status, content = self.cs_utils.create_credential(
            self.rest, cred_id, payload, username=username, password=password
        )
        if self.cs_utils._is_success_status(status):
            if cred_id not in self._created_creds:
                self._created_creds.append(cred_id)
        return status, content

    def _cleanup_created_credentials(self):
        """Delete all credentials tracked in self._created_creds."""
        for cred_id in list(self._created_creds):
            status, _ = self.cs_utils.delete_credential(self.rest, cred_id)
            if self.cs_utils._is_success_status(status) or status == 404:
                self._created_creds.remove(cred_id)
                self.log.info(f"Deleted credential: {cred_id}")
            else:
                self.log.warning(f"Could not delete credential '{cred_id}': status={status}")

    def _cleanup_service_roles(self):
        """Delete roles for all services touched during the test."""
        for service_name in list(self._modified_services):
            status, _ = self.cs_utils.delete_service_roles(self.rest, service_name)
            if self.cs_utils._is_success_status(status) or status == 404:
                self._modified_services.remove(service_name)
                self.log.info(f"Service roles deleted: {service_name}")
            else:
                self.log.warning(
                    f"Could not delete service roles for '{service_name}': status={status}"
                )

    # ── Consume-path helpers ──────────────────────────────────────────────────

    def _require_cbq_password(self):
        """
        Fail the test immediately if @cbq-engine cbauth password is not available.

        Call at the start of any test that exercises the consume path (S2, S3, S4).
        Password is resolved in setUp via ns_config_auth:get_password(special);
        failure here means the cluster did not return it (e.g. no n1ql node, or
        diag/eval disabled).
        """
        if not self.cbq_engine_password:
            self.fail(
                "P0 consume test requires @cbq-engine cbauth password. "
                "Resolution via ns_config_auth:get_password(special) failed. "
                "Ensure diag/eval is enabled and admin REST auth works on this cluster. "
                "Override: -p cbq_engine_password=<value> or "
                "export CBAUTH_CBQ_ENGINE_PASSWORD=<value>."
            )

    def _consume_as_cbq_on_behalf_of(self, cred_id, user, domain):
        """
        Pattern A consume: @cbq-engine on behalf of user/domain.

        Callers must invoke _require_cbq_password() before calling this.
        Returns (status_code, body).
        """
        return self.cs_utils.consume_credential(
            base_url=self.cluster_base_url,
            cred_id=cred_id,
            service_name="cbq-engine",
            service_password=self.cbq_engine_password,
            on_behalf_user=user,
            on_behalf_domain=domain,
        )

    def _consume_as_cbq_service_identity(self, cred_id):
        """
        Attempt Pattern B consume: @cbq-engine on behalf of @cbq-engine/admin.

        Returns (status_code, body) or (None, None) if service password is unavailable.
        """
        return self._consume_as_cbq_on_behalf_of(cred_id, "@cbq-engine", "admin")

    def _grant_service_roles(self, service_name, roles):
        """
        Grant roles to a service via PUT /settings/rbac/services/:name/roles.
        Registers the service in _modified_services for tearDown cleanup.
        """
        status, content = self.cs_utils.put_service_roles(
            self.rest, service_name, roles
        )
        if self.cs_utils._is_success_status(status):
            if service_name not in self._modified_services:
                self._modified_services.append(service_name)
        return status, content

    # ── Assertion helpers ─────────────────────────────────────────────────────

    def _assert_consume_allowed(self, status, body, context=""):
        """Assert consume returned 200. Fails if status is None (password missing)."""
        self.assertIsNotNone(
            status,
            f"Consume returned no status ({context}) — "
            "call _require_cbq_password() before using this assertion.",
        )
        self.assertEqual(
            int(status), 200,
            f"Expected consume 200 ({context}), got status={status} body={body}",
        )

    def _assert_consume_denied(self, status, body, expected_error_code, context=""):
        """Assert consume returned 403 with the expected error code. Fails if status is None."""
        self.assertIsNotNone(
            status,
            f"Consume returned no status ({context}) — "
            "call _require_cbq_password() before using this assertion.",
        )
        self.assertEqual(
            int(status), 403,
            f"Expected consume 403/{expected_error_code} ({context}), "
            f"got status={status} body={body}",
        )
        actual_code = self.cs_utils.get_cbauth_error_code(body)
        self.assertEqual(
            actual_code, expected_error_code,
            f"Expected cbauth error code '{expected_error_code}' ({context}), "
            f"got '{actual_code}'. body={body}",
        )

    def _assert_consume_has_secret(self, body, cred_id, expected_secret):
        """
        Assert that a consume 200 response contains the plaintext secret.

        The consume path decrypts the credential and returns fields in cleartext —
        unlike admin GET responses which mask sensitive fields as '********'.
        """
        self.assertIsNotNone(body, "Consume 200 body should not be None")
        if isinstance(body, dict):
            self.assertEqual(
                body.get("id"), cred_id,
                f"Consume response id should be '{cred_id}'. body={body}",
            )
            self.assertEqual(
                body.get("fields", {}).get("secretAccessKey"), expected_secret,
                f"Consume 200 must return plaintext secretAccessKey='{expected_secret}'. "
                f"fields={body.get('fields')}",
            )

    def _get_credential_guardrails(self, cred_id):
        """
        GET the credential and return its guardrails dict.

        Handles both flat (guardrails at top level) and nested
        (meta.guardrails) response shapes.

        Returns:
            dict: guardrails dict, or {} if absent/unreadable
        """
        _, content = self.cs_utils.get_credential(self.rest, cred_id)
        parsed = self.cs_utils.parse_content(content)
        if not isinstance(parsed, dict):
            return {}
        guardrails = parsed.get("meta", {}).get("guardrails", {})
        if not guardrails:
            guardrails = parsed.get("guardrails", {})
        return guardrails or {}

    def _verify_user_has_consume_role(self, username, cred_id):
        """
        Assert that a local user has a credential_consumer role for cred_id.

        Couchbase RBAC GET returns parameterized roles in two shapes:
          - bracket notation: {"role": "credential_consumer[p0-s3-c1]"}
          - separate field:   {"role": "credential_consumer", "credential_id": "p0-s3-c1"}

        Either shape is accepted as proof the grant was persisted.
        """
        encoded_user = self.cs_utils._encode_path_segment(username)
        url = (
            f"{self.cs_utils._base_url(self.rest)}/"
            f"settings/rbac/users/local/{encoded_user}"
        )
        ok, content, _ = self.rest._http_request(url, "GET")
        if not ok or not content:
            self.log.warning(
                f"Could not GET user '{username}' to verify consume role — skipping check"
            )
            return
        user_data = self.cs_utils.parse_content(content)
        if not isinstance(user_data, dict):
            return
        role_entries = [r for r in user_data.get("roles", []) if isinstance(r, dict)]
        has_role = any(
            r.get("role") == f"credential_consumer[{cred_id}]"
            or (
                r.get("role") == "credential_consumer"
                and r.get("credential_id", r.get("params", {}).get("credential_id")) == cred_id
            )
            for r in role_entries
        )
        self.assertTrue(
            has_role,
            f"User '{username}' should have credential_consumer role for '{cred_id}' "
            f"after grant. Actual role entries: {role_entries}",
        )

    def _verify_user_no_consume_role(self, username):
        """
        Assert that a local user has no credential_consumer roles.

        Used to confirm a 'bob' baseline before the consume deny assertion —
        prevents false passes if a prior test accidentally granted consume to this user.
        """
        encoded_user = self.cs_utils._encode_path_segment(username)
        url = (
            f"{self.cs_utils._base_url(self.rest)}/"
            f"settings/rbac/users/local/{encoded_user}"
        )
        ok, content, _ = self.rest._http_request(url, "GET")
        if not ok or not content:
            self.log.warning(
                f"Could not GET user '{username}' to verify no consume role — "
                "consume-deny assertion may not be grounded"
            )
            return
        user_data = self.cs_utils.parse_content(content)
        if not isinstance(user_data, dict):
            return
        roles = [
            r.get("role", "") for r in user_data.get("roles", []) if isinstance(r, dict)
        ]
        consume_roles = [r for r in roles if r.startswith("credential_consumer")]
        self.assertEqual(
            consume_roles, [],
            f"User '{username}' must have no credential_consumer roles before "
            f"consume-denied assertion. Found: {consume_roles}",
        )
