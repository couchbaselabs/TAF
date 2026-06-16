import os
import time
import urllib.parse

import requests
import urllib3
from membase.api.rest_client import RestConnection
from shell_util.remote_connection import RemoteMachineShellConnection

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from couchbase_utils.security_utils.credential_store_utils import (
    CredentialStoreUtils,
    ERROR_CREDENTIAL_EXPIRED,
)
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

    USER_ADMIN_USER = "cs_user_admin"
    USER_ADMIN_PASS = "UserAdmin@1234"

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

    def _fetch_cbq_engine_password_via_diag_eval(self, rest=None):
        """
        Read the @cbq-engine cbauth token from ns_config via POST /diag/eval.

        The special cbauth password is PER-NODE — each ns_server generates its
        own local token.  Pass `rest` to fetch the password for a specific node
        (e.g. a non-master node in cross-node tests).  Defaults to self.rest
        (the master) when omitted.

        ns_server stores rotating service auth tokens in ns_config.
        We try multiple known key paths (varies by ns_server version) and
        return the first non-empty result.

        diag/eval is already enabled on all nodes by ClusterSetup.setUp().
        """
        rest = rest or self.rest
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
                ok, result = rest.diag_eval(expr)
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
            (self.USER_ADMIN_USER, self.USER_ADMIN_PASS, "user_admin_local"),
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
        """Delete all provisioned test users."""
        for username in (self.SEC_ADMIN_USER, self.USER_ADMIN_USER, self.ALICE_USER, self.BOB_USER):
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

    # ── Multi-node helpers ────────────────────────────────────────────────────

    def _node_base_url(self, server):
        """
        Build the management base URL for any cluster node.

        Handles explicit-None port the same way setUp does for the master node.
        Use this to construct base_url values for cross-node consume calls.
        """
        ip = getattr(server, "ip", None) or getattr(server, "hostname", None)
        port = getattr(server, "port", None) or 8091
        try:
            port = int(port)
        except (TypeError, ValueError):
            port = 8091
        scheme = "https" if port == 18091 else "http"
        return f"{scheme}://{ip}:{port}"

    def _non_master_node(self):
        """
        Return the first server in self.cluster.servers that is NOT the master.

        Compares by IP address rather than object identity because TAF can
        return different ServerInfo instances for the same physical node.
        Fails the test immediately if no second node is available — the caller
        should only invoke this when nodes_init>=2 is guaranteed by the conf.
        """
        master_ip = getattr(self.cluster.master, "ip", None)
        for server in self.cluster.servers:
            if getattr(server, "ip", None) != master_ip:
                return server
        self.fail(
            "Cross-node test requires nodes_init>=2 but no non-master node found. "
            "Check that node.ini has 2+ servers and nodes_init=2 is set."
        )

    def _poll_until(self, label, fn, timeout_s=15, interval_s=0.5):
        """
        Poll fn() every interval_s until it returns a truthy value or timeout_s elapses.

        Use this before cross-node consume calls to wait for Chronicle replication
        without introducing a fixed sleep. Fails the test with a clear message if
        the condition is never satisfied within the timeout.

        Args:
            label:      Human-readable description logged on timeout.
            fn:         Zero-argument callable; truthy return = condition met.
            timeout_s:  Maximum seconds to wait (default 15).
            interval_s: Polling interval in seconds (default 0.5).
        """
        deadline = time.time() + timeout_s
        last = None
        while time.time() < deadline:
            last = fn()
            if last:
                return last
            time.sleep(interval_s)
        self.fail(
            f"Timed out after {timeout_s}s waiting for: {label}. last_result={last!r}"
        )

    # ── Node lifecycle helpers ──

    def _stop_couchbase_on_node(self, server):
        """
        Stop the Couchbase service on `server` via SSH.

        Uses systemctl (or the non-root equivalent on the target platform).
        Always disconnects the SSH session before returning.
        """
        shell = RemoteMachineShellConnection(server)
        try:
            self.log.info(f"Stopping couchbase-server on {server.ip}")
            shell.stop_couchbase()
        finally:
            shell.disconnect()

    def _start_couchbase_on_node(self, server, timeout_s=120):
        """
        Start the Couchbase service on `server` via SSH and wait until healthy.

        Polls cluster_util.is_ns_server_running() up to timeout_s seconds.
        Fails the test if the node does not come back within that window.
        """
        shell = RemoteMachineShellConnection(server)
        try:
            self.log.info(f"Starting couchbase-server on {server.ip}")
            shell.start_couchbase()
        finally:
            shell.disconnect()
        self.log.info(f"Waiting up to {timeout_s}s for {server.ip} to become healthy")
        if not self.cluster_util.is_ns_server_running(server, timeout_in_seconds=timeout_s):
            self.fail(
                f"Node {server.ip} did not return to 'healthy' within {timeout_s}s "
                "after couchbase-server restart."
            )
        self.log.info(f"{server.ip} is healthy")

    def _is_node_rest_reachable(self, server, connect_timeout=2):
        """Return True if the node's /pools endpoint responds within connect_timeout seconds."""
        port = getattr(server, "port", None) or 8091
        try:
            port = int(port)
        except (TypeError, ValueError):
            port = 8091
        ip = getattr(server, "ip", None) or getattr(server, "hostname", None)
        scheme = "https" if port == 18091 else "http"
        url = f"{scheme}://{ip}:{port}/pools"
        try:
            requests.get(url, timeout=(connect_timeout, connect_timeout), verify=False)
            return True
        except Exception as exc:
            self.log.debug(
                f"Node {ip} reachability probe failed: {type(exc).__name__}: {exc}"
            )
            return False

    def _consume_as_cbq_on_behalf_of_on_node(
        self, base_url, cred_id, user, domain, service_password=None
    ):
        """
        Pattern A consume targeting a specific node's management URL.

        Use for cross-node tests where consumption must be directed to a
        non-master node.  Callers must invoke _require_cbq_password() first.

        The cbauth special password is per-node.  Pass `service_password` when
        calling a non-master node — fetch it via
        _fetch_cbq_engine_password_via_diag_eval(node_rest).
        Defaults to self.cbq_engine_password (master) when omitted.

        Args:
            base_url:         Management URL of the target node
            cred_id:          Credential ID to consume
            user:             On-behalf-of user (e.g. ALICE_USER)
            domain:           Domain of on_behalf_user (e.g. "local")
            service_password: Node-specific cbauth token; defaults to master's

        Returns:
            tuple: (status_code, body)
        """
        return self.cs_utils.consume_credential(
            base_url=base_url,
            cred_id=cred_id,
            service_name="cbq-engine",
            service_password=service_password if service_password is not None else self.cbq_engine_password,
            on_behalf_user=user,
            on_behalf_domain=domain,
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

    def _run_expiry_enforcement_subtest(self, cred_id, known_secret):
        """
        Full expiry enforcement sub-test with boundary validation.

        Three consume checkpoints:
        1. Immediately after grant → 200 (well before expiry)
        2. At t = expiresAt − 2s (boundary) → 200 (credential still valid)
        3. At t = expiresAt + 6s → 403 CREDENTIAL_EXPIRED

        Checkpoint 2 proves the server does not expire early.
        Checkpoint 3 proves the server enforces expiry.

        Requires cbauth password (@cbq-engine) and cred created with
        allowed_services=["n1ql"] (Pattern A path).
        Gated behind test_expiry_wait=True.
        Typical runtime: expiresAt_offset + ~8s ≈ 6 min when created with +360s.
        """
        self.log.info("Running expiry enforcement sub-test (test_expiry_wait=True)")
        self._require_cbq_password()

        status_grant, _ = self.cs_utils.grant_consume_to_local_user(
            self.rest, self.ALICE_USER, cred_id
        )
        self.assertEqual(
            int(status_grant) if status_grant else 0, 200,
            f"Grant consume role for expiry test expected 200, got {status_grant}",
        )

        status_get, content_get = self.cs_utils.get_credential(self.rest, cred_id)
        self.assertEqual(int(status_get) if status_get else 0, 200,
                         f"GET {cred_id} expected 200, got {status_get}")
        parsed_cred = self.cs_utils.parse_content(content_get)
        expires_at_ms = (
            parsed_cred.get("meta", {}).get("expiresAt")
            or parsed_cred.get("expiresAt")
        ) if isinstance(parsed_cred, dict) else None
        self.assertIsNotNone(
            expires_at_ms,
            f"Credential '{cred_id}' must have expiresAt set. parsed={parsed_cred}",
        )
        current_time_ms = int(time.time() * 1000)
        self.assertGreater(
            expires_at_ms, current_time_ms,
            f"expiresAt already passed before subtest started: "
            f"expires_at_ms={expires_at_ms} <= now_ms={current_time_ms}",
        )
        expires_at_s = expires_at_ms // 1000
        self.log.info(
            f"[EXPIRY-WAIT] expires_at_ms={expires_at_ms} "
            f"time_until_expiry_s={expires_at_s - int(time.time())}s"
        )

        # Checkpoint 1: consume immediately — expect 200
        self.log.info("Checkpoint 1: consume immediately after grant (expect 200)")
        status_c1, body_c1 = self._consume_as_cbq_on_behalf_of(cred_id, self.ALICE_USER, "local")
        self._assert_consume_allowed(status_c1, body_c1, context="checkpoint 1 — well before expiry")
        self._assert_consume_has_secret(body_c1, cred_id, known_secret)
        self.log.info("Checkpoint 1 PASSED: credential valid, plaintext secret returned")

        # Checkpoint 2: consume at t = expiresAt − 2s — expect 200
        wait_to_boundary = max(0, expires_at_s - 2 - int(time.time()))
        if wait_to_boundary > 0:
            self.log.info(f"Waiting {wait_to_boundary}s to reach 2s-before-expiry boundary...")
            self.sleep(wait_to_boundary, "Waiting to reach pre-expiry boundary")
        self.log.info("Checkpoint 2: consume at t=expiresAt−2s boundary (expect 200)")
        status_c2, body_c2 = self._consume_as_cbq_on_behalf_of(cred_id, self.ALICE_USER, "local")
        self._assert_consume_allowed(status_c2, body_c2, context="checkpoint 2 — 2s before expiry")
        self._assert_consume_has_secret(body_c2, cred_id, known_secret)
        self.log.info("Checkpoint 2 PASSED: credential still valid 2s before expiry")

        # Checkpoint 3: consume at t = expiresAt + 6s — expect 403
        self.log.info("Waiting 8s to pass expiry + buffer...")
        self.sleep(8, "Waiting past expiry boundary")
        self.log.info("Checkpoint 3: consume at t=expiresAt+6s (expect 403 CREDENTIAL_EXPIRED)")
        status_c3, body_c3 = self._consume_as_cbq_on_behalf_of(cred_id, self.ALICE_USER, "local")
        self._assert_consume_denied(
            status_c3, body_c3, ERROR_CREDENTIAL_EXPIRED,
            context="checkpoint 3 — 6s after expiry",
        )
        self.log.info(
            f"Checkpoint 3 PASSED: expired credential rejected "
            f"(status={status_c3} error={ERROR_CREDENTIAL_EXPIRED})"
        )

    # ── T11 consume-matrix helpers ────────────────────────────────────────────

    def _t11_update_guardrail(self, cred_id, guardrail, known_secret):
        """Update the T11 credential's allowedServices guardrail in-place."""
        payload = self.cs_utils.build_aws_payload(
            access_key_id="AKIAT11EXAMPLE",
            secret_access_key=known_secret,
            region="us-east-1",
            allowed_services=guardrail,
        )
        status_upd, content_upd = self.cs_utils.update_credential(self.rest, cred_id, payload)
        self.assertEqual(
            int(status_upd) if status_upd else 0, 200,
            f"[T11] Guardrail update to {guardrail} expected 200, got {status_upd}. "
            f"content={content_upd}",
        )

    def _setup_consume_matrix_rbac(self, row, cred_id):
        """Grant the RBAC state required by a T11 matrix row."""
        setup = row.get("setup", "")
        if setup == "no_service_role":
            # Defensively clear any service roles left by a previous row
            for svc in ("n1ql", "backup"):
                try:
                    self.cs_utils.delete_service_roles(self.rest, svc)
                except Exception:
                    pass
        elif setup == "n1ql_service_role":
            status, content = self.cs_utils.put_service_roles(
                self.rest, "n1ql", [f"credential_consumer[{cred_id}]"]
            )
            self.assertEqual(
                int(status) if status else 0, 200,
                f"[T11 {row['id']}] Grant n1ql service role expected 200, got {status}. "
                f"content={content}",
            )
        elif setup == "backup_service_role":
            status, content = self.cs_utils.put_service_roles(
                self.rest, "backup", [f"credential_consumer[{cred_id}]"]
            )
            self.assertEqual(
                int(status) if status else 0, 200,
                f"[T11 {row['id']}] Grant backup service role expected 200, got {status}. "
                f"content={content}",
            )
        # A2: verify bob has no consume role before the deny assertion
        if row.get("user") == self.BOB_USER:
            self._verify_user_no_consume_role(self.BOB_USER)

    def _teardown_consume_matrix_rbac(self, row):
        """Revoke service roles granted for a T11 matrix row (best-effort)."""
        setup = row.get("setup", "")
        try:
            if setup == "n1ql_service_role":
                self.cs_utils.delete_service_roles(self.rest, "n1ql")
            elif setup == "backup_service_role":
                self.cs_utils.delete_service_roles(self.rest, "backup")
        except Exception as exc:
            self.log.warning(
                f"[T11 {row['id']}] RBAC teardown warning (non-fatal): "
                f"{type(exc).__name__}: {exc}"
            )

    def _assert_consume_matrix_row(self, row, status, body):
        """Assert the expected outcome for a T11 matrix row."""
        row_id = row["id"]
        exp_status = row["exp_status"]
        exp_error = row.get("exp_error")
        actual = int(status) if status else 0

        if exp_status == 200:
            self._assert_consume_allowed(status, body, context=f"T11 row {row_id}")
        elif exp_status == 403:
            self._assert_consume_denied(status, body, exp_error, context=f"T11 row {row_id}")
        else:
            self.assertEqual(
                actual, exp_status,
                f"[T11 {row_id}] Expected {exp_status}, got {actual}. body={body}",
            )

    # ── T12 RBAC grant-matrix helpers ─────────────────────────────────────────

    def _get_test_user_password(self, username):
        """Return the known password for a provisioned test user, or None if unknown."""
        return {
            self.ALICE_USER: self.ALICE_PASS,
            self.BOB_USER: self.BOB_PASS,
            self.SEC_ADMIN_USER: self.SEC_ADMIN_PASS,
            self.USER_ADMIN_USER: self.USER_ADMIN_PASS,
        }.get(username)

    def _set_user_roles(self, username, roles_str):
        """
        Set a local user's roles to an explicit value.

        Always uses self.rest (Full Admin) — never a limited actor.
        Accepts any role string, including wildcard patterns such as
        'credential_consumer[db/*]' or 'credential_consumer[*]'.
        Pass roles_str="" to clear all roles (same PUT semantics as
        _teardown_grant_row, which also sends roles= to wipe alice).

        Returns:
            tuple: (status_code, content)
        """
        encoded = self.cs_utils._encode_path_segment(username)
        url = (
            f"{self.cs_utils._base_url(self.rest)}"
            f"/settings/rbac/users/local/{encoded}"
        )
        body_fields = {"roles": roles_str}
        target_password = self._get_test_user_password(username)
        if target_password:
            body_fields["password"] = target_password
            body_fields["name"] = username
        _, content, resp = self.rest._http_request(
            url, "PUT", urllib.parse.urlencode(body_fields)
        )
        return self.cs_utils.status_code(resp), content

    def _actor_credentials(self, actor_name):
        """
        Return (username, password) for a T12 matrix actor name.

        "Administrator" returns (None, None) — self.rest uses its own admin credentials.
        "cbq_engine"   returns ("@cbq-engine", self.cbq_engine_password).
        """
        if actor_name == "cs_sec_admin":
            return self.SEC_ADMIN_USER, self.SEC_ADMIN_PASS
        if actor_name == "cs_user_admin":
            return self.USER_ADMIN_USER, self.USER_ADMIN_PASS
        if actor_name == "cbq_engine":
            return "@cbq-engine", self.cbq_engine_password
        if actor_name == "Administrator":
            return None, None
        self.fail(f"[T12] Unknown actor: {actor_name!r}")

    def _call_grant_api(self, row):
        """
        Execute the API call for one T12 grant-matrix row.

        Dispatches on row["api"]:
          "user_role"           PUT /settings/rbac/users/local/:target
          "service_role"        PUT /settings/rbac/services/:target/roles
          "service_role_delete" DELETE /settings/rbac/services/:target/roles
          "credential_create"   POST /settings/credentials/:target

        Returns (status_code, content).
        """
        api = row["api"]
        target = row["target"]
        role = row.get("role")
        username, password = self._actor_credentials(row["actor"])

        if api == "user_role":
            encoded = self.cs_utils._encode_path_segment(target)
            url = (
                f"{self.cs_utils._base_url(self.rest)}"
                f"/settings/rbac/users/local/{encoded}"
            )
            body_fields = {"roles": role or ""}
            target_password = self._get_test_user_password(target)
            if target_password:
                body_fields["password"] = target_password
                body_fields["name"] = target
            body = urllib.parse.urlencode(body_fields)
            if username:
                headers = self.rest._create_capi_headers(
                    username=username,
                    password=password,
                    contentType="application/x-www-form-urlencoded",
                )
                _, content, resp = self.rest._http_request(url, "PUT", body, headers=headers)
            else:
                _, content, resp = self.rest._http_request(url, "PUT", body)
            return self.cs_utils.status_code(resp), content

        if api == "service_role":
            status, content = self.cs_utils.put_service_roles(
                self.rest, target, [role],
                username=username, password=password,
            )
            if self.cs_utils._is_success_status(status) and target not in self._modified_services:
                self._modified_services.append(target)
            return status, content

        if api == "service_role_delete":
            return self.cs_utils.delete_service_roles(
                self.rest, target,
                username=username, password=password,
            )

        if api == "credential_create":
            payload = self.cs_utils.build_aws_payload(
                access_key_id="AKIAG4EXAMPLE",
                secret_access_key="G4_SECRET_VALUE",
                region="us-east-1",
            )
            status, content = self.cs_utils.create_credential(
                self.rest, target, payload,
                username=username, password=password,
            )
            if self.cs_utils._is_success_status(status) and target not in self._created_creds:
                self._created_creds.append(target)
            return status, content

        self.fail(f"[T12] Unknown API type: {api!r}")

    def _run_grant_row(self, row, cred_id):
        """Execute one T12 grant-matrix row: call → assert → [verify] → teardown."""
        row_id = row["id"]
        exp_status = row["exp_status"]
        raw_role = row.get("role") or ""
        resolved_row = dict(row)
        resolved_row["role"] = raw_role.format(cred_id=cred_id) if raw_role else None
        try:
            status, content = self._call_grant_api(resolved_row)
            actual = int(status) if status else 0
            self.assertEqual(
                actual, exp_status,
                f"[T12 {row_id}] {row['actor']} {row['api']} "
                f"expected {exp_status}, got {actual}. content={content}",
            )
            if actual == 400 and row["api"] == "user_role":
                parsed = self.cs_utils.parse_content(content) or {}
                err = str((parsed.get("errors") or {}).get("roles", ""))
                if err:
                    self.assertTrue(
                        any(w in err.lower() for w in ("undefined", "unknown", "malformed")),
                        f"[T12 {row_id}] 400 error should mention undefined/unknown/malformed. "
                        f"roles_error={err!r}",
                    )
            if actual == 200 and row.get("verify"):
                if row["verify"] == "user_role":
                    self._verify_user_has_consume_role(row["target"], cred_id)
                elif row["verify"] == "service_role":
                    self._verify_service_has_role(row["target"], cred_id)
            self.log.info(f"[T12 {row_id}] PASSED")
        finally:
            self._teardown_grant_row(resolved_row)

    def _verify_service_has_role(self, service_name, cred_id):
        """
        Assert that a service has credential_consumer[cred_id] in its role list.

        Accepts both bracket notation {"role": "credential_consumer[id]"} and
        separate-field notation {"role": "credential_consumer", "credential_id": "id"}.
        """
        status, content = self.cs_utils.get_service_roles(self.rest, service_name)
        if not self.cs_utils._is_success_status(status):
            self.log.warning(
                f"Could not GET service roles for '{service_name}' "
                f"(status={status}) — skipping role verify"
            )
            return
        parsed = self.cs_utils.parse_content(content)
        roles = []
        if isinstance(parsed, list):
            roles = parsed
        elif isinstance(parsed, dict):
            roles = parsed.get("roles", [])
        has_role = any(
            (
                isinstance(r, dict)
                and (
                    r.get("role") == f"credential_consumer[{cred_id}]"
                    or (
                        r.get("role") == "credential_consumer"
                        and r.get(
                            "credential_id",
                            (r.get("params") or {}).get("credential_id"),
                        ) == cred_id
                    )
                )
            )
            or r == f"credential_consumer[{cred_id}]"
            for r in roles
        )
        self.assertTrue(
            has_role,
            f"Service '{service_name}' should have credential_consumer[{cred_id}] after grant. "
            f"Actual roles: {roles}",
        )

    def _teardown_grant_row(self, row):
        """Best-effort cleanup after a T12 grant-matrix row (only needed on 200 rows)."""
        if row["exp_status"] != 200:
            return
        api = row["api"]
        target = row["target"]
        try:
            if api == "user_role":
                encoded = self.cs_utils._encode_path_segment(target)
                url = (
                    f"{self.cs_utils._base_url(self.rest)}"
                    f"/settings/rbac/users/local/{encoded}"
                )
                body_fields = {"roles": ""}
                target_password = self._get_test_user_password(target)
                if target_password:
                    body_fields["password"] = target_password
                    body_fields["name"] = target
                self.rest._http_request(url, "PUT", urllib.parse.urlencode(body_fields))
            elif api == "service_role":
                if target not in self._modified_services:
                    self._modified_services.append(target)
                self.cs_utils.delete_service_roles(self.rest, target)
        except Exception as exc:
            self.log.warning(
                f"[T12 {row['id']}] Teardown warning (non-fatal): "
                f"{type(exc).__name__}: {exc}"
            )

    def _run_t11_expiry_row(self, row, known_secret):
        """Run A6 (expiry) sub-row: create an expiring credential, wait, assert 403 EXPIRED."""
        self._require_cbq_password()
        expiry_cred_id = "p1-t11-a6"
        expires_at_ms = self.cs_utils.expires_at_ms(360)
        payload = self.cs_utils.build_aws_payload(
            access_key_id="AKIAT11A6EXAMPLE",
            secret_access_key=known_secret,
            region="us-east-1",
            allowed_services=row["guardrail"],
            expires_at_ms=expires_at_ms,
        )
        status_create, content_create = self._create_tracked_credential(expiry_cred_id, payload)
        self.assertEqual(
            int(status_create) if status_create else 0, 201,
            f"[T11 A6] Expiry credential create expected 201, got {status_create}. "
            f"content={content_create}",
        )
        status_grant, _ = self.cs_utils.grant_consume_to_local_user(
            self.rest, self.ALICE_USER, expiry_cred_id
        )
        self.assertEqual(
            int(status_grant) if status_grant else 0, 200,
            f"[T11 A6] Grant alice role expected 200, got {status_grant}",
        )

        _, content_get = self.cs_utils.get_credential(self.rest, expiry_cred_id)
        parsed = self.cs_utils.parse_content(content_get)
        server_expires_at = (
            (parsed.get("meta") or {}).get("expiresAt")
            or parsed.get("expiresAt")
            if isinstance(parsed, dict) else None
        )
        self.assertIsNotNone(server_expires_at, "[T11 A6] expiresAt missing from GET response")

        now_ms = int(time.time() * 1000)
        wait_s = max(0, (server_expires_at - now_ms) / 1000) + 6
        self.log.info(f"[T11 A6] Waiting {wait_s:.1f}s for credential to expire")
        self.sleep(wait_s, "Waiting for credential to expire")

        status, body = self.cs_utils.consume_credential(
            base_url=self.cluster_base_url,
            cred_id=expiry_cred_id,
            service_name=row["caller"],
            service_password=self.cbq_engine_password,
            on_behalf_user=row["user"],
            on_behalf_domain=row["domain"],
        )
        self._assert_consume_denied(
            status, body, ERROR_CREDENTIAL_EXPIRED, context="T11 row A6"
        )
        self.log.info("[T11 A6] PASSED — expired credential correctly returns 403 CREDENTIAL_EXPIRED")
