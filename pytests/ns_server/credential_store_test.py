import time

from couchbase_utils.security_utils.credential_store_utils import (
    ERROR_CREDENTIAL_EXPIRED,
    ERROR_INSUFFICIENT_PERMISSIONS,
    ERROR_SERVICE_GUARDRAIL_BLOCKED,
)
from pytests.ns_server.credential_store_base import CredentialStoreBase


class CredentialStoreTest(CredentialStoreBase):
    """
    P0 automation suite for the Credential Store feature (CB 8.1+ Enterprise).

    All setUp/tearDown/helpers live in CredentialStoreBase.
    This class contains only test_* methods mapped to the five P0 scenarios
    from the Credential Store P0 Automation Draft.

    Consume-path tests (S2, S3, S4) require the cbauth service password for
    @cbq-engine.  Supply it via:
        -p cbq_engine_password=<value>
    or set the environment variable CBAUTH_CBQ_ENGINE_PASSWORD.
    Without it, consume assertions are skipped with a warning — all admin-API
    and RBAC assertions still run.
    """

    def test_admin_crud_and_secret_redaction(self):
        """
        Validate core admin behavior and ensure secrets are never exposed.

        Covers:
        - POST aws credential → 201
        - POST 201 body: raw secret absent, secretAccessKey masked as "********"
        - GET by id: id/type correct, raw secret absent, secretAccessKey = "********"
        - GET list: credential present, raw secret absent
        - POST missing required field → 400
        - PUT type change aws→gcp → 400, response mentions immutable
        - DELETE → 200
        - GET deleted credential → 404

        Preconditions (supplied by setUp):
        - n2nEncryptionOverride=true
        - self.rest authenticated as Full Admin
        """
        self.log.info("Testing admin CRUD and secret redaction")

        cred_id = "p0-s1-c1"
        known_secret = "SUPER_SECRET_AKIAIOSFODNN7"
        payload = self.cs_utils.build_aws_payload(
            access_key_id="AKIAIOSFODNN7EXAMPLE",
            secret_access_key=known_secret,
            region="us-east-1",
            description="P0 S1 test credential",
        )

        # Create credential — expect 201
        self.log.info(f"Creating aws credential: {cred_id}")
        status, content = self._create_tracked_credential(cred_id, payload)
        self.assertEqual(
            int(status) if status else 0, 201,
            f"Create expected 201, got {status}. content={content}",
        )
        self.cs_utils.assert_no_raw_secret(content, known_secret, context="POST create")
        parsed_create = self.cs_utils.parse_content(content)
        if isinstance(parsed_create, dict) and parsed_create.get("fields"):
            self.assertEqual(
                parsed_create["fields"].get("secretAccessKey"), "********",
                f"secretAccessKey must be masked as '********' in POST 201 body. "
                f"fields={parsed_create['fields']}",
            )
        self.log.info(f"Create: status={status}, POST body redaction verified")

        # GET by id — 200, id/type correct, secretAccessKey masked
        self.log.info(f"GET credential by id: {cred_id}")
        status_get, content_get = self.cs_utils.get_credential(self.rest, cred_id)
        self.assertEqual(
            int(status_get) if status_get else 0, 200,
            f"GET credential expected 200, got {status_get}",
        )
        self.cs_utils.assert_no_raw_secret(content_get, known_secret, context="GET by id")
        parsed_get = self.cs_utils.parse_content(content_get)
        self.assertIsNotNone(parsed_get, "GET response should be valid JSON")
        self.assertEqual(
            parsed_get.get("id"), cred_id,
            f"GET response id mismatch: expected '{cred_id}', got '{parsed_get.get('id')}'",
        )
        self.assertEqual(
            parsed_get.get("type"), "aws",
            f"GET response type mismatch: expected 'aws', got '{parsed_get.get('type')}'",
        )
        self.assertEqual(
            parsed_get.get("fields", {}).get("secretAccessKey"), "********",
            f"secretAccessKey must be masked as '********' in GET response. "
            f"fields={parsed_get.get('fields')}",
        )
        self.log.info("GET by id: id/type correct, secretAccessKey masked as '********'")

        # GET list — 200, credential present, raw secret absent
        self.log.info("Listing all credentials")
        status_list, content_list = self.cs_utils.list_credentials(self.rest)
        self.assertEqual(
            int(status_list) if status_list else 0, 200,
            f"List credentials expected 200, got {status_list}",
        )
        self.cs_utils.assert_no_raw_secret(content_list, known_secret, context="GET list")
        listed_ids = self.cs_utils.get_credential_ids_from_list(content_list)
        self.assertIn(
            cred_id, listed_ids,
            f"Credential '{cred_id}' should appear in list. Found: {listed_ids}",
        )
        self.log.info(f"List: credential present, raw secret absent. ids={listed_ids}")

        # POST missing required field — expect 400
        self.log.info("Testing POST with missing required field (secretAccessKey)")
        invalid_id = "p0-s1-invalid"
        invalid_payload = {
            "type": "aws",
            "fields": {
                "accessKeyId": "AKIAIOSFODNN7EXAMPLE",
                # secretAccessKey intentionally absent
                "region": "us-east-1",
            },
        }
        status_invalid, content_invalid = self.cs_utils.create_credential(
            self.rest, invalid_id, invalid_payload
        )
        self.assertEqual(
            int(status_invalid) if status_invalid else 0, 400,
            f"POST missing secretAccessKey expected 400, got {status_invalid}. "
            f"content={content_invalid}",
        )
        # Guard: if it unexpectedly succeeded, track for tearDown cleanup
        if self.cs_utils._is_success_status(status_invalid):
            self._created_creds.append(invalid_id)
        self.log.info(f"Missing required field correctly rejected: status={status_invalid}")

        # PUT type change aws→gcp — expect 400, response mentions immutable
        self.log.info(f"Testing PUT type change aws→gcp on {cred_id}")
        type_change_payload = {
            "type": "gcp",
            "fields": {
                # Valid GCP service-account JSON to avoid field-validation failing
                # before the immutable-type check is reached
                "jsonCredentials": '{"type":"service_account","project_id":"p0-s1"}',
            },
        }
        status_type_change, content_type_change = self.cs_utils.update_credential(
            self.rest, cred_id, type_change_payload
        )
        self.assertEqual(
            int(status_type_change) if status_type_change else 0, 400,
            f"PUT type change aws→gcp expected 400, got {status_type_change}. "
            f"content={content_type_change}",
        )
        self.assertIn(
            "immutable", (content_type_change or "").lower()
            if isinstance(content_type_change, str)
            else str(content_type_change or "").lower(),
            f"400 response for type change should mention 'immutable'. "
            f"content={content_type_change}",
        )
        self.log.info(f"Immutable type change correctly rejected: status={status_type_change}")

        # Delete credential — expect 200
        self.log.info(f"Deleting credential: {cred_id}")
        status_del, content_del = self.cs_utils.delete_credential(self.rest, cred_id)
        self.assertEqual(
            int(status_del) if status_del else 0, 200,
            f"DELETE credential expected 200, got {status_del}. content={content_del}",
        )
        if cred_id in self._created_creds:
            self._created_creds.remove(cred_id)
        self.log.info(f"Delete: status={status_del}")

        # GET deleted credential — expect 404
        self.log.info(f"GET deleted credential (expect 404): {cred_id}")
        status_gone, _ = self.cs_utils.get_credential(self.rest, cred_id)
        self.assertEqual(
            int(status_gone) if status_gone else 0, 404,
            f"GET after DELETE expected 404, got {status_gone}",
        )
        self.log.info("GET deleted credential correctly returned 404")

        self.log.info("Admin CRUD, redaction, and validation verified")

    def test_pattern_a_end_user_consumption(self):
        """
        Validate end-user consume path (@cbq-engine on behalf of user).

        Covers:
        - Create aws credential with allowedServices=["n1ql"], verify guardrails persisted
        - Grant alice credential_consumer[cred_id] → 200, verify via GET user roles
        - Verify bob has no credential_consumer roles (explicit pre-check)
        - Consume as @cbq-engine on behalf of alice → 200 + plaintext secret returned
        - Consume as @cbq-engine on behalf of bob (no role) → 403 INSUFFICIENT_PERMISSIONS

        Requires @cbq-engine cbauth password resolved in setUp via
        ns_config_auth:get_password(special).  Fails (not skips) if unresolved.
        Requires a Query (n1ql) node in the cluster (services_init must include n1ql).
        """
        self.log.info("Testing Pattern A end user consumption")

        # Password must be available — fail immediately if not (no silent skip)
        self._require_cbq_password()

        cred_id = "p0-s2-c1"
        known_secret = "S2_SECRET_VALUE"
        payload = self.cs_utils.build_aws_payload(
            access_key_id="AKIAS2EXAMPLE",
            secret_access_key=known_secret,
            region="us-east-1",
            allowed_services=["n1ql"],
            description="P0 S2 Pattern-A test credential",
        )

        # Create credential — expect 201
        self.log.info(f"Creating credential {cred_id} with allowedServices=[n1ql]")
        status_create, content_create = self._create_tracked_credential(cred_id, payload)
        self.assertEqual(
            int(status_create) if status_create else 0, 201,
            f"Create expected 201, got {status_create}. content={content_create}",
        )

        # Verify guardrails persisted: GET and check allowedServices
        status_get, content_get = self.cs_utils.get_credential(self.rest, cred_id)
        self.assertEqual(int(status_get) if status_get else 0, 200,
                         f"GET after create expected 200, got {status_get}")
        parsed_get = self.cs_utils.parse_content(content_get)
        guardrails = (
            parsed_get.get("meta", {}).get("guardrails", {})
            if isinstance(parsed_get, dict)
            else {}
        )
        if not guardrails:
            guardrails = parsed_get.get("guardrails", {}) if isinstance(parsed_get, dict) else {}
        self.assertEqual(
            guardrails.get("allowedServices"), ["n1ql"],
            f"allowedServices should be ['n1ql'] after create. guardrails={guardrails}",
        )
        self.log.info("Credential created and guardrails verified")

        # Verify bob has no consume role before the deny assertion
        self._verify_user_no_consume_role(self.BOB_USER)
        self.log.info(f"Confirmed {self.BOB_USER} has no credential_consumer roles")

        # Grant alice consume role — expect 200
        self.log.info(f"Granting alice credential_consumer[{cred_id}]")
        status_grant, _ = self.cs_utils.grant_consume_to_local_user(
            self.rest, self.ALICE_USER, cred_id
        )
        self.assertEqual(
            int(status_grant) if status_grant else 0, 200,
            f"Grant credential_consumer[{cred_id}] to alice expected 200, "
            f"got {status_grant}",
        )
        self.log.info(f"Role grant: status={status_grant}")

        # Consume as @cbq-engine on behalf of alice — expect 200 + plaintext secret
        self.log.info("Consume: @cbq-engine on behalf of alice/local (expect 200)")
        status_alice, body_alice = self._consume_as_cbq_on_behalf_of(
            cred_id, self.ALICE_USER, "local"
        )
        self._assert_consume_allowed(status_alice, body_alice, context="alice with consume role")
        self._assert_consume_has_secret(body_alice, cred_id, known_secret)
        self.log.info("Alice consume: ALLOWED (200), plaintext secret returned")

        # Consume as @cbq-engine on behalf of bob — expect 403 INSUFFICIENT_PERMISSIONS
        self.log.info("Consume: @cbq-engine on behalf of bob/local (expect 403)")
        status_bob, body_bob = self._consume_as_cbq_on_behalf_of(
            cred_id, self.BOB_USER, "local"
        )
        self._assert_consume_denied(
            status_bob, body_bob, ERROR_INSUFFICIENT_PERMISSIONS,
            context="bob without consume role",
        )
        self.log.info("Bob consume: correctly DENIED (403 INSUFFICIENT_PERMISSIONS)")

        self.log.info("Pattern A consume path and RBAC enforcement verified")

    def test_guardrails_enforcement_pattern_a(self):
        """
        Validate both allow and deny branches of service guardrails in the end-user path.

        Covers:
        - Create credential with allowedServices=["n1ql"], verify guardrails stored
        - Grant alice consume role, verify role persisted
        - Consume as @cbq-engine on behalf of alice → 200 + plaintext secret
        - PUT update allowedServices to ["backup"], verify guardrails updated via GET
        - Consume again → 403 SERVICE_GUARDRAIL_BLOCKED

        Requires @cbq-engine cbauth password (same as S2).  Fails if unresolved.
        """
        self.log.info("Testing guardrails enforcement (Pattern A)")

        self._require_cbq_password()

        cred_id = "p0-s3-c1"
        known_secret = "S3_SECRET_VALUE"
        initial_payload = self.cs_utils.build_aws_payload(
            access_key_id="AKIAS3EXAMPLE",
            secret_access_key=known_secret,
            region="eu-west-1",
            allowed_services=["n1ql"],
            description="P0 S3 guardrails test credential",
        )

        # Create credential — expect 201
        self.log.info(f"Creating {cred_id} with allowedServices=[n1ql]")
        status_create, content_create = self._create_tracked_credential(cred_id, initial_payload)
        self.assertEqual(
            int(status_create) if status_create else 0, 201,
            f"Create expected 201, got {status_create}. content={content_create}",
        )

        # Verify guardrails stored correctly after create
        guardrails_initial = self._get_credential_guardrails(cred_id)
        self.assertEqual(
            guardrails_initial.get("allowedServices"), ["n1ql"],
            f"allowedServices should be ['n1ql'] after create. guardrails={guardrails_initial}",
        )
        self.log.info(f"Guardrails after create: {guardrails_initial}")

        # Grant alice consume role — expect 200, verify persisted
        self.log.info(f"Granting alice credential_consumer[{cred_id}]")
        status_grant, _ = self.cs_utils.grant_consume_to_local_user(
            self.rest, self.ALICE_USER, cred_id
        )
        self.assertEqual(
            int(status_grant) if status_grant else 0, 200,
            f"Grant consume role to alice expected 200, got {status_grant}",
        )
        self._verify_user_has_consume_role(self.ALICE_USER, cred_id)
        self.log.info("Alice consume role granted and verified")

        # Consume with allowedServices=[n1ql] — expect 200 + plaintext secret
        self.log.info("Consume with allowedServices=[n1ql] (expect 200)")
        status_allow, body_allow = self._consume_as_cbq_on_behalf_of(
            cred_id, self.ALICE_USER, "local"
        )
        self._assert_consume_allowed(status_allow, body_allow, context="guardrail allows n1ql")
        self._assert_consume_has_secret(body_allow, cred_id, known_secret)
        self.log.info("Guardrail ALLOW: @cbq-engine consume returned 200, plaintext secret verified")

        # Update guardrails to exclude n1ql — expect 200
        self.log.info(f"Updating {cred_id} allowedServices to [backup]")
        blocked_payload = self.cs_utils.build_aws_payload(
            access_key_id="AKIAS3EXAMPLE",
            secret_access_key=known_secret,
            region="eu-west-1",
            allowed_services=["backup"],
            description="P0 S3 guardrails test credential — n1ql blocked",
        )
        status_update, content_update = self.cs_utils.update_credential(
            self.rest, cred_id, blocked_payload
        )
        self.assertEqual(
            int(status_update) if status_update else 0, 200,
            f"PUT to update allowedServices expected 200, got {status_update}. "
            f"content={content_update}",
        )

        # Verify guardrails updated via GET before consuming
        guardrails_updated = self._get_credential_guardrails(cred_id)
        self.assertEqual(
            guardrails_updated.get("allowedServices"), ["backup"],
            f"allowedServices should be ['backup'] after PUT. guardrails={guardrails_updated}",
        )
        self.log.info(f"Guardrails after update: {guardrails_updated}")

        # Consume after guardrail update — expect 403 SERVICE_GUARDRAIL_BLOCKED
        self.log.info("Consume after allowedServices=[backup] (expect 403)")
        status_blocked, body_blocked = self._consume_as_cbq_on_behalf_of(
            cred_id, self.ALICE_USER, "local"
        )
        self._assert_consume_denied(
            status_blocked, body_blocked, ERROR_SERVICE_GUARDRAIL_BLOCKED,
            context="guardrail blocks n1ql after update to backup-only",
        )
        self.log.info(
            "Guardrail DENY: @cbq-engine correctly blocked "
            "(403 SERVICE_GUARDRAIL_BLOCKED after allowedServices=[backup])"
        )

        self.log.info("Guardrail allow and deny paths verified")

    def test_pattern_b_service_identity(self):
        """
        Validate service-identity path semantics.

        Flow (ordered to avoid grant/delete fragility):
        1. Create credential with allowedServices=["backup"], verify guardrails
        2. sec_admin PUT service roles → 403 (RBAC enforcement)
        3. Consume as @cbq-engine service identity with no role yet → 403
        4. Full Admin grant n1ql credential_consumer[cred_id] → 200
        5. Consume as @cbq-engine service identity → 200 + plaintext secret
           (guardrail [backup-only] bypassed on service-identity path)
        6. Identity mismatch: @cbq-engine on behalf of @backup/admin → 403

        Requires @cbq-engine cbauth password.  Fails if unresolved.
        """
        self.log.info("Testing Pattern B service identity")

        self._require_cbq_password()

        cred_id = "p0-s4-c1"
        known_secret = "S4_SECRET_VALUE"
        payload = self.cs_utils.build_aws_payload(
            access_key_id="AKIAS4EXAMPLE",
            secret_access_key=known_secret,
            region="ap-southeast-1",
            allowed_services=["backup"],
            description="P0 S4 Pattern-B test credential",
        )

        # Create credential — expect 201
        self.log.info(f"Creating {cred_id} with allowedServices=[backup] (excludes n1ql)")
        status_create, content_create = self._create_tracked_credential(cred_id, payload)
        self.assertEqual(
            int(status_create) if status_create else 0, 201,
            f"Create expected 201, got {status_create}. content={content_create}",
        )

        # Verify guardrails stored: allowedServices=["backup"]
        guardrails = self._get_credential_guardrails(cred_id)
        self.assertEqual(
            guardrails.get("allowedServices"), ["backup"],
            f"allowedServices should be ['backup'] after create. guardrails={guardrails}",
        )
        self.log.info(f"Guardrails after create: {guardrails} — n1ql excluded, bypass test is meaningful")

        # RBAC check: sec_admin cannot manage service roles — expect 403
        self.log.info("sec_admin PUT /settings/rbac/services/n1ql/roles (expect 403)")
        status_sec_admin_put, _ = self.cs_utils.put_service_roles(
            self.rest,
            "n1ql",
            [f"credential_consumer[{cred_id}]"],
            username=self.SEC_ADMIN_USER,
            password=self.SEC_ADMIN_PASS,
        )
        self.assertEqual(
            int(status_sec_admin_put) if status_sec_admin_put else 0, 403,
            f"security_admin should NOT manage service roles, got {status_sec_admin_put}",
        )
        self.log.info(f"sec_admin service role PUT correctly denied: status={status_sec_admin_put}")

        # Consume as service identity with no role yet — expect 403
        self.log.info("Consuming as @cbq-engine (no role) — expect 403")
        status_no_role, body_no_role = self._consume_as_cbq_service_identity(cred_id)
        self._assert_consume_denied(
            status_no_role, body_no_role, ERROR_INSUFFICIENT_PERMISSIONS,
            context="service identity without credential_consumer role",
        )
        self.log.info("Service identity without role correctly denied (403 INSUFFICIENT_PERMISSIONS)")

        # Full Admin grant n1ql service credential_consumer[cred_id] — expect 200
        self.log.info(f"Full Admin granting n1ql service credential_consumer[{cred_id}]")
        status_grant, _ = self._grant_service_roles("n1ql", [f"credential_consumer[{cred_id}]"])
        self.assertEqual(
            int(status_grant) if status_grant else 0, 200,
            f"Full Admin service role grant expected 200, got {status_grant}",
        )
        self.log.info(f"Service role granted: status={status_grant}")

        # Consume as service identity with role — expect 200 + plaintext secret
        # guardrail allowedServices=["backup"] must be bypassed on Pattern B path
        self.log.info(
            "Consuming as @cbq-engine (service identity, role granted) — expect 200. "
            "guardrail [backup-only] must be bypassed."
        )
        status_with_role, body_with_role = self._consume_as_cbq_service_identity(cred_id)
        self._assert_consume_allowed(
            status_with_role, body_with_role,
            context="service identity with role, guardrail bypassed",
        )
        self._assert_consume_has_secret(body_with_role, cred_id, known_secret)
        self.log.info("Service identity consume: ALLOWED (200), guardrail bypass + plaintext secret confirmed")

        # Identity mismatch: @cbq-engine authenticates but claims @backup identity — expect 403
        self.log.info("Identity mismatch — @cbq-engine on behalf of @backup/admin (expect 403)")
        status_mismatch, body_mismatch = self._consume_as_cbq_on_behalf_of(
            cred_id, "@backup", "admin"
        )
        self._assert_consume_denied(
            status_mismatch, body_mismatch, ERROR_INSUFFICIENT_PERMISSIONS,
            context="@cbq-engine claiming @backup identity",
        )
        mismatch_code = self.cs_utils.get_cbauth_error_code(body_mismatch)
        self.log.info(
            f"Identity mismatch correctly rejected: status={status_mismatch} "
            f"code={mismatch_code} body={body_mismatch}"
        )

        self.log.info("Pattern B service identity path and RBAC enforcement verified")

    def test_prerequisites_override_and_expiry(self):
        """
        Validate security prerequisite enforcement, store override settings,
        warnings, and credential expiry.

        Covers:
        - GET store settings → 200
        - PUT both overrides=false (strict) → 200
        - Credential creation with strict settings:
            - If n2n is OFF on the cluster: create fails → assertEqual 400
            - If n2n is ON (cluster has it enabled): create succeeds; strict-block
              sub-assertion is skipped (logged).  Override + warning paths still run.
        - PUT n2nEncryptionOverride=true → 200, GET confirms setting persisted
        - Credential creation after override → 201
        - GET /settings/credentialStore and GET /settings/credentials both return
          non-empty warnings while override is active
        - DELETE /settings/credentialStore → 200, GET confirms both overrides reset to false
        - expiresAt < 5 min → 400
        - expiresAt >= 5 min (6 min) → 201
        - [Slow path] test_expiry_wait=True: consume before/after expiry (nightly only)
        """
        self.log.info("Testing prerequisites, override settings, and expiry")

        test_expiry_wait = self.input.param("test_expiry_wait", False)

        # GET current store settings — expect 200
        status_settings, content_settings = self.cs_utils.get_store_settings(self.rest)
        self.assertEqual(
            int(status_settings) if status_settings else 0, 200,
            f"GET /settings/credentialStore expected 200, got {status_settings}",
        )
        self.log.info(f"Current store settings: {content_settings}")

        # PUT strict settings (both overrides false) — expect 200
        self.log.info("Setting both overrides to false (strict mode)")
        status_strict, content_strict = self._set_strict_store_settings()
        self.assertEqual(
            int(status_strict) if status_strict else 0, 200,
            f"PUT strict settings expected 200, got {status_strict}. content={content_strict}",
        )

        # Attempt credential creation with strict settings
        cred_strict = "p0-s5-strict-c1"
        strict_payload = self.cs_utils.build_aws_payload(
            access_key_id="AKIAS5STRICT",
            secret_access_key="STRICT_SECRET",
            region="us-west-2",
        )
        status_strict_create, content_strict_create = self.cs_utils.create_credential(
            self.rest, cred_strict, strict_payload
        )
        cluster_has_n2n = self.cs_utils._is_success_status(status_strict_create)
        if cluster_has_n2n:
            # n2n is enabled on this cluster — strict block is implicitly satisfied.
            # Warnings will NOT appear later because the cluster is already secure.
            self._created_creds.append(cred_strict)
            self.log.info(
                f"[STRICT-BRANCH: n2n=ON] Strict create succeeded (status={status_strict_create}). "
                "Prerequisite enforcement implicitly satisfied; override PUT/GET still run."
            )
        else:
            # n2n is OFF — prerequisite must have returned 400.
            # Warnings WILL appear after override because encryption gap is real.
            self.assertEqual(
                int(status_strict_create) if status_strict_create else 0, 400,
                f"Strict create should return 400 when n2n is off. "
                f"Got {status_strict_create}. content={content_strict_create}",
            )
            self.log.info(
                f"[STRICT-BRANCH: n2n=OFF] Prerequisite enforced: create blocked "
                f"(status={status_strict_create}). content={content_strict_create}"
            )

        # PUT n2nEncryptionOverride=true — expect 200
        self.log.info("Enabling n2nEncryptionOverride=true")
        status_override, content_override = self.cs_utils.put_store_settings(
            self.rest,
            config_encryption_override=False,
            n2n_encryption_override=True,
        )
        self.assertEqual(
            int(status_override) if status_override else 0, 200,
            f"PUT n2nEncryptionOverride=true expected 200, got {status_override}",
        )

        # GET store settings and assert n2nEncryptionOverride=true persisted
        _, content_override_get = self.cs_utils.get_store_settings(self.rest)
        parsed_override = self.cs_utils.parse_content(content_override_get)
        if isinstance(parsed_override, dict):
            self.assertTrue(
                parsed_override.get("n2nEncryptionOverride"),
                f"n2nEncryptionOverride should be true after PUT. settings={parsed_override}",
            )
        self.log.info(f"Override enabled and confirmed: n2nEncryptionOverride=true")

        # Create credential after override — expect 201
        cred_after_override = "p0-s5-c1"
        override_payload = self.cs_utils.build_aws_payload(
            access_key_id="AKIAS5OVERRIDE",
            secret_access_key="OVERRIDE_SECRET",
            region="us-west-2",
            description="P0 S5 override test credential",
        )
        status_after_override, content_after_override = self._create_tracked_credential(
            cred_after_override, override_payload
        )
        self.assertEqual(
            int(status_after_override) if status_after_override else 0, 201,
            f"Create after override expected 201, got {status_after_override}. "
            f"content={content_after_override}",
        )
        self.log.info(f"Credential created after override: status={status_after_override}")

        # Warnings appear only when credentials exist AND a real encryption gap is
        # present.  When n2n is ON (cluster_has_n2n=True), the override is redundant
        # and warnings must be empty.  When n2n is OFF, the override masks a real gap
        # and warnings must be non-empty.
        self.log.info("Verifying warnings in GET /settings/credentialStore")
        status_warn_store, content_warn_store = self.cs_utils.get_store_settings(self.rest)
        self.assertEqual(int(status_warn_store) if status_warn_store else 0, 200,
                         "GET /settings/credentialStore expected 200")
        warnings_store = self.cs_utils.get_warnings(content_warn_store)
        if cluster_has_n2n:
            self.assertEqual(
                warnings_store, [],
                f"[WARNINGS-BRANCH: n2n=ON] Expected empty warnings when n2n is fully "
                f"enabled. Got: {warnings_store}",
            )
        else:
            self.assertTrue(
                len(warnings_store) > 0,
                f"[WARNINGS-BRANCH: n2n=OFF] Expected non-empty warnings when n2n is "
                f"off and override is active. Got: {warnings_store}",
            )
        self.log.info(
            f"[WARNINGS-BRANCH: n2n={'ON' if cluster_has_n2n else 'OFF'}] "
            f"Store settings warnings: {warnings_store}"
        )

        self.log.info("Verifying warnings in GET /settings/credentials")
        status_warn_creds, content_warn_creds = self.cs_utils.list_credentials(self.rest)
        self.assertEqual(int(status_warn_creds) if status_warn_creds else 0, 200,
                         "GET /settings/credentials expected 200")
        warnings_creds = self.cs_utils.get_warnings(content_warn_creds)
        if cluster_has_n2n:
            self.assertEqual(
                warnings_creds, [],
                f"[WARNINGS-BRANCH: n2n=ON] Expected empty warnings when n2n is fully "
                f"enabled. Got: {warnings_creds}",
            )
        else:
            self.assertTrue(
                len(warnings_creds) > 0,
                f"[WARNINGS-BRANCH: n2n=OFF] Expected non-empty warnings when n2n is "
                f"off and override is active. Got: {warnings_creds}",
            )
        self.log.info(
            f"[WARNINGS-BRANCH: n2n={'ON' if cluster_has_n2n else 'OFF'}] "
            f"Credentials list warnings: {warnings_creds}"
        )

        # DELETE /settings/credentialStore — expect 200, then GET confirms defaults
        self.log.info("Resetting store settings via DELETE")
        status_reset, content_reset = self.cs_utils.delete_store_settings(self.rest)
        self.assertEqual(
            int(status_reset) if status_reset else 0, 200,
            f"DELETE /settings/credentialStore expected 200, got {status_reset}. "
            f"content={content_reset}",
        )
        _, content_after_reset = self.cs_utils.get_store_settings(self.rest)
        parsed_reset = self.cs_utils.parse_content(content_after_reset)
        if isinstance(parsed_reset, dict):
            self.assertFalse(
                parsed_reset.get("configEncryptionOverride", False),
                "configEncryptionOverride should be false after DELETE reset",
            )
            self.assertFalse(
                parsed_reset.get("n2nEncryptionOverride", False),
                "n2nEncryptionOverride should be false after DELETE reset",
            )
            self.log.info("Store settings correctly reset to defaults")

        # Re-enable override so expiry tests can create credentials
        status_re_enable, content_re_enable = self.cs_utils.put_store_settings(
            self.rest, config_encryption_override=False, n2n_encryption_override=True
        )
        if not self.cs_utils._is_success_status(status_re_enable):
            self.log.warning(
                f"Re-enabling n2nEncryptionOverride failed (status={status_re_enable}). "
                "Expiry sub-tests may fail due to missing prerequisite. "
                f"content={content_re_enable}"
            )

        # expiresAt < 5 min — must be rejected with 400 (PRD minimum)
        self.log.info("Testing expiresAt=60s (expect 400 — below 5-min minimum)")
        too_soon_ms = self.cs_utils.expires_at_ms(60)
        status_soon, content_soon = self.cs_utils.create_credential(
            self.rest,
            "p0-s5-expires-soon",
            self.cs_utils.build_aws_payload(
                access_key_id="AKIAS5SOON",
                secret_access_key="SOON_SECRET",
                region="us-east-1",
                expires_at_ms=too_soon_ms,
            ),
        )
        self.assertEqual(
            int(status_soon) if status_soon else 0, 400,
            f"expiresAt=60s should return 400, got {status_soon}. content={content_soon}",
        )
        self.log.info(f"Short expiresAt correctly rejected: status={status_soon}")

        # expiresAt = 360s (6 min) — must succeed with 201.
        # allowed_services=["n1ql"] required: subtest uses Pattern A consume via @cbq-engine.
        self.log.info("Testing expiresAt=360s (expect 201)")
        valid_expires_ms = self.cs_utils.expires_at_ms(360)  # 6 min
        cred_expiring = "p0-s5-expiring-c1"
        status_valid_expiry, content_valid_expiry = self._create_tracked_credential(
            cred_expiring,
            self.cs_utils.build_aws_payload(
                access_key_id="AKIAS5EXPIRY",
                secret_access_key="EXPIRY_SECRET",
                region="us-east-1",
                expires_at_ms=valid_expires_ms,
                allowed_services=["n1ql"],
                description="P0 S5 expiry test credential",
            ),
        )
        self.assertEqual(
            int(status_valid_expiry) if status_valid_expiry else 0, 201,
            f"expiresAt=360s should return 201, got {status_valid_expiry}. "
            f"content={content_valid_expiry}",
        )
        self.log.info(f"Expiring credential created: status={status_valid_expiry}")

        # Expiry enforcement (slow path — gated on test_expiry_wait)
        if not test_expiry_wait:
            self.log.info(
                "Expiry enforcement wait skipped (test_expiry_wait=False). "
                "Add test_expiry_wait=True to conf for nightly runs (~7 min wait)."
            )
        else:
            self._run_expiry_enforcement_subtest(cred_expiring, "EXPIRY_SECRET")

        self.log.info("Prerequisites, override settings, warnings, and expiry verified")

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

        # Grant alice consume role
        self.log.info(f"Granting alice credential_consumer[{cred_id}] for expiry test")
        status_grant, _ = self.cs_utils.grant_consume_to_local_user(
            self.rest, self.ALICE_USER, cred_id
        )
        self.assertEqual(
            int(status_grant) if status_grant else 0, 200,
            f"Grant consume role for expiry test expected 200, got {status_grant}",
        )

        # Read expiresAt from credential — hard assert, no silent fallback
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

        # Checkpoint 1: consume immediately — expect 200 (well before expiry)
        self.log.info("Checkpoint 1: consume immediately after grant (expect 200)")
        status_c1, body_c1 = self._consume_as_cbq_on_behalf_of(
            cred_id, self.ALICE_USER, "local"
        )
        self._assert_consume_allowed(status_c1, body_c1, context="checkpoint 1 — well before expiry")
        self._assert_consume_has_secret(body_c1, cred_id, known_secret)
        self.log.info("Checkpoint 1 PASSED: credential valid, plaintext secret returned")

        # Wait until 2 seconds before expiresAt (boundary)
        wait_to_boundary = max(0, expires_at_s - 2 - int(time.time()))
        if wait_to_boundary > 0:
            self.log.info(f"Waiting {wait_to_boundary}s to reach 2s-before-expiry boundary...")
            self.sleep(wait_to_boundary, "Waiting to reach pre-expiry boundary")

        # Checkpoint 2: consume at t = expiresAt − 2s — credential not yet expired → 200
        self.log.info("Checkpoint 2: consume at t=expiresAt−2s boundary (expect 200)")
        status_c2, body_c2 = self._consume_as_cbq_on_behalf_of(
            cred_id, self.ALICE_USER, "local"
        )
        self._assert_consume_allowed(status_c2, body_c2, context="checkpoint 2 — 2s before expiry")
        self._assert_consume_has_secret(body_c2, cred_id, known_secret)
        self.log.info("Checkpoint 2 PASSED: credential still valid 2s before expiry")

        # Wait past expiry: 2s remaining + 6s buffer = 8s
        self.log.info("Waiting 8s to pass expiry + buffer...")
        self.sleep(8, "Waiting past expiry boundary")

        # Checkpoint 3: consume at t = expiresAt + 6s — must be expired → 403
        self.log.info("Checkpoint 3: consume at t=expiresAt+6s (expect 403 CREDENTIAL_EXPIRED)")
        status_c3, body_c3 = self._consume_as_cbq_on_behalf_of(
            cred_id, self.ALICE_USER, "local"
        )
        self._assert_consume_denied(
            status_c3, body_c3, ERROR_CREDENTIAL_EXPIRED,
            context="checkpoint 3 — 6s after expiry",
        )
        self.log.info(
            f"Checkpoint 3 PASSED: expired credential rejected "
            f"(status={status_c3} error={ERROR_CREDENTIAL_EXPIRED})"
        )
