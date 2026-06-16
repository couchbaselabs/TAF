from membase.api.rest_client import RestConnection

from couchbase_utils.security_utils.credential_store_utils import (
    ERROR_CREDENTIAL_EXPIRED,
    ERROR_INSUFFICIENT_PERMISSIONS,
    ERROR_SERVICE_GUARDRAIL_BLOCKED,
)
from pytests.ns_server.credential_store_base import CredentialStoreBase

# ── T08: Credential type smoke ──────────────────────────────────────────────
# Each row: (cred_type, fields_dict, known_secret_or_None, non_sensitive_fields)
# known_secret_or_None: value expected to be masked in GET; None when the type
#   has zero sensitive fields (azureManaged).
# non_sensitive_fields: {field: expected_value} pairs that must survive round-trip
#   unchanged (not "********" or absent).  {} when all fields are sensitive.
_TYPE_SMOKE_ROWS = (
    ("aws",
     {"accessKeyId": "AKIASMOKE001", "secretAccessKey": "T08_AWS_SECRET", "region": "us-east-1"},
     "T08_AWS_SECRET",
     {"accessKeyId": "AKIASMOKE001", "region": "us-east-1"}),
    ("azureShared",
     {"accountName": "smokeaccount", "accountKey": "T08_AZURE_SHARED_KEY=="},
     "T08_AZURE_SHARED_KEY==",
     {"accountName": "smokeaccount"}),
    ("azureAd",
     {"tenantId": "smoke-tenant-id", "clientId": "smoke-client-id", "clientSecret": "T08_AZURE_AD_SECRET"},
     "T08_AZURE_AD_SECRET",
     {"tenantId": "smoke-tenant-id", "clientId": "smoke-client-id"}),
    ("azureSas",
     {"accountName": "smokeaccount", "sharedAccessSignature": "T08_SAS_TOKEN?sv=2020-08-04&sig=smoke"},
     "T08_SAS_TOKEN?sv=2020-08-04&sig=smoke",
     {"accountName": "smokeaccount"}),
    ("azureManaged",
     {"managedIdentityId": "22222222-2222-2222-2222-222222222222"},
     None,
     {"managedIdentityId": "22222222-2222-2222-2222-222222222222"}),
    ("gcp",
     {"jsonCredentials": '{"type":"service_account","project_id":"smoke-project"}'},
     '{"type":"service_account","project_id":"smoke-project"}',
     {}),
    ("http",
     {"authScheme": "bearer", "token": "T08_HTTP_BEARER_TOKEN", "headerName": "Authorization"},
     "T08_HTTP_BEARER_TOKEN",
     {"authScheme": "bearer", "headerName": "Authorization"}),
    ("couchbase",
     {"encryptionType": "none", "username": "smokeuser", "password": "T08_CB_PASSWORD"},
     "T08_CB_PASSWORD",
     {"encryptionType": "none", "username": "smokeuser"}),
)

# ── T11: Consume auth matrix ────────────────────────────────────────────────
# Fields: id, pattern (A|B), caller (service without @), user (on_behalf),
# domain, guardrail, setup, exp_status, exp_error,
# use_missing_id (optional bool), requires_expiry (optional bool)
_CONSUME_AUTH_MATRIX = (
    # — Pattern A: @cbq-engine on behalf of end user ——————————————————————
    {"id": "A1", "pattern": "A", "caller": "cbq-engine", "user": "cs_alice", "domain": "local",
     "guardrail": ["n1ql"],   "setup": "alice_role",      "exp_status": 200, "exp_error": None},
    {"id": "A2", "pattern": "A", "caller": "cbq-engine", "user": "cs_bob",   "domain": "local",
     "guardrail": ["n1ql"],   "setup": "alice_role",      "exp_status": 403, "exp_error": ERROR_INSUFFICIENT_PERMISSIONS},
    {"id": "A3", "pattern": "A", "caller": "backup",      "user": "cs_alice", "domain": "local",
     "guardrail": ["n1ql"],   "setup": "alice_role",      "exp_status": 403, "exp_error": ERROR_SERVICE_GUARDRAIL_BLOCKED},
    {"id": "A4", "pattern": "A", "caller": "cbq-engine", "user": "cs_alice", "domain": "local",
     "guardrail": ["backup"],  "setup": "alice_role",      "exp_status": 403, "exp_error": ERROR_SERVICE_GUARDRAIL_BLOCKED},
    {"id": "A5", "pattern": "A", "caller": "cbq-engine", "user": "cs_alice", "domain": "local",
     "guardrail": ["n1ql"],   "setup": "alice_role",      "exp_status": 403, "exp_error": ERROR_INSUFFICIENT_PERMISSIONS,
     "use_missing_id": True},
    {"id": "A6", "pattern": "A", "caller": "cbq-engine", "user": "cs_alice", "domain": "local",
     "guardrail": ["n1ql"],   "setup": "alice_role",      "exp_status": 403, "exp_error": ERROR_CREDENTIAL_EXPIRED,
     "requires_expiry": True},
    # — Pattern B: service-identity path ——————————————————————————————————
    {"id": "B1", "pattern": "B", "caller": "cbq-engine", "user": "@cbq-engine", "domain": "admin",
     "guardrail": ["backup"], "setup": "no_service_role", "exp_status": 403, "exp_error": ERROR_INSUFFICIENT_PERMISSIONS},
    {"id": "B2", "pattern": "B", "caller": "cbq-engine", "user": "@cbq-engine", "domain": "admin",
     "guardrail": ["backup"], "setup": "n1ql_service_role", "exp_status": 200, "exp_error": None},
    {"id": "B3", "pattern": "B", "caller": "cbq-engine", "user": "@backup",     "domain": "admin",
     "guardrail": ["backup"], "setup": "n1ql_service_role", "exp_status": 403, "exp_error": ERROR_INSUFFICIENT_PERMISSIONS},
    {"id": "B4", "pattern": "B", "caller": "backup",      "user": "@backup",     "domain": "admin",
     "guardrail": ["n1ql"],   "setup": "backup_service_role", "exp_status": 200, "exp_error": None},
    # B5: @cbcontbk is canonicalized to @backup by misc:canonical_admin_identity/1,
    #     so it inherits credential_consumer grants made to the backup service.
    {"id": "B5", "pattern": "B", "caller": "cbcontbk",    "user": "@cbcontbk",   "domain": "admin",
     "guardrail": ["n1ql"],   "setup": "backup_service_role", "exp_status": 200, "exp_error": None},
    {"id": "B6", "pattern": "B", "caller": "backup",      "user": "@backup",     "domain": "admin",
     "guardrail": ["n1ql"],   "setup": "no_service_role", "exp_status": 403, "exp_error": ERROR_INSUFFICIENT_PERMISSIONS,
     "use_missing_id": True},
)


# ── T12: RBAC grant matrix ──────────────────────────────────────────────────
# Each row: actor calls an API and the result is asserted.
# The credential "p1-rbac-g1" is created once at the start of the test.
# "verify" keys:
#   "user_role"    → GET user and confirm credential_consumer[cred_id] role present
#   "service_role" → GET service and confirm credential_consumer[cred_id] role present
# "requires_cbq_password": True → row is skipped when cbq_engine_password is unavailable.
_GRANT_MATRIX = (
    # G1: security_admin cannot grant credential_consumer to a user
    {"id": "G1", "actor": "cs_sec_admin",  "api": "user_role",
     "target": "cs_alice", "role": "credential_consumer[{cred_id}]",
     "exp_status": 403, "verify": None},
    # G2: security_admin cannot assign credential_consumer to a service
    {"id": "G2", "actor": "cs_sec_admin",  "api": "service_role",
     "target": "n1ql",     "role": "credential_consumer[{cred_id}]",
     "exp_status": 403, "verify": None},
    # G3: user_admin can grant credential_consumer to a user; role must persist
    {"id": "G3", "actor": "cs_user_admin", "api": "user_role",
     "target": "cs_alice", "role": "credential_consumer[{cred_id}]",
     "exp_status": 200, "verify": "user_role"},
    # G4: user_admin cannot create credentials (credential_admin / full_admin only)
    {"id": "G4", "actor": "cs_user_admin", "api": "credential_create",
     "target": "p1-rbac-g2", "role": None,
     "exp_status": 403, "verify": None},
    # G5: user_admin cannot assign credential_consumer to a service
    {"id": "G5", "actor": "cs_user_admin", "api": "service_role",
     "target": "n1ql",     "role": "credential_consumer[*]",
     "exp_status": 403, "verify": None},
    # G6: @cbq-engine service identity cannot assign service roles
    {"id": "G6", "actor": "cbq_engine",    "api": "service_role",
     "target": "n1ql",     "role": "credential_consumer[*]",
     "exp_status": 403, "verify": None, "requires_cbq_password": True},
    # G7: @cbq-engine service identity cannot delete service roles
    {"id": "G7", "actor": "cbq_engine",    "api": "service_role_delete",
     "target": "n1ql",     "role": None,
     "exp_status": 403, "verify": None, "requires_cbq_password": True},
    # G8: Full Admin can assign credential_consumer to a service; role must persist
    {"id": "G8", "actor": "Administrator", "api": "service_role",
     "target": "backup",   "role": "credential_consumer[{cred_id}]",
     "exp_status": 200, "verify": "service_role"},
    # G9: user_admin grant with a non-existent credential ID → 400 (server validates existence at grant time)
    {"id": "G9", "actor": "cs_user_admin", "api": "user_role",
     "target": "cs_alice", "role": "credential_consumer[ghost-id]",
     "exp_status": 400, "verify": None},
    # G10: user_admin grant where no credential matches the prefix → 400 (same existence check as G9)
    # credential_consumer[no/prefix/*] is syntactically valid; rejected because no credential
    # exists with that prefix, not because of a format error.
    {"id": "G10", "actor": "cs_user_admin", "api": "user_role",
     "target": "cs_alice",  "role": "credential_consumer[no/prefix/*]",
     "exp_status": 400, "verify": None},
)

# ── T14: Credential ID validation matrix ────────────────────────────────────
# Fields: (cred_id, expected_status, description)
_ID_MATRIX = [
    ("a" * 128, 201, "Exact maximum length (128 chars)"),
    ("b" * 129, 400, "Exceeds maximum length (129 chars)"),
    ("my key", 400, "Contains invalid character: space"),
    ("keyñ", 400, "Contains invalid character: non-ASCII"),
    ("valid/folder/key", 201, "Valid hierarchical ID with slashes"),
]


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

    def test_cross_node_credential_consume(self):
        """S6: Chronicle replication — create/update on Node A, Pattern A consume on Node B (200 then 403)."""
        self.log.info("Testing cross-node credential consume (Chronicle replication)")

        self._require_cbq_password()

        # Select Node B by IP — avoids false pass when servers[1] is also the master
        node_b = self._non_master_node()
        node_b_url = self._node_base_url(node_b)
        node_b_rest = RestConnection(node_b)
        node_a_label = getattr(self.cluster.master, "ip", "node-A")
        node_b_label = getattr(node_b, "ip", "node-B")
        self.log.info(
            f"Node A (create/update): {node_a_label}  "
            f"Node B (consume/poll): {node_b_label} -> {node_b_url}"
        )

        # The cbauth special password is per-node — Node B has a different token
        # from the one fetched at setUp (which came from the master/Node A).
        # Fetch Node B's own token before calling its /_cbauth/ endpoint.
        node_b_cbq_password = self._fetch_cbq_engine_password_via_diag_eval(rest=node_b_rest)
        if not node_b_cbq_password:
            self.fail(
                f"Could not resolve @cbq-engine cbauth password for Node B ({node_b_label}). "
                "Ensure diag/eval is enabled and n1ql is running on Node B "
                "(services_init=kv:n1ql required for both nodes)."
            )
        self.log.info(f"[Node B: {node_b_label}] @cbq-engine password resolved via diag/eval")

        cred_id = "p0-s6-c1"
        known_secret = "S6_CROSS_NODE_SECRET"

        # Phase 1: CREATE propagation

        payload = self.cs_utils.build_aws_payload(
            access_key_id="AKIAS6EXAMPLE",
            secret_access_key=known_secret,
            region="us-east-1",
            allowed_services=["n1ql"],
            description="P0 S6 cross-node consistency test",
        )

        self.log.info(f"[Node A] Creating credential {cred_id} with allowedServices=[n1ql]")
        status_create, content_create = self._create_tracked_credential(cred_id, payload)
        self.assertEqual(
            int(status_create) if status_create else 0, 201,
            f"[Node A] Create expected 201, got {status_create}. content={content_create}",
        )
        self.log.info(f"[Node A] Credential created: status={status_create}")

        # Grant alice consume role on Node A; RBAC replicates via Chronicle too
        self.log.info(f"[Node A] Granting alice credential_consumer[{cred_id}]")
        status_grant, _ = self.cs_utils.grant_consume_to_local_user(
            self.rest, self.ALICE_USER, cred_id
        )
        self.assertEqual(
            int(status_grant) if status_grant else 0, 200,
            f"[Node A] Grant consume role expected 200, got {status_grant}",
        )
        # Pre-check: confirm role persisted on Node A before relying on Node B consume
        self._verify_user_has_consume_role(self.ALICE_USER, cred_id)
        self.log.info(f"[Node A] alice credential_consumer[{cred_id}] verified")

        # Poll Node B admin GET until credential is visible (proves data replicated)
        self.log.info(
            f"[Node B: {node_b_label}] Polling GET credential until Chronicle replication lands (<=15s)"
        )
        self._poll_until(
            f"credential '{cred_id}' visible via admin GET on Node B ({node_b_label})",
            lambda: self.cs_utils._is_success_status(
                self.cs_utils.get_credential(node_b_rest, cred_id)[0]
            ),
            timeout_s=15,
        )
        self.log.info(
            f"[Node B: {node_b_label}] GET credential returned 200 — CREATE replicated via Chronicle"
        )

        # Consume on Node B after replication confirmed
        self.log.info(f"[Node B: {node_b_label}] Consuming credential (Pattern A) — expect 200")
        status_b1, body_b1 = self._consume_as_cbq_on_behalf_of_on_node(
            node_b_url, cred_id, self.ALICE_USER, "local",
            service_password=node_b_cbq_password,
        )
        self._assert_consume_allowed(
            status_b1, body_b1,
            context=f"Phase 1 CREATE propagation: Node A -> Node B ({node_b_label})",
        )
        self._assert_consume_has_secret(body_b1, cred_id, known_secret)
        self.log.info(
            f"[Phase 1 PASSED] Node B ({node_b_label}) consume returned 200 + plaintext secret"
        )

        # Phase 2: UPDATE propagation (guardrail metadata change)

        self.log.info(
            f"[Node A] Updating {cred_id} guardrail to allowedServices=[backup] "
            "(n1ql excluded — Node B consume must return 403 after propagation)"
        )
        blocked_payload = self.cs_utils.build_aws_payload(
            access_key_id="AKIAS6EXAMPLE",
            secret_access_key=known_secret,
            region="us-east-1",
            allowed_services=["backup"],
            description="P0 S6 guardrail update propagation",
        )
        status_update, content_update = self.cs_utils.update_credential(
            self.rest, cred_id, blocked_payload
        )
        self.assertEqual(
            int(status_update) if status_update else 0, 200,
            f"[Node A] Guardrail update expected 200, got {status_update}. "
            f"content={content_update}",
        )
        self.log.info(f"[Node A] Guardrail updated to allowedServices=[backup]")

        # Poll Node B admin GET until guardrail metadata reflects the update
        self.log.info(
            f"[Node B: {node_b_label}] Polling GET credential until allowedServices=[backup] appears (<=15s)"
        )

        def _guardrail_updated_on_b():
            _, content = self.cs_utils.get_credential(node_b_rest, cred_id)
            parsed = self.cs_utils.parse_content(content)
            if not isinstance(parsed, dict):
                return False
            guardrails = (
                parsed.get("meta", {}).get("guardrails", {})
                or parsed.get("guardrails", {})
            )
            return guardrails.get("allowedServices") == ["backup"]

        self._poll_until(
            f"guardrail update (allowedServices=[backup]) replicated to Node B ({node_b_label})",
            _guardrail_updated_on_b,
            timeout_s=15,
        )
        self.log.info(
            f"[Node B: {node_b_label}] GET shows allowedServices=[backup] — UPDATE replicated via Chronicle"
        )

        # Consume on Node B after guardrail update confirmed
        self.log.info(
            f"[Node B: {node_b_label}] Consuming after guardrail update — expect 403 SERVICE_GUARDRAIL_BLOCKED"
        )
        status_b2, body_b2 = self._consume_as_cbq_on_behalf_of_on_node(
            node_b_url, cred_id, self.ALICE_USER, "local",
            service_password=node_b_cbq_password,
        )
        self._assert_consume_denied(
            status_b2, body_b2, ERROR_SERVICE_GUARDRAIL_BLOCKED,
            context=f"Phase 2 UPDATE propagation: Node A -> Node B ({node_b_label})",
        )
        self.log.info(
            f"[Phase 2 PASSED] Node B ({node_b_label}) returned 403/SERVICE_GUARDRAIL_BLOCKED "
            "after guardrail update on Node A — Chronicle UPDATE replication confirmed"
        )

        self.log.info(
            "Cross-node Chronicle replication verified: "
            "CREATE and UPDATE (guardrail metadata) both propagated to Node B"
        )

    def test_node_failure_resilience(self):
        """S6.2: Node A killed via SSH; Node B must serve GET credential 200 from local Chronicle replica."""
        self.log.info("Testing node failure resilience (Chronicle local replica)")

        self.assertGreaterEqual(
            len(self.cluster.servers), 2,
            "test_node_failure_resilience requires nodes_init>=2 but only "
            f"{len(self.cluster.servers)} server(s) configured in node.ini.",
        )

        node_a = self.cluster.master
        node_b = self._non_master_node()
        node_b_rest = RestConnection(node_b)
        node_a_label = getattr(node_a, "ip", "node-A")
        node_b_label = getattr(node_b, "ip", "node-B")
        self.log.info(
            f"Node A (create + kill): {node_a_label}  "
            f"Node B (resilience read): {node_b_label}"
        )

        cred_id = "p1-s6-2-c1"
        known_secret = "S6_2_RESILIENCE_SECRET"

        payload = self.cs_utils.build_aws_payload(
            access_key_id="AKIAP162EXAMPLE",
            secret_access_key=known_secret,
            region="us-east-1",
            allowed_services=["n1ql"],
            description="P1 S6.2 node failure resilience test",
        )

        self.log.info(f"[Node A] Creating credential {cred_id}")
        status_create, content_create = self._create_tracked_credential(cred_id, payload)
        self.assertEqual(
            int(status_create) if status_create else 0, 201,
            f"[Node A] Create expected 201, got {status_create}. content={content_create}",
        )
        self.log.info(f"[Node A] Credential {cred_id} created")

        # Wait for Chronicle replication to Node B before killing Node A
        self.log.info(
            f"[Node B: {node_b_label}] Polling GET until Chronicle replication lands (<=15s)"
        )
        self._poll_until(
            f"credential '{cred_id}' visible via admin GET on Node B ({node_b_label})",
            lambda: self.cs_utils._is_success_status(
                self.cs_utils.get_credential(node_b_rest, cred_id)[0]
            ),
            timeout_s=15,
        )
        self.log.info(
            f"[Node B: {node_b_label}] GET credential 200 — replicated before Node A kill"
        )

        node_a_stopped = False
        try:
            # Kill Node A
            self._stop_couchbase_on_node(node_a)
            node_a_stopped = True

            # Confirm Node A's REST port is unreachable before asserting Node B
            self.log.info(
                f"[Node A: {node_a_label}] Polling until management port is unreachable (<=20s)"
            )
            self._poll_until(
                f"Node A ({node_a_label}) management port becomes unreachable after stop",
                lambda: not self._is_node_rest_reachable(node_a),
                timeout_s=20,
            )
            self.log.info(f"[Node A: {node_a_label}] Confirmed down")

            # Read credential from Node B — must return 200 from local Chronicle replica
            self.log.info(
                f"[Node B: {node_b_label}] GET credential while Node A is down — expect 200"
            )
            status_b, content_b = self.cs_utils.get_credential(node_b_rest, cred_id)
            self.assertTrue(
                self.cs_utils._is_success_status(status_b),
                f"[Node B: {node_b_label}] Expected 200 from local Chronicle replica "
                f"while Node A is down, got {status_b}. content={content_b}",
            )
            parsed = self.cs_utils.parse_content(content_b)
            self.assertIsInstance(
                parsed, dict,
                f"[Node B: {node_b_label}] GET body is not a JSON object: {content_b}",
            )
            # Credential ID must match exactly — drop name fallback to avoid masking wrong shape
            self.assertEqual(
                parsed.get("id"), cred_id,
                f"[Node B: {node_b_label}] Response credential ID mismatch. "
                f"expected={cred_id} body={parsed}",
            )
            # Type sanity check
            self.assertEqual(
                parsed.get("type"), "aws",
                f"[Node B: {node_b_label}] Expected type=aws, got {parsed.get('type')!r}. "
                f"body={parsed}",
            )
            # Secret must be redacted in admin GET response
            self.cs_utils.assert_secrets_redacted(
                content_b, "aws", known_secret_values=[known_secret]
            )
            self.log.info(
                f"[Node B: {node_b_label}] GET assertions passed: id={cred_id}, "
                "type=aws, secrets redacted"
            )

            # Informational: LIST while master is down
            # In a 2-node cluster the LIST endpoint may return [] because it
            # requires orchestrator quorum, while GET by ID reads from the local
            # Chronicle snapshot and succeeds.  Log observed behavior; do not
            # assert — the test plan only requires the GET path.
            status_list, content_list = self.cs_utils.list_credentials(node_b_rest)
            cred_ids_on_b = [
                c.get("id") for c in (self.cs_utils.parse_content(content_list) or [])
                if isinstance(c, dict)
            ]
            self.log.info(
                f"[Node B: {node_b_label}] LIST while Node A down: "
                f"status={status_list} cred_ids={cred_ids_on_b} "
                f"(empty list is expected in 2-node cluster — orchestrator quorum required for LIST)"
            )

            self.log.info(
                f"[PASSED] Node B ({node_b_label}) returned 200 + correct credential "
                "while Node A was offline — Chronicle local replica confirmed resilient"
            )

        finally:
            if node_a_stopped:
                self.log.info(f"[Node A: {node_a_label}] Restarting couchbase-server")
                self._start_couchbase_on_node(node_a, timeout_s=120)
                self.log.info(
                    f"[Node A: {node_a_label}] Back online — tearDown cleanup will proceed normally"
                )

    def test_credential_type_smoke(self):
        """POST → GET → assert type/id/redaction/non-sensitive round-trip for all 8 types."""
        self.log.info("Testing credential type smoke (8 types)")

        for cred_type, fields, known_secret, non_sensitive in _TYPE_SMOKE_ROWS:
            cred_id = f"p1-t08-{cred_type.lower()}"
            with self.subTest(type=cred_type):
                payload = {"type": cred_type, "fields": fields}
                status_post, content_post = self._create_tracked_credential(cred_id, payload)
                self.assertEqual(
                    int(status_post) if status_post else 0, 201,
                    f"[T08 {cred_type}] POST expected 201, got {status_post}. "
                    f"content={content_post}",
                )
                parsed_post = self.cs_utils.parse_content(content_post)
                if isinstance(parsed_post, dict):
                    self.assertEqual(
                        parsed_post.get("type"), cred_type,
                        f"[T08 {cred_type}] POST 201 body type mismatch. body={parsed_post}",
                    )

                status_get, content_get = self.cs_utils.get_credential(self.rest, cred_id)
                self.assertTrue(
                    self.cs_utils._is_success_status(status_get),
                    f"[T08 {cred_type}] GET expected 200, got {status_get}. "
                    f"content={content_get}",
                )
                parsed_get = self.cs_utils.parse_content(content_get)
                self.assertIsInstance(
                    parsed_get, dict,
                    f"[T08 {cred_type}] GET body should be a JSON object. content={content_get}",
                )
                self.assertEqual(
                    parsed_get.get("type"), cred_type,
                    f"[T08 {cred_type}] GET type mismatch. body={parsed_get}",
                )
                self.assertEqual(
                    parsed_get.get("id"), cred_id,
                    f"[T08 {cred_type}] GET id mismatch. body={parsed_get}",
                )

                known = [known_secret] if known_secret else []
                self.cs_utils.assert_secrets_redacted(
                    content_get, cred_type, known_secret_values=known
                )

                got_fields = parsed_get.get("fields", {})
                for field, expected in non_sensitive.items():
                    self.assertEqual(
                        got_fields.get(field), expected,
                        f"[T08 {cred_type}] Non-sensitive field '{field}' round-trip failed. "
                        f"expected={expected!r} got={got_fields.get(field)!r}",
                    )

                self.log.info(
                    f"[T08 {cred_type}] PASSED — POST 201, GET 200, "
                    "type/id/redaction/fields verified"
                )

    def test_rbac_grant_matrix(self):
        """
        T12: RBAC grant matrix — 10 rows covering who can grant credential_consumer
        to users vs services (maps to manual M1–M3 and QA PRD §9.5).

        A single credential (p1-rbac-g1) is created at the start.
        Each row calls a specific API as a specific actor and asserts the HTTP status.
        On 200, role persistence is verified via GET.

        Actors:
          cs_sec_admin  — security_admin role
          cs_user_admin — user_admin_local role
          cbq_engine    — @cbq-engine service identity (skipped if no cbauth password)
          Administrator — Full Admin (self.rest default credentials)

        G9 and G10 rely on the server validating credential-id existence and format
        at role-assignment time.  If the server uses lazy validation (returns 200),
        adjust the expected status to 200 and remove the "verify" key.
        """
        cred_id = "p1-rbac-g1"
        payload = self.cs_utils.build_aws_payload(
            access_key_id="AKIAG1EXAMPLE",
            secret_access_key="G1_GRANT_SECRET",
            region="us-east-1",
        )
        status_create, content_create = self._create_tracked_credential(cred_id, payload)
        self.assertEqual(
            int(status_create) if status_create else 0, 201,
            f"[T12] Setup: credential create expected 201, got {status_create}. "
            f"content={content_create}",
        )

        for row in _GRANT_MATRIX:
            row_id = row["id"]
            with self.subTest(row=row_id):
                if row.get("requires_cbq_password") and not self.cbq_engine_password:
                    self.skipTest(
                        f"[T12 {row_id}] requires cbq_engine_password — "
                        "use services_init=kv:n1ql to enable @cbq-engine rows"
                    )
                self._run_grant_row(row, cred_id)

    def test_wildcard_role_boundaries(self):
        """
        T13: Wildcard role boundary semantics for credential_consumer.

        Three phases:

        Phase 1 — suffix wildcard (db/*):
          Grant alice credential_consumer[db/*].
          Consume db/prod/1 → 200 — db/* covers this path.
          Consume db_test/1 → 403 INSUFFICIENT_PERMISSIONS —
            the slash is literal; db_test/1 does not share the "db/" prefix.

        Phase 2 — prefix asterisk rejected (*prod/key):
          Attempt to grant alice credential_consumer[*prod/key] → 400.
          Wildcard is suffix-only; *prod/key is treated as a literal credential ID
          lookup, and no such credential exists in the vault.

        Phase 3 — master key (*):
          Grant alice credential_consumer[*].
          Consume db/prod/1 → 200 and db_test/1 → 200.
          Standalone * matches all credential IDs, bypassing prefix checks.

        Requires cbq_engine_password — Pattern A consume calls @cbq-engine.
        """
        self._require_cbq_password()

        # ── Setup: create target (db/prod/1) and spoof (db_test/1) ──────────
        target_cred_id = "db/prod/1"
        target_secret = "T13_PROD_SECRET"
        status_t, content_t = self._create_tracked_credential(
            target_cred_id,
            self.cs_utils.build_aws_payload(
                access_key_id="AKIAT13PROD",
                secret_access_key=target_secret,
                region="us-east-1",
                allowed_services=["n1ql"],
            ),
        )
        self.assertEqual(
            int(status_t) if status_t else 0, 201,
            f"[T13] Setup db/prod/1: expected 201, got {status_t}. content={content_t}",
        )

        spoof_cred_id = "db_test/1"
        spoof_secret = "T13_SPOOF_SECRET"
        status_s, content_s = self._create_tracked_credential(
            spoof_cred_id,
            self.cs_utils.build_aws_payload(
                access_key_id="AKIAT13SPOOF",
                secret_access_key=spoof_secret,
                region="us-east-1",
                allowed_services=["n1ql"],
            ),
        )
        self.assertEqual(
            int(status_s) if status_s else 0, 201,
            f"[T13] Setup db_test/1: expected 201, got {status_s}. content={content_s}",
        )

        try:
            # ── Phase 1: suffix wildcard db/* ─────────────────────────────────
            with self.subTest(phase="1-suffix-wildcard"):
                self.log.info("[T13 Phase 1] Granting alice credential_consumer[db/*]")
                status_g1, _ = self._set_user_roles(
                    self.ALICE_USER, "credential_consumer[db/*]"
                )
                self.assertEqual(
                    int(status_g1) if status_g1 else 0, 200,
                    f"[T13 Phase 1] Grant credential_consumer[db/*] expected 200, got {status_g1}. "
                    "Server validates prefix existence at grant time — verify db/prod/1 was created.",
                )

                self.log.info("[T13 Phase 1] Consume db/prod/1 as alice (expect 200)")
                status_allow, body_allow = self._consume_as_cbq_on_behalf_of(
                    target_cred_id, self.ALICE_USER, "local"
                )
                self._assert_consume_allowed(
                    status_allow, body_allow,
                    context="credential_consumer[db/*] covers db/prod/1",
                )
                self._assert_consume_has_secret(body_allow, target_cred_id, target_secret)
                self.log.info("[T13 Phase 1] db/prod/1 → 200 (db/* matched)")

                self.log.info(
                    "[T13 Phase 1] Consume db_test/1 as alice "
                    "(expect 403 — slash is literal, not a prefix glob)"
                )
                status_deny, body_deny = self._consume_as_cbq_on_behalf_of(
                    spoof_cred_id, self.ALICE_USER, "local"
                )
                self._assert_consume_denied(
                    status_deny, body_deny, ERROR_INSUFFICIENT_PERMISSIONS,
                    context=(
                        "credential_consumer[db/*] does NOT cover db_test/1 — "
                        "the Erlang router matches 'db/' literally, not as a glob prefix"
                    ),
                )
                self.log.info(
                    "[T13 Phase 1] db_test/1 → 403 INSUFFICIENT_PERMISSIONS "
                    "(slash boundary enforced: db_test/ ≠ db/)"
                )

            # Reset between phases — runs even when Phase 1 assertions fail because
            # subTest swallows AssertionError and resumes after the with block.
            self._set_user_roles(self.ALICE_USER, "")

            with self.subTest(phase="2-prefix-asterisk"):
                self.log.info(
                    "[T13 Phase 2] Attempting grant credential_consumer[*prod/key] (expect 400)"
                )
                status_bad, content_bad = self._set_user_roles(
                    self.ALICE_USER, "credential_consumer[*prod/key]"
                )
                actual_bad = int(status_bad) if status_bad else 0
                self.assertEqual(
                    actual_bad, 400,
                    f"[T13 Phase 2] credential_consumer[*prod/key] expected 400 — "
                    "wildcard is suffix-only; *prod/key is treated as a literal ID lookup "
                    f"and no such credential exists. Got {actual_bad}. content={content_bad}",
                )
                parsed_bad = self.cs_utils.parse_content(content_bad) or {}
                err = str((parsed_bad.get("errors") or {}).get("roles", ""))
                if err:
                    self.assertTrue(
                        any(w in err.lower() for w in ("undefined", "unknown", "malformed")),
                        f"[T13 Phase 2] 400 error should mention undefined/unknown/malformed. "
                        f"roles_error={err!r}",
                    )
                self.log.info("[T13 Phase 2] credential_consumer[*prod/key] correctly rejected (400)")

            with self.subTest(phase="3-master-key"):
                self.log.info("[T13 Phase 3] Granting alice credential_consumer[*] (master key)")
                status_g3, _ = self._set_user_roles(
                    self.ALICE_USER, "credential_consumer[*]"
                )
                self.assertEqual(
                    int(status_g3) if status_g3 else 0, 200,
                    f"[T13 Phase 3] Grant credential_consumer[*] expected 200, got {status_g3}",
                )

                self.log.info("[T13 Phase 3] Consume db/prod/1 as alice (expect 200 — * matches all)")
                status_m1, body_m1 = self._consume_as_cbq_on_behalf_of(
                    target_cred_id, self.ALICE_USER, "local"
                )
                self._assert_consume_allowed(
                    status_m1, body_m1,
                    context="credential_consumer[*] covers db/prod/1",
                )
                self._assert_consume_has_secret(body_m1, target_cred_id, target_secret)

                self.log.info("[T13 Phase 3] Consume db_test/1 as alice (expect 200 — * matches all)")
                status_m2, body_m2 = self._consume_as_cbq_on_behalf_of(
                    spoof_cred_id, self.ALICE_USER, "local"
                )
                self._assert_consume_allowed(
                    status_m2, body_m2,
                    context="credential_consumer[*] covers db_test/1",
                )
                self._assert_consume_has_secret(body_m2, spoof_cred_id, spoof_secret)

                self.log.info(
                    "[T13] All phases PASSED — "
                    "db/* slash boundary enforced, *prod/key rejected (400), * is master key"
                )

        finally:
            self._set_user_roles(self.ALICE_USER, "")
            self.log.info("[T13] Alice roles cleared in finally block")

    def test_id_validation_matrix(self):
        """
        T14: Validate credential ID constraints (Campaign 2).
        - Max length is 128 characters. 129 must fail.
        - Must be printable ASCII. Spaces and non-ASCII must fail.
        - Hierarchical slashes are valid.
        - Duplicate POST to an existing ID must fail with 409 (chronicle conflict).
        """
        self.log.info("Testing credential ID validation matrix")

        base_payload = self.cs_utils.build_aws_payload(
            access_key_id="AKIA-ID-TEST",
            secret_access_key="SECRET-ID-TEST",
            region="us-east-1"
        )

        # 1. Run the validation matrix
        for cred_id, expected_status, context in _ID_MATRIX:
            with self.subTest(cred_id=cred_id, context=context):
                self.log.info(f"Testing ID rule: {context}")

                # We do not use _create_tracked_credential for expected failures
                # because we don't want tearDown to try deleting non-existent IDs.
                status, content = self.cs_utils.create_credential(
                    self.rest, cred_id, base_payload
                )
                actual_status = int(status) if status else 0

                self.assertEqual(
                    actual_status, expected_status,
                    f"[{context}] Expected {expected_status}, got {actual_status}. content={content}"
                )

                # Manually track successful creations for cleanup
                if actual_status == 201:
                    self._created_creds.append(cred_id)

                self.log.info(f"PASSED: {context} -> {actual_status}")

        # 2. Test Duplicate Creation (Collision)
        dup_id = "p1-duplicate-id"
        self.log.info(f"Testing duplicate POST creation for ID: {dup_id}")

        # First creation should succeed
        status_first, _ = self._create_tracked_credential(dup_id, base_payload)
        self.assertEqual(int(status_first) if status_first else 0, 201, "Initial creation failed")

        # Second creation (POST) to the exact same ID should fail
        status_dup, content_dup = self.cs_utils.create_credential(self.rest, dup_id, base_payload)
        actual_dup_status = int(status_dup) if status_dup else 0

        with self.subTest(phase="duplicate-post"):
            self.assertEqual(
                actual_dup_status, 409,
                f"Duplicate POST expected 409 (chronicle conflict), got {actual_dup_status}. content={content_dup}"
            )
        self.log.info(f"PASSED: Duplicate creation rejected with status {actual_dup_status}")

    def test_consume_auth_matrix(self):
        """T11: Pattern A/B consume matrix — all error codes + both patterns (12 rows)."""
        self._require_cbq_password()
        test_expiry_wait = self.input.param("test_expiry_wait", False)

        cred_id = "p1-t11-1"
        missing_id = "p1-t11-missing"
        known_secret = "T11_CONSUME_SECRET"

        payload = self.cs_utils.build_aws_payload(
            access_key_id="AKIAT11EXAMPLE",
            secret_access_key=known_secret,
            region="us-east-1",
            allowed_services=["n1ql"],
        )
        status_create, content_create = self._create_tracked_credential(cred_id, payload)
        self.assertEqual(
            int(status_create) if status_create else 0, 201,
            f"[T11] Credential create expected 201, got {status_create}. "
            f"content={content_create}",
        )

        # Grant alice role once — persists for all alice_role rows
        status_grant, _ = self.cs_utils.grant_consume_to_local_user(
            self.rest, self.ALICE_USER, cred_id
        )
        self.assertEqual(
            int(status_grant) if status_grant else 0, 200,
            f"[T11] Grant alice role expected 200, got {status_grant}",
        )

        current_guardrail = ["n1ql"]

        for row in _CONSUME_AUTH_MATRIX:
            row_id = row["id"]

            if row.get("requires_expiry"):
                if not test_expiry_wait:
                    self.log.info(f"[T11 {row_id}] Skipped — requires_expiry, test_expiry_wait=False")
                    continue
                with self.subTest(row=row_id):
                    self._run_t11_expiry_row(row, known_secret)
                continue

            with self.subTest(row=row_id):
                # Sync guardrail to what this row needs
                target_guardrail = row["guardrail"]
                if target_guardrail != current_guardrail:
                    self._t11_update_guardrail(cred_id, target_guardrail, known_secret)
                    current_guardrail = target_guardrail

                self._setup_consume_matrix_rbac(row, cred_id)
                try:
                    consume_id = missing_id if row.get("use_missing_id") else cred_id
                    if row["caller"] != "cbq-engine":
                        self.log.info(
                            f"[T11 {row_id}] Authenticating as @{row['caller']} "
                            "using shared cbauth special password"
                        )
                    status, body = self.cs_utils.consume_credential(
                        base_url=self.cluster_base_url,
                        cred_id=consume_id,
                        service_name=row["caller"],
                        service_password=self.cbq_engine_password,
                        on_behalf_user=row["user"],
                        on_behalf_domain=row["domain"],
                    )
                    self._assert_consume_matrix_row(row, status, body)
                    self.log.info(f"[T11 {row_id}] PASSED")
                finally:
                    self._teardown_consume_matrix_rbac(row)

