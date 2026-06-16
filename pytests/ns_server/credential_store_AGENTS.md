---
name: credential-store-test-agent
description: >
  Credential Store P0/P1 tests for Couchbase Server 8.1+ Enterprise.
  Covers admin CRUD, secret redaction, Pattern A/B consume paths, guardrail
  enforcement, prerequisite overrides, credential expiry, Chronicle cross-node
  replication, node failure resilience, all 8 credential type smoke tests,
  a 12-row consume auth matrix covering every error code and both patterns,
  a 10-row RBAC grant matrix covering who can grant credential_consumer
  to users vs services (maps to manual M1–M3 and QA PRD §9.5), and
  wildcard role boundary tests (T13: db/* slash boundary, *prod/key rejection, * master key).
model: inherit
---

# Credential Store Test Suite

**Test file:** `pytests/ns_server/credential_store_test.py`
**Base class:** `pytests/ns_server/credential_store_base.py`
**Config file:** `conf/ns_server/py-credential_store_test.conf`
**Utility file:** `couchbase_utils/security_utils/credential_store_utils.py`
**Low-level REST:** `couchbase_utils/cb_server_rest_util/security/credential_store.py`

## Purpose

Validates Couchbase Server's Credential Store subsystem (8.1+ Enterprise only).
Tests cover: admin CRUD with secret redaction, Pattern A end-user consume via
`@cbq-engine`, guardrail enforcement (`allowedServices`), Pattern B service-identity
consume, prerequisite enforcement (n2n encryption / override settings), credential
expiry (including boundary validation), Chronicle cross-node replication (CREATE +
UPDATE propagation), node failure resilience (GET from local Chronicle replica
while master is offline), credential type smoke for all 8 types (T08), a 12-row consume auth matrix covering
all error codes + Pattern A/B (T11), a 10-row RBAC grant matrix (T12), and
wildcard role boundary tests (T13).

---

## Architecture

Four-layer design. Each layer has a single responsibility and no upward dependency.

```
Layer 1 — Raw REST client (transport only)
┌─────────────────────────────────────────────────────────────────────────────┐
│  cb_server_rest_util/security/credential_store.py :: CredentialStoreAPI     │
│    (CBRestConnection)                                                       │
│    Credential CRUD  create_credential()  get_credential()                   │
│                     list_credentials()   update_credential()                │
│                     delete_credential()                                     │
│    Store settings   get_credential_store_settings()                         │
│                     put_credential_store_settings()                         │
│                     delete_credential_store_settings()                      │
│    Service roles    get_service_roles()  put_service_roles()                │
│                     delete_service_roles()                                  │
│    Static helper    _encode_id(cred_id) — URL-encodes path segment,        │
│                     raises ValueError on None/empty                         │
│    Does NOT cover /_cbauth/getCredential (requires service Basic Auth,      │
│    not admin credentials — handled in CredentialStoreUtils).               │
└─────────────────────────────────────────────────────────────────────────────┘
          │  used by
          ▼
Layer 2 — Utility + all constants (no unittest dependency)
┌─────────────────────────────────────────────────────────────────────────────┐
│  couchbase_utils/security_utils/credential_store_utils.py                   │
├─────────────────────────────────────────────────────────────────────────────┤
│  Module-level constants (single source of truth for all paths)              │
│                                                                             │
│  Couchbase endpoint constants (re-exported from credential_store.py):       │
│    ENDPOINT_CREDENTIALS          /settings/credentials                      │
│    ENDPOINT_CREDENTIAL_STORE     /settings/credentialStore                  │
│    ENDPOINT_RBAC_SERVICES        /settings/rbac/services                    │
│    ENDPOINT_CBAUTH_CREDENTIAL    /_cbauth/getCredential                     │
│    ENDPOINT_RBAC_USERS           /settings/rbac/users                       │
│                                                                             │
│  cbauth error codes (re-exported from credential_store.py):                 │
│    ERROR_INSUFFICIENT_PERMISSIONS                                           │
│    ERROR_SERVICE_GUARDRAIL_BLOCKED                                          │
│    ERROR_CREDENTIAL_EXPIRED                                                 │
│    ERROR_UNSUPPORTED_SCHEMA_VERSION                                         │
│                                                                             │
│  SENSITIVE_FIELDS_BY_TYPE — per-type set of fields that must never          │
│    appear as plaintext in admin (non-consume) API responses                 │
│                                                                             │
│  CredentialStoreUtils class:                                                │
│    Payload builders  build_aws_payload()  expires_at_ms()                  │
│    Admin CRUD        create_credential()  get_credential()                  │
│                      list_credentials()   update_credential()               │
│                      delete_credential()                                    │
│    Store settings    get_store_settings()  put_store_settings()             │
│                      delete_store_settings()                                │
│    Service roles     get_service_roles()  put_service_roles()               │
│                      delete_service_roles()                                 │
│    Local users       create_local_user()  delete_local_user()               │
│                      grant_consume_to_local_user()                          │
│    Consume path      consume_credential() — uses requests.get() with        │
│                      service Basic Auth (@<service_name>:<password>)        │
│    Redaction         assert_secrets_redacted()  assert_no_raw_secret()      │
│    Parsing           parse_content()  get_warnings()                        │
│                      get_credential_ids_from_list()                         │
│    Error helpers     get_error_code()  get_cbauth_error_code()              │
│                      (handles nested {"error": {"code": "..."}} format)     │
│    Status helpers    assert_success_status()  assert_error_status()         │
│                      _is_success_status()  status_code()                    │
│    Logging           log_safe()  _redact_for_log()                          │
│    Internal          _cs_api()  _base_url()  _url_host()                    │
│                      _connection_use_https()  _encode_path_segment()        │
└─────────────────────────────────────────────────────────────────────────────┘
          │  used by
          ▼
Layer 3 — Test base class (framework helpers, setUp/tearDown)
┌─────────────────────────────────────────────────────────────────────────────┐
│  pytests/ns_server/credential_store_base.py :: CredentialStoreBase          │
│    (ClusterSetup)                                                           │
├─────────────────────────────────────────────────────────────────────────────┤
│  Test users (class-level constants):                                        │
│    SEC_ADMIN_USER / SEC_ADMIN_PASS   — security_admin role                  │
│    USER_ADMIN_USER / USER_ADMIN_PASS — user_admin_local role (T12)          │
│    ALICE_USER / ALICE_PASS           — no initial roles                     │
│    BOB_USER  / BOB_PASS             — no initial roles                      │
│                                                                             │
│  setUp()                                                                    │
│    ├── CredentialStoreUtils(log=self.log) instance                          │
│    ├── RestConnection to cluster master                                     │
│    ├── cluster_base_url (scheme://ip:port, handles explicit-None port)      │
│    ├── cbq_engine_password  via _resolve_cbq_engine_password()             │
│    ├── _created_creds / _modified_services  (tearDown cleanup lists)        │
│    ├── _provision_test_users()  (fails fast on user creation error)         │
│    └── _enable_n2n_override()  (n2nEncryptionOverride=true for all tests)   │
│  tearDown()  →  cleanup creds → cleanup service roles                       │
│              →  delete_store_settings() → deprovision users                 │
│                                                                             │
│  cbauth password resolution                                                 │
│    _resolve_cbq_engine_password()    explicit param → env var → diag/eval  │
│    _fetch_cbq_engine_password_via_diag_eval()                               │
│      tries ns_config_auth:get_password(special). first — no Jenkins        │
│      credential needed for typical CI runs                                  │
│    _require_cbq_password()           self.fail() if password unresolved     │
│                                                                             │
│  Store settings management                                                  │
│    _enable_n2n_override()    PUT n2nEncryptionOverride=true                │
│    _set_strict_store_settings()  PUT both overrides=false (S5 only)        │
│                                                                             │
│  Credential lifecycle                                                       │
│    _create_tracked_credential(cred_id, payload)  create + register cleanup │
│    _cleanup_created_credentials()                delete all tracked creds   │
│    _cleanup_service_roles()                      delete modified services   │
│                                                                             │
│  Consume-path helpers                                                       │
│    _consume_as_cbq_on_behalf_of(cred_id, user, domain)  Pattern A call     │
│    _consume_as_cbq_service_identity(cred_id)             Pattern B call     │
│    _grant_service_roles(service, roles)                  PUT + track        │
│                                                                             │
│  Assertion helpers                                                          │
│    _assert_consume_allowed(status, body, context)                           │
│    _assert_consume_denied(status, body, expected_error_code, context)       │
│    _assert_consume_has_secret(body, cred_id, expected_secret)               │
│    _get_credential_guardrails(cred_id)  handles flat + nested shapes       │
│    _verify_user_has_consume_role(username, cred_id)                         │
│      handles both {"role":"credential_consumer[id]"} and                   │
│      {"role":"credential_consumer","credential_id":"id"} formats            │
│    _verify_user_no_consume_role(username)  pre-check baseline               │
│                                                                             │
│  Multi-node helpers (S6+)                                                   │
│    _non_master_node()   first server in cluster.servers whose IP ≠ master  │
│    _poll_until(label, fn, timeout_s=15, interval_s=0.5)                     │
│      bounded poll — fails with TimeoutError message if fn() never truthy   │
│    _consume_as_cbq_on_behalf_of_on_node(base_url, cred_id, user, domain,   │
│      service_password=None)  Pattern A targeting a specific node's URL;    │
│      pass per-node cbauth token via service_password                        │
│    _fetch_cbq_engine_password_via_diag_eval(rest=None)                      │
│      accepts optional rest= to fetch password from a non-master node        │
│      (cbauth special password is PER-NODE — not cluster-wide)               │
│                                                                             │
│  Node lifecycle helpers (S6.2+)                                             │
│    _stop_couchbase_on_node(server)   SSH systemctl stop + disconnect        │
│    _start_couchbase_on_node(server, timeout_s=120)                          │
│      SSH systemctl start, then polls cluster_util.is_ns_server_running()   │
│      up to timeout_s; self.fail() if node doesn't return healthy            │
│                                                                             │
│  T12/T13 role helpers                                                       │
│    _get_test_user_password(username)  lookup known password for test user   │
│    _set_user_roles(username, roles_str)  PUT roles as Full Admin; ""=clear  │
│      accepts wildcards (credential_consumer[db/*], [*], etc.)               │
│      used by T13 to set/clear alice's roles between phases                  │
│    _actor_credentials(actor_name)                                           │
│      "cs_sec_admin" → (SEC_ADMIN_USER, SEC_ADMIN_PASS)                      │
│      "cs_user_admin" → (USER_ADMIN_USER, USER_ADMIN_PASS)                   │
│      "cbq_engine" → ("@cbq-engine", cbq_engine_password)                   │
│      "Administrator" → (None, None) — uses rest connection default creds   │
│    _call_grant_api(row)                                                     │
│      dispatches row["api"]: user_role / service_role /                      │
│      service_role_delete / credential_create                                │
│      returns (status_code, content)                                         │
│    _run_grant_row(row, cred_id)   call → assert → [verify] → teardown      │
│    _verify_service_has_role(service_name, cred_id)                          │
│      GET service roles; accepts bracket and separate-field notation         │
│    _teardown_grant_row(row)   resets alice roles or deletes service roles   │
│      only on 200 rows; best-effort, warns on failure                        │
└─────────────────────────────────────────────────────────────────────────────┘
          │  inherits
          ▼
Layer 4 — Tests only
┌─────────────────────────────────────────────────────────────────────────────┐
│  pytests/ns_server/credential_store_test.py :: CredentialStoreTest          │
│    (CredentialStoreBase)                                                    │
│    test_* methods + _is_node_rest_reachable() private helper                │
│    Imports ERROR_* constants from credential_store_utils for assertions     │
│    Imports requests for short-timeout reachability probe (S6.2)             │
│                                                                             │
│  Module-level constants (defined before class, shared across test methods): │
│    _TYPE_SMOKE_ROWS — tuple of (cred_type, fields_dict, known_secret,      │
│      non_sensitive_fields) for all 8 types                                  │
│    _CONSUME_AUTH_MATRIX — tuple of 12 dicts (T11): id, pattern (A/B),      │
│      caller, user, domain, guardrail, setup, exp_status, exp_error,        │
│      and optional use_missing_id / requires_expiry flags                    │
│    _GRANT_MATRIX — tuple of 10 dicts (T12): id, actor, api, target,        │
│      role, exp_status, verify, and optional requires_cbq_password flag      │
│                                                                             │
│  T11 private helpers (test_consume_auth_matrix only):                       │
│    _t11_update_guardrail(cred_id, guardrail, known_secret)                  │
│    _setup_consume_matrix_rbac(row, cred_id)                                 │
│    _teardown_consume_matrix_rbac(row)                                       │
│    _assert_consume_matrix_row(row, status, body)                            │
│    _run_t11_expiry_row(row, known_secret)                                   │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Consume path URL pattern

```python
# Pattern A — @cbq-engine on behalf of end user
GET /_cbauth/getCredential/<cred_id>?user=alice&domain=local
Authorization: Basic @cbq-engine:<cbauth_service_password>

# Pattern B — @cbq-engine as itself (service identity)
GET /_cbauth/getCredential/<cred_id>?user=@cbq-engine&domain=admin
Authorization: Basic @cbq-engine:<cbauth_service_password>

# Identity mismatch test (Pattern B deny)
GET /_cbauth/getCredential/<cred_id>?user=@backup&domain=admin
Authorization: Basic @cbq-engine:<cbauth_service_password>
```

### cbauth error format

```python
# Nested dict (actual format returned by CB 8.1):
{"error": {"code": "INSUFFICIENT_PERMISSIONS", "reason": "..."}}

# Flat string (legacy format):
{"error": "INSUFFICIENT_PERMISSIONS"}

# get_cbauth_error_code() handles both transparently
```

### RBAC role representation

Server returns parameterized roles in two possible shapes; both must be accepted:

```python
# Bracket notation (some paths):
{"role": "credential_consumer[p0-s3-c1]"}

# Separate-field notation (most paths in 8.1):
{"role": "credential_consumer", "credential_id": "p0-s3-c1"}
```

`_verify_user_has_consume_role()` accepts either shape.

### Warnings semantics

Warnings appear in `GET /settings/credentialStore` and `GET /settings/credentials`
**only when** credentials exist AND a real encryption gap is present
(n2n OFF + n2nEncryptionOverride=ON). When n2n is fully enabled on the cluster,
the override is redundant and warnings must be empty.

The `cluster_has_n2n` flag in S5 branches warning assertions accordingly —
look for `[WARNINGS-BRANCH: n2n=ON/OFF]` in CI logs.

---

## Test Users

| User | Password | Roles | Purpose |
|------|----------|-------|---------|
| `cs_sec_admin` | `Couchbase@1234` | `security_admin` | RBAC enforcement tests (G1, G2, S4) |
| `cs_user_admin` | `UserAdmin@1234` | `user_admin_local` | T12 grant matrix actor |
| `cs_alice` | `Alice@1234` | none initially | consume-allowed assertions; T12 grant target |
| `cs_bob` | `Bob@1234` | none | consume-denied assertions |

Users are provisioned in setUp and deleted in tearDown. Creation failure is a
hard `self.fail()` — no silent skip.

---

## Environment Variables (Secrets)

No secrets are required for S1 and S5.  Consume-path tests (S2, S3, S4) need the
`@cbq-engine` cbauth service password.  It is resolved automatically at setUp time
via `ns_config_auth:get_password(special).` through `diag/eval` — no Jenkins
credential binding is needed for standard clusters.

| Variable | Description | Used By |
|----------|-------------|---------|
| `CBAUTH_CBQ_ENGINE_PASSWORD` | Override for @cbq-engine cbauth password | S2, S3, S4 |

### Resolution order

```python
def _resolve_cbq_engine_password():
    # 1. -p cbq_engine_password=<value>    (local runs / CI override)
    # 2. CBAUTH_CBQ_ENGINE_PASSWORD env var
    # 3. POST /diag/eval  ns_config_auth:get_password(special).
    #    (automatic — reads the real-time rotating token from the cluster)
```

### Local Setup

```bash
# Only needed if diag/eval is disabled or returns nothing
export CBAUTH_CBQ_ENGINE_PASSWORD="<password>"

# Or pass inline
python testrunner.py -i node.ini \
    -t ns_server.credential_store_test.CredentialStoreTest.test_pattern_a_end_user_consumption,\
nodes_init=1,services_init=kv:n1ql,GROUP=P0 \
    -p cbq_engine_password=<password>
```

---

## Test Methods

| Test | Scenario | GROUP | `nodes_init` | `services_init` | Description |
|------|----------|-------|--------------|-----------------|-------------|
| `test_admin_crud_and_secret_redaction` | S1 | P0 | 1 | `kv` | POST→GET→list→invalid POST→type-change PUT→DELETE→404; secretAccessKey masked |
| `test_pattern_a_end_user_consumption` | S2 | P0 | 1 | `kv:n1ql` | @cbq-engine on behalf of alice (200) and bob (403 INSUFFICIENT_PERMISSIONS) |
| `test_guardrails_enforcement_pattern_a` | S3 | P0 | 1 | `kv:n1ql` | allowedServices=[n1ql] → 200; update to [backup] → 403 SERVICE_GUARDRAIL_BLOCKED |
| `test_pattern_b_service_identity` | S4 | P0 | 1 | `kv:n1ql` | sec_admin PUT 403; no-role 403; grant; service-identity 200 + guardrail bypass; mismatch 403 |
| `test_prerequisites_override_and_expiry` | S5 | P0 | 1 | `kv` | strict settings, n2n override, warnings, DELETE reset, expiresAt <5min=400, 6min=201 |
| `test_cross_node_credential_consume` | S6 | P1 | 2 | `kv:n1ql` | Chronicle CREATE propagation → Node B 200; guardrail UPDATE → Node B 403 SERVICE_GUARDRAIL_BLOCKED |
| `test_node_failure_resilience` | S6.2 | P1 | 2 | `kv` | Node A stopped via SSH; Node B must return 200 from local Chronicle replica |
| `test_credential_type_smoke` | T08 | P1 | 1 | `kv` | POST→GET→assert secrets redacted for all 8 credential types |
| `test_consume_auth_matrix` | T11 | P1 | 1 | `kv:n1ql` | 12-row matrix: all cbauth error codes, Pattern A (rows A1–A6) and Pattern B (rows B1–B6) |
| `test_rbac_grant_matrix` | T12 | P1 | 1 | `kv:n1ql` | 10-row matrix: who can grant credential_consumer to users vs services; G6/G7 skip if no cbq password |
| `test_wildcard_role_boundaries` | T13 | P1 | 1 | `kv:n1ql` | 3-phase: db/* slash boundary (200+403), *prod/key rejected (400), * master key (200+200) |
| `test_id_validation_matrix` | T14 | P1 | 1 | `kv` | ID validation: 128 chars=201, 129=400, space=400, non-ASCII=400, slashes=201, duplicate=409 |

### `test_prerequisites_override_and_expiry` branches

**Strict block branch** (logged as `[STRICT-BRANCH: n2n=ON/OFF]`):
- If cluster n2n is OFF → credential create with strict settings returns 400 (`cluster_has_n2n=False`)
- If cluster n2n is ON → create succeeds; strict block is implicitly satisfied (`cluster_has_n2n=True`)

**Warnings branch** (logged as `[WARNINGS-BRANCH: n2n=ON/OFF]`):
- `cluster_has_n2n=True` → asserts `warnings == []`
- `cluster_has_n2n=False` → asserts `len(warnings) > 0`

**Expiry slow path** (gated on `test_expiry_wait=True`):
- Creates credential with `expiresAt=now+360s` and `allowedServices=["n1ql"]`
- Grants alice consume role
- Checkpoint 1: consume immediately → 200 (well before expiry)
- Checkpoint 2: consume at `expiresAt − 2s` → 200 (proves no early expiry)
- Checkpoint 3: consume at `expiresAt + 6s` → 403 CREDENTIAL_EXPIRED
- Typical runtime: ~6 min when credential TTL is 360s

### `test_cross_node_credential_consume` phases (S6)

**Phase 1 — CREATE propagation:**
- Create credential on Node A (master), grant alice role on Node A
- `_poll_until` Node B admin GET returns 200 (Chronicle landed, ≤15s)
- Pattern A consume on Node B using Node B's per-node cbauth password → 200 + plaintext secret

**Phase 2 — UPDATE propagation:**
- PUT guardrail update on Node A (`allowedServices=[backup]`, excludes n1ql)
- `_poll_until` Node B admin GET shows `allowedServices=["backup"]` (≤15s)
- Pattern A consume on Node B → 403 SERVICE_GUARDRAIL_BLOCKED

**Per-node cbauth password:** `ns_config_auth:get_password(special)` is local to each
ns_server instance. Node B's token is fetched via `_fetch_cbq_engine_password_via_diag_eval(rest=node_b_rest)`
and passed as `service_password=node_b_cbq_password` to all Node B consume calls.
Using Node A's token on Node B returns 401.

### `test_node_failure_resilience` flow (S6.2)

1. Create credential on Node A, poll Node B GET until replicated (≤15s)
2. `_stop_couchbase_on_node(node_a)` — SSH `systemctl stop`
3. `_poll_until` `_is_node_rest_reachable(node_a)` returns False (≤20s, 2s timeout per probe)
4. GET credential from Node B → assert 200, correct `id`, correct `type`, secrets redacted
5. LIST is informational only — returns `[]` in a 2-node cluster with master down (orchestrator quorum required for LIST, but GET by ID reads from local Chronicle snapshot)
6. `finally`: `_start_couchbase_on_node(node_a, timeout_s=120)` always runs so tearDown can delete the credential

### `test_credential_type_smoke` flow (T08)

Iterates `_TYPE_SMOKE_ROWS` (8 entries) using `with self.subTest(type=cred_type)`:

1. POST `/settings/credentials/<cred_id>` with type-specific `fields` dict → assert 201
2. GET `/settings/credentials/<cred_id>` → assert success status
3. `cs_utils.assert_secrets_redacted(body, cred_type, known_secret_values=[known_secret])` — ensures none of the secret values appear as plaintext in the GET response
4. `azureManaged` has no secret field (`known_secret=None`) → redaction list is empty, just confirms GET succeeds
5. Credentials are tracked for tearDown auto-cleanup

### `test_consume_auth_matrix` flow (T11)

Iterates `_CONSUME_AUTH_MATRIX` (12 rows) using `with self.subTest(id=row["id"])`:

**Setup (before loop):**
- Creates one `aws` credential `p1-t11-c1` with guardrail `allowedServices=["n1ql"]`
- Grants `cs_alice` consume role (persists for all `alice_role` rows)

**Per-row logic:**
- Updates guardrail only when `row["guardrail"]` differs from previous row (avoids redundant PUTs)
- Calls `_setup_consume_matrix_rbac(row, cred_id)` — grants n1ql or backup service role for Pattern B rows; no-op for Pattern A rows
- `requires_expiry=True` rows (A6): delegated to `_run_t11_expiry_row`; skipped unless `test_expiry_wait=True`
- `use_missing_id=True` rows (A5, B6): use `p1-t11-missing` (never created) → expect 404
- B4/B5 rows: Pattern B with `@backup`/`@cbcontbk` service identity
- `_assert_consume_matrix_row(row, status, body)` dispatches: 200→`_assert_consume_allowed`, 403→`_assert_consume_denied`, 404→`assertEqual`
- `_teardown_consume_matrix_rbac(row)` revokes service roles (best-effort, warns on failure)

**Matrix rows:**

| Row | Pattern | Caller | User | Guardrail | Setup | Expected |
|-----|---------|--------|------|-----------|-------|---------|
| A1 | A | cbq-engine | cs_alice | n1ql | alice_role | 200 |
| A2 | A | cbq-engine | cs_bob | n1ql | alice_role (bob denied) | 403 INSUFFICIENT_PERMISSIONS |
| A3 | A | backup | cs_alice | n1ql | alice_role | 403 SERVICE_GUARDRAIL_BLOCKED |
| A4 | A | cbq-engine | cs_alice | [backup] | alice_role | 403 SERVICE_GUARDRAIL_BLOCKED |
| A5 | A | cbq-engine | cs_alice | n1ql | alice_role | 403 INSUFFICIENT_PERMISSIONS (missing ID, RBAC before lookup) |
| A6 | A | cbq-engine | cs_alice | n1ql | alice_role | 403 CREDENTIAL_EXPIRED (slow) |
| B1 | B | cbq-engine | @cbq-engine | backup | no_service_role | 403 INSUFFICIENT_PERMISSIONS |
| B2 | B | cbq-engine | @cbq-engine | backup | n1ql_service_role | 200 |
| B3 | B | cbq-engine | @backup | backup | n1ql_service_role | 403 INSUFFICIENT_PERMISSIONS |
| B4 | B | backup | @backup | n1ql | backup_service_role | 200 |
| B5 | B | cbcontbk | @cbcontbk | n1ql | backup_service_role | 200 |
| B6 | B | backup | @backup | n1ql | no_service_role | 403 INSUFFICIENT_PERMISSIONS (missing ID, RBAC before lookup) |

### `test_rbac_grant_matrix` flow (T12)

Creates credential `p1-rbac-g1` once, then iterates `_GRANT_MATRIX` (10 rows) using
`with self.subTest(row=row_id)`. G6/G7 are skipped if `cbq_engine_password` is None.

**Per-row logic (`_run_grant_row`):**
1. `_actor_credentials(row["actor"])` resolves (username, password) for the actor
2. `_call_grant_api(row)` dispatches the right HTTP call (PUT user / PUT service / DELETE service / POST credential)
3. `assertEqual(actual, exp_status)` — hard fail on mismatch
4. On 200 + `row["verify"]`: `_verify_user_has_consume_role` or `_verify_service_has_role`
5. `finally: _teardown_grant_row(row)` — resets alice roles or deletes service roles (200 rows only)

**Matrix rows:**

| Row | Actor | API | Target | Role | Expected |
|-----|-------|-----|--------|------|---------|
| G1 | cs_sec_admin | user_role | cs_alice | `credential_consumer[p1-rbac-g1]` | 403 |
| G2 | cs_sec_admin | service_role | n1ql | `credential_consumer[p1-rbac-g1]` | 403 |
| G3 | cs_user_admin | user_role | cs_alice | `credential_consumer[p1-rbac-g1]` | 200 + verify |
| G4 | cs_user_admin | credential_create | p1-rbac-g2 | — | 403 |
| G5 | cs_user_admin | service_role | n1ql | `credential_consumer[*]` | 403 |
| G6 | cbq_engine | service_role | n1ql | `credential_consumer[*]` | 403 (skip if no cbq pwd) |
| G7 | cbq_engine | service_role_delete | n1ql | — | 403 (skip if no cbq pwd) |
| G8 | Administrator | service_role | backup | `credential_consumer[p1-rbac-g1]` | 200 + verify |
| G9 | cs_user_admin | user_role | cs_alice | `credential_consumer[ghost-id]` | 400 (non-existent ID) |
| G10 | cs_user_admin | user_role | cs_alice | `credential_consumer[no/prefix/*]` | 400 (non-existent prefix) |

**G9/G10 note:** Both rows rely on the server validating credential existence at grant
time. `credential_consumer[no/prefix/*]` is syntactically valid (suffix wildcard); it
fails because no credential with prefix `no/prefix/` exists — same existence check as
G9. If the server uses lazy validation, adjust `exp_status` to 200.

### `test_wildcard_role_boundaries` flow (T13)

Two credentials are created up front: `db/prod/1` (target, inside `db/` namespace)
and `db_test/1` (spoof, same starting letters but without the `db/` directory prefix).
Both have `allowedServices=["n1ql"]` so guardrail is not the reason for allow/deny.

**Phase 1 — suffix wildcard:**
- `_set_user_roles(ALICE_USER, "credential_consumer[db/*]")` → expect 200
  (server validates at least one credential matches the `db/` prefix at grant time)
- Consume `db/prod/1` as alice → 200 + plaintext secret (wildcard matches)
- Consume `db_test/1` as alice → 403 INSUFFICIENT_PERMISSIONS
  (the Erlang router treats `/` literally: `db_test/` is not a sub-path of `db/`)
- `_set_user_roles(ALICE_USER, "")` — clear alice's roles before Phase 2

**Phase 2 — prefix asterisk rejected:**
- `_set_user_roles(ALICE_USER, "credential_consumer[*prod/key]")` → expect 400
  (wildcard is suffix-only; `*prod/key` is treated as a literal ID lookup; no such credential exists)
- Error body must mention "undefined", "unknown", or "malformed" in the `errors.roles` field

**Phase 3 — master key:**
- `_set_user_roles(ALICE_USER, "credential_consumer[*]")` → expect 200
- Consume `db/prod/1` → 200 + plaintext secret
- Consume `db_test/1` → 200 + plaintext secret (standalone `*` bypasses all prefix checks)

**Cleanup:** `finally:` block calls `_set_user_roles(ALICE_USER, "")` regardless of phase outcome.
Tracked credentials (`db/prod/1`, `db_test/1`) are deleted in tearDown via `_cleanup_created_credentials`.

### `test_id_validation_matrix` flow (T14)

Validates `menelaus_web_credentials.erl` ID string constraints. No consume path — `services_init=kv` only.

**Matrix (5 rows, each in `with self.subTest(cred_id=..., context=...)`):**

| ID value | Expected | Reason |
|----------|---------|--------|
| `"a" * 128` | 201 | Exact maximum length |
| `"b" * 129` | 400 | Exceeds 128-char limit |
| `"my key"` | 400 | Space is not printable-ASCII-without-spaces |
| `"keyñ"` | 400 | Non-ASCII character |
| `"valid/folder/key"` | 201 | Hierarchical slash path is valid |

**Tracking note:** `cs_utils.create_credential` (not `_create_tracked_credential`) is used for expected-failure rows so tearDown does not attempt to DELETE IDs that were never created. Successful rows (`201`) are manually appended to `self._created_creds`.

**Duplicate collision (separate assertion after the matrix):**
- POST `p1-duplicate-id` → 201 via `_create_tracked_credential`
- POST same ID again → `assertEqual(status, 409)`
  `menelaus_web_credentials.erl` returns 409 explicitly; confirmed by `credential_store_tests.py` cluster tests.

---

## Test Execution

```bash
# Run full P0 suite
python testrunner.py -i node.ini -c conf/ns_server/py-credential_store_test.conf -p skip_cluster_reset=True,get-cbcollect-info=False,skip_core_dump_check=True

# S1 — Admin CRUD and secret redaction (no n1ql needed)
python testrunner.py -i node.ini -t ns_server.credential_store_test.CredentialStoreTest.test_admin_crud_and_secret_redaction,nodes_init=1,services_init=kv,GROUP=P0,skip_cluster_reset=True,get-cbcollect-info=False,skip_core_dump_check=True

# S2 — Pattern A end-user consumption (n1ql required)
python testrunner.py -i node.ini -t ns_server.credential_store_test.CredentialStoreTest.test_pattern_a_end_user_consumption,nodes_init=1,services_init=kv:n1ql,GROUP=P0,skip_cluster_reset=True,get-cbcollect-info=False,skip_core_dump_check=True

# S3 — Guardrails enforcement (n1ql required)
python testrunner.py -i node.ini -t ns_server.credential_store_test.CredentialStoreTest.test_guardrails_enforcement_pattern_a,nodes_init=1,services_init=kv:n1ql,GROUP=P0,skip_cluster_reset=True,get-cbcollect-info=False,skip_core_dump_check=True

# S4 — Pattern B service identity (n1ql required)
python testrunner.py -i node.ini -t ns_server.credential_store_test.CredentialStoreTest.test_pattern_b_service_identity,nodes_init=1,services_init=kv:n1ql,GROUP=P0,skip_cluster_reset=True,get-cbcollect-info=False,skip_core_dump_check=True

# S5 — Prerequisites, override, expiry (fast path — no wait)
python testrunner.py -i node.ini -t ns_server.credential_store_test.CredentialStoreTest.test_prerequisites_override_and_expiry,nodes_init=1,services_init=kv,test_expiry_wait=False,GROUP=P0,skip_cluster_reset=True,get-cbcollect-info=False,skip_core_dump_check=True

# S5 — Prerequisites, override, expiry (nightly — includes ~6 min expiry wait)
python testrunner.py -i node.ini -t ns_server.credential_store_test.CredentialStoreTest.test_prerequisites_override_and_expiry,nodes_init=1,services_init=kv:n1ql,test_expiry_wait=True,GROUP=P0,skip_cluster_reset=True,get-cbcollect-info=False,skip_core_dump_check=True

# S6 — Chronicle cross-node replication (nodes_init=2, kv:n1ql on both nodes)
python testrunner.py -i node.ini -t ns_server.credential_store_test.CredentialStoreTest.test_cross_node_credential_consume,nodes_init=2,services_init=kv:n1ql -p get-cbcollect-info=False,skip_core_dump_check=True,rerun=False

# S6.2 — Node failure resilience (nodes_init=2, kv only, SSH access required)
python testrunner.py -i node.ini -t ns_server.credential_store_test.CredentialStoreTest.test_node_failure_resilience,nodes_init=2,services_init=kv -p skip_cluster_reset=True,get-cbcollect-info=False,skip_core_dump_check=True,rerun=False

# T08 — Credential type smoke (all 8 types; kv only, no n1ql required)
python testrunner.py -i node.ini -t ns_server.credential_store_test.CredentialStoreTest.test_credential_type_smoke,nodes_init=1,services_init=kv -p skip_cluster_reset=True,get-cbcollect-info=False,skip_core_dump_check=True,rerun=False

# T11 — Consume auth matrix (12 rows; A6 expiry skipped by default)
python testrunner.py -i node.ini -t ns_server.credential_store_test.CredentialStoreTest.test_consume_auth_matrix,nodes_init=1,services_init=kv:n1ql,test_expiry_wait=False -p skip_cluster_reset=True,get-cbcollect-info=False,skip_core_dump_check=True,rerun=False

# T11 nightly — includes A6 expiry row (~6 min wait)
python testrunner.py -i node.ini -t ns_server.credential_store_test.CredentialStoreTest.test_consume_auth_matrix,nodes_init=1,services_init=kv:n1ql,test_expiry_wait=True -p skip_cluster_reset=True,get-cbcollect-info=False,skip_core_dump_check=True,rerun=False

# T12 — RBAC grant matrix (10 rows; G6/G7 require n1ql for @cbq-engine password)
python testrunner.py -i node.ini -t ns_server.credential_store_test.CredentialStoreTest.test_rbac_grant_matrix,nodes_init=1,services_init=kv:n1ql -p skip_cluster_reset=False,get-cbcollect-info=False,skip_core_dump_check=True,rerun=False

# T13 — Wildcard role boundaries (3 phases: db/*, *prod/key, *)
python testrunner.py -i node.ini -t ns_server.credential_store_test.CredentialStoreTest.test_wildcard_role_boundaries,nodes_init=1,services_init=kv:n1ql -p skip_cluster_reset=True,get-cbcollect-info=False,skip_core_dump_check=True,rerun=False

# T14 — Credential ID validation (max length, character whitelist, duplicate collision; kv only)
python testrunner.py -i node.ini -t ns_server.credential_store_test.CredentialStoreTest.test_id_validation_matrix,nodes_init=1,services_init=kv -p skip_cluster_reset=True,get-cbcollect-info=False,skip_core_dump_check=True,rerun=False
```

---

## Common Errors & Fixes

### Error: "P0 consume test requires @cbq-engine cbauth password"

**Cause:** `_resolve_cbq_engine_password()` got nothing from all three sources.

**Most likely cause:** Cluster was started with `services_init=kv` only — no n1ql
service → no `@cbq-engine` process → `diag/eval` returns `undefined` for all
ns_config paths.

**Fix:**
- Use `services_init=kv:n1ql` in the conf or `-t` argument (S2/S3/S4 all require this)
- Or supply the password explicitly:
  ```bash
  export CBAUTH_CBQ_ENGINE_PASSWORD="<password>"
  # or
  python testrunner.py ... -p cbq_engine_password=<value>
  ```

### Error: "Create expected 201, got 400. content=...Unsupported key: meta"

**Cause:** Payload builder was wrapping `description`, `guardrails`, or `expiresAt`
under a `meta` key. The server expects these as **top-level** request body fields.

**Fix (already applied):** `build_aws_payload()` puts `description`, `expiresAt`,
and `guardrails` at the top level of the payload dict.

### Error: `_assert_consume_denied` fails: expected 403, got 200

**Cause:** Role grant from a previous test was not cleaned up; bob/no-role user
accidentally already has `credential_consumer` for this credential ID.

**Fix:** `_verify_user_no_consume_role()` is called before every consume-deny
assertion in S2. If it fails, check tearDown cleanup logic and that test IDs are
unique (`p0-s2-c1`, `p0-s3-c1`, etc.).

### Error: `_assert_consume_denied` — expected code 'INSUFFICIENT_PERMISSIONS', got None

**Cause:** `get_cbauth_error_code()` failed to parse the nested error format.

**Expected format (CB 8.1):**
```json
{"error": {"code": "INSUFFICIENT_PERMISSIONS", "reason": "..."}}
```

**Fix (already applied):** `get_cbauth_error_code()` unwraps the nested dict before
extracting `code`.

### Error: `_verify_user_has_consume_role` fails: expected role not found

**Cause:** Role verification only checked bracket notation (`credential_consumer[id]`)
but server returned separate-field notation.

**Fix (already applied):** `_verify_user_has_consume_role()` checks both:
```python
r.get("role") == f"credential_consumer[{cred_id}]"
or (r.get("role") == "credential_consumer" and r.get("credential_id") == cred_id)
```

### Error: `[WARNINGS-BRANCH: n2n=ON] Expected empty warnings, got [...]`

**Cause:** Prior test left a credential in a bad state, or cluster has real encryption
gap despite n2n being configured.

**Explanation:** Warnings appear only when BOTH conditions are true:
1. Credentials exist in the store
2. A real encryption gap is present (n2n OFF + override active)

When n2n is ON, warnings should always be `[]` regardless of override state.

### Error: "PUT strict settings expected 200" (S5) or "Strict create should return 400"

**Cause:** Cluster does not support the `/settings/credentialStore` endpoint (not 8.1+
Enterprise), or n2n was silently ON.

**Fix:** Verify the cluster is 8.1+ Enterprise. `cluster_has_n2n` logic adapts
automatically to n2n=ON clusters — check `[STRICT-BRANCH: n2n=ON/OFF]` in logs.

### Error: "expiresAt=360s should return 201" (S5) but got 400

**Cause:** n2nEncryptionOverride was not re-enabled after the DELETE reset.

**Fix (already applied):** After `delete_store_settings()`, a `put_store_settings()`
re-enable call runs and logs a warning if it fails. Check that log line in CI output.

### Error: 401 on Node B consume in S6 (`consume_credential: status=401`)

**Cause:** The cbauth special password (`ns_config_auth:get_password(special)`) is
**per-node** — each ns_server generates its own local token. Using Node A's token
(fetched at setUp from the master) to call Node B's `/_cbauth/getCredential` returns 401.

**Fix (already applied):** S6 fetches Node B's password explicitly:
```python
node_b_cbq_password = self._fetch_cbq_engine_password_via_diag_eval(rest=node_b_rest)
```
and passes it to every Node B consume call via `service_password=node_b_cbq_password`.

### Error: "Checkpoint 2 PASSED" but "Checkpoint 3" gets 200 instead of 403

**Cause:** Credential has not expired yet — clock drift between test runner and
cluster, or the `expiresAt` value read from GET was larger than expected.

**Fix:** The subtest reads `expiresAt` from a GET response and asserts
`assertGreater(expires_at_ms, current_time_ms)` before waiting. The 8-second buffer
(2s remaining + 6s past) is usually sufficient. If the cluster clock is ahead of the
test runner, increase the buffer in `_run_expiry_enforcement_subtest`.

---

## Key Implementation Details

### Payload structure (POST/PUT)

```python
# Top-level fields — NOT nested under "meta"
{
    "type": "aws",
    "fields": {
        "accessKeyId": "AKIAIOSFODNN7EXAMPLE",
        "secretAccessKey": "<secret>",
        "region": "us-east-1"
    },
    "description": "My credential",          # top-level
    "expiresAt": 1749600000000,              # ms epoch, top-level
    "guardrails": {"allowedServices": ["n1ql"]}  # top-level
}

# GET response includes these under "meta" — only the server puts them there
{
    "id": "my-cred",
    "type": "aws",
    "fields": {...},
    "meta": {
        "description": "My credential",
        "expiresAt": 1749600000000,
        "guardrails": {"allowedServices": ["n1ql"]}
    }
}
```

### Store settings structure

```python
# PUT /settings/credentialStore — both fields are always required
{
    "configEncryptionOverride": False,   # override config-encryption prerequisite
    "n2nEncryptionOverride": True        # override n2n-encryption prerequisite
}

# GET /settings/credentialStore response
{
    "configEncryptionOverride": False,
    "n2nEncryptionOverride": True,
    "warnings": ["n2n encryption is not enabled"]
}
```

### Expiry enforcement flow

```
1. Build payload with expiresAt = int((time.time() + 360) * 1000)
   + allowed_services=["n1ql"]  ← required for Pattern A consume path
2. POST /settings/credentials/:id  → 201
3. Grant alice credential_consumer[cred_id]  → 200
4. GET credential, read expiresAt from response
   assertIsNotNone(expires_at_ms)
   assertGreater(expires_at_ms, current_time_ms)
5. Checkpoint 1: consume immediately  → 200
6. sleep until expiresAt − 2s
7. Checkpoint 2: consume at boundary  → 200  (proves no early expiry)
8. sleep(8)  # 2s remaining + 6s past expiry
9. Checkpoint 3: consume at expiresAt + 6s  → 403 CREDENTIAL_EXPIRED
```

---

## Dependencies

### Internal

| File | Role |
|------|------|
| `couchbase_utils/security_utils/credential_store_utils.py` | All credential store utilities + all endpoint/error constants |
| `couchbase_utils/cb_server_rest_util/security/credential_store.py` | Low-level REST client (`/settings/credentials`, `/settings/credentialStore`, `/settings/rbac/services`) |
| `pytests/ns_server/credential_store_base.py` | Test base class: setUp/tearDown/all helpers |
| `pytests/onPrem_basetestcase.py::ClusterSetup` | TAF multi-node test infrastructure |
| `lib/membase/api/rest_client.py` | Legacy transport (`_http_request`, `baseUrl`) — do not add constants here |

---

## Adding New Tests

1. Add `test_*` method to `CredentialStoreTest` in `credential_store_test.py` — no helpers, no setUp changes needed
2. Use helpers from `CredentialStoreBase` (inherited):
   - `_create_tracked_credential(cred_id, payload)` — create + auto-cleanup on tearDown
   - `_require_cbq_password()` — fail fast if consume path is unavailable
   - `_consume_as_cbq_on_behalf_of(cred_id, user, domain)` — Pattern A consume call (master)
   - `_consume_as_cbq_service_identity(cred_id)` — Pattern B consume call (master)
   - `_assert_consume_allowed(status, body, context)` — pins 200
   - `_assert_consume_denied(status, body, ERROR_CODE, context)` — pins 403 + error code
   - `_assert_consume_has_secret(body, cred_id, expected_secret)` — plaintext secret check
   - `_get_credential_guardrails(cred_id)` — GET + extract guardrails dict
   - `_verify_user_has_consume_role(username, cred_id)` — role persistence check
   - `_set_user_roles(username, roles_str)` — direct role set as Full Admin (wildcards OK; ""=clear)
   - `_grant_service_roles(service, roles)` — PUT + track for tearDown cleanup
   - **Multi-node tests** (`nodes_init>=2`):
     - `_non_master_node()` — returns first non-master server by IP
     - `_poll_until(label, fn, timeout_s, interval_s)` — bounded poll before cross-node assertions
     - `_fetch_cbq_engine_password_via_diag_eval(rest=node_rest)` — per-node cbauth token
     - `_consume_as_cbq_on_behalf_of_on_node(base_url, cred_id, user, domain, service_password)` — Pattern A to specific node
     - `_node_base_url(server)` — build `scheme://ip:port` for any server object
   - **Node lifecycle tests** (`nodes_init>=2`, SSH required):
     - `_stop_couchbase_on_node(server)` — SSH stop; always disconnects
     - `_start_couchbase_on_node(server, timeout_s=120)` — SSH start + poll healthy; self.fail() on timeout
3. Use `ENDPOINT_*` constants from `credential_store_utils` for endpoint strings
4. Use `ERROR_*` constants for cbauth error code assertions
5. Add entry to `conf/ns_server/py-credential_store_test.conf`
6. If the test needs n1ql (consume path), use `services_init=kv:n1ql`

### Example: New Test

```python
from couchbase_utils.security_utils.credential_store_utils import (
    ERROR_INSUFFICIENT_PERMISSIONS,
)
from pytests.ns_server.credential_store_base import CredentialStoreBase


class CredentialStoreTest(CredentialStoreBase):

    def test_http_credential_consume(self):
        """Consume an http-type credential via Pattern A."""
        self._require_cbq_password()

        cred_id = "p0-http-c1"
        known_secret = "HTTP_TOKEN_VALUE"
        payload = {
            "type": "http",
            "fields": {"token": known_secret},
            "guardrails": {"allowedServices": ["n1ql"]},
        }

        status, _ = self._create_tracked_credential(cred_id, payload)
        self.assertEqual(int(status) if status else 0, 201)

        status_grant, _ = self.cs_utils.grant_consume_to_local_user(
            self.rest, self.ALICE_USER, cred_id
        )
        self.assertEqual(int(status_grant) if status_grant else 0, 200)

        status_c, body_c = self._consume_as_cbq_on_behalf_of(cred_id, self.ALICE_USER, "local")
        self._assert_consume_allowed(status_c, body_c, context="http credential consume")
        # http type: sensitive field is "token", not "secretAccessKey"
        self.assertIsNotNone(body_c)
        if isinstance(body_c, dict):
            self.assertEqual(body_c.get("fields", {}).get("token"), known_secret)
```
