---
name: jwt-oidc-test-agent
description: >
  OIDC/JWT authentication tests for Couchbase Server with Keycloak IdP.
  Covers token auth, RBAC, key rotation, cluster consistency, and security validation.
model: inherit
---

# JWT OIDC Test Suite

**Test file:** `pytests/security/jwt_oidc_test.py`
**Base class:** `pytests/security/jwt_oidc_base.py`
**Config file:** `conf/security/py-jwt_oidc_test.conf`
**Utility file:** `couchbase_utils/security_utils/jwt_utils.py`
**Low-level REST:** `couchbase_utils/cb_server_rest_util/security/jwt.py`

## Purpose

Validates Couchbase Server's OIDC/JWT authentication with external Identity Provider (Keycloak).
Tests cover the full auth lifecycle: config, token acquisition, identity validation, RBAC enforcement,
key rotation, and cluster-wide consistency.

---

## Architecture

Four-layer design. Each layer has a single responsibility and no upward dependency.

```
Layer 1 — Raw REST client (transport only)
┌─────────────────────────────────────────────────────────────────────────┐
│  cb_server_rest_util/security/jwt.py :: JWTAPI(CBRestConnection)        │
│    Config   put_jwt_config()  get_jwt_config()  disable_jwt()           │
│    OIDC     request_with_bearer(token, endpoint, method, params)        │
│             get_whoami_with_bearer(token)                               │
│    Owns all JWT/OIDC Couchbase paths internally.                        │
│    No jwt_utils dependency (no circular imports).                       │
└─────────────────────────────────────────────────────────────────────────┘
          │  used by
          ▼
Layer 2 — JWT utility + all constants (no unittest dependency)
┌─────────────────────────────────────────────────────────────────────────┐
│  couchbase_utils/security_utils/jwt_utils.py                            │
├─────────────────────────────────────────────────────────────────────────┤
│  Module-level constants (single source of truth for all paths)          │
│                                                                         │
│  Couchbase endpoint constants:                                          │
│    ENDPOINT_WHOAMI            /whoami                                   │
│    ENDPOINT_POOLS_DEFAULT     /pools/default                            │
│    ENDPOINT_POOLS_BUCKETS     /pools/default/buckets                    │
│    ENDPOINT_SETTINGS_JWT      /settings/jwt                             │
│    ENDPOINT_OIDC_AUTH         /oidc/auth                                │
│    ENDPOINT_SETTINGS_STATS    /settings/stats                           │
│    ENDPOINT_SETTINGS_WEB      /settings/web                             │
│                                                                         │
│  Keycloak path templates (.format(realm=...) before use):               │
│    KC_PATH_REALM              /realms/{realm}                           │
│    KC_PATH_TOKEN              /realms/{realm}/protocol/openid-connect/token  │
│    KC_PATH_JWKS               /realms/{realm}/protocol/openid-connect/certs  │
│    KC_PATH_DISCOVERY          /realms/{realm}/.well-known/openid-configuration │
│    KC_PATH_ADMIN_TOKEN        /realms/master/protocol/openid-connect/token    │
│    KC_PATH_ADMIN_KEYS         /admin/realms/{realm}/keys                │
│    KC_PATH_ADMIN_COMPONENTS   /admin/realms/{realm}/components          │
│    KC_PATH_ADMIN_COMPONENT    /admin/realms/{realm}/components/{component_id} │
│                                                                         │
│  JWTUtils class:                                                        │
│    Config       get_oidc_jwt_config()  put_jwt_config()                 │
│                 get_jwt_config_settings()  disable_jwt()                │
│                 parse_jwt_config_content()  wait_jwt_config_propagation() │
│    Token        get_token_from_keycloak()  get_payload_from_token()     │
│                 get_kid_from_token()  verify_token_whoami_rest()        │
│                 request_with_jwt()  get_user_info_from_whoami()         │
│    Keycloak     get_keycloak_admin_token()  get_keycloak_jwks()         │
│    Admin API    rotate_keycloak_signing_key()  get_keycloak_realm_keys() │
│                 verify_keycloak_connectivity()                          │
│    Assertions   assert_external_identity()  assert_role_present()       │
│                 assert_success_status()  assert_unauthorized_status()   │
│    Logging      log_oidc_config_safe()  (redacts clientSecret)         │
└─────────────────────────────────────────────────────────────────────────┘
          │  used by
          ▼
Layer 3 — Test base class (framework helpers, setUp/tearDown)
┌─────────────────────────────────────────────────────────────────────────┐
│  pytests/security/jwt_oidc_base.py :: JWTOIDCBase(ClusterSetup)        │
├─────────────────────────────────────────────────────────────────────────┤
│  setUp()                                                                │
│    ├── Keycloak params (ip, port, realm, client, algorithm)            │
│    ├── OIDC settings (roles_claim, sub_claim, aud_claim, jit)         │
│    ├── jwt_utils.JWTUtils(log=self.log) instance                       │
│    ├── RestConnection to cluster master                                │
│    └── _ensure_keycloak_running() (once per class via class flag)     │
│  tearDown()  →  jwt_utils.disable_jwt()                                │
│                                                                         │
│  Secret resolution                                                      │
│    _secret(param, env)          env var → -p param → fail with msg     │
│    _user_password(param, env)   per-user env → KEYCLOAK_TEST_USER_PASSWORD │
│    _load_realm_params(prefix)   ttl / rotation realm config bundle     │
│                                                                         │
│  Infrastructure                                                         │
│    _ensure_keycloak_running()   SSH start via docker/compose if down   │
│                                                                         │
│  OIDC config helpers                                                    │
│    _get_oidc_jwt_config(**overrides)    build config with test defaults │
│    _enable_oidc_config(**overrides)     PUT config + sleep             │
│    _get_parsed_jwt_config(rest)         GET + parse /settings/jwt      │
│    _enable_oidc_and_verify(jit=...)     enable + assert config fields  │
│                                                                         │
│  Token helpers                                                          │
│    _get_token_from_keycloak(user, pass) OAuth2 password grant          │
│    _verify_keycloak_connectivity()      fail test if unreachable       │
│                                                                         │
│  RBAC helpers                                                           │
│    _ensure_external_user(username, roles)                               │
│    _wait_jwt_config_on_all_nodes(nodes, config)                        │
│                                                                         │
│  Assertion helpers                                                      │
│    _assert_admin_authz(token)           identity + role + read + write │
│    _assert_node_jwt_ok(node_rest, ...)  per-node config + auth + authz │
│    _validate_redirect_location(loc)     scheme, host, OIDC params      │
└─────────────────────────────────────────────────────────────────────────┘
          │  inherits
          ▼
Layer 4 — Tests only
┌─────────────────────────────────────────────────────────────────────────┐
│  pytests/security/jwt_oidc_test.py :: JWTOIDCTest(JWTOIDCBase)         │
│    test_* methods only — no setUp/tearDown/helpers                      │
│    Uses jwt_utils.ENDPOINT_* constants for any inline endpoint strings  │
└─────────────────────────────────────────────────────────────────────────┘
```

### Endpoint constant usage pattern

```python
# With request_with_jwt() — strips leading slash automatically:
ok, status, _ = self.jwt_utils.request_with_jwt(
    rest, token, jwt_utils.ENDPOINT_POOLS_DEFAULT, method="GET"
)

# With direct _http_request + baseUrl (baseUrl already ends with "/"):
api = f"{self.rest.baseUrl}{jwt_utils.ENDPOINT_POOLS_DEFAULT.lstrip('/')}"
ok, _, _ = self.rest._http_request(api, "GET")

# Keycloak URL construction (in jwt_utils.py methods):
token_url = f"{scheme}://{ip}:{port}{KC_PATH_TOKEN.format(realm=realm)}"
keys_url  = f"{scheme}://{ip}:{port}{KC_PATH_ADMIN_KEYS.format(realm=realm)}"
```

### Why not `lib/membase/api/rest_client.py`?

`rest_client.py` is TAF's legacy 4213-line Couchbase REST client. It has all paths hardcoded inline
(`self.baseUrl + 'pools/default'`) and no JWT knowledge. It provides the transport primitives
(`_http_request`, `baseUrl`, `_create_capi_headers`) that `JWTUtils` receives as a parameter.
The team is migrating to `cb_server_rest_util/`. Do not add JWT constants there.

---

## Keycloak Lab Instance

| Property | Value |
|----------|-------|
| **IP** | `172.23.106.226` |
| **Port** | `8444` (HTTPS) |
| **Admin** | `admin` / (see `KEYCLOAK_ADMIN_PASSWORD`) |
| **TLS** | Self-signed cert (`verify=False`) |
| **SSH** | `root` / (see `KEYCLOAK_SSH_PASSWORD`) — only needed if auto-restart is triggered |

### Realms

| Realm | Client ID | Purpose | Algorithm |
|-------|-----------|---------|-----------|
| `cb` | `test-client` | Default tests | RS384 |
| `cb-shortttl` | `test-client_ttl` | Token expiry (10s TTL) | RS256 |
| `cb-rotation` | `rotation-client` | JWKS key rotation | RS256 |

### Users

| Username | Realm(s) | Couchbase Role |
|----------|----------|----------------|
| `admin@localhost.com` | cb | admin |
| `adminttl@localhost.com` | cb-shortttl | admin |
| `rotation_user@localhost.com` | cb-rotation | admin |
| `combined_user@localhost.com` | cb | ro_admin |
| `unmapped_user@localhost.com` | cb | (none — RBAC denial test) |

### `test-client` Browser OIDC Settings

Bearer-token tests use Keycloak's token endpoint directly, but manual browser
authorization-code checks exercise Couchbase `/oidc/auth`. Couchbase redirects to
Keycloak with a JWT Secured Authorization Request (`request=<encrypted JWE>`), so
the `cb` realm client `test-client` must allow that request object.

In Keycloak UI:

1. Select realm `cb`
2. Open `Clients` → `test-client`
3. Open `Advanced`
4. Configure request object settings:

| Setting | Value |
|---------|-------|
| Request object signature algorithm | `RS384` |
| Request object encryption algorithm | `RSA-OAEP` |
| Request object content encryption algorithm | `A256GCM` |
| Request object required | `Not required` |
| Valid request URIs | empty |

The `cb` realm must also have an active encryption key exposed in JWKS with
`use=enc` and `alg=RSA-OAEP`.

Failure signature when this is missing:

```text
Browser OIDC flow result: success=False cookies=[] error=Keycloak login page failed: 400
step={'step': 'oidc_auth', 'status': 302}
step={'step': 'keycloak_login_page', 'status': 400}
```

Keycloak shows `Invalid Request` because it rejects Couchbase's encrypted `request=`
object before rendering the login form. Plain bearer-token tests can still pass in this
state because they do not use the browser authorization request object.

---

## Environment Variables (Secrets)

All secrets are injected via environment variables (Jenkins Credentials Plugin) or TAF `-p` params.
**Never hardcode secrets in test code or config files.**

| Variable | Description | Used By |
|----------|-------------|---------|
| `KEYCLOAK_IDP_IP` | Keycloak server IP (optional — defaults to lab IP) | All tests |
| `KEYCLOAK_IDP_CLIENT_SECRET` | cb realm client secret | Default tests |
| `KEYCLOAK_TTL_CLIENT_SECRET` | cb-shortttl realm secret | Expiry test |
| `KEYCLOAK_ROTATION_CLIENT_SECRET` | cb-rotation realm secret | Key rotation test |
| `KEYCLOAK_ADMIN_PASSWORD` | Keycloak admin password | Key rotation test |
| `KEYCLOAK_TEST_USER_PASSWORD` | Shared user password (fallback for all users) | All tests |
| `KEYCLOAK_SSH_PASSWORD` | SSH password for Keycloak host | Auto-restart only |

### Resolution Order

```python
def _secret(self, param_name, env_name):
    # 1. Check TAF -p parameter  (local runs)
    # 2. Check environment variable  (Jenkins injects these)
    # 3. Fail with clear error message including variable name
```

### Local Setup

```bash
export KEYCLOAK_IDP_IP="172.23.106.226"
export KEYCLOAK_IDP_CLIENT_SECRET="<secret>"
export KEYCLOAK_TTL_CLIENT_SECRET="<secret>"
export KEYCLOAK_ROTATION_CLIENT_SECRET="<secret>"
export KEYCLOAK_ADMIN_PASSWORD="<secret>"
export KEYCLOAK_TEST_USER_PASSWORD="password"
# KEYCLOAK_SSH_PASSWORD only needed if Keycloak may be down
```

### Jenkins Setup

```groovy
withCredentials([
    string(credentialsId: 'taf-keycloak-cb-client-secret',    variable: 'KEYCLOAK_IDP_CLIENT_SECRET'),
    string(credentialsId: 'taf-keycloak-ttl-client-secret',   variable: 'KEYCLOAK_TTL_CLIENT_SECRET'),
    string(credentialsId: 'taf-keycloak-rotation-secret',     variable: 'KEYCLOAK_ROTATION_CLIENT_SECRET'),
    string(credentialsId: 'taf-keycloak-admin-password',      variable: 'KEYCLOAK_ADMIN_PASSWORD'),
    string(credentialsId: 'taf-keycloak-test-user-password',  variable: 'KEYCLOAK_TEST_USER_PASSWORD'),
    string(credentialsId: 'taf-keycloak-ssh-password',        variable: 'KEYCLOAK_SSH_PASSWORD'),
]) {
    sh 'python testrunner.py -i node.ini -c conf/security/py-jwt_oidc_test.conf'
}
```

---

## Test Methods

| Test | Priority | Description |
|------|----------|-------------|
| `test_oidc_admin_login_with_keycloak_token` | P0 | Full auth flow: config, token, identity, authz |
| `test_oidc_redirect_flow` | P0 | Validate 302 redirect to Keycloak login |
| `test_oidc_external_readonly_user` | P0 | RBAC: readonly user can read, cannot write |
| `test_oidc_no_rbac_mapping` | P0 | Security: unmapped user denied (jit=False) |
| `test_oidc_jit_provisioning_allows_unmapped_user` | P0 | JIT auto-creates user from a clean RBAC state; no roles are granted without rolesClaim |
| `test_oidc_jit_with_roles_claim` | P0 | JIT + rolesClaim: expected token role is required, mapped into Couchbase, and grants authz without manual RBAC |
| `test_oidc_get_settings_masked` | P0 | Secrets masked in GET /settings/jwt |
| `test_oidc_invalid_config_rejected` | P0 | Invalid configs return 400, not 500 |
| `test_oidc_disable_preserves_local_admin_auth` | P0 | Disabling OIDC doesn't break local auth |
| `test_oidc_config_persists_after_restart` | P0 | Config survives ns_server restart |
| `test_oidc_expired_token_rejected` | P0 | Expired tokens return 401 |
| `test_oidc_jwks_key_rotation` | P0 | Keycloak key rotation via Admin API with config reapply |
| `test_oidc_cluster_wide_consistency` | P0 | Config propagates to all nodes + rebalance |
| `test_oidc_invalid_bearer_tokens_rejected` | P0 | Rejects 7 invalid token variants: tampered payload, alg=none, wrong issuer, wrong azp/aud, nbf in future, malformed, empty |
| `test_oidc_jwks_rotation_without_config_reapply` | P0 | CB auto-discovers new JWKS keys on unknown kid (no admin PUT required) |
| `test_oidc_callback_rejects_invalid_state` | P0 | /oidc/callback with random/missing state must not create a CB session (CSRF defense) |
| `test_oidc_query_service_with_bearer_token` | P0 | Bearer token auth on N1QL port 8093: admin allowed, unmapped user denied |

### Manual-Only Browser OIDC Checks

Do not run full browser session checks in Jenkins until CB-node -> Keycloak TCP
reachability and Keycloak request-object settings are guaranteed across the fleet.
Manual coverage should validate:

- Full browser SSO: `/oidc/auth` -> Keycloak -> `/oidc/callback` -> session -> `/whoami`
- Manual endpoint configuration without `oidcDiscoveryUri`
- RP-initiated logout through `/oidc/deauth`

### Pure JWT Backend Field Coverage

Additional UI/backend field coverage lives in `pytests/security/jwt_token_test.py`
and is run through `conf/security/py-jwt_auth_test.conf`:

| Test | Priority | Description |
|------|----------|-------------|
| `test_jwt_hmac_algorithms` | P0 | HS256/HS384/HS512 shared-secret signing, /whoami + /pools/default success, wrong-secret and algorithm-mismatch rejection |
| `test_jwt_nested_claim_paths` | P0 | Dot-notation extraction for subClaim, audClaim, and groupsClaim |
| `test_jwt_mapping_rules_with_capture_groups` | P0 | subMaps regex capture substitution, e.g. `(.*)@example.com cb-\1` |
| `test_jwt_custom_claims_validation` | P0 | customClaims validation for string pattern, number min/max, boolean const, and mandatory claims |

---

## Test Execution

```bash
# Run all P0 tests
python testrunner.py -i node.ini -c conf/security/py-jwt_oidc_test.conf

# Run single test
python testrunner.py -i node.ini \
    -t security.jwt_oidc_test.JWTOIDCTest.test_oidc_admin_login_with_keycloak_token,nodes_init=1

# Run cluster test (requires 3+ nodes in node.ini)
python testrunner.py -i node.ini \
    -t security.jwt_oidc_test.JWTOIDCTest.test_oidc_cluster_wide_consistency,nodes_init=3
```

---

## Common Errors & Fixes

### Error: "Missing secret 'keycloak_client_secret'"

**Cause:** Environment variable not set.

**Fix:**
```bash
export KEYCLOAK_IDP_CLIENT_SECRET="<your-secret>"
# or
python testrunner.py ... -p keycloak_client_secret=<secret>
```

### Error: "admin_password is required for get_keycloak_admin_token"

**Cause:** `KEYCLOAK_ADMIN_PASSWORD` not set. The default was removed — secrets must be explicit.

**Fix:**
```bash
export KEYCLOAK_ADMIN_PASSWORD="<admin-password>"
```

### Error: "Failed to get token from Keycloak"

**Cause:** Keycloak unreachable, wrong credentials, or realm/client doesn't exist.

**Debug:**
```bash
# Test connectivity
curl -k https://172.23.106.226:8444/realms/cb/.well-known/openid-configuration

# Test token endpoint
curl -k -X POST https://172.23.106.226:8444/realms/cb/protocol/openid-connect/token \
    -d "grant_type=password&client_id=test-client&client_secret=<secret>&username=admin@localhost.com&password=password"
```

**Fixes:**
- Verify `KEYCLOAK_IDP_IP` is correct
- Check Keycloak is running and port 8444 is open
- Verify realm `cb` and client `test-client` exist
- Check client secret matches Keycloak config

### Error: "Token issuer mismatch"

**Cause:** Token `iss` claim doesn't match configured issuer.

**Example:**
```
Expected: https://172.23.106.226:8444/realms/cb
Got: https://keycloak.example.com/realms/cb
```

**Fix:** Ensure Keycloak's "Frontend URL" setting matches what tests expect, or override via `-p keycloak_ip=...`

### Error: "Audience validation failed"

**Cause:** Keycloak uses `azp` (authorized party) not `aud` for client ID.

**Fix:** Test uses `aud_claim=azp` by default. If your Keycloak is configured differently:
```bash
python testrunner.py ... -p aud_claim=aud
```

### Error: "Need at least 2 signing keys for rotation testing"

**Cause:** `cb-rotation` realm doesn't have two RS256 signing keys.

**Fix in Keycloak Admin Console:**
1. Go to cb-rotation realm → Realm Settings → Keys → Providers
2. Add new "rsa-generated" provider named "rotation-key-2"
3. Set Algorithm = RS256
4. Set Priority = 50 (lower than default key's 100)

### Error: "Cluster-wide test requires at least 2 nodes"

**Cause:** Test skipped because `nodes_in_cluster < 2`.

**Fix:** Ensure `node.ini` has 2+ servers and use `nodes_init=2` or higher:
```bash
python testrunner.py -i node.ini \
    -t security.jwt_oidc_test.JWTOIDCTest.test_oidc_cluster_wide_consistency,nodes_init=3
```

### Error: "Key rotation cleanup failed"

**Cause:** Keycloak admin token expired during cleanup.

**Manual Fix:**
1. Open Keycloak Admin Console
2. Go to cb-rotation realm → Realm Settings → Keys → Providers
3. Set `rsa-generated` priority to 100 (or 200)
4. Set `rotation-key-2` priority to 50

### Error: SSL certificate verification failed

**Cause:** Keycloak uses self-signed cert.

**Fix:** Test defaults to `keycloak_tls_verify=False`. If you have a valid cert:
```bash
python testrunner.py ... -p keycloak_tls_verify=True
```

---

## Key Implementation Details

### OIDC Config Structure

```python
config = {
    "enabled": True,
    "issuers": [{
        "name": "https://172.23.106.226:8444/realms/cb",
        "signingAlgorithm": "RS384",
        "publicKeySource": "jwks_uri",
        "jwksUriTlsVerifyPeer": False,
        "subClaim": "preferred_username",
        "audClaim": "azp",
        "audienceHandling": "any",
        "audiences": ["test-client"],
        "jitProvisioning": False,
        "rolesClaim": "resource_access.test-client.roles",
        "oidcSettings": {
            "clientId": "test-client",
            "clientSecret": "<secret>",
            "baseRedirectUris": ["http://172.23.109.124:8091/"],
            "oidcDiscoveryUri": "https://172.23.106.226:8444/realms/cb/.well-known/openid-configuration",
            "scopes": ["openid", "profile", "email"],
            "pkceEnabled": True,
            "nonceValidation": True,
            "disablePushedAuthorizationRequests": True,
            "tlsVerifyPeer": False,
        }
    }]
}
```

### Token Flow

```
1. Test calls _get_token_from_keycloak(username, password)
2. jwt_utils POSTs to KC_PATH_TOKEN → Keycloak /protocol/openid-connect/token
3. Keycloak returns JWT signed with realm's active RS256/RS384 key
4. Test calls jwt_utils.verify_token_whoami_rest(rest, token)
5. Couchbase validates JWT:
   - Fetches JWKS via oidcDiscoveryUri → KC_PATH_JWKS
   - Verifies signature using kid from JWT header
   - Validates iss, aud (azp), exp claims
   - Extracts identity from subClaim (preferred_username)
6. Couchbase maps identity to external RBAC user
7. ENDPOINT_WHOAMI returns user info with roles
```

### Key Rotation Flow

```
1. Get Keycloak admin token via KC_PATH_ADMIN_TOKEN (realms/master)
2. Fetch current JWKS via KC_PATH_JWKS — verify 2+ signing keys exist
3. Get token BEFORE rotation — note kid1
4. Call rotate_keycloak_signing_key():
   - GET KC_PATH_ADMIN_KEYS → list key provider components
   - GET KC_PATH_ADMIN_COMPONENTS → find RSA provider component IDs
   - Swap priorities of two RS256 providers (true swap, not +100/-100)
   - PUT KC_PATH_ADMIN_COMPONENT for each provider
5. Get token AFTER rotation — note kid2
6. Assert kid1 != kid2 (rotation worked)
7. Re-apply Couchbase JWT config (forces JWKS refresh)
8. Verify NEW token (kid2) authenticates on ENDPOINT_WHOAMI
9. Verify OLD token (kid1) still works (both keys in JWKS)
10. Cleanup in finally block: rotate back to original key
```

### Cluster Consistency Flow

```
1. Enable OIDC config on master node
2. Poll all nodes via wait_jwt_config_propagation() until config matches master:
   - GET ENDPOINT_SETTINGS_JWT on each node
   - Compare issuer.name, audiences, subClaim, publicKeySource
3. Get JWT token from Keycloak
4. For each node (_assert_node_jwt_ok):
   - Verify config enabled
   - Verify token authenticates (ENDPOINT_WHOAMI)
   - Verify identity via assert_external_identity()
   - Verify authz (ENDPOINT_POOLS_DEFAULT)
5. Rebalance out one node, then back in
6. Poll for config propagation again
7. Repeat auth/authz validation on all nodes
```

---

## Dependencies

### External

- Keycloak instance at `172.23.106.226:8444`
- Network access to Keycloak from test runner and Couchbase nodes
- Realms/clients/users pre-configured (see Keycloak section above)

### Internal

| File | Role |
|------|------|
| `couchbase_utils/security_utils/jwt_utils.py` | All JWT/OIDC utilities + all endpoint constants |
| `couchbase_utils/cb_server_rest_util/security/jwt.py` | Low-level `/settings/jwt` REST client |
| `pytests/security/jwt_oidc_base.py` | Test base class: setUp/tearDown/all helpers |
| `pytests/onPrem_basetestcase.py::ClusterSetup` | TAF multi-node test infrastructure |
| `lib/membase/api/rest_client.py` | Legacy transport (`_http_request`, `baseUrl`) — do not add constants here |

---

## Adding New Tests

1. Add `test_*` method to `JWTOIDCTest` in `jwt_oidc_test.py` — **no helpers, no setUp changes needed**
2. Use helpers from `JWTOIDCBase` (inherited):
   - `_enable_oidc_config(**overrides)` — enable OIDC with test defaults
   - `_get_token_from_keycloak(username, password)` — OAuth2 password grant
   - `_assert_admin_authz(token)` — identity + role + read + write probe
   - `_ensure_external_user(username, roles)` — create RBAC entry
3. Use `jwt_utils.ENDPOINT_*` for any endpoint strings; use `jwt_utils.KC_PATH_*` for Keycloak URLs
4. Use `self.jwt_utils.assert_external_identity(whoami, username)` and `assert_role_present(whoami, role)` for identity assertions
5. Add test to `conf/security/py-jwt_oidc_test.conf`
6. If a new realm/client is needed, document it in the Keycloak section above

### Example: New Test

```python
def test_oidc_custom_claims(self):
    """Test custom claim extraction from JWT."""
    self._ensure_external_user(self.keycloak_username, roles="admin")
    self._enable_oidc_config(roles_claim="custom.roles.path")

    token = self._get_token_from_keycloak()
    claims = self.jwt_utils.get_payload_from_token(token)
    self.assertIn("custom", claims)

    whoami = self.jwt_utils.get_user_info_from_whoami(self.rest, token)
    self.jwt_utils.assert_external_identity(whoami, self.keycloak_username)

    # Use constants for all endpoint references
    ok, status, _ = self.jwt_utils.request_with_jwt(
        self.rest, token, jwt_utils.ENDPOINT_POOLS_DEFAULT, method="GET"
    )
    self.jwt_utils.assert_success_status(status, "Admin should access /pools/default")
```
