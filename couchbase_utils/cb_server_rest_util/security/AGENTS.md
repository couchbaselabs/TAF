---
name: cb-rest-security-agent
description: >
  REST API methods for Couchbase security — RBAC, certificates, auditing,
  JWT, encryption keys, and node addition restrictions in TAF.
model: inherit
---

# security/

**Composite class:** `SecurityRestAPI` (`security_api.py`)
**Base URL attribute:** `self.base_url`

## Files → Classes

| File | Class | Inherits |
|---|---|---|
| `security_api.py` | `SecurityRestAPI` | All below (composite) |
| `auditing.py` | `Auditing` | `CBRestConnection` |
| `certificate_management.py` | `CertificateMangementAPI` | `CBRestConnection` |
| `encryption_at_rest.py` | `EncryptionAtRest` | `CBRestConnection` |
| `jwt.py` | `JWTAPI` | `CBRestConnection` |
| `rbac_authorization.py` | `RbacAuthorization` | `CBRestConnection` |
| `restrict_node_addition.py` | `NodeInitAddition` | `CBRestConnection` |

## Method Inventory

### auditing.py — `Auditing`

No REST methods implemented yet — placeholder class.

### certificate_management.py — `CertificateMangementAPI`

| Method | Verb | Path |
|---|---|---|
| `get_trusted_root_certificates` | GET | `/pools/default/trustedCAs` |
| `get_node_certificates` | GET | `/pools/default/certificates` |
| `get_client_cert_auth_config` | GET | `/settings/clientCertAuth` |
| `set_client_cert_auth_config` | POST | `/settings/clientCertAuth` |
| `cleanup_client_cert_auth` | POST | `/settings/clientCertAuth` |
| `generate_client_cert_prefixes_json` | — | utility (no REST call) |

### encryption_at_rest.py — `EncryptionAtRest`

| Method | Verb | Path |
|---|---|---|
| `list_encryption_at_rest_keys` | GET | `/settings/encryptionKeys/` |
| `create_encryption_at_rest_key` | POST | `/settings/encryptionKeys` |
| `update_encryption_at_rest_key` | PUT | `/settings/encryptionKeys/{keyId}` |
| `test_encryption_at_rest_key` | POST | `/settings/encryptionKeys/{keyId}/test` |

### jwt.py — `JWTAPI`

| Method | Verb | Path |
|---|---|---|
| `put_jwt_config` | PUT | `/settings/jwt` |
| `get_jwt_config` | GET | `/settings/jwt` |
| `disable_jwt` | GET then PUT | `/settings/jwt` |

### rbac_authorization.py — `RbacAuthorization`

| Method | Verb | Path |
|---|---|---|
| `list_roles` | GET | `/settings/rbac/roles` |
| `list_current_users_and_roles` | GET | `/settings/rbac/users` |
| `check_permissions` | POST | `/pools/default/checkPermissions` |
| `create_local_user` | PUT | `/settings/rbac/users/local/{username}` |
| `update_local_user` | PATCH | `/settings/rbac/users/local/{username}` |
| `delete_local_user` | DELETE | `/settings/rbac/users/local/{username}` |
| `create_external_user` | PUT | `/settings/rbac/users/external/{username}` |
| `delete_external_user` | DELETE | `/settings/rbac/users/external/{username}` |
| `create_new_group` | PUT | `/settings/rbac/groups/{groupname}` |
| `delete_group` | DELETE | `/settings/rbac/groups/{groupname}` |

### restrict_node_addition.py — `NodeInitAddition`

| Method | Verb | Path |
|---|---|---|
| `set_internal_password_rotation_interval` | POST | `/settings/security` |

## No Business Logic — Pure Wrappers Only

Every method must be a single REST call that returns `(status, content)`.
**Forbidden in this directory:** response parsing (`content["key"]`), validation,
retries, polling loops, helper/utility functions, wrapping functions, `self.fail()`,
exceptions, or logging beyond a single debug line.
Put all such logic in the caller or a utility helper outside `cb_server_rest_util/`.

## HTTP Library

All HTTP calls must use `self.request()` inherited from `CBRestConnection` (which uses `requests` internally).
**Never** import `http.client`, `urllib.request`, `urllib3`, `httplib2`, `aiohttp`, `httpx`, or any other HTTP library.
If you encounter such an import, warn the user and replace it with `self.request()`.

## Notes

- `EncryptionAtRest` is not yet included in `SecurityRestAPI`'s inheritance chain — verify before using via `rest.security`.
- `disable_jwt` makes two sequential requests (GET then PUT) — this is a known pattern exception; do not split.
- `Auditing` class is a stub; REST methods for `/settings/audit` are not yet implemented.
