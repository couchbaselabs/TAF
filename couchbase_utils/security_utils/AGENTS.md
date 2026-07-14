---
name: security-utils-agent
description: >
  Utilities for security operations including TLS certificates, JWT tokens,
  audit logging, and encryption management.
model: inherit
---

# security_utils

- Security utilities: TLS/certs, JWT, audit, Credential Store.

## Files

| File | Purpose |
|---|---|
| `x509main.py` | x509main for TLS certificate operations |
| `x509_multiple_CA_util.py` | Multiple CA certificate utilities |
| `jwt_utils.py` | JWTUtils for JWT token operations |
| `audit_ready_functions.py` | Audit utilities for audit logging |
| `security_utils.py` | General security utilities |
| `credential_store_utils.py` | CredentialStoreUtils for Credential Store REST API (`/settings/credentials/*`) |

## x509main

TLS certificate generation and management.
- Generate CA and node certificates
- Client certificate authentication
- Chain certificate creation
- Certificate deployment

```python
from couchbase_utils.security_utils.x509main import x509main

cert_util = x509main(host=server)
cert_util.generate_certificates()
```

## JWTUtils

JWT token generation and validation.
- Generate RSA/ECDSA key pairs
- Create signed JWT tokens
- Configure JWT authentication
- Validate tokens against endpoints

```python
from couchbase_utils.security_utils.jwt_utils import JWTUtils

jwt_util = JWTUtils(log=logger)
private_key, public_key = jwt_util.generate_key_pair("RS256")
token = jwt_util.create_jwt_token(issuer, subject, audience, private_key)
```

## CredentialStoreUtils

- Helpers for Credential Store tests: `/settings/credentials`, `/settings/credentialStore`, `/settings/rbac/services/*/roles`, `/settings/rbac/users/local/*`, cbauth consume path.
- Every public method takes `rest_connection` as first arg; falls back from `CredentialStoreAPI` to raw `_http_request` if API class can't init.
- Secrets never stored on the object — pass explicitly per call.

**Payload builders** (`@staticmethod`, return dict for `POST`/`PUT /settings/credentials/:id`; share optional `description`/`allowed_services`/`expires_at_ms` kwargs → top-level `description`/`expiresAt`/`guardrails.allowedServices`):

| Method | Credential type | Fields built |
|---|---|---|
| `build_aws_payload(access_key_id, secret_access_key, region, ...)` | `aws` | `accessKeyId`, `secretAccessKey`, `region`, optional `sessionToken`/`endpoint` |
| `build_azure_payload(account_name, account_key, endpoint, ...)` | `azureShared` | `accountName`, `accountKey`, `endpoint` |
| `build_gcp_service_account_payload(json_credentials, region, ...)` | `gcp` | `jsonCredentials`, `region` |

- `build_gcp_service_account_payload` takes **raw GCP service-account key JSON file contents as a string** (not a file path, not parsed) — mirrors `jq -Rs '{type:"gcp",fields:{jsonCredentials:.,region:"us"}}' file`. Caller reads file (`open(path).read()`) and passes text in; builder does no file I/O.
- `SENSITIVE_FIELDS_BY_TYPE` (module dict) — fields that must never appear as plaintext in admin responses per credential type; used by `assert_secrets_redacted()`.

```python
from couchbase_utils.security_utils.credential_store_utils import CredentialStoreUtils

cs_util = CredentialStoreUtils(log=logger)
payload = cs_util.build_azure_payload("myaccount", "mykey", "https://myaccount.blob.core.windows.net")
status, content = cs_util.create_credential(rest, "azure-cred-1", payload)

with open("/path/to/gcp_key.json") as f:
    gcp_payload = cs_util.build_gcp_service_account_payload(f.read(), region="us")
status, content = cs_util.create_credential(rest, "gcp-cred-1", gcp_payload)
```

## Supported Algorithms

| Type | Algorithms |
|---|---|
| RSA | RS256, RS384, RS512, PS256, PS384, PS512 |
| ECDSA | ES256, ES384, ES512, ES256K |

## Dependencies

- `cb_server_rest_util.security.security_api.SecurityRestAPI`
- `cryptography` library for key generation
- `jwt` library for token operations
