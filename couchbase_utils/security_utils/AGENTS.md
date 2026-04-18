---
name: security-utils-agent
description: >
  Utilities for security operations including TLS certificates, JWT tokens,
  audit logging, and encryption management.
model: inherit
---

# security_utils

**Security utilities for TLS, JWT, and audit operations.**
Provides helpers for certificate management, JWT authentication, and audit configuration.

## Files

| File | Purpose |
|---|---|
| `x509main.py` | x509main for TLS certificate operations |
| `x509_multiple_CA_util.py` | Multiple CA certificate utilities |
| `jwt_utils.py` | JWTUtils for JWT token operations |
| `audit_ready_functions.py` | Audit utilities for audit logging |
| `security_utils.py` | General security utilities |

## Key Classes

### x509main
TLS certificate generation and management.

**Capabilities:**
- Generate CA and node certificates
- Client certificate authentication
- Chain certificate creation
- Certificate deployment

**Usage:**
```python
from couchbase_utils.security_utils.x509main import x509main

cert_util = x509main(host=server)
cert_util.generate_certificates()
```

### JWTUtils
JWT token generation and validation.

**Capabilities:**
- Generate RSA/ECDSA key pairs
- Create signed JWT tokens
- Configure JWT authentication
- Validate tokens against endpoints

**Usage:**
```python
from couchbase_utils.security_utils.jwt_utils import JWTUtils

jwt_util = JWTUtils(log=logger)
private_key, public_key = jwt_util.generate_key_pair("RS256")
token = jwt_util.create_jwt_token(issuer, subject, audience, private_key)
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
