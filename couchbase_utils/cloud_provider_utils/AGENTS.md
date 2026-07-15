---
name: cloud-provider-utils-agent
description: >
  Cloud provider abstraction for backup/restore tooling. Each provider reads
  its own credentials from env vars and exposes cbbackupmgr/cbcontbk CLI
  flags plus an object-store cleanup helper.
model: inherit
---

# cloud_provider_utils

- Cloud provider abstraction for cbbackupmgr/cbcontbk backup-restore tests.
- One interface, 4 providers: AWS, Azure, GCP, Localstack.
- Each provider: reads own env-var credentials, formats itself as CLI flags, cleans up its object-store location, builds/creates its Credential Store payload.

## Files

| File | Purpose |
|---|---|
| `cloud_provider_interface.py` | `CloudProviderInterface` — ABC all providers implement |
| `aws_provider.py` | `AWSProvider` — AWS S3 |
| `azure_provider.py` | `AzureProvider` — Azure Blob Storage |
| `gcp_provider.py` | `GCPProvider` — Google Cloud Storage |
| `localstack_provider.py` | `LocalstackProvider` — Localstack (S3-compatible, local) |

## Interface

`CloudProviderInterface` (`abc.ABCMeta`-style, matches `lib/SecurityLib/user_base_abc.py`), 5 abstract methods:

| Method | Purpose |
|---|---|
| `__init__()` | Read provider-specific credentials from env vars |
| `get_cbbackupmgr_flags(shell=None)` | Return `cbbackupmgr` object-store flags string |
| `get_cbconbk_flags(shell=None)` | Return `cbcontbk` object-store flags string (identical to `get_cbbackupmgr_flags()` for every provider today) |
| `cleanup_for_bkrs(location)` | Clean up object-store location used by a backup-restore test run |
| `create_credential_store(rest, cred_id, ...)` | Build provider's Credential Store payload, `POST /settings/credentials/:id` via `CredentialStoreUtils` |

Facts:
- `__metaclass__ = ABCMeta` is Python-2 idiom — does **not** enforce abstractness under Python 3 (needs `class Foo(metaclass=ABCMeta):`). Pre-existing repo-wide pattern, kept as-is. All 4 providers implement all 5 methods for real; only matters for a hypothetical future provider that skips one.
- `create_credential_store()` is deliberately per-provider, NOT a shared dispatcher on the interface (a common `isinstance`/duck-typing dispatcher was tried and rejected — defeats per-provider polymorphism).
- All providers call cloud SDKs directly (`boto3`, `azure.storage.blob`, `google.cloud.storage`) — do NOT reuse `lib/awsLib/S3.py`, `lib/azureLib/Azure.py`, `lib/gcs.py` (deliberate, keeps package self-contained).
- `shell` param on `get_cbbackupmgr_flags`/`get_cbconbk_flags`: the shell connection to the node cbbackupmgr/cbcontbk will actually run on (`CbBackupMgr`/`CbContBk` pass `self.shellConn`). AWS/Azure/Localstack ignore it — their flags are inline key/secret strings. GCPProvider uses it to stage its local auth file onto that node first (see below); passing `shell=None` makes it fall back to the local path, which only works if that path happens to also exist on the target node.

## AWSProvider

| Env var | Meaning |
|---|---|
| `AWS_REGION` | Region |
| `AWS_ACCESS_KEY_ID` | Access key ID |
| `AWS_SECRET_ACCESS_KEY` | Secret access key |

- `get_cbbackupmgr_flags()`/`get_cbconbk_flags()`: `--obj-region <AWS_REGION> --obj-access-key-id <AWS_ACCESS_KEY_ID> --obj-secret-access-key <AWS_SECRET_ACCESS_KEY>`
- `cleanup_for_bkrs(s3_path)` — `s3://bucket-name/some-dir`; `boto3.resource("s3")` deletes objects with key prefix `some-dir/` (real prefix delete, bucket untouched).
- `create_credential_store(...)` — `CredentialStoreUtils.build_aws_payload()` from `aws_access_key_id`/`aws_secret_access_key`/`aws_region` → `create_credential(...)`.

## AzureProvider

| Env var | Meaning |
|---|---|
| `AZURE_STORAGE_ACCOUNT` | Storage account name |
| `AZURE_STORAGE_KEY` | Storage account key |
| `AZURE_REGION` | Region |
| `AZURE_ENDPOINT` | Blob endpoint (falls back to `https://<account>.blob.core.windows.net`) |

- Flags: `--obj-region <AZURE_REGION> --obj-endpoint <AZURE_ENDPOINT> --obj-access-key-id <AZURE_STORAGE_ACCOUNT> --obj-secret-access-key <AZURE_STORAGE_KEY>`
- `cleanup_for_bkrs(azure_path)` — `az://container-name/some-dir`; `BlobServiceClient` deletes blobs with name prefix `some-dir/` (container untouched).
- `create_credential_store(...)` — `build_azure_payload()` from `azure_storage_account`/`azure_storage_key`/`azure_endpoint` → `create_credential(...)`.

## GCPProvider

| Env var | Meaning |
|---|---|
| `GOOGLE_APPLICATION_CREDENTIALS` | Path to GCP service-account key JSON (standard GCP SDK var), read on the test controller |
| `GCP_REGION` | Region |

- Flags: `--obj-auth-file <path> --obj-region <GCP_REGION>`, where `<path>` is `REMOTE_AUTH_FILE_PATH` (`/tmp/tmp_gcp_service_account.json`) once a `shell` is passed to `get_cbbackupmgr_flags`/`get_cbconbk_flags` — cbbackupmgr/cbcontbk run on a cluster node, not the controller, so the raw `GOOGLE_APPLICATION_CREDENTIALS` path wouldn't resolve there. `_stage_remote_auth_file()` SFTPs the key file (`shell.copy_file_local_to_remote`) to that fixed path the first time a given `shell.ip` is seen, tracked in `_remote_auth_file_staged_hosts`, then reuses it on subsequent calls to the same node. No `shell` → falls back to the local `gcp_service_account_key_file` path.
- `cleanup_for_bkrs(gcs_path)` — `gs://bucket-name/some-dir`; loads service-account JSON (`json.load`) from the **local** `gcp_service_account_key_file` (this runs on the controller via the GCP SDK, not over `shell`), `google.cloud.storage.Client.from_service_account_info(...)` deletes blobs with prefix `some-dir/` (bucket untouched).
- `create_credential_store(...)` — reads `gcp_service_account_key_file` **raw** (whole file as one string, NOT `json.load`-parsed — different from `cleanup_for_bkrs`), builds `build_gcp_service_account_payload(json_credentials, gcp_region, ...)` → `create_credential(...)`.

## LocalstackProvider

| Env var | Meaning |
|---|---|
| `LOCALSTACK_REGION` | Region |
| `LOCALSTACK_ACCESS_KEY_ID` | Access key ID |
| `LOCALSTACK_SECRET_ACCESS_KEY` | Secret access key |
| `LOCALSTACK_ENDPOINT` | Localstack endpoint URL |

- Flags: `--obj-region <LOCALSTACK_REGION> --obj-access-key-id <LOCALSTACK_ACCESS_KEY_ID> --obj-secret-access-key <LOCALSTACK_SECRET_ACCESS_KEY> --obj-endpoint <LOCALSTACK_ENDPOINT> --s3-force-path-style`
- `cleanup_for_bkrs(s3_path)` — **NOT a prefix delete** (deliberate hack, cheap/local buckets):
  - Bucket doesn't exist → create it; if sub-dir given, add zero-byte placeholder object `some-dir/`.
  - Bucket exists → empty + delete whole bucket.
- `create_credential_store(...)` — reuses `"aws"` type (S3-compatible) via `build_aws_payload()` from `localstack_access_key_id`/`localstack_secret_access_key`/`localstack_region`, with `endpoint=localstack_endpoint` → `create_credential(...)`.

## Credential Store integration

- `create_credential_store(rest, cred_id, username=None, password=None, description=None, allowed_services=None, expires_at_ms=None)` per provider:
  1. Builds type-specific payload via matching `CredentialStoreUtils.build_*_payload()` (see [security_utils/AGENTS.md](../security_utils/AGENTS.md)).
  2. Calls `CredentialStoreUtils().create_credential(rest, cred_id, payload, username=username, password=password)` — same pattern as `pytests/ns_server/credential_store_base.py`.
  3. Returns `(status_code, content)`.
- `rest` = `RestConnection`/`CBRestConnection`-shaped object, NOT a shell connection — unrelated to `shell_conn` used by `CbBackupMgr`/`CbContBk`.

```python
from couchbase_utils.cloud_provider_utils.aws_provider import AWSProvider

provider = AWSProvider()  # reads AWS_REGION / AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
flags = provider.get_cbbackupmgr_flags()
provider.cleanup_for_bkrs("s3://my-bucket/backup-restore-dir")
status, content = provider.create_credential_store(rest, "aws-cred-1")
```

## Dependencies

- `boto3` (AWS, Localstack)
- `azure-storage-blob` / `azure-core` (Azure)
- `google-cloud-storage` (GCP)
- `couchbase_utils.security_utils.credential_store_utils.CredentialStoreUtils` — used by every provider's `create_credential_store()`

SDK packages above already in `requirements.txt`.
