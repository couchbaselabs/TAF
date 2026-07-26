---
name: cloud-provider-utils-agent
description: >
  Cloud provider abstraction for backup/restore tooling. Each provider reads
  its own credentials from env vars and exposes cbbackupmgr/cbcontbk CLI
  flags, an object-store cleanup helper, and WORM object-store operations
  (list/read/overwrite/delete/versions/retention) for backup-restore and
  WORM immutability tests.
model: inherit
---

# cloud_provider_utils

One interface, **4 providers** — AWS, Azure, GCP, Localstack — backing cbbackupmgr/cbcontbk backup-restore tests and WORM immutability tests that poke the object store directly.

Each provider:
- Reads its own env-var credentials and validates them in `__init__`.
- Formats itself as cbbackupmgr/cbcontbk CLI flags.
- Cleans up its object-store location.
- Builds and creates its Credential Store payload.
- Implements the WORM object-store primitives (**AWS/Azure/GCP only** — not Localstack).

## Files

| File | Class | Backend |
|---|---|---|
| `cloud_provider_interface.py` | `CloudProviderInterface` | ABC all providers implement |
| `aws_provider.py` | `AWSProvider` | AWS S3 |
| `azure_provider.py` | `AzureProvider` | Azure Blob Storage |
| `gcp_provider.py` | `GCPProvider` | Google Cloud Storage |
| `localstack_provider.py` | `LocalstackProvider` | Localstack (S3-compatible, local) |

## Interface

`CloudProviderInterface` (`abc.ABCMeta`-style, matches `lib/SecurityLib/user_base_abc.py`). **6 abstract methods:**

| Method | Purpose |
|---|---|
| `__init__()` | Read provider credentials from env vars, then call `validate_credentials()` |
| `validate_credentials()` | Raise `CloudOperationError` if required credentials are missing/incomplete. Called from `__init__`, so misconfiguration fails at construction time |
| `get_cbbackupmgr_flags(shell=None)` | Return `cbbackupmgr` object-store flags string |
| `get_cbconbk_flags(shell=None)` | Return `cbcontbk` object-store flags string (identical to `get_cbbackupmgr_flags()` for every provider today) |
| `cleanup_for_bkrs(location)` | Clean up object-store location used by a backup-restore run |
| `create_credential_store(rest, cred_id, ...)` | Build provider payload, `POST /settings/credentials/:id` via `CredentialStoreUtils` |

### Constraints and gotchas

- **`__metaclass__ = ABCMeta` is a Python-2 idiom** — does NOT enforce abstractness under Python 3 (would need `class Foo(metaclass=ABCMeta):`). Kept as-is (repo-wide pattern). All 4 providers implement every method for real; enforcement only matters for a hypothetical future provider that skips one.
- **`create_credential_store()` is per-provider by design**, not a shared dispatcher. A common `isinstance`/duck-typing dispatcher was tried and rejected — it defeats per-provider polymorphism.
- **Providers call cloud SDKs directly** (`boto3`, `azure.storage.blob`, `google.cloud.storage`). Do NOT reuse `lib/awsLib/S3.py`, `lib/azureLib/Azure.py`, `lib/gcs.py` — deliberate, keeps the package self-contained.
- **`shell` param** on `get_cbbackupmgr_flags`/`get_cbconbk_flags` = the shell connection to the node cbbackupmgr/cbcontbk runs on (`CbBackupMgr`/`CbContBk` pass `self.shellConn`).
  - AWS/Azure/Localstack ignore it — flags are inline key/secret strings.
  - `GCPProvider` uses it to stage its auth file onto that node first (see GCP section). `shell=None` falls back to the local path, which only works if that path also exists on the target node.
- **`CloudOperationError`** (`cloud_provider_interface.py`) is the shared exception raised by every provider's `validate_credentials()`.

## WORM object-store operations

- Absorbed from a since-removed standalone `WormCloudHelper` (`pytests/backup_restore/WORM_backup/worm_cloud_helper.py`); its logic now lives here.
- `pytests/backup_restore/WORM_backup/*.py` calls these directly on `self.backup_cloud_provider` (built by `CollectionBase.setUp()` from `cbbackup_test`), via `WormBackupBase._require_cloud_helper()`/`_require_storage_provider()`.
- **Localstack does not implement these primitives** — WORM/object-lock semantics aren't meaningful there; no error since ABC enforcement is a no-op (see above).

**7 abstract primitives**, one real SDK call each, implemented in AWS/Azure/GCP:

| Method | Returns |
|---|---|
| `list_objects(archive_uri, repo_name, relative_prefix="")` | Object keys/blob names under `repo_name` |
| `object_exists(archive_uri, repo_name, relative_path)` | `bool` |
| `read_text(archive_uri, repo_name, relative_path)` | Object body as `str` |
| `get_retention_until(archive_uri, repo_name, relative_path)` | Unix timestamp, or `None` if unset |
| `attempt_overwrite(archive_uri, repo_name, relative_path, content="tampered")` | `(succeeded: bool, message: str)` |
| `delete_object(archive_uri, repo_name, relative_path)` | `(succeeded: bool, message: str)` |
| `list_object_versions(archive_uri, repo_name, relative_path)` | `[{version_id, is_latest, delete_marker}, ...]` |

Every method takes `archive_uri` (e.g. `s3://bucket/prefix`) **per call** rather than storing it on `self` — same convention as `cleanup_for_bkrs(location)`, since one provider instance is reused across archive locations in a run.

**Concrete helpers on `CloudProviderInterface`** — built only on the primitives above, so implemented once instead of per subclass:

| Method | Purpose |
|---|---|
| `_parse_location(archive_uri)` (static) | `{"bucket": <netloc>, "prefix": <path>}`; `AzureProvider` overrides to also accept the `https://<account>.blob.core.windows.net/...` form |
| `_object_path(prefix, repo_name, relative_path="")` (static) | Join prefix/repo/relative into one key |
| `_to_timestamp(value)` (static) | Normalize `int`/`float`/`datetime`/ISO-`str` → unix timestamp |
| `relative_path(archive_uri, repo_name, object_name)` | Strip the repo prefix off a full object key |
| `read_json(archive_uri, repo_name, relative_path)` | `json.loads(self.read_text(...))` |
| `upload_text(archive_uri, repo_name, relative_path, content)` | Alias for `attempt_overwrite(...)` — same body, different call-site intent |
| `find_backup_names(archive_uri, repo_name)` | Sorted top-level backup dir names under the repo |
| `find_latest_backup_name(archive_uri, repo_name)` | Last of `find_backup_names(...)`, or `None` |
| `find_relative_paths(archive_uri, repo_name, suffix=None, contains=None)` | Filtered relative paths |
| `find_metadata_path(archive_uri, repo_name, names)` | First relative path ending in any of `names` |
| `find_first_data_object(archive_uri, repo_name)` | First object that isn't `.worm`/`.status_flag`/`.obj_versions`/`plan.json`/a directory marker |
| `wait_for_objects(archive_uri, repo_name, timeout=300, relative_prefix="")` | Poll `list_objects(...)` every 10s until non-empty or timeout |

## AWSProvider

| Env var | Meaning |
|---|---|
| `AWS_REGION` | Region |
| `AWS_ACCESS_KEY_ID` | Access key ID |
| `AWS_SECRET_ACCESS_KEY` | Secret access key |

- **Flags:** `--obj-region <AWS_REGION> --obj-access-key-id <AWS_ACCESS_KEY_ID> --obj-secret-access-key <AWS_SECRET_ACCESS_KEY>`
- **`validate_credentials()`** — raises `CloudOperationError` if access key ID or secret is missing.
- **`cleanup_for_bkrs(s3_path)`** — `s3://bucket-name/some-dir`; `boto3.resource("s3")` deletes objects with key prefix `some-dir/` (real prefix delete, bucket untouched).
- **`create_credential_store(...)`** — `CredentialStoreUtils.build_aws_payload()` from `aws_access_key_id`/`aws_secret_access_key`/`aws_region` → `create_credential(...)`.
- **WORM ops** use a lazily-cached low-level `boto3.client("s3", ...)` (`self._s3_client` via `_client()`) — **distinct** from the `boto3.resource("s3")` created fresh inside `cleanup_for_bkrs()` each call.
  - `list_objects`/`list_object_versions` paginate via `ContinuationToken` / `KeyMarker`+`VersionIdMarker`.
  - `get_retention_until` tries `get_object_retention()` first, falls back to `head_object()`'s `ObjectLockRetainUntilDate`.

## AzureProvider

| Env var | Meaning |
|---|---|
| `AZURE_STORAGE_ACCOUNT` | Storage account name |
| `AZURE_STORAGE_KEY` | Storage account key |
| `AZURE_REGION` | Region |
| `AZURE_ENDPOINT` | Blob endpoint (falls back to `https://<account>.blob.core.windows.net`) |

- **Flags:** `--obj-region <AZURE_REGION> --obj-endpoint <AZURE_ENDPOINT> --obj-access-key-id <AZURE_STORAGE_ACCOUNT> --obj-secret-access-key <AZURE_STORAGE_KEY>`
- **`validate_credentials()`** — raises `CloudOperationError` if storage account or storage key is missing.
- **`cleanup_for_bkrs(azure_path)`** — `az://container-name/some-dir`; `BlobServiceClient` deletes blobs with name prefix `some-dir/` (container untouched).
- **`create_credential_store(...)`** — `build_azure_payload()` from `azure_storage_account`/`azure_storage_key`/`azure_endpoint` → `create_credential(...)`.
- **WORM ops** use a lazily-cached `BlobServiceClient` (`self._blob_service_client` via `_client()`) plus `_blob_client(container, key)`.
  - `get_retention_until` reads `blob_properties.immutability_policy.expiry_time` (falls back to `.expires_on`).
  - `list_object_versions` uses `list_blobs(include=["versions"])`.
- **Overrides `_parse_location()`** to also accept `https://<account>.blob.core.windows.net/<container>/<prefix>` (falls back to base `scheme://netloc/path` parsing for `az://`/`azblob://`).

## GCPProvider

| Env var | Meaning |
|---|---|
| `GOOGLE_APPLICATION_CREDENTIALS` | Path to GCP service-account key JSON (standard GCP SDK var), read on the test controller |
| `GCP_REGION` | Region |

- **`validate_credentials()`** — raises `CloudOperationError` if `gcp_service_account_key_file` is unset.
- **Flags:** `--obj-auth-file <path> --obj-region <GCP_REGION>`, where `<path>` is `REMOTE_AUTH_FILE_PATH` (`/tmp/tmp_gcp_service_account.json`) once a `shell` is passed.
  - cbbackupmgr/cbcontbk run on a cluster node, not the controller, so the raw `GOOGLE_APPLICATION_CREDENTIALS` path wouldn't resolve there.
  - `_stage_remote_auth_file()` SFTPs the key file (`shell.copy_file_local_to_remote`) to that fixed path the first time a given `shell.ip` is seen (tracked in `_remote_auth_file_staged_hosts`), then reuses it.
  - No `shell` → falls back to the local `gcp_service_account_key_file` path.
- **`cleanup_for_bkrs(gcs_path)`** — `gs://bucket-name/some-dir`; `json.load`s the service-account JSON from the **local** `gcp_service_account_key_file` (runs on the controller via the GCP SDK, not over `shell`), then `storage.Client.from_service_account_info(...)` deletes blobs with prefix `some-dir/` (bucket untouched).
- **`create_credential_store(...)`** — reads `gcp_service_account_key_file` **raw** (whole file as one string, NOT `json.load`-parsed — unlike `cleanup_for_bkrs`), builds `build_gcp_service_account_payload(json_credentials, gcp_region, ...)` → `create_credential(...)`.
- **WORM ops** use a lazily-cached `storage.Client` (`self._gcs_client` via `_client()` — loads/parses the local key file once, separate from `cleanup_for_bkrs()`'s own fresh load).
  - `get_retention_until` calls `blob.reload()` then reads `retention_expiration_time`.
  - `list_object_versions` needs `bucket.list_blobs(prefix=key, versions=True)` (GCS "versions" = object generations).

## LocalstackProvider

| Env var | Meaning |
|---|---|
| `LOCALSTACK_REGION` | Region |
| `LOCALSTACK_ACCESS_KEY_ID` | Access key ID |
| `LOCALSTACK_SECRET_ACCESS_KEY` | Secret access key |
| `LOCALSTACK_ENDPOINT` | Localstack endpoint URL |

- **Flags:** `--obj-region <LOCALSTACK_REGION> --obj-access-key-id <LOCALSTACK_ACCESS_KEY_ID> --obj-secret-access-key <LOCALSTACK_SECRET_ACCESS_KEY> --obj-endpoint <LOCALSTACK_ENDPOINT> --s3-force-path-style`
- **`validate_credentials()`** — raises `CloudOperationError` if access key ID, secret, or endpoint is missing.
- **`cleanup_for_bkrs(s3_path)`** — **NOT a prefix delete** (deliberate hack, cheap/local buckets):
  - Bucket missing → create it; if sub-dir given, add zero-byte placeholder object `some-dir/`.
  - Bucket exists → empty + delete the whole bucket.
- **`create_credential_store(...)`** — reuses the `"aws"` type (S3-compatible) via `build_aws_payload()` from `localstack_access_key_id`/`localstack_secret_access_key`/`localstack_region` with `endpoint=localstack_endpoint` → `create_credential(...)`.
- **Does NOT implement WORM primitives.**

## Credential Store integration

`create_credential_store(rest, cred_id, username=None, password=None, description=None, allowed_services=None, expires_at_ms=None)`, per provider:

1. Build the type-specific payload via the matching `CredentialStoreUtils.build_*_payload()` (see [security_utils/AGENTS.md](../security_utils/AGENTS.md)).
2. Call `CredentialStoreUtils().create_credential(rest, cred_id, payload, username=username, password=password)` — same pattern as `pytests/ns_server/credential_store_base.py`.
3. Return `(status_code, content)`.

**`rest`** = a `RestConnection`/`CBRestConnection`-shaped object, NOT a shell connection — unrelated to the `shell_conn` used by `CbBackupMgr`/`CbContBk`.

```python
from couchbase_utils.cloud_provider_utils.aws_provider import AWSProvider

# __init__ reads AWS_REGION / AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY,
# then validate_credentials() raises CloudOperationError if any are missing.
provider = AWSProvider()
flags = provider.get_cbbackupmgr_flags()
provider.cleanup_for_bkrs("s3://my-bucket/backup-restore-dir")
status, content = provider.create_credential_store(rest, "aws-cred-1")
```

## Dependencies

- `boto3` — AWS, Localstack
- `azure-storage-blob` / `azure-core` — Azure
- `google-cloud-storage` — GCP
- `couchbase_utils.security_utils.credential_store_utils.CredentialStoreUtils` — every provider's `create_credential_store()`

All SDK packages above are already in `requirements.txt`.
