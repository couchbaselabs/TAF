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

- Cloud provider abstraction for cbbackupmgr/cbcontbk backup-restore tests, and for WORM immutability tests that need to poke the object store directly.
- One interface, 4 providers: AWS, Azure, GCP, Localstack.
- Each provider: reads own env-var credentials, formats itself as CLI flags, cleans up its object-store location, builds/creates its Credential Store payload, and (AWS/Azure/GCP only) implements the WORM object-store primitives.

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

## WORM object-store operations

Originally prototyped as a standalone `WormCloudHelper` in `pytests/backup_restore/WORM_backup/worm_cloud_helper.py`; that file has since been removed and its logic fully absorbed here — `pytests/backup_restore/WORM_backup/*.py` calls these methods on `self.backup_cloud_provider` (the provider CollectionBase.setUp() already built from `cbbackup_test`) directly, via `WormBackupBase._require_cloud_helper()`/`_require_storage_provider()`. Localstack does NOT implement these primitives (WORM/object-lock semantics aren't meaningful there, and ABC enforcement is a no-op per the fact above, so this doesn't error).

7 abstract primitives, one real SDK call each, implemented in AWSProvider/AzureProvider/GCPProvider:

| Method | Purpose |
|---|---|
| `list_objects(archive_uri, repo_name, relative_prefix="")` | List object keys/blob names under `repo_name` |
| `object_exists(archive_uri, repo_name, relative_path)` | `bool` |
| `read_text(archive_uri, repo_name, relative_path)` | Object body as `str` |
| `get_retention_until(archive_uri, repo_name, relative_path)` | Unix timestamp, or `None` if unset |
| `attempt_overwrite(archive_uri, repo_name, relative_path, content="tampered")` | `(succeeded: bool, message: str)` |
| `delete_object(archive_uri, repo_name, relative_path)` | `(succeeded: bool, message: str)` |
| `list_object_versions(archive_uri, repo_name, relative_path)` | `[{version_id, is_latest, delete_marker}, ...]` |

Every method takes `archive_uri` (e.g. `s3://bucket/prefix`) per call rather than storing it on `self` — same convention as `cleanup_for_bkrs(location)`, since one provider instance is reused across archive locations within a test run.

Concrete (non-abstract) helpers on `CloudProviderInterface` itself, built only on the primitives above — generic across every provider, so implemented once instead of duplicated per subclass:

| Method | Purpose |
|---|---|
| `_parse_location(archive_uri)` (staticmethod) | `{"bucket": <netloc>, "prefix": <path>}`; `AzureProvider` overrides to also accept `https://<account>.blob.core.windows.net/<container>/<prefix>` |
| `_object_path(prefix, repo_name, relative_path="")` (staticmethod) | Joins prefix/repo/relative into one key |
| `_to_timestamp(value)` (staticmethod) | Normalizes `int`/`float`/`datetime`/ISO-`str` → unix timestamp |
| `relative_path(archive_uri, repo_name, object_name)` | Strips the repo prefix off a full object key |
| `read_json(archive_uri, repo_name, relative_path)` | `json.loads(self.read_text(...))` |
| `upload_text(archive_uri, repo_name, relative_path, content)` | Alias for `attempt_overwrite(...)` — same body, different call-site intent |
| `find_backup_names(archive_uri, repo_name)` | Sorted top-level backup dir names under the repo |
| `find_latest_backup_name(archive_uri, repo_name)` | Last of `find_backup_names(...)`, or `None` |
| `find_relative_paths(archive_uri, repo_name, suffix=None, contains=None)` | Filtered relative paths |
| `find_metadata_path(archive_uri, repo_name, names)` | First relative path ending in any of `names` |
| `find_first_data_object(archive_uri, repo_name)` | First object that isn't `.worm`/`.statusflag`/`.obj_versions`/`plan.json`/a directory marker |
| `wait_for_objects(archive_uri, repo_name, timeout=300, relative_prefix="")` | Polls `list_objects(...)` every 10s until non-empty or timeout |

`CloudOperationError` (in `cloud_provider_interface.py`) is the shared exception raised by provider-specific credential validation (e.g. `AWSProvider.validate_credentials()`, `GCPProvider.validate_credentials()` — not part of the interface contract since Azure doesn't need an equivalent check).

## AWSProvider

| Env var | Meaning |
|---|---|
| `AWS_REGION` | Region |
| `AWS_ACCESS_KEY_ID` | Access key ID |
| `AWS_SECRET_ACCESS_KEY` | Secret access key |

- `get_cbbackupmgr_flags()`/`get_cbconbk_flags()`: `--obj-region <AWS_REGION> --obj-access-key-id <AWS_ACCESS_KEY_ID> --obj-secret-access-key <AWS_SECRET_ACCESS_KEY>`
- `cleanup_for_bkrs(s3_path)` — `s3://bucket-name/some-dir`; `boto3.resource("s3")` deletes objects with key prefix `some-dir/` (real prefix delete, bucket untouched).
- `create_credential_store(...)` — `CredentialStoreUtils.build_aws_payload()` from `aws_access_key_id`/`aws_secret_access_key`/`aws_region` → `create_credential(...)`.
- `validate_credentials()` — raises `CloudOperationError` if access key ID/secret are missing. AWS-only (not on the interface).
- WORM ops use a lazily-cached low-level `boto3.client("s3", ...)` (`self._s3_client`, built on first use via `_client()`) — distinct from the `boto3.resource("s3")` created fresh inside `cleanup_for_bkrs()` each call. `list_objects`/`list_object_versions` paginate via `ContinuationToken`/`KeyMarker`+`VersionIdMarker`. `get_retention_until` tries `get_object_retention()` first, falls back to `head_object()`'s `ObjectLockRetainUntilDate`.

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
- WORM ops use a lazily-cached `BlobServiceClient` (`self._blob_service_client`, via `_client()`) plus `_blob_client(container, key)`. `get_retention_until` reads `blob_properties.immutability_policy.expiry_time` (falls back to `.expires_on`). `list_object_versions` uses `list_blobs(include=["versions"])`.
- Overrides `_parse_location()` to additionally accept `https://<account>.blob.core.windows.net/<container>/<prefix>` (falls back to the base `scheme://netloc/path` parsing for `az://`/`azblob://`).

## GCPProvider

| Env var | Meaning |
|---|---|
| `GOOGLE_APPLICATION_CREDENTIALS` | Path to GCP service-account key JSON (standard GCP SDK var), read on the test controller |
| `GCP_REGION` | Region |

- Flags: `--obj-auth-file <path> --obj-region <GCP_REGION>`, where `<path>` is `REMOTE_AUTH_FILE_PATH` (`/tmp/tmp_gcp_service_account.json`) once a `shell` is passed to `get_cbbackupmgr_flags`/`get_cbconbk_flags` — cbbackupmgr/cbcontbk run on a cluster node, not the controller, so the raw `GOOGLE_APPLICATION_CREDENTIALS` path wouldn't resolve there. `_stage_remote_auth_file()` SFTPs the key file (`shell.copy_file_local_to_remote`) to that fixed path the first time a given `shell.ip` is seen, tracked in `_remote_auth_file_staged_hosts`, then reuses it on subsequent calls to the same node. No `shell` → falls back to the local `gcp_service_account_key_file` path.
- `cleanup_for_bkrs(gcs_path)` — `gs://bucket-name/some-dir`; loads service-account JSON (`json.load`) from the **local** `gcp_service_account_key_file` (this runs on the controller via the GCP SDK, not over `shell`), `google.cloud.storage.Client.from_service_account_info(...)` deletes blobs with prefix `some-dir/` (bucket untouched).
- `create_credential_store(...)` — reads `gcp_service_account_key_file` **raw** (whole file as one string, NOT `json.load`-parsed — different from `cleanup_for_bkrs`), builds `build_gcp_service_account_payload(json_credentials, gcp_region, ...)` → `create_credential(...)`.
- `validate_credentials()` — raises `CloudOperationError` if `gcp_service_account_key_file` is unset. GCP-only (not on the interface).
- WORM ops use a lazily-cached `storage.Client` (`self._gcs_client`, via `_client()` — loads/parses the local key file once, separate from `cleanup_for_bkrs()`'s own fresh load). `get_retention_until` calls `blob.reload()` then reads `retention_expiration_time`. `list_object_versions` needs `bucket.list_blobs(prefix=key, versions=True)` (GCS "versions" = object generations).

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
