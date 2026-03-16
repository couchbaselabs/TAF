# TAF Repository Inventory

## Languages and Runtimes
- **Primary**: Python 3.10.14 (main test framework)
- **Secondary**: Java (DocLoader submodule)
- **Runtime**: unittest

## Package and Build Tools

### Build Tools
- `testrunner.py` – Primary test execution engine
- `Makefile` – Package distribution and basic test orchestration(used by dev team for unit-testing for local changes)

### Package Management
- `requirements.txt` – Python dependencies (uses unpinned versions)
  - Key deps: couchbase==4.4.0, paramiko, requests, boto3, deepdiff, pandas
- No lockfile (no poetry.lock or requirements.lock)
- DocLoader managed as git submodule

## Key Directories

**Test Implementation**
- `pytests/` – All test code (41+ component directories)
  - `basetestcase.py` – Base test class factory
  - `epengine/` – KV engine tests
  - `cbas/` – Columnar/Analytics tests
  - `security/` – Security and authorization tests
  - `storage/[magma|plasma|fusion]/` – Storage engine tests
  - `upgrade/` – Version upgrade validation
  - `Capella/` – Capella REST API tests
  - `Atomicity/` – ACID transaction tests

**Core Libraries**
- `lib/sdk_client3.py` – Python SDK client (72KB)
- `lib/couchbase_helper/` – Cluster operations, document generation
- `lib/BucketLib/` – Bucket operations
- `lib/CbasLib/` – Columnar operations
- `lib/SecurityLib/` – Security utilities
- `lib/SystemEventLogLib/` – Event log validation
- `lib/framework_lib/` – Test runner framework
- `lib/Jython_tasks/` – Jython task execution
- `lib/memcached/` – Memcached protocol clients
- `lib/backup_service_client/` – Backup service API client
- `lib/capellaAPI/` – Capella API libraries (submodule)

**Feature Utilities**
- `couchbase_utils/cb_server_rest_util/` – REST API endpoint mappings
- `couchbase_utils/security_utils/` – TLS, encryption, X.509 certificates
- `couchbase_utils/bucket_utils/` – Bucket management
- `couchbase_utils/cluster_utils/` – Cluster operations
- `couchbase_utils/upgrade_utils/` – Upgrade logic
- `couchbase_utils/rebalance_utils/` – Rebalancing helpers
- `couchbase_utils/dcp_utils/` – DCP protocol utilities

**Infrastructure**
- `platform_utils/ssh_util/` – Paramiko SSH management
- `platform_utils/error_simulation/` – Network/disk error simulation
- `platform_utils/docker_utils/` – Docker operations

**Configuration**
- `conf/` – Test suite configurations (280+ .conf files)
- `node.ini` – Cluster topology template
- `logging.conf` – Logging configuration template

**Constants**
- `py_constants/cb_constants/` – Couchbase server constants
- `py_constants/cb_constants/CBServer.py` – Version mappings
- `py_constants/cb_constants/system_event_log.py` – Event schemas
- `constants/platform_constants/` – Platform-specific values
- `constants/cloud_constants/` – Cloud provider data

## Test Entry Points

### Primary Test Runner
- **File**: `testrunner.py`
- **Framework**: unittest
- **Usage**: `python testrunner.py -i node.ini -c conf/sanity.conf`

### Configuration Format
- `.ini` files: Cluster topology and credentials
- `.conf` files: Test selections with parameters
  Format: `module.class.test_method,param1=value1,param2=value2`

## Important Configurations

### Python Path Setup
```python
sys.path = [".", "lib", "pytests", "pysystests", "couchbase_utils",
            "platform_utils", "platform_utils/ssh_util",
            "connections", "constants", "py_constants"]
```

### Runtype Selection (basetestcase.py)
- `dedicated` → `ProvisionedBaseTestCase`
- `serverless` → `OnCloudBaseTest`
- `columnar` → `ColumnarBaseTest`
- `default` → `OnPremBaseTest`

### Command-Line Arguments (testrunner.py)
- `-i, --ini` – Required: cluster configuration file
- `-c, --config` – Test suite configuration
- `-t, --test` – Single test specification
- `-m, --mode` – doc_loader mode (rest/cli/java)
- `-p, --params` – Global parameters
- `--launch_java_doc_loader` – Launch Java doc loader
- `--launch_sirius_process` – Launch Sirius client
- `-g, --globalsearch` – Global test search

## Dependencies

### Core Python Libraries
- `couchbase==4.4.0` – Couchbase Python SDK
- `paramiko` – SSH client
- `requests` – HTTP requests
- `boto3==1.42.59` – AWS SDK
- `deepdiff` – Deep comparison
- `pandas==2.2.3` – Data manipulation

### Analytics/Data
- `delta-spark==3.2.1` – Delta Lake Spark
- `pyspark==3.5.3` – Apache Spark
- `google-cloud-storage` – GCS integration
- `azure-storage-blob` – Azure blob storage

### Testing
- `unittest` – Built-in Python testing

## Unknowns

**Missing Configurations**
- No `.gitignore` excludes `.env` files hardcoded secrets
- No .env.example template for environment variables
- No pyproject.toml or setup.cfg for modern Python packaging
- No pre-commit hooks configuration
- No linting/formatting configuration

**Submodule Status**
- `DocLoader/` – Java-based document loader (submodule)
- `lib/capellaAPI/` – Capella API libraries (submodule)

**Logging Configuration**
- `logging.conf` – Template requiring filename replacement
- `logging.conf.sample` – Sample configuration
