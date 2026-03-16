# Columnar Pytest Agent Generation Guide

## Deployment Modes & Base Class Inheritance

**Columnar** is Couchbase's analytics service with two deployment modes:
- **Enterprise Analytics** (On-Prem) - Self-hosted clusters
- **Capella Analytics** (Cloud) - Managed cloud services

---

### Cloud Mode (`runtype="columnar"`)

**Inheritance Flow**:
```
Test → ColumnarBaseTest → ProvisionedBaseTestCase → CapellaBaseTest → CouchbaseBaseTest
```

**What Each Base Class Does (Available Context When Creating New Test)**:
- **CouchbaseBaseTest** (`pytests/cb_basetest.py`): Provides base testing framework with logging (self.log), task management (self.task_manager, self.task), input parameters (self.input), and SDK client management
- **CapellaBaseTest** (`pytests/dedicatedbasetestcase.py`): Initializes Capella tenant/project/cluster infrastructure, provides Capella utilities (self.capella, self.pod, self.tenant), manages cloud credentials and API keys
- **ProvisionedBaseTestCase** (`pytests/dedicatedbasetestcase.py`): Handles Couchbase cluster provisioning in Capella, manages cluster lifecycles, provides Couchbase cluster objects (self.clusters) and REST clients
- **ColumnarBaseTest** (`pytests/Columnar/columnar_base.py`): **Your main entry point for cloud tests** - initializes analytics instances via Capella V4 API (self.capellaAPI), sets up columnar_cluster (self.columnar_cluster) and optional remote_cluster (self.remote_cluster), configures CbasUtil (self.cbas_util), manages AWS/integration credentials, provides document loading infrastructure

**When you create a new cloud test, you inherit access to**:
```python
self.columnar_cluster      # Capella analytics instance object
self.remote_cluster        # Optional Couchbase KV cluster (if specified)
self.capellaAPI            # Capella V4 API client for instance management
self.cbas_util             # CbasUtil for analytics operations
self.analytics_cluster     # Alias for self.columnar_cluster
self.analytics_api         # AnalyticsRestAPI (initialize yourself)
self.log                   # Logger instance
self.task_manager          # Task management for async operations
self.input                 # Test input parameters
self.bucket_util           # Bucket utilities (if remote cluster exists)
```

**Example Usage**:
```python
from pytests.Columnar.columnar_base import ColumnarBaseTest

class CloudTest(ColumnarBaseTest):
    def setUp(self):
        super(CloudTest, self).setUp()
        # Base class already provided: self.columnar_cluster, self.capellaAPI, self.cbas_util
        self.analytics_api = AnalyticsRestAPI(self.analytics_cluster.master)
        self.columnar_spec = self.cbas_util.get_columnar_spec("full_template")
```

---

### On-Prem Mode (`runtype="onprem-columnar"`)

**Inheritance Flow**:
```
Test → ColumnarOnPremBase → CBASBaseTest → BaseTestCase → OnPremBaseTest → CouchbaseBaseTest
```

**What Each Base Class Does (Available Context When Creating New Test)**:
- **CouchbaseBaseTest** (`pytests/cb_basetest.py`): Provides base testing framework with logging (self.log), task management (self.task_manager, self.task), input parameters (self.input), and SDK client management
- **OnPremBaseTest** (`pytests/onPrem_basetestcase.py`): Handles local/VM cluster setup, provides cluster management utilities, initializes Couchbase cluster objects (self.cb_clusters), manages REST connections to cluster nodes
- **BaseTestCase** (`pytests/basetestcase.py`): Dynamic base class selection layer - when runtype="onprem-columnar", this selects OnPremBaseTest as the parent, providing unified interface regardless of runtype
- **CBASBaseTest** (`pytests/cbas/cbas_base_EA.py`): **Enterprise Analytics base** - initializes analytics service on local clusters, provides SDK for analytics (self.sdk_client_pool), manages rebalance utilities (self.rebalance_util), sets up security/certificates for local clusters
- **ColumnarOnPremBase** (`pytests/Columnar/onprem/columnar_onprem_base.py`): **Your main entry point for on-prem tests** - extends CBASBaseTest for columnar testing, provides Sirius document loading (load_remote_collections method), configures columnar-specific utilities, separates analytics from KV clusters in self.cb_clusters

**When you create a new on-prem test, you inherit access to**:
```python
self.analytics_cluster      # Local analytics cluster (from self.cb_clusters)
self.columnar_cluster       # Alias for self.analytics_cluster
self.remote_cluster          # KV cluster (if multi-cluster setup)
self.cb_clusters            # Dictionary of all clusters {cluster_name: cluster_obj}
self.cbas_util              # CbasUtil for analytics operations
self.rebalance_util         # CBASRebalanceUtil for rebalance operations
self.sdk_client_pool        # SDK client pool for SDK-based operations
self.analytics_api          # AnalyticsRestAPI (initialize yourself)
self.log                    # Logger instance
self.task_manager           # Task management for async operations
self.input                  # Test input parameters
self.bucket_util            # Bucket utilities
```

**Example Usage**:
```python
from pytests.Columnar.onprem.columnar_onprem_base import ColumnarOnPremBase

class OnPremTest(ColumnarOnPremBase):
    def setUp(self):
        super(OnPremTest, self).setUp()
        # Base class already provided: self.analytics_cluster, self.cbas_util, self.rebalance_util
        self.analytics_api = AnalyticsRestAPI(self.analytics_cluster.master)
        self.columnar_spec = self.cbas_util.get_columnar_spec("full_template")
        
        # Use built-in document loading (if needed)
        self.load_remote_collections(
            cluster=self.remote_cluster,
            create_percent=100,  # 100% creates
            create_start_index=0, create_end_index=1000
        )
```

---

### Dynamic Base Class Selection

**Location**: `pytests/basetestcase.py`

```python
runtype = TestInputSingleton.input.param("runtype", "default").lower()

if runtype == "columnar":
    from columnarbasetestcase import ColumnarBaseTest as CbBaseTest
else:
    from onPrem_basetestcase import OnPremBaseTest as CbBaseTest

class BaseTestCase(CbBaseTest):
    pass
```

---

---

## Available Context vs Manual Initialization

### What's Automatically Provided (Inherited from Base Classes)

**Common to Both Modes**:
- `self.log` - Logger instance (info, warning, error methods)
- `self.task_manager` - Task management for async operations
- `self.task` - Task object for test operations
- `self.input` - Test input parameters from command line/config
- `self.cbas_util` - CbasUtil for analytics operations (dataset management, query execution)

**Cloud Mode Only**:
- `self.columnar_cluster` - Capella analytics instance object
- `self.remote_cluster` - Optional Couchbase KV cluster
- `self.capellaAPI` - Capella V4 API client
- `self.bucket_util` - Bucket utilities (if remote cluster exists)
- AWS credentials (self.aws_access_key, self.aws_secret_key, etc.)

**On-Prem Mode Only**:
- `self.analytics_cluster` / `self.columnar_cluster` - Local analytics cluster
- `self.remote_cluster` - KV cluster (if multi-cluster)
- `self.cb_clusters` - Dictionary of all clusters
- `self.rebalance_util` - CBAS rebalance utilities
- `self.sdk_client_pool` - SDK client pool for SDK operations
- `self.bucket_util` - Bucket utilities

### What You Must Initialize (Test Author Responsibilities)

**Required for Most Tests**:
```python
self.analytics_api = AnalyticsRestAPI(self.analytics_cluster.master)
self.columnar_spec = self.cbas_util.get_columnar_spec(self.columnar_spec_name)
```

**Optional Based on Test Needs**:
```python
# For query execution
from couchbase_utils.cb_server_rest_util.analytics.analytics_api import AnalyticsRestAPI

# For SDK-based operations (if self.use_sdk_for_cbas)
from sdk_client3 import SDKClient

# For document loading (on-prem)
from Jython_tasks.java_loader_tasks import SiriusCouchbaseLoader
```

---

## Core Utilities

### Essential Classes

**CbasUtil** (`couchbase_utils/cbas_utils/cbas_utils_columnar.py`)
- Main analytics operations utility (7,575+ lines)
- Handles dataset creation, query execution, validation
- Manages specs, links, and infrastructure

**AnalyticsRestAPI** (`couchbase_utils/cb_server_rest_util/analytics/analytics_api.py`)
- REST API client for analytics operations
- Supports async request management and polling

**CapellaAPI** (`capellaAPI/capella/dedicated/CapellaAPI_v4.py`)
- V4 API client for Capella cloud operations
- Manages tenants, projects, clusters, and analytics instances

---

## Test Structure Pattern

### Standard Test Template

```python
class NewTest(ColumnarBaseTest):  # or ColumnarOnPremBase
    def setUp(self):
        super(NewTest, self).setUp()
        self.analytics_api = AnalyticsRestAPI(self.analytics_cluster.master)
        self.cbas_util = CbasUtil(self.task, self.use_sdk_for_cbas)
        self.columnar_spec = self.cbas_util.get_columnar_spec(self.columnar_spec_name)
        
    def test_method_name(self):
        # 1. Setup infrastructure
        self.columnar_spec = self.populate_columnar_infra_spec(...)
        self.cbas_util.create_cbas_infra_from_spec(...)
        
        # 2. Execute operations
        status, result, response = self.analytics_api.submit_service_request(...)        
        # 3. Validate results
        self.assertTrue(status and response.status_code == 200)
        
    def tearDown(self):
        if hasattr(self, "columnar_spec"):
            self.cbas_util.delete_cbas_infra_created_from_spec(self.columnar_cluster)
        super(NewTest, self).tearDown()
```

---

## Configuration & Execution

### Configuration Files

**Cloud Tests**: `conf/columnar/*.conf` (32 files)
**On-Prem Tests**: `conf/columnar/onprem/*.conf` (12 files)

**Example config**:
```
Columnar.async_rest_api.AsyncRestApi:
    test_request_success,GROUP=sanity
    test_status_queued,GROUP=sanity,num_remote_links=1,num_remote_collections=1,initial_doc_count=1000,doc_size=1024
```

### Command Line Execution

**Cloud Execution**:
```bash
export AWS_ACCESS_KEY_ID=<access_key> AWS_SECRET_ACCESS_KEY=<secret_key> AWS_SESSION_TOKEN=<token>
python testrunner.py -i b/resources/capella/goldfish-template.ini \
  -c conf/columnar/copy_into_standalone_collection_from_blob_storage.conf \
  -p runtype=columnar,GROUP=sanity,num_nodes_in_columnar_instance=4,instance_type=8vCPUs:32GB \
  -m rest
```

**On-Prem Execution**:
```bash
export AWS_ACCESS_KEY_ID=<access_key> AWS_SECRET_ACCESS_KEY=<secret_key> AWS_SESSION_TOKEN=<token>
python testrunner.py -i b/resources/2-nodes-template.ini \
  -c conf/columnar/onprem/async_rest_api.conf \
  -p runtype=onprem-columnar,services_init=kv|columnar,nodes_init=1|1,GROUP=sanity \
  -m rest
```

---

## Development Guidelines

### Best Practices

1. **Naming Conventions**
   - Test classes: `Test[Feature]` or `[Feature]Test`
   - Test methods: `test_[specific_scenario]`
   - Config files: `[feature_test].conf`

2. **Setup/Teardown**
   - Always call parent setUp/tearDown methods
   - Clean up created resources in tearDown
   - Handle errors gracefully in teardown

3. **Assertions**
   - Use specific assertions with descriptive messages
   - Check status codes, response structures, and data

## References

### External Documentation
- **Enterprise Analytics (On-Prem)**: https://docs.couchbase.com/enterprise-analytics/current/intro/intro.html
- **Capella Analytics (Cloud)**: https://docs.couchbase.com/analytics/intro/intro.html
- **SQL++ Reference**: https://docs.couchbase.com/enterprise-analytics/current/sqlpp/1_intro.html
- Capella API Documentation: Internal Capella docs
- Couchbase Analytics Service: Analytics service docs
- Python Testing Framework: pytest, unittest documentation

**Framework Version**: TAF Master Branch | **Test Coverage**: 46+ test files, 44+ config files
