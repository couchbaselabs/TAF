# TAF Domain Glossary

## Couchbase Server Terms

### Couchbase Server Supported Services

**Core Data Services**
- **Data Service (kv)** – Stores, updates, and retrieves data items by key. Core storage functionality for the cluster.

**Indexing and Query Services**
- **Query Service (n1ql)** – Parses and executes SQL++ queries, and returns results。 Interacts with Data and Index services。
- **Index Service (index)** – Creates indexes for use by Query and Analytics services。

**Advanced Analytics and Search Services**
- **Analytics Service (cbas)** – Performs join, set, aggregation, and grouping operations。 Designed for large, long-running workloads that require significant memory and CPU resources。
- **Search Service (fts)** – Creates indexes for fully customizable search experience。 Supports near real-time search capabilities for diverse data types including text, dates, geospatial data, and vectors。

**Event Processing and Data Management Services**
- **Eventing Service (eventing)** – Handles near real-time responses to data changes。 Executes code in response to document mutations or on scheduled timers。
- **Backup Service (backup)** – Schedules full and incremental backups for specific buckets or all buckets in cluster。 Supports merging and pruning of existing backups。

**Service Deployment Notes**
- Services are configured per node with each node capable of running up to 7 services total
- Data Service must run on at least 1 node in the cluster
- Services can be deployed, maintained, and provisioned independently for Multi-Dimensional Scaling (MDS)
- Eventing, Backup, Analytics, Index, Search, and Query services can be added/removed on-demand in Couchbase Server 8.0+
- Arbiter nodes can be added in Couchbase Server 7.6+ for fast failover and quorum arbitration

### Core Components
- **NS Server** – Node services (formerly "ns_server"), cluster management and coordination
- **Magma** – Next-generation storage engine for Couchbase (replaces Couchstore in many scenarios)
- **Plasma** – Previous generation storage engine (deprecated)
- **Couchstore** – Legacy storage engine for non-Magma buckets
- **Fusion** – Tiered storage combining RAM and magnetic storage
- **XDCR** – Cross Data Center Replication
- **Arbiter Node** – Special node type in Couchbase Server 7.6+ that provides fast failover and quorum arbitration

### Cluster Topology
- **Node** – Single Couchbase Server instance
- **Cluster** – Collection of nodes working together
- **Bucket** – Namespace for data (similar to database)
- **Scope** – Logical grouping of collections within a bucket
- **Collection** – Logical grouping of documents within a scope
- **Vbucket (VB)** – Virtual bucket, unit of data distribution
- **Replica** – Copy of data for high availability across a different data node

### Operations
- **Rebalance** – Redistributing data across nodes during topology changes
- **Failover** – Promoting replica to primary when node fails
- **Auto-failover** – Automatic detection and promotion of replicas
- **Warmup** – Loading data from disk to RAM on startup
- **Compaction** – Reclaiming space from deleted documents
- **Flush** – Emptying a bucket of all data

### Durability and Consistency
- **Durability Level** – "None", "Majority", "MajorityAndPersist", "PersistToMajority"
- **Sync Write** – Write durability enforcement protocol
- **CAS** – Compare-and-swap for optimistic concurrency
- **OSO** – Observe-by-sequencing, read-your-own-writes consistency

### Storage Features
- **DGM (Data-Greater_than-Memory)** – Memory pressure state where eviction occurs
- **History Retention or CDC** – Keep older document versions for streaming using DCP (Supported only in Magma storage)

## Capella Terms

### Deployment Models
- **On-Premise** – Self-hosted Couchbase Server
- **Dedicated** – Capella dedicated clusters (Provisioned)
- **Serverless** – Capella serverless (shared resources, pay-per-usage)
- **Columnar** – Capella Columnar analytics service

### Capella-Specific
- **Tenant** – Serverless customer/workload isolation unit
- **Project** – Logical grouping of resources in Capella
- **App Service** – Integration framework for external applications
- **On-Off Schedule** – Automated cluster start/stop for cost savings
- **Hibernation** – Pausing serverless cluster to reduce costs

## TAF-Specific Terms

### Test Infrastructure
- **TAF** – This repo
- **testrunner.py** – Primary test execution engine (unittest-based)
- **TestInputSingleton** – Global singleton for test parameter access
- **runtype** – Parameter selecting base test class environment

### Test Execution
- **testrunner.py** - Primary entry point for any testrun
- **.ini file** – Cluster topology and credentials configuration
- **.conf file** – Test suite selection and parameters
- **basetestcase.py** – Base test class factory
- **TestCase** – Individual test method in framework
- **Test suite** – Collection of tests (.conf file)
- **Test run** – Execution of one or more test cases

### Environment Types
- **OnPremBaseTest** – Base for on-premise Couchbase Server tests
- **ProvisionedBaseTestCase** – Base for dedicated cluster tests
- **OnCloudBaseTest** – Base for serverless tests
- **ColumnarBaseTest** – Base for Columnar analytics tests

### Test Parameters
- **get-cbcollect-info** – Collect server logs on test failure
- **skip_cluster_reset** – Skip cluster reset between tests
  - False: Reset cluster before each test
  - True: Preserve cluster state for continuity
- **nodes_init** – Number of nodes to initialize
- **num_items** – Number of documents to load
- **replicas** – Number of replica copies
- **load_ratio** – Data load percentage relative to memory
- **load_docs_using** – Document loading method (default_loader, sirius_java_sdk, or sirius_go_sdk)

### Document Loading
- **default_loader** – Built-in Python SDK document loader (default option)
- **sirius_java_sdk** – Sirius Java SDK for document operations via DocLoader
- **sirius_go_sdk** – Sirius Go SDK for document operations via sirius submodule
- **DocLoader** – Java-based document generator (submodule)
- **Sirius** – Go-based document client
- **Java SDK** – Couchbase Java client for document operations
- **Python SDK** – Couchbase Python SDK (sdk_client3.py)
- **Workload generator** – Tool to create realistic test data

### Test Results
- **XUnit** – XML test result format
- **Pass/Fail** – Test execution status
- **Rerun** – Retrying failed tests
- **Skip** – Tests not executed (excluded or not applicable)

## Protocol and API Terms

### Network Protocols
- **DCP** – Database Change Protocol for replication
- **Memcached ASCII** – Legacy protocol
- **Memcached Binary** – Current protocol for KV operations
- **REST API** – HTTP-based management API
- **GRPC** – Capella Columnar API (gRPC-based)

### Security
- **TLS** – Transport Layer Security
- **X.509** – Certificate-based authentication
- **RBAC** – Role-Based Access Control
- **LDAP** – Lightweight Directory Access Protocol for user auth
- **JWT** – JSON Web Token for Capella SSO
- **CM** – Customer Managed keys (encryption)

## Version and Release Terms

- **Build ID** – Specific server build identifier
- **Version** – Major.minor.patch (e.g., 7.2.4)
- **Branch** – Development line (e.g., cherry-pick-7.2.4)
- **Upgrade** – Migrating to newer version
- **Migration** – Converting data between storage engines

## Testing Concepts

- **Smoke test** – Quick functionality validation
- **Sanity test** – Basic feature verification
- **E2E (End-to-End)** – Full workflow validation
- **Regression** – Ensuring fixes don't break existing features
- **Volume test** – Performance under heavy load
- **Steady state** – Long-running stability test
- **Duration** – Test execution time
- **Throughput** – Operations per second
- **Latency** – Time per operation

## Cloud Platform Terms

- **AWS** – Amazon Web Services
- **Azure** – Microsoft Azure
- **GCS** – Google Cloud Storage
- **S3** – Simple Storage Service (AWS)
- **Blob Storage** – Azure object storage
- **Kafka** – Event streaming platform (Confluent, Molo17)

## Advanced Terms

- **Guardrails** – Resource limits enforcement (disk, data size, buckets)
- **C-Group** – Control Groups (Linux resource management)
- **Delta Lake** – ACID transactions on data lakes
- **Iceberg** – Table format for large analytic datasets
- **Parquet** – Columnar storage format
- **Heterogeneous Index** – Multiple index types on same data

## Acronym Quick Reference

- TAF – Test Automation Framework
- KV – Key-Value
- NS – Node Services
- GSI – Global Secondary Index
- DCP – Database Change Protocol
- DGM – Data-Get-Me
- TTL – Time To Live
- CBAS – Couchbase Analytics Service
- N1QL – Named Query Language (Query Service)
- FTS – Full Text Search
- API – Application Programming Interface
- REST – Representational State Transfer
- TLS – Transport Layer Security
- RBAC – Role-Based Access Control
- JWT – JSON Web Token
- CM – Customer Managed
- XDCR – Cross Data Center Replication
